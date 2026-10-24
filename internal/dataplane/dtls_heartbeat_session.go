// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// dtlsFlowAckWait bounds how long the first local datagram waits for the
	// server's flow v1 acknowledgement before legacy record framing is used.
	dtlsFlowAckWait = 2 * time.Second
	// dtlsHeartbeatUnit scales the heartbeat interval the server announces.
	dtlsHeartbeatUnit = time.Second
)

// dtlsHeartbeatMissLimit is how many silent heartbeat intervals end a session.
const dtlsHeartbeatMissLimit = 3

var errDTLSServerUnresponsive = errors.New("dtls: server stopped answering heartbeats")

// Framing modes of one DTLS session. Undecided moves to legacy at most once
// and to flow at most once; flow is final.
const (
	dtlsFramingUndecided int32 = iota
	dtlsFramingLegacy
	dtlsFramingFlow
)

// dtlsUDPSessionOptions tune one DTLS reverse UDP session.
type dtlsUDPSessionOptions struct {
	ackWait       time.Duration
	heartbeatUnit time.Duration
	// clock returns monotonic time elapsed since the session started; nil
	// uses the process monotonic clock.
	clock func() time.Duration
}

func defaultDTLSUDPSessionOptions() dtlsUDPSessionOptions {
	return dtlsUDPSessionOptions{ackWait: dtlsFlowAckWait, heartbeatUnit: dtlsHeartbeatUnit}
}

// dtlsBootstrapRecord encodes the first DTLS record of a reverse UDP session.
// "framing":"record" keeps an old server on record framing; "flow":"v1" asks a
// new server for flow framing, which it confirms with an ack record.
func dtlsBootstrapRecord(authToken, tunnelID, udpDst string) ([]byte, error) {
	return encodePreface(map[string]string{
		"auth": authToken, "tunnel_id": tunnelID, "dst": udpDst, "framing": dtlsFramingRecord, "flow": dtlsFlowBootstrapValue,
	})
}

// dtlsUDPSession relays one DTLS reverse UDP session. It starts undecided,
// switches to flow framing for good on the first server ack (late acks
// included) and starts the heartbeat then; when the first local datagram finds
// no ack after ackWait it uses legacy record framing, whose replies go to the
// last local sender.
type dtlsUDPSession struct {
	conn  io.ReadWriter
	local *net.UDPConn
	opts  dtlsUDPSessionOptions
	flows *flowRegistry
	// writeMu serializes record writes from the local relay and the heartbeat.
	writeMu     sync.Mutex
	mode        atomic.Int32
	acked       chan struct{}
	ackOnce     sync.Once
	ackDeadline time.Time
	// clock is monotonic time since the session started; lastRecv is its
	// reading at the last record from the server, so wall-clock steps never
	// count as silence or hide it.
	clock     func() time.Duration
	lastRecv  atomic.Int64
	lastSrcMu sync.RWMutex
	lastSrc   *net.UDPAddr
	errCh     chan error
}

// runDTLSUDPSession relays datagrams between the local socket and a DTLS
// connection whose flow v1 bootstrap was already written. It returns the first
// relay or heartbeat error; the caller closes conn and uc, which ends the relay
// goroutines.
func runDTLSUDPSession(ctx context.Context, conn io.ReadWriter, uc *net.UDPConn, opts dtlsUDPSessionOptions) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s := &dtlsUDPSession{
		conn: conn, local: uc, opts: opts, flows: newUDPFlowRegistry(), clock: opts.clock,
		acked: make(chan struct{}), ackDeadline: time.Now().Add(opts.ackWait), errCh: make(chan error, 3),
	}
	if s.clock == nil {
		start := time.Now()
		s.clock = func() time.Duration { return time.Since(start) }
	}
	go s.readServerRecords(ctx)
	go s.forwardLocalDatagrams(ctx)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-s.errCh:
		return err
	}
}

func (s *dtlsUDPSession) fail(err error) {
	select {
	case s.errCh <- err:
	default:
	}
}

func (s *dtlsUDPSession) forwardLocalDatagrams(ctx context.Context) {
	buf := make([]byte, udpMaxPacketSize)
	var drops uint64
	for {
		n, src, err := s.local.ReadFromUDP(buf)
		if err != nil {
			s.fail(err)
			return
		}
		if n <= 0 {
			continue
		}
		if err := s.sendLocalDatagram(ctx, src, buf[:n], &drops); err != nil {
			s.fail(err)
			return
		}
	}
}

func (s *dtlsUDPSession) sendLocalDatagram(ctx context.Context, src *net.UDPAddr, payload []byte, drops *uint64) error {
	if s.framing(ctx) == dtlsFramingFlow {
		if len(payload) > maxDTLSFlowPayload {
			logDTLSPayloadDrop(drops, len(payload), maxDTLSFlowPayload)
			return nil
		}
		frame, err := encodeUDPFlowFrame(s.flows.udpFlowID(src), payload)
		if err != nil {
			return err
		}
		return s.writeRecord(frame)
	}
	if len(payload) > maxDTLSRecordPayload {
		logDTLSRecordDrop(drops, len(payload))
		return nil
	}
	s.lastSrcMu.Lock()
	s.lastSrc = src
	s.lastSrcMu.Unlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return writeUDPDatagramRecord(s.conn, payload)
}

// framing returns the session's framing, deciding it on first use: it waits
// for the ack until ackDeadline and falls back to legacy framing after that.
func (s *dtlsUDPSession) framing(ctx context.Context) int32 {
	if mode := s.mode.Load(); mode != dtlsFramingUndecided {
		return mode
	}
	timer := time.NewTimer(time.Until(s.ackDeadline))
	defer timer.Stop()
	select {
	case <-s.acked:
	case <-ctx.Done():
	case <-timer.C:
		if s.mode.CompareAndSwap(dtlsFramingUndecided, dtlsFramingLegacy) {
			log.Printf("dtls: server did not acknowledge flow framing within %s; using legacy record framing", s.opts.ackWait)
		}
	}
	return s.mode.Load()
}

func (s *dtlsUDPSession) writeRecord(record []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	n, err := s.conn.Write(record)
	if err != nil {
		return err
	}
	if n != len(record) {
		return io.ErrShortWrite
	}
	return nil
}

func (s *dtlsUDPSession) readServerRecords(ctx context.Context) {
	buf := make([]byte, udpMaxPacketSize)
	var drops uint64
	for {
		n, err := s.conn.Read(buf)
		if err != nil {
			s.fail(err)
			return
		}
		s.lastRecv.Store(int64(s.clock()))
		if err := s.handleServerRecord(ctx, buf[:n], &drops); err != nil {
			s.fail(err)
			return
		}
	}
}

func (s *dtlsUDPSession) handleServerRecord(ctx context.Context, record []byte, drops *uint64) error {
	if interval, ok := parseDTLSFlowAck(record, s.opts.heartbeatUnit); ok {
		s.acknowledge(ctx, interval)
		return nil
	}
	if _, ok := dtlsControlOp(record); ok {
		return nil // A pong only proves the server is alive (lastRecv).
	}
	if s.mode.Load() == dtlsFramingFlow {
		flowID, payload, err := parseUDPFlowRecord(record, maxDTLSFlowPayload)
		if err != nil {
			logDTLSPayloadDrop(drops, len(record), maxDTLSFlowPayload)
			return nil
		}
		return deliverUDPFlowDatagram(s.local, s.flows, flowID, payload, drops)
	}
	payload, err := parseUDPDatagramRecord(record)
	if err != nil {
		logDTLSRecordDrop(drops, len(payload))
		return nil
	}
	s.lastSrcMu.RLock()
	dst := s.lastSrc
	s.lastSrcMu.RUnlock()
	if dst == nil {
		return nil
	}
	_, err = s.local.WriteToUDP(payload, dst)
	return err
}

// acknowledge switches the session to flow framing for good; the first ack
// also releases a datagram waiting in framing and starts the heartbeat.
func (s *dtlsUDPSession) acknowledge(ctx context.Context, interval time.Duration) {
	if previous := s.mode.Swap(dtlsFramingFlow); previous == dtlsFramingLegacy {
		log.Printf("dtls: late flow framing acknowledgement; switching from legacy record framing")
	}
	s.ackOnce.Do(func() {
		close(s.acked)
		go s.heartbeat(ctx, interval)
	})
}

// heartbeat checks every half interval. Once no record arrived for one
// interval it pings, and again every half interval while the silence lasts (a
// lost ping or pong does not cost a whole interval); after
// dtlsHeartbeatMissLimit silent intervals it ends the session so the reconnect
// loop redials a dead server. Any record (data, pong, ack) counts as alive.
// Silence is measured on the session's monotonic clock.
func (s *dtlsUDPSession) heartbeat(ctx context.Context, interval time.Duration) {
	tick := max(interval/2, time.Millisecond)
	timer := time.NewTimer(tick)
	defer timer.Stop()
	pingedAt := time.Duration(-1)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		now, last := s.clock(), time.Duration(s.lastRecv.Load())
		silent := now - last
		if silent >= dtlsHeartbeatMissLimit*interval {
			s.fail(fmt.Errorf("%w (silent for %s)", errDTLSServerUnresponsive, silent.Round(time.Millisecond)))
			return
		}
		// A record after the last ping starts a new silent period.
		if silent >= interval && (pingedAt < last || now-pingedAt >= tick) {
			if err := s.writeRecord(dtlsControlRecord(dtlsControlPing)); err != nil {
				s.fail(err)
				return
			}
			pingedAt = now
		}
		timer.Reset(tick)
	}
}
