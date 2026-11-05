// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testRecordFlow   = "flow"
	testRecordLegacy = "legacy"
	testRecordPing   = "ping"
	testRecordOther  = "other"
)

func testFlowAck(interval uint16) []byte {
	ack := []byte{0x00, 0x00, 0x01, 0x00, 0x00}
	binary.BigEndian.PutUint16(ack[3:], interval)
	return ack
}

// classifyTestRecord decodes a record the client wrote, independently of the
// production parser.
func classifyTestRecord(record []byte) (testFlowFrame, string) {
	switch {
	case bytes.Equal(record, []byte{0xFF, 0xFF, 0x01}):
		return testFlowFrame{}, testRecordPing
	case len(record) > 6 && int(binary.BigEndian.Uint16(record[:2])) == len(record)-6:
		return testFlowFrame{flowID: binary.BigEndian.Uint32(record[2:6]), payload: record[6:]}, testRecordFlow
	case len(record) > 2 && int(binary.BigEndian.Uint16(record[:2])) == len(record)-2:
		return testFlowFrame{payload: record[2:]}, testRecordLegacy
	default:
		return testFlowFrame{}, testRecordOther
	}
}

type dtlsSessionHarness struct {
	peer    net.Conn
	records chan []byte
	local   string
	// stopped is closed once the session returned err.
	stopped chan struct{}
	err     error
}

// startDTLSSessionHarness runs a session over an in-memory record pipe. The
// test side of the pipe stands in for the server.
func startDTLSSessionHarness(t *testing.T, opts dtlsUDPSessionOptions) *dtlsSessionHarness {
	t.Helper()
	clientEnd, serverEnd := net.Pipe()
	uc, err := net.ListenUDP(udpTransport, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	h := &dtlsSessionHarness{peer: serverEnd, records: make(chan []byte, 64), local: uc.LocalAddr().String(), stopped: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		h.err = runDTLSUDPSession(ctx, clientEnd, uc, opts)
		close(h.stopped)
	}()
	go func() {
		defer close(h.records)
		buf := make([]byte, udpMaxPacketSize)
		for {
			n, readErr := serverEnd.Read(buf)
			if readErr != nil {
				return
			}
			h.records <- bytes.Clone(buf[:n])
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = clientEnd.Close()
		_ = serverEnd.Close()
		_ = uc.Close()
		select {
		case <-h.stopped:
		case <-time.After(2 * time.Second):
			t.Error("DTLS session did not stop")
		}
	})
	return h
}

func (h *dtlsSessionHarness) write(t *testing.T, record []byte) {
	t.Helper()
	require.NoError(t, h.peer.SetWriteDeadline(time.Now().Add(2*time.Second)))
	_, err := h.peer.Write(record)
	require.NoError(t, err, "client session did not read the server record")
}

// sendUntilRecord resends payload until the fake server reads it with kind.
func (h *dtlsSessionHarness) sendUntilRecord(t *testing.T, sender *net.UDPConn, payload, kind string) testFlowFrame {
	t.Helper()
	target := mustResolveUDPAddr(t, h.local)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, err := sender.WriteToUDP([]byte(payload), target)
		require.NoError(t, err)
		timeout := time.After(200 * time.Millisecond)
	wait:
		for {
			select {
			case record, ok := <-h.records:
				require.True(t, ok, "session pipe closed")
				frame, got := classifyTestRecord(record)
				if got == kind && string(frame.payload) == payload {
					return frame
				}
			case <-timeout:
				break wait
			}
		}
	}
	t.Fatalf("server never received %q as a %s record", payload, kind)
	return testFlowFrame{}
}

func shortDTLSOptions() dtlsUDPSessionOptions {
	return dtlsUDPSessionOptions{ackWait: 50 * time.Millisecond, heartbeatUnit: time.Second}
}

func TestDTLSUDPSession_FlowAckRoutesRepliesToEachSource(t *testing.T) {
	h := startDTLSSessionHarness(t, dtlsUDPSessionOptions{ackWait: 2 * time.Second, heartbeatUnit: time.Second})
	h.write(t, testFlowAck(30))
	a, b := localUDPSender(t), localUDPSender(t)
	fromA := h.sendUntilRecord(t, a, "a-hello", testRecordFlow)
	fromB := h.sendUntilRecord(t, b, "b-hello", testRecordFlow)
	require.NotZero(t, fromA.flowID)
	require.NotEqual(t, fromA.flowID, fromB.flowID)

	h.write(t, testEncodeFlowFrame(fromA.flowID, []byte("re:a-hello")))
	h.write(t, testEncodeFlowFrame(fromB.flowID, []byte("re:b-hello")))
	h.write(t, testEncodeFlowFrame(fromB.flowID+1000, []byte("unknown flow")))
	expectUDPReply(t, a, "re:a-hello")
	expectUDPReply(t, b, "re:b-hello")
	expectNoUDPReply(t, a)
	expectNoUDPReply(t, b)
}

func TestDTLSUDPSession_FirstDatagramWaitsForAck(t *testing.T) {
	h := startDTLSSessionHarness(t, dtlsUDPSessionOptions{ackWait: 2 * time.Second, heartbeatUnit: time.Second})
	a := localUDPSender(t)
	_, err := a.WriteToUDP([]byte("first"), mustResolveUDPAddr(t, h.local))
	require.NoError(t, err)
	select {
	case record := <-h.records:
		t.Fatalf("datagram %x forwarded before the ack wait ended", record)
	case <-time.After(150 * time.Millisecond):
	}
	h.write(t, testFlowAck(30))
	select {
	case record := <-h.records:
		frame, kind := classifyTestRecord(record)
		require.Equal(t, testRecordFlow, kind)
		require.Equal(t, "first", string(frame.payload))
	case <-time.After(2 * time.Second):
		t.Fatal("first datagram never forwarded after the ack")
	}
}

func TestDTLSUDPSession_WithoutAckFallsBackToLegacyRecords(t *testing.T) {
	h := startDTLSSessionHarness(t, shortDTLSOptions())
	a := localUDPSender(t)
	h.sendUntilRecord(t, a, "old-server", testRecordLegacy)
	h.write(t, testEncodeLegacyFrame([]byte("re:old-server")))
	expectUDPReply(t, a, "re:old-server")
}

func TestDTLSUDPSession_LateAckSwitchesToFlowFraming(t *testing.T) {
	h := startDTLSSessionHarness(t, shortDTLSOptions())
	a := localUDPSender(t)
	h.sendUntilRecord(t, a, "before-ack", testRecordLegacy)

	h.write(t, testFlowAck(30))
	frame := h.sendUntilRecord(t, a, "after-ack", testRecordFlow)
	require.NotZero(t, frame.flowID)
	h.write(t, testEncodeFlowFrame(frame.flowID, []byte("re:after-ack")))
	expectUDPReply(t, a, "re:after-ack")
}

// answerPings counts pings and, when pong is set, answers each one.
func (h *dtlsSessionHarness) answerPings(pong bool, pings *atomic.Int32) {
	go func() {
		for record := range h.records {
			if _, kind := classifyTestRecord(record); kind != testRecordPing {
				continue
			}
			pings.Add(1)
			if pong {
				_ = h.peer.SetWriteDeadline(time.Now().Add(time.Second))
				_, _ = h.peer.Write([]byte{0xFF, 0xFF, 0x02})
			}
		}
	}()
}

func TestDTLSUDPSession_HeartbeatPingPongKeepsSessionUp(t *testing.T) {
	h := startDTLSSessionHarness(t, dtlsUDPSessionOptions{ackWait: time.Second, heartbeatUnit: 20 * time.Millisecond})
	h.write(t, testFlowAck(1))
	var pings atomic.Int32
	h.answerPings(true, &pings)
	select {
	case <-h.stopped:
		t.Fatalf("a session whose server answers pings ended: %v", h.err)
	case <-time.After(400 * time.Millisecond):
	}
	require.GreaterOrEqual(t, pings.Load(), int32(3), "an idle flow session pings every interval")
}

func TestDTLSUDPSession_HeartbeatEndsSilentSession(t *testing.T) {
	h := startDTLSSessionHarness(t, dtlsUDPSessionOptions{ackWait: time.Second, heartbeatUnit: 20 * time.Millisecond})
	h.write(t, testFlowAck(1))
	var pings atomic.Int32
	h.answerPings(false, &pings)
	select {
	case <-h.stopped:
		require.ErrorIs(t, h.err, errDTLSServerUnresponsive)
	case <-time.After(2 * time.Second):
		t.Fatal("a server that stopped answering was never detected")
	}
	require.GreaterOrEqual(t, pings.Load(), int32(3), "the session pings every half interval before giving up")
}

// manualElapsed is an injected monotonic session clock.
type manualElapsed struct{ now atomic.Int64 }

func (c *manualElapsed) elapsed() time.Duration { return time.Duration(c.now.Load()) }
func (c *manualElapsed) set(d time.Duration)    { c.now.Store(int64(d)) }

func TestDTLSUDPSession_HeartbeatMeasuresSilenceOnSessionClock(t *testing.T) {
	const unit = 20 * time.Millisecond
	clock := &manualElapsed{}
	h := startDTLSSessionHarness(t, dtlsUDPSessionOptions{ackWait: time.Second, heartbeatUnit: unit, clock: clock.elapsed})
	h.write(t, testFlowAck(1)) // interval = 1 unit, received at session time 0
	var pings atomic.Int32
	h.answerPings(false, &pings)

	time.Sleep(10 * unit)
	require.Zero(t, pings.Load(), "wall time passing is not silence: only the session clock counts")

	// Pings go out every half interval once silent for one interval: 1, 1.5,
	// 2 and 2.5 intervals, then 3 silent intervals end the session.
	for i, at := range []time.Duration{unit, unit * 3 / 2, 2 * unit, unit * 5 / 2} {
		clock.set(at)
		want := int32(i + 1)
		require.Eventually(t, func() bool { return pings.Load() >= want }, time.Second, unit/4, "ping %d at %s of silence", want, at)
		time.Sleep(3 * unit)
		require.Equal(t, want, pings.Load(), "one ping per half interval of silence")
	}
	select {
	case <-h.stopped:
		t.Fatalf("session ended before 3 silent intervals: %v", h.err)
	default:
	}
	clock.set(3 * unit)
	select {
	case <-h.stopped:
		require.ErrorIs(t, h.err, errDTLSServerUnresponsive)
	case <-time.After(time.Second):
		t.Fatal("3 silent intervals did not end the session")
	}
}

func TestDTLSUDPSession_LegacyModeNeverPings(t *testing.T) {
	h := startDTLSSessionHarness(t, dtlsUDPSessionOptions{ackWait: 20 * time.Millisecond, heartbeatUnit: 10 * time.Millisecond})
	a := localUDPSender(t)
	h.sendUntilRecord(t, a, "legacy", testRecordLegacy)
	var pings atomic.Int32
	h.answerPings(false, &pings)
	select {
	case <-h.stopped:
		t.Fatalf("a legacy session must not time out on heartbeats: %v", h.err)
	case <-time.After(200 * time.Millisecond):
	}
	require.Zero(t, pings.Load())
}
