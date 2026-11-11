// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"errors"
	"io"
	"log"
	"net"
	"time"
)

const (
	// udpFlowIdleTTL outlives the server's per-flow UDP idle timeout, which
	// admins can raise well past its 30s default, so a local source keeps its
	// flow ID while the server can reply. udpFlowLimit bounds memory.
	udpFlowIdleTTL = time.Hour
	// udpFlowLimit bounds the local source table of one reverse UDP session.
	udpFlowLimit = 4096
)

func newUDPFlowRegistry() *flowRegistry {
	return newBoundedFlowRegistry(udpFlowIdleTTL, udpFlowLimit, time.Now)
}

// startUDPFlowStreamRelays bridges the local UDP socket and a flow-framed
// stream: each local source sends under its own flow ID, and a reply reaches
// only the source its flow ID maps to. Each direction reports its terminal
// error on errCh.
func startUDPFlowStreamRelays(stream io.ReadWriter, uc *net.UDPConn, errCh chan<- error, flows *flowRegistry) {
	replies := flowReplies{local: uc, flows: flows}
	go func() {
		buf := make([]byte, udpMaxPacketSize)
		for {
			n, src, err := uc.ReadFromUDP(buf)
			if err != nil {
				errCh <- err
				return
			}
			if n <= 0 {
				continue
			}
			frame, err := encodeUDPFlowFrame(flows.udpFlowID(src), buf[:n])
			if err == nil {
				_, err = stream.Write(frame)
			}
			if err != nil {
				errCh <- err
				return
			}
		}
	}()
	go func() {
		var drops uint64
		for {
			flowID, payload, err := readUDPFlowFrame(stream, udpDatagramMaxSize)
			if err == nil {
				err = replies.deliver(flowID, payload, &drops)
			}
			if err != nil {
				errCh <- err
				return
			}
		}
	}()
}

// flowReplies routes server replies to the local source their flow ID maps to.
type flowReplies struct {
	local udpReplyWriter
	flows *flowRegistry
}

// deliver writes a reply to the local source of flowID. A reply for an unknown
// or expired flow is dropped: it has no source to go to.
func (r flowReplies) deliver(flowID uint32, payload []byte, drops *uint64) error {
	peer, ok := r.flows.udpFlowPeer(flowID)
	if !ok {
		*drops++
		if *drops == 1 || *drops%100 == 0 {
			log.Printf("udp: dropped reply for unknown flow %d (drops=%d)", flowID, *drops)
		}
		return nil
	}
	return writeLocalReply(r.local, payload, peer, drops)
}

// writeLocalReply sends a reply to a local UDP source. UDP is lossy by design:
// a write that fails for one source (full send buffer, vanished host) drops
// that datagram instead of ending every other flow of the session. Only a
// closed socket is fatal.
func writeLocalReply(w udpReplyWriter, payload []byte, peer *net.UDPAddr, drops *uint64) error {
	_, err := w.WriteToUDP(payload, peer)
	if err == nil {
		return nil
	}
	if errors.Is(err, net.ErrClosed) {
		return err
	}
	*drops++
	if *drops == 1 || *drops%100 == 0 {
		log.Printf("udp: dropped reply to %s: %v (drops=%d)", peer, err, *drops)
	}
	return nil
}
