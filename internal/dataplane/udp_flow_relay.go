// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
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
				err = deliverUDPFlowDatagram(uc, flows, flowID, payload, &drops)
			}
			if err != nil {
				errCh <- err
				return
			}
		}
	}()
}

// deliverUDPFlowDatagram writes a reply to the local source of flowID. A reply
// for an unknown or expired flow is dropped: it has no source to go to.
func deliverUDPFlowDatagram(uc *net.UDPConn, flows *flowRegistry, flowID uint32, payload []byte, drops *uint64) error {
	peer, ok := flows.udpFlowPeer(flowID)
	if !ok {
		*drops++
		if *drops == 1 || *drops%100 == 0 {
			log.Printf("udp: dropped reply for unknown flow %d (drops=%d)", flowID, *drops)
		}
		return nil
	}
	_, err := uc.WriteToUDP(payload, peer)
	return err
}
