// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Byte-level flow v1 vectors. The server package pins the same bytes
// (apps/server/internal/tunnel/udp_flow_frame_test.go); change both or neither.
var (
	udpFlowVectorAck  = []byte{0x00, 0x00, 0x01, 0x00, 0x0A}                   // ack v1, heartbeat 10s
	udpFlowVectorData = []byte{0x00, 0x02, 0x00, 0x00, 0x00, 0x07, 0x68, 0x69} // flow 7, "hi"
	udpFlowVectorPing = []byte{0xFF, 0xFF, 0x01}
	udpFlowVectorPong = []byte{0xFF, 0xFF, 0x02}
)

func TestUDPFlowWireVectors(t *testing.T) {
	frame, err := encodeUDPFlowFrame(7, []byte("hi"))
	require.NoError(t, err)
	require.Equal(t, udpFlowVectorData, frame)

	flowID, payload, err := parseUDPFlowRecord(udpFlowVectorData, maxDTLSFlowPayload)
	require.NoError(t, err)
	require.Equal(t, uint32(7), flowID)
	require.Equal(t, []byte("hi"), payload)

	stream := bytes.NewReader(append(bytes.Clone(udpFlowVectorData), udpFlowVectorData...))
	for range 2 {
		flowID, payload, err = readUDPFlowFrame(stream, udpDatagramMaxSize)
		require.NoError(t, err)
		require.Equal(t, uint32(7), flowID)
		require.Equal(t, []byte("hi"), payload)
	}

	require.Equal(t, udpFlowVectorPing, dtlsControlRecord(dtlsControlPing))
	op, ok := dtlsControlOp(udpFlowVectorPong)
	require.True(t, ok)
	require.Equal(t, dtlsControlPong, op)

	interval, ok := parseDTLSFlowAck(udpFlowVectorAck, time.Second)
	require.True(t, ok)
	require.Equal(t, 10*time.Second, interval)
}

func TestUDPFlowFrame_RejectsInvalidFrames(t *testing.T) {
	_, err := encodeUDPFlowFrame(0, []byte("x"))
	require.ErrorIs(t, err, errInvalidUDPFlowFrame, "flow ID 0 is reserved")
	_, err = encodeUDPFlowFrame(1, nil)
	require.ErrorIs(t, err, errInvalidUDPFlowFrame, "empty payload")

	for name, record := range map[string][]byte{
		"ack":           udpFlowVectorAck,
		"ping":          udpFlowVectorPing,
		"legacy record": {0x00, 0x02, 'h', 'i'},
		"flow id zero":  {0x00, 0x01, 0, 0, 0, 0, 'x'},
		"length short":  {0x00, 0x03, 0, 0, 0, 7, 'h', 'i'},
		"oversize":      {0x00, 0x03, 0, 0, 0, 7, 'a', 'b', 'c'},
	} {
		limit := maxDTLSFlowPayload
		if name == "oversize" {
			limit = 2
		}
		_, _, err = parseUDPFlowRecord(record, limit)
		require.ErrorIs(t, err, errInvalidUDPFlowFrame, name)
	}

	_, _, err = readUDPFlowFrame(bytes.NewReader([]byte{0x00, 0x00, 0, 0, 0, 1}), udpDatagramMaxSize)
	require.ErrorIs(t, err, errInvalidUDPFlowFrame, "zero payload length on a stream")

	_, ok := dtlsControlOp(udpFlowVectorData)
	require.False(t, ok)
	for _, record := range [][]byte{udpFlowVectorData, udpFlowVectorPing, {0x00, 0x00, 0x02, 0x00, 0x0A}, {0x00, 0x00, 0x01, 0x00}} {
		_, ok = parseDTLSFlowAck(record, time.Second)
		require.False(t, ok, "%x is not a v1 ack", record)
	}
}

func TestDTLSFlowPayloadLimitLeavesRoomForFlowID(t *testing.T) {
	frame, err := encodeUDPFlowFrame(1, bytes.Repeat([]byte{'x'}, maxDTLSFlowPayload))
	require.NoError(t, err)
	require.Len(t, frame, maxDTLSRecordPayload+2, "a full flow record matches the legacy record size limit")
}

func TestDTLSBootstrapRecordRequestsFlowFraming(t *testing.T) {
	record, err := dtlsBootstrapRecord("auth", "t1", "127.0.0.1:53")
	require.NoError(t, err)
	require.JSONEq(t, `{"auth":"auth","tunnel_id":"t1","dst":"127.0.0.1:53","framing":"record","flow":"v1"}`,
		string(bytes.TrimSuffix(record, []byte("\n"))))
}
