// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

// Flow v1 framing tags every reverse UDP datagram with the client-chosen flow
// ID of its local source, so the server keeps one backend socket per source and
// replies reach only that source. All integers are big-endian.
//
//	data frame:    [u16 payloadLen][u32 flowID][payload]   payloadLen >= 1, flowID >= 1
//	DTLS control:  [0xFF 0xFF][u8 op]                       op 1 = ping, op 2 = pong
//	DTLS flow ack: [0x00 0x00][u8 version=1][u16 heartbeatIntervalSeconds]
//
// On a WebSocket/smux stream data frames follow each other; on DTLS one record
// carries exactly one frame, so payloadLen == recordLen-6.
const (
	udpFlowHeaderLen       = 6
	dtlsControlRecordLen   = 3
	dtlsControlPing        = byte(1)
	dtlsControlPong        = byte(2)
	dtlsFlowAckRecordLen   = 5
	dtlsFlowAckVersion     = byte(1)
	dtlsFlowControlMarker  = 0xFF
	maxDTLSFlowPayload     = maxDTLSRecordPayload - 4
	udpFlowFramingName     = "flow"
	dtlsFlowBootstrapValue = "v1"
)

var errInvalidUDPFlowFrame = errors.New("invalid UDP flow frame")

// encodeUDPFlowFrame builds one data frame.
func encodeUDPFlowFrame(flowID uint32, payload []byte) ([]byte, error) {
	size := len(payload)
	// 0xFFFF is reserved for DTLS control records.
	if flowID == 0 || size == 0 || size >= math.MaxUint16 {
		return nil, fmt.Errorf("%w: flow %d, payload %d", errInvalidUDPFlowFrame, flowID, size)
	}
	frame := make([]byte, udpFlowHeaderLen+size)
	binary.BigEndian.PutUint16(frame[:2], uint16(size))
	binary.BigEndian.PutUint32(frame[2:udpFlowHeaderLen], flowID)
	copy(frame[udpFlowHeaderLen:], payload)
	return frame, nil
}

// readUDPFlowFrame reads one data frame from a stream. A malformed header
// desynchronizes the stream, so it is returned as an error.
func readUDPFlowFrame(r io.Reader, maxPayload int) (flowID uint32, payload []byte, err error) {
	var hdr [udpFlowHeaderLen]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	size, flowID, err := validUDPFlowHeader(hdr[:], maxPayload)
	if err != nil {
		return 0, nil, err
	}
	payload = make([]byte, size)
	if _, err = io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return flowID, payload, nil
}

// parseUDPFlowRecord decodes the single data frame a DTLS record carries. The
// payload aliases record.
func parseUDPFlowRecord(record []byte, maxPayload int) (flowID uint32, payload []byte, err error) {
	if len(record) < udpFlowHeaderLen {
		return 0, nil, fmt.Errorf("%w: record size %d", errInvalidUDPFlowFrame, len(record))
	}
	size, flowID, err := validUDPFlowHeader(record, maxPayload)
	if err != nil {
		return 0, nil, err
	}
	if size != len(record)-udpFlowHeaderLen {
		return 0, nil, fmt.Errorf("%w: length %d in record of %d", errInvalidUDPFlowFrame, size, len(record))
	}
	return flowID, record[udpFlowHeaderLen:], nil
}

func validUDPFlowHeader(hdr []byte, maxPayload int) (size int, flowID uint32, err error) {
	size = int(binary.BigEndian.Uint16(hdr[:2]))
	flowID = binary.BigEndian.Uint32(hdr[2:udpFlowHeaderLen])
	if size == 0 || size > maxPayload || size == math.MaxUint16 || flowID == 0 {
		return 0, 0, fmt.Errorf("%w: flow %d, payload %d (limit %d)", errInvalidUDPFlowFrame, flowID, size, maxPayload)
	}
	return size, flowID, nil
}

// dtlsControlRecord builds a DTLS control record.
func dtlsControlRecord(op byte) []byte {
	return []byte{dtlsFlowControlMarker, dtlsFlowControlMarker, op}
}

// dtlsControlOp reports the operation of a DTLS control record.
func dtlsControlOp(record []byte) (byte, bool) {
	if len(record) != dtlsControlRecordLen || record[0] != dtlsFlowControlMarker || record[1] != dtlsFlowControlMarker {
		return 0, false
	}
	return record[2], true
}

// parseDTLSFlowAck decodes the server's flow v1 acknowledgement and returns
// its heartbeat interval (announced in seconds, scaled by unit, at least one
// unit).
func parseDTLSFlowAck(record []byte, unit time.Duration) (time.Duration, bool) {
	if len(record) != dtlsFlowAckRecordLen || record[0] != 0 || record[1] != 0 || record[2] != dtlsFlowAckVersion {
		return 0, false
	}
	seconds := max(binary.BigEndian.Uint16(record[3:]), 1)
	return time.Duration(seconds) * unit, true
}
