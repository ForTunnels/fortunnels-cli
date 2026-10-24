// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"encoding/binary"
	"testing"
)

// lengthPrefixedUDPFrame builds a wire frame: a big-endian uint16 payload length followed by n zero bytes.
func lengthPrefixedUDPFrame(n int) []byte {
	frame := make([]byte, 2+n)
	binary.BigEndian.PutUint16(frame, uint16(n))
	return frame
}

func TestReadUDPPacket_EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		frame   []byte
		wantErr bool
	}{
		{
			name:    "single byte packet",
			frame:   []byte{0, 1, 0x42},
			wantErr: false,
		},
		{
			// The largest UDP datagram payload is the contract limit (the server clamps to the same value).
			name:    "max size packet",
			frame:   lengthPrefixedUDPFrame(udpDatagramMaxSize),
			wantErr: false,
		},
		{
			name:    "one byte over max size packet",
			frame:   lengthPrefixedUDPFrame(udpDatagramMaxSize + 1),
			wantErr: true,
		},
		{
			name:    "incomplete payload",
			frame:   []byte{0, 5, 1, 2}, // Says 5 bytes but only 2 provided
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := &mockReader{data: tt.frame}
			_, err := readUDPPacket(reader)
			if (err != nil) != tt.wantErr {
				t.Errorf("readUDPPacket() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// mockWriter and mockReader are defined in udp_test.go
// We need to reference them here
