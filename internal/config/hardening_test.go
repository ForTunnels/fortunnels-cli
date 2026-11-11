// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetDefaultServerURLTrimsTrailingSlashes(t *testing.T) {
	original := defaultServerURL
	t.Cleanup(func() { defaultServerURL = original })

	SetDefaultServerURL("  https://example.com//  ")

	assert.Equal(t, "https://example.com", defaultServerURL)
}

func TestValidateEncryptDataPlane(t *testing.T) {
	const psk = "12345678901234567890123456789012"
	reverse := func(plane string, encrypt bool) *Config {
		return &Config{
			Protocol: protoUDP, UDPListen: "127.0.0.1:5353", UDPDst: "127.0.0.1:53",
			DataPlane: plane, Encrypt: encrypt, PSK: psk,
		}
	}
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{"quic with encrypt", reverse("quic", true), true},
		{"dtls with encrypt", reverse("DTLS", true), true},
		{"ws with encrypt", reverse("ws", true), false},
		{"default plane with encrypt", reverse("", true), false},
		{"quic without encrypt", reverse("quic", false), false},
		{"http with encrypt", &Config{Protocol: protoHTTP, Encrypt: true, PSK: psk}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateEncryptDataPlane(tt.cfg)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), "--encrypt")
		})
	}
}
