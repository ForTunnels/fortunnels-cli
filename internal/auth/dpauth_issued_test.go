// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolveDataPlaneAuth(t *testing.T) {
	const tid = "t-1"
	const psk = "psk-fallback-psk-fallback-psk"
	tests := []struct {
		name string
		in   DataPlaneAuthInputs
		want string
	}{
		{"issued credential when nothing is configured", DataPlaneAuthInputs{TunnelID: tid, Issued: " issued "}, "issued"},
		{"token flag beats issued", DataPlaneAuthInputs{TunnelID: tid, Issued: "issued", TokenFlag: "flag-token"}, "flag-token"},
		{"secret flag beats issued", DataPlaneAuthInputs{TunnelID: tid, Issued: "issued", SecretFlag: "s3cret"}, computeHMAC("s3cret", tid)},
		{"issued beats the PSK fallback", DataPlaneAuthInputs{TunnelID: tid, Issued: "issued", PSK: psk, Encrypt: true}, "issued"},
		{"PSK fallback without issued", DataPlaneAuthInputs{TunnelID: tid, PSK: psk, Encrypt: true}, computeHMAC(psk, tid)},
		{"nothing configured", DataPlaneAuthInputs{TunnelID: tid}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ResolveDataPlaneAuth(&tt.in))
		})
	}
}
