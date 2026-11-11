// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// ComputeDataPlaneAuthWithPSK computes the data-plane authentication token if requested.
// It returns the computed token, or an empty string if no auth is configured. It falls
// back to psk when encrypt is true and no explicit dp-auth secret/token was provided.
// This reuses stream encryption material for local dev only; production should set
// --dp-auth-secret separately from --psk.
func ComputeDataPlaneAuthWithPSK(tunnelID, dpAuthTokenFlag, dpAuthSecretFlag, psk string, encrypt bool) string {
	if strings.TrimSpace(dpAuthTokenFlag) != "" {
		return dpAuthTokenFlag
	}
	sec := strings.TrimSpace(dpAuthSecretFlag)
	if sec == "" && encrypt {
		sec = strings.TrimSpace(psk)
	}
	if sec == "" {
		return ""
	}
	return computeHMAC(sec, tunnelID)
}

// DataPlaneAuthInputs are the credential sources for one tunnel's data-plane
// connection.
type DataPlaneAuthInputs struct {
	TunnelID   string
	Issued     string // dp_auth from the tunnel-create response
	TokenFlag  string // --dp-auth-token
	SecretFlag string // --dp-auth-secret
	PSK        string
	Encrypt    bool
}

// ResolveDataPlaneAuth picks the data-plane credential: an explicit token or
// secret flag wins, then the credential the server issued for this tunnel, then
// the PSK fallback of ComputeDataPlaneAuthWithPSK.
func ResolveDataPlaneAuth(in *DataPlaneAuthInputs) string {
	if explicit := ComputeDataPlaneAuthWithPSK(in.TunnelID, in.TokenFlag, in.SecretFlag, "", false); explicit != "" {
		return explicit
	}
	if issued := strings.TrimSpace(in.Issued); issued != "" {
		return issued
	}
	return ComputeDataPlaneAuthWithPSK(in.TunnelID, "", "", in.PSK, in.Encrypt)
}

// computeHMAC returns hex-encoded HMAC-SHA256(secret, message)
func computeHMAC(secret, message string) string {
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write([]byte(message))
	return hex.EncodeToString(h.Sum(nil))
}
