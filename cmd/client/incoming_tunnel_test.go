// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fortunnels/client/internal/config"
	ctrl "github.com/fortunnels/client/internal/control"
	dp "github.com/fortunnels/client/internal/dataplane"
)

func TestIncomingTunnelCarriesTargetAndEncryption(t *testing.T) {
	cfg := &config.Config{
		ServerURL: "https://example.test", TunnelSelection: config.TunnelSelection{TargetAddr: "localhost:3000"},
		Encrypt: true, PSK: "k",
	}

	got := incomingTunnel(cfg, &ctrl.TunnelResponse{ID: "t1"}, "")

	require.Equal(t, "localhost:3000", got.Target)
	require.Equal(t, config.EncryptionSettings{Enabled: true, PSK: "k"}, got.Encryption)
}

func TestIncomingTunnelCarriesConfiguredBackoff(t *testing.T) {
	cfg := &config.Config{ServerURL: "https://example.test", BackoffInitial: 3 * time.Second, BackoffMax: 45 * time.Second}

	got := incomingTunnel(cfg, &ctrl.TunnelResponse{ID: "t1"}, "dp-token")

	require.Equal(t, &dp.IncomingTunnel{
		ServerURL: "https://example.test",
		TunnelID:  "t1",
		AuthToken: "dp-token",
		Backoff:   config.ReconnectBackoff{Initial: 3 * time.Second, Max: 45 * time.Second},
	}, got)
}
