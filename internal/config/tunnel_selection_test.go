// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package config

import (
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func parseRecoveryTestFlags(t *testing.T, args ...string) (*Config, error) {
	t.Helper()
	oldArgs, oldFlags := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = oldArgs, oldFlags })
	os.Args = append([]string{"fortunnels"}, args...)
	flag.CommandLine = flag.NewFlagSet("fortunnels", flag.ContinueOnError)
	return Parse()
}

func TestRecovery_SelectsProvisionOrAttach(t *testing.T) {
	t.Run("keep with positional protocol and port", func(t *testing.T) {
		cfg, err := parseRecoveryTestFlags(t, "--token=fixture", "--keep-tunnel", "http", "8080")
		require.NoError(t, err)
		require.NoError(t, Validate(cfg))
		require.True(t, cfg.PreservesTunnel())
		require.Equal(t, "localhost:8080", cfg.TargetAddr)
	})
	t.Run("saved configuration", func(t *testing.T) {
		cfg, err := parseRecoveryTestFlags(t, "--token=fixture", "--tunnel-id", "saved-id")
		require.NoError(t, err)
		require.NoError(t, Validate(cfg))
		require.Empty(t, cfg.Protocol)
		require.Empty(t, cfg.TargetAddr)
		require.NoError(t, ResolveAttachmentTarget(cfg, "tcp", "example.test:3000"))
		require.Equal(t, "example.test:3000", cfg.TargetAddr)
	})
	t.Run("saved UDP port", func(t *testing.T) {
		cfg, err := parseRecoveryTestFlags(t, "--token=fixture", "--tunnel-id=saved", "9000")
		require.NoError(t, err)
		require.NoError(t, Validate(cfg))
		require.NoError(t, ResolveAttachmentTarget(cfg, "udp", "127.0.0.1:9000"))
		require.Equal(t, "udp", cfg.Protocol)
	})
}

func TestRecovery_ExplicitValuesAndIncompatibleModes(t *testing.T) {
	t.Run("explicit selection", func(t *testing.T) {
		cfg, err := parseRecoveryTestFlags(t, "--token=fixture", "--tunnel-id=saved", "tcp", "LOCALHOST:3000")
		require.NoError(t, err)
		require.ErrorContains(t, ResolveAttachmentTarget(cfg, "udp", "localhost:3000"), "protocol differs")
		require.ErrorContains(t, ResolveAttachmentTarget(cfg, "tcp", "localhost:3001"), "target differs")
		require.NoError(t, ResolveAttachmentTarget(cfg, " TCP ", "localhost:3000"))
	})
	for _, tc := range []struct {
		args    []string
		message string
	}{
		{[]string{"--keep-tunnel", "--tunnel-id=saved"}, "cannot be combined"},
		{[]string{"--tunnel-id=saved", "--udp-listen=:3000"}, "reverse UDP"},
		{[]string{"--tunnel-id=saved", "--dp-auth-token=explicit"}, "server-issued"},
		{[]string{"--tunnel-id=saved", "--dp=quic"}, "requires WS"},
		{[]string{"--tunnel-id=../saved"}, "invalid --tunnel-id"},
	} {
		t.Run(tc.message, func(t *testing.T) {
			cfg, err := parseRecoveryTestFlags(t, append(tc.args, "--token=fixture")...)
			require.NoError(t, err)
			require.ErrorContains(t, Validate(cfg), tc.message)
		})
	}
	t.Run("anonymous", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.KeepTunnel = true
		require.ErrorContains(t, Validate(cfg), "owner authentication")
	})
}

func TestRecovery_InvalidFlagAndSavedTarget(t *testing.T) {
	_, err := parseRecoveryTestFlags(t, "--tunnel-id=")
	require.ErrorContains(t, err, "requires a tunnel ID")
	cfg, err := parseRecoveryTestFlags(t, "--token=fixture", "--tunnel-id=saved", "--local=bad")
	require.NoError(t, err)
	require.ErrorContains(t, Validate(cfg), "invalid target address")
	require.ErrorContains(t, ResolveAttachmentTarget(cfg, "tcp", "bad"), "invalid target address")
	require.Error(t, ResolveAttachmentTarget(cfg, "smtp", "localhost:25"))
}
