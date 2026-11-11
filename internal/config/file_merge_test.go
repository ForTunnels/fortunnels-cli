// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestSaveAuthtokenKeepsUnrelatedSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fortunnels.yml")
	existing := "version: 3\nregion: eu\nagent:\n  authtoken: old-token\n  label: laptop\n"
	require.NoError(t, os.WriteFile(path, []byte(existing), 0o600))

	require.NoError(t, SaveAuthtoken(path, "ft_new_token"))

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, yaml.Unmarshal(raw, &got))
	assert.Equal(t, "eu", got["region"])
	assert.Equal(t, 3, got["version"])
	agent, ok := got["agent"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ft_new_token", agent["authtoken"])
	assert.Equal(t, "laptop", agent["label"])
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestSaveAuthtokenReplacesUnparsableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fortunnels.yml")
	require.NoError(t, os.WriteFile(path, []byte("{{ not yaml"), 0o600))

	require.NoError(t, SaveAuthtoken(path, "ft_tok"))

	fc, err := LoadFileConfig(path)
	require.NoError(t, err)
	require.NoError(t, ValidateFileConfig(fc))
	assert.Equal(t, "ft_tok", fc.Agent.Authtoken)
}
