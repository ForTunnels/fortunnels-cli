// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestSaveLoadValidateFileConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fortunnels.yml")
	token := "ft_testtoken1234567890"
	require.NoError(t, SaveAuthtoken(path, token))

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	fc, err := LoadFileConfig(path)
	require.NoError(t, err)
	require.NoError(t, ValidateFileConfig(fc))
	require.Equal(t, token, fc.Agent.Authtoken)
	require.Equal(t, fileConfigVersion, fc.Version)
}

func TestValidateFileConfigErrors(t *testing.T) {
	require.Error(t, ValidateFileConfig(&FileConfig{Version: 2}))
	require.Error(t, ValidateFileConfig(&FileConfig{Version: 3}))
}

func TestDefaultConfigPathOverride(t *testing.T) {
	t.Setenv(envConfigPath, "/tmp/custom-fortunnels.yml")
	path, err := DefaultConfigPath()
	require.NoError(t, err)
	require.Equal(t, "/tmp/custom-fortunnels.yml", path)
}

func TestDefaultConfigPathWindowsUsesLocalAppData(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("windows only")
	}
	t.Setenv(envConfigPath, "")
	t.Setenv("LOCALAPPDATA", `C:\Users\test\AppData\Local`)
	path, err := DefaultConfigPath()
	require.NoError(t, err)
	require.Contains(t, path, `AppData\Local\fortunnels\fortunnels.yml`)
}

func TestAuthtokenFromFileMissing(t *testing.T) {
	t.Setenv(envConfigPath, filepath.Join(t.TempDir(), "missing.yml"))
	require.Empty(t, AuthtokenFromFile())
}

func TestSaveAuthtoken_StaleWorldReadableTempDoesNotExposeToken(t *testing.T) {
	if runtime.GOOS == goosWindows {
		t.Skip("POSIX permission bits")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "fortunnels.yml")
	stale := path + ".tmp"
	require.NoError(t, os.WriteFile(stale, []byte("stale"), 0o644))
	require.NoError(t, os.Chmod(stale, 0o644))

	require.NoError(t, SaveAuthtoken(path, "ft_secret_token"))

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "the saved authtoken must be owner-only")
	_, err = os.Stat(stale)
	require.ErrorIs(t, err, os.ErrNotExist, "no temp file may be left behind")
	fc, err := LoadFileConfig(path)
	require.NoError(t, err)
	require.Equal(t, "ft_secret_token", fc.Agent.Authtoken)
}

// useTempUserConfigDir points os.UserConfigDir at a temp directory and returns
// the client's default config directory inside it.
func useTempUserConfigDir(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv(envConfigPath, "")
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	base, err := userConfigBaseDir()
	require.NoError(t, err)
	return filepath.Join(base, "fortunnels")
}

func TestSaveAuthtoken_LeavesCustomConfigDirMode(t *testing.T) {
	if runtime.GOOS == goosWindows {
		t.Skip("POSIX permission bits")
	}
	useTempUserConfigDir(t)
	shared := t.TempDir()
	require.NoError(t, os.Chmod(shared, 0o755))
	path := filepath.Join(shared, "fortunnels.yml")
	t.Setenv(envConfigPath, path)

	require.NoError(t, SaveAuthtoken(path, "ft_secret_token"))

	dirInfo, err := os.Stat(shared)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), dirInfo.Mode().Perm(), "a FORTUNNELS_CONFIG directory belongs to the user")
	fileInfo, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fileInfo.Mode().Perm())
}

func TestSaveAuthtokenNormalizesVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fortunnels.yml")
	initial := FileConfig{Version: 2}
	initial.Agent.Authtoken = "old"
	data, err := yaml.Marshal(initial)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	require.NoError(t, SaveAuthtoken(path, "ft_newtoken"))
	fc, err := LoadFileConfig(path)
	require.NoError(t, err)
	require.Equal(t, "ft_newtoken", fc.Agent.Authtoken)
	require.Equal(t, fileConfigVersion, fc.Version)
	require.NoError(t, ValidateFileConfig(fc))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}
