// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/fortunnels/client/internal/support"
)

func clearSensitiveArgs(flags secretFlagSet) {
	type entry struct {
		provided bool
		names    []string
	}
	entries := []entry{
		{flags.token, []string{"-token", "--token"}},
		{flags.password, []string{"-pass", "--pass"}},
		{flags.psk, []string{"-psk", "--psk"}},
		{flags.dpAuthToken, []string{"-dp-auth-token", "--dp-auth-token"}},
		{flags.dpAuthSecret, []string{"-dp-auth-secret", "--dp-auth-secret"}},
	}
	for _, entry := range entries {
		if entry.provided {
			redactArgs(entry.names)
		}
	}
}

// redactArgs blanks the value of every os.Args occurrence of the named flags.
func redactArgs(names []string) {
	for i := 1; i < len(os.Args); i++ {
		for _, name := range names {
			redactArgAt(i, name)
		}
	}
}

func redactArgAt(i int, name string) {
	arg := os.Args[i]
	switch {
	case arg == name:
		if i+1 < len(os.Args) && !strings.HasPrefix(os.Args[i+1], "-") {
			os.Args[i+1] = ""
		}
	case strings.HasPrefix(arg, name+"="):
		os.Args[i] = name + "="
	}
}

type secretFlagSet struct {
	login        bool
	token        bool
	password     bool
	psk          bool
	dpAuthToken  bool
	dpAuthSecret bool
}

func detectFlagOverrides() (localProvided, protocolProvided bool, secrets secretFlagSet) {
	flagProvided := func(name string) bool {
		for _, a := range os.Args[1:] {
			if a == "-"+name ||
				a == "--"+name ||
				strings.HasPrefix(a, "-"+name+"=") ||
				strings.HasPrefix(a, "--"+name+"=") {
				return true
			}
		}
		return false
	}
	secrets = secretFlagSet{
		login:        flagProvided("login"),
		token:        flagProvided("token"),
		password:     flagProvided("pass"),
		psk:          flagProvided("psk"),
		dpAuthToken:  flagProvided("dp-auth-token"),
		dpAuthSecret: flagProvided("dp-auth-secret"),
	}
	return flagProvided("local"), flagProvided("protocol"), secrets
}

type secretSource struct {
	label     string
	value     *string
	file      *string
	fromStdin *bool
	envVar    string
}

func applySecretSources(cfg *Config) error {
	sources := []secretSource{
		{
			label:     "token",
			value:     &cfg.Token,
			file:      &cfg.TokenFile,
			fromStdin: &cfg.TokenFromStdin,
			envVar:    "FORTUNNELS_TOKEN",
		},
		{
			label:     "pass",
			value:     &cfg.Password,
			file:      &cfg.PasswordFile,
			fromStdin: &cfg.PasswordFromStdin,
			envVar:    "FORTUNNELS_PASSWORD",
		},
		{
			label:     "psk",
			value:     &cfg.PSK,
			file:      &cfg.PSKFile,
			fromStdin: &cfg.PSKFromStdin,
			envVar:    "FORTUNNELS_PSK",
		},
		{
			label:     "dp-auth-token",
			value:     &cfg.DPAuthToken,
			file:      &cfg.DPAuthTokenFile,
			fromStdin: &cfg.DPAuthTokenFromStdin,
			envVar:    "FORTUNNELS_DP_AUTH_TOKEN",
		},
		{
			label:     "dp-auth-secret",
			value:     &cfg.DPAuthSecret,
			file:      &cfg.DPAuthSecretFile,
			fromStdin: &cfg.DPAuthSecretFromStdin,
			envVar:    "FORTUNNELS_DP_AUTH_SECRET",
		},
	}

	if err := ensureSingleStdinSource(sources); err != nil {
		return err
	}
	for i := range sources {
		if err := applySecretSource(&sources[i]); err != nil {
			return err
		}
	}
	return nil
}

var errMultipleStdinSources = errors.New("only one --*-stdin option can be used at a time")

func ensureSingleStdinSource(sources []secretSource) error {
	var stdinFlags []string
	for _, src := range sources {
		if src.fromStdin != nil && *src.fromStdin {
			stdinFlags = append(stdinFlags, src.label)
		}
	}
	if len(stdinFlags) > 1 {
		return fmt.Errorf("%w: %s", errMultipleStdinSources, strings.Join(stdinFlags, ", "))
	}
	return nil
}

func applySecretSource(source *secretSource) error {
	if source == nil || source.value == nil {
		return nil
	}
	if *source.value != "" {
		return nil
	}
	if source.file != nil && *source.file != "" {
		secret, err := support.ReadSecretFile(*source.file)
		if err != nil {
			return err
		}
		*source.value = secret
		return nil
	}
	if source.fromStdin != nil && *source.fromStdin {
		secret, err := support.ReadSecretStdin(source.label)
		if err != nil {
			return err
		}
		*source.value = secret
		return nil
	}
	*source.value = support.GetEnvTrimmed(source.envVar)
	return nil
}
