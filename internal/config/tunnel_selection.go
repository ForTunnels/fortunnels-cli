// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package config

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"net/netip"
	"regexp"
	"strconv"
	"strings"

	"github.com/fortunnels/client/internal/support"
)

// TunnelSelection describes the local target and whether to keep or recover a tunnel.
type TunnelSelection struct {
	Protocol         string
	TargetAddr       string
	KeepTunnel       bool
	TunnelID         string
	protocolProvided bool
	targetProvided   bool
	positionalPort   string
}

var (
	errRecoveryIDRequired       = errors.New("--tunnel-id requires a tunnel ID")
	errRecoveryIDInvalid        = errors.New("invalid --tunnel-id")
	errRecoverySelection        = errors.New("--keep-tunnel and --tunnel-id cannot be combined")
	errRecoveryTargetSyntax     = errors.New("expected a tunnel protocol or target address")
	errRecoveryReverseUDP       = errors.New("tunnel recovery does not support reverse UDP mode")
	errRecoveryDataplaneAuth    = errors.New("tunnel recovery uses server-issued data-plane credentials")
	errRecoveryTransport        = errors.New("tunnel recovery requires WS data-plane")
	errRecoveryOwner            = errors.New("tunnel recovery requires owner authentication")
	errRecoveryProtocolMismatch = errors.New("the requested protocol differs from the saved tunnel")
	errRecoveryTargetMismatch   = errors.New("the requested target differs from the saved tunnel")
	errTooManyPositionals       = errors.New("too many positional arguments: at most 2 allowed (protocol and address)")
	errPositionalOrder          = errors.New("invalid argument order: protocol must come before address")
	recoveryIDPattern           = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
)

// PreservesTunnel reports whether the CLI should retain the server record on exit.
func (s *TunnelSelection) PreservesTunnel() bool {
	return s.KeepTunnel || s.TunnelID != ""
}

func registerTunnelFlags(fs *flag.FlagSet, s *TunnelSelection) {
	fs.StringVar(&s.TargetAddr, "local", s.TargetAddr, "Target address to tunnel")
	fs.StringVar(&s.Protocol, "protocol", s.Protocol, "Protocol (http, https, tcp, udp)")
	fs.BoolVar(&s.KeepTunnel, "keep-tunnel", false, "Keep the tunnel for restart (e.g. fortunnels --keep-tunnel http 8080)")
	fs.StringVar(&s.TunnelID, "tunnel-id", "", "Attach to an existing owned tunnel ID and keep it on exit")
}

func (c *Config) applyTunnelSelection(fs *flag.FlagSet, args []string, local, protocol bool) error {
	if c.TunnelID == "" && flagWasSet(fs, "tunnel-id") {
		return errRecoveryIDRequired
	}
	if c.TunnelID == "" {
		c.TunnelSelection.applyCreationPositionals(args, local, protocol)
		return nil
	}
	return c.TunnelSelection.selectAttachment(args, local, protocol)
}

func flagWasSet(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) { found = found || f.Name == name })
	return found
}

func (s *TunnelSelection) selectAttachment(args []string, local, protocol bool) error {
	s.protocolProvided, s.targetProvided = protocol, local
	if !protocol {
		s.Protocol = ""
	}
	if !local {
		s.TargetAddr = ""
	}
	if len(args) == 0 {
		return nil
	}
	first := strings.ToLower(strings.TrimSpace(args[0]))
	if isSupportedProtocol(first) {
		if !protocol {
			s.Protocol, s.protocolProvided = first, true
		}
		args = args[1:]
	}
	return s.selectAttachmentTarget(args, local)
}

func (s *TunnelSelection) selectAttachmentTarget(args []string, local bool) error {
	switch {
	case len(args) == 0:
		return nil
	case len(args) != 1 || (support.ParsePort(args[0]) == "" && !support.LooksLikeHostPort(args[0])):
		return errRecoveryTargetSyntax
	case local:
		return nil
	default:
		s.TargetAddr, s.targetProvided = args[0], true
		s.positionalPort = support.ParsePort(args[0])
		return nil
	}
}

func validateRecoverySelection(cfg *Config) error {
	if !cfg.PreservesTunnel() {
		return nil
	}
	switch {
	case cfg.KeepTunnel && cfg.TunnelID != "":
		return errRecoverySelection
	case cfg.TunnelID != "" && !recoveryIDPattern.MatchString(cfg.TunnelID):
		return errRecoveryIDInvalid
	case hasReverseUDPFlags(cfg):
		return errRecoveryReverseUDP
	case explicitDataplaneAuth(cfg):
		return errRecoveryDataplaneAuth
	case !allowsRecoveryTransport(cfg.DataPlane):
		return errRecoveryTransport
	case !hasRecoveryAccount(cfg):
		return errRecoveryOwner
	default:
		return nil
	}
}

func hasReverseUDPFlags(cfg *Config) bool {
	return strings.TrimSpace(cfg.UDPListen) != "" || strings.TrimSpace(cfg.UDPDst) != ""
}

func allowsRecoveryTransport(transport string) bool {
	transport = strings.ToLower(strings.TrimSpace(transport))
	return transport == "" || transport == "ws"
}

func explicitDataplaneAuth(cfg *Config) bool {
	return cfg.DPAuthToken != "" || cfg.DPAuthSecret != "" || cfg.DPAuthTokenFile != "" || cfg.DPAuthSecretFile != "" ||
		cfg.DPAuthTokenFromStdin || cfg.DPAuthSecretFromStdin || cfg.DPAuthTokenFlagProvided || cfg.DPAuthSecretFlagProvided
}

func hasRecoveryAccount(cfg *Config) bool {
	return strings.TrimSpace(cfg.Token) != "" || (strings.TrimSpace(cfg.Login) != "" && strings.TrimSpace(cfg.Password) != "")
}

// ResolveAttachmentTarget fills omitted values from the owned tunnel and checks explicit values.
func ResolveAttachmentTarget(cfg *Config, protocol, target string) error {
	protocol, target = strings.ToLower(strings.TrimSpace(protocol)), strings.TrimSpace(target)
	if err := validateProtocolFlag(protocol); err != nil {
		return err
	}
	if err := validateTargetAddress(target); err != nil {
		return err
	}
	selection := &cfg.TunnelSelection
	explicitTarget := selection.TargetAddr
	if selection.positionalPort != "" {
		explicitTarget = targetForPort(protocol, selection.positionalPort)
	}
	if selection.protocolProvided && selection.Protocol != protocol {
		return errRecoveryProtocolMismatch
	}
	if selection.targetProvided && normalizedTarget(explicitTarget) != normalizedTarget(target) {
		return errRecoveryTargetMismatch
	}
	selection.Protocol, selection.TargetAddr = protocol, target
	return ValidateUDPExposeLocalOptions(cfg)
}

func normalizedTarget(target string) string {
	host, port, err := net.SplitHostPort(strings.TrimSpace(target))
	if err != nil {
		return ""
	}
	if ip, ipErr := netip.ParseAddr(host); ipErr == nil {
		host = ip.String()
	} else {
		host = strings.ToLower(host)
	}
	number, err := strconv.Atoi(port)
	if err != nil {
		return ""
	}
	return net.JoinHostPort(host, strconv.Itoa(number))
}

func validatePositionalArgs(args []string) error {
	if len(args) > 2 {
		return fmt.Errorf(
			"%w; supported protocols: %s, %s, %s, %s; use -protocol / -local for explicit values",
			errTooManyPositionals, protoHTTP, protoHTTPS, protoTCP, protoUDP,
		)
	}
	if len(args) != 2 {
		return nil
	}
	a0 := strings.ToLower(args[0])
	a1 := strings.ToLower(args[1])
	if isSupportedProtocol(a1) && !isSupportedProtocol(a0) {
		return fmt.Errorf(
			"%w (e.g. %s %s); or use -protocol and -local", errPositionalOrder, args[1], args[0],
		)
	}
	return nil
}

func (s *TunnelSelection) applyCreationPositionals(args []string, local, protocol bool) {
	switch len(args) {
	case 0:
		return
	case 1:
		s.handleSingleArg(args[0], local, protocol)
	default:
		s.handleProtocolAndAddrArgs(args[0], args[1], local, protocol)
	}
}

func (s *TunnelSelection) handleSingleArg(arg string, local, protocol bool) {
	if p := support.ParsePort(arg); p != "" {
		setSelectionIfMissing(&s.Protocol, protocol, protoHTTP)
		setSelectionIfMissing(&s.TargetAddr, local, targetForPort(s.Protocol, p))
		return
	}
	if support.LooksLikeHostPort(arg) {
		setSelectionIfMissing(&s.Protocol, protocol, protoHTTP)
		setSelectionIfMissing(&s.TargetAddr, local, arg)
	}
}

func (s *TunnelSelection) handleProtocolAndAddrArgs(protoArg, addrArg string, local, protocol bool) {
	argProto := strings.ToLower(protoArg)
	if !isSupportedProtocol(argProto) {
		return
	}
	setSelectionIfMissing(&s.Protocol, protocol, argProto)
	if p := support.ParsePort(addrArg); p != "" {
		setSelectionIfMissing(&s.TargetAddr, local, targetForPort(s.Protocol, p))
		return
	}
	if support.LooksLikeHostPort(addrArg) {
		setSelectionIfMissing(&s.TargetAddr, local, addrArg)
	}
}

func setSelectionIfMissing(selection *string, provided bool, value string) {
	if !provided {
		*selection = value
	}
}

func targetForPort(protocol, port string) string {
	if strings.EqualFold(strings.TrimSpace(protocol), protoUDP) {
		return "127.0.0.1:" + port
	}
	return "localhost:" + port
}

func isSupportedProtocol(p string) bool {
	switch p {
	case protoHTTP, protoHTTPS, protoTCP, protoUDP:
		return true
	default:
		return false
	}
}
