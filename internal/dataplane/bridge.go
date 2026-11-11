// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/fortunnels/client/internal/config"
	sec "github.com/fortunnels/client/internal/security"
)

// SafeClose closes the given io.Closer and logs any error.
// This is suitable for cleanup operations where we don't want to fail the main operation.
func SafeClose(c io.Closer) {
	if c != nil {
		if err := c.Close(); err != nil {
			log.Printf("error closing resource: %v", err)
		}
	}
}

// WrapClientStream wraps the stream with encryption if needed.
func WrapClientStream(s io.ReadWriteCloser, tunnelID string, enc config.EncryptionSettings) (io.ReadWriteCloser, error) {
	if !enc.Enabled {
		return s, nil
	}
	mgr := sec.NewClientPSK([]byte(enc.PSK))
	wrapped, err := mgr.Wrap(s, tunnelID)
	if err != nil {
		return nil, fmt.Errorf("wrap client stream: %w", err)
	}
	return wrapped, nil
}

var wrapClientStream = WrapClientStream

var errDestinationNotAllowed = errors.New("stream destination is not on the configured target host")

// incomingPolicy is what a client enforces on streams the server opens: where
// they may connect and whether their payload carries the PSK layer.
type incomingPolicy struct {
	reporter BackendStateReporter
	tunnelID string
	// target is the backend this client was started for. The server names the
	// destination of every stream, so the client only dials that host. An empty
	// target accepts any destination.
	target string
	enc    config.EncryptionSettings
	// prefaceTimeout overrides defaultPrefaceTimeout when positive.
	prefaceTimeout time.Duration
}

func (p incomingPolicy) prefaceWait() time.Duration {
	if p.prefaceTimeout > 0 {
		return p.prefaceTimeout
	}
	return defaultPrefaceTimeout
}

// destinationAllowed reports whether dst is on the configured target host. The
// port is not compared: the server legitimately redirects a stream to another
// local port of the same host (gateway destinations).
func destinationAllowed(target, dst string) bool {
	target = strings.TrimSpace(target)
	if target == "" {
		return true
	}
	wantHost, _, err := net.SplitHostPort(target)
	if err != nil {
		return false
	}
	gotHost, _, err := net.SplitHostPort(strings.TrimSpace(dst))
	if err != nil {
		return false
	}
	return strings.EqualFold(wantHost, gotHost) || (isLoopbackHost(wantHost) && isLoopbackHost(gotHost))
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// splitConn joins the read half of a buffered stream with its write half.
type splitConn struct {
	io.Reader
	io.Writer
	io.Closer
}

// securedStream is a PSK-encrypted payload stream that keeps the raw stream's
// read deadline and half-close, which the bridge relies on to end a stream whose
// peer went silent after the backend finished.
type securedStream struct {
	io.ReadWriteCloser
	raw io.ReadWriteCloser
}

func (s *securedStream) SetReadDeadline(t time.Time) error {
	if d, ok := s.raw.(readDeadlineSetter); ok {
		return d.SetReadDeadline(t)
	}
	return nil
}

// CloseWrite ends the write side only, so the server still sees the backend's
// EOF as a clean frame boundary while its own data can keep arriving.
func (s *securedStream) CloseWrite() error {
	if cw, ok := s.raw.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return s.Close()
}

// secureStream wraps the payload that follows the preface and setup ack. Reads
// go through rd so bytes already buffered behind the preface are decrypted too.
func (p incomingPolicy) secureStream(stream io.ReadWriteCloser, rd *bufio.Reader) (*securedStream, error) {
	secured, err := wrapClientStream(splitConn{Reader: rd, Writer: stream, Closer: stream}, p.tunnelID, p.enc)
	if err != nil {
		return nil, err
	}
	return &securedStream{ReadWriteCloser: secured, raw: stream}, nil
}
