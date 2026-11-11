// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/fortunnels/client/internal/config"
)

const (
	wsReadTimeout       = 2*config.MaxPingInterval + 15*time.Second
	udpMaxPacketSize    = 65535
	udpDatagramMaxSize  = 65507
	headerDataPlaneAuth = "X-Fortunnels-DP-Auth"
	schemeHTTP          = "http"
	schemeHTTPS         = "https"
)

func encodePreface(fields map[string]string) ([]byte, error) {
	b, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("marshal preface: %w", err)
	}
	return append(b, '\n'), nil
}

func buildWebSocketURL(serverURL, tunnelID, authToken string) (wsURL, origin, token string, err error) {
	u, parseErr := url.Parse(serverURL)
	if parseErr != nil || u.Scheme == "" || u.Host == "" {
		if parseErr == nil {
			parseErr = fmt.Errorf("invalid server url")
		}
		return "", "", "", parseErr
	}

	var originScheme, wsScheme string
	switch u.Scheme {
	case schemeHTTP:
		originScheme = schemeHTTP
		wsScheme = "ws"
	case schemeHTTPS:
		originScheme = schemeHTTPS
		wsScheme = "wss"
	default:
		return "", "", "", fmt.Errorf("unsupported server scheme: %s", u.Scheme)
	}

	u.Scheme = wsScheme
	u.Path = "/ws"
	q := u.Query()
	q.Set("mode", "data")
	q.Set("tunnel_id", tunnelID)
	q.Del("auth")
	u.RawQuery = q.Encode()

	wsURL = u.String()
	origin = originScheme + "://" + u.Host
	return wsURL, origin, strings.TrimSpace(authToken), nil
}

// maxPrefaceBytes bounds the JSON line a server may send ahead of a stream's
// payload; a legitimate preface is a few dozen bytes.
const maxPrefaceBytes = 4096

// defaultPrefaceTimeout bounds how long a server-opened stream may stay silent
// before it names its destination.
const defaultPrefaceTimeout = 15 * time.Second

var (
	errPrefaceTooLong = errors.New("stream preface too long")
	errPrefaceTimeout = errors.New("stream preface timed out")
)

// readPrefaceBounded reads a stream preface within wait when the stream
// supports read deadlines.
func readPrefaceBounded(stream io.Reader, rd *bufio.Reader, wait time.Duration) (proto, dst string, err error) {
	if ds, ok := stream.(readDeadlineSetter); ok {
		if deadlineErr := ds.SetReadDeadline(time.Now().Add(wait)); deadlineErr != nil {
			return "", "", deadlineErr
		}
		//nolint:errcheck // best-effort reset once the preface is read
		defer ds.SetReadDeadline(time.Time{})
	}
	proto, dst, err = readStreamPreface(rd)
	if err != nil && os.IsTimeout(err) {
		err = fmt.Errorf("%w: %w", errPrefaceTimeout, err)
	}
	return proto, dst, err
}

// readBoundedLine reads one newline-terminated line of at most maxPrefaceBytes.
func readBoundedLine(rd *bufio.Reader) (string, error) {
	var line []byte
	for {
		chunk, err := rd.ReadSlice('\n')
		line = append(line, chunk...)
		if len(line) > maxPrefaceBytes {
			return "", errPrefaceTooLong
		}
		if err == nil {
			return string(line), nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return "", err
		}
	}
}
