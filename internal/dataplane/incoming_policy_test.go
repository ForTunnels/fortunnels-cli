// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fortunnels/client/internal/config"
)

func TestDestinationAllowed(t *testing.T) {
	tests := []struct {
		target, dst string
		want        bool
	}{
		{"", "anything.example:1", true},
		{"localhost:3000", "localhost:3000", true},
		{"localhost:3000", "LOCALHOST:4000", true},
		{"localhost:3000", "127.0.0.1:3000", true},
		{"127.0.0.1:3000", "[::1]:3000", true},
		{"localhost:3000", "10.0.0.5:3000", false},
		{"localhost:3000", "evil.example:3000", false},
		{"10.0.0.5:80", "10.0.0.5:81", true},
		{"10.0.0.5:80", "10.0.0.6:80", false},
		{"10.0.0.5:80", "localhost:80", false},
		{"10.0.0.5:80", "not-a-host-port", false},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, destinationAllowed(tt.target, tt.dst), "target=%q dst=%q", tt.target, tt.dst)
	}
}

func TestServeIncomingStream_RejectsDestinationOutsideTarget(t *testing.T) {
	connected := make(chan struct{}, 1)
	addr, cleanup := createTestTCPServer(t, func(c net.Conn) {
		connected <- struct{}{}
		c.Close()
	})
	defer cleanup()
	stream := &mockTCPReadWriteCloser{readData: []byte(`{"dst": "` + addr + `", "proto": "tcp"}` + "\n")}

	err := serveIncomingStream(stream, incomingPolicy{target: "10.0.0.5:80"})

	require.ErrorIs(t, err, errDestinationNotAllowed)
	assert.Contains(t, string(stream.writeData), `"ok":false`)
	select {
	case <-connected:
		t.Fatal("client dialed a destination outside the configured target")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestServeIncomingStream_AllowsPortOverrideOnTargetHost(t *testing.T) {
	addr, cleanup := createTestTCPServer(t, func(c net.Conn) {
		defer c.Close()
		_, _ = c.Write([]byte("pong"))
	})
	defer cleanup()
	clientSide, serverSide := net.Pipe()
	defer serverSide.Close()
	errCh := make(chan error, 1)
	go func() { errCh <- serveIncomingStream(clientSide, incomingPolicy{target: "127.0.0.1:9"}) }()

	_, err := serverSide.Write([]byte(`{"dst": "` + addr + `", "proto": "tcp"}` + "\n"))
	require.NoError(t, err)
	rd := bufio.NewReader(serverSide)
	ack, err := rd.ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, ack, `"ok":true`)
	require.NoError(t, serverSide.SetReadDeadline(time.Now().Add(2*time.Second)))
	body, err := io.ReadAll(rd)

	require.NoError(t, err)
	assert.Equal(t, "pong", string(body))
	require.NoError(t, <-errCh)
}

func TestReadStreamPreface_RejectsOversizedLine(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader(strings.Repeat("a", maxPrefaceBytes*3)))

	_, _, err := readStreamPreface(rd)

	require.ErrorIs(t, err, errPrefaceTooLong)
}

func TestServeIncomingStream_PrefaceTimesOut(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer serverSide.Close()
	errCh := make(chan error, 1)
	go func() {
		errCh <- serveIncomingStream(clientSide, incomingPolicy{prefaceTimeout: 50 * time.Millisecond})
	}()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.True(t, errors.Is(err, errPrefaceTimeout) || isTimeoutError(err), "got %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("silent server held the stream past the preface timeout")
	}
}

func isTimeoutError(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func TestServeIncomingStream_EncryptsPayloadAfterSetupAck(t *testing.T) {
	const psk = "0123456789abcdef0123456789abcdef"
	enc := config.EncryptionSettings{Enabled: true, PSK: psk}
	received := make(chan string, 1)
	addr, cleanup := createTestTCPServer(t, func(c net.Conn) {
		defer c.Close()
		buf := make([]byte, 4)
		_, _ = io.ReadFull(c, buf)
		received <- string(buf)
		_, _ = c.Write([]byte("pong"))
	})
	defer cleanup()
	clientSide, serverSide := net.Pipe()
	defer serverSide.Close()
	errCh := make(chan error, 1)
	go func() { errCh <- serveIncomingStream(clientSide, incomingPolicy{tunnelID: "t-1", enc: enc}) }()

	// The preface and the setup ack stay plaintext; the payload is encrypted.
	_, err := serverSide.Write([]byte(`{"dst": "` + addr + `", "proto": "tcp"}` + "\n"))
	require.NoError(t, err)
	rd := bufio.NewReader(serverSide)
	ack, err := rd.ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, ack, `"ok":true`)
	peer, err := WrapClientStream(struct {
		io.Reader
		io.Writer
		io.Closer
	}{rd, serverSide, serverSide}, "t-1", enc)
	require.NoError(t, err)
	_, err = peer.Write([]byte("ping"))
	require.NoError(t, err)
	require.NoError(t, serverSide.SetReadDeadline(time.Now().Add(2*time.Second)))
	reply := make([]byte, 4)
	_, err = io.ReadFull(peer, reply)

	require.NoError(t, err)
	assert.Equal(t, "pong", string(reply))
	assert.Equal(t, "ping", <-received)
	_ = serverSide.Close()
	<-errCh
}

func TestServeIncomingStream_EncryptedStreamKeepsHalfClose(t *testing.T) {
	previous := halfCloseIdleTimeout
	halfCloseIdleTimeout = 400 * time.Millisecond
	t.Cleanup(func() { halfCloseIdleTimeout = previous })
	const psk = "0123456789abcdef0123456789abcdef"
	enc := config.EncryptionSettings{Enabled: true, PSK: psk}
	addr, cleanup := createTestTCPServer(t, func(c net.Conn) {
		defer c.Close()
		_, _ = c.Write([]byte("bye"))
	})
	defer cleanup()
	// TCP connections, like smux streams, support CloseWrite and read deadlines.
	clientSide, serverSide := tcpBridgeTestPair(t)
	errCh := make(chan error, 1)
	go func() { errCh <- serveIncomingStream(clientSide, incomingPolicy{tunnelID: "t-1", enc: enc}) }()
	_, err := serverSide.Write([]byte(`{"dst": "` + addr + `", "proto": "tcp"}` + "\n"))
	require.NoError(t, err)
	rd := bufio.NewReader(serverSide)
	_, err = rd.ReadString('\n')
	require.NoError(t, err)
	peer, err := WrapClientStream(splitConn{Reader: rd, Writer: serverSide, Closer: serverSide}, "t-1", enc)
	require.NoError(t, err)
	reply := make([]byte, 3)
	_, err = io.ReadFull(peer, reply)
	require.NoError(t, err)
	require.NoError(t, serverSide.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, err = peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "backend EOF must reach the server as a half-close")

	// Half-closed, not closed: the stream outlives backend EOF until the idle
	// timeout ends it, because the server keeps its write side open and silent.
	select {
	case <-errCh:
		t.Fatal("stream was fully closed on backend EOF instead of half-closed")
	case <-time.After(150 * time.Millisecond):
	}
	select {
	case <-errCh:
	case <-time.After(3 * time.Second):
		t.Fatal("encrypted stream ignored the half-close idle timeout")
	}
}

func TestServeIncomingUDPStream_RejectsDestinationOutsideTarget(t *testing.T) {
	stream := &mockTCPReadWriteCloser{readData: []byte(`{"dst": "10.255.255.1:9", "proto": "udp"}` + "\n")}

	err := serveIncomingUDPStream(stream, incomingPolicy{target: "127.0.0.1:9"})

	require.ErrorIs(t, err, errDestinationNotAllowed)
}
