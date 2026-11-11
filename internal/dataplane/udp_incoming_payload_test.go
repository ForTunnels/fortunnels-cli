// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fortunnels/client/internal/config"
)

func TestServeIncomingUDPStream_RelaysBufferedPayload(t *testing.T) {
	t.Run("plaintext", func(t *testing.T) {
		assertIncomingUDPRoundTrip(t, config.EncryptionSettings{})
	})
	t.Run("encrypted", func(t *testing.T) {
		assertIncomingUDPRoundTrip(t, config.EncryptionSettings{Enabled: true, PSK: "incoming-udp-test-pre-shared-key"})
	})
}

func assertIncomingUDPRoundTrip(t *testing.T, enc config.EncryptionSettings) {
	t.Helper()
	backend, err := listenTestUDP()
	require.NoError(t, err)
	defer backend.Close()
	stream, peer := net.Pipe()
	done := make(chan error, 1)
	t.Cleanup(func() {
		_ = peer.Close()
		select {
		case serveErr := <-done:
			require.NoError(t, serveErr)
		case <-time.After(2 * time.Second):
			t.Error("closing the stream did not stop the UDP relay")
		}
	})
	deadline := time.Now().Add(2 * time.Second)
	require.NoError(t, backend.SetDeadline(deadline))
	require.NoError(t, peer.SetDeadline(deadline))
	const tunnelID = "udp-tunnel"
	dst := backend.LocalAddr().String()
	go func() {
		done <- serveIncomingUDPStream(stream, incomingPolicy{tunnelID: tunnelID, target: dst, enc: enc})
	}()

	preface, err := encodePreface(map[string]string{"dst": dst, "proto": "udp"})
	require.NoError(t, err)
	wire := bytes.NewBuffer(preface)
	framedPeer, err := WrapClientStream(splitConn{Reader: peer, Writer: wire, Closer: peer}, tunnelID, enc)
	require.NoError(t, err)
	require.NoError(t, writeUDPPacket(framedPeer, []byte("request")))
	// One write guarantees that the preface reader also buffers the first packet.
	_, err = peer.Write(wire.Bytes())
	require.NoError(t, err)

	buf := make([]byte, 64)
	n, src, err := backend.ReadFromUDP(buf)
	require.NoError(t, err)
	require.Equal(t, "request", string(buf[:n]))
	_, err = backend.WriteToUDP([]byte("reply"), src)
	require.NoError(t, err)
	reply, err := readUDPPacket(framedPeer)
	require.NoError(t, err)
	require.Equal(t, "reply", string(reply))
}
