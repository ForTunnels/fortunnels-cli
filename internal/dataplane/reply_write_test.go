// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"io"
	"net"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scriptedReplyWriter fails the first failures writes with err, then records.
type scriptedReplyWriter struct {
	err      error
	failures int
	wrote    [][]byte
}

func (w *scriptedReplyWriter) WriteToUDP(b []byte, _ *net.UDPAddr) (int, error) {
	if w.failures > 0 {
		w.failures--
		return 0, w.err
	}
	w.wrote = append(w.wrote, append([]byte(nil), b...))
	return len(b), nil
}

var testPeer = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5000}

func TestWriteLocalReply_DropsInsteadOfFailingSession(t *testing.T) {
	for _, writeErr := range []error{syscall.ENOBUFS, syscall.EHOSTUNREACH, syscall.ECONNREFUSED} {
		var drops uint64
		w := &scriptedReplyWriter{err: writeErr, failures: 1}

		require.NoError(t, writeLocalReply(w, []byte("x"), testPeer, &drops), "%v", writeErr)
		assert.Equal(t, uint64(1), drops)
	}
}

func TestWriteLocalReply_ClosedSocketEndsSession(t *testing.T) {
	var drops uint64
	w := &scriptedReplyWriter{err: net.ErrClosed, failures: 1}

	require.ErrorIs(t, writeLocalReply(w, []byte("x"), testPeer, &drops), net.ErrClosed)
	assert.Zero(t, drops)
}

func TestFlowRepliesDeliver_WriteFailureDropsOneDatagram(t *testing.T) {
	flows := newUDPFlowRegistry()
	flowID := flows.udpFlowID(testPeer)
	w := &scriptedReplyWriter{err: syscall.ENOBUFS, failures: 1}
	var drops uint64

	replies := flowReplies{local: w, flows: flows}

	require.NoError(t, replies.deliver(flowID, []byte("lost"), &drops))
	require.NoError(t, replies.deliver(flowID, []byte("kept"), &drops))

	assert.Equal(t, [][]byte{[]byte("kept")}, w.wrote)
}

func TestStartPacketsToUDPLocal_WriteFailureKeepsRelayRunning(t *testing.T) {
	w := &scriptedReplyWriter{err: syscall.ENOBUFS, failures: 1}
	packets := [][]byte{[]byte("lost"), []byte("kept")}
	var mu sync.RWMutex
	src := testPeer
	errCh := make(chan error, 1)

	startPacketsToUDPLocal(func() ([]byte, error) {
		if len(packets) == 0 {
			return nil, io.EOF
		}
		next := packets[0]
		packets = packets[1:]
		return next, nil
	}, w, errCh, &mu, &src)

	require.ErrorIs(t, <-errCh, io.EOF)
	assert.Equal(t, [][]byte{[]byte("kept")}, w.wrote)
}
