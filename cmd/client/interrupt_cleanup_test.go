// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ctrl "github.com/fortunnels/client/internal/control"
)

func canceledContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	return ctx
}

func newTestTerminal() *ctrl.TerminalCoordinator {
	return ctrl.NewTerminalCoordinator(nil, func() {})
}

func TestWaitForProtocol_InterruptRunsCleanupOnce(t *testing.T) {
	var cleanups atomic.Int32

	err := waitForProtocol(canceledContext(t), newTestTerminal(), make(chan error),
		serveExit{onInterrupt: func() { cleanups.Add(1) }})

	require.NoError(t, err)
	assert.Equal(t, int32(1), cleanups.Load())
}

func TestWaitForProtocol_ServeLoopStoppedByInterruptIsNotAFailure(t *testing.T) {
	var cleanups, failures atomic.Int32
	errCh := make(chan error, 1)
	errCh <- context.Canceled

	err := waitForProtocol(canceledContext(t), newTestTerminal(), errCh, serveExit{
		onError:     func(err error) error { failures.Add(1); return err },
		onInterrupt: func() { cleanups.Add(1) },
	})

	require.NoError(t, err)
	assert.Equal(t, int32(1), cleanups.Load())
	assert.Zero(t, failures.Load())
}

func TestWaitForProtocol_DataPlaneFailureSkipsInterruptCleanup(t *testing.T) {
	var cleanups atomic.Int32
	errCh := make(chan error, 1)
	errCh <- assert.AnError

	err := waitForProtocol(t.Context(), newTestTerminal(), errCh,
		serveExit{onInterrupt: func() { cleanups.Add(1) }})

	require.ErrorIs(t, err, assert.AnError)
	assert.Zero(t, cleanups.Load())
}

func TestWaitForProtocol_ServerSideEndSkipsInterruptCleanup(t *testing.T) {
	var cleanups atomic.Int32
	terminal := newTestTerminal()
	terminal.Signal(nil)

	err := waitForProtocol(canceledContext(t), terminal, make(chan error),
		serveExit{onInterrupt: func() { cleanups.Add(1) }})

	require.NoError(t, err)
	assert.Zero(t, cleanups.Load())
}

func TestWaitForDataPlaneServe_InterruptDeletesTunnel(t *testing.T) {
	deleted := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted <- r.URL.Query().Get("id")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	err := waitForDataPlaneServe(canceledContext(t), newTestTerminal(),
		make(chan error), server.URL, "tunnel-9", nil, "", "")

	require.NoError(t, err)
	assert.Equal(t, "tunnel-9", <-deleted)
}
