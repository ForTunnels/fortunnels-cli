// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package control

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fortunnels/client/internal/config"
)

func TestRunLifecycleWatch_ReconnectsAfterServerClose(t *testing.T) {
	var upgrades atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if upgrades.Add(1) == 1 {
			return
		}
		_ = conn.WriteJSON(map[string]any{"type": "tunnel_closed", "payload": map[string]string{"reason": "user_request"}})
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	terminal := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		RunLifecycleWatch(ctx, server.Client(), server.URL, "t1", "", config.RuntimeSettings{PingInterval: time.Hour, PingTimeout: time.Second}, func(err error) { terminal <- err })
		close(done)
	}()
	select {
	case err := <-terminal:
		require.EqualError(t, err, MsgTunnelRemovedExiting)
	case <-ctx.Done():
		t.Fatal("terminal event did not arrive after reconnect")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watch did not end after terminal event")
	}
	assert.GreaterOrEqual(t, upgrades.Load(), int32(2))
}

func TestRunLifecycleWatch_StopsOnContextCancel(t *testing.T) {
	for _, holdHandshake := range []bool{false, true} {
		name := "connected"
		if holdHandshake {
			name = "handshake"
		}
		t.Run(name, func(t *testing.T) {
			requested, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if holdHandshake {
					close(requested)
					<-release
				}
				upgrader := websocket.Upgrader{}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				if !holdHandshake {
					close(requested)
				}
				_, _, _ = conn.ReadMessage()
			}))
			defer server.Close()
			defer unblock()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var terminal atomic.Bool
			done := make(chan struct{})
			go func() {
				RunLifecycleWatch(ctx, server.Client(), server.URL, "t1", "", config.RuntimeSettings{PingInterval: time.Hour, PingTimeout: time.Second}, func(error) { terminal.Store(true) })
				close(done)
			}()
			select {
			case <-requested:
			case <-time.After(time.Second):
				t.Fatal("watch did not start")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("watch ignored cancellation")
			}
			assert.False(t, terminal.Load())
		})
	}
}
