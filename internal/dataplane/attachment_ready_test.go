// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xtaci/smux"

	"github.com/fortunnels/client/internal/config"
	protocolv1 "github.com/fortunnels/client/shared/protocol/v1"
	"github.com/fortunnels/client/shared/wsconn"
)

func TestRecovery_ReadyKeepsBufferedLifecycleEvent(t *testing.T) {
	wire := "{\"event\":\"tunnel_attach_ready\",\"tunnel_id\":\"saved\"}\n" +
		"{\"event\":\"tunnel_closed\",\"tunnel_id\":\"saved\",\"reason\":\"deleted\"}\n"
	reader := bufio.NewReaderSize(strings.NewReader(wire), maxPrefaceBytes)
	require.NoError(t, readAttachmentReady(reader, "saved"))
	var received protocolv1.LifecycleEventPayload
	readLifecycleControlStream(reader, func(e protocolv1.LifecycleEventPayload) { received = e })
	require.Equal(t, "deleted", received.Reason)
	require.Equal(t, "saved", received.TunnelID)
}

func TestRecovery_WebSocketRequiresReadyBeforePublishingSession(t *testing.T) {
	for _, event := range []string{protocolv1.EventTunnelAttachReady, protocolv1.EventTunnelAttachFailed} {
		t.Run(event, func(t *testing.T) {
			finished := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(finished)
				assert.Equal(t, "one-use", r.Header.Get(protocolv1.AttachmentHeader))
				writeTestAttachmentEvent(t, w, r, event)
			}))
			defer server.Close()
			episode := func(ctx context.Context, bind func(context.Context, http.Header) error) error {
				ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				defer cancel()
				return bind(ctx, http.Header{protocolv1.AttachmentHeader: {"one-use"}})
			}
			manager := NewRecoveryManager(server.URL, "saved", testRecoveryRuntime(), episode)
			defer manager.Close()
			session, err := manager.EnsureSessionContext(t.Context())
			if event == protocolv1.EventTunnelAttachReady {
				require.NoError(t, err)
				require.False(t, session.IsClosed())
			} else {
				require.ErrorContains(t, err, "address_unavailable")
				require.Nil(t, session)
			}
			manager.Close()
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("fixture WebSocket remained open")
			}
		})
	}
}

type recoveryDialClose struct {
	started chan struct{}
	release chan struct{}
}

func (c recoveryDialClose) Close() error {
	close(c.started)
	<-c.release
	return nil
}

func TestRecovery_WebSocketCancellationHandoff(t *testing.T) {
	t.Run("disarmed watcher cannot close a handed-off connection", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		conn := recoveryDialClose{started: make(chan struct{}), release: make(chan struct{})}
		close(conn.release)
		handoff := guardWebSocketDial(ctx, conn)
		handoff()
		cancel()
		handoff()
		require.Empty(t, conn.started)
		select {
		case <-conn.started:
			t.Fatal("cancellation closed the handed-off connection")
		default:
		}
	})
	t.Run("handoff waits for in-flight close", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		conn := recoveryDialClose{started: make(chan struct{}), release: make(chan struct{})}
		handoff := guardWebSocketDial(ctx, conn)
		cancel()
		<-conn.started
		done := make(chan struct{})
		go func() { handoff(); close(done) }()
		select {
		case <-done:
			t.Error("handoff returned while the canceled connection was closing")
		default:
		}
		close(conn.release)
		<-done
	})
}

func testRecoveryRuntime() config.RuntimeSettings {
	return config.RuntimeSettings{
		PingInterval: time.Second, PingTimeout: time.Second,
		SmuxKeepAliveInterval: time.Second, SmuxKeepAliveTimeout: 5 * time.Second,
	}
}

//nolint:testifylint // HTTP server goroutines must report failures without calling FailNow.
func writeTestAttachmentEvent(t *testing.T, w http.ResponseWriter, r *http.Request, name string) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	conn, err := upgrader.Upgrade(w, r, nil)
	if !assert.NoError(t, err) {
		return
	}
	defer conn.Close()
	session, err := smux.Server(wsconn.NewWSConn(conn), smux.DefaultConfig())
	if !assert.NoError(t, err) {
		return
	}
	defer session.Close()
	if !assert.NoError(t, session.SetDeadline(time.Now().Add(3*time.Second))) {
		return
	}
	stream, err := session.AcceptStream()
	if !assert.NoError(t, err) {
		return
	}
	defer stream.Close()
	line, err := bufio.NewReader(stream).ReadString('\n')
	if !assert.NoError(t, err) || !assert.JSONEq(t, `{"proto":"control"}`, strings.TrimSpace(line)) {
		return
	}
	event := protocolv1.TunnelAttachmentEvent{Event: name, TunnelID: "saved"}
	if name == protocolv1.EventTunnelAttachFailed {
		event.Code = protocolv1.AttachmentAddressUnavailable
	}
	if assert.NoError(t, json.NewEncoder(stream).Encode(event)) {
		_, err := session.AcceptStream()
		assert.Error(t, err)
	}
}

func TestRecovery_RejectsWrongMissingAndFailedAcknowledgements(t *testing.T) {
	for _, tc := range []struct {
		record  string
		message string
	}{
		{"{\"event\":\"tunnel_attach_ready\",\"tunnel_id\":\"other\"}\n", "invalid attachment"},
		{"{\"event\":\"tunnel_attach_failed\",\"tunnel_id\":\"saved\",\"code\":\"address_unavailable\"}\n", "address_unavailable"},
		{strings.Repeat("x", maxPrefaceBytes+1), "before readiness"},
		{"", "before readiness"},
	} {
		reader := bufio.NewReaderSize(strings.NewReader(tc.record), maxPrefaceBytes)
		require.ErrorContains(t, readAttachmentReady(reader, "saved"), tc.message)
	}
}
