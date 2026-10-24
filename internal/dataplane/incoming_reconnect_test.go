// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/xtaci/smux"

	"github.com/fortunnels/client/internal/config"
)

func TestNewIncomingDataPlaneManager_UsesConfiguredBackoff(t *testing.T) {
	for _, tt := range []struct {
		name             string
		initial, maximum time.Duration
		wantInit         time.Duration
		wantMax          time.Duration
	}{
		{"configured", 3 * time.Second, 45 * time.Second, 3 * time.Second, 45 * time.Second},
		{"unset falls back to defaults", 0, 0, time.Second, 30 * time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tunnel := IncomingTunnel{
				ServerURL: "http://example.com", TunnelID: "t1", AuthToken: "auth",
				Backoff: config.ReconnectBackoff{Initial: tt.initial, Max: tt.maximum},
			}
			mgr := newIncomingDataPlaneManager(tunnel, config.RuntimeSettings{})
			defer mgr.Close()
			require.Equal(t, tt.wantInit, mgr.boInit)
			require.Equal(t, tt.wantMax, mgr.boMax)
		})
	}
}

// scriptedIncomingSessions hands out sessions in order (repeating the last one)
// and counts the loop's reconnect calls.
type scriptedIncomingSessions struct {
	sessions []dataPlaneSession
	ensured  int
	resets   int
}

func (s *scriptedIncomingSessions) EnsureSessionContext(context.Context) (dataPlaneSession, error) {
	sess := s.sessions[min(s.ensured, len(s.sessions)-1)]
	s.ensured++
	return sess, nil
}

func (s *scriptedIncomingSessions) resetSession() { s.resets++ }

// acceptOnceSession accepts one real stream and then fails like a dropped session.
type acceptOnceSession struct {
	resetSessionStub
	stream   *smux.Stream
	accepted bool
}

func (s *acceptOnceSession) AcceptStream() (*smux.Stream, error) {
	if s.accepted {
		return nil, errors.New("closed")
	}
	s.accepted = true
	return s.stream, nil
}

func newPipeSmuxStream(t *testing.T) *smux.Stream {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	client, err := smux.Client(clientConn, smux.DefaultConfig())
	require.NoError(t, err)
	server, err := smux.Server(serverConn, smux.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	stream, err := client.OpenStream()
	require.NoError(t, err)
	return stream
}

// recordingWaiter records each requested delay and stops the loop after limit waits.
func recordingWaiter(limit int) (retryWaiter, *[]time.Duration) {
	var waits []time.Duration
	return func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		if len(waits) >= limit {
			return context.Canceled
		}
		return nil
	}, &waits
}

func TestIncomingStreamLoop_BackoffGrowsAcrossRejectedSessions(t *testing.T) {
	sessions := &scriptedIncomingSessions{sessions: []dataPlaneSession{&resetSessionStub{}}}
	wait, waits := recordingWaiter(6)
	loop := incomingStreamLoop{
		sessions: sessions,
		retry:    newReconnectBackoff(10*time.Millisecond, 80*time.Millisecond),
		wait:     wait,
		dispatch: func(io.ReadWriteCloser) { t.Fatal("a rejected session must not dispatch a stream") },
	}

	err := loop.run(context.Background())

	require.ErrorIs(t, err, context.Canceled)
	ms := time.Millisecond
	require.Equal(t, []time.Duration{10 * ms, 20 * ms, 40 * ms, 80 * ms, 80 * ms, 80 * ms}, *waits,
		"each redial after a session that accepted no stream must wait longer, up to the maximum")
	require.Equal(t, 6, sessions.ensured)
	require.Equal(t, 6, sessions.resets)
}

func TestIncomingStreamLoop_AcceptedStreamResetsBackoff(t *testing.T) {
	healthy := &acceptOnceSession{stream: newPipeSmuxStream(t)}
	rejected := &resetSessionStub{}
	sessions := &scriptedIncomingSessions{sessions: []dataPlaneSession{rejected, rejected, rejected, healthy, rejected}}
	wait, waits := recordingWaiter(5)
	dispatched := 0
	loop := incomingStreamLoop{
		sessions: sessions,
		retry:    newReconnectBackoff(10*time.Millisecond, 80*time.Millisecond),
		wait:     wait,
		dispatch: func(io.ReadWriteCloser) { dispatched++ },
	}

	err := loop.run(context.Background())

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, dispatched)
	ms := time.Millisecond
	require.Equal(t, []time.Duration{10 * ms, 20 * ms, 40 * ms, 10 * ms, 20 * ms}, *waits,
		"a session that accepted a stream starts the next reconnect sequence from the initial delay")
}

func TestStartDataPlaneServeIncoming_RejectedSessionsBackOff(t *testing.T) {
	const wantDials = 6
	var mu sync.Mutex
	var dials []time.Time
	enough := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		dials = append(dials, time.Now())
		if len(dials) == wantDials {
			close(enough)
		}
		mu.Unlock()
		// Accept the WebSocket, then reject the session like a lifecycle refusal.
		_ = conn.Close()
	}))
	defer server.Close()
	tunnel := IncomingTunnel{
		ServerURL: server.URL, TunnelID: "t1", AuthToken: "auth",
		Backoff: config.ReconnectBackoff{Initial: 20 * time.Millisecond, Max: 320 * time.Millisecond},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- StartDataPlaneServeIncomingContext(ctx, tunnel, managerTestSettings(), nil, nil)
	}()

	select {
	case <-enough:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not redial the rejecting server")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("serve loop ignored cancellation")
	}

	mu.Lock()
	defer mu.Unlock()
	first := dials[1].Sub(dials[0])
	last := dials[wantDials-1].Sub(dials[wantDials-2])
	require.GreaterOrEqual(t, last, 3*first,
		"redial interval must grow with the configured backoff (first gap %s, last gap %s)", first, last)
}
