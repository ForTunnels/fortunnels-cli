// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package control

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/fortunnels/client/internal/config"
	protocolv1 "github.com/fortunnels/client/shared/protocol/v1"
)

func TestRunLifecycleRetries_SubscribedSessionResetsBackoff(t *testing.T) {
	refused := lifecycleAttemptResult{err: errors.New("dial refused")}
	results := []lifecycleAttemptResult{
		refused, refused, refused,
		{subscribed: true, err: errors.New("server restarted")},
		refused,
	}
	attempts := 0
	var waits []time.Duration
	runLifecycleRetries(context.Background(), func(context.Context) lifecycleAttemptResult {
		result := results[attempts]
		attempts++
		return result
	}, func(_ context.Context, d time.Duration) bool {
		waits = append(waits, d)
		return len(waits) < len(results)
	})

	s := time.Second
	require.Equal(t, []time.Duration{s, 2 * s, 4 * s, s, 2 * s}, waits,
		"after a connection the server subscribed, the next reconnect waits the initial delay")
}

func TestLifecycleWatchAttempt_ReportsServerSubscription(t *testing.T) {
	for _, tt := range []struct {
		name      string
		subscribe bool
	}{
		{"subscribed then dropped", true},
		{"dropped before subscribing", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			upgrader := websocket.Upgrader{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				if tt.subscribe {
					_ = conn.WriteJSON(map[string]any{
						"type":    protocolv1.MessageTypeSubscribed,
						"payload": map[string]string{"tunnel_id": "t1"},
					})
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			runtime := config.RuntimeSettings{PingInterval: time.Hour, PingTimeout: time.Second}

			result := NewWatcher(nopOutput{}).lifecycleWatchAttempt(ctx, server.Client(), server.URL, "t1", "", runtime)

			require.False(t, result.terminal)
			require.Error(t, result.err, "the dropped connection must be reported")
			require.Equal(t, tt.subscribe, result.subscribed)
		})
	}
}

type nopOutput struct{}

func (nopOutput) Printf(string, ...any) {}
func (nopOutput) Println(...any)        {}
