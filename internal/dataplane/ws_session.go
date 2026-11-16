// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xtaci/smux"

	"github.com/fortunnels/client/internal/config"
	"github.com/fortunnels/client/shared/wsconn"
)

// RecoveryEpisode acquires a ticket and calls bind under the episode deadline.
type RecoveryEpisode func(context.Context, func(context.Context, http.Header) error) error

var (
	errInvalidWebsocketURL = errors.New("invalid websocket url")
	errAttachmentUpgrade   = errors.New("attachment upgrade failed")
)

type websocketDialResult struct {
	resources *sessionResources
	response  *http.Response
}

func (m *Manager) dialSession(ctx context.Context, settings config.RuntimeSettings, handler LifecycleHandler) (websocketDialResult, error) {
	wsURL, headers := m.sessionDialParams()
	if wsURL == "" {
		return websocketDialResult{}, errInvalidWebsocketURL
	}
	if m.endpoint.recovery != nil {
		return m.dialRecoverySession(ctx, wsURL, settings, websocketSessionSetup{
			headers: headers, lifecycle: lifecycleSetup{handler: handler, tunnelID: m.endpoint.tunnelID},
		})
	}
	return dialSessionWithHeaders(ctx, wsURL, settings, websocketSessionSetup{headers: headers, lifecycle: lifecycleSetup{handler: handler}})
}

type websocketSessionSetup struct {
	headers   http.Header
	lifecycle lifecycleSetup
}

func (m *Manager) dialRecoverySession(ctx context.Context, wsURL string, settings config.RuntimeSettings, setup websocketSessionSetup) (websocketDialResult, error) {
	var result websocketDialResult
	err := m.endpoint.recovery(ctx, func(attempt context.Context, headers http.Header) error {
		headers = headers.Clone()
		headers.Set("Origin", setup.headers.Get("Origin"))
		lifecycle := setup.lifecycle
		lifecycle.deadline, _ = attempt.Deadline()
		var bindErr error
		result, bindErr = dialSessionWithHeaders(attempt, wsURL, settings, websocketSessionSetup{headers: headers, lifecycle: lifecycle})
		return bindErr
	})
	if err != nil && result.resources != nil {
		result.resources.close()
		return websocketDialResult{}, err
	}
	return result, err
}

func dialSessionWithHeaders(ctx context.Context, wsURL string, settings config.RuntimeSettings, setup websocketSessionSetup) (websocketDialResult, error) {
	conn, resp, stopCancellation, err := dialWebSocketContext(ctx, wsURL, setup.headers)
	defer stopCancellation()
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	result := websocketDialResult{response: resp}
	if err != nil {
		if setup.lifecycle.tunnelID != "" {
			return result, fmt.Errorf("%w: %s", errAttachmentUpgrade, redactDialError(err, resp))
		}
		return result, err
	}
	resources, err := initializeSession(conn, settings, setup.lifecycle)
	stopCancellation()
	if err == nil && ctx.Err() != nil {
		resources.close()
		return result, ctx.Err()
	}
	result.resources = resources
	return result, err
}

type wsKeepaliveConn interface {
	SetReadDeadline(time.Time) error
	SetPongHandler(func(string) error)
}

func configureWSReadKeepalive(conn wsKeepaliveConn) {
	//nolint:errcheck // best-effort read deadline
	_ = conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
	conn.SetPongHandler(func(string) error {
		//nolint:errcheck // pong handler best-effort deadline refresh
		_ = conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
		return nil
	})
}

func setupWSSmuxSession(conn *websocket.Conn, settings config.RuntimeSettings) (*smux.Session, error) {
	configureWSReadKeepalive(conn)

	cfg := smux.DefaultConfig()
	cfg.KeepAliveInterval = settings.SmuxKeepAliveInterval
	cfg.KeepAliveTimeout = settings.SmuxKeepAliveTimeout

	sess, err := smux.Client(wsconn.NewWSConn(conn), cfg)
	if err != nil {
		return nil, err
	}
	return sess, nil
}

// StartPingLoop sends WebSocket ping frames until done is closed.
func StartPingLoop(done <-chan struct{}, conn *websocket.Conn, ticker *time.Ticker, pingTimeout time.Duration) {
	go func() {
		for {
			select {
			case <-ticker.C:
				deadline := time.Now().Add(pingTimeout)
				//nolint:errcheck // best-effort ping
				_ = conn.WriteControl(websocket.PingMessage, nil, deadline)
			case <-done:
				return
			}
		}
	}()
}

// StartControlPingLoop sends pings and closes done when a ping write fails.
func StartControlPingLoop(
	done chan struct{},
	doneOnce *sync.Once,
	conn *websocket.Conn,
	ticker *time.Ticker,
	pingTimeout time.Duration,
) {
	go func() {
		for {
			select {
			case <-ticker.C:
				deadline := time.Now().Add(pingTimeout)
				if err := conn.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
					doneOnce.Do(func() { close(done) })
					return
				}
			case <-done:
				return
			}
		}
	}()
}

func sleepReconnectBackoffContext(ctx context.Context, stopped func() bool, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil && !stopped()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	pollEvery := min(d, 50*time.Millisecond)
	poll := time.NewTicker(pollEvery)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return !stopped()
		case <-poll.C:
		}
		if stopped() {
			return false
		}
	}
}
