// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package control

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/fortunnels/client/internal/config"
	"github.com/fortunnels/client/internal/dataplane"
	protocolv1 "github.com/fortunnels/client/shared/protocol/v1"
	"github.com/fortunnels/client/shared/wsconn"
)

var debugLogging = strings.Contains(
	strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))), "debug",
)

// logDebug logs at DEBUG level when LOG_LEVEL env contains "debug" or "DEBUG".
func logDebug(format string, args ...any) {
	if debugLogging {
		log.Printf("[DEBUG] "+format, args...)
	}
}

// Watcher prints control-plane lifecycle events and reports terminal ones.
type Watcher struct {
	out        Output
	onTerminal func(error)
}

func NewWatcher(out Output) *Watcher {
	if out == nil {
		out = StdOutput{}
	}
	return &Watcher{out: out}
}

func NewLifecycleWatcher(out Output, onTerminal func(error)) *Watcher {
	w := NewWatcher(out)
	w.onTerminal = onTerminal
	return w
}

// RunLifecycleWatch feeds terminal control-plane events to the shared CLI
// coordinator. It owns the WebSocket until the command is canceled or the
// server sends a terminal event.
func RunLifecycleWatch(
	ctx context.Context,
	httpClient *http.Client,
	serverURL, tunnelID, bearer string,
	runtime config.RuntimeSettings,
	onTerminal func(error),
) {
	w := NewLifecycleWatcher(nil, onTerminal)
	runLifecycleRetries(ctx, func(ctx context.Context) lifecycleAttemptResult {
		return w.lifecycleWatchAttempt(ctx, httpClient, serverURL, tunnelID, bearer, runtime)
	}, waitLifecycleRetry)
}

const (
	lifecycleBackoffInitial = time.Second
	lifecycleBackoffMax     = 30 * time.Second
	lifecycleReadTimeout    = 90 * time.Second
)

// lifecycleAttemptResult summarizes one lifecycle WebSocket connection.
type lifecycleAttemptResult struct {
	terminal   bool  // a terminal event ended the watch
	subscribed bool  // the server confirmed the subscription before the connection ended
	err        error // why a non-terminal connection ended
}

// runLifecycleRetries reconnects the lifecycle watch with exponential backoff
// until a terminal event arrives or ctx ends.
func runLifecycleRetries(
	ctx context.Context,
	attempt func(context.Context) lifecycleAttemptResult,
	wait func(context.Context, time.Duration) bool,
) {
	backoff := lifecycleBackoffInitial
	for ctx.Err() == nil {
		result := attempt(ctx)
		if result.terminal || ctx.Err() != nil {
			return
		}
		if result.subscribed {
			// The server accepted this watch, so its drop starts a new failure sequence.
			backoff = lifecycleBackoffInitial
		}
		if result.err != nil {
			logDebug("lifecycle WebSocket unavailable; next retry in %s", backoff)
		}
		if !wait(ctx, backoff) {
			return
		}
		backoff = min(backoff*2, lifecycleBackoffMax)
	}
}

// waitLifecycleRetry sleeps for d and reports false when ctx ended first.
func waitLifecycleRetry(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (w *Watcher) lifecycleWatchAttempt(ctx context.Context, httpClient *http.Client, serverURL, tunnelID, bearer string, runtime config.RuntimeSettings) lifecycleAttemptResult {
	// handleControlMessage signals the server's "subscribed" acknowledgement here.
	ackCh := make(chan struct{}, 1)
	terminal, err := w.watchLifecycleConnection(ctx, httpClient, serverURL, tunnelID, bearer, runtime, ackCh)
	return lifecycleAttemptResult{terminal: terminal, subscribed: len(ackCh) > 0, err: err}
}

func (w *Watcher) watchLifecycleConnection(
	ctx context.Context,
	httpClient *http.Client,
	serverURL, tunnelID, bearer string,
	runtime config.RuntimeSettings,
	ackCh chan<- struct{},
) (bool, error) {
	wsURL, err := buildControlWebSocketURL(serverURL, tunnelID)
	if err != nil {
		return false, err
	}
	dialer := *websocket.DefaultDialer
	baseDial := dialer.NetDialContext
	if baseDial == nil {
		baseDial = (&net.Dialer{}).DialContext
	}
	stopDialCancellation := func() bool { return false }
	dialer.NetDialContext = func(dialCtx context.Context, network, addr string) (net.Conn, error) {
		dialConn, dialErr := baseDial(dialCtx, network, addr)
		if dialErr == nil {
			stopDialCancellation = context.AfterFunc(ctx, func() { _ = dialConn.Close() })
		}
		return dialConn, dialErr
	}
	conn, resp, err := dialer.DialContext(ctx, wsURL, wsDialHeaders(httpClient, serverURL, bearer))
	defer stopDialCancellation()
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return false, err
	}
	defer conn.Close()
	conn.SetReadLimit(wsconn.MaxWebSocketMessageSize)
	if deadlineErr := conn.SetReadDeadline(time.Now().Add(lifecycleReadTimeout)); deadlineErr != nil {
		return false, deadlineErr
	}
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(lifecycleReadTimeout)) })
	intervalCh := make(chan time.Duration, 1)
	done := make(chan struct{})
	var doneOnce sync.Once
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		select {
		case <-ctx.Done():
		case <-done:
		}
		_ = conn.Close()
	}()
	defer func() { doneOnce.Do(func() { close(done) }); <-closed }()
	pingInterval, pingTimeout := runtime.PingInterval, runtime.PingTimeout
	if pingInterval <= 0 {
		pingInterval = 30 * time.Second
	}
	if pingTimeout <= 0 {
		pingTimeout = 5 * time.Second
	}
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	dataplane.StartControlPingLoop(done, &doneOnce, conn, ticker, pingTimeout)
	lastStatus := statusActive
	for {
		var msg protocolv1.Envelope
		if readErr := conn.ReadJSON(&msg); readErr != nil {
			return false, readErr
		}
		if w.handleControlMessage(msg, ackCh, intervalCh, done, &doneOnce, runtime.WatchInterval, &lastStatus) {
			return true, nil
		}
	}
}

// StatusExpired is the wire value for expired tunnel status.
const StatusExpired = protocolv1.StatusExpired

// Lifecycle contract: HTTP 401 from GET /api/tunnels?id=<id> means the tunnel
// was removed for this client session/token (e.g. revoked access, session expired).
// The client must treat this as terminal and exit cleanly. Technical details
// (tunnelID, auth mode, status) belong in DEBUG; user-facing output uses
// MsgTunnelRemovedExiting.
const MsgTunnelRemovedExiting = "Tunnel was removed. Exiting."

const (
	statusActive    = protocolv1.StatusActive
	statusNotActive = protocolv1.StatusNotActive
	statusPaused    = protocolv1.StatusPaused
)

// ensurePollClient returns a client suitable for polling. If httpClient is nil or
// has no timeout, a default client with 2s timeout is used. Preserves cookie jar
// when present for session auth.
func ensurePollClient(httpClient *http.Client) *http.Client {
	if httpClient == nil {
		return &http.Client{Timeout: 2 * time.Second}
	}
	if httpClient.Timeout <= 0 {
		c := *httpClient
		c.Timeout = 2 * time.Second
		return &c
	}
	return httpClient
}

// RunFallbackLifecyclePollerWithReasonContext polls the server and calls onTerminal
// when the tunnel is deleted or expired. Use it for HTTP/TCP/UDP modes that lack
// in-band lifecycle. Polling stops as soon as ctx is canceled, e.g. when another
// signal wins.
// httpClient may be nil; when provided with a cookie jar (session auth), it is used
// for authenticated requests. bearer is used when non-empty. The callback receives
// a non-nil TrafficQuotaError only for combined monthly traffic exhaustion.
func RunFallbackLifecyclePollerWithReasonContext(
	ctx context.Context,
	httpClient *http.Client,
	serverURL, tunnelID, bearer string,
	onTerminal func(error),
	interval time.Duration,
) {
	client := ensurePollClient(httpClient)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	authRejections := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			terminal, _, statusCode, terminalErr := checkTunnelTerminalDetailedContext(ctx, client, serverURL, tunnelID, bearer)
			if !terminal {
				authRejections = 0
				continue
			}
			if isAuthRejection(statusCode) {
				// A proxy or WAF can answer one poll with 401/403 while the
				// tunnel is fine; only a repeated rejection means removal.
				authRejections++
				if authRejections < authFailureConfirmations {
					continue
				}
			}
			onTerminal(terminalErr)
			return
		}
	}
}

// authFailureConfirmations is how many consecutive polls must answer 401/403
// before the tunnel counts as removed or access revoked.
const authFailureConfirmations = 3

func isAuthRejection(statusCode int) bool {
	return statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden
}

func checkTunnelTerminalDetailedContext(parent context.Context, client *http.Client, serverURL, tunnelID, bearer string) (terminal bool, status string, statusCode int, terminalErr error) {
	timeout := client.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", serverURL+"/api/tunnels?id="+tunnelID, http.NoBody)
	if err != nil {
		return false, "", 0, nil
	}
	if strings.TrimSpace(bearer) != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, "", 0, nil
	}
	defer resp.Body.Close()

	// 401/403 from GET /api/tunnels?id=<id> means tunnel removed or access revoked for this session/token.
	// One answer is reported as terminal here; the poll loop requires authFailureConfirmations in a row.
	if isAuthRejection(resp.StatusCode) {
		logDebug("%d from GET /api/tunnels → terminal (tunnel removed or access revoked)", resp.StatusCode)
		return true, "", resp.StatusCode, nil
	}
	if resp.StatusCode != http.StatusOK {
		return false, "", resp.StatusCode, nil
	}

	var payload protocolv1.TunnelListResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return false, "", resp.StatusCode, nil
	}
	status = tunnelStatusFromPayload(payload)
	if quotaErr := trafficQuotaErrorFromPayload(payload, tunnelID); quotaErr != nil {
		return true, status, resp.StatusCode, quotaErr
	}

	if !payload.Exists {
		return true, status, resp.StatusCode, nil
	}
	if status == StatusExpired {
		return true, status, resp.StatusCode, nil
	}
	return false, status, resp.StatusCode, nil
}

func trafficQuotaErrorFromPayload(payload protocolv1.TunnelListResponse, tunnelID string) error {
	for i := range payload.Tunnels {
		tunnel := &payload.Tunnels[i]
		if tunnelID != "" && tunnel.ID != "" && tunnel.ID != tunnelID {
			continue
		}
		if tunnel.LimitIndicators == nil {
			continue
		}
		indicators := tunnel.LimitIndicators
		monthlyIndicators := []protocolv1.MonthlyEgressLimitIndicator{
			indicators.MonthlyTraffic,
			indicators.MonthlyEgress,
		}
		trafficExhausted := indicators.Traffic.State == protocolv1.LimitIndicatorStateExhausted
		monthlyExhausted := false
		resetAt := time.Time{}
		for _, monthly := range monthlyIndicators {
			if monthly.State != protocolv1.LimitIndicatorStateExhausted {
				continue
			}
			monthlyExhausted = true
			if monthly.ResetAt != nil {
				resetAt = monthly.ResetAt.UTC()
			}
			break
		}
		if !trafficExhausted && !monthlyExhausted {
			continue
		}
		if trafficExhausted && !monthlyExhausted {
			for _, monthly := range monthlyIndicators {
				if monthly.ResetAt == nil {
					continue
				}
				resetAt = monthly.ResetAt.UTC()
				break
			}
		}
		return &TrafficQuotaError{ResetAt: resetAt}
	}
	return nil
}

func tunnelStatusFromPayload(response protocolv1.TunnelListResponse) string {
	if response.Status != "" {
		return response.Status
	}
	if len(response.Tunnels) > 0 && response.Tunnels[0].Status != "" {
		return response.Tunnels[0].Status
	}
	return ""
}

func (w *Watcher) printTunnelStatusChange(status string) {
	switch status {
	case StatusExpired:
		return
	case statusActive:
		w.out.Printf("✅ Tunnel status changed to active on server\n")
	case statusPaused:
		w.out.Printf("⏸️ Tunnel status changed to paused on server\n")
	case statusNotActive:
		w.out.Printf("⚪ Tunnel status changed to not active on server\n")
	default:
		w.out.Printf("📨 Tunnel status changed on server: %s\n", status)
	}
}

func (w *Watcher) handleControlMessage(
	msg protocolv1.Envelope,
	ackCh chan<- struct{},
	intervalCh chan<- time.Duration,
	done chan struct{},
	doneOnce *sync.Once,
	defaultWatchInterval time.Duration,
	lastStatus *string,
) bool {
	switch msg.Type {
	case protocolv1.MessageTypePong:
		w.out.Printf("💓 Ping received at %s\n", time.Now().Format("15:04:05"))
	case protocolv1.EventTunnelClosed:
		payload := decodeTunnelClosePayload(msg)
		reason := payload.Reason
		logDebug("tunnel_closed reason=%s", reason)
		var terminalErr error
		if reason == protocolv1.ReasonMonthlyTraffic {
			terminalErr = TrafficQuotaErrorFromLifecycle(payload)
		} else {
			terminalErr = newTunnelRemovedError()
		}
		if w.onTerminal != nil {
			w.onTerminal(terminalErr)
		} else {
			w.out.Println(terminalErr.Error())
		}
		doneOnce.Do(func() { close(done) })
		return true
	case protocolv1.EventTunnelUpdated:
		var payload protocolv1.LifecycleEventPayload
		if err := msg.DecodePayload(&payload); err == nil {
			if payload.Status == StatusExpired {
				if w.onTerminal != nil {
					w.onTerminal(newTunnelRemovedError())
				} else {
					w.out.Println(MsgTunnelRemovedExiting)
				}
				doneOnce.Do(func() { close(done) })
				return true
			}
			if payload.Status != "" && (lastStatus == nil || *lastStatus != payload.Status) {
				w.printTunnelStatusChange(payload.Status)
				if lastStatus != nil {
					*lastStatus = payload.Status
				}
			}
		}
	case protocolv1.MessageTypeSubscribed:
		notifyAckReceived(ackCh)
		updateFallbackInterval(intervalCh, defaultWatchInterval)
		w.out.Printf("📨 Message: %s\n", msg.Type)
	case protocolv1.MessageTypeError:
		var payload protocolv1.ErrorPayload
		if err := msg.DecodePayload(&payload); err == nil && payload.Message != "" {
			w.out.Printf("❌ Error: %s\n", payload.Message)
		}
	default:
		w.out.Printf("📨 Message: %s\n", msg.Type)
	}
	return false
}

func decodeTunnelClosePayload(msg protocolv1.Envelope) protocolv1.LifecycleEventPayload {
	var payload protocolv1.LifecycleEventPayload
	if err := msg.DecodePayload(&payload); err != nil {
		logDebug("decode tunnel_closed payload: %v", err)
	}
	if payload.Reason == "" {
		payload.Reason = protocolv1.ReasonUnknown
	}
	return payload
}

func notifyAckReceived(ackCh chan<- struct{}) {
	select {
	case ackCh <- struct{}{}:
	default:
	}
}

func updateFallbackInterval(intervalCh chan<- time.Duration, d time.Duration) {
	if d <= 0 {
		return
	}
	select {
	case intervalCh <- d:
	default:
	}
}
