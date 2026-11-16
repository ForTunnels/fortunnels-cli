// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xtaci/smux"

	"github.com/fortunnels/client/internal/config"
	ctrl "github.com/fortunnels/client/internal/control"
	protocolv1 "github.com/fortunnels/client/shared/protocol/v1"
	"github.com/fortunnels/client/shared/wsconn"
)

func TestRecovery_ProvisionAttachAndGracefulShutdown(t *testing.T) {
	runtime := &recoveryTestRuntime{
		t: t, tunnel: ctrl.Response{ID: "saved", Protocol: "http", TargetAddr: "localhost:3000", UserID: 7, PublicURL: "https://saved.example.test"},
		ready: make(chan struct{}), attached: make(chan struct{}), released: make(chan struct{}),
	}
	server := httptest.NewServer(http.HandlerFunc(runtime.serveHTTP))
	defer server.Close()
	cfg := &config.Config{
		ServerURL: server.URL, Token: "owner-fixture",
		TunnelSelection: config.TunnelSelection{KeepTunnel: true, Protocol: "http", TargetAddr: "localhost:3000"},
		PingInterval:    time.Second, PingTimeout: time.Second,
		SmuxInterval: time.Second, SmuxTimeout: 5 * time.Second, WatchInterval: 20 * time.Millisecond,
	}
	client := ctrl.NewRecoveryClient(server.URL, server.Client(), cfg.Token, "")
	id, err := prepareRecoveryTunnel(t.Context(), cfg, client)
	require.NoError(t, err)
	require.Equal(t, "saved", id)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- serveRecoveryTunnel(ctx, client, &tunnelWatch{cfg: cfg, id: id, client: server.Client(), bearer: cfg.Token})
	}()
	select {
	case <-runtime.ready:
	case err := <-done:
		t.Fatalf("recovery stopped before readiness: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-runtime.attached:
	case err := <-done:
		t.Fatalf("recovery stopped before starting the lifecycle poller: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancel()
	require.NoError(t, <-done)
	select {
	case <-runtime.released:
	case <-time.After(5 * time.Second):
		t.Fatal("fixture did not observe data-plane closure")
	}
}

type recoveryTestRuntime struct {
	t        *testing.T
	tunnel   ctrl.Response
	ready    chan struct{}
	attached chan struct{}
	released chan struct{}
	poll     sync.Once
}

func (r *recoveryTestRuntime) serveHTTP(w http.ResponseWriter, req *http.Request) {
	assert.Equal(r.t, "Bearer owner-fixture", req.Header.Get("Authorization"))
	switch req.Method + " " + req.URL.Path {
	case "POST /api/tunnels/reusable":
		w.WriteHeader(http.StatusCreated)
		assert.NoError(r.t, json.NewEncoder(w).Encode(ctrl.CreatedTunnel{Response: r.tunnel}))
	case "POST /api/tunnels/saved/attach":
		grant := protocolv1.TunnelAttachmentResponse{Tunnel: r.tunnel, DPAuth: "fixture-key", AttachmentTicket: "fixture-ticket"}
		assert.NoError(r.t, json.NewEncoder(w).Encode(grant))
	case "GET /ws":
		r.serveDataplane(w, req)
	case "GET /api/tunnels":
		r.poll.Do(func() { close(r.attached) })
		assert.NoError(r.t, json.NewEncoder(w).Encode(map[string]any{"exists": true, "tunnels": []ctrl.Response{r.tunnel}}))
	default:
		r.t.Errorf("unexpected recovery request %s %s", req.Method, req.URL.Path)
		http.NotFound(w, req)
	}
}

func (r *recoveryTestRuntime) serveDataplane(w http.ResponseWriter, req *http.Request) {
	defer close(r.released)
	assert.Equal(r.t, "saved", req.URL.Query().Get("tunnel_id"))
	assert.Equal(r.t, "fixture-ticket", req.Header.Get(protocolv1.AttachmentHeader))
	upgrader := websocket.Upgrader{}
	conn, err := upgrader.Upgrade(w, req, nil)
	if !assert.NoError(r.t, err) {
		return
	}
	defer conn.Close()
	session, err := smux.Server(wsconn.NewWSConn(conn), smux.DefaultConfig())
	if !assert.NoError(r.t, err) {
		return
	}
	defer session.Close()
	if !assert.NoError(r.t, session.SetDeadline(time.Now().Add(4*time.Second))) {
		return
	}
	stream, err := session.AcceptStream()
	if !assert.NoError(r.t, err) {
		return
	}
	defer stream.Close()
	line, err := bufio.NewReader(stream).ReadString('\n')
	if !assert.NoError(r.t, err) || !assert.JSONEq(r.t, `{"proto":"control"}`, strings.TrimSpace(line)) {
		return
	}
	event := protocolv1.TunnelAttachmentEvent{Event: protocolv1.EventTunnelAttachReady, TunnelID: "saved"}
	if assert.NoError(r.t, json.NewEncoder(stream).Encode(event)) {
		close(r.ready)
		_, err := session.AcceptStream()
		assert.Error(r.t, err)
	}
}

func TestRecovery_StopPolicyAndNoCreateFallback(t *testing.T) {
	methods := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods <- r.Method + " " + r.URL.Path
		http.Error(w, "unsupported", http.StatusNotFound)
	}))
	defer server.Close()
	cfg := &config.Config{
		ServerURL: server.URL, Token: "owner-fixture",
		TunnelSelection: config.TunnelSelection{TunnelID: "saved"},
	}
	require.ErrorContains(t, runRecoveryWorkflow(cfg), "tunnel saved: attachment rejected (HTTP 404)")
	require.Equal(t, "POST /api/tunnels/saved/attach", <-methods)
	require.Empty(t, methods)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	terminal := ctrl.NewTerminalCoordinator(nil, nil)
	require.NoError(t, waitForProtocol(ctx, terminal, make(chan error), serveExit{}))
	require.Empty(t, methods)
}

func TestRecovery_InterruptedBeforeReadyNamesSavedTunnel(t *testing.T) {
	attempted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, "POST /api/tunnels/saved/attach", req.Method+" "+req.URL.Path)
		close(attempted)
		w.WriteHeader(http.StatusConflict)
		assert.NoError(t, json.NewEncoder(w).Encode(protocolv1.APIError{Code: string(protocolv1.AttachmentTunnelInUse)}))
	}))
	defer server.Close()
	cfg := &config.Config{ServerURL: server.URL, TunnelSelection: config.TunnelSelection{TunnelID: "saved"}}
	client := ctrl.NewRecoveryClient(server.URL, server.Client(), "owner", "")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serveRecoveryTunnel(ctx, client, &tunnelWatch{cfg: cfg, id: "saved"}) }()
	select {
	case <-attempted:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not request attachment")
	}
	cancel()
	require.ErrorContains(t, <-done, "retry with --tunnel-id saved")
}

func TestRecovery_RetriesOnlyOccupiedAndKeepsDeadlineThroughBind(t *testing.T) {
	attempts, waiting, ready := 0, 0, 0
	e := recoveryEpisode{
		request: func(ctx context.Context) (*ctrl.TunnelAttachment, error) {
			attempts++
			d, ok := ctx.Deadline()
			require.True(t, ok)
			require.LessOrEqual(t, time.Until(d), recoveryRequestTimeout)
			if attempts <= 2 {
				return nil, &ctrl.TunnelCreateError{Status: http.StatusConflict, Code: string(protocolv1.AttachmentTunnelInUse)}
			}
			return &ctrl.TunnelAttachment{Headers: http.Header{protocolv1.AttachmentHeader: {"ticket"}}}, nil
		},
		validate: func(*ctrl.Response) error { return nil },
		waiting:  func() { waiting++ },
		ready:    func(*ctrl.Response) { ready++ },
		delay:    func(context.Context) error { return nil },
	}
	require.NoError(t, e.run(t.Context(), func(ctx context.Context, h http.Header) error {
		d, ok := ctx.Deadline()
		require.True(t, ok)
		require.Greater(t, time.Until(d), recoveryRequestTimeout)
		require.LessOrEqual(t, time.Until(d), recoveryEpisodeTimeout)
		require.Equal(t, "ticket", h.Get(protocolv1.AttachmentHeader))
		return nil
	}))
	require.Equal(t, 3, attempts)
	require.Equal(t, 1, waiting)
	require.Equal(t, 1, ready)
}

func TestRecovery_AmbiguousBindAndCancellationEndEpisode(t *testing.T) {
	ambiguous := errors.New("transport ended before acknowledgement")
	e := recoveryEpisode{
		request:  func(context.Context) (*ctrl.TunnelAttachment, error) { return &ctrl.TunnelAttachment{}, nil },
		validate: func(*ctrl.Response) error { return nil },
		waiting:  func() { t.Fatal("transport errors must not retry") },
		ready:    func(*ctrl.Response) { t.Fatal("connection was never ready") },
	}
	require.ErrorIs(t, e.run(t.Context(), func(context.Context, http.Header) error { return ambiguous }), ambiguous)

	ctx, cancel := context.WithCancel(t.Context())
	e.request = func(context.Context) (*ctrl.TunnelAttachment, error) {
		cancel()
		return nil, &ctrl.TunnelCreateError{Status: http.StatusConflict, Code: string(protocolv1.AttachmentTunnelInUse)}
	}
	e.waiting = func() {}
	e.delay = recoveryDelay
	require.ErrorIs(t, e.run(ctx, func(context.Context, http.Header) error { return nil }), context.Canceled)
}
