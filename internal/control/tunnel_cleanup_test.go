// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package control

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteTunnelWithClientSurvivesCanceledCaller(t *testing.T) {
	var got *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(t.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cleanup runs after the serve context is gone and must still reach the server
	DeleteTunnelWithClient(ctx, srv.URL, "tun-1", srv.Client(), " token ", " csrf ")

	require.NotNil(t, got, "cleanup request never reached the server")
	assert.Equal(t, http.MethodDelete, got.Method)
	assert.Equal(t, "/api/tunnels", got.URL.Path)
	assert.Equal(t, "tun-1", got.URL.Query().Get("id"))
	assert.Equal(t, "Bearer token", got.Header.Get("Authorization"))
	assert.Equal(t, "csrf", got.Header.Get("X-CSRF-Token"))
}

func TestDeleteTunnelWithClientSkipsEmptyID(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	t.Cleanup(srv.Close)

	DeleteTunnelWithClient(t.Context(), srv.URL, "", srv.Client(), "", "")

	assert.Zero(t, calls)
}
