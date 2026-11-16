// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package control

import (
	"encoding/json"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocolv1 "github.com/fortunnels/client/shared/protocol/v1"
)

func TestRecovery_OwnerHeadersAndExactRoutes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "csrf", r.Header.Get("X-CSRF-Token"))
		session, err := r.Cookie("session")
		if assert.NoError(t, err) {
			assert.Equal(t, "owner", session.Value)
		}
		switch r.URL.Path {
		case "/api/tunnels/reusable":
			var request protocolv1.TunnelCreateRequest
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			assert.Equal(t, "localhost:8080", request.TargetAddr)
			w.WriteHeader(http.StatusCreated)
			assert.NoError(t, json.NewEncoder(w).Encode(CreatedTunnel{Response: Response{ID: "saved"}}))
		case "/api/tunnels/saved/attach":
			assert.NoError(t, json.NewEncoder(w).Encode(protocolv1.TunnelAttachmentResponse{
				Tunnel: Response{ID: "saved", PublicURL: "https://saved.example.test"},
				DPAuth: "proof", AttachmentTicket: "one-use",
			}))
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	endpoint, err := url.Parse(server.URL)
	require.NoError(t, err)
	jar.SetCookies(endpoint, []*http.Cookie{{Name: "session", Value: "owner"}})
	client := NewRecoveryClient(server.URL, &http.Client{Jar: jar}, "", "csrf")
	created, err := client.Provision(t.Context(), "localhost:8080", "http", "")
	require.NoError(t, err)
	grant, err := client.Attach(t.Context(), created.ID)
	require.NoError(t, err)
	require.Equal(t, "session=owner", grant.Headers.Get("Cookie"))
	require.Equal(t, "proof", grant.Headers.Get("X-Fortunnels-DP-Auth"))
	require.Equal(t, "one-use", grant.Headers.Get(protocolv1.AttachmentHeader))
}

func TestRecovery_OnlyTypedOccupiedResponsePermitsRetry(t *testing.T) {
	for _, status := range []int{http.StatusConflict, http.StatusForbidden} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "Bearer owner-token", r.Header.Get("Authorization"))
			w.WriteHeader(status)
			assert.NoError(t, json.NewEncoder(w).Encode(protocolv1.APIError{Code: "tunnel_in_use", Message: "private body"}))
		}))
		client := NewRecoveryClient(server.URL, server.Client(), "owner-token", "")
		_, err := client.Attach(t.Context(), "saved")
		server.Close()
		require.Error(t, err)
		require.Equal(t, status == http.StatusConflict, IsAttachmentOccupied(err))
		require.NotContains(t, err.Error(), "private body")
	}
}

func TestRecovery_RejectsMalformedAndMismatchedGrant(t *testing.T) {
	for _, body := range []string{`{"dp_auth":`, `{"tunnel":{"id":"other"},"dp_auth":"secret","attachment_ticket":"ticket"}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, err := w.Write([]byte(body))
			assert.NoError(t, err)
		}))
		client := NewRecoveryClient(server.URL, server.Client(), "owner", "")
		grant, err := client.Attach(t.Context(), "saved")
		server.Close()
		require.Nil(t, grant)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "secret")
	}
}
