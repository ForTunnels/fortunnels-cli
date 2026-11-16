// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package control

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreatedTunnelCarriesIssuedDPAuth(t *testing.T) {
	var created CreatedTunnel
	require.NoError(t, json.Unmarshal([]byte(`{"id":"t-1","is_guest":true,"dp_auth":"abc123"}`), &created))
	assert.Equal(t, "t-1", created.ID)
	assert.True(t, created.IsGuest)
	assert.Equal(t, "abc123", created.DPAuth)

	var listed TunnelResponse
	require.NoError(t, json.Unmarshal([]byte(`{"id":"t-2","dp_auth":"never-on-list"}`), &listed))
	assert.Equal(t, "t-2", listed.ID, "list responses stay plain tunnels")
}

func TestCreateTunnelReturnsIssuedDPAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"t-9","dp_auth":"issued-token"}`))
	}))
	defer server.Close()

	created, err := CreateTunnelWithClient(server.URL, "localhost:3000", "http", "", nil, "", "")

	require.NoError(t, err)
	assert.Equal(t, "t-9", created.ID)
	assert.Equal(t, "issued-token", created.DPAuth)
}
