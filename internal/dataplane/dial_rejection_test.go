// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rejectingUpgradeServer(t *testing.T, status int) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)
	return server
}

func TestEnsureSessionContext_RejectionWithoutCredentialIsPermanent(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		server := rejectingUpgradeServer(t, status)
		mgr := NewManager(server.URL, "tunnel-1", "", time.Millisecond, 5*time.Millisecond, managerTestSettings())
		t.Cleanup(mgr.Close)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)

		_, err := mgr.EnsureSessionContext(ctx)
		cancel()

		var permanent permanentError
		require.ErrorAs(t, err, &permanent, "status %d", status)
		assert.Contains(t, err.Error(), "--dp-auth-secret")
	}
}

func TestEnsureSessionContext_RejectionWithCredentialKeepsRetrying(t *testing.T) {
	server := rejectingUpgradeServer(t, http.StatusForbidden)
	mgr := NewManager(server.URL, "tunnel-1", "issued-token", time.Millisecond, 5*time.Millisecond, managerTestSettings())
	t.Cleanup(mgr.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	_, err := mgr.EnsureSessionContext(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	var permanent permanentError
	assert.NotErrorAs(t, err, &permanent)
}

func TestDialRejectionHint(t *testing.T) {
	assert.Contains(t, dialRejectionHint(&http.Response{StatusCode: http.StatusForbidden}), "credential")
	assert.Contains(t, dialRejectionHint(&http.Response{StatusCode: http.StatusUnauthorized}), "credential")
	assert.Empty(t, dialRejectionHint(&http.Response{StatusCode: http.StatusBadGateway}))
	assert.Empty(t, dialRejectionHint(nil))
}

func TestCreateDataPlaneSession_RejectionWithoutCredentialIsPermanent(t *testing.T) {
	server := rejectingUpgradeServer(t, http.StatusForbidden)

	sess, _, cleanup, err := createDataPlaneSessionWithFeatures(server.URL, "tunnel-1", managerTestSettings(), "")

	require.Nil(t, sess)
	require.Nil(t, cleanup)
	var permanent permanentError
	require.ErrorAs(t, err, &permanent)
}

func TestCreateDataPlaneSession_RejectionWithCredentialStaysRetryable(t *testing.T) {
	server := rejectingUpgradeServer(t, http.StatusForbidden)

	sess, _, cleanup, err := createDataPlaneSessionWithFeatures(server.URL, "tunnel-1", managerTestSettings(), "token")

	require.Nil(t, sess)
	require.Nil(t, cleanup)
	require.Error(t, err)
	var permanent permanentError
	assert.NotErrorAs(t, err, &permanent)
}
