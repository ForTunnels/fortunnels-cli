// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package control

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scriptedPollClient answers the nth poll with script[n]; the last entry repeats.
func scriptedPollClient(script []int) (*http.Client, *atomic.Int32) {
	var calls atomic.Int32
	return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		n := int(calls.Add(1)) - 1
		status := script[min(n, len(script)-1)]
		body := `{"exists":true,"status":"active"}`
		if status == 0 { // scripted "tunnel is gone": 200 with exists=false
			body = `{"exists":false}`
		}
		return &http.Response{
			StatusCode: max(status, http.StatusOK), Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(body)), Request: r,
		}, nil
	})}, &calls
}

func runPoller(t *testing.T, client *http.Client, wait time.Duration) (terminal <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), wait)
	t.Cleanup(cancel)
	out := make(chan error, 1)
	go RunFallbackLifecyclePollerWithReasonContext(ctx, client, "https://server.test", "tunnel-1", "token",
		func(err error) { out <- err }, time.Millisecond)
	return out
}

func TestPoller_IsolatedAuthFailuresDoNotEndTheTunnel(t *testing.T) {
	for name, script := range map[string][]int{
		"recovers before confirmation": {403, 403, 200},
		"alternating":                  {401, 200, 403, 200},
	} {
		client, calls := scriptedPollClient(script)
		terminal := runPoller(t, client, 200*time.Millisecond)

		select {
		case <-terminal:
			t.Fatalf("%s: tunnel ended after a transient 401/403", name)
		case <-time.After(200 * time.Millisecond):
		}
		assert.Greater(t, calls.Load(), int32(authFailureConfirmations), name)
	}
}

func TestPoller_ConfirmedAuthFailureEndsTheTunnel(t *testing.T) {
	client, calls := scriptedPollClient([]int{http.StatusForbidden})
	terminal := runPoller(t, client, 2*time.Second)

	select {
	case err := <-terminal:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("persistent 403 never ended the tunnel")
	}
	assert.Equal(t, int32(authFailureConfirmations), calls.Load())
}

func TestPoller_MissingTunnelEndsImmediately(t *testing.T) {
	client, calls := scriptedPollClient([]int{0})
	terminal := runPoller(t, client, 2*time.Second)

	select {
	case <-terminal:
	case <-time.After(2 * time.Second):
		t.Fatal("exists=false did not end the tunnel")
	}
	assert.Equal(t, int32(1), calls.Load())
}
