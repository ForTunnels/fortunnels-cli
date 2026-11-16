// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// A timeout_ms of 0 selects the server's maximum wait, so sending it at the end
// of the deadline would overshoot --timeout by up to 30 seconds.
func TestWebhookWaitNeverRequestsZeroTimeout(t *testing.T) {
	var mu sync.Mutex
	var requested []int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			TimeoutMS int `json:"timeout_ms"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		requested = append(requested, body.TimeoutMS)
		mu.Unlock()
		writeJSON(w, 408, map[string]any{"code": "wait_timeout", "message": "no matching event"})
	}))
	t.Cleanup(ts.Close)

	code, _, _ := captureCLI(t, "wait", "--api-url", ts.URL, "--api-token", "test-token",
		"--endpoint", "ep-1", "--timeout", "150")
	if code != exitFail {
		t.Fatalf("exit=%d want %d", code, exitFail)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(requested) == 0 {
		t.Fatal("the CLI sent no wait request")
	}
	for _, ms := range requested {
		if ms < 1 || ms > 150 {
			t.Fatalf("wait request asked for %d ms; want 1..150", ms)
		}
	}
}

func TestWaitSliceMS(t *testing.T) {
	for _, tt := range []struct {
		remaining time.Duration
		step      int
		want      int
	}{
		{remaining: 45 * time.Second, step: 30000, want: 30000},
		{remaining: 1500 * time.Millisecond, step: 30000, want: 1500},
		{remaining: 400 * time.Microsecond, step: 30000, want: 1},
	} {
		if got := waitSliceMS(tt.remaining, tt.step); got != tt.want {
			t.Errorf("waitSliceMS(%s, %d) = %d, want %d", tt.remaining, tt.step, got, tt.want)
		}
	}
}
