// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"context"
	"time"
)

const (
	defaultReconnectBackoffInitial = time.Second
	defaultReconnectBackoffMax     = 30 * time.Second
)

// normalizeReconnectBackoff applies the CLI defaults to unset bounds and keeps
// the maximum at or above the initial delay.
func normalizeReconnectBackoff(initial, maximum time.Duration) (normalizedInitial, normalizedMax time.Duration) {
	if initial <= 0 {
		initial = defaultReconnectBackoffInitial
	}
	if maximum <= 0 {
		maximum = defaultReconnectBackoffMax
	}
	if maximum < initial {
		maximum = initial
	}
	return initial, maximum
}

// reconnectBackoff yields the exponential wait between consecutive failed
// connection attempts. The owner resets it once a connection proved healthy,
// so the next failure starts again from the initial delay. It is not safe for
// concurrent use; each reconnect loop owns one.
type reconnectBackoff struct {
	initial time.Duration
	maximum time.Duration
	next    time.Duration
}

func newReconnectBackoff(initial, maximum time.Duration) *reconnectBackoff {
	initial, maximum = normalizeReconnectBackoff(initial, maximum)
	return &reconnectBackoff{initial: initial, maximum: maximum, next: initial}
}

// Next returns the wait before the upcoming attempt and doubles the following
// one, capped at the maximum.
func (b *reconnectBackoff) Next() time.Duration {
	wait := b.next
	b.next = nextBackoff(wait, b.maximum)
	return wait
}

// Reset restarts the sequence from the initial delay.
func (b *reconnectBackoff) Reset() { b.next = b.initial }

// retryWaiter blocks for d, or until ctx ends and then returns ctx's error.
type retryWaiter func(ctx context.Context, d time.Duration) error

func waitRetryDelay(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
