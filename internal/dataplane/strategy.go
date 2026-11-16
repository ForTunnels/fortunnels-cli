// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/fortunnels/client/internal/config"
)

const (
	quicDescription = "\n📡 UDP over QUIC: listening on %s and forwarding to %s via QUIC datagrams ...\n"
	dtlsDescription = "\n📡 UDP over DTLS: listening on %s and forwarding to %s via DTLS ...\n"
	wsDescription   = "\n📡 UDP mode: listening on %s and forwarding to %s over WS→smux (preface proto=udp) ...\n"
)

// Strategy encapsulates a UDP data-plane mode.
type Strategy struct {
	Description    string
	RunningMessage string
	ErrLabel       string
	contextRunner  func(context.Context) error
	backoff        *strategyBackoff
}

type strategyBackoff struct {
	initial time.Duration
	max     time.Duration
}

func (s Strategy) RunContext(ctx context.Context) error {
	if s.contextRunner == nil {
		return nil
	}
	var backoff strategyBackoff
	if s.backoff != nil {
		backoff = *s.backoff
	}
	return runReconnecting(ctx, backoff.initial, backoff.max, s.contextRunner)
}

// NewStrategy builds a strategy for the requested UDP mode.
func NewStrategy(
	kind string,
	serverURL, tunnelID, authToken, dst, listen string,
	runtime config.RuntimeSettings,
	enc config.EncryptionSettings,
	boInit, boMax time.Duration,
) Strategy {
	var strategy Strategy
	switch kind {
	case "quic", "dtls":
		strategy = secureUDPStrategy(kind, serverURL, tunnelID, authToken, dst, listen, runtime)
	default:
		strategy = contextStrategy(
			fmt.Sprintf(wsDescription, listen, dst),
			"🔌 UDP tunnel running. Press Ctrl+C to stop.",
			"udp mode error",
			func(ctx context.Context) error {
				return StartDataPlaneUDPContext(ctx, serverURL, tunnelID, dst, listen, runtime, enc, authToken)
			},
		)
	}
	strategy.backoff = &strategyBackoff{initial: boInit, max: boMax}
	return strategy
}

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

func runReconnecting(ctx context.Context, boInit, boMax time.Duration, attempt func(context.Context) error) error {
	return newReconnectRunner(boInit, boMax).run(ctx, attempt)
}

// reconnectRunner reruns a long-lived transport attempt with exponential
// backoff between failures. An attempt that stayed up for at least the maximum
// backoff was a working session, not part of a fast failure loop, so the retry
// after it starts again from the initial delay.
type reconnectRunner struct {
	retry *reconnectBackoff
	now   func() time.Time
	wait  retryWaiter
}

func newReconnectRunner(boInit, boMax time.Duration) reconnectRunner {
	return reconnectRunner{retry: newReconnectBackoff(boInit, boMax), now: time.Now, wait: waitRetryDelay}
}

func (r reconnectRunner) run(ctx context.Context, attempt func(context.Context) error) error {
	for attempts := 1; ; attempts++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		started := r.now()
		err := attempt(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			return nil
		}
		var permanent permanentError
		if errors.As(err, &permanent) {
			return err
		}
		if r.now().Sub(started) >= r.retry.maximum {
			r.retry.Reset()
		}
		delay := r.retry.Next()
		// Raw transport errors can include an authenticated URL. Keep this
		// retry notice limited to attempt metadata.
		log.Printf("data-plane attempt %d failed; next retry in %s", attempts, delay)
		if waitErr := r.wait(ctx, delay); waitErr != nil {
			return waitErr
		}
	}
}

func secureUDPStrategy(
	kind string,
	serverURL, tunnelID, authToken, dst, listen string,
	runtime config.RuntimeSettings,
) Strategy {
	description := quicDescription
	running := "🔌 UDP QUIC tunnel running. Press Ctrl+C to stop."
	errLabel := "udp quic mode error"
	port := runtime.QUICPortString()
	start := startQUICDataPlaneUDPContext
	if kind == "dtls" {
		description = dtlsDescription
		running = "🔌 UDP DTLS tunnel running. Press Ctrl+C to stop."
		errLabel = "udp dtls mode error"
		port = runtime.DTLSPortString()
		start = startDTLSDataPlaneUDPContext
	}
	return contextStrategy(
		fmt.Sprintf(description, listen, dst),
		running,
		errLabel,
		func(ctx context.Context) error {
			return start(ctx, serverURL, port, tunnelID, authToken, dst, listen, runtime.TransportCAPath)
		},
	)
}

func contextStrategy(description, running, errLabel string, contextRunner func(context.Context) error) Strategy {
	return Strategy{
		Description:    description,
		RunningMessage: running,
		ErrLabel:       errLabel,
		contextRunner:  contextRunner,
	}
}
