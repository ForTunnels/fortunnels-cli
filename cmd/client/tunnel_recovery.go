// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fortunnels/client/internal/auth"
	"github.com/fortunnels/client/internal/config"
	ctrl "github.com/fortunnels/client/internal/control"
	dp "github.com/fortunnels/client/internal/dataplane"
)

const (
	recoveryEpisodeTimeout = 150 * time.Second
	recoveryRequestTimeout = 5 * time.Second
)

var (
	errRecoveryAccount        = errors.New("tunnel recovery requires owner authentication")
	errProvisionedOwner       = errors.New("reusable tunnel creation did not return an owned tunnel")
	errRecoveredTargetChanged = errors.New("the saved protocol or target changed")
	errRecoveryInterrupted    = errors.New("attachment interrupted before readiness; retry with --tunnel-id")
)

type recoveryEpisode struct {
	request  func(context.Context) (*ctrl.TunnelAttachment, error)
	validate func(*ctrl.TunnelResponse) error
	ready    func(*ctrl.TunnelResponse)
	waiting  func()
	delay    func(context.Context) error
}

func (e recoveryEpisode) run(parent context.Context, bind func(context.Context, http.Header) error) error {
	ctx, cancel := context.WithTimeout(parent, recoveryEpisodeTimeout)
	defer cancel()
	reported := false
	for {
		grant, err := requestRecoveryTicket(ctx, e.request)
		if err == nil {
			if targetErr := e.validate(&grant.Tunnel); targetErr != nil {
				return targetErr
			}
			if bindErr := bind(ctx, grant.Headers); bindErr != nil {
				return bindErr
			}
			e.ready(&grant.Tunnel)
			return nil
		}
		if !ctrl.IsAttachmentOccupied(err) {
			return err
		}
		if !reported {
			e.waiting()
			reported = true
		}
		if err := e.delay(ctx); err != nil {
			return err
		}
	}
}

func requestRecoveryTicket(ctx context.Context, request func(context.Context) (*ctrl.TunnelAttachment, error)) (*ctrl.TunnelAttachment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	attempt, cancel := context.WithTimeout(ctx, recoveryRequestTimeout)
	defer cancel()
	return request(attempt)
}

func recoveryDelay(ctx context.Context) error {
	jitter, err := rand.Int(rand.Reader, big.NewInt(int64(4*time.Second+1)))
	if err != nil {
		return fmt.Errorf("generate recovery retry jitter: %w", err)
	}
	timer := time.NewTimer(time.Second + time.Duration(jitter.Int64()))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func runRecoveryWorkflow(cfg *config.Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	httpClient, bearer, csrf, err := auth.SetupAuthentication(cfg)
	if err != nil {
		return fmt.Errorf("authentication failed: %w", err)
	}
	if bearer == "" && httpClient == nil {
		return errRecoveryAccount
	}
	client := ctrl.NewRecoveryClient(cfg.ServerURL, httpClient, bearer, csrf)
	id, err := prepareRecoveryTunnel(ctx, cfg, client)
	if err != nil {
		return err
	}
	err = serveRecoveryTunnel(ctx, client, &tunnelWatch{cfg: cfg, id: id, client: httpClient, bearer: bearer})
	if ctrl.IsReportedTerminalError(err) {
		return err
	}
	if err != nil {
		return fmt.Errorf("tunnel %s: %w", id, err)
	}
	return nil
}

func prepareRecoveryTunnel(ctx context.Context, cfg *config.Config, client ctrl.RecoveryClient) (string, error) {
	if cfg.TunnelID != "" {
		return cfg.TunnelID, nil
	}
	attempt, cancel := context.WithTimeout(ctx, recoveryRequestTimeout)
	defer cancel()
	created, err := client.Provision(attempt, cfg.TargetAddr, cfg.Protocol, cfg.UserID)
	if err != nil {
		return "", fmt.Errorf("create reusable tunnel: %w", err)
	}
	if created.ID == "" || created.UserID <= 0 || created.IsGuest {
		return "", errProvisionedOwner
	}
	return created.ID, nil
}

func serveRecoveryTunnel(ctx context.Context, client ctrl.RecoveryClient, watch *tunnelWatch) error {
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	terminal := ctrl.NewTerminalCoordinator(nil, cancel)
	episode := newRecoveryEpisode(watch, client)
	cfg := watch.cfg
	manager := dp.NewRecoveryManager(cfg.ServerURL, watch.id, cfg.RuntimeSettings(), episode.run)
	defer manager.Close()
	manager.SetLifecycleHandler(lifecycleSignal(terminal))
	if _, err := manager.EnsureSessionContext(serveCtx); err != nil {
		if terminal.Err() != nil {
			return terminal.ExitError()
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%w %s", errRecoveryInterrupted, watch.id)
		}
		return err
	}
	watchTunnel(serveCtx, terminal, watch)
	incoming := incomingTunnel(cfg, &ctrl.TunnelResponse{ID: watch.id}, "")
	incoming.Manager = manager
	return waitForProtocol(serveCtx, terminal, serveIncomingTunnel(serveCtx, terminal, cfg, incoming), serveExit{})
}

func newRecoveryEpisode(watch *tunnelWatch, client ctrl.RecoveryClient) recoveryEpisode {
	cfg, id := watch.cfg, watch.id
	first := true
	return recoveryEpisode{
		request: func(ctx context.Context) (*ctrl.TunnelAttachment, error) { return client.Attach(ctx, id) },
		validate: func(tun *ctrl.TunnelResponse) error {
			if first {
				return config.ResolveAttachmentTarget(cfg, tun.Protocol, tun.TargetAddr)
			}
			// The live serve loop owns its initial target and protocol.
			if tun.Protocol != cfg.Protocol || tun.TargetAddr != cfg.TargetAddr {
				return errRecoveredTargetChanged
			}
			return nil
		},
		ready: func(tun *ctrl.TunnelResponse) {
			first = false
			fmt.Printf("✅ Tunnel attached successfully!\n🔗 Public URL: %s\n🆔 Tunnel ID: %s\n", tun.PublicURL, tun.ID)
		},
		waiting: func() { fmt.Printf("Waiting for the current client to release tunnel %s.\n", id) },
		delay:   recoveryDelay,
	}
}
