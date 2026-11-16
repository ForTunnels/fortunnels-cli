// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package main

// Command client provides a CLI to create tunnels and test data-plane.
// Modes:
// - HTTP/HTTPS: creates control-plane tunnel and prints usage hints
// - TCP test: establishes WS→smux session and sends parallel echo messages
// - TCP listen: accepts local connections and forwards via smux streams

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/fortunnels/client/internal/auth"
	"github.com/fortunnels/client/internal/config"
	ctrl "github.com/fortunnels/client/internal/control"
	dp "github.com/fortunnels/client/internal/dataplane"
	clierrors "github.com/fortunnels/client/internal/support"
	protocolv1 "github.com/fortunnels/client/shared/protocol/v1"
)

const (
	protoHTTP  = "http"
	protoHTTPS = "https"
)

var (
	defaultServerURL = "https://fortunnels.ru"
	version          = "dev" // Set via ldflags during build
)

type exitCodeError struct {
	code int
	err  error
}

func (e *exitCodeError) Error() string { return e.err.Error() }

func (e *exitCodeError) Unwrap() error { return e.err }

func exitCodeFor(err error) int {
	var codeErr *exitCodeError
	if errors.As(err, &codeErr) {
		return codeErr.code
	}
	return 1
}

func main() {
	// Check for version flag first
	if len(os.Args) > 1 && (os.Args[1] == "--version" || os.Args[1] == "-v" || os.Args[1] == "version") {
		fmt.Printf("fortunnels-client %s\n", version)
		os.Exit(0)
	}

	if len(os.Args) > 1 && os.Args[1] == "config" {
		os.Exit(runConfigCommand(os.Args[2:]))
	}

	if len(os.Args) > 1 && os.Args[1] == "webhook" {
		os.Exit(runWebhookCommand(os.Args[2:]))
	}

	cfg, err := parseConfig()
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(exitCodeFor(err))
	}

	if err := runClientWorkflow(cfg); err != nil {
		writeWorkflowError(os.Stderr, err)
		os.Exit(1)
	}
}

func writeWorkflowError(output io.Writer, err error) {
	if err == nil || ctrl.IsReportedTerminalError(err) {
		return
	}
	fmt.Fprintf(output, "%v\n", err)
}

func parseConfig() (*config.Config, error) {
	config.SetDefaultServerURL(defaultServerURL)
	cfg, err := config.Parse()
	if err != nil {
		return nil, err
	}
	if err := config.Validate(cfg); err != nil {
		return nil, &exitCodeError{code: 2, err: fmt.Errorf("❌ %w", err)}
	}
	if cfg.TunnelID != "" {
		return cfg, nil
	}
	if err := ensureHTTPHasTarget(cfg); err != nil {
		return nil, err
	}
	if err := ensureTCPHasTarget(cfg); err != nil {
		return nil, err
	}
	if err := ensureUDPHasTarget(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func runClientWorkflow(cfg *config.Config) error {
	if cfg.PreservesTunnel() {
		return runRecoveryWorkflow(cfg)
	}
	fmt.Printf("Creating tunnel for %s://%s\n", cfg.Protocol, cfg.TargetAddr)
	fmt.Printf("Connecting to server: %s\n", cfg.ServerURL)

	httpClient, bearer, csrf, err := auth.SetupAuthentication(cfg)
	if err != nil {
		return fmt.Errorf("❌ Authentication failed: %w", err)
	}

	created, err := ctrl.CreateTunnelWithClient(
		cfg.ServerURL,
		cfg.TargetAddr,
		cfg.Protocol,
		cfg.UserID,
		httpClient,
		bearer,
		csrf,
	)
	if err != nil {
		if authErr := auth.MapCreateTunnelAuthError(err, bearer, cfg.TokenFromConfigFile, cfg.Protocol); authErr != nil {
			return fmt.Errorf("❌ Authentication failed: %w", authErr)
		}
		return clierrors.HandleTunnelCreationError(err, cfg.ServerURL, cfg.Protocol, cfg.TargetAddr)
	}

	tun := &created.Response
	if err := auth.CheckBearerNotRejectedAsGuest(bearer, cfg.TokenFromConfigFile, auth.TunnelGuestSignals{
		IsGuest: tun.IsGuest,
		UserID:  tun.UserID,
	}, cfg.Protocol); err != nil {
		// Guest tunnel was created without valid auth; omit rejected bearer on cleanup.
		ctrl.DeleteTunnelWithClient(context.Background(), cfg.ServerURL, tun.ID, httpClient, "", csrf)
		return fmt.Errorf("❌ Authentication failed: %w", err)
	}

	runtime := cfg.RuntimeSettings()
	enc := cfg.EncryptionSettings()
	authToken := auth.ResolveDataPlaneAuth(&auth.DataPlaneAuthInputs{
		TunnelID: tun.ID, Issued: created.DPAuth, TokenFlag: cfg.DPAuthToken, SecretFlag: cfg.DPAuthSecret,
		PSK: cfg.PSK, Encrypt: enc.Enabled,
	})

	ctrl.PrintTunnelInfo(cfg.ServerURL, tun)
	loginUsed := strings.TrimSpace(cfg.Login) != "" && strings.TrimSpace(cfg.Password) != ""
	ctrl.WarnGuestTunnelWithLogin(loginUsed, tun)
	// Interrupts are intercepted only once the tunnel exists, so Ctrl+C during
	// login or creation still stops the CLI at once.
	interruptCtx, stopInterrupts := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopInterrupts()
	serveCtx, cancelServe := context.WithCancel(interruptCtx)
	defer cancelServe()
	terminal := ctrl.NewTerminalCoordinator(nil, cancelServe)
	watchTunnel(serveCtx, terminal, &tunnelWatch{cfg: cfg, id: tun.ID, client: httpClient, bearer: bearer})
	if config.IsUDPReverseMode(cfg) && cfg.Protocol == "udp" {
		strategy := dp.NewStrategy(
			strings.ToLower(cfg.DataPlane), cfg.ServerURL, tun.ID, authToken,
			cfg.UDPDst, cfg.UDPListen, runtime, enc, cfg.BackoffInitial, cfg.BackoffMax,
		)
		return handleUDPReverseMode(serveCtx, terminal, strategy, cfg.ServerURL, tun.ID, httpClient, bearer, csrf)
	}
	errCh := serveIncomingTunnel(serveCtx, terminal, cfg, incomingTunnel(cfg, tun, authToken))
	return waitForDataPlaneServe(serveCtx, terminal, errCh, cfg.ServerURL, tun.ID, httpClient, bearer, csrf)
}

// incomingTunnel describes the created tunnel for the expose-local serve loops,
// including the configured --backoff-initial/--backoff-max.
func incomingTunnel(cfg *config.Config, tun *ctrl.Response, dpAuthToken string) *dp.IncomingTunnel {
	return &dp.IncomingTunnel{
		ServerURL: cfg.ServerURL, TunnelID: tun.ID, AuthToken: dpAuthToken, Backoff: cfg.ReconnectBackoff(),
		Target: cfg.TargetAddr, Encryption: cfg.EncryptionSettings(),
	}
}

type tunnelWatch struct {
	cfg    *config.Config
	id     string
	client *http.Client
	bearer string
}

func watchTunnel(ctx context.Context, terminal *ctrl.TerminalCoordinator, watch *tunnelWatch) {
	cfg := watch.cfg
	runtime := cfg.RuntimeSettings()
	go ctrl.RunFallbackLifecyclePollerWithReasonContext(ctx, watch.client, cfg.ServerURL, watch.id, watch.bearer, terminal.Signal, runtime.WatchInterval)
	if cfg.WatchWS {
		go ctrl.RunLifecycleWatch(ctx, watch.client, cfg.ServerURL, watch.id, watch.bearer, runtime, terminal.Signal)
	}
}

func serveIncomingTunnel(ctx context.Context, terminal *ctrl.TerminalCoordinator, cfg *config.Config, incoming *dp.IncomingTunnel) <-chan error {
	errCh := make(chan error, 1)
	runtime := cfg.RuntimeSettings()
	onLifecycle := lifecycleSignal(terminal)
	switch cfg.Protocol {
	case "udp":
		go func() { errCh <- dp.StartDataPlaneServeIncomingUDPContext(ctx, incoming, runtime, nil, onLifecycle) }()
		log.Printf("INFO: UDP expose-local mode active; backend target %s", cfg.TargetAddr)
	default:
		reporter := dp.NewBackendStateReporter(cfg.Protocol)
		go func() { errCh <- dp.StartDataPlaneServeIncomingContext(ctx, incoming, runtime, reporter, onLifecycle) }()
	}
	fmt.Printf("💡 If the local %s backend is unavailable, the tunnel remains active; later incoming traffic retries it.\n", strings.ToUpper(cfg.Protocol))
	fmt.Printf("\n🔌 Serving %s over data-plane. Press Ctrl+C to stop.\n", strings.ToUpper(cfg.Protocol))
	return errCh
}

func handleUDPReverseMode(
	ctx context.Context,
	terminal *ctrl.TerminalCoordinator,
	strategy dp.Strategy,
	serverURL, tunnelID string,
	httpClient *http.Client,
	bearer, csrf string,
) error {
	errCh := make(chan error, 1)
	fmt.Print(strategy.Description)
	fmt.Println("\n🔌 Press Ctrl+C to stop.")
	go func() {
		errCh <- runUDPStrategyContext(ctx, strategy, serverURL, tunnelID, httpClient, bearer, csrf)
	}()
	return waitForProtocol(ctx, terminal, errCh, serveExit{onInterrupt: func() {
		ctrl.DeleteTunnelWithClient(ctx, serverURL, tunnelID, httpClient, bearer, csrf)
	}})
}

func waitForDataPlaneServe(
	ctx context.Context,
	terminal *ctrl.TerminalCoordinator,
	errCh <-chan error,
	serverURL, tunnelID string,
	httpClient *http.Client,
	bearer, csrf string,
) error {
	deleteTunnel := func() { ctrl.DeleteTunnelWithClient(ctx, serverURL, tunnelID, httpClient, bearer, csrf) }
	return waitForProtocol(ctx, terminal, errCh, serveExit{
		onError: func(err error) error {
			if err == nil {
				return nil
			}
			deleteTunnel()
			return fmt.Errorf("❌ Data-plane serve stopped: %w", err)
		},
		onInterrupt: deleteTunnel,
	})
}

// serveExit are the ways a serve loop ends besides a terminal lifecycle event.
type serveExit struct {
	onError     func(error) error // maps a serve-loop failure to the CLI result
	onInterrupt func()            // runs when the user interrupts, e.g. to delete the tunnel
}

// waitForProtocol blocks until the serve loop fails, the server ends the
// tunnel, or ctx is canceled by a user interrupt. A user interrupt runs
// exit.onInterrupt so a tunnel the CLI created is not left behind on the
// server; a server-initiated end does not, since the tunnel is already gone.
func waitForProtocol(ctx context.Context, terminal *ctrl.TerminalCoordinator, errCh <-chan error, exit serveExit) error {
	select {
	case <-ctx.Done():
		return finishInterrupted(terminal, exit)
	case <-terminal.Done():
		return terminal.ExitError()
	case err := <-errCh:
		if terminal.Err() != nil {
			return terminal.ExitError()
		}
		if ctx.Err() != nil {
			// The serve loop stopped because the user interrupted it.
			return finishInterrupted(terminal, exit)
		}
		if exit.onError != nil {
			return exit.onError(err)
		}
		return err
	}
}

func finishInterrupted(terminal *ctrl.TerminalCoordinator, exit serveExit) error {
	if terminal.Err() != nil {
		return terminal.ExitError()
	}
	if exit.onInterrupt != nil {
		exit.onInterrupt()
	}
	return nil
}

func lifecycleSignal(terminal *ctrl.TerminalCoordinator) func(protocolv1.LifecycleEventPayload) {
	return func(payload protocolv1.LifecycleEventPayload) {
		err := ctrl.TrafficQuotaErrorFromLifecycle(payload)
		terminal.Signal(err)
	}
}

func isHTTPProtocol(value string) bool {
	return value == protoHTTP || value == protoHTTPS
}

func ensureHTTPHasTarget(cfg *config.Config) error {
	if isHTTPProtocol(cfg.Protocol) && cfg.TargetAddr == "" {
		return fmt.Errorf("target address is required (e.g. 127.0.0.1:8000)")
	}
	return nil
}

func ensureTCPHasTarget(cfg *config.Config) error {
	if !strings.EqualFold(cfg.Protocol, "tcp") {
		return nil
	}
	if cfg.TargetAddr == "" {
		return fmt.Errorf("target address is required for TCP expose-local mode (e.g. 127.0.0.1:5433)")
	}
	return nil
}

func ensureUDPHasTarget(cfg *config.Config) error {
	if !config.IsUDPExposeLocalMode(cfg) {
		return nil
	}
	if cfg.TargetAddr == "" {
		return fmt.Errorf("target address is required for UDP expose-local mode (e.g. 127.0.0.1:9000)")
	}
	return nil
}

// --- UDP strategy helpers ----------------------------------------------------

func runUDPStrategyContext(ctx context.Context, strategy dp.Strategy, serverURL, tunnelID string, httpClient *http.Client, bearer, csrf string) error {
	fmt.Println(strategy.RunningMessage)
	if err := strategy.RunContext(ctx); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ctrl.DeleteTunnelWithClient(ctx, serverURL, tunnelID, httpClient, bearer, csrf)
		return fmt.Errorf("%s: %w", strategy.ErrLabel, err)
	}
	return nil
}
