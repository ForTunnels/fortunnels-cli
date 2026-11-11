// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fortunnels/client/internal/config"
	"github.com/fortunnels/client/internal/support"
	protocolv1 "github.com/fortunnels/client/shared/protocol/v1"
)

// BackendStateReporter is called on backend dial success/failure for CLI transition messages.
// Success means TCP dial reached the target; it does not imply HTTP/TLS readiness.
// If nil, no reporting is done.
type BackendStateReporter func(dst string, err error)

const backendDialTimeout = 5 * time.Second

var (
	backendDialer        = net.Dialer{Timeout: backendDialTimeout}
	dialBackend          = backendDialer.Dial
	halfCloseIdleTimeout = 60 * time.Second
)

// NewBackendStateReporter returns a reporter that prints one-time messages on backend dial
// down/up transitions for the requested protocol. Messages reflect transport-level
// reachability only, not full proxy readiness.
func NewBackendStateReporter(protocol string) BackendStateReporter {
	return newBackendStateReporter(protocol, os.Stdout)
}

func newBackendStateReporter(protocol string, output io.Writer) BackendStateReporter {
	if output == nil {
		output = io.Discard
	}
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	var mu sync.Mutex
	state := make(map[string]bool) // dst -> wasDown
	return func(dst string, err error) {
		mu.Lock()
		defer mu.Unlock()
		wasDown := state[dst]
		if err != nil {
			if !wasDown {
				fmt.Fprintf(
					output,
					"⚠️  Backend unreachable for %s (%s) — tunnel remains active; %s. Start the local backend on this target.\n",
					dst,
					backendProtocolLabel(protocol),
					backendRetryGuidance(protocol),
				)
			}
			state[dst] = true
		} else {
			if wasDown {
				fmt.Fprintf(output, "✅ Backend reachable for %s (%s); forwarding new traffic.\n", dst, backendProtocolLabel(protocol))
			}
			state[dst] = false
		}
	}
}

func backendProtocolLabel(protocol string) string {
	if protocol == "" {
		return "TCP"
	}
	return strings.ToUpper(protocol)
}

func backendRetryGuidance(protocol string) string {
	switch protocol {
	case "udp":
		return "the next incoming datagram retries this UDP backend"
	case "tcp":
		return "the next incoming connection retries this TCP backend"
	case "https":
		return "the next incoming HTTPS request retries this backend"
	default:
		return "the next incoming HTTP request retries this backend"
	}
}

// IncomingTunnel identifies the tunnel an expose-local mode serves and how its
// data-plane session reconnects.
type IncomingTunnel struct {
	ServerURL string
	TunnelID  string
	AuthToken string // data-plane auth token
	Backoff   config.ReconnectBackoff
	// Target is the configured local backend; streams may only name its host.
	Target string
	// Encryption is the PSK layer applied to the payload of incoming streams.
	Encryption config.EncryptionSettings
}

func StartDataPlaneServeIncoming(serverURL, tunnelID string, runtime config.RuntimeSettings, reporter BackendStateReporter, dpAuthToken string) error {
	tunnel := IncomingTunnel{ServerURL: serverURL, TunnelID: tunnelID, AuthToken: dpAuthToken}
	return StartDataPlaneServeIncomingContext(context.Background(), &tunnel, runtime, reporter, nil)
}

func StartDataPlaneServeIncomingContext(
	ctx context.Context,
	tunnel *IncomingTunnel,
	runtime config.RuntimeSettings,
	reporter BackendStateReporter,
	onLifecycle func(protocolv1.LifecycleEventPayload),
) error {
	return startIncomingDataPlaneContext(ctx, tunnel, runtime, reporter, onLifecycle, serveIncomingStream, "incoming stream error")
}

type incomingStreamHandler func(io.ReadWriteCloser, incomingPolicy) error

func startIncomingDataPlaneContext(
	ctx context.Context,
	tunnel *IncomingTunnel,
	runtime config.RuntimeSettings,
	reporter BackendStateReporter,
	onLifecycle func(protocolv1.LifecycleEventPayload),
	handleStream incomingStreamHandler,
	errorLabel string,
) error {
	mgr := newIncomingDataPlaneManager(tunnel, runtime)
	mgr.SetLifecycleHandler(onLifecycle)
	defer mgr.Close()
	stopCloser := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			mgr.Close()
		case <-stopCloser:
		}
	}()
	defer close(stopCloser)
	policy := incomingPolicy{reporter: reporter, tunnelID: tunnel.TunnelID, target: tunnel.Target, enc: tunnel.Encryption}
	loop := incomingStreamLoop{
		sessions: mgr,
		retry:    newReconnectBackoff(mgr.bounds.init, mgr.bounds.max),
		wait:     waitRetryDelay,
		dispatch: func(s io.ReadWriteCloser) {
			go func() {
				if err := handleStream(s, policy); err != nil && !support.IsBenignCopyError(err) {
					log.Printf("%s: %v", errorLabel, err)
				}
			}()
		},
	}
	return loop.run(ctx)
}

// incomingSessionSource supplies live data-plane sessions to the incoming-stream loop.
type incomingSessionSource interface {
	EnsureSessionContext(ctx context.Context) (dataPlaneSession, error)
	resetSession()
}

// incomingStreamLoop accepts server-opened streams on the current session and
// re-establishes the session when accepting fails. A server can accept the
// WebSocket and then reject the session, so the redial delay keeps growing
// across sessions until one of them accepts a stream.
type incomingStreamLoop struct {
	sessions incomingSessionSource
	retry    *reconnectBackoff
	wait     retryWaiter
	dispatch func(io.ReadWriteCloser)
}

func (l incomingStreamLoop) run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		sess, err := l.sessions.EnsureSessionContext(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		st, err := sess.AcceptStream()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			l.sessions.resetSession()
			if waitErr := l.wait(ctx, l.retry.Next()); waitErr != nil {
				return waitErr
			}
			continue
		}
		l.retry.Reset()
		l.dispatch(st)
	}
}

// newIncomingDataPlaneManager builds the session manager for the expose-local
// HTTP, TCP and UDP modes from the configured --backoff-initial/--backoff-max.
func newIncomingDataPlaneManager(tunnel *IncomingTunnel, runtime config.RuntimeSettings) *Manager {
	return NewManager(tunnel.ServerURL, tunnel.TunnelID, tunnel.AuthToken, tunnel.Backoff.Initial, tunnel.Backoff.Max, runtime)
}

// setupAck and setupError are JSON lines sent to the server for proxy error classification.
const setupAckLine = `{"ok":true}` + "\n"

func writeSetupError(stream io.Writer, err error) {
	payload := map[string]interface{}{"ok": false, "error": err.Error()}
	if b, e := json.Marshal(payload); e == nil {
		if _, wErr := stream.Write(append(b, '\n')); wErr != nil {
			log.Printf("writeSetupError: %v", wErr)
		}
	}
}

func serveIncomingStream(stream io.ReadWriteCloser, policy incomingPolicy) error {
	defer stream.Close()
	rd := bufio.NewReader(stream)
	_, dst, err := readPrefaceBounded(stream, rd, policy.prefaceWait())
	if err != nil {
		return err
	}
	if dst == "" {
		return fmt.Errorf("stream preface missing or empty dst")
	}
	if !destinationAllowed(policy.target, dst) {
		writeSetupError(stream, errDestinationNotAllowed)
		return fmt.Errorf("%w: %q", errDestinationNotAllowed, dst)
	}
	bc, err := dialBackend("tcp", dst)
	if err != nil {
		if policy.reporter != nil {
			policy.reporter(dst, err)
		}
		writeSetupError(stream, err)
		return err
	}
	defer bc.Close()

	if policy.reporter != nil {
		policy.reporter(dst, nil)
	}
	if _, err := stream.Write([]byte(setupAckLine)); err != nil {
		return err
	}
	return bridgeIncoming(stream, rd, bc, policy)
}

// bridgeIncoming relays the payload after the setup ack. With --encrypt the
// payload is PSK-wrapped like the server's side of the stream; the preface and
// the ack stay plaintext.
func bridgeIncoming(stream io.ReadWriteCloser, rd *bufio.Reader, backend net.Conn, policy incomingPolicy) error {
	if policy.enc.Enabled {
		secured, err := policy.secureStream(stream, rd)
		if err != nil {
			return err
		}
		return bridgeStreamAndBackend(secured, secured, backend)
	}
	if err := flushBufferedBytes(rd, backend); err != nil {
		return err
	}
	return bridgeStreamAndBackend(stream, rd, backend)
}

func bridgeStreamAndBackend(stream io.ReadWriteCloser, streamReader io.Reader, backendConn net.Conn) error {
	type copyResult struct {
		err    error
		reader *idleDeadlineReader
	}
	errCh := make(chan copyResult, 2)
	backendReader := &idleDeadlineReader{reader: backendConn, deadline: backendConn, timeout: halfCloseIdleTimeout}
	requestReader := &idleDeadlineReader{reader: streamReader, timeout: halfCloseIdleTimeout}
	if deadline, ok := stream.(readDeadlineSetter); ok {
		requestReader.deadline = deadline
	}

	go func() {
		_, err := io.Copy(stream, backendReader)
		// Propagate response EOF to the server-side proxy. Without this, HTTP/1.0
		// responses without Content-Length can hang until client timeout.
		closeWriteOrClose(stream)
		if err != nil && !isBenignBridgeCopyError(err) {
			errCh <- copyResult{err: err, reader: backendReader}
			return
		}
		errCh <- copyResult{reader: backendReader}
	}()

	go func() {
		_, err := io.Copy(backendConn, requestReader)
		closeWriteIfPossible(backendConn)
		if err != nil && !isBenignBridgeCopyError(err) {
			errCh <- copyResult{err: err, reader: requestReader}
			return
		}
		errCh <- copyResult{reader: requestReader}
	}()

	firstResult := <-errCh
	first := firstResult.err
	// Both copies already have a pending reader. Activating also sets the
	// deadline now so a read blocked before half-close has a bounded idle wait.
	remainingReader := backendReader
	if firstResult.reader == backendReader {
		remainingReader = requestReader
	}
	if deadlineErr := remainingReader.activate(); deadlineErr != nil && !isBenignBridgeCopyError(deadlineErr) {
		// A failed deadline must not leave the remaining reader blocked forever.
		_ = stream.Close()
		_ = backendConn.Close()
		if first == nil {
			first = deadlineErr
		}
	}
	second := (<-errCh).err
	if first != nil {
		if second != nil && !support.IsBenignCopyError(second) {
			log.Printf("bridgeStreamAndBackend secondary error: %v", second)
		}
		return first
	}
	if second != nil && !support.IsBenignCopyError(second) {
		log.Printf("bridgeStreamAndBackend secondary error: %v", second)
	}
	return second
}

type readDeadlineSetter interface{ SetReadDeadline(time.Time) error }

type idleDeadlineReader struct {
	reader   io.Reader
	deadline readDeadlineSetter
	timeout  time.Duration
	active   atomic.Bool
}

func (r *idleDeadlineReader) activate() error {
	r.active.Store(true)
	if r.deadline != nil {
		return r.deadline.SetReadDeadline(time.Now().Add(r.timeout))
	}
	return nil
}

func (r *idleDeadlineReader) Read(p []byte) (int, error) {
	if r.active.Load() && r.deadline != nil {
		if err := r.deadline.SetReadDeadline(time.Now().Add(r.timeout)); err != nil {
			return 0, err
		}
	}
	return r.reader.Read(p)
}

func isBenignBridgeCopyError(err error) bool {
	return support.IsBenignCopyError(err) || os.IsTimeout(err)
}

func closeWriteIfPossible(c net.Conn) {
	type closeWriter interface{ CloseWrite() error }
	if cw, ok := c.(closeWriter); ok {
		if err := cw.CloseWrite(); err != nil {
			log.Printf("closeWriteIfPossible: %v", err)
		}
	}
}

func closeWriteOrClose(stream io.ReadWriteCloser) {
	type closeWriter interface{ CloseWrite() error }
	if cw, ok := stream.(closeWriter); ok {
		if err := cw.CloseWrite(); err != nil {
			log.Printf("closeWriteOrClose: %v", err)
		}
		return
	}
	_ = stream.Close()
}

// flushBufferedBytes forwards only bytes already buffered in rd without blocking.
// This prevents a deadlock where io.Copy waits for stream close before bridge startup.
// The caller must not read from rd concurrently while this runs.
func flushBufferedBytes(rd *bufio.Reader, dst io.Writer) error {
	buffered := rd.Buffered()
	if buffered == 0 {
		return nil
	}
	buf := make([]byte, buffered)
	if _, err := io.ReadFull(rd, buf); err != nil {
		return err
	}
	for len(buf) > 0 {
		n, err := dst.Write(buf)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		buf = buf[n:]
	}
	return nil
}

func readStreamDestination(rd *bufio.Reader) (string, error) {
	_, dst, err := readStreamPreface(rd)
	return dst, err
}

func readStreamPreface(rd *bufio.Reader) (proto, dst string, err error) {
	for {
		line, readErr := readBoundedLine(rd)
		if readErr != nil {
			return "", "", readErr
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var pre map[string]string
		if unmarshalErr := json.Unmarshal([]byte(line), &pre); unmarshalErr != nil {
			return "", "", unmarshalErr
		}
		return pre["proto"], pre["dst"], nil
	}
}
