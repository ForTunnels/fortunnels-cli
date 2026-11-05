// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"
)

// quicUDPStreamFrameVectors are shared byte-for-byte with the server's
// quic_udp_stream_test.go: both ends must agree on the wire format.
var quicUDPStreamFrameVectors = []struct {
	name    string
	flowID  string
	payload []byte
	wire    string
}{
	{name: "one byte", flowID: "a", payload: []byte{0x01}, wire: "01" + "61" + "0001" + "01"},
	{name: "ipv4 source", flowID: "127.0.0.1:5353", payload: []byte("ping"), wire: "0e" + "3132372e302e302e313a35333533" + "0004" + "70696e67"},
	{name: "ipv6 source", flowID: "[::1]:40000", payload: []byte{0x00, 0xff}, wire: "0b" + "5b3a3a315d3a3430303030" + "0002" + "00ff"},
}

func TestQUICUDPStreamFrame_ByteVectors(t *testing.T) {
	var stream []byte
	for _, vector := range quicUDPStreamFrameVectors {
		wire, err := hex.DecodeString(vector.wire)
		require.NoError(t, err)
		encoded, err := appendQUICUDPStreamFrame(nil, vector.flowID, vector.payload)
		require.NoError(t, err, vector.name)
		require.Equal(t, wire, encoded, vector.name)
		stream = append(stream, wire...)
	}
	frames := quicUDPStreamFrameReader{r: bytes.NewReader(stream)}
	for _, vector := range quicUDPStreamFrameVectors {
		flowID, payload, err := frames.next()
		require.NoError(t, err, vector.name)
		require.Equal(t, vector.flowID, flowID, vector.name)
		require.Equal(t, vector.payload, payload, vector.name)
	}
	_, _, err := frames.next()
	require.ErrorIs(t, err, io.EOF, "a clean end of stream falls on a frame boundary")

	longest, err := appendQUICUDPStreamFrame(nil, strings.Repeat("f", 255), bytes.Repeat([]byte{0x5a}, 65535))
	require.NoError(t, err)
	require.Len(t, longest, 1+255+2+65535)
	require.Equal(t, byte(0xff), longest[0])
	require.Equal(t, []byte{0xff, 0xff}, longest[256:258])
}

func TestQUICUDPStreamFrame_RejectsInvalidFrames(t *testing.T) {
	for name, frame := range map[string]struct {
		flowID  string
		payload []byte
	}{
		"empty flow id":     {flowID: "", payload: []byte("x")},
		"flow id too long":  {flowID: strings.Repeat("f", 256), payload: []byte("x")},
		"empty payload":     {flowID: "a", payload: nil},
		"payload too large": {flowID: "a", payload: make([]byte, 65536)},
	} {
		_, err := appendQUICUDPStreamFrame(nil, frame.flowID, frame.payload)
		require.ErrorIs(t, err, errQUICUDPStreamFrame, name)
	}
	for name, wire := range map[string]string{
		"zero flow id length": "00",
		"zero payload length": "01" + "61" + "0000",
	} {
		raw, err := hex.DecodeString(wire)
		require.NoError(t, err)
		frames := quicUDPStreamFrameReader{r: bytes.NewReader(raw)}
		_, _, err = frames.next()
		require.ErrorIs(t, err, errQUICUDPStreamFrame, name)
	}
	valid, err := hex.DecodeString(quicUDPStreamFrameVectors[1].wire)
	require.NoError(t, err)
	for cut := 1; cut < len(valid); cut++ {
		frames := quicUDPStreamFrameReader{r: bytes.NewReader(valid[:cut])}
		_, _, err = frames.next()
		require.ErrorIs(t, err, io.ErrUnexpectedEOF, "frame truncated after %d bytes", cut)
	}
}

// pipeQUICStream gives one end of net.Pipe the quic.Stream surface the
// fallback uses.
type pipeQUICStream struct {
	net.Conn
}

func (s pipeQUICStream) CancelRead(quic.StreamErrorCode) { _ = s.SetReadDeadline(time.Now()) }

// fakeUDPStreamServer hands every stream the client opens to the test.
type fakeUDPStreamServer struct {
	opens   atomic.Int32
	streams chan net.Conn
}

func (s *fakeUDPStreamServer) open(context.Context) (quicUDPByteStream, error) {
	s.opens.Add(1)
	client, server := net.Pipe()
	s.streams <- server
	return pipeQUICStream{Conn: client}, nil
}

func (s *fakeUDPStreamServer) accept(t *testing.T) (net.Conn, *bufio.Reader) {
	t.Helper()
	select {
	case server := <-s.streams:
		t.Cleanup(func() { _ = server.Close() })
		require.NoError(t, server.SetDeadline(time.Now().Add(5*time.Second)))
		return server, bufio.NewReader(server)
	case <-time.After(2 * time.Second):
		t.Fatal("the client did not open a udp stream")
		return nil, nil
	}
}

// limitedDatagramConn accepts DATAGRAM frames of at most limit bytes, like a
// QUIC connection whose packet size caps the frame.
type limitedDatagramConn struct {
	limit int
	sent  chan []byte
}

func (c *limitedDatagramConn) SendDatagram(b []byte) error {
	if len(b) > c.limit {
		return &quic.DatagramTooLargeError{MaxDatagramPayloadSize: int64(c.limit)}
	}
	c.sent <- append([]byte(nil), b...)
	return nil
}

func (*limitedDatagramConn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type udpStreamForwardHarness struct {
	peer   *net.UDPConn
	conn   *limitedDatagramConn
	server *fakeUDPStreamServer
	done   chan error
}

func startUDPStreamForwardHarness(t *testing.T) *udpStreamForwardHarness {
	t.Helper()
	uc, err := listenTestUDP()
	require.NoError(t, err)
	peer, err := net.DialUDP("udp", nil, uc.LocalAddr().(*net.UDPAddr))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	h := &udpStreamForwardHarness{
		peer:   peer,
		conn:   &limitedDatagramConn{limit: 600, sent: make(chan []byte, 16)},
		server: &fakeUDPStreamServer{streams: make(chan net.Conn, 4)},
		done:   make(chan error, 1),
	}
	flows := newFlowRegistry()
	fallback := newQUICUDPStreamFallback(ctx, h.server.open, uc, flows, "t1", "secret-auth", "127.0.0.1:53")
	go func() {
		h.done <- forwardUDPPacketsOverQUIC(ctx, cancel, h.conn, uc, "t1", "secret-auth", "127.0.0.1:53", flows, fallback)
	}()
	t.Cleanup(func() {
		cancel()
		_ = uc.Close()
		select {
		case err := <-h.done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Error("forward loop did not stop")
		}
		fallback.close()
		_ = peer.Close()
	})
	return h
}

func (h *udpStreamForwardHarness) send(t *testing.T, payload []byte) {
	t.Helper()
	_, err := h.peer.Write(payload)
	require.NoError(t, err)
}

// requireDatagram waits for the next DATAGRAM frame and checks its payload.
func (h *udpStreamForwardHarness) requireDatagram(t *testing.T, want []byte) {
	t.Helper()
	select {
	case frame := <-h.conn.sent:
		var decoded struct {
			Data []byte `json:"data"`
		}
		require.NoError(t, json.Unmarshal(frame, &decoded))
		require.Equal(t, want, decoded.Data)
	case <-time.After(2 * time.Second):
		t.Fatal("datagram was not sent")
	}
}

func (h *udpStreamForwardHarness) requireRunning(t *testing.T) {
	t.Helper()
	select {
	case err := <-h.done:
		t.Fatalf("forward loop ended: %v", err)
	default:
	}
}

// writeTestUDPStreamFrame and readTestUDPStreamFrame encode the wire format
// independently of the code under test.
func writeTestUDPStreamFrame(t *testing.T, w io.Writer, flowID string, payload []byte) {
	t.Helper()
	frame := []byte{byte(len(flowID))}
	frame = append(frame, flowID...)
	frame = append(frame, byte(len(payload)>>8), byte(len(payload)))
	frame = append(frame, payload...)
	_, err := w.Write(frame)
	require.NoError(t, err)
}

func readTestUDPStreamFrame(t *testing.T, r io.Reader) (flowID string, payload []byte) {
	t.Helper()
	var idLen [1]byte
	_, err := io.ReadFull(r, idLen[:])
	require.NoError(t, err)
	id := make([]byte, idLen[0])
	_, err = io.ReadFull(r, id)
	require.NoError(t, err)
	var size [2]byte
	_, err = io.ReadFull(r, size[:])
	require.NoError(t, err)
	payload = make([]byte, int(size[0])<<8|int(size[1]))
	_, err = io.ReadFull(r, payload)
	require.NoError(t, err)
	return string(id), payload
}

func requireUDPStreamHandshake(t *testing.T, reader *bufio.Reader) {
	t.Helper()
	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	var handshake map[string]string
	require.NoError(t, json.Unmarshal([]byte(line), &handshake))
	require.Equal(t, map[string]string{
		"auth": "secret-auth", "tunnel_id": "t1", "kind": "udp", "dst": "127.0.0.1:53",
	}, handshake)
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previous) })
	return &logs
}

func shortenUDPStreamAckTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	previous := quicUDPStreamAckTimeout
	quicUDPStreamAckTimeout = timeout
	t.Cleanup(func() { quicUDPStreamAckTimeout = previous })
}

// C6: a datagram too large for one DATAGRAM frame switches to the udp stream
// once the server acks it; replies on the stream reach the right local source.
func TestForwardUDPPacketsOverQUIC_OversizeSwitchesToAckedUDPStream(t *testing.T) {
	h := startUDPStreamForwardHarness(t)
	oversized := bytes.Repeat([]byte("A"), 1000)
	h.send(t, oversized)
	server, reader := h.server.accept(t)
	requireUDPStreamHandshake(t, reader)
	_, err := server.Write([]byte(`{"udp_stream":"v1"}` + "\n"))
	require.NoError(t, err)

	flowID, payload := readTestUDPStreamFrame(t, reader)
	require.Equal(t, h.peer.LocalAddr().String(), flowID)
	require.Equal(t, oversized, payload)

	reply := bytes.Repeat([]byte("B"), 1200)
	writeTestUDPStreamFrame(t, server, flowID, reply)
	require.NoError(t, h.peer.SetReadDeadline(time.Now().Add(2*time.Second)))
	buf := make([]byte, 2048)
	n, err := h.peer.Read(buf)
	require.NoError(t, err)
	require.Equal(t, reply, buf[:n])

	later := bytes.Repeat([]byte("C"), 900)
	h.send(t, later)
	_, payload = readTestUDPStreamFrame(t, reader)
	require.Equal(t, later, payload, "later oversized datagrams reuse the stream")
	require.EqualValues(t, 1, h.server.opens.Load(), "one udp stream per connection")

	h.send(t, []byte("small"))
	h.requireDatagram(t, []byte("small"))
	h.requireRunning(t)
}

// C6 compatibility: an old server never acks (it waits for a TCP destination
// line). The client closes the stream without sending frame bytes and keeps
// dropping oversized datagrams for this connection.
func TestForwardUDPPacketsOverQUIC_UnackedUDPStreamFallsBackToDrop(t *testing.T) {
	shortenUDPStreamAckTimeout(t, 150*time.Millisecond)
	logs := captureLogs(t)
	h := startUDPStreamForwardHarness(t)
	h.send(t, bytes.Repeat([]byte("A"), 1000))
	_, reader := h.server.accept(t)
	requireUDPStreamHandshake(t, reader)
	rest, err := io.ReadAll(reader)
	require.NoError(t, err, "the client must close the un-acked stream")
	require.Empty(t, rest, "no frame bytes may follow the handshake on an un-acked stream")

	h.send(t, bytes.Repeat([]byte("D"), 1000))
	h.send(t, []byte("small"))
	h.requireDatagram(t, []byte("small"))
	require.EqualValues(t, 1, h.server.opens.Load(), "an unsupported fallback is not retried on this connection")
	require.Contains(t, logs.String(), "QUIC datagram dropped")
	require.Contains(t, logs.String(), "dropped=1\n")
	require.NotContains(t, logs.String(), "secret-auth")
	h.requireRunning(t)
}

// C6: a malformed server frame ends the stream reader without a panic; the
// client stops using the stream and drops later oversized datagrams.
func TestForwardUDPPacketsOverQUIC_MalformedUDPStreamFrameEndsStream(t *testing.T) {
	logs := captureLogs(t)
	h := startUDPStreamForwardHarness(t)
	h.send(t, bytes.Repeat([]byte("A"), 1000))
	server, reader := h.server.accept(t)
	requireUDPStreamHandshake(t, reader)
	_, err := server.Write([]byte(`{"udp_stream":"v1"}` + "\n"))
	require.NoError(t, err)
	_, _ = readTestUDPStreamFrame(t, reader)

	_, err = server.Write([]byte{0x00}) // zero flow ID length
	require.NoError(t, err)
	rest, err := io.ReadAll(reader)
	require.NoError(t, err, "the client must close the stream after a malformed frame")
	require.Empty(t, rest)

	h.send(t, bytes.Repeat([]byte("E"), 1000))
	h.send(t, []byte("small"))
	h.requireDatagram(t, []byte("small"))
	require.EqualValues(t, 1, h.server.opens.Load())
	require.Contains(t, logs.String(), "QUIC datagram dropped")
	h.requireRunning(t)
}
