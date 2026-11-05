// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/xtaci/smux"

	"github.com/fortunnels/client/internal/config"
	"github.com/fortunnels/client/shared/wsconn"
)

// testFlowFrame is one decoded flow v1 frame; flowID 0 marks a legacy frame.
type testFlowFrame struct {
	flowID  uint32
	payload []byte
}

// Test-local codecs, independent of the production encoder under test.
func testEncodeFlowFrame(flowID uint32, payload []byte) []byte {
	size := len(payload)
	if size > math.MaxUint16 {
		panic("test payload too large")
	}
	frame := make([]byte, 6+size)
	binary.BigEndian.PutUint16(frame[:2], uint16(size))
	binary.BigEndian.PutUint32(frame[2:6], flowID)
	copy(frame[6:], payload)
	return frame
}

func testEncodeLegacyFrame(payload []byte) []byte {
	size := len(payload)
	if size > math.MaxUint16 {
		panic("test payload too large")
	}
	frame := make([]byte, 2+size)
	binary.BigEndian.PutUint16(frame[:2], uint16(size))
	copy(frame[2:], payload)
	return frame
}

type wsFlowPeer struct {
	r *bufio.Reader
	w io.Writer
}

type wsFlowFixture struct {
	prefaces chan map[string]string
	peers    chan wsFlowPeer
}

// startWSFlowFixture serves one reverse UDP data-plane session per dial and
// hands the test its preface and stream. advertise controls the feature header.
func startWSFlowFixture(t *testing.T, advertise bool) (*httptest.Server, *wsFlowFixture) {
	t.Helper()
	fx := &wsFlowFixture{prefaces: make(chan map[string]string, 4), peers: make(chan wsFlowPeer, 4)}
	stop := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := http.Header{}
		if advertise {
			header.Set("X-Fortunnels-Dataplane-Features", "udp-flow-v1")
		}
		conn, err := upgrader.Upgrade(w, r, header)
		if err != nil {
			return
		}
		defer conn.Close()
		session, err := smux.Server(wsconn.NewWSConn(conn), smux.DefaultConfig())
		if err != nil {
			return
		}
		defer session.Close()
		stream, err := session.AcceptStream()
		if err != nil {
			return
		}
		defer stream.Close()
		reader := bufio.NewReader(stream)
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		preface := map[string]string{}
		if json.Unmarshal(bytes.TrimSpace(line), &preface) != nil {
			return
		}
		fx.prefaces <- preface
		fx.peers <- wsFlowPeer{r: reader, w: stream}
		<-stop
	}))
	t.Cleanup(func() {
		close(stop)
		server.CloseClientConnections()
		server.Close()
	})
	return server, fx
}

func testUDPRuntime() config.RuntimeSettings {
	return config.RuntimeSettings{
		PingInterval:          30 * time.Second,
		PingTimeout:           5 * time.Second,
		SmuxKeepAliveInterval: 30 * time.Second,
		SmuxKeepAliveTimeout:  90 * time.Second,
	}
}

func freeLocalUDPAddr(t *testing.T) string {
	t.Helper()
	probe, err := net.ListenUDP(udpTransport, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	addr := probe.LocalAddr().String()
	require.NoError(t, probe.Close())
	return addr
}

// runReverseUDPClient starts StartDataPlaneUDPContext and stops it at cleanup.
func runReverseUDPClient(t *testing.T, serverURL string) string {
	t.Helper()
	listenAddr := freeLocalUDPAddr(t)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- StartDataPlaneUDPContext(ctx, serverURL, "t-flow", "127.0.0.1:9", listenAddr, testUDPRuntime(), config.EncryptionSettings{}, "tok")
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("reverse UDP client did not stop")
		}
	})
	return listenAddr
}

func awaitPreface(t *testing.T, fx *wsFlowFixture) (map[string]string, wsFlowPeer) {
	t.Helper()
	select {
	case preface := <-fx.prefaces:
		return preface, <-fx.peers
	case <-time.After(5 * time.Second):
		t.Fatal("client never opened a reverse UDP stream")
		return nil, wsFlowPeer{}
	}
}

func readTestFrames(r io.Reader, flow bool) <-chan testFlowFrame {
	frames := make(chan testFlowFrame, 64)
	go func() {
		defer close(frames)
		for {
			hdrLen := 2
			if flow {
				hdrLen = 6
			}
			hdr := make([]byte, hdrLen)
			if _, err := io.ReadFull(r, hdr); err != nil {
				return
			}
			frame := testFlowFrame{payload: make([]byte, binary.BigEndian.Uint16(hdr[:2]))}
			if flow {
				frame.flowID = binary.BigEndian.Uint32(hdr[2:6])
			}
			if _, err := io.ReadFull(r, frame.payload); err != nil {
				return
			}
			frames <- frame
		}
	}()
	return frames
}

func localUDPSender(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP(udpTransport, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// sendUntilFrame resends payload from sender until the fake server sees it.
func sendUntilFrame(t *testing.T, sender *net.UDPConn, to string, payload string, frames <-chan testFlowFrame) testFlowFrame {
	t.Helper()
	target := mustResolveUDPAddr(t, to)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, err := sender.WriteToUDP([]byte(payload), target)
		require.NoError(t, err)
		timeout := time.After(200 * time.Millisecond)
	wait:
		for {
			select {
			case frame, ok := <-frames:
				require.True(t, ok, "server stream ended")
				if string(frame.payload) == payload {
					return frame
				}
			case <-timeout:
				break wait
			}
		}
	}
	t.Fatalf("server never received %q", payload)
	return testFlowFrame{}
}

func expectUDPReply(t *testing.T, conn *net.UDPConn, want string) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	buf := make([]byte, 256)
	n, _, err := conn.ReadFromUDP(buf)
	require.NoError(t, err, "no reply for %q", want)
	require.Equal(t, want, string(buf[:n]))
}

func expectNoUDPReply(t *testing.T, conn *net.UDPConn) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(150*time.Millisecond)))
	buf := make([]byte, 256)
	n, _, err := conn.ReadFromUDP(buf)
	require.Error(t, err, "unexpected datagram %q", buf[:n])
}

func TestReverseUDPWS_FlowFramingRoutesRepliesToEachSource(t *testing.T) {
	server, fx := startWSFlowFixture(t, true)
	listenAddr := runReverseUDPClient(t, server.URL)
	preface, peer := awaitPreface(t, fx)
	require.Equal(t, "flow", preface["framing"], "an advertising server must be asked for flow framing")
	require.Equal(t, "udp", preface["proto"])
	frames := readTestFrames(peer.r, true)

	a, b := localUDPSender(t), localUDPSender(t)
	fromA := sendUntilFrame(t, a, listenAddr, "a-hello", frames)
	fromB := sendUntilFrame(t, b, listenAddr, "b-hello", frames)
	require.NotZero(t, fromA.flowID)
	require.NotZero(t, fromB.flowID)
	require.NotEqual(t, fromA.flowID, fromB.flowID, "each local source is its own flow")

	// Replies arrive after both sources sent: the last sender must not get both.
	_, err := peer.w.Write(testEncodeFlowFrame(fromA.flowID, []byte("re:a-hello")))
	require.NoError(t, err)
	_, err = peer.w.Write(testEncodeFlowFrame(fromB.flowID, []byte("re:b-hello")))
	require.NoError(t, err)
	_, err = peer.w.Write(testEncodeFlowFrame(fromB.flowID+1000, []byte("unknown flow")))
	require.NoError(t, err)

	expectUDPReply(t, a, "re:a-hello")
	expectUDPReply(t, b, "re:b-hello")
	expectNoUDPReply(t, a)
	expectNoUDPReply(t, b)
}

func TestReverseUDPWS_ServerWithoutFeatureHeaderKeepsLegacyFraming(t *testing.T) {
	server, fx := startWSFlowFixture(t, false)
	listenAddr := runReverseUDPClient(t, server.URL)
	preface, peer := awaitPreface(t, fx)
	_, hasFraming := preface["framing"]
	require.False(t, hasFraming, "an old server must get today's preface")
	frames := readTestFrames(peer.r, false)

	a := localUDPSender(t)
	frame := sendUntilFrame(t, a, listenAddr, "legacy", frames)
	require.Zero(t, frame.flowID)
	_, err := peer.w.Write(testEncodeLegacyFrame([]byte("re:legacy")))
	require.NoError(t, err)
	expectUDPReply(t, a, "re:legacy")
}

func TestDataPlaneFeaturesFromResponse(t *testing.T) {
	require.False(t, dataPlaneFeaturesFromResponse(nil).udpFlowV1)
	resp := &http.Response{Header: http.Header{}}
	require.False(t, dataPlaneFeaturesFromResponse(resp).udpFlowV1)
	resp.Header.Set("X-Fortunnels-Dataplane-Features", "other, UDP-Flow-V1")
	require.True(t, dataPlaneFeaturesFromResponse(resp).udpFlowV1)
	resp.Header.Set("X-Fortunnels-Dataplane-Features", "udp-flow-v2")
	require.False(t, dataPlaneFeaturesFromResponse(resp).udpFlowV1)
}
