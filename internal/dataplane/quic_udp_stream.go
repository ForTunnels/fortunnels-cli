// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
)

// A QUIC DATAGRAM frame must fit in one QUIC packet, so a local UDP datagram
// whose JSON frame is too large travels over one reliable bidirectional "udp"
// stream per connection instead. The client opens the stream lazily with the
// JSON handshake line {"auth","tunnel_id","kind":"udp","dst"} and sends frames
// only after the server's ack line {"udp_stream":"v1"}. Both directions then
// carry frames
//
//	[u8 flowIDLen][flowID][u16 payloadLen, big-endian][payload]
//
// with flowIDLen 1..255 and payloadLen 1..65535; flowID is the same local
// source address string the datagram path uses.
const (
	quicUDPStreamKind       = "udp"
	quicUDPStreamAckVersion = "v1"
	quicUDPStreamMaxFlowID  = 255
	quicUDPStreamMaxPayload = 65535
	// quicUDPStreamAckLineMax bounds the server's ack line.
	quicUDPStreamAckLineMax = 256
)

var (
	// quicUDPStreamAckTimeout bounds opening the stream, writing the handshake
	// and reading the ack. The local read loop blocks at most this long, once
	// per connection.
	quicUDPStreamAckTimeout = 3 * time.Second
	// quicUDPStreamWriteTimeout bounds one frame write on an acked stream.
	quicUDPStreamWriteTimeout = 3 * time.Second
)

var (
	errQUICUDPStreamFrame = errors.New("quic udp stream: malformed frame")
	errQUICUDPStreamAck   = errors.New("quic udp stream: unexpected ack")
	errQUICUDPStreamEnded = errors.New("quic udp stream: ended by peer")
)

// quicUDPByteStream is the QUIC stream surface the fallback uses (*quic.Stream).
type quicUDPByteStream interface {
	io.ReadWriteCloser
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	CancelRead(quic.StreamErrorCode)
}

// quicUDPStreamOpener opens one bidirectional stream on the QUIC connection.
type quicUDPStreamOpener func(context.Context) (quicUDPByteStream, error)

// quicOversizeSender carries a local datagram that does not fit one QUIC
// DATAGRAM frame; it reports false when the datagram must be dropped.
type quicOversizeSender interface {
	sendOversize(flowID string, payload []byte) bool
}

// udpReplyWriter delivers a reply to a local UDP source (*net.UDPConn).
type udpReplyWriter interface {
	WriteToUDP(b []byte, addr *net.UDPAddr) (int, error)
}

func quicConnStreamOpener(qc *quic.Conn) quicUDPStreamOpener {
	return func(ctx context.Context) (quicUDPByteStream, error) {
		stream, err := qc.OpenStreamSync(ctx)
		if err != nil {
			return nil, err
		}
		return stream, nil
	}
}

// appendQUICUDPStreamFrame appends one frame to dst.
func appendQUICUDPStreamFrame(dst []byte, flowID string, payload []byte) ([]byte, error) {
	idLen, ok := quicUDPStreamFlowIDLen(flowID)
	if !ok {
		return dst, errQUICUDPStreamFrame
	}
	payloadLen, ok := quicUDPStreamPayloadLen(payload)
	if !ok {
		return dst, errQUICUDPStreamFrame
	}
	dst = append(dst, idLen)
	dst = append(dst, flowID...)
	dst = binary.BigEndian.AppendUint16(dst, payloadLen)
	return append(dst, payload...), nil
}

func quicUDPStreamFlowIDLen(flowID string) (uint8, bool) {
	n := len(flowID)
	if n < 1 || n > quicUDPStreamMaxFlowID {
		return 0, false
	}
	return uint8(n), true
}

func quicUDPStreamPayloadLen(payload []byte) (uint16, bool) {
	n := len(payload)
	if n < 1 || n > quicUDPStreamMaxPayload {
		return 0, false
	}
	return uint16(n), true
}

// quicUDPStreamFrameReader decodes frames from r. The returned payload stays
// valid until the next call.
type quicUDPStreamFrameReader struct {
	r       io.Reader
	flowID  [quicUDPStreamMaxFlowID]byte
	payload []byte
}

// next returns io.EOF only when the stream ends on a frame boundary; a
// truncated frame fails with io.ErrUnexpectedEOF and an invalid length with
// errQUICUDPStreamFrame.
func (fr *quicUDPStreamFrameReader) next() (flowID string, payload []byte, err error) {
	var idLen [1]byte
	if _, err = io.ReadFull(fr.r, idLen[:]); err != nil {
		return "", nil, err
	}
	if idLen[0] == 0 {
		return "", nil, errQUICUDPStreamFrame
	}
	payload, err = fr.readBody(int(idLen[0]))
	if err != nil {
		return "", nil, err
	}
	return string(fr.flowID[:idLen[0]]), payload, nil
}

// readBody reads the rest of a frame whose flow ID is idLen bytes long; an
// end of stream here truncates the frame.
func (fr *quicUDPStreamFrameReader) readBody(idLen int) ([]byte, error) {
	if _, err := io.ReadFull(fr.r, fr.flowID[:idLen]); err != nil {
		return nil, truncatedQUICUDPStreamFrame(err)
	}
	var size [2]byte
	if _, err := io.ReadFull(fr.r, size[:]); err != nil {
		return nil, truncatedQUICUDPStreamFrame(err)
	}
	n := int(binary.BigEndian.Uint16(size[:]))
	if n == 0 {
		return nil, errQUICUDPStreamFrame
	}
	if cap(fr.payload) < n {
		fr.payload = make([]byte, n)
	}
	if _, err := io.ReadFull(fr.r, fr.payload[:n]); err != nil {
		return nil, truncatedQUICUDPStreamFrame(err)
	}
	return fr.payload[:n], nil
}

func truncatedQUICUDPStreamFrame(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

type quicUDPStreamState uint8

const (
	quicUDPStreamUntried quicUDPStreamState = iota
	quicUDPStreamActive
	quicUDPStreamUnavailable
)

type quicUDPStreamHandshake struct {
	Auth     string `json:"auth"`
	TunnelID string `json:"tunnel_id"`
	Kind     string `json:"kind"`
	Dst      string `json:"dst"`
}

// quicUDPStreamFallback owns a connection's single "udp" stream. The forward
// loop is its only writer and the only goroutine that changes state; one
// reader goroutine delivers server frames to local sources until the stream
// or the connection context ends. Once the stream is refused or fails, the
// fallback stays unavailable for the connection.
type quicUDPStreamFallback struct {
	ctx         context.Context
	open        quicUDPStreamOpener
	replies     udpReplyWriter
	flows       *flowRegistry
	handshake   quicUDPStreamHandshake
	state       quicUDPStreamState
	stream      quicUDPByteStream
	frame       []byte
	readerEnded atomic.Bool
	closeOnce   sync.Once
}

func newQUICUDPStreamFallback(
	ctx context.Context,
	open quicUDPStreamOpener,
	replies udpReplyWriter,
	flows *flowRegistry,
	tunnelID, authToken, udpDst string,
) *quicUDPStreamFallback {
	return &quicUDPStreamFallback{
		ctx:     ctx,
		open:    open,
		replies: replies,
		flows:   flows,
		handshake: quicUDPStreamHandshake{
			Auth: authToken, TunnelID: tunnelID, Kind: quicUDPStreamKind, Dst: udpDst,
		},
	}
}

// sendOversize writes one datagram as a stream frame, opening and
// handshaking the stream on first use.
func (f *quicUDPStreamFallback) sendOversize(flowID string, payload []byte) bool {
	if f == nil || f.open == nil {
		return false
	}
	if f.state == quicUDPStreamUntried {
		f.state = quicUDPStreamUnavailable
		if err := f.establish(); err != nil {
			log.Printf("QUIC udp stream unavailable, oversized datagrams will be dropped: %v", err)
			return false
		}
		f.state = quicUDPStreamActive
	}
	if f.state != quicUDPStreamActive {
		return false
	}
	if f.readerEnded.Load() {
		f.disable(errQUICUDPStreamEnded)
		return false
	}
	frame, err := appendQUICUDPStreamFrame(f.frame[:0], flowID, payload)
	if err != nil {
		return false
	}
	f.frame = frame
	if err := f.writeFrame(frame); err != nil {
		f.disable(err)
		return false
	}
	return true
}

// establish opens the stream and waits for the ack, all within
// quicUDPStreamAckTimeout. Nothing but the handshake is written before the ack.
func (f *quicUDPStreamFallback) establish() error {
	deadline := time.Now().Add(quicUDPStreamAckTimeout)
	ctx, cancel := context.WithDeadline(f.ctx, deadline)
	defer cancel()
	stream, err := f.open(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	reader, err := handshakeQUICUDPStream(stream, f.handshake, deadline)
	if err != nil {
		closeQUICUDPByteStream(stream)
		return err
	}
	f.stream = stream
	context.AfterFunc(f.ctx, f.close)
	go f.readFrames(reader)
	return nil
}

func handshakeQUICUDPStream(stream quicUDPByteStream, hs quicUDPStreamHandshake, deadline time.Time) (*bufio.Reader, error) {
	line, marshalErr := json.Marshal(hs)
	if marshalErr != nil {
		return nil, marshalErr
	}
	if err := stream.SetWriteDeadline(deadline); err != nil {
		return nil, err
	}
	if _, err := stream.Write(append(line, '\n')); err != nil {
		return nil, fmt.Errorf("write handshake: %w", err)
	}
	if err := stream.SetReadDeadline(deadline); err != nil {
		return nil, err
	}
	reader := bufio.NewReaderSize(stream, quicUDPStreamAckLineMax)
	ackLine, readErr := reader.ReadSlice('\n')
	if readErr != nil {
		return nil, fmt.Errorf("read ack: %w", readErr)
	}
	var ack struct {
		UDPStream string `json:"udp_stream"`
	}
	if json.Unmarshal(ackLine, &ack) != nil || ack.UDPStream != quicUDPStreamAckVersion {
		return nil, errQUICUDPStreamAck
	}
	if err := stream.SetReadDeadline(time.Time{}); err != nil {
		return nil, err
	}
	return reader, nil
}

func (f *quicUDPStreamFallback) writeFrame(frame []byte) error {
	if err := f.stream.SetWriteDeadline(time.Now().Add(quicUDPStreamWriteTimeout)); err != nil {
		return err
	}
	_, err := f.stream.Write(frame)
	return err
}

// readFrames delivers server frames like DATAGRAM replies; a malformed frame
// or the end of the stream ends it and closes the stream.
func (f *quicUDPStreamFallback) readFrames(r io.Reader) {
	defer f.close()
	defer f.readerEnded.Store(true)
	frames := quicUDPStreamFrameReader{r: r}
	for {
		flowID, payload, err := frames.next()
		if err != nil {
			if f.ctx.Err() == nil && !errors.Is(err, io.EOF) {
				log.Printf("QUIC udp stream ended: %v", err)
			}
			return
		}
		deliverQUICReply(f.flows, f.replies, flowID, payload)
	}
}

// disable stops using the stream for the rest of the connection.
func (f *quicUDPStreamFallback) disable(reason error) {
	f.state = quicUDPStreamUnavailable
	f.close()
	log.Printf("QUIC udp stream stopped, oversized datagrams will be dropped: %v", reason)
}

// close releases the stream; it is idempotent and safe from any goroutine.
func (f *quicUDPStreamFallback) close() {
	if f == nil {
		return
	}
	f.closeOnce.Do(func() {
		if f.stream != nil {
			closeQUICUDPByteStream(f.stream)
		}
	})
}

// closeQUICUDPByteStream ends both directions: Close only sends FIN.
func closeQUICUDPByteStream(stream quicUDPByteStream) {
	stream.CancelRead(0)
	if err := stream.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Printf("QUIC udp stream close: %v", err)
	}
}
