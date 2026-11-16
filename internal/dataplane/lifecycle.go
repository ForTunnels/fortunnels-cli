// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	protocolv1 "github.com/fortunnels/client/shared/protocol/v1"
)

type LifecycleHandler func(protocolv1.LifecycleEventPayload)

var (
	errAttachmentAcknowledgement = errors.New("invalid attachment acknowledgement")
	errAttachmentFailed          = errors.New("attachment failed")
)

type lifecycleSetup struct {
	handler  LifecycleHandler
	tunnelID string
	deadline time.Time
}

func openLifecycleStream(sess dataPlaneSession, setup lifecycleSetup) (io.Closer, error) {
	stream, err := sess.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("open lifecycle control stream: %w", err)
	}
	if err := initializeLifecycleStream(stream, setup); err != nil {
		_ = stream.Close()
		return nil, err
	}
	return stream, nil
}

type lifecycleStream interface {
	io.ReadWriter
	SetDeadline(time.Time) error
}

func initializeLifecycleStream(stream lifecycleStream, setup lifecycleSetup) error {
	if setup.tunnelID != "" {
		deadline := time.Now().Add(15 * time.Second)
		if !setup.deadline.IsZero() && setup.deadline.Before(deadline) {
			deadline = setup.deadline
		}
		if err := stream.SetDeadline(deadline); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(stream, "{\"proto\":\"control\"}\n"); err != nil {
		return fmt.Errorf("write lifecycle control preface: %w", err)
	}
	if setup.tunnelID == "" {
		go readLifecycleControlStream(stream, setup.handler)
		return nil
	}
	reader := bufio.NewReaderSize(stream, maxPrefaceBytes)
	if err := readAttachmentReady(reader, setup.tunnelID); err != nil {
		return err
	}
	if err := stream.SetDeadline(time.Time{}); err != nil {
		return err
	}
	go readLifecycleControlStream(reader, setup.handler)
	return nil
}

func readAttachmentReady(reader *bufio.Reader, id string) error {
	line, err := reader.ReadSlice('\n')
	if err != nil {
		return fmt.Errorf("attachment ended before readiness: %w", err)
	}
	var event protocolv1.TunnelAttachmentEvent
	if err := json.Unmarshal(line, &event); err != nil || event.TunnelID != id {
		return errAttachmentAcknowledgement
	}
	switch event.Event {
	case protocolv1.EventTunnelAttachReady:
		if event.Code == "" {
			return nil
		}
	case protocolv1.EventTunnelAttachFailed:
		if event.Code == protocolv1.AttachmentAddressUnavailable || event.Code == protocolv1.AttachmentTunnelNotAttachable {
			return fmt.Errorf("%w: %s", errAttachmentFailed, event.Code)
		}
	}
	return errAttachmentAcknowledgement
}

func readLifecycleControlStream(reader io.Reader, handler LifecycleHandler) {
	decoder := json.NewDecoder(reader)
	for {
		var event struct {
			Event    string     `json:"event"`
			TunnelID string     `json:"tunnel_id"`
			Status   string     `json:"status,omitempty"`
			Reason   string     `json:"reason,omitempty"`
			ResetAt  *time.Time `json:"reset_at,omitempty"`
		}
		if err := decoder.Decode(&event); err != nil {
			return
		}
		if event.Event != protocolv1.EventTunnelClosed && event.Status != protocolv1.StatusExpired {
			continue
		}
		if handler != nil {
			handler(protocolv1.LifecycleEventPayload{
				TunnelID: event.TunnelID,
				Status:   event.Status,
				Reason:   event.Reason,
				ResetAt:  event.ResetAt,
			})
		}
		return
	}
}
