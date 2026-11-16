// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	protocolv1 "github.com/fortunnels/client/shared/protocol/v1"
)

// RecoveryClient uses the same authenticated client for provisioning and attachment.
type RecoveryClient struct {
	serverURL string
	auth      requestAuth
}

type requestAuth struct {
	client *http.Client
	bearer string
	csrf   string
}

func (a requestAuth) do(req *http.Request) (*http.Response, error) {
	if b := strings.TrimSpace(a.bearer); b != "" {
		req.Header.Set("Authorization", "Bearer "+b)
	}
	if csrf := strings.TrimSpace(a.csrf); csrf != "" {
		req.Header.Set("X-CSRF-Token", csrf)
	}
	client := a.client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return client.Do(req)
}

// NewRecoveryClient binds account credentials to a tunnel server.
func NewRecoveryClient(serverURL string, client *http.Client, bearer, csrf string) RecoveryClient {
	return RecoveryClient{serverURL: serverURL, auth: requestAuth{client: client, bearer: bearer, csrf: csrf}}
}

// Provision creates a tunnel that can be attached again by its owner.
func (c RecoveryClient) Provision(ctx context.Context, target, protocol, owner string) (*CreatedTunnel, error) {
	return createTunnelRequest(ctx, c.serverURL+"/api/tunnels/reusable", c.auth, tunnelCreationBody(target, protocol, owner))
}

// TunnelAttachment contains the owned tunnel and headers for its one data-plane upgrade.
type TunnelAttachment struct {
	Tunnel  TunnelResponse
	Headers http.Header
}

var (
	errAttachmentResponse   = errors.New("invalid tunnel attachment response")
	errAttachmentIncomplete = errors.New("incomplete tunnel attachment response")
	errAttachmentRejected   = errors.New("attachment rejected")
)

// Attach requests a fresh one-use ticket without changing the tunnel or its address.
func (c RecoveryClient) Attach(ctx context.Context, id string) (*TunnelAttachment, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.serverURL+"/api/tunnels/"+url.PathEscape(id)+"/attach", http.NoBody)
	if err != nil {
		return nil, err
	}
	resp, err := c.auth.do(req)
	if err != nil {
		return nil, fmt.Errorf("request attachment: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, attachmentResponseError(resp)
	}
	var grant protocolv1.TunnelAttachmentResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxTunnelErrorBodyBytes)).Decode(&grant); err != nil {
		return nil, errAttachmentResponse
	}
	if grant.Tunnel.ID != id || grant.Tunnel.PublicURL == "" || grant.DPAuth == "" || grant.AttachmentTicket == "" {
		return nil, errAttachmentIncomplete
	}
	headers := wsDialHeaders(c.auth.client, c.serverURL+"/ws", c.auth.bearer)
	headers.Set(protocolv1.AttachmentHeader, grant.AttachmentTicket)
	headers.Set("X-Fortunnels-DP-Auth", grant.DPAuth)
	return &TunnelAttachment{Tunnel: grant.Tunnel, Headers: headers}, nil
}

func attachmentResponseError(resp *http.Response) error {
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTunnelErrorBodyBytes))
	if err != nil {
		return fmt.Errorf("%w (HTTP %d)", errAttachmentRejected, resp.StatusCode)
	}
	var result protocolv1.APIError
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("%w (HTTP %d)", errAttachmentRejected, resp.StatusCode)
	}
	// Only allow known codes to reach output. Error bodies can contain secrets.
	switch protocolv1.AttachmentCode(result.Code) {
	case protocolv1.AttachmentTunnelInUse, protocolv1.AttachmentAddressUnavailable, protocolv1.AttachmentTunnelNotAttachable:
		return &TunnelCreateError{Status: resp.StatusCode, Code: result.Code, Message: result.Code}
	default:
		return fmt.Errorf("%w (HTTP %d)", errAttachmentRejected, resp.StatusCode)
	}
}

// IsAttachmentOccupied permits retry only for the typed conflict from the attach route.
func IsAttachmentOccupied(err error) bool {
	var rejection *TunnelCreateError
	return errors.As(err, &rejection) && rejection.Status == http.StatusConflict &&
		rejection.Code == string(protocolv1.AttachmentTunnelInUse)
}
