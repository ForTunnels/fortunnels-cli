// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package webhookapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultBaseURL is the production API base.
const DefaultBaseURL = "https://fortunnels.ru"

// apiBase is the fixed webhook-test API path prefix.
const apiBase = "/api/v1/webhook-test"

// Client talks to the webhook-test CI API with a bearer token. The token is
// held only in memory and is never printed or persisted.
type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

// NewWebhookClient builds a client. baseURL may be empty to use DefaultBaseURL.
func NewWebhookClient(baseURL, token string) *Client {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	return &Client{
		baseURL: baseURL,
		token:   strings.TrimSpace(token),
		http:    &http.Client{Timeout: 35 * time.Second},
	}
}

// Error is a stable machine error from the API. Status >= 500 or 0 means an
// internal/transport failure; the caller maps these to exit code 2.
type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("api error (%s): %s", e.Code, e.Message)
	}
	return fmt.Sprintf("api error: HTTP %d", e.Status)
}

// IsTimeout reports whether err is a wait/assert timeout.
func IsTimeout(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *Error
	if !asError(err, &apiErr) {
		return false
	}
	return apiErr.Code == "wait_timeout"
}

// IsAuth reports whether err is an authentication/authorization failure.
func IsAuth(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *Error
	if !asError(err, &apiErr) {
		return false
	}
	switch apiErr.Code {
	case "invalid_token", "token_expired", "token_revoked", "invalid_scope", "foreign_scope", "unauthorized":
		return true
	}
	return apiErr.Status == http.StatusUnauthorized || apiErr.Status == http.StatusForbidden
}

func asError(err error, target **Error) bool {
	return errors.As(err, target)
}

// Send dispatches one test event.
func (c *Client) Send(ctx context.Context, req *SendRequest) (WebhookClientSendResult, error) {
	var out WebhookClientSendResult
	err := c.doJSON(ctx, &jsonCall{method: http.MethodPost, path: apiBase + "/events/send", body: req, out: &out})
	return out, err
}

// ListEvents returns a page of events after an optional cursor.
func (c *Client) ListEvents(ctx context.Context, endpointID, cursor string, limit int) (ListEventsResponse, error) {
	q := url.Values{}
	q.Set("endpoint_id", endpointID)
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	var out ListEventsResponse
	err := c.doJSON(ctx, &jsonCall{method: http.MethodGet, path: apiBase + "/events", query: q, out: &out})
	return out, err
}

// Wait blocks until a matching event arrives, the timeout expires, or the
// context is canceled. Returns ErrTimeout-style Error on wait_timeout.
func (c *Client) Wait(ctx context.Context, req WaitRequest) (WaitResponse, error) {
	return postJSON[WaitResponse](ctx, c, "/events/wait", req)
}

// Clear deletes (or dry-run counts) test-generated events in scope.
func (c *Client) Clear(ctx context.Context, req ClearRequest) (ClearResponse, error) {
	return postJSON[ClearResponse](ctx, c, "/events/clear", req)
}

func postJSON[T any](ctx context.Context, c *Client, path string, body requestPayload) (T, error) {
	var out T
	err := c.doJSON(ctx, &jsonCall{method: http.MethodPost, path: apiBase + path, body: body, out: &out})
	return out, err
}

// ListMock returns the endpoint's mock rules.
func (c *Client) ListMock(ctx context.Context, endpointID string) ([]StubRule, error) {
	q := url.Values{}
	q.Set("endpoint_id", endpointID)
	var out struct {
		Rules []StubRule `json:"rules"`
	}
	err := c.doJSON(ctx, &jsonCall{method: http.MethodGet, path: apiBase + "/mock/rules", query: q, out: &out})
	return out.Rules, err
}

// CreateMock creates one mock rule.
func (c *Client) CreateMock(ctx context.Context, endpointID string, in *UpsertStubRuleRequest) (StubRule, error) {
	var out struct {
		Rule StubRule `json:"rule"`
	}
	body := map[string]any{
		"endpoint_id": endpointID,
		"priority":    in.Priority,
		"method":      in.Method,
		"path_mode":   in.PathMode,
		"path":        in.Path,
		"preset":      in.Preset,
		"response":    in.Response,
		"max_uses":    in.MaxUses,
		"enabled":     in.Enabled,
	}
	err := c.doJSON(ctx, &jsonCall{method: http.MethodPost, path: apiBase + "/mock/rules", body: body, out: &out})
	return out.Rule, err
}

// UpdateMock updates one mock rule.
func (c *Client) UpdateMock(ctx context.Context, endpointID, ruleID string, in *UpsertStubRuleRequest) (StubRule, error) {
	var out struct {
		Rule StubRule `json:"rule"`
	}
	body := map[string]any{
		"endpoint_id": endpointID,
		"priority":    in.Priority,
		"method":      in.Method,
		"path_mode":   in.PathMode,
		"path":        in.Path,
		"preset":      in.Preset,
		"response":    in.Response,
		"max_uses":    in.MaxUses,
		"version":     in.Version,
		"enabled":     in.Enabled,
	}
	err := c.doJSON(ctx, &jsonCall{method: http.MethodPatch, path: apiBase + "/mock/rules/" + url.PathEscape(ruleID), body: body, out: &out})
	return out.Rule, err
}

// DeleteMock removes one mock rule.
func (c *Client) DeleteMock(ctx context.Context, endpointID, ruleID string) error {
	q := url.Values{}
	q.Set("endpoint_id", endpointID)
	return c.doJSON(ctx, &jsonCall{method: http.MethodDelete, path: apiBase + "/mock/rules/" + url.PathEscape(ruleID), query: q})
}

// requestPayload is any JSON-encodable request body; nil sends no body.
type requestPayload any

// responseTarget is the pointer a JSON response is decoded into; nil discards it.
type responseTarget any

// jsonCall describes one API round trip.
type jsonCall struct {
	method string
	path   string
	query  url.Values
	body   requestPayload
	out    responseTarget
}

func (c *Client) doJSON(ctx context.Context, call *jsonCall) error {
	req, err := c.newRequest(ctx, call)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return decodeAPIError(resp.StatusCode, raw)
	}
	if call.out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, call.out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (c *Client) newRequest(ctx context.Context, call *jsonCall) (*http.Request, error) {
	var reader io.Reader
	if call.body != nil {
		encoded, err := json.Marshal(call.body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, call.method, c.baseURL+call.path, reader)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	if call.query != nil {
		req.URL.RawQuery = call.query.Encode()
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if call.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

func decodeAPIError(status int, raw []byte) *Error {
	apiErr := &Error{Status: status}
	if err := json.Unmarshal(raw, apiErr); err != nil || apiErr.Code == "" {
		apiErr.Code = "http_error"
		apiErr.Message = http.StatusText(status)
	}
	return apiErr
}
