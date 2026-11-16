// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package webhookapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testToken = "tok-123"

// newTestClient points a Client at handler; the fake server closes with the test.
func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return NewWebhookClient(srv.URL, testToken)
}

// checkRequest reports a request whose method, path or bearer token differ from
// the API contract. It runs on the server goroutine, so it only uses Errorf.
func checkRequest(t *testing.T, r *http.Request, method, path string) {
	t.Helper()
	if r.Method != method {
		t.Errorf("method = %s, want %s", r.Method, method)
	}
	if r.URL.Path != path {
		t.Errorf("path = %s, want %s", r.URL.Path, path)
	}
	if got, want := r.Header.Get("Authorization"), "Bearer "+testToken; got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}
}

// decodeRequest unmarshals a JSON request body into v.
func decodeRequest(t *testing.T, r *http.Request, v any) {
	t.Helper()
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		t.Errorf("read request body: %v", err)
		return
	}
	if err := json.Unmarshal(raw, v); err != nil {
		t.Errorf("decode request body %q: %v", raw, err)
	}
}

// writeJSON replies with status and body encoded as JSON.
func writeJSON(t *testing.T, w http.ResponseWriter, status int, body any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		t.Errorf("encode response: %v", err)
	}
}

func TestNewNormalizesBaseURLAndToken(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		token   string
		wantURL string
		wantTok string
	}{
		{name: "trims slashes and spaces", baseURL: "  https://api.example.test/ ", token: " tok ", wantURL: "https://api.example.test", wantTok: "tok"},
		{name: "empty base uses default", wantURL: DefaultBaseURL},
		{name: "blank base uses default", baseURL: "   ", wantURL: DefaultBaseURL},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := NewWebhookClient(tc.baseURL, tc.token)
			if c.baseURL != tc.wantURL {
				t.Errorf("baseURL = %q, want %q", c.baseURL, tc.wantURL)
			}
			if c.token != tc.wantTok {
				t.Errorf("token = %q, want %q", c.token, tc.wantTok)
			}
		})
	}
}

func TestSendPostsEventAndDecodesResult(t *testing.T) {
	status := http.StatusAccepted
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		checkRequest(t, r, http.MethodPost, apiBase+"/events/send")
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}
		var got SendRequest
		decodeRequest(t, r, &got)
		if got.EndpointID != "ep-1" || got.Request.Path != "/hook" {
			t.Errorf("send body = %+v, want endpoint ep-1 and path /hook", got)
		}
		writeJSON(t, w, http.StatusOK, WebhookClientSendResult{EventID: "evt-1", Delivered: true, DownstreamStatus: &status})
	})

	res, err := c.Send(t.Context(), &SendRequest{EndpointID: "ep-1", Request: SendRequestReq{Method: "POST", Path: "/hook"}})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if res.EventID != "evt-1" || !res.Delivered {
		t.Errorf("result = %+v, want delivered evt-1", res)
	}
	if res.DownstreamStatus == nil || *res.DownstreamStatus != http.StatusAccepted {
		t.Errorf("DownstreamStatus = %v, want 202", res.DownstreamStatus)
	}
}

func TestListEventsSendsOnlySetQueryParams(t *testing.T) {
	tests := []struct {
		name      string
		cursor    string
		limit     int
		wantQuery string
	}{
		{name: "cursor and limit", cursor: "c-1", limit: 25, wantQuery: "cursor=c-1&endpoint_id=ep-1&limit=25"},
		{name: "endpoint only", wantQuery: "endpoint_id=ep-1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				checkRequest(t, r, http.MethodGet, apiBase+"/events")
				if r.URL.RawQuery != tc.wantQuery {
					t.Errorf("query = %q, want %q", r.URL.RawQuery, tc.wantQuery)
				}
				writeJSON(t, w, http.StatusOK, ListEventsResponse{Events: []WebhookClientEvent{{ID: "evt-1", Status: 200}}, NextCursor: "c-2", HasMore: true})
			})

			page, err := c.ListEvents(t.Context(), "ep-1", tc.cursor, tc.limit)
			if err != nil {
				t.Fatalf("ListEvents: %v", err)
			}
			if len(page.Events) != 1 || page.Events[0].ID != "evt-1" || page.NextCursor != "c-2" || !page.HasMore {
				t.Errorf("page = %+v, want evt-1 with next cursor c-2", page)
			}
		})
	}
}

func TestWaitPostsMatcherAndReturnsMatchedEvent(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		checkRequest(t, r, http.MethodPost, apiBase+"/events/wait")
		var got WaitRequest
		decodeRequest(t, r, &got)
		if got.TimeoutMS != 5000 || got.Matcher["path"] != "/hook" {
			t.Errorf("wait body = %+v, want timeout 5000 and path matcher", got)
		}
		writeJSON(t, w, http.StatusOK, WaitResponse{Matched: true, Cursor: "c-2", Event: &WebhookClientEvent{ID: "evt-2", Status: 200}})
	})

	res, err := c.Wait(t.Context(), WaitRequest{EndpointID: "ep-1", TimeoutMS: 5000, Matcher: WaitMatcher{"path": "/hook"}})
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Matched || res.Event == nil || res.Event.ID != "evt-2" || res.Cursor != "c-2" {
		t.Errorf("wait result = %+v, want matched evt-2 at cursor c-2", res)
	}
}

func TestWaitFailsBeforeSendingUnencodableMatcher(t *testing.T) {
	c := newTestClient(t, func(_ http.ResponseWriter, r *http.Request) {
		t.Errorf("server received %s %s; an unencodable body must not be sent", r.Method, r.URL.Path)
	})

	_, err := c.Wait(t.Context(), WaitRequest{EndpointID: "ep-1", Matcher: WaitMatcher{"bad": make(chan int)}})
	if err == nil || !strings.Contains(err.Error(), "unsupported type") {
		t.Fatalf("Wait error = %v, want an unsupported type encode error", err)
	}
}

func TestClearPostsScopeAndDecodesCount(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		checkRequest(t, r, http.MethodPost, apiBase+"/events/clear")
		var got ClearRequest
		decodeRequest(t, r, &got)
		if got.EndpointID != "ep-1" || got.RunID != "run-7" || !got.DryRun {
			t.Errorf("clear body = %+v, want dry run for ep-1 run run-7", got)
		}
		writeJSON(t, w, http.StatusOK, ClearResponse{DryRun: true, Count: 3})
	})

	res, err := c.Clear(t.Context(), ClearRequest{EndpointID: "ep-1", RunID: "run-7", DryRun: true})
	if err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if !res.DryRun || res.Count != 3 {
		t.Errorf("clear result = %+v, want dry run with count 3", res)
	}
}

func TestListMockReadsRulesForEndpoint(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		checkRequest(t, r, http.MethodGet, apiBase+"/mock/rules")
		if got := r.URL.Query().Get("endpoint_id"); got != "ep-1" {
			t.Errorf("endpoint_id = %q, want ep-1", got)
		}
		writeJSON(t, w, http.StatusOK, map[string]any{"rules": []StubRule{{
			ID:            "r-1",
			Enabled:       true,
			MockRuleMatch: MockRuleMatch{Method: "GET", PathMode: "exact", Path: "/x"},
			Response:      MockResp{Status: http.StatusNoContent},
		}}})
	})

	rules, err := c.ListMock(t.Context(), "ep-1")
	if err != nil {
		t.Fatalf("ListMock: %v", err)
	}
	if len(rules) != 1 || rules[0].ID != "r-1" || rules[0].Path != "/x" || rules[0].Response.Status != http.StatusNoContent {
		t.Errorf("rules = %+v, want one exact GET /x rule", rules)
	}
}

func TestCreateMockPostsRuleAndReturnsIt(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		checkRequest(t, r, http.MethodPost, apiBase+"/mock/rules")
		var got map[string]any
		decodeRequest(t, r, &got)
		if got["endpoint_id"] != "ep-1" || got["path"] != "/x" || got["enabled"] != true {
			t.Errorf("create body = %v, want endpoint ep-1, path /x, enabled", got)
		}
		writeJSON(t, w, http.StatusCreated, map[string]any{"rule": StubRule{ID: "r-new", Version: 1}})
	})

	enabled := true
	rule, err := c.CreateMock(t.Context(), "ep-1", &UpsertStubRuleRequest{
		Priority: 2,
		Enabled:  &enabled,
		Method:   "GET",
		PathMode: "exact",
		Path:     "/x",
		Response: MockResp{Status: http.StatusOK},
	})
	if err != nil {
		t.Fatalf("CreateMock: %v", err)
	}
	if rule.ID != "r-new" || rule.Version != 1 {
		t.Errorf("rule = %+v, want r-new version 1", rule)
	}
}

func TestUpdateMockPatchesEscapedRuleID(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("method = %s, want PATCH", r.Method)
		}
		if got, want := r.URL.EscapedPath(), apiBase+"/mock/rules/a%20b"; got != want {
			t.Errorf("escaped path = %s, want %s", got, want)
		}
		var body map[string]any
		decodeRequest(t, r, &body)
		if body["version"] != float64(3) {
			t.Errorf("version = %v, want 3", body["version"])
		}
		writeJSON(t, w, http.StatusOK, map[string]any{"rule": StubRule{ID: "a b", Version: 4}})
	})

	rule, err := c.UpdateMock(t.Context(), "ep-1", "a b", &UpsertStubRuleRequest{Priority: 1, Response: MockResp{Status: http.StatusOK}, Version: 3})
	if err != nil {
		t.Fatalf("UpdateMock: %v", err)
	}
	if rule.ID != "a b" || rule.Version != 4 {
		t.Errorf("rule = %+v, want a b version 4", rule)
	}
}

func TestDeleteMockSendsEndpointQueryAndAcceptsEmptyReply(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		checkRequest(t, r, http.MethodDelete, apiBase+"/mock/rules/r-9")
		if got := r.URL.RawQuery; got != "endpoint_id=ep-1" {
			t.Errorf("query = %q, want endpoint_id=ep-1", got)
		}
		w.WriteHeader(http.StatusNoContent)
	})

	if err := c.DeleteMock(t.Context(), "ep-1", "r-9"); err != nil {
		t.Fatalf("DeleteMock: %v", err)
	}
}

func TestAPIErrorsAreDecodedAndClassified(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		wantCode    string
		wantMessage string
		wantTimeout bool
		wantAuth    bool
	}{
		{name: "wait timeout", status: http.StatusRequestTimeout, body: `{"code":"wait_timeout","message":"no event"}`, wantCode: "wait_timeout", wantMessage: "no event", wantTimeout: true},
		{name: "expired token", status: http.StatusUnauthorized, body: `{"code":"token_expired","message":"expired"}`, wantCode: "token_expired", wantMessage: "expired", wantAuth: true},
		{name: "forbidden with non-JSON body", status: http.StatusForbidden, body: "not json", wantCode: "http_error", wantMessage: "Forbidden", wantAuth: true},
		{name: "JSON without code", status: http.StatusNotFound, body: `{"message":"missing"}`, wantCode: "http_error", wantMessage: "Not Found"},
		{name: "server error", status: http.StatusInternalServerError, body: "boom", wantCode: "http_error", wantMessage: "Internal Server Error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})

			_, err := c.Wait(t.Context(), WaitRequest{EndpointID: "ep-1", TimeoutMS: 100})
			var apiErr *Error
			if !errors.As(err, &apiErr) {
				t.Fatalf("Wait error = %v, want *Error", err)
			}
			if apiErr.Status != tc.status || apiErr.Code != tc.wantCode || apiErr.Message != tc.wantMessage {
				t.Errorf("error = %+v, want status %d code %q message %q", apiErr, tc.status, tc.wantCode, tc.wantMessage)
			}
			if got := IsTimeout(err); got != tc.wantTimeout {
				t.Errorf("IsTimeout = %v, want %v", got, tc.wantTimeout)
			}
			if got := IsAuth(err); got != tc.wantAuth {
				t.Errorf("IsAuth = %v, want %v", got, tc.wantAuth)
			}
		})
	}
}

func TestErrorMessageNamesCodeOrStatus(t *testing.T) {
	if got, want := (&Error{Status: 409, Code: "conflict", Message: "stale version"}).Error(), "api error (conflict): stale version"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	if got, want := (&Error{Status: 418}).Error(), "api error: HTTP 418"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestPredicatesIgnoreNilAndForeignErrors(t *testing.T) {
	foreign := errors.New("dial tcp: refused")
	if IsTimeout(nil) || IsAuth(nil) {
		t.Error("nil error classified as an API failure")
	}
	if IsTimeout(foreign) || IsAuth(foreign) {
		t.Error("non-API error classified as an API failure")
	}
}

func TestUndecodableSuccessBodyIsNotAnAPIError(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not json"))
	})

	_, err := c.Send(t.Context(), &SendRequest{EndpointID: "ep-1"})
	if err == nil || !strings.Contains(err.Error(), "decode response") {
		t.Fatalf("Send error = %v, want decode response error", err)
	}
	var apiErr *Error
	if errors.As(err, &apiErr) {
		t.Errorf("decode failure reported as API error %+v", apiErr)
	}
}

func TestTransportFailureIsWrapped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	c := NewWebhookClient(srv.URL, testToken)
	srv.Close()

	_, err := c.ListMock(t.Context(), "ep-1")
	if err == nil || !strings.Contains(err.Error(), "request failed") {
		t.Fatalf("ListMock error = %v, want request failed", err)
	}
}

func TestMalformedBaseURLFailsRequestBuild(t *testing.T) {
	c := NewWebhookClient("http://[::1", testToken)

	_, err := c.ListEvents(t.Context(), "ep-1", "", 0)
	if err == nil || !strings.Contains(err.Error(), "build request") {
		t.Fatalf("ListEvents error = %v, want build request error", err)
	}
}
