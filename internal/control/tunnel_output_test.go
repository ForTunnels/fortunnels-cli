// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package control

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type capturedOutput struct{ lines []string }

func (o *capturedOutput) Printf(format string, args ...any) {
	o.lines = append(o.lines, fmt.Sprintf(format, args...))
}
func (o *capturedOutput) Println(args ...any) { o.lines = append(o.lines, fmt.Sprintln(args...)) }
func (o *capturedOutput) text() string        { return strings.Join(o.lines, "") }

func TestPrintTunnelInfo_GuestBannerUsesServerTrafficLimit(t *testing.T) {
	expires := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		limit int64
		want  string
		wants bool
	}{
		{"one gigabyte", 1 << 30, "лимит трафика 1 GB", true},
		{"half gigabyte", 512 << 20, "лимит трафика 512 MB", true},
		{"fractional", 1536 << 20, "лимит трафика 1.5 GB", true},
		{"unknown limit", 0, "лимит трафика", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := &capturedOutput{}
			PrintTunnelInfoWithOutput(out, "https://server.test", &Response{
				ID: "t1", IsGuest: true, ExpiresAt: expires, TrafficLimitBytes: tt.limit,
			})

			text := out.text()
			assert.Contains(t, text, "Гостевой туннель")
			assert.Equal(t, tt.wants, strings.Contains(text, tt.want), text)
			if !tt.wants {
				assert.NotContains(t, text, "1 GB")
			}
		})
	}
}

func TestCreateTunnel_LocalHTTPSTargetsSkipVerification(t *testing.T) {
	for target, wantInsecure := range map[string]bool{
		"localhost:8443":   true,
		"127.0.0.1:8443":   true,
		"[::1]:8443":       true,
		"example.com:8443": false,
	} {
		bodies := make(chan string, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			buf := make([]byte, 1024)
			n, _ := r.Body.Read(buf)
			bodies <- string(buf[:n])
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"t1"}`))
		}))

		_, err := CreateTunnelWithClient(server.URL, target, "https", "", nil, "", "")
		server.Close()

		require.NoError(t, err)
		body := <-bodies
		assert.Equal(t, wantInsecure, strings.Contains(body, `"tls_insecure_skip_verify":true`), "%s: %s", target, body)
	}
}

func TestPrintTunnelInfo_GuestShowsSignupHint(t *testing.T) {
	out := &capturedOutput{}
	PrintTunnelInfoWithOutput(out, "https://fortunnels.ru", &Response{ID: "t1", IsGuest: true})

	text := out.text()
	assert.Contains(t, text,
		"💡 Бесплатный аккаунт: дашборд, инспектор запросов, TCP/UDP-туннели и CLI-токен — https://fortunnels.ru/?register=1\n")
	for _, marker := range []string{"Tunnel created successfully", "Tunnel ID:", "Public URL:"} {
		assert.Equal(t, 1, strings.Count(text, marker), marker)
	}
}

func TestPrintTunnelInfo_NonGuestHasNoSignupHint(t *testing.T) {
	out := &capturedOutput{}
	PrintTunnelInfoWithOutput(out, "https://fortunnels.ru", &Response{ID: "t1"})

	assert.NotContains(t, out.text(), "register=1")
}

func TestPrintTunnelInfo_GuestHintSkippedForUnparsableServerURL(t *testing.T) {
	out := &capturedOutput{}
	PrintTunnelInfoWithOutput(out, "://bad", &Response{ID: "t1", IsGuest: true})

	assert.Contains(t, out.text(), "Гостевой туннель")
	assert.NotContains(t, out.text(), "register=1")
}

func TestGuestSignupHint_KeepsCustomHostAndPort(t *testing.T) {
	assert.Equal(t, "http://127.0.0.1:8080/?register=1", guestSignupHint("http://127.0.0.1:8080"))
}
