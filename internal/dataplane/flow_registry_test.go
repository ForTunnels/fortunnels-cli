// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type manualClock struct{ now time.Time }

func (c *manualClock) Now() time.Time          { return c.now }
func (c *manualClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func testFlowAddr(port int) *net.UDPAddr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}
}

func flowCount(r *flowRegistry) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.flows)
}

func TestFlowRegistry_ExpiresIdleFlowsAndKeepsActiveOnes(t *testing.T) {
	clock := &manualClock{now: time.Unix(1000, 0)}
	reg := newBoundedFlowRegistry(time.Minute, 16, clock.Now)
	require.True(t, reg.registerQUICFlow("idle", testFlowAddr(1)))
	require.True(t, reg.registerQUICFlow("sending", testFlowAddr(2)))
	require.True(t, reg.registerQUICFlow("replied", testFlowAddr(3)))

	clock.Advance(40 * time.Second)
	require.False(t, reg.registerQUICFlow("sending", testFlowAddr(2)), "a known source is not a new flow")
	_, ok := reg.get("replied")
	require.True(t, ok)

	clock.Advance(30 * time.Second)
	_, ok = reg.get("idle")
	require.False(t, ok, "a flow idle past the TTL must expire")
	_, ok = reg.get("sending")
	require.True(t, ok, "outgoing datagrams keep a flow alive")
	_, ok = reg.get("replied")
	require.True(t, ok, "replies keep a flow alive")
	require.Equal(t, 2, flowCount(reg))

	clock.Advance(2 * time.Minute)
	require.True(t, reg.registerQUICFlow("fresh", testFlowAddr(4)))
	require.Equal(t, 1, flowCount(reg), "registering prunes every expired flow")
}

func TestFlowRegistry_EvictsLeastRecentlyActiveAtLimit(t *testing.T) {
	clock := &manualClock{now: time.Unix(1000, 0)}
	reg := newBoundedFlowRegistry(time.Hour, 3, clock.Now)
	for i, id := range []string{"a", "b", "c"} {
		clock.Advance(time.Second)
		require.True(t, reg.registerQUICFlow(id, testFlowAddr(10+i)))
	}
	clock.Advance(time.Second)
	_, ok := reg.get("a") // a reply makes "a" the most recently active flow
	require.True(t, ok)

	clock.Advance(time.Second)
	require.True(t, reg.registerQUICFlow("d", testFlowAddr(13)))

	require.Equal(t, 3, flowCount(reg), "the table never exceeds its limit")
	_, ok = reg.get("b")
	require.False(t, ok, "the least recently active flow is evicted first")
	for _, id := range []string{"a", "c", "d"} {
		_, ok = reg.get(id)
		require.True(t, ok, "flow %s must stay", id)
	}
}
