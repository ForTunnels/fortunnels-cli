// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFlowRegistry_UDPFlowIDsMapSourcesBothWays(t *testing.T) {
	reg := newBoundedFlowRegistry(time.Minute, 16, (&manualClock{now: time.Unix(1000, 0)}).Now)
	first, second := reg.udpFlowID(testFlowAddr(1)), reg.udpFlowID(testFlowAddr(2))
	require.NotZero(t, first)
	require.NotZero(t, second)
	require.NotEqual(t, first, second)
	require.Equal(t, first, reg.udpFlowID(testFlowAddr(1)), "a known source keeps its flow ID")

	addr, ok := reg.udpFlowPeer(second)
	require.True(t, ok)
	require.Equal(t, testFlowAddr(2).String(), addr.String())
	_, ok = reg.udpFlowPeer(second + 100)
	require.False(t, ok, "an unknown flow ID has no peer")
}

func TestFlowRegistry_UDPFlowIDsExpireAndAreNeverReused(t *testing.T) {
	clock := &manualClock{now: time.Unix(1000, 0)}
	reg := newBoundedFlowRegistry(time.Minute, 16, clock.Now)
	idle := reg.udpFlowID(testFlowAddr(1))
	active := reg.udpFlowID(testFlowAddr(2))

	clock.Advance(40 * time.Second)
	_, ok := reg.udpFlowPeer(active)
	require.True(t, ok, "a reply keeps a flow alive")
	clock.Advance(30 * time.Second)
	_, ok = reg.udpFlowPeer(idle)
	require.False(t, ok, "a flow idle past the TTL expires")
	_, ok = reg.udpFlowPeer(active)
	require.True(t, ok)

	again := reg.udpFlowID(testFlowAddr(1))
	require.NotEqual(t, idle, again, "an expired source gets a fresh ID so late replies cannot reach a new source")
	require.Equal(t, 2, flowCount(reg))
}

func TestFlowRegistry_UDPFlowIDsEvictLeastRecentlyActiveAtLimit(t *testing.T) {
	clock := &manualClock{now: time.Unix(1000, 0)}
	reg := newBoundedFlowRegistry(time.Hour, 2, clock.Now)
	a := reg.udpFlowID(testFlowAddr(1))
	clock.Advance(time.Second)
	b := reg.udpFlowID(testFlowAddr(2))
	clock.Advance(time.Second)
	_, ok := reg.udpFlowPeer(a)
	require.True(t, ok)
	clock.Advance(time.Second)
	c := reg.udpFlowID(testFlowAddr(3))

	require.Equal(t, 2, flowCount(reg))
	_, ok = reg.udpFlowPeer(b)
	require.False(t, ok, "the least recently active flow is evicted")
	for _, id := range []uint32{a, c} {
		_, ok = reg.udpFlowPeer(id)
		require.True(t, ok)
	}
}

func TestFlowRegistryTTLsOutliveConfigurableServerIdle(t *testing.T) {
	require.Equal(t, time.Hour, newFlowRegistry().ttl, "QUIC flows")
	require.Equal(t, time.Hour, newUDPFlowRegistry().ttl, "reverse UDP flow IDs")

	clock := &manualClock{now: time.Unix(1000, 0)}
	reg := newBoundedFlowRegistry(udpFlowIdleTTL, udpFlowLimit, clock.Now)
	id := reg.udpFlowID(testFlowAddr(1))
	clock.Advance(59 * time.Minute)
	_, ok := reg.udpFlowPeer(id)
	require.True(t, ok, "a late reply after a long quiet period still reaches its source")
	require.True(t, reg.registerQUICFlow("quic", testFlowAddr(2)))
	clock.Advance(59 * time.Minute)
	_, ok = reg.get("quic")
	require.True(t, ok)
}
