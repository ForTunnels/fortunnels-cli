// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"net"
	"sync"
	"time"
)

const (
	// quicFlowIdleTTL outlives the server's per-flow UDP idle timeout, which
	// admins can raise well past its 30s default, so a mapping stays while the
	// server can still reply. The size limit bounds memory.
	quicFlowIdleTTL = time.Hour
	// quicFlowLimit bounds the local source table for one QUIC connection.
	quicFlowLimit = 4096
)

// flowRegistry maps QUIC flow IDs, and the numeric flow IDs of reverse UDP flow
// framing, to the local UDP peer address. Datagrams in either direction refresh
// a flow; flows idle longer than ttl expire lazily on the next access, and at
// the size limit the least recently active flow is evicted. Entries form a list
// ordered by activity, newest first.
type flowRegistry struct {
	mu      sync.RWMutex
	flows   map[string]*flowEntry
	recency flowRecency
	ttl     time.Duration
	limit   int
	now     func() time.Time
	// byNum indexes numerically identified flows (reverse UDP flow framing);
	// it stays nil for registries used only by flow ID string.
	byNum   map[uint32]*flowEntry
	lastNum uint32
}

type flowEntry struct {
	id       string
	num      uint32
	addr     *net.UDPAddr
	lastSeen time.Time
	newer    *flowEntry
	older    *flowEntry
}

func newFlowRegistry() *flowRegistry {
	return newBoundedFlowRegistry(quicFlowIdleTTL, quicFlowLimit, time.Now)
}

func newBoundedFlowRegistry(ttl time.Duration, limit int, now func() time.Time) *flowRegistry {
	return &flowRegistry{flows: make(map[string]*flowEntry), ttl: ttl, limit: max(limit, 1), now: now}
}

// set records addr for flowID and marks the flow active.
func (r *flowRegistry) set(flowID string, addr *net.UDPAddr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.activeLocked(flowID); ok {
		entry.addr = addr
		return
	}
	r.insertLocked(flowID, addr)
}

// get returns the peer of a live flow; a reply counts as flow activity.
func (r *flowRegistry) get(flowID string) (*net.UDPAddr, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.activeLocked(flowID)
	if !ok {
		return nil, false
	}
	return entry.addr, true
}

// registerQUICFlow marks the source of an outgoing datagram active and reports
// whether it started a new flow. A known flow keeps its original address.
func (r *flowRegistry) registerQUICFlow(flowID string, addr *net.UDPAddr) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.activeLocked(flowID); ok {
		return false
	}
	r.insertLocked(flowID, addr)
	return true
}

// discardQUICFlow removes a flow only while it still maps to addr.
func (r *flowRegistry) discardQUICFlow(flowID string, addr *net.UDPAddr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.flows[flowID]; ok && entry.addr == addr {
		r.removeLocked(entry)
	}
}

// udpFlowID returns the numeric flow ID of a local source and marks the flow
// active. A new source gets the next unused ID; IDs are not reused while the
// counter has not wrapped, so a late reply for an expired flow never reaches a
// newer source.
func (r *flowRegistry) udpFlowID(addr *net.UDPAddr) uint32 {
	key := addr.String()
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.activeLocked(key)
	if !ok {
		entry = r.insertLocked(key, addr)
	}
	if entry.num == 0 {
		r.numberLocked(entry)
	}
	return entry.num
}

// udpFlowPeer returns the local source of a live numeric flow; a reply counts
// as flow activity.
func (r *flowRegistry) udpFlowPeer(flowID uint32) (*net.UDPAddr, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.byNum[flowID]
	if !ok {
		return nil, false
	}
	if entry, ok = r.activeLocked(entry.id); !ok {
		return nil, false
	}
	return entry.addr, true
}

// numberLocked gives entry the next free non-zero numeric flow ID.
func (r *flowRegistry) numberLocked(entry *flowEntry) {
	if r.byNum == nil {
		r.byNum = make(map[uint32]*flowEntry)
	}
	for {
		r.lastNum++
		if _, used := r.byNum[r.lastNum]; r.lastNum != 0 && !used {
			break
		}
	}
	entry.num = r.lastNum
	r.byNum[entry.num] = entry
}

// activeLocked prunes expired flows and refreshes flowID when it is still live.
func (r *flowRegistry) activeLocked(flowID string) (*flowEntry, bool) {
	now := r.now()
	r.expireLocked(now)
	entry, ok := r.flows[flowID]
	if !ok {
		return nil, false
	}
	entry.lastSeen = now
	r.recency.unlink(entry)
	r.recency.pushNewest(entry)
	return entry, true
}

func (r *flowRegistry) insertLocked(flowID string, addr *net.UDPAddr) *flowEntry {
	for len(r.flows) >= r.limit && r.recency.oldest != nil {
		r.removeLocked(r.recency.oldest)
	}
	entry := &flowEntry{id: flowID, addr: addr, lastSeen: r.now()}
	r.flows[flowID] = entry
	r.recency.pushNewest(entry)
	return entry
}

// expireLocked drops flows from the oldest end while they are idle past ttl.
func (r *flowRegistry) expireLocked(now time.Time) {
	for r.recency.oldest != nil && now.Sub(r.recency.oldest.lastSeen) >= r.ttl {
		r.removeLocked(r.recency.oldest)
	}
}

func (r *flowRegistry) removeLocked(entry *flowEntry) {
	r.recency.unlink(entry)
	delete(r.flows, entry.id)
	if entry.num != 0 {
		delete(r.byNum, entry.num)
	}
}

// flowRecency is the doubly linked list of flows ordered by last activity.
type flowRecency struct {
	newest *flowEntry
	oldest *flowEntry
}

func (l *flowRecency) pushNewest(entry *flowEntry) {
	entry.newer, entry.older = nil, l.newest
	if l.newest != nil {
		l.newest.newer = entry
	} else {
		l.oldest = entry
	}
	l.newest = entry
}

func (l *flowRecency) unlink(entry *flowEntry) {
	if entry.newer != nil {
		entry.newer.older = entry.older
	} else if l.newest == entry {
		l.newest = entry.older
	}
	if entry.older != nil {
		entry.older.newer = entry.newer
	} else if l.oldest == entry {
		l.oldest = entry.newer
	}
	entry.newer, entry.older = nil, nil
}
