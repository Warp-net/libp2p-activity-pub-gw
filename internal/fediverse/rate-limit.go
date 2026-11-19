/*

 Warpnet - Decentralized Social Network
 Copyright (C) 2025 Vadim Filin, https://github.com/Warp-net,
 <github.com.mecdy@passmail.net>

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <https://www.gnu.org/licenses/>.

WarpNet is provided “as is” without warranty of any kind, either expressed or implied.
Use at your own risk. The maintainers shall not be liable for any damages or data loss
resulting from the use or misuse of this software.
*/

// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package fediverse

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ratelimiter "github.com/filinvadim/ratelimiter"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

// Rate limiting (weighted sliding window, github.com/filinvadim/ratelimiter).
//
// Key: the remote client IP — one budget per federation peer (a Mastodon
// instance fans out from a handful of addresses), plus a global budget that
// caps total spend so many distinct IPs can't overwhelm the libp2p link to the
// member nodes, which is the scarce resource behind every expensive route.
//
// Weights price a request by what it costs the gateway, not by request count:
// nodeinfo/static are served from memory (1); webfinger and the actor document
// cost one node round-trip (2); collections and the inbox cost several
// round-trips or an outbound signature-key fetch (4); the media proxy pulls
// full image bytes through the node (8).
const (
	rateWindow = time.Minute

	// perClientBudget: weight units one client IP may spend per window
	// (~600 actor lookups, ~300 collections, or ~150 media fetches a minute).
	// A busy instance legitimately bursts well past a hundred fetches when it
	// discovers a new actor (actor+webfinger+outbox+followers+following+avatar+
	// header) and then dereferences a thread's statuses, so a low budget 429s
	// real peers mid-resolve and the note/profile never renders.
	perClientBudget uint32 = 1200
	// globalBudget bounds gateway-wide spend per window regardless of how
	// many client IPs the traffic comes from.
	globalBudget uint32 = 6000

	weightStatic     uint32 = 1
	weightActor      uint32 = 2
	weightCollection uint32 = 4
	weightInbox      uint32 = 4
	weightMedia      uint32 = 8

	// Per-client limiters live in an expirable LRU: idle clients are dropped
	// after clientLimiterTTL instead of accumulating forever (statelessness —
	// the set is transient and in memory only).
	maxClientLimiters = 4096
	clientLimiterTTL  = 5 * time.Minute

	// drainInterval is how often the drain checks for a limiter that has been
	// stuck locked (see drain).
	drainInterval = 500 * time.Millisecond
)

// rateLimiters is the gateway's request throttle: one global limiter plus
// per-client-IP limiters held in an expirable LRU (evicted limiters are closed).
type rateLimiters struct {
	window       time.Duration
	clientBudget uint32
	globalBudget uint32
	global       atomic.Pointer[ratelimiter.Limiter] // swapped out by drain when stuck

	mu      sync.Mutex // serializes get-or-create on clients
	clients *expirable.LRU[string, *ratelimiter.Limiter]
}

func newRateLimiters() *rateLimiters {
	return newRateLimitersWith(globalBudget, perClientBudget, rateWindow)
}

// taskQueue is the sliding-window log backing a limiter. It replaces the
// library's default queue, whose CutOffBefore is a no-op when every task has
// expired (start == len): Limit() has already subtracted those weights, so the
// next call subtracts them again, the unsigned total underflows to ~4e9 and
// IsLocked() latches. On a low-traffic gateway (idle gaps longer than the
// window) that 429s peers far under budget — including Mastodon fetching our
// actor document to verify a signature, which fails the whole delivery.
type taskQueue struct {
	mu    sync.Mutex
	tasks []ratelimiter.Task
}

func (q *taskQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func (q *taskQueue) TaskByIndex(i int) ratelimiter.Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	if i < 0 {
		i = 0
	}
	if i >= len(q.tasks) {
		i = len(q.tasks) - 1
	}
	return q.tasks[i]
}

func (q *taskQueue) CutOffBefore(start int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if start <= 0 {
		return
	}
	if start >= len(q.tasks) {
		q.tasks = q.tasks[:0]
		return
	}
	q.tasks = q.tasks[start:]
}

func (q *taskQueue) Append(tasks ...ratelimiter.Task) []ratelimiter.Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, tasks...)
	return q.tasks
}

// newLimiter builds a limiter over the fixed taskQueue.
func newLimiter(budget uint32, window time.Duration) *ratelimiter.Limiter {
	return ratelimiter.NewLimiter(budget, window, &taskQueue{})
}

// newRateLimitersWith parameterizes the budgets/window so tests can exercise
// the middleware without spending 120 requests per case.
func newRateLimitersWith(global, perClient uint32, window time.Duration) *rateLimiters {
	rl := &rateLimiters{
		window:       window,
		clientBudget: perClient,
		globalBudget: global,
		clients: expirable.NewLRU(maxClientLimiters,
			func(_ string, l *ratelimiter.Limiter) { l.Close() },
			clientLimiterTTL),
	}
	rl.global.Store(newLimiter(global, window))
	return rl
}

func (rl *rateLimiters) client(ip string) *ratelimiter.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if l, ok := rl.clients.Get(ip); ok {
		// Get doesn't extend the entry's TTL; re-Add so eviction hits idle
		// clients only — an active client must not get a fresh budget every
		// clientLimiterTTL.
		rl.clients.Add(ip, l)
		return l
	}
	l := newLimiter(rl.clientBudget, rl.window)
	rl.clients.Add(ip, l)
	return l
}

// Drain recovers a limiter that has latched permanently locked. The library only
// expires old tasks inside Limit(), but the middleware fast-429s a locked limiter
// without calling it, so once a budget is exceeded nothing ever reclaims the
// weight: the global limiter stays 429'd until restart (whole data plane down),
// and a per-client limiter never evicts because client() refreshes its LRU TTL on
// every request. While a limiter is locked the middleware 429s every request, so
// no weight is spent — meaning after one window its sliding window is logically
// empty. drain detects a limiter that has stayed locked for a full window and
// replaces it with a fresh one. It never calls Limit(), so it can't race with the
// middleware on the (unsynchronised) task queue. Runs until ctx is cancelled.
func (rl *rateLimiters) Drain(ctx context.Context) {
	t := time.NewTicker(drainInterval)
	defer t.Stop()
	// lockedSince is drain-local (single goroutine), so it needs no synchronising.
	var globalLockedSince time.Time
	clientLockedSince := map[string]time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			globalLockedSince = rl.resetGlobalIfStuck(globalLockedSince, now)
			rl.resetStuckClients(clientLockedSince, now)
		}
	}
}

// resetGlobalIfStuck swaps in a fresh global limiter once the current one has
// been continuously locked for a full window, returning the updated lockedSince.
func (rl *rateLimiters) resetGlobalIfStuck(lockedSince, now time.Time) time.Time {
	if !rl.global.Load().IsLocked() {
		return time.Time{}
	}
	if lockedSince.IsZero() {
		return now
	}
	if now.Sub(lockedSince) >= rl.window {
		rl.global.Store(newLimiter(rl.globalBudget, rl.window))
		return time.Time{}
	}
	return lockedSince
}

// resetStuckClients replaces any per-client limiter that has been locked for a
// full window with a fresh one. lockedSince tracks per-IP first-seen-locked times
// and is pruned as clients recover or leave the LRU.
func (rl *rateLimiters) resetStuckClients(lockedSince map[string]time.Time, now time.Time) {
	present := make(map[string]struct{})
	for _, ip := range rl.clients.Keys() {
		lim, ok := rl.clients.Peek(ip) // Peek: don't refresh the entry's TTL
		if !ok || !lim.IsLocked() {
			continue
		}
		present[ip] = struct{}{}
		since, seen := lockedSince[ip]
		if !seen {
			lockedSince[ip] = now
			continue
		}
		if now.Sub(since) >= rl.window {
			rl.mu.Lock()
			rl.clients.Add(ip, newLimiter(rl.clientBudget, rl.window))
			rl.mu.Unlock()
			delete(lockedSince, ip)
		}
	}
	for ip := range lockedSince { // drop clients that recovered or aged out
		if _, ok := present[ip]; !ok {
			delete(lockedSince, ip)
		}
	}
}

// middleware rejects requests from exhausted clients with 429 + Retry-After
// (Limit alone would sleep the handler, holding the connection); under the
// threshold it spends the route's weight, which may briefly throttle a request
// that crosses the boundary.
func (rl *rateLimiters) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lim := rl.client(clientIP(r))
		// The federation control plane (our actor document, inbox, webfinger) is
		// exempt from the shared global budget: peers fetch our signing key from
		// the actor document to verify every delivery, so crawlers exhausting the
		// global budget must never lock it out (that breaks follow/reply/like).
		// The per-client budget still applies, so a single abusive IP is capped.
		ctrl := isControlPlane(r.URL.Path)
		global := rl.global.Load()
		if lim.IsLocked() || (!ctrl && global.IsLocked()) {
			w.Header().Set("Retry-After", strconv.Itoa(int(rl.window/time.Second)))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		weight := requestWeight(r)
		if ctrl {
			lim.Limit(weight, func() {
				next.ServeHTTP(w, r)
			})
			return
		}
		global.Limit(weight, func() {
			lim.Limit(weight, func() {
				next.ServeHTTP(w, r)
			})
		})
	})
}

// isControlPlane reports whether the path is part of the federation control
// plane (actor document, any inbox, webfinger) that peers must always reach to
// verify and deliver activities. Collections and media stay data-plane.
func isControlPlane(p string) bool {
	if p == pathInbox || p == pathActor || strings.HasPrefix(p, "/.well-known/webfinger") {
		return true
	}
	if strings.HasPrefix(p, PathUsers) {
		rest := strings.TrimPrefix(p, PathUsers)
		i := strings.IndexByte(rest, '/')
		if i < 0 {
			return true // actor document /users/{id}
		}
		sub := rest[i+1:]
		// /users/{id}/ (trailing slash) is also served as the actor document.
		return sub == "" || strings.HasPrefix(sub, "inbox")
	}
	return false
}

// clientIP keys the limiter. Tailscale Funnel's listener preserves the public
// source address, so RemoteAddr is the real client, not a proxy hop.
func clientIP(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

func requestWeight(r *http.Request) uint32 {
	p := r.URL.Path
	switch {
	case strings.HasPrefix(p, PathMedia):
		return weightMedia
	case p == pathInbox:
		return weightInbox
	case strings.HasPrefix(p, PathUsers):
		rest := strings.TrimPrefix(p, PathUsers)
		if i := strings.IndexByte(rest, '/'); i >= 0 && rest[i+1:] != "" {
			if strings.HasPrefix(rest[i+1:], "inbox") {
				return weightInbox
			}
			return weightCollection
		}
		return weightActor // actor document: one GetUser round-trip
	case strings.HasPrefix(p, "/.well-known/webfinger"):
		return weightActor // webfinger also resolves the user via the node
	}
	return weightStatic // nodeinfo, static assets
}
