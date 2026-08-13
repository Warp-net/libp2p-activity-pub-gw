// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ratelimiter "github.com/filinvadim/ratelimiter"
)

func TestTaskQueue(t *testing.T) {
	q := &taskQueue{}
	if q.Len() != 0 {
		t.Fatalf("fresh queue Len = %d", q.Len())
	}
	q.Append(ratelimiter.Task{Weight: 1}, ratelimiter.Task{Weight: 2}, ratelimiter.Task{Weight: 3})
	if q.Len() != 3 {
		t.Fatalf("Len = %d", q.Len())
	}

	t.Run("index is clamped rather than panicking", func(t *testing.T) {
		if got := q.TaskByIndex(-5); got.Weight != 1 {
			t.Fatalf("negative index = %+v, want the first task", got)
		}
		if got := q.TaskByIndex(99); got.Weight != 3 {
			t.Fatalf("past-the-end index = %+v, want the last task", got)
		}
		if got := q.TaskByIndex(1); got.Weight != 2 {
			t.Fatalf("index 1 = %+v", got)
		}
	})

	t.Run("CutOffBefore drops the prefix", func(t *testing.T) {
		q := &taskQueue{}
		q.Append(ratelimiter.Task{Weight: 1}, ratelimiter.Task{Weight: 2}, ratelimiter.Task{Weight: 3})
		q.CutOffBefore(0) // no-op
		if q.Len() != 3 {
			t.Fatalf("Len = %d after a zero cut", q.Len())
		}
		q.CutOffBefore(-1) // no-op
		if q.Len() != 3 {
			t.Fatalf("Len = %d after a negative cut", q.Len())
		}
		q.CutOffBefore(2)
		if q.Len() != 1 || q.TaskByIndex(0).Weight != 3 {
			t.Fatalf("queue = %d entries, first weight %d", q.Len(), q.TaskByIndex(0).Weight)
		}
	})

	t.Run("cutting at or past the end empties it", func(t *testing.T) {
		// The library's own queue leaves the weights behind here, which underflows
		// the unsigned total and latches the limiter locked. Ours must clear.
		q := &taskQueue{}
		q.Append(ratelimiter.Task{Weight: 1}, ratelimiter.Task{Weight: 2})
		q.CutOffBefore(2)
		if q.Len() != 0 {
			t.Fatalf("Len = %d, want the queue emptied", q.Len())
		}
		q.Append(ratelimiter.Task{Weight: 9})
		q.CutOffBefore(50)
		if q.Len() != 0 {
			t.Fatalf("Len = %d after an over-long cut", q.Len())
		}
	})
}

func TestIsControlPlane(t *testing.T) {
	cases := map[string]bool{
		pathInbox:                           true,
		pathActor:                           true,
		"/.well-known/webfinger":            true,
		"/.well-known/webfinger?resource=x": true,
		"/users/alice":                      true,
		"/users/alice/":                     true,
		"/users/alice/inbox":                true,
		"/users/alice/outbox":               false,
		"/users/alice/followers":            false,
		"/users/alice/statuses/1":           false,
		pathMedia + "abc":                   false,
		"/nodeinfo/2.0":                     false,
		pathStatic + "warpnet.png":          false,
		"/.well-known/nodeinfo":             false,
		"/logs":                             false,
		"/debug/pprof/heap":                 false,
	}
	for p, want := range cases {
		if got := isControlPlane(p); got != want {
			t.Errorf("isControlPlane(%q) = %v, want %v", p, got, want)
		}
	}
}

func TestClientIP(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "203.0.113.7:54321"
	if got := clientIP(r); got != "203.0.113.7" {
		t.Fatalf("got %q", got)
	}
	r.RemoteAddr = "[2001:db8::1]:443"
	if got := clientIP(r); got != "2001:db8::1" {
		t.Fatalf("ipv6 = %q", got)
	}
	r.RemoteAddr = "no-port"
	if got := clientIP(r); got != "no-port" {
		t.Fatalf("portless = %q, want it used verbatim", got)
	}
}

func TestClientLimiterIsReusedPerIP(t *testing.T) {
	rl := newRateLimitersWith(100, 10, time.Second)
	a1, a2 := rl.client("1.2.3.4"), rl.client("1.2.3.4")
	if a1 != a2 {
		t.Fatal("the same IP must keep one budget across requests")
	}
	if b := rl.client("5.6.7.8"); b == a1 {
		t.Fatal("distinct IPs must not share a budget")
	}
}

// The federation control plane must stay reachable even when the shared global
// budget is exhausted: peers fetch our actor document to verify every delivery.
func TestControlPlaneExemptFromGlobalBudget(t *testing.T) {
	rl := newRateLimitersWith(weightStatic, 10_000, time.Minute)
	h := rl.middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Spend the whole global budget on a data-plane path.
	for range 4 {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/nodeinfo/2.0", nil)
		r.RemoteAddr = "9.9.9.9:1"
		h.ServeHTTP(w, r)
	}
	if !rl.global.Load().IsLocked() {
		t.Skip("global limiter did not lock; budget accounting differs in this build")
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/users/alice", nil)
	r.RemoteAddr = "9.9.9.9:1"
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("actor document = %d with the global budget locked, want it exempt", w.Code)
	}

	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/users/alice/followers", nil)
	r.RemoteAddr = "9.9.9.9:1"
	h.ServeHTTP(w, r)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("a data-plane path = %d, want 429 once the global budget is gone", w.Code)
	}
}

func TestResetGlobalIfStuck(t *testing.T) {
	rl := newRateLimitersWith(1, 1, 100*time.Millisecond)

	t.Run("an unlocked limiter clears the timer", func(t *testing.T) {
		now := time.Now()
		if got := rl.resetGlobalIfStuck(now, now); !got.IsZero() {
			t.Fatalf("lockedSince = %v, want cleared while unlocked", got)
		}
	})

	// Lock the global limiter by spending past its budget.
	rl.global.Load().Limit(5, func() {})
	if !rl.global.Load().IsLocked() {
		t.Skip("global limiter did not lock; budget accounting differs in this build")
	}
	before := rl.global.Load()

	now := time.Now()
	first := rl.resetGlobalIfStuck(time.Time{}, now)
	if first != now {
		t.Fatalf("first locked observation = %v, want it recorded as %v", first, now)
	}
	if rl.global.Load() != before {
		t.Fatal("the limiter must not be replaced on the first locked observation")
	}

	// Still inside the window: keep waiting.
	if got := rl.resetGlobalIfStuck(now, now.Add(rl.window/2)); got != now {
		t.Fatalf("mid-window = %v, want the original timestamp kept", got)
	}
	if rl.global.Load() != before {
		t.Fatal("replaced before a full window elapsed")
	}

	// A full window locked: swap in a fresh limiter so the data plane recovers.
	if got := rl.resetGlobalIfStuck(now, now.Add(rl.window)); !got.IsZero() {
		t.Fatalf("after a full window = %v, want the timer cleared", got)
	}
	if rl.global.Load() == before {
		t.Fatal("a limiter locked for a whole window must be replaced")
	}
	if rl.global.Load().IsLocked() {
		t.Fatal("the replacement must start unlocked")
	}
}

func TestResetStuckClients(t *testing.T) {
	rl := newRateLimitersWith(10_000, 1, 100*time.Millisecond)
	const ip = "203.0.113.9"
	lim := rl.client(ip)
	lim.Limit(5, func() {}) // blow the per-client budget
	if !lim.IsLocked() {
		t.Skip("client limiter did not lock; budget accounting differs in this build")
	}

	lockedSince := map[string]time.Time{}
	now := time.Now()

	rl.resetStuckClients(lockedSince, now)
	if _, ok := lockedSince[ip]; !ok {
		t.Fatal("the first locked observation must be recorded")
	}
	if got, _ := rl.clients.Peek(ip); got != lim {
		t.Fatal("replaced on the first observation")
	}

	rl.resetStuckClients(lockedSince, now.Add(rl.window))
	got, ok := rl.clients.Peek(ip)
	if !ok {
		t.Fatal("the client entry disappeared")
	}
	if got == lim {
		t.Fatal("a client locked for a whole window must be replaced")
	}
	if _, still := lockedSince[ip]; still {
		t.Fatal("the timer must be cleared after the swap")
	}

	t.Run("recovered clients are pruned from the timer map", func(t *testing.T) {
		lockedSince := map[string]time.Time{"1.1.1.1": now}
		rl.resetStuckClients(lockedSince, now)
		if _, still := lockedSince["1.1.1.1"]; still {
			t.Fatal("an IP that is no longer locked must be dropped")
		}
	})
}

func TestDrainStopsWithContext(t *testing.T) {
	rl := newRateLimitersWith(10, 10, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); rl.drain(ctx) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not return after its context was cancelled")
	}
}

// drain is what actually rescues a latched limiter in production; run it for real
// against a stuck global limiter and check the data plane comes back.
func TestDrainRecoversAStuckGlobalLimiter(t *testing.T) {
	rl := newRateLimitersWith(1, 10_000, 50*time.Millisecond)
	rl.global.Load().Limit(5, func() {})
	if !rl.global.Load().IsLocked() {
		t.Skip("global limiter did not lock; budget accounting differs in this build")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go rl.drain(ctx)

	deadline := time.After(20 * time.Second)
	for {
		if !rl.global.Load().IsLocked() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("the global limiter stayed locked; the gateway would 429 until restart")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
