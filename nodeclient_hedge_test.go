// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// TestTryMembersHedgesPastDeadPeer proves the fix for "very long requests": a
// first candidate that never answers must not stall the request for the whole
// budget — a reachable later candidate should win quickly via hedging.
func TestTryMembersHedgesPastDeadPeer(t *testing.T) {
	dead := peer.ID("dead-node")
	good := peer.ID("good-node")

	c := &nodeClient{}
	c.stream = func(ctx context.Context, p peer.ID, route string, payload any) ([]byte, error) {
		switch p {
		case good:
			return []byte("ok"), nil
		default: // dead: block until the attempt/overall context is cancelled
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	start := time.Now()
	bt, err := c.tryMembers(ctx, []peer.ID{dead, good}, routeGetUser, nil)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("tryMembers: unexpected error: %v", err)
	}
	if string(bt) != "ok" {
		t.Fatalf("tryMembers: got %q, want %q", bt, "ok")
	}
	// Should return shortly after the hedge fires, not after perPeerTimeout or
	// the full requestTimeout.
	if elapsed >= perPeerTimeout {
		t.Fatalf("tryMembers took %s; expected to hedge within ~%s", elapsed, hedgeDelay)
	}
	// The winning node should be remembered so later requests try it first.
	if len(c.good) == 0 || c.good[0] != good {
		t.Fatalf("tryMembers: winning peer not remembered: %v", c.good)
	}
}

// TestTryMembersAllFail returns an error (not a hang) when no candidate answers.
// The route is a write, so this also covers the un-hedged sequential walk.
func TestTryMembersAllFail(t *testing.T) {
	boom := errors.New("boom")
	c := &nodeClient{}
	c.stream = func(ctx context.Context, p peer.ID, route string, payload any) ([]byte, error) {
		return nil, boom
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	start := time.Now()
	_, err := c.tryMembers(ctx, []peer.ID{"a", "b", "c"}, "/route", nil)
	if err == nil {
		t.Fatal("tryMembers: expected error when all candidates fail")
	}
	if time.Since(start) >= requestTimeout {
		t.Fatalf("tryMembers: fast-failing candidates should not consume the whole budget")
	}
}
