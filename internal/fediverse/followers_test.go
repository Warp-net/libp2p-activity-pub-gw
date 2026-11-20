// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package fediverse

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/Warp-net/bridge-gateway/internal/warp"
)

func TestEncodeDecodeActorID(t *testing.T) {
	const actorURL = "https://mastodon.social/users/bob"
	id := encodeActorID(actorURL)

	if !strings.HasPrefix(id, warp.APFollowerPrefix) {
		t.Fatalf("id = %q, want the ap: tag", id)
	}
	// The id is a datastore key segment: '/' would split it into two levels.
	if strings.Contains(strings.TrimPrefix(id, warp.APFollowerPrefix), "/") {
		t.Fatalf("id = %q must not contain '/'", id)
	}
	got, err := decodeActorID(id)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != actorURL {
		t.Fatalf("round-trip = %q, want %q", got, actorURL)
	}
}

func TestDecodeActorIDRejectsNonAPIDs(t *testing.T) {
	if _, err := decodeActorID("01KSGHBHKG0N77T6A3RZV8WSH5"); !errors.Is(err, errNotAPFollower) {
		t.Fatalf("a native ULID: err = %v", err)
	}
	if _, err := decodeActorID(warp.APFollowerPrefix + "!!!not base64!!!"); err == nil {
		t.Fatal("malformed base64 must fail")
	}
}

func TestIsBridgedUserID(t *testing.T) {
	cases := map[string]bool{
		encodeActorID("https://m/users/bob"): true,  // gateway-minted follow-graph id
		"bob@mastodon.social":                true,  // attribution handle
		"01KSGHBHKG0N77T6A3RZV8WSH5":         false, // native Warpnet ULID
		"alice":                              false,
		"":                                   false,
	}
	for id, want := range cases {
		if got := warp.IsBridgedUserID(id); got != want {
			t.Errorf("isBridgedUserID(%q) = %v, want %v", id, got, want)
		}
	}
}

func TestMemFollowerStoreIsolatesUsersAndCopies(t *testing.T) {
	s := newMemFollowerStore()
	if err := s.Add("alice", "https://m/users/bob"); err != nil {
		t.Fatal(err)
	}
	if err := s.Add("carol", "https://m/users/dave"); err != nil {
		t.Fatal(err)
	}

	alice, err := s.List("alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(alice) != 1 || alice[0] != "https://m/users/bob" {
		t.Fatalf("alice = %v", alice)
	}
	if got, _ := s.List("carol"); len(got) != 1 || got[0] != "https://m/users/dave" {
		t.Fatalf("carol = %v", got)
	}
	if got, _ := s.List("nobody"); len(got) != 0 {
		t.Fatalf("unknown user = %v", got)
	}

	// List must hand back a copy; mutating it must not corrupt the store.
	alice[0] = "tampered"
	if got, _ := s.List("alice"); got[0] != "https://m/users/bob" {
		t.Fatalf("the store was mutated through a returned slice: %v", got)
	}
}

func TestMemFollowerStoreConcurrentAdd(t *testing.T) {
	s := newMemFollowerStore()
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Half the writers race on the same url so Add's read-modify-write is
			// exercised under contention as well.
			if i%2 == 0 {
				_ = s.Add("alice", "https://m/users/shared")
				return
			}
			_ = s.Add("alice", "https://m/users/bob")
		}()
	}
	wg.Wait()
	got, err := s.List("alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("followers = %v, want the two distinct urls exactly once each", got)
	}
}
