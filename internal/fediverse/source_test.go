// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package fediverse

import "testing"

func TestStaticSource(t *testing.T) {
	s := StaticSource{User: WarpnetUser{ID: "alice", PreferredUsername: "alice", DisplayName: "Alice"}}
	if u, ok := s.GetUser("alice"); !ok || u.DisplayName != "Alice" {
		t.Fatalf("got (%+v, %v)", u, ok)
	}
	if _, ok := s.GetUser("bob"); ok {
		t.Fatal("a static source serves exactly one user")
	}
	if _, ok := (StaticSource{}).GetUser(""); !ok {
		t.Fatal("the empty fallback matches the empty username")
	}
}
