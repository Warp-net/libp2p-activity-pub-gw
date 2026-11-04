// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"net/http"
	"testing"

	"github.com/Warp-net/warpnet/event"
)

func TestWhoToFollowDirectory(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	t.Setenv("GATEWAY_DISCOVERY_HOST", f.host())
	f.on("/api/v1/directory", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("order") != "active" || q.Get("local") != "false" || q.Get("limit") != "2" {
			t.Errorf("query = %v", q)
		}
		if q.Get("offset") == "0" {
			writeJSON(w, "application/json", []any{
				map[string]any{"acct": "alice", "username": "alice", "display_name": "Alice", "avatar": f.url("/alice.png")},
				map[string]any{"acct": "bob@remote.example", "username": "bob"},
			})
			return
		}
		if q.Get("offset") != "2" {
			t.Errorf("offset = %q", q.Get("offset"))
		}
		writeJSON(w, "application/json", []any{})
	})
	limit := uint64(2)
	first, err := b.GetWhoToFollow(context.Background(), getAllUsersEvent{Limit: &limit})
	if err != nil || len(first.Users) != 2 || first.Cursor != "directory:2" {
		t.Fatalf("first = %+v, err = %v", first, err)
	}
	if u := first.Users[0]; u.Id != "alice@"+f.host() || u.Username != "Alice" || u.NodeId != b.nodeID || u.Network != mastodonNetwork || u.AvatarKey != f.url("/alice.png") {
		t.Fatalf("local account = %+v", u)
	}
	if first.Users[1].Id != "bob@remote.example" {
		t.Fatalf("remote account = %+v", first.Users[1])
	}
	if _, err := b.GetWhoToFollow(context.Background(), getAllUsersEvent{Limit: &limit}); err != nil || f.hitCount("/api/v1/directory") != 1 {
		t.Fatalf("cache: hits=%d, err=%v", f.hitCount("/api/v1/directory"), err)
	}
	last, err := b.GetWhoToFollow(context.Background(), getAllUsersEvent{Limit: &limit, Cursor: &first.Cursor})
	if err != nil || len(last.Users) != 0 || last.Cursor != "end" {
		t.Fatalf("last = %+v, err = %v", last, err)
	}
	if _, err := b.GetWhoToFollow(context.Background(), getAllUsersEvent{Cursor: &last.Cursor}); err != nil || f.hitCount("/api/v1/directory") != 2 {
		t.Fatalf("end cursor: hits=%d, err=%v", f.hitCount("/api/v1/directory"), err)
	}
}

func TestWhoToFollowDirectoryFiltering(t *testing.T) {
	b, g, f := newBridgeFixture(t)
	t.Setenv("GATEWAY_DISCOVERY_HOST", f.host())
	f.serveDoc("/api/v1/directory", "application/json", []any{
		nil, "invalid", map[string]any{},
		map[string]any{"acct": "alice", "username": "alice"},
		map[string]any{"acct": "alice", "username": "alice"},
		map[string]any{"acct": "suspended", "suspended": true},
		map[string]any{"acct": "moved", "moved": map[string]any{"acct": "elsewhere"}},
		map[string]any{"acct": "owner@" + g.host},
		map[string]any{"acct": "01KTRA1QJ8M2W7Y4ZB6C9D3E5F@another-gateway.example"},
		map[string]any{"acct": "@invalid.example"},
		map[string]any{"acct": "alice@"},
		map[string]any{"acct": "alice@remote@example.com"},
	})
	resp, err := b.GetWhoToFollow(context.Background(), getAllUsersEvent{})
	if err != nil || len(resp.Users) != 1 || resp.Users[0].Id != "alice@"+f.host() {
		t.Fatalf("filtered = %+v, err=%v", resp, err)
	}
}

func TestWhoToFollowDirectoryFailureAndLimits(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	t.Setenv("GATEWAY_DISCOVERY_HOST", f.host())
	f.on("/api/v1/directory", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("limit") != "80" {
			t.Errorf("limit = %q", r.URL.Query().Get("limit"))
		}
		http.Error(w, "unavailable", http.StatusForbidden)
	})
	limit := ^uint64(0)
	if _, err := b.GetWhoToFollow(context.Background(), getAllUsersEvent{Limit: &limit}); err == nil {
		t.Fatal("expected remote error")
	}
	for _, cursor := range []string{"https://evil.example", "directory:-1", "directory:18446744073709551615"} {
		if _, err := b.GetWhoToFollow(context.Background(), getAllUsersEvent{Cursor: &cursor}); err == nil {
			t.Fatalf("accepted invalid cursor %q", cursor)
		}
	}
	t.Setenv("GATEWAY_DISCOVERY_HOST", "")
	resp, err := b.GetWhoToFollow(context.Background(), getAllUsersEvent{})
	if err != nil || len(resp.Users) != 0 || resp.Cursor != "end" || f.hitCount("/api/v1/directory") != 1 {
		t.Fatalf("disabled = %+v, err=%v", resp, err)
	}
}

func TestNodeServerWhoToFollow(t *testing.T) {
	fx := newNodeFixture(t)
	t.Setenv("GATEWAY_DISCOVERY_HOST", fx.peer.host())
	fx.peer.serveDoc("/api/v1/directory", "application/json", []any{map[string]any{"acct": "alice", "username": "Alice"}})
	for _, route := range []string{event.PUBLIC_GET_WHOTOFOLLOW, routeGetUsers} {
		var resp usersResponse
		fx.call(t, route, getAllUsersEvent{UserId: defaultOwnerHandle}, &resp)
		if len(resp.Users) != 1 || resp.Users[0].NodeId != fx.server.h.ID().String() {
			t.Fatalf("%s response = %+v", route, resp)
		}
	}
}
