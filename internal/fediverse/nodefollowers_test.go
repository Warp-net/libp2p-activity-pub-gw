// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package fediverse

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Warp-net/activity-pub-gw/internal/warp"
)

func TestNodeResponseError(t *testing.T) {
	t.Run("an error envelope is reported", func(t *testing.T) {
		bt := []byte(`{"code":500,"message":"boom"}`)
		err := warp.NodeResponseError(bt)
		if err == nil || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("a normal response is not an error", func(t *testing.T) {
		bt, _ := json.Marshal(warp.FollowersResponse{Followers: []string{"bob@m"}})
		if err := warp.NodeResponseError(bt); err != nil {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("a non-JSON body is left to the caller", func(t *testing.T) {
		if err := warp.NodeResponseError([]byte("not json")); err != nil {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("an empty message is not an error", func(t *testing.T) {
		if err := warp.NodeResponseError([]byte(`{"code":0,"message":""}`)); err != nil {
			t.Fatalf("err = %v", err)
		}
	})
}

// pagedRequester serves a fixed sequence of follower pages, recording the cursor
// it was asked for on each call.
type pagedRequester struct {
	pages   [][]byte
	call    int
	cursors []string
	err     error
}

func (p *pagedRequester) Request(string, any) ([]byte, error) {
	if p.err != nil {
		return nil, p.err
	}
	i := p.call
	p.call++
	if i >= len(p.pages) {
		return p.pages[len(p.pages)-1], nil
	}
	return p.pages[i], nil
}

func (p *pagedRequester) RequestUser(_, route string, payload any) ([]byte, error) {
	if ev, ok := payload.(warp.GetFollowersEvent); ok {
		c := ""
		if ev.Cursor != nil {
			c = *ev.Cursor
		}
		p.cursors = append(p.cursors, c)
	}
	return p.Request(route, payload)
}

func page(t *testing.T, cursor string, followers ...string) []byte {
	t.Helper()
	bt, err := json.Marshal(warp.FollowersResponse{Followers: followers, Cursor: cursor})
	if err != nil {
		t.Fatal(err)
	}
	return bt
}

func TestNodeFollowerStoreListPaginates(t *testing.T) {
	req := &pagedRequester{pages: [][]byte{
		page(t, "c1", "bob@m.example"),
		page(t, "", "carol@m.example"), // an empty cursor ends the walk
	}}
	s := nodeFollowerStore{req: req, resolver: stubResolver{byID: map[string]string{
		"bob@m.example":   "https://m.example/users/bob",
		"carol@m.example": "https://m.example/users/carol",
	}}}

	urls, err := s.List("alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 2 {
		t.Fatalf("urls = %v, want both pages", urls)
	}
	if len(req.cursors) != 2 || req.cursors[0] != "" || req.cursors[1] != "c1" {
		t.Fatalf("cursors = %v, want the second page requested with c1", req.cursors)
	}
}

func TestNodeFollowerStoreListStopsOnRepeatedCursor(t *testing.T) {
	// A node that keeps echoing the same cursor must not spin for maxPages.
	req := &pagedRequester{pages: [][]byte{page(t, "same", "bob@m.example")}}
	s := nodeFollowerStore{req: req, resolver: stubResolver{byID: map[string]string{
		"bob@m.example": "https://m.example/users/bob",
	}}}
	urls, err := s.List("alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 2 {
		t.Fatalf("urls = %v — the walk stops once the cursor repeats", urls)
	}
	if req.call != 2 {
		t.Fatalf("requests = %d, want the walk to stop at the repeated cursor", req.call)
	}
}

func TestNodeFollowerStoreListErrors(t *testing.T) {
	t.Run("a transport error propagates", func(t *testing.T) {
		boom := errors.New("unreachable")
		s := nodeFollowerStore{req: errRequester{err: boom}, resolver: stubResolver{}}
		if _, err := s.List("alice"); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("a node error envelope propagates", func(t *testing.T) {
		s := nodeFollowerStore{
			req:      rawRequester{body: []byte(`{"code":500,"message":"node down"}`)},
			resolver: stubResolver{},
		}
		_, err := s.List("alice")
		if err == nil || !strings.Contains(err.Error(), "node down") {
			t.Fatalf("err = %v — an error envelope must not read as an empty follower list", err)
		}
	})

	t.Run("a malformed page propagates", func(t *testing.T) {
		s := nodeFollowerStore{req: rawRequester{body: []byte(`["not","a","page"]`)}, resolver: stubResolver{}}
		if _, err := s.List("alice"); err == nil {
			t.Fatal("expected a decode error")
		}
	})

	t.Run("an unresolvable follower is skipped, not fatal", func(t *testing.T) {
		req := &pagedRequester{pages: [][]byte{page(t, "", "bob@m.example", "ghost@m.example")}}
		s := nodeFollowerStore{req: req, resolver: stubResolver{byID: map[string]string{
			"bob@m.example": "https://m.example/users/bob",
		}}}
		urls, err := s.List("alice")
		if err != nil {
			t.Fatal(err)
		}
		if len(urls) != 1 || urls[0] != "https://m.example/users/bob" {
			t.Fatalf("urls = %v, want the resolvable follower only", urls)
		}
	})
}

func TestNodeFollowerStoreAddErrors(t *testing.T) {
	boom := errors.New("unreachable")
	s := nodeFollowerStore{req: errRequester{err: boom}, resolver: stubResolver{}}
	if err := s.Add("alice", "https://m/users/bob"); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}

	s = nodeFollowerStore{req: rawRequester{body: []byte(`{"code":403,"message":"refused"}`)}, resolver: stubResolver{}}
	err := s.Add("alice", "https://m/users/bob")
	if err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("err = %v — a refused follow must not look like success", err)
	}
}

// resolveActorID is the store's resolver in production; a handle it cannot parse
// must surface as an error rather than a silently dropped follower.
func TestResolveActorIDRejectsMalformedHandles(t *testing.T) {
	g := testGateway(t)
	for _, in := range []string{"nohandle", "@", "@instance", "name@", ""} {
		if _, err := g.resolveActorID(context.Background(), in); err == nil {
			t.Fatalf("%q resolved, want an error", in)
		}
	}
}
