// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestNetworkEntries(t *testing.T) {
	for _, network := range []string{defaultWarpnetNetwork, "testnet"} {
		entries, err := networkEntries(network)
		if err != nil {
			t.Fatalf("%s: %v", network, err)
		}
		if len(entries) != len(bootstrapByNetwork[network]) {
			t.Fatalf("%s: %d entries, want %d — a bad bootstrap string was silently dropped",
				network, len(entries), len(bootstrapByNetwork[network]))
		}
		for _, e := range entries {
			if e.ID == "" || len(e.Addrs) == 0 {
				t.Fatalf("%s: incomplete entry %+v", network, e)
			}
		}
	}

	t.Run("an unknown network has no entry peers", func(t *testing.T) {
		if _, err := networkEntries("nope"); !errors.Is(err, errNoEntryPeers) {
			t.Fatalf("err = %v, want errNoEntryPeers", err)
		}
	})
}

// connectNetwork must fail fast (rather than build a host) when asked for a
// network with no bootstrap relays.
func TestConnectNetworkRejectsAnUnknownNetwork(t *testing.T) {
	if _, err := connectNetwork(context.Background(), "definitely-not-a-warpnet"); !errors.Is(err, errNoEntryPeers) {
		t.Fatalf("err = %v, want errNoEntryPeers", err)
	}
}

// Every network the gateway can bootstrap into needs its own libp2p port: they
// are all joined in one process, so a shared address would leave the second node
// unable to listen and the gateway silently serving one network.
func TestP2PListenAddrsAreDistinct(t *testing.T) {
	seen := make(map[string]string, len(bootstrapByNetwork))
	for network := range bootstrapByNetwork {
		addr, ok := p2pListenByNetwork[network]
		if !ok {
			t.Fatalf("network %q has bootstrap peers but no listen address", network)
		}
		if other, dup := seen[addr]; dup {
			t.Fatalf("networks %q and %q share listen address %s", network, other, addr)
		}
		seen[addr] = network
	}
}

func TestMemberCandidates(t *testing.T) {
	c := newTestNode(t, "candidates")
	relay := peer.ID("relay-1")
	c.relays[relay] = struct{}{}

	member := peer.ID("member-1")
	c.good = []peer.ID{member, member, relay, c.h.ID(), ""}

	got := c.memberCandidates()
	// Relays serve discovery only, our own id is not a data peer, and duplicates
	// would just re-dial the same node.
	if len(got) != 1 || got[0] != member {
		t.Fatalf("candidates = %v, want only the member node once", got)
	}
}

func TestMemberCandidatesIsCapped(t *testing.T) {
	c := newTestNode(t, "cap")
	for i := range maxMemberCandidates + 10 {
		c.good = append(c.good, peer.ID("peer-"+strings.Repeat("x", i%7)+string(rune('a'+i%26))+string(rune('a'+i/26))))
	}
	if got := len(c.memberCandidates()); got > maxMemberCandidates {
		t.Fatalf("candidates = %d, want at most %d", got, maxMemberCandidates)
	}
}

func TestRememberMovesTheAnsweringPeerToTheFront(t *testing.T) {
	c := &nodeClient{good: []peer.ID{"a", "b", "c"}}
	c.remember("c")
	if len(c.good) != 3 || c.good[0] != "c" {
		t.Fatalf("good = %v, want c first and no duplicate", c.good)
	}
	c.remember("d")
	if len(c.good) != 4 || c.good[0] != "d" {
		t.Fatalf("good = %v", c.good)
	}

	t.Run("the cache stays bounded", func(t *testing.T) {
		c := &nodeClient{}
		for i := range maxMemberCandidates + 20 {
			c.remember(peer.ID(string(rune('a'+i%26)) + string(rune('a'+i/26))))
		}
		if len(c.good) > maxMemberCandidates {
			t.Fatalf("good = %d entries, want at most %d", len(c.good), maxMemberCandidates)
		}
	})
}

func TestRequestWithoutDiscoveredMembers(t *testing.T) {
	c := newTestNode(t, "no-members")
	_, err := c.request(routeGetUser, getUserEvent{UserId: "alice"})
	if err == nil || !strings.Contains(err.Error(), "no Warpnet member nodes discovered") {
		t.Fatalf("err = %v", err)
	}
}

// stubbedClient is a nodeClient whose wire send is replaced, so owner routing can
// be exercised without a network.
func stubbedClient(t *testing.T, send func(peer.ID, string, any) ([]byte, error)) (*nodeClient, *[]peer.ID) {
	t.Helper()
	c := newTestNode(t, "stubbed-"+t.Name())
	var mu sync.Mutex
	var targets []peer.ID
	c.stream = func(_ context.Context, p peer.ID, route string, payload any) ([]byte, error) {
		mu.Lock()
		targets = append(targets, p)
		mu.Unlock()
		return send(p, route, payload)
	}
	return c, &targets
}

func TestRequestUserTargetsTheOwnerNode(t *testing.T) {
	owner := newTestNode(t, "owner-node").h.ID()
	member := peer.ID("member-1")

	c, targets := stubbedClient(t, func(p peer.ID, route string, _ any) ([]byte, error) {
		if route == routeGetUser {
			return json.Marshal(user{Id: "alice", NodeId: owner.String()})
		}
		return []byte(`{"ok":true}`), nil
	})
	c.good = []peer.ID{member}

	// Routes like GET_FOLLOWERS are authoritative only on the owner node; a random
	// member answers with an empty list.
	if _, err := c.requestUser("alice", routeGetFollowers, getFollowersEvent{UserId: "alice"}); err != nil {
		t.Fatalf("requestUser: %v", err)
	}
	got := *targets
	if len(got) < 2 || got[len(got)-1] != owner {
		t.Fatalf("targets = %v, want the last request sent to the owner %s", got, owner)
	}

	t.Run("the resolved owner is cached", func(t *testing.T) {
		before := len(*targets)
		if _, err := c.requestUser("alice", routeGetFollowers, getFollowersEvent{UserId: "alice"}); err != nil {
			t.Fatal(err)
		}
		if got := len(*targets) - before; got != 1 {
			t.Fatalf("%d requests on a cached owner, want just the targeted one", got)
		}
	})

	t.Run("forgetOwner forces a re-resolve", func(t *testing.T) {
		c.forgetOwner("alice")
		before := len(*targets)
		if _, err := c.requestUser("alice", routeGetFollowers, getFollowersEvent{UserId: "alice"}); err != nil {
			t.Fatal(err)
		}
		if got := len(*targets) - before; got < 2 {
			t.Fatalf("%d requests after forgetting, want the owner resolved again", got)
		}
	})
}

func TestRequestUserFallsBackToBroadcast(t *testing.T) {
	ownerNode := newTestNode(t, "unreachable-owner").h.ID()
	member := peer.ID("member-1")
	boom := errors.New("owner unreachable")

	c, targets := stubbedClient(t, func(p peer.ID, route string, _ any) ([]byte, error) {
		switch {
		case route == routeGetUser:
			return json.Marshal(user{Id: "alice", NodeId: ownerNode.String()})
		case p == ownerNode:
			return nil, boom
		default:
			return []byte(`{"broadcast":true}`), nil
		}
	})
	c.good = []peer.ID{member}

	bt, err := c.requestUser("alice", routeGetFollowers, getFollowersEvent{UserId: "alice"})
	if err != nil {
		t.Fatalf("requestUser: %v", err)
	}
	if !strings.Contains(string(bt), "broadcast") {
		t.Fatalf("body = %s, want the broadcast answer", bt)
	}
	if got := *targets; got[len(got)-1] == ownerNode {
		t.Fatalf("targets = %v, want the last attempt broadcast, not the dead owner", got)
	}
	// A failed owner must be dropped so the next call re-resolves it.
	if _, cached := c.owner.Get("alice"); cached {
		t.Fatal("a failed owner stayed cached")
	}
}

func TestOwnerNodeResolutionFailures(t *testing.T) {
	member := peer.ID("member-1")

	cases := map[string]func(string, any) ([]byte, error){
		"transport error": func(string, any) ([]byte, error) { return nil, errors.New("down") },
		"bad json":        func(string, any) ([]byte, error) { return []byte("not json"), nil },
		"no node id":      func(string, any) ([]byte, error) { return json.Marshal(user{Id: "alice"}) },
		"bad node id": func(string, any) ([]byte, error) {
			return json.Marshal(user{Id: "alice", NodeId: "not-a-peer-id"})
		},
	}
	for name, answer := range cases {
		t.Run(name, func(t *testing.T) {
			c := newTestNode(t, "owner-fail-"+name)
			c.good = []peer.ID{member}
			c.stream = func(_ context.Context, _ peer.ID, route string, payload any) ([]byte, error) {
				if route == routeGetUser {
					return answer(route, payload)
				}
				return []byte(`{}`), nil
			}
			if _, ok := c.ownerNode("alice"); ok {
				t.Fatal("owner must not resolve")
			}
		})
	}
}

func TestNodeClientClose(t *testing.T) {
	var nilClient *nodeClient
	nilClient.close() // must not panic

	c := &nodeClient{owner: expirable.NewLRU[string, peer.ID](1, nil, ownerCacheTTL)}
	c.close() // no host, no dht

	real := newTestNode(t, "closable")
	real.close()
}

func TestNodeSourceGetUser(t *testing.T) {
	member := peer.ID("member-1")

	newSource := func(t *testing.T, answer func() ([]byte, error)) nodeSource {
		t.Helper()
		c := newTestNode(t, "source-"+t.Name())
		c.good = []peer.ID{member}
		c.stream = func(context.Context, peer.ID, string, any) ([]byte, error) { return answer() }
		return nodeSource{client: c}
	}

	t.Run("maps a warpnet profile onto the actor fields", func(t *testing.T) {
		s := newSource(t, func() ([]byte, error) {
			return json.Marshal(user{
				Id: "alice", Username: "Alice", Bio: "hi",
				AvatarKey: "avatar", BackgroundImageKey: "bg",
			})
		})
		u, ok := s.GetUser("alice")
		if !ok {
			t.Fatal("not ok")
		}
		if u.ID != "alice" || u.PreferredUsername != "alice" || u.DisplayName != "Alice" {
			t.Fatalf("user = %+v", u)
		}
		if u.Summary != "hi" || u.Avatar != "avatar" || u.Background != "bg" {
			t.Fatalf("profile = %+v", u)
		}
	})

	t.Run("an unreachable network yields no user", func(t *testing.T) {
		s := newSource(t, func() ([]byte, error) { return nil, errors.New("down") })
		if _, ok := s.GetUser("alice"); ok {
			t.Fatal("expected no user")
		}
	})

	t.Run("a malformed or empty profile yields no user", func(t *testing.T) {
		s := newSource(t, func() ([]byte, error) { return []byte("not json"), nil })
		if _, ok := s.GetUser("alice"); ok {
			t.Fatal("bad json must not resolve")
		}
		s = newSource(t, func() ([]byte, error) { return json.Marshal(user{}) })
		if _, ok := s.GetUser("alice"); ok {
			t.Fatal("an empty id must not resolve")
		}
	})
}

func TestStaticSource(t *testing.T) {
	s := staticSource{user: warpnetUser{ID: "alice", PreferredUsername: "alice", DisplayName: "Alice"}}
	if u, ok := s.GetUser("alice"); !ok || u.DisplayName != "Alice" {
		t.Fatalf("got (%+v, %v)", u, ok)
	}
	if _, ok := s.GetUser("bob"); ok {
		t.Fatal("a static source serves exactly one user")
	}
	if _, ok := (staticSource{}).GetUser(""); !ok {
		t.Fatal("the empty fallback matches the empty username")
	}
}

// streamToMember must still attempt the send when there is no DHT to resolve
// addresses through (the dev/no-discovery path), reporting the dial failure.
func TestStreamToMemberWithoutADHT(t *testing.T) {
	c := newTestNode(t, "no-dht")
	other := newTestNode(t, "unknown-peer")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.streamToMember(ctx, other.h.ID(), routeGetUser, getUserEvent{UserId: "alice"}); err == nil {
		t.Fatal("expected a dial error for a peer with no known addresses")
	}
}

// withDHT joins the node to an isolated Kademlia DHT so the discovery paths that
// only run when a routing table exists are exercised.
func withDHT(t *testing.T, c *nodeClient) *nodeClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	kdht, err := dht.New(ctx, c.h, dht.Mode(dht.ModeServer))
	if err != nil {
		t.Fatalf("dht: %v", err)
	}
	t.Cleanup(func() { _ = kdht.Close() })
	c.dht = kdht
	return c
}

// With a DHT but no known addresses, streamToMember must try to resolve the peer
// through it and then report the failure rather than hang.
func TestStreamToMemberResolvesThroughTheDHT(t *testing.T) {
	c := withDHT(t, newTestNode(t, "dht-resolver"))
	other := newTestNode(t, "dht-unknown")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := c.streamToMember(ctx, other.h.ID(), routeGetUser, getUserEvent{UserId: "alice"}); err == nil {
		t.Fatal("expected a failure for a peer no routing table knows")
	}
}

// An empty routing table must make request refresh once and then report that no
// member nodes are known — never block for the whole request budget.
func TestRequestRefreshesAnEmptyRoutingTable(t *testing.T) {
	c := withDHT(t, newTestNode(t, "dht-empty"))

	done := make(chan error, 1)
	go func() {
		_, err := c.request(routeGetUser, getUserEvent{UserId: "alice"})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "no Warpnet member nodes discovered") {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(requestTimeout):
		t.Fatal("request did not give up on an empty routing table")
	}
}
