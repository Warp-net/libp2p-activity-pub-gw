// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/warp"
	"github.com/Warp-net/warpnet/event"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestNetworkEntries(t *testing.T) {
	for _, network := range []string{warp.DefaultNetwork, "testnet"} {
		entries, err := parseEntries(network)
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
		if _, err := parseEntries("nope"); !errors.Is(err, errNoEntryPeers) {
			t.Fatalf("err = %v, want errNoEntryPeers", err)
		}
	})
}

// connectNetwork must fail fast (rather than build a host) when asked for a
// network with no bootstrap relays.
func TestConnectNetworkRejectsAnUnknownNetwork(t *testing.T) {
	if _, err := NewNode(context.Background(), "definitely-not-a-warpnet", defaultGatewaySeed); !errors.Is(err, errNoEntryPeers) {
		t.Fatalf("err = %v, want errNoEntryPeers", err)
	}
}

// Every network the gateway can bootstrap into needs its own libp2p port: they
// are all joined in one process, so a shared address would leave the second node
// unable to listen and the gateway silently serving one network.
func TestP2PListenAddrsAreDistinct(t *testing.T) {
	seen := make(map[string]string, len(bootstrapByNetwork))
	for network := range bootstrapByNetwork {
		addr, ok := listenByNetwork[network]
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
	c.members = []peer.ID{member, member, relay, c.host.ID(), ""}

	got := c.memberCandidates(warp.RouteGetUser)
	// Relays serve discovery only, our own id is not a data peer, and duplicates
	// would just re-dial the same node.
	if len(got) != 1 || got[0] != member {
		t.Fatalf("candidates = %v, want only the member node once", got)
	}
}

func TestMemberCandidatesIsCapped(t *testing.T) {
	c := newTestNode(t, "cap")
	for i := range maxMemberCandidates + 10 {
		c.members = append(c.members, peer.ID("peer-"+strings.Repeat("x", i%7)+string(rune('a'+i%26))+string(rune('a'+i/26))))
	}
	if got := len(c.memberCandidates(warp.RouteGetUser)); got > maxMemberCandidates {
		t.Fatalf("candidates = %d, want at most %d", got, maxMemberCandidates)
	}
}

func TestRememberMovesTheAnsweringPeerToTheFront(t *testing.T) {
	c := &Node{members: []peer.ID{"a", "b", "c"}}
	c.remember("c")
	if len(c.members) != 3 || c.members[0] != "c" {
		t.Fatalf("good = %v, want c first and no duplicate", c.members)
	}
	c.remember("d")
	if len(c.members) != 4 || c.members[0] != "d" {
		t.Fatalf("good = %v", c.members)
	}

	t.Run("the cache stays bounded", func(t *testing.T) {
		c := &Node{}
		for i := range maxMemberCandidates + 20 {
			c.remember(peer.ID(string(rune('a'+i%26)) + string(rune('a'+i/26))))
		}
		if len(c.members) > maxMemberCandidates {
			t.Fatalf("good = %d entries, want at most %d", len(c.members), maxMemberCandidates)
		}
	})
}

func TestRequestWithoutDiscoveredMembers(t *testing.T) {
	c := newTestNode(t, "no-members")
	_, err := c.Request(warp.RouteGetUser, warp.GetUserEvent{UserId: "alice"})
	if err == nil || !strings.Contains(err.Error(), "no Warpnet member nodes discovered") {
		t.Fatalf("err = %v", err)
	}
}

// stubbedClient is a Node whose wire send is replaced, so owner routing can
// be exercised without a network.
func stubbedClient(t *testing.T, send func(peer.ID, string, any) ([]byte, error)) (*Node, *[]peer.ID) {
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
	owner := newTestNode(t, "owner-node").host.ID()
	member := peer.ID("member-1")

	c, targets := stubbedClient(t, func(p peer.ID, route string, _ any) ([]byte, error) {
		if route == warp.RouteGetUser {
			return json.Marshal(warp.User{Id: "alice", NodeId: owner.String()})
		}
		return []byte(`{"ok":true}`), nil
	})
	c.members = []peer.ID{member}

	// Routes like GET_FOLLOWERS are authoritative only on the owner node; a random
	// member answers with an empty list.
	if _, err := c.RequestUser("alice", warp.RouteGetFollowers, warp.GetFollowersEvent{UserId: "alice"}); err != nil {
		t.Fatalf("requestUser: %v", err)
	}
	got := *targets
	if len(got) < 2 || got[len(got)-1] != owner {
		t.Fatalf("targets = %v, want the last request sent to the owner %s", got, owner)
	}

	t.Run("the resolved owner is cached", func(t *testing.T) {
		before := len(*targets)
		if _, err := c.RequestUser("alice", warp.RouteGetFollowers, warp.GetFollowersEvent{UserId: "alice"}); err != nil {
			t.Fatal(err)
		}
		if got := len(*targets) - before; got != 1 {
			t.Fatalf("%d requests on a cached owner, want just the targeted one", got)
		}
	})

	t.Run("forgetOwner forces a re-resolve", func(t *testing.T) {
		c.owners.Remove("alice")
		before := len(*targets)
		if _, err := c.RequestUser("alice", warp.RouteGetFollowers, warp.GetFollowersEvent{UserId: "alice"}); err != nil {
			t.Fatal(err)
		}
		if got := len(*targets) - before; got < 2 {
			t.Fatalf("%d requests after forgetting, want the owner resolved again", got)
		}
	})
}

func TestRequestUserFallsBackToBroadcast(t *testing.T) {
	ownerNode := newTestNode(t, "unreachable-owner").host.ID()
	member := peer.ID("member-1")
	boom := errors.New("owner unreachable")

	c, targets := stubbedClient(t, func(p peer.ID, route string, _ any) ([]byte, error) {
		switch {
		case route == warp.RouteGetUser:
			return json.Marshal(warp.User{Id: "alice", NodeId: ownerNode.String()})
		case p == ownerNode:
			return nil, boom
		default:
			return []byte(`{"broadcast":true}`), nil
		}
	})
	c.members = []peer.ID{member}

	bt, err := c.RequestUser("alice", warp.RouteGetFollowers, warp.GetFollowersEvent{UserId: "alice"})
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
	if _, cached := c.owners.Get("alice"); cached {
		t.Fatal("a failed owner stayed cached")
	}
}

func TestOwnerNodeResolutionFailures(t *testing.T) {
	member := peer.ID("member-1")

	cases := map[string]func(string, any) ([]byte, error){
		"transport error": func(string, any) ([]byte, error) { return nil, errors.New("down") },
		"bad json":        func(string, any) ([]byte, error) { return []byte("not json"), nil },
		"no node id":      func(string, any) ([]byte, error) { return json.Marshal(warp.User{Id: "alice"}) },
		"bad node id": func(string, any) ([]byte, error) {
			return json.Marshal(warp.User{Id: "alice", NodeId: "not-a-peer-id"})
		},
	}
	for name, answer := range cases {
		t.Run(name, func(t *testing.T) {
			c := newTestNode(t, "owner-fail-"+name)
			c.members = []peer.ID{member}
			c.stream = func(_ context.Context, _ peer.ID, route string, payload any) ([]byte, error) {
				if route == warp.RouteGetUser {
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

func TestNodeClose(t *testing.T) {
	n := newTestNode(t, "closable")
	if err := n.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestNodeGetUser(t *testing.T) {
	member := peer.ID("member-1")

	newSource := func(t *testing.T, answer func() ([]byte, error)) *Node {
		t.Helper()
		c := newTestNode(t, "source-"+t.Name())
		c.members = []peer.ID{member}
		c.stream = func(context.Context, peer.ID, string, any) ([]byte, error) { return answer() }
		return c
	}

	t.Run("maps a warpnet profile onto the actor fields", func(t *testing.T) {
		s := newSource(t, func() ([]byte, error) {
			return json.Marshal(warp.User{
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
		s = newSource(t, func() ([]byte, error) { return json.Marshal(warp.User{}) })
		if _, ok := s.GetUser("alice"); ok {
			t.Fatal("an empty id must not resolve")
		}
	})
}

// streamToMember must still attempt the send when there is no DHT to resolve
// addresses through (the dev/no-discovery path), reporting the dial failure.
func TestStreamToMemberWithoutADHT(t *testing.T) {
	c := newTestNode(t, "no-dht")
	other := newTestNode(t, "unknown-peer")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.streamToMember(ctx, other.host.ID(), warp.RouteGetUser, warp.GetUserEvent{UserId: "alice"}); err == nil {
		t.Fatal("expected a dial error for a peer with no known addresses")
	}
}

// withDHT joins the node to an isolated Kademlia DHT so the discovery paths that
// only run when a routing table exists are exercised.
func withDHT(t *testing.T, c *Node) *Node {
	t.Helper()
	kdht, err := dht.New(c.host, dht.Mode(dht.ModeServer))
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
	if _, err := c.streamToMember(ctx, other.host.ID(), warp.RouteGetUser, warp.GetUserEvent{UserId: "alice"}); err == nil {
		t.Fatal("expected a failure for a peer no routing table knows")
	}
}

// An empty routing table must make request refresh once and then report that no
// member nodes are known — never block for the whole request budget.
func TestRequestRefreshesAnEmptyRoutingTable(t *testing.T) {
	c := withDHT(t, newTestNode(t, "dht-empty"))

	done := make(chan error, 1)
	go func() {
		_, err := c.Request(warp.RouteGetUser, warp.GetUserEvent{UserId: "alice"})
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

func rateLimitedBody(t *testing.T) []byte {
	t.Helper()
	bt, err := json.Marshal(event.ResponseError{
		Code: event.RateLimitErrorCode, Message: "middleware: too many requests for this route",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return bt
}

func TestTryMembersSkipsARateLimitedMember(t *testing.T) {
	limited, member := peer.ID("limited-node"), peer.ID("member-1")

	c, _ := stubbedClient(t, func(p peer.ID, _ string, _ any) ([]byte, error) {
		if p == limited {
			return rateLimitedBody(t), nil
		}
		return []byte(`{"id":"alice"}`), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	bt, err := c.tryMembers(ctx, []peer.ID{limited, member}, warp.RouteGetUser, nil)
	if err != nil {
		t.Fatalf("tryMembers: %v", err)
	}
	if string(bt) != `{"id":"alice"}` {
		t.Fatalf("tryMembers returned the rate-limit body: %s", bt)
	}
	if len(c.members) == 0 || c.members[0] != member {
		t.Fatalf("winning peer not remembered: %v", c.members)
	}
	if slices.Contains(c.members, limited) {
		t.Fatalf("rate-limited peer was remembered as members: %v", c.members)
	}
	if !c.isThrottled(warp.RouteGetUser, limited) {
		t.Fatal("rate-limited peer was not marked to cool down")
	}
	if c.isThrottled(warp.RouteGetTweets, limited) {
		t.Fatal("cooldown leaked to another route")
	}
}

func TestTryMembersAllRateLimited(t *testing.T) {
	c, _ := stubbedClient(t, func(peer.ID, string, any) ([]byte, error) {
		return rateLimitedBody(t), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	if _, err := c.tryMembers(ctx, []peer.ID{"a", "b"}, warp.RouteGetUser, nil); !errors.Is(err, errNodeRateLimited) {
		t.Fatalf("err = %v, want errNodeRateLimited", err)
	}
}

func TestTryMembersDoesNotHedgeWrites(t *testing.T) {
	release := make(chan struct{})
	var mu sync.Mutex
	var inFlight int

	c := &Node{}
	c.stream = func(ctx context.Context, _ peer.ID, _ string, _ any) ([]byte, error) {
		mu.Lock()
		inFlight++
		mu.Unlock()
		select {
		case <-release:
			return []byte(`{}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c.tryMembers(ctx, []peer.ID{"a", "b", "c"}, warp.RoutePostFollow, nil)
	}()

	time.Sleep(3 * hedgeDelay)
	mu.Lock()
	launched := inFlight
	mu.Unlock()
	close(release)
	<-done

	if launched != 1 {
		t.Fatalf("write route launched %d attempts, want 1 (no hedging)", launched)
	}
}

func TestMemberCandidatesPutCoolingMembersLast(t *testing.T) {
	c := newTestNode(t, "demote")
	cooling, ready := peer.ID("cooling"), peer.ID("ready")
	c.members = []peer.ID{cooling, ready}
	c.markThrottled(warp.RouteGetUser, cooling)

	got := c.memberCandidates(warp.RouteGetUser)
	if len(got) != 2 || got[0] != ready || got[1] != cooling {
		t.Fatalf("candidates = %v, want the cooling member last but kept", got)
	}
}

func TestGetUserCachesTheProfile(t *testing.T) {
	var calls int
	c, _ := stubbedClient(t, func(_ peer.ID, route string, _ any) ([]byte, error) {
		if route != warp.RouteGetUser {
			return []byte(`{}`), nil
		}
		calls++
		return json.Marshal(warp.User{Id: "alice", Username: "Alice"})
	})
	c.members = []peer.ID{peer.ID("member-1")}

	for range 3 {
		u, ok := c.GetUser("alice")
		if !ok || u.DisplayName != "Alice" {
			t.Fatalf("GetUser = %+v, %v", u, ok)
		}
	}
	if calls != 1 {
		t.Fatalf("GetUser hit the node %d times, want 1", calls)
	}
}

func TestGetUserDoesNotCacheAMiss(t *testing.T) {
	var calls int
	c, _ := stubbedClient(t, func(_ peer.ID, route string, _ any) ([]byte, error) {
		if route == warp.RouteGetUser {
			calls++
		}
		return []byte(`{}`), nil
	})
	c.members = []peer.ID{peer.ID("member-1")}

	for range 2 {
		if _, ok := c.GetUser("nobody"); ok {
			t.Fatal("GetUser: unknown handle reported as found")
		}
	}
	if calls != 2 {
		t.Fatalf("GetUser made %d node calls, want 2 (misses are not cached)", calls)
	}
}
