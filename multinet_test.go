// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

// stubNet is one network's node: its members know the listed users and record
// every route they are asked for, so a test can prove where a call landed.
type stubNet struct {
	client *nodeClient
	users  map[string]bool

	mu    sync.Mutex
	calls []string // routes this network was asked for
	fail  error    // when set, every route fails
}

func newStubNet(t *testing.T, network string, users ...string) *stubNet {
	t.Helper()
	s := &stubNet{users: map[string]bool{}}
	for _, u := range users {
		s.users[u] = true
	}
	c := newTestNode(t, "multinet-"+network+"-"+t.Name())
	c.network = network
	c.good = []peer.ID{peer.ID("member-" + network)}
	c.stream = func(_ context.Context, _ peer.ID, route string, payload any) ([]byte, error) {
		s.mu.Lock()
		s.calls = append(s.calls, route)
		fail := s.fail
		s.mu.Unlock()
		if fail != nil {
			return nil, fail
		}
		if route == routeGetUser {
			ev, _ := payload.(getUserEvent)
			if !s.users[string(ev.UserId)] {
				return []byte(`{}`), nil // this network doesn't have the user
			}
			// No node_id: requestUser then falls back to an in-network
			// broadcast, which is what the pre-multi-network code did too.
			return json.Marshal(user{Id: string(ev.UserId), Username: string(ev.UserId)})
		}
		return []byte(`{}`), nil
	}
	s.client = c
	return s
}

// routes returns the routes this network served, minus the GET_USER probes the
// gateway uses to locate a user.
func (s *stubNet) routes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.calls))
	for _, r := range s.calls {
		if r != routeGetUser {
			out = append(out, r)
		}
	}
	return out
}

func (s *stubNet) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = nil
}

func TestConfiguredNetworks(t *testing.T) {
	t.Run("unset joins the default network alone", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", "")
		if got := configuredNetworks(); len(got) != 1 || got[0] != defaultWarpnetNetwork {
			t.Fatalf("networks = %v, want just %s", got, defaultWarpnetNetwork)
		}
	})

	t.Run("a list joins every network, in order", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", "warpnet,testnet")
		got := configuredNetworks()
		if len(got) != 2 || got[0] != "warpnet" || got[1] != "testnet" {
			t.Fatalf("networks = %v", got)
		}
	})

	t.Run("whitespace and duplicates are ignored", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", " warpnet , testnet ,,warpnet")
		got := configuredNetworks()
		if len(got) != 2 || got[0] != "warpnet" || got[1] != "testnet" {
			t.Fatalf("networks = %v, want each network once", got)
		}
	})
}

// Each node has its own off switch, so one can be taken out of a running
// deployment without rewriting NODE_NETWORK.
func TestNetworkDisableFlags(t *testing.T) {
	t.Run("disables just the named network", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", "warpnet,testnet")
		t.Setenv("GATEWAY_DISABLE_TESTNET", "true")
		if got := configuredNetworks(); len(got) != 1 || got[0] != "warpnet" {
			t.Fatalf("networks = %v, want testnet dropped", got)
		}
	})

	t.Run("works for either node", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", "warpnet,testnet")
		t.Setenv("GATEWAY_DISABLE_WARPNET", "1")
		if got := configuredNetworks(); len(got) != 1 || got[0] != "testnet" {
			t.Fatalf("networks = %v, want warpnet dropped", got)
		}
	})

	t.Run("disabling every network leaves none", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", "warpnet,testnet")
		t.Setenv("GATEWAY_DISABLE_WARPNET", "true")
		t.Setenv("GATEWAY_DISABLE_TESTNET", "true")
		if got := configuredNetworks(); len(got) != 0 {
			t.Fatalf("networks = %v, want none", got)
		}
	})

	// A typo must not silently drop a network — only a real boolean switches
	// a node off.
	t.Run("an unparseable value keeps the node on", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", "warpnet,testnet")
		t.Setenv("GATEWAY_DISABLE_TESTNET", "maybe")
		if got := configuredNetworks(); len(got) != 2 {
			t.Fatalf("networks = %v, want both kept", got)
		}
	})

	t.Run("false keeps the node on", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", "testnet")
		t.Setenv("GATEWAY_DISABLE_TESTNET", "false")
		if got := configuredNetworks(); len(got) != 1 {
			t.Fatalf("networks = %v, want testnet kept", got)
		}
	})
}

func TestMultiNodeResolvesAUserOnWhicheverNetworkHasThem(t *testing.T) {
	main := newStubNet(t, "warpnet", "alice")
	test := newStubNet(t, "testnet", "bob")
	m := newMultiNode([]*nodeClient{main.client, test.client})

	for _, tc := range []struct{ user, network string }{{"alice", "warpnet"}, {"bob", "testnet"}} {
		u, ok := m.GetUser(tc.user)
		if !ok {
			t.Fatalf("%s did not resolve", tc.user)
		}
		if u.ID != tc.user {
			t.Fatalf("user = %+v, want %s", u, tc.user)
		}
		c, ok := m.locate(tc.user)
		if !ok || c.network != tc.network {
			t.Fatalf("%s located on %v, want %s", tc.user, c, tc.network)
		}
	}

	if _, ok := m.GetUser("nobody"); ok {
		t.Fatal("a user on no network must not resolve")
	}
}

// The whole point of locating a user: a user-scoped call must never touch
// another network, or a follow lands where nobody can read it back.
func TestMultiNodeConfinesUserScopedCallsToTheHomeNetwork(t *testing.T) {
	main := newStubNet(t, "warpnet", "alice")
	test := newStubNet(t, "testnet", "bob")
	m := newMultiNode([]*nodeClient{main.client, test.client})

	if _, err := m.requestUser("bob", routePostFollow, newFollowEvent{FollowingId: "bob"}); err != nil {
		t.Fatalf("follow for bob: %v", err)
	}
	if got := main.routes(); len(got) != 0 {
		t.Fatalf("warpnet served %v for a testnet user — the follow landed in the wrong network", got)
	}
	if got := test.routes(); len(got) != 1 || got[0] != routePostFollow {
		t.Fatalf("testnet routes = %v, want the follow", got)
	}

	main.reset()
	test.reset()

	// requestIn (inbound Fediverse writes) is confined the same way.
	if _, err := m.requestIn("alice", routePostReact, reactionEvent{UserId: "alice"}); err != nil {
		t.Fatalf("react for alice: %v", err)
	}
	if got := test.routes(); len(got) != 0 {
		t.Fatalf("testnet served %v for a warpnet user", got)
	}
	if got := main.routes(); len(got) != 1 || got[0] != routePostReact {
		t.Fatalf("warpnet routes = %v, want the reaction", got)
	}
}

// An unlocatable user must fail rather than have the write guess a network.
func TestMultiNodeRefusesAWriteForAnUnknownUser(t *testing.T) {
	main := newStubNet(t, "warpnet", "alice")
	test := newStubNet(t, "testnet", "bob")
	m := newMultiNode([]*nodeClient{main.client, test.client})

	for name, call := range map[string]func() error{
		"requestUser": func() error {
			_, err := m.requestUser("nobody", routePostFollow, newFollowEvent{})
			return err
		},
		"requestIn": func() error {
			_, err := m.requestIn("nobody", routePostReact, reactionEvent{})
			return err
		},
	} {
		main.reset()
		test.reset()
		err := call()
		if !errors.Is(err, errNoHomeNetwork) {
			t.Fatalf("%s err = %v, want errNoHomeNetwork", name, err)
		}
		// The error must name the networks tried, so /logs says where to look.
		if !strings.Contains(err.Error(), "warpnet") || !strings.Contains(err.Error(), "testnet") {
			t.Fatalf("%s err = %q, want the joined networks named", name, err)
		}
		if len(main.routes()) != 0 || len(test.routes()) != 0 {
			t.Fatalf("%s attempted the write anyway: warpnet=%v testnet=%v",
				name, main.routes(), test.routes())
		}
	}
}

// With one network there is no wrong network to reach, so a user-scoped call must
// not be gated on locating the profile first — that would fail calls that worked
// before multi-network support.
func TestMultiNodeWithOneNetworkSkipsLocation(t *testing.T) {
	only := newStubNet(t, "warpnet") // knows no users at all
	m := newMultiNode([]*nodeClient{only.client})

	if _, err := m.requestUser("ghost", routePostFollow, newFollowEvent{}); err != nil {
		t.Fatalf("single-network requestUser: %v", err)
	}
	if got := only.routes(); len(got) != 1 || got[0] != routePostFollow {
		t.Fatalf("routes = %v, want the follow served anyway", got)
	}
}

// A request with no user to route by must not be served by racing the networks:
// whichever answered first would be speaking for a user who may live on the
// other one. It fails closed instead, so a leak cannot be reintroduced by a
// caller that simply forgets to name the user.
func TestMultiNodeRefusesARequestItCannotRoute(t *testing.T) {
	main := newStubNet(t, "warpnet", "alice")
	test := newStubNet(t, "testnet", "bob")
	m := newMultiNode([]*nodeClient{main.client, test.client})

	_, err := m.request(routeGetTweet, getTweetEvent{TweetId: "t1"})
	if !errors.Is(err, errNoRoutingUser) {
		t.Fatalf("err = %v, want errNoRoutingUser", err)
	}
	if len(main.routes()) != 0 || len(test.routes()) != 0 {
		t.Fatalf("asked a network anyway: warpnet=%v testnet=%v", main.routes(), test.routes())
	}

	// Named, it is served — by that user's network only.
	if _, err := m.requestIn("bob", routeGetTweet, getTweetEvent{TweetId: "t1"}); err != nil {
		t.Fatalf("routed read: %v", err)
	}
	if len(main.routes()) != 0 {
		t.Fatalf("warpnet served %v for a testnet user", main.routes())
	}
	if got := test.routes(); len(got) != 1 || got[0] != routeGetTweet {
		t.Fatalf("testnet routes = %v", got)
	}
}

// With a single network there is no other network to reach, so an unrouted
// request is still served — the pre-multi-network behaviour.
func TestMultiNodeWithOneNetworkServesUnroutedRequests(t *testing.T) {
	only := newStubNet(t, "warpnet")
	m := newMultiNode([]*nodeClient{only.client})

	if _, err := m.request(routeGetTweet, getTweetEvent{TweetId: "t1"}); err != nil {
		t.Fatalf("single-network read: %v", err)
	}
	if got := only.routes(); len(got) != 1 || got[0] != routeGetTweet {
		t.Fatalf("routes = %v", got)
	}
}

// A requester that spans no networks (the single-node client, or a test fake)
// keeps the plain broadcast behaviour.
func TestRequestForUserFallsBackToBroadcast(t *testing.T) {
	g := testGateway(t)
	fake := &fakeRequester{}
	g.req = fake

	if _, err := g.requestForUser("alice", routePostReact, reactionEvent{}); err != nil {
		t.Fatalf("broadcast fallback: %v", err)
	}
	if fake.lastRoute != routePostReact {
		t.Fatalf("route = %q, want the reaction broadcast", fake.lastRoute)
	}
}

// The public ActivityPub surface is shared by every network, but each url names
// its owner — so serving one must never read from another user's network. A
// testnet node answering for a mainnet user's status or avatar is exactly the
// cross-network leak this guards.
func TestPublicSurfaceReadsStayOnTheOwnersNetwork(t *testing.T) {
	main := newStubNet(t, "warpnet", "alice")
	test := newStubNet(t, "testnet", "bob")
	m := newMultiNode([]*nodeClient{main.client, test.client})

	g := testGateway(t)
	g.req = m
	g.source = m
	srv := httptest.NewServer(g.routes())
	defer srv.Close()

	cases := map[string]struct {
		path            string
		owner, intruder *stubNet
	}{
		"a mainnet user's status is not read from testnet": {
			path: pathUsers + "alice" + pathStatuses + "t1", owner: main, intruder: test,
		},
		"a testnet user's status is not read from mainnet": {
			path: pathUsers + "bob" + pathStatuses + "t2", owner: test, intruder: main,
		},
		"a mainnet user's media is not read from testnet": {
			path: pathMedia + encodeMediaRef("alice", "avatar-key"), owner: main, intruder: test,
		},
		"a testnet user's media is not read from mainnet": {
			path: pathMedia + encodeMediaRef("bob", "avatar-key"), owner: test, intruder: main,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			main.reset()
			test.reset()

			resp, err := http.Get(srv.URL + tc.path)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()

			if got := tc.intruder.routes(); len(got) != 0 {
				t.Fatalf("%s was asked %v for a user it does not serve", tc.intruder.client.network, got)
			}
			if got := tc.owner.routes(); len(got) == 0 {
				t.Fatalf("%s was never asked, so the read went nowhere", tc.owner.client.network)
			}
		})
	}
}
