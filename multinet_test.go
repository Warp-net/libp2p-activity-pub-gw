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

func TestMultiNodeRacesReadsAcrossNetworks(t *testing.T) {
	main := newStubNet(t, "warpnet")
	test := newStubNet(t, "testnet")
	m := newMultiNode([]*nodeClient{main.client, test.client})

	t.Run("one network answering is enough", func(t *testing.T) {
		main.mu.Lock()
		main.fail = errors.New("warpnet down")
		main.mu.Unlock()
		if _, err := m.request(routeGetTweet, getTweetEvent{TweetId: "t1"}); err != nil {
			t.Fatalf("read = %v, want the healthy network's answer", err)
		}
	})

	t.Run("every network failing names them all", func(t *testing.T) {
		test.mu.Lock()
		test.fail = errors.New("testnet down")
		test.mu.Unlock()
		_, err := m.request(routeGetTweet, getTweetEvent{TweetId: "t1"})
		if err == nil {
			t.Fatal("want an error when no network answers")
		}
		if !strings.Contains(err.Error(), "warpnet") || !strings.Contains(err.Error(), "testnet") {
			t.Fatalf("err = %q, want both networks named", err)
		}
	})
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
