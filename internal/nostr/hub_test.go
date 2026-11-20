// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package nostr

import (
	"net/http"
	"testing"

	"github.com/Warp-net/bridge-gateway/internal/warp"
)

type hubTestNode struct{ network string }

func (n hubTestNode) Request(string, any) ([]byte, error)             { return nil, nil }
func (n hubTestNode) RequestUser(string, string, any) ([]byte, error) { return nil, nil }
func (n hubTestNode) HomeUser(string) (warp.User, bool)               { return warp.User{}, false }
func (n hubTestNode) Followings(string) ([]string, bool)              { return nil, false }
func (n hubTestNode) ID() string                                      { return "12D3KooWGateway" }
func (n hubTestNode) NetworkName() string                             { return n.network }

type hubTestKeys struct{}

func (hubTestKeys) BridgeKey(string, string) ([]byte, bool) { return nil, false }

func TestHubPublishesOnlyWhenEnabled(t *testing.T) {
	pool := NewRelayPool(http.DefaultClient)
	n := hubTestNode{network: warp.DefaultNetwork}

	readOnly := NewHub(pool, hubTestKeys{}, testLinks{}, nil, "gw.example", false)
	b, pub := readOnly.NewBridge(n)
	if pub != nil || b.keys == nil {
		t.Fatal("a read-only hub still recognises own npubs but publishes nothing")
	}
	if got := readOnly.publishers(); len(got) != 0 {
		t.Fatalf("publishers = %v, want none", got)
	}

	publishing := NewHub(pool, hubTestKeys{}, testLinks{}, nil, "gw.example", true)
	b, pub = publishing.NewBridge(n)
	if pub == nil || pub.network != n.network || b.pub != pub {
		t.Fatal("a publishing hub writes for the bridge's network")
	}
	if got := publishing.publishers(); len(got) != 1 || got[0] != pub {
		t.Fatalf("publishers = %v, want the bridge's publisher", got)
	}
}
