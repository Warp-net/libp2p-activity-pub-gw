// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"net/http"
	"testing"

	"github.com/Warp-net/bridge-gateway/internal/fediverse"
	"github.com/Warp-net/bridge-gateway/internal/warp"
	"github.com/Warp-net/warpnet/security"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/libp2p/go-libp2p"
	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// defaultGatewaySeed is the legacy public identity seed: anyone can claim its
// peer id, so no gateway may derive its identity from it.
const defaultGatewaySeed = "warpnet-activitypub-gateway"

// newTestNode builds an in-process node whose peer id comes from the key its
// envelopes are signed with, as the signature check requires.
func newTestNode(t *testing.T, seed string) *Node {
	t.Helper()
	privKey, err := security.GenerateKeyFromSeed([]byte(seed))
	if err != nil {
		t.Fatalf("seed key: %v", err)
	}
	p2pKey, err := p2pcrypto.UnmarshalEd25519PrivateKey(privKey)
	if err != nil {
		t.Fatalf("unmarshal key: %v", err)
	}
	h, err := libp2p.New(
		libp2p.Identity(p2pKey),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.DisableRelay(),
	)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	n := &Node{
		host:       h,
		privKey:    privKey,
		network:    warp.DefaultNetwork,
		relays:     map[peer.ID]struct{}{},
		owners:     expirable.NewLRU[string, peer.ID](ownersSize, nil, ownersTTL),
		bridgeKeys: expirable.NewLRU[peer.ID, []byte](ownersSize, nil, bridgeKeysTTL),
		throttled:  expirable.NewLRU[string, struct{}](throttledSize, nil, throttledTTL),
		users:      expirable.NewLRU[string, fediverse.WarpnetUser](usersSize, nil, usersTTL),
	}
	n.stream = n.streamToMember
	return n
}

// newTestGateway builds the ActivityPub front as main does, serving alice.
func newTestGateway(t *testing.T, cfg fediverse.Config) *fediverse.Gateway {
	t.Helper()
	t.Setenv("GATEWAY_AP_MIRROR", "")
	key, err := fediverse.LoadOrCreateKey(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	cfg.Host, cfg.Key, cfg.AllowPrivateTargets = "gw.example", key, true
	if cfg.Source == nil {
		cfg.Source = fediverse.StaticSource{User: fediverse.WarpnetUser{ID: "alice", PreferredUsername: "alice", DisplayName: "Alice"}}
	}
	cfg.Client = http.DefaultClient
	g, err := fediverse.New(cfg)
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	return g
}
