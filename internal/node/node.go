/*

 Warpnet - Decentralized Social Network
 Copyright (C) 2025 Vadim Filin, https://github.com/Warp-net,
 <github.com.mecdy@passmail.net>

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <https://www.gnu.org/licenses/>.

WarpNet is provided “as is” without warranty of any kind, either expressed or implied.
Use at your own risk. The maintainers shall not be liable for any damages or data loss
resulting from the use or misuse of this software.
*/

// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/Warp-net/bridge-gateway/internal/fediverse"
	"github.com/Warp-net/bridge-gateway/internal/warp"
	camouflage "github.com/Warp-net/libp2p-camouflage-transport"
	"github.com/Warp-net/warpnet/security"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/pnet"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	noise "github.com/libp2p/go-libp2p/p2p/security/noise"
)

const (
	dialTimeout      = time.Minute
	bootstrapTimeout = 20 * time.Second

	ownersSize = 1024
	ownersTTL  = 10 * time.Minute

	bridgeKeysTTL = time.Hour

	throttledSize = 2048
	throttledTTL  = 15 * time.Second

	usersSize = 2048
	usersTTL  = time.Minute
)

var (
	errNoEntryReachable = errors.New("nodeclient: no Warpnet entry peer reachable")
	errNoListenAddr     = errors.New("no libp2p listen address (add one to listenByNetwork)")
)

// Node is the gateway's member peer in one Warpnet network. It joins the DHT
// through the network's relays, streams the public routes to the member nodes
// it discovers there, and answers the routes the gateway serves.
type Node struct {
	host    host.Host
	privKey ed25519.PrivateKey
	dht     *dht.IpfsDHT
	network string
	relays  map[peer.ID]struct{}

	mu         sync.Mutex
	members    []peer.ID
	owners     *expirable.LRU[string, peer.ID]
	bridgeKeys *expirable.LRU[peer.ID, []byte]
	throttled  *expirable.LRU[string, struct{}]
	users      *expirable.LRU[string, fediverse.WarpnetUser]

	stream func(ctx context.Context, p peer.ID, route string, payload any) ([]byte, error)
}

// NewNode joins the named network. The fixed seed keeps the peer id stable
// across restarts, so the node id stamped on bridged users keeps resolving to
// this gateway.
func NewNode(ctx context.Context, network, seed string) (*Node, error) {
	entries, err := parseEntries(network)
	if err != nil {
		return nil, err
	}
	listen, ok := listenByNetwork[network]
	if !ok {
		return nil, fmt.Errorf("nodeclient: %s: %w", network, errNoListenAddr)
	}

	privKey, err := security.GenerateKeyFromSeed([]byte(seed))
	if err != nil {
		return nil, fmt.Errorf("nodeclient: identity: %w", err)
	}
	p2pKey, err := p2pcrypto.UnmarshalEd25519PrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("nodeclient: key: %w", err)
	}
	psk, err := security.GeneratePSK(network, semver.MustParse("0.0.0"))
	if err != nil {
		return nil, fmt.Errorf("nodeclient: psk: %w", err)
	}
	// The whole testnet shares one IP, so any per-IP connection cap cuts the
	// gateway off from the member nodes.
	rm, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits))
	if err != nil {
		return nil, fmt.Errorf("nodeclient: resource manager: %w", err)
	}

	h, err := libp2p.New(
		libp2p.ResourceManager(rm),
		libp2p.Identity(p2pKey),
		libp2p.PrivateNetwork(pnet.PSK(psk)),
		libp2p.ListenAddrStrings(listen),
		libp2p.WithDialTimeout(dialTimeout),
		libp2p.Transport(camouflage.NewCamouflageTransport),
		libp2p.Ping(true),
		libp2p.Security(noise.ID, noise.New),
		libp2p.EnableRelay(),
		// Reachable only through the relays: no direct address is ever advertised.
		libp2p.ForceReachabilityPrivate(),
		libp2p.EnableAutoRelayWithStaticRelays(entries),
	)
	if err != nil {
		return nil, fmt.Errorf("nodeclient: new host: %w", err)
	}

	// A DHT server, so member nodes resolve the gateway's circuit address.
	kdht, err := dht.New(h,
		dht.Mode(dht.ModeServer),
		dht.ProtocolPrefix(protocol.ID("/"+network)),
		dht.BootstrapPeers(entries...),
	)
	if err != nil {
		_ = h.Close()
		return nil, fmt.Errorf("nodeclient: dht: %w", err)
	}

	relays := make(map[peer.ID]struct{}, len(entries))
	var connected int
	for _, e := range entries {
		relays[e.ID] = struct{}{}
		if err := h.Connect(ctx, e); err != nil {
			warp.NetLog(network).Warnf("nodeclient: connect %s: %v", e.ID, err)
			continue
		}
		connected++
	}
	if connected == 0 {
		_ = kdht.Close()
		_ = h.Close()
		return nil, errNoEntryReachable
	}

	if err := kdht.Bootstrap(ctx); err != nil {
		warp.NetLog(network).Warnf("nodeclient: dht bootstrap: %v", err)
	}
	select {
	case err := <-kdht.RefreshRoutingTable():
		if err != nil {
			warp.NetLog(network).Warnf("nodeclient: dht refresh after bootstrap: %v", err)
		}
	case <-time.After(bootstrapTimeout):
	case <-ctx.Done():
	}
	warp.NetLog(network).Infof("nodeclient %v: joined Warpnet via %d relay(s); discovering members via DHT", h.ID(), connected)

	n := &Node{
		host:       h,
		privKey:    privKey,
		dht:        kdht,
		network:    network,
		relays:     relays,
		owners:     expirable.NewLRU[string, peer.ID](ownersSize, nil, ownersTTL),
		bridgeKeys: expirable.NewLRU[peer.ID, []byte](ownersSize, nil, bridgeKeysTTL),
		throttled:  expirable.NewLRU[string, struct{}](throttledSize, nil, throttledTTL),
		users:      expirable.NewLRU[string, fediverse.WarpnetUser](usersSize, nil, usersTTL),
	}
	n.stream = n.streamToMember
	return n, nil
}

// ID is the node's peer id, the node id of every user it bridges in.
func (n *Node) ID() string { return n.host.ID().String() }

// NetworkName is the Warpnet network the node joined.
func (n *Node) NetworkName() string { return n.network }

func (n *Node) Close() error {
	if n.dht != nil {
		_ = n.dht.Close()
	}
	return n.host.Close()
}
