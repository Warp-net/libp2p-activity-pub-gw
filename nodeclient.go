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

package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
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
	log "github.com/sirupsen/logrus"
)

var (
	errNoEntryPeers     = errors.New("no Warpnet entry peers (check NODE_NETWORK)")
	errNoEntryReachable = errors.New("nodeclient: no Warpnet entry peer reachable")
	errNoListenAddr     = errors.New("no libp2p listen address (add one to p2pListenByNetwork)")
)

// owner cache bounds: ownership can move between nodes, so entries expire and
// re-resolve via GET_USER; a miss costs one broadcast request.
const (
	ownerCacheSize = 1024
	ownerCacheTTL  = 10 * time.Minute
)

// Request timing. A route request used to share a single 1-minute budget across
// a sequential walk of up to maxMemberCandidates peers, so one dead or slow node
// stalled the whole request — that is what made AP HTTP requests hang for tens of
// seconds. Now each attempt is individually bounded and candidates are hedged
// (raced with a small stagger), so a stuck node costs at most hedgeDelay.
const (
	// requestTimeout bounds one whole route request end to end.
	requestTimeout = 25 * time.Second
	// perPeerTimeout bounds a single member attempt (DHT FindPeer + dial + round
	// trip) so an unreachable node is abandoned quickly.
	perPeerTimeout = 8 * time.Second
	// hedgeDelay is how long to wait for the in-flight attempt(s) before racing
	// the next candidate in parallel. The fast common case (the remembered-good
	// node answers) still makes a single dial; a stalled primary is overtaken.
	hedgeDelay = 2 * time.Second
)

// nodeClient joins the Warpnet DHT through the network's relays and streams the
// /public/... routes to the member nodes it discovers via the DHT.
type nodeClient struct {
	h       host.Host
	priv    ed25519.PrivateKey
	dht     *dht.IpfsDHT
	network string               // the Warpnet network this node joined
	relays  map[peer.ID]struct{} // entry peers (relays): discovery/connectivity only, not data routes

	mu    sync.Mutex
	good  []peer.ID                       // member nodes known to answer data routes; tried first
	owner *expirable.LRU[string, peer.ID] // userID -> its home node (domain.User.NodeId); user-scoped routes target it

	// stream sends one attempt to a single member; defaults to streamToMember and
	// is overridable in tests to exercise the hedging/timeout logic without a host.
	stream func(ctx context.Context, p peer.ID, route string, payload any) ([]byte, error)
}

// networkEntries are the network's bootstrap relays (the DHT entry points).
func networkEntries(network string) ([]peer.AddrInfo, error) {
	var entries []peer.AddrInfo
	for _, s := range bootstrapByNetwork[network] {
		ai, err := peer.AddrInfoFromString(s)
		if err != nil {
			log.Warnf("nodeclient: bad bootstrap %q: %v", s, err)
			continue
		}
		entries = append(entries, *ai)
	}
	if len(entries) == 0 {
		return nil, errNoEntryPeers
	}
	return entries, nil
}

// connectNetwork builds a libp2p host wired for Warpnet and joins the named
// network through its entry peers. One process calls it once per network in
// NODE_NETWORK (see connectNetworks), so everything it builds — host, DHT,
// listen port — is per network.
func connectNetwork(ctx context.Context, network string) (*nodeClient, error) {
	entries, err := networkEntries(network)
	if err != nil {
		return nil, err
	}
	listen, ok := p2pListenByNetwork[network]
	if !ok {
		return nil, fmt.Errorf("nodeclient: %s: %w", network, errNoListenAddr)
	}

	// Deterministic identity: a fixed seed yields a stable peer id across
	// restarts so the node_id carried on bridged Mastodon users keeps resolving
	// back to this gateway (a rotating id would orphan them). Same derivation as
	// a Warpnet member node (security.GenerateKeyFromSeed).
	priv, err := security.GenerateKeyFromSeed([]byte(defaultGatewaySeed))
	if err != nil {
		return nil, fmt.Errorf("nodeclient: identity: %w", err)
	}
	p2pPriv, err := p2pcrypto.UnmarshalEd25519PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("nodeclient: key: %w", err)
	}

	// PSK keys the private network on the network name + MAJOR version (warpnet's
	// security.GeneratePSK); major 0 matches the live networks.
	ver, err := semver.NewVersion("0.0.0")
	if err != nil {
		return nil, fmt.Errorf("nodeclient: version: %w", err)
	}
	psk, err := security.GeneratePSK(network, ver)
	if err != nil {
		return nil, fmt.Errorf("nodeclient: psk: %w", err)
	}

	// No resource-manager limits: the whole testnet shares one IP, so the default
	// per-IP connection cap blocks the gateway from reaching the member node and
	// makes resolution/federation flaky.
	rm, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits))
	if err != nil {
		return nil, fmt.Errorf("nodeclient: resource manager: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.ResourceManager(rm),
		libp2p.Identity(p2pPriv),
		libp2p.PrivateNetwork(pnet.PSK(psk)),
		libp2p.ListenAddrStrings(listen),
		libp2p.WithDialTimeout(60 * time.Second),
		libp2p.Transport(camouflage.NewCamouflageTransport),
		libp2p.Ping(true),
		libp2p.Security(noise.ID, noise.New),
		libp2p.EnableRelay(),
		// Reach the gateway ONLY through the network's relays (security): force
		// private reachability so it never advertises a direct address, and take
		// circuit-relay reservations on the bootstrap relays, like a NAT'd member
		// node. Member nodes dial it over the relay; it is never directly exposed.
		libp2p.ForceReachabilityPrivate(),
		libp2p.EnableAutoRelayWithStaticRelays(entries),
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("nodeclient: new host: %w", err)
	}

	// Join Warpnet's Kademlia DHT (prefix "/<network>", bootstrapped via the
	// relays) as a server so member nodes can still resolve the gateway's
	// circuit address via FindPeer, even though it is only reachable via relays.
	kdht, err := dht.New(ctx, h,
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
		if cerr := h.Connect(ctx, e); cerr != nil {
			log.Warnf("nodeclient: connect %s: %v", e.ID, cerr)
			continue
		}
		connected++
	}
	if connected == 0 {
		_ = kdht.Close()
		_ = h.Close()
		return nil, errNoEntryReachable
	}

	if berr := kdht.Bootstrap(ctx); berr != nil {
		log.Warnf("nodeclient: dht bootstrap: %v", berr)
	}
	select {
	case <-kdht.RefreshRoutingTable():
	case <-time.After(20 * time.Second):
	case <-ctx.Done():
	}
	log.Infof("nodeclient %v: joined Warpnet (%s) via %d relay(s); discovering members via DHT", h.ID(), network, connected)

	c := &nodeClient{
		h: h, priv: priv, dht: kdht, network: network, relays: relays,
		owner: expirable.NewLRU[string, peer.ID](ownerCacheSize, nil, ownerCacheTTL),
	}
	c.stream = c.streamToMember
	return c, nil
}

// request streams the route to the member nodes discovered via the DHT, trying
// each until one answers (relays serve only discovery, so they are excluded).
func (c *nodeClient) request(route string, payload any) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	peers := c.memberCandidates()
	if len(peers) == 0 && c.dht != nil {
		select { // routing table not populated yet — refresh and retry
		case <-c.dht.RefreshRoutingTable():
		case <-time.After(15 * time.Second):
		case <-ctx.Done():
		}
		peers = c.memberCandidates()
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("nodeclient: %s: no Warpnet member nodes discovered yet", route)
	}

	return c.tryMembers(ctx, peers, route, payload)
}

// tryMembers streams the route to the candidates using hedged requests: it starts
// with the first (the remembered-good node), and every hedgeDelay without a reply
// it races one more candidate in parallel. A candidate that fails fast triggers
// the next immediately. The first success wins and cancels the rest, so a slow or
// dead node adds at most hedgeDelay to the request instead of stalling it for the
// whole budget. Each attempt is bounded by perPeerTimeout.
func (c *nodeClient) tryMembers(ctx context.Context, peers []peer.ID, route string, payload any) ([]byte, error) {
	type reply struct {
		peer peer.ID
		bt   []byte
		err  error
	}

	attemptCtx, cancelAll := context.WithCancel(ctx)
	defer cancelAll() // cancel any still-running attempts on return

	replies := make(chan reply, len(peers))
	launch := func(p peer.ID) {
		go func() {
			aCtx, cancel := context.WithTimeout(attemptCtx, perPeerTimeout)
			defer cancel()
			bt, err := c.stream(aCtx, p, route, payload)
			replies <- reply{peer: p, bt: bt, err: err}
		}()
	}

	next, inFlight := 0, 0
	launchNext := func() {
		if next < len(peers) {
			launch(peers[next])
			next++
			inFlight++
		}
	}
	launchNext()

	hedge := time.NewTimer(hedgeDelay)
	defer hedge.Stop()

	var lastErr error
	for inFlight > 0 {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("nodeclient: %s: %w", route, ctx.Err())
		case r := <-replies:
			inFlight--
			if r.err == nil {
				c.remember(r.peer)
				return r.bt, nil
			}
			lastErr = r.err
			launchNext() // a candidate failed — try the next one right away
		case <-hedge.C:
			launchNext() // in-flight attempt is slow — hedge with another
			hedge.Reset(hedgeDelay)
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no reachable member nodes")
	}
	return nil, fmt.Errorf("nodeclient: %s failed on all member nodes: %w", route, lastErr)
}

// requestUser streams a user-scoped route directly to the node that OWNS userID
// (its domain.User.NodeId). Routes like POST_FOLLOW and GET_FOLLOWERS are
// authoritative only on the owner node: a random member node silently "accepts"
// a follow without persisting it and reads back an empty follower list, which
// made federation depend on which node the DHT happened to answer with. Falls
// back to the broadcast request when the owner can't be resolved or reached.
func (c *nodeClient) requestUser(userID, route string, payload any) ([]byte, error) {
	if owner, ok := c.ownerNode(userID); ok {
		ctx, cancel := context.WithTimeout(context.Background(), perPeerTimeout)
		bt, err := c.stream(ctx, owner, route, payload)
		cancel()
		if err == nil {
			c.remember(owner)
			return bt, nil
		}
		log.Warnf("nodeclient: %s on owner of %s failed, falling back to broadcast: %v", route, userID, err)
		c.forgetOwner(userID)
	}
	return c.request(route, payload)
}

// ownerNode resolves userID's home node from domain.User.NodeId (via GET_USER,
// which any node can answer because profiles are replicated; the node_id it
// carries is the user's authoritative node regardless of who replied) and
// caches it.
func (c *nodeClient) ownerNode(userID string) (peer.ID, bool) {
	if p, ok := c.owner.Get(userID); ok {
		return p, true
	}

	bt, err := c.request(routeGetUser, getUserEvent{UserId: userID})
	if err != nil {
		return "", false
	}
	var u user
	if json.Unmarshal(bt, &u) != nil || u.NodeId == "" {
		return "", false
	}
	p, err := peer.Decode(u.NodeId)
	if err != nil {
		log.Warnf("nodeclient: bad node_id %q for %s: %v", u.NodeId, userID, err)
		return "", false
	}
	c.owner.Add(userID, p)
	log.Infof("nodeclient: owner of %s resolved to node %s; user-scoped routes target it directly", userID, p)
	return p, true
}

// forgetOwner drops a cached owner mapping so the next user-scoped request
// re-resolves it (e.g. after the owner node moved or went briefly unreachable).
func (c *nodeClient) forgetOwner(userID string) {
	c.owner.Remove(userID)
}

func (c *nodeClient) close() {
	if c == nil {
		return
	}
	if c.dht != nil {
		_ = c.dht.Close()
	}
	if c.h != nil {
		_ = c.h.Close()
	}
}

// nodeSource reads any requested user's profile live from the Warpnet network
// via the user route, so the gateway is agnostic to which user it serves and
// stores no profile of its own.
type nodeSource struct {
	client *nodeClient
}

func (s nodeSource) GetUser(preferredUsername string) (warpnetUser, bool) {
	return s.client.lookupUser(preferredUsername)
}

// lookupUser reads a profile from this one network. A miss is logged at debug,
// not error: the gateway asks every joined network for a handle (which carries
// no network of its own), so a miss on the others is the normal case — the
// caller reports it once when no network has the user.
func (c *nodeClient) lookupUser(preferredUsername string) (warpnetUser, bool) {
	bt, err := c.request(routeGetUser, getUserEvent{UserId: preferredUsername})
	if err != nil {
		log.Debugf("nodesource: %s: get user %s: %v", c.network, preferredUsername, err)
		return warpnetUser{}, false
	}
	var u user
	if uerr := json.Unmarshal(bt, &u); uerr != nil || u.Id == "" {
		return warpnetUser{}, false
	}
	return warpnetUser{
		ID:                u.Id,
		PreferredUsername: u.Id,
		DisplayName:       u.Username,
		Summary:           u.Bio,
		Avatar:            u.AvatarKey,
		Background:        u.BackgroundImageKey,
	}, true
}
