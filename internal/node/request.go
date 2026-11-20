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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/fediverse"
	"github.com/Warp-net/bridge-gateway/internal/warp"
	"github.com/libp2p/go-libp2p/core/peer"
)

// A request is bounded end to end, every member attempt on its own, and a slow
// member is overtaken by the next one after hedgeDelay instead of stalling the
// whole request.
const (
	requestTimeout = 40 * time.Second
	perPeerTimeout = 15 * time.Second
	hedgeDelay     = 2 * time.Second
	refreshTimeout = 15 * time.Second
)

var (
	errNodeRateLimited = errors.New("node rate limited this route")
	errNoMembers       = errors.New("no reachable member nodes")
)

// Request streams the route to the discovered member nodes until one answers.
// The relays serve discovery only and are never asked.
func (n *Node) Request(route string, payload any) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	peers := n.memberCandidates(route)
	if len(peers) == 0 && n.dht != nil {
		select {
		case err := <-n.dht.RefreshRoutingTable():
			if err != nil {
				warp.NetLog(n.network).Warnf("nodeclient: dht refresh for %s: %v", route, err)
			}
		case <-time.After(refreshTimeout):
		case <-ctx.Done():
		}
		peers = n.memberCandidates(route)
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("nodeclient: %s: %s: no Warpnet member nodes discovered yet", n.network, route)
	}
	return n.tryMembers(ctx, peers, route, payload)
}

// tryMembers asks the first candidate and races one more every hedgeDelay
// without a reply; a failed candidate is replaced at once. The first answer
// wins. Writes are never hedged: a raced write could land twice.
func (n *Node) tryMembers(ctx context.Context, peers []peer.ID, route string, payload any) ([]byte, error) {
	type reply struct {
		peer peer.ID
		bt   []byte
		err  error
	}

	attemptCtx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()

	replies := make(chan reply, len(peers))
	next, inFlight := 0, 0
	launchNext := func() {
		if next >= len(peers) {
			return
		}
		p := peers[next]
		next++
		inFlight++
		go func() {
			ctx, cancel := context.WithTimeout(attemptCtx, perPeerTimeout)
			defer cancel()
			bt, err := n.stream(ctx, p, route, payload)
			replies <- reply{peer: p, bt: bt, err: err}
		}()
	}
	launchNext()

	hedge := time.NewTimer(hedgeDelay)
	defer hedge.Stop()
	if !strings.Contains(route, "/get/") {
		hedge.Stop()
	}

	lastErr := errNoMembers
	for inFlight > 0 {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("nodeclient: %s: %s: %w", n.network, route, ctx.Err())
		case r := <-replies:
			inFlight--
			switch {
			case r.err != nil:
				lastErr = r.err
			case warp.IsRateLimited(r.bt):
				n.markThrottled(route, r.peer)
				warp.NetLog(n.network).Infof("nodeclient: %s rate limited %s, trying another member", r.peer, route)
				lastErr = errNodeRateLimited
			default:
				n.remember(r.peer)
				return r.bt, nil
			}
			launchNext()
		case <-hedge.C:
			launchNext()
			hedge.Reset(hedgeDelay)
		}
	}
	return nil, fmt.Errorf("nodeclient: %s: %s failed on all member nodes: %w", n.network, route, lastErr)
}

// RequestUser streams a user-scoped route to the node that owns userID: follows
// and follower lists are authoritative only there. Any other member would
// accept a follow without keeping it. It falls back to Request when the owner
// cannot be resolved or reached.
func (n *Node) RequestUser(userID, route string, payload any) ([]byte, error) {
	owner, ok := n.ownerNode(userID)
	if !ok || n.isThrottled(route, owner) {
		return n.Request(route, payload)
	}

	ctx, cancel := context.WithTimeout(context.Background(), perPeerTimeout)
	bt, err := n.stream(ctx, owner, route, payload)
	cancel()
	switch {
	case err != nil:
		warp.NetLog(n.network).Warnf("nodeclient: %s on owner of %s failed, falling back to broadcast: %v", route, userID, err)
		n.owners.Remove(userID)
	case warp.IsRateLimited(bt):
		n.markThrottled(route, owner)
		warp.NetLog(n.network).Infof("nodeclient: %s rate limited %s on owner of %s, falling back to broadcast", owner, route, userID)
	default:
		n.remember(owner)
		return bt, nil
	}
	return n.Request(route, payload)
}

// ownerNode reads userID's node id from the replicated profile, which any
// member answers with the same owner.
func (n *Node) ownerNode(userID string) (peer.ID, bool) {
	if p, ok := n.owners.Get(userID); ok {
		return p, true
	}
	bt, err := n.Request(warp.RouteGetUser, warp.GetUserEvent{UserId: userID})
	if err != nil {
		return "", false
	}
	var u warp.User
	if json.Unmarshal(bt, &u) != nil || u.NodeId == "" {
		return "", false
	}
	p, err := peer.Decode(u.NodeId)
	if err != nil {
		warp.NetLog(n.network).Warnf("nodeclient: bad node_id %q for %s: %v", u.NodeId, userID, err)
		return "", false
	}
	n.owners.Add(userID, p)
	warp.NetLog(n.network).Infof("nodeclient: owner of %s resolved to node %s; user-scoped routes target it directly", userID, p)
	return p, true
}

// GetUser reads a profile from this network only. A miss is the normal case
// on every network but the user's own, so it is not reported here.
func (n *Node) GetUser(preferredUsername string) (fediverse.WarpnetUser, bool) {
	if u, ok := n.users.Get(preferredUsername); ok {
		return u, true
	}
	bt, err := n.Request(warp.RouteGetUser, warp.GetUserEvent{UserId: preferredUsername})
	if err != nil {
		warp.NetLog(n.network).Debugf("nodesource: get user %s: %v", preferredUsername, err)
		return fediverse.WarpnetUser{}, false
	}
	var u warp.User
	if err := json.Unmarshal(bt, &u); err != nil || u.Id == "" {
		return fediverse.WarpnetUser{}, false
	}
	wu := fediverse.WarpnetUser{
		ID:                u.Id,
		PreferredUsername: u.Id,
		DisplayName:       u.Username,
		Summary:           u.Bio,
		Avatar:            u.AvatarKey,
		Background:        u.BackgroundImageKey,
	}
	n.users.Add(preferredUsername, wu)
	return wu, true
}
