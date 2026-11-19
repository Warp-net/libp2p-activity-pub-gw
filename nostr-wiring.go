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
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"

	"github.com/Warp-net/activity-pub-gw/internal/fediverse"
	"github.com/Warp-net/activity-pub-gw/internal/nostr"
	"github.com/Warp-net/activity-pub-gw/internal/warp"
)

// nostrHub wires the Nostr bridge into the gateway: one relay pool and live
// follow graph for every network, one bridge and publisher per network.
type nostrHub struct {
	g       *fediverse.Gateway
	relays  nostr.Relays
	live    *nostr.Live
	keys    *nostr.Keys
	publish bool

	mu   sync.Mutex
	pubs []*nostr.Publisher
}

// bridgeFor serves Nostr for one network; nil when Nostr is off.
func (h *nostrHub) bridgeFor(c *nodeClient) *nostr.Bridge {
	if h == nil || h.relays == nil {
		return nil
	}
	nb := nostr.NewBridge(h.relays, nostr.RelaysFromEnv(), c.h.ID().String(), h.g.Host)
	nb.Trusted = h.live.Trusted
	nb.Keys, nb.Links, nb.Home, nb.Network = h.keys, h.g, c, c.network
	if h.publish && h.keys != nil {
		nb.Pub = nostr.NewPublisher(h.keys, nb, c, c.network, h.g)
		h.mu.Lock()
		h.pubs = append(h.pubs, nb.Pub)
		h.mu.Unlock()
	}
	return nb
}

// publishers are the per-network publishers bridgeFor created.
func (h *nostrHub) publishers() []*nostr.Publisher {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*nostr.Publisher{}, h.pubs...)
}

// setFollow records a Warpnet user following a Nostr account.
func (h *nostrHub) setFollow(follower, handle string, undo bool) {
	if h != nil {
		h.live.SetFollow(follower, handle, undo)
	}
}

// push delivers a followed Nostr author's note to a follower's node.
func (h *nostrHub) push(follower string, t warp.Tweet) error {
	if h.g.Requester == nil {
		return nil
	}
	bt, err := h.g.RequestForUser(follower, warp.RoutePostTimeline, t)
	if err == nil {
		err = warp.NodeResponseError(bt)
	}
	return err
}

// HomeUser asks the node that owns userID for the user, ignoring every other
// node's copy.
func (c *nodeClient) HomeUser(userID string) (warp.User, bool) {
	bt, owner, ok := c.askOwner(userID, warp.RouteGetUser, warp.GetUserEvent{UserId: userID})
	if !ok {
		return warp.User{}, false
	}
	var u warp.User
	if json.Unmarshal(bt, &u) != nil || !strings.EqualFold(u.Id, userID) || u.NodeId != owner {
		return warp.User{}, false
	}
	return u, true
}

// NostrKey asks userID's own node for their Nostr secret key. The node answers
// only this gateway and only with consent; the key is used and dropped.
func (c *nodeClient) NostrKey(userID string) ([32]byte, bool) {
	var sk [32]byte
	bt, _, ok := c.askOwner(userID, warp.RouteGetNostrKey, warp.GetNostrKeyEvent{UserId: userID})
	if !ok {
		return sk, false
	}
	var resp warp.GetNostrKeyResponse
	if json.Unmarshal(bt, &resp) != nil || !strings.EqualFold(resp.UserId, userID) {
		return sk, false
	}
	raw, err := hex.DecodeString(resp.Key)
	if err != nil || len(raw) != len(sk) {
		return sk, false
	}
	copy(sk[:], raw)
	return sk, true
}

// nodeKeys fetches users' Nostr keys through the client of their network.
type nodeKeys map[string]*nodeClient

func (n nodeKeys) NostrKey(network, userID string) ([32]byte, bool) {
	c := n[network]
	if c == nil {
		return [32]byte{}, false
	}
	return c.NostrKey(userID)
}

// askOwner sends a route to the node that owns userID only, never falling
// back to another node's copy.
func (c *nodeClient) askOwner(userID, route string, payload any) ([]byte, string, bool) {
	owner, ok := c.ownerNode(userID)
	if !ok {
		return nil, "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), perPeerTimeout)
	defer cancel()
	bt, err := c.stream(ctx, owner, route, payload)
	if err != nil || warp.NodeResponseError(bt) != nil || warp.IsRateLimited(bt) {
		return nil, "", false
	}
	return bt, owner.String(), true
}

// Followings reads all of userID's followings from their node; ok is false
// unless the whole list was read.
func (c *nodeClient) Followings(userID string) ([]string, bool) {
	var out []string
	var cursor *string
	for range nostr.FollowPages {
		bt, _, ok := c.askOwner(userID, warp.RouteGetFollowings, warp.GetFollowersEvent{UserId: userID, Cursor: cursor})
		if !ok {
			return nil, false
		}
		var resp warp.FollowingsResponse
		if json.Unmarshal(bt, &resp) != nil {
			return nil, false
		}
		out = append(out, resp.Followings...)
		if resp.Cursor == "" || resp.Cursor == "end" || len(resp.Followings) == 0 || (cursor != nil && *cursor == resp.Cursor) {
			return out, true
		}
		next := resp.Cursor
		cursor = &next
	}
	return nil, false
}
