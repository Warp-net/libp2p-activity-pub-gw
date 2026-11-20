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
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/Warp-net/bridge-gateway/internal/warp"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	bridgeKeySize   = 32
	followingsPages = 10
	endCursor       = "end"
)

// HomeUser reads userID from the node that owns them, ignoring every other
// node's copy: only the owner speaks for consent and the lock flag.
func (n *Node) HomeUser(userID string) (warp.User, bool) {
	bt, owner, ok := n.askOwner(userID, warp.RouteGetUser, warp.GetUserEvent{UserId: userID})
	if !ok {
		return warp.User{}, false
	}
	var u warp.User
	if json.Unmarshal(bt, &u) != nil || !strings.EqualFold(u.Id, userID) || u.NodeId != owner {
		return warp.User{}, false
	}
	return u, true
}

// Followings reads all of userID's followings from their own node; ok is false
// unless the whole list was read.
func (n *Node) Followings(userID string) ([]string, bool) {
	var followings []string
	var cursor *string
	for range followingsPages {
		bt, _, ok := n.askOwner(userID, warp.RouteGetFollowings, warp.GetFollowersEvent{UserId: userID, Cursor: cursor})
		if !ok {
			return nil, false
		}
		var resp warp.FollowingsResponse
		if json.Unmarshal(bt, &resp) != nil {
			return nil, false
		}
		followings = append(followings, resp.Followings...)
		isLastPage := resp.Cursor == "" || resp.Cursor == endCursor || len(resp.Followings) == 0
		if isLastPage || (cursor != nil && *cursor == resp.Cursor) {
			return followings, true
		}
		next := resp.Cursor
		cursor = &next
	}
	return nil, false
}

// BridgeKey is the key userID's own node last sent, while it is fresh.
func (n *Node) BridgeKey(userID string) ([]byte, bool) {
	owner, ok := n.ownerNode(userID)
	if !ok {
		return nil, false
	}
	return n.bridgeKeys.Get(owner)
}

// rememberBridgeKey keeps the key in memory only, under the peer that signed
// the envelope carrying it.
func (n *Node) rememberBridgeKey(from peer.ID, hexKey string) {
	if hexKey == "" {
		return
	}
	key, err := hex.DecodeString(hexKey)
	if err != nil || len(key) != bridgeKeySize {
		return
	}
	if _, isKnown := n.bridgeKeys.Peek(from); !isKnown {
		warp.NetLog(n.network).Infof("nodeserver: bridge key from %s", from)
	}
	n.bridgeKeys.Add(from, key)
}

// askOwner sends the route to userID's own node only, never to a copy.
func (n *Node) askOwner(userID, route string, payload any) ([]byte, string, bool) {
	owner, ok := n.ownerNode(userID)
	if !ok {
		return nil, "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), perPeerTimeout)
	defer cancel()
	bt, err := n.stream(ctx, owner, route, payload)
	if err != nil || warp.NodeResponseError(bt) != nil || warp.IsRateLimited(bt) {
		return nil, "", false
	}
	return bt, owner.String(), true
}
