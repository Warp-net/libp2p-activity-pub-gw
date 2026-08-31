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
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

const maxMemberCandidates = 64

// memberCandidates lists peers that may serve the /public/... routes: members
// known to have answered before (first), then the DHT routing-table peers
// (Warpnet member/moderator nodes are DHT servers). The relays are excluded.
func (c *nodeClient) memberCandidates(route string) []peer.ID {
	seen := make(map[peer.ID]struct{})
	out := make([]peer.ID, 0, maxMemberCandidates)
	add := func(p peer.ID) {
		if p == "" || p == c.h.ID() {
			return
		}
		if _, ok := c.relays[p]; ok {
			return
		}
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}

	c.mu.Lock()
	for _, p := range c.good {
		add(p)
	}
	c.mu.Unlock()

	if c.dht != nil {
		for _, p := range c.dht.RoutingTable().ListPeers() {
			add(p)
		}
	}
	if len(out) > maxMemberCandidates {
		out = out[:maxMemberCandidates]
	}
	return c.demoteThrottled(route, out)
}

func (c *nodeClient) demoteThrottled(route string, peers []peer.ID) []peer.ID {
	if c.throttled == nil || c.throttled.Len() == 0 {
		return peers
	}
	ready := make([]peer.ID, 0, len(peers))
	cooling := make([]peer.ID, 0, len(peers))
	for _, p := range peers {
		if c.isThrottled(route, p) {
			cooling = append(cooling, p)
			continue
		}
		ready = append(ready, p)
	}
	return append(ready, cooling...)
}

// streamToMember resolves the peer's addresses via the DHT if needed, then sends
// the signed request.
func (c *nodeClient) streamToMember(ctx context.Context, p peer.ID, route string, payload any) ([]byte, error) {
	if len(c.h.Peerstore().Addrs(p)) == 0 && c.dht != nil {
		fctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		if pi, err := c.dht.FindPeer(fctx, p); err == nil {
			c.h.Peerstore().AddAddrs(p, pi.Addrs, time.Hour)
		}
		cancel()
	}
	return streamSend(ctx, c.h, p, c.priv, route, payload)
}

// remember moves a member that answered to the front of the cache so later
// requests try it first.
func (c *nodeClient) remember(p peer.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]peer.ID, 0, len(c.good)+1)
	out = append(out, p)
	for _, q := range c.good {
		if q != p {
			out = append(out, q)
		}
	}
	if len(out) > maxMemberCandidates {
		out = out[:maxMemberCandidates]
	}
	c.good = out
}
