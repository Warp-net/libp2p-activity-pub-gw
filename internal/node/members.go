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
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	maxMemberCandidates = 64
	findPeerTimeout     = 15 * time.Second
	foundAddrsTTL       = time.Hour
)

// memberCandidates lists the members that answered before, then the DHT
// routing table; members that throttled the route go last.
func (n *Node) memberCandidates(route string) []peer.ID {
	seen := make(map[peer.ID]struct{})
	candidates := make([]peer.ID, 0, maxMemberCandidates)
	add := func(p peer.ID) {
		if p == "" || p == n.host.ID() {
			return
		}
		if _, isRelay := n.relays[p]; isRelay {
			return
		}
		if _, isSeen := seen[p]; isSeen {
			return
		}
		seen[p] = struct{}{}
		candidates = append(candidates, p)
	}

	n.mu.Lock()
	for _, p := range n.members {
		add(p)
	}
	n.mu.Unlock()

	if n.dht != nil {
		for _, p := range n.dht.RoutingTable().ListPeers() {
			add(p)
		}
	}
	if len(candidates) > maxMemberCandidates {
		candidates = candidates[:maxMemberCandidates]
	}

	ready := make([]peer.ID, 0, len(candidates))
	var cooling []peer.ID
	for _, p := range candidates {
		if n.isThrottled(route, p) {
			cooling = append(cooling, p)
			continue
		}
		ready = append(ready, p)
	}
	return append(ready, cooling...)
}

// remember puts a member that answered first in line for the next request.
func (n *Node) remember(p peer.ID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	members := make([]peer.ID, 0, len(n.members)+1)
	members = append(members, p)
	for _, q := range n.members {
		if q != p {
			members = append(members, q)
		}
	}
	if len(members) > maxMemberCandidates {
		members = members[:maxMemberCandidates]
	}
	n.members = members
}

func (n *Node) markThrottled(route string, p peer.ID) {
	n.throttled.Add(route+"|"+p.String(), struct{}{})
}

func (n *Node) isThrottled(route string, p peer.ID) bool {
	return n.throttled.Contains(route + "|" + p.String())
}

func (n *Node) streamToMember(ctx context.Context, p peer.ID, route string, payload any) ([]byte, error) {
	if len(n.host.Peerstore().Addrs(p)) == 0 && n.dht != nil {
		findCtx, cancel := context.WithTimeout(ctx, findPeerTimeout)
		if pi, err := n.dht.FindPeer(findCtx, p); err == nil {
			n.host.Peerstore().AddAddrs(p, pi.Addrs, foundAddrsTTL)
		}
		cancel()
	}
	return sendStream(ctx, n.host, p, n.privKey, route, payload)
}
