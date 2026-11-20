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

package nostr

import (
	"context"
	"net/http"
	"sync"

	"github.com/Warp-net/bridge-gateway/internal/warp"
)

// HubNode is the gateway's node in one Warpnet network.
type HubNode interface {
	warp.Requester
	HomeReader
	ID() string
	NetworkName() string
}

// TimelineRequester delivers to a Warpnet user inside their own network.
type TimelineRequester interface {
	RequestForUser(localUser, route string, payload any) ([]byte, error)
}

// Hub is the Nostr side of the gateway: one relay pool and one live follow
// graph for every network, one bridge and publisher per network.
type Hub struct {
	relays       Relays
	defaults     []string
	selfHost     string
	keys         *Keys
	links        Links
	timeline     TimelineRequester
	isPublishing bool
	live         *Live

	mu       sync.Mutex
	networks []hubNetwork
}

type hubNetwork struct {
	node HubNode
	pub  *Publisher
}

func NewHub(
	relays Relays,
	keySource KeySource,
	links Links,
	timeline TimelineRequester,
	selfHost string,
	isPublishing bool,
) *Hub {
	h := &Hub{
		relays:       relays,
		defaults:     RelaysFromEnv(),
		selfHost:     selfHost,
		keys:         newKeys(keySource),
		links:        links,
		timeline:     timeline,
		isPublishing: isPublishing,
	}
	feed := newBridge(relays, h.defaults, "", selfHost)
	h.live = newLive(feed, h.push)
	feed.trusted = h.live.Trusted
	return h
}

// NewBridge serves Nostr for one network. The publisher is nil unless the hub
// publishes, and every Publisher method is a no-op on nil.
func (h *Hub) NewBridge(n HubNode) (*Bridge, *Publisher) {
	b := newBridge(h.relays, h.defaults, n.ID(), h.selfHost)
	b.trusted = h.live.Trusted
	b.keys, b.links, b.home, b.network = h.keys, h.links, n, n.NetworkName()
	if h.isPublishing {
		b.pub = newPublisher(h.keys, b, n, n.NetworkName(), h.links)
	}

	h.mu.Lock()
	h.networks = append(h.networks, hubNetwork{node: n, pub: b.pub})
	h.mu.Unlock()
	return b, b.pub
}

// SetFollow records a Warpnet user following a Nostr account, so the live feed
// delivers that account's notes to them.
func (h *Hub) SetFollow(follower, handle string, undo bool) {
	h.live.SetFollow(follower, handle, undo)
}

// NIP05Handler names the consenting Warpnet users' keys.
func (h *Hub) NIP05Handler() http.Handler {
	return newNIP05Handler(h.publishers)
}

// Run feeds followed Nostr authors to Warpnet and publishes consenting users'
// own posts. scanUser pages through every network's users.
func (h *Hub) Run(ctx context.Context, scanUser string) {
	go h.live.Run(ctx)
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, network := range h.networks {
		go h.live.RunScanner(ctx, network.node, scanUser)
		if network.pub != nil {
			go newOutbound(ctx, network.node, network.pub).RunScanner(scanUser)
		}
	}
}

func (h *Hub) publishers() []*Publisher {
	h.mu.Lock()
	defer h.mu.Unlock()
	pubs := make([]*Publisher, 0, len(h.networks))
	for _, network := range h.networks {
		if network.pub != nil {
			pubs = append(pubs, network.pub)
		}
	}
	return pubs
}

func (h *Hub) push(follower string, t warp.Tweet) error {
	bt, err := h.timeline.RequestForUser(follower, warp.RoutePostTimeline, t)
	if err != nil {
		return err
	}
	return warp.NodeResponseError(bt)
}
