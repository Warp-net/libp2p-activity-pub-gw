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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/fediverse"
	"github.com/hashicorp/golang-lru/v2/expirable"
	log "github.com/sirupsen/logrus"
)

const (
	homesSize = 4096
	homesTTL  = 30 * time.Minute
)

var (
	errNoHomeNetwork = errors.New("no joined Warpnet network serves this user")
	errNoRoutingUser = errors.New("cross-network request needs a user to route by")
)

// MultiNode is the gateway's Warpnet side across every joined network. A
// handle carries no network, so it locates each user on the network that has
// them and keeps every write of theirs there: a write raced across networks
// lands on whichever answers first and is lost.
type MultiNode struct {
	nodes []*Node
	homes *expirable.LRU[string, *Node]
}

func NewMultiNode(nodes []*Node) *MultiNode {
	return &MultiNode{
		nodes: nodes,
		homes: expirable.NewLRU[string, *Node](homesSize, nil, homesTTL),
	}
}

// Networks names the joined networks.
func (m *MultiNode) Networks() string {
	names := make([]string, 0, len(m.nodes))
	for _, n := range m.nodes {
		names = append(names, n.network)
	}
	return strings.Join(names, ", ")
}

// Locate finds the network that serves userID. With one network there is
// nothing to find, and a probe could fail a call that would have worked.
func (m *MultiNode) Locate(userID string) (*Node, bool) {
	if len(m.nodes) == 1 {
		return m.nodes[0], true
	}
	if userID == "" || len(m.nodes) == 0 {
		return nil, false
	}
	if n, ok := m.homes.Get(userID); ok {
		return n, true
	}
	n, _, ok := m.raceUser(userID)
	if !ok {
		return nil, false
	}
	m.rememberHome(userID, n)
	return n, true
}

// GetUser resolves a handle on whichever network serves it.
func (m *MultiNode) GetUser(preferredUsername string) (fediverse.WarpnetUser, bool) {
	if n, ok := m.homes.Get(preferredUsername); ok {
		if u, isFound := n.GetUser(preferredUsername); isFound {
			return u, true
		}
		m.homes.Remove(preferredUsername)
	}
	n, u, ok := m.raceUser(preferredUsername)
	if !ok {
		log.Warnf("multinet: no network (%s) serves user %s", m.Networks(), preferredUsername)
		return fediverse.WarpnetUser{}, false
	}
	m.rememberHome(preferredUsername, n)
	return u, true
}

// Request carries no user to route by. With several networks, whichever
// answered first would speak for a user who may live on another, so it is
// refused rather than leaked across.
func (m *MultiNode) Request(route string, payload any) ([]byte, error) {
	switch len(m.nodes) {
	case 0:
		return nil, fmt.Errorf("multinet: %s: %w", route, errNoHomeNetwork)
	case 1:
		return m.nodes[0].Request(route, payload)
	}
	return nil, fmt.Errorf("multinet: %s: %w (%s)", route, errNoRoutingUser, m.Networks())
}

// RequestUser streams a user-scoped route to the owner of userID inside the
// network that serves them, never to another network.
func (m *MultiNode) RequestUser(userID, route string, payload any) ([]byte, error) {
	n, ok := m.Locate(userID)
	if !ok {
		return nil, fmt.Errorf("multinet: %s for %s: %w (%s)", route, userID, errNoHomeNetwork, m.Networks())
	}
	return n.RequestUser(userID, route, payload)
}

// RequestIn lets any member of localUser's network answer, keeping the call
// inside that network. Inbound Fediverse writes use it.
func (m *MultiNode) RequestIn(localUser, route string, payload any) ([]byte, error) {
	n, ok := m.Locate(localUser)
	if !ok {
		return nil, fmt.Errorf("multinet: %s for %s: %w (%s)", route, localUser, errNoHomeNetwork, m.Networks())
	}
	return n.Request(route, payload)
}

// BridgeKey is the key userID's own node in that network last sent.
func (m *MultiNode) BridgeKey(network, userID string) ([]byte, bool) {
	for _, n := range m.nodes {
		if n.network == network {
			return n.BridgeKey(userID)
		}
	}
	return nil, false
}

// raceUser asks every network at once: an unreachable network takes its whole
// budget to say so, and a sequential walk would wait through it.
func (m *MultiNode) raceUser(userID string) (*Node, fediverse.WarpnetUser, bool) {
	type found struct {
		node *Node
		user fediverse.WarpnetUser
		ok   bool
	}
	results := make(chan found, len(m.nodes))
	for _, n := range m.nodes {
		go func() {
			u, ok := n.GetUser(userID)
			results <- found{node: n, user: u, ok: ok}
		}()
	}
	for range m.nodes {
		if r := <-results; r.ok {
			return r.node, r.user, true
		}
	}
	return nil, fediverse.WarpnetUser{}, false
}

func (m *MultiNode) rememberHome(userID string, n *Node) {
	if prev, ok := m.homes.Get(userID); ok && prev == n {
		return
	}
	m.homes.Add(userID, n)
	log.Infof("multinet: %s is served by %s", userID, n.network)
}
