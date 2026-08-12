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

// The gateway joins every network named in NODE_NETWORK at once, one libp2p node
// each (see connectNetwork). The ActivityPub side stays a single host, so a
// handle carries no network: multiNode locates a user by asking every network
// and remembering which one answered, then confines that user's traffic there.
//
// Confinement is the point. Reads may be raced across networks harmlessly, but a
// write must not be: fanning a follow or a reaction out to every network records
// it against whichever node answers first, so it lands in the wrong one and is
// silently lost. Every write therefore goes through a located home network.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	log "github.com/sirupsen/logrus"
)

// homeCache bounds the userID -> network mapping. It is derived from the
// networks at runtime and re-derived on a miss, so nothing is persisted (see
// CLAUDE.md); the TTL lets a user that moves networks be picked up.
const (
	homeCacheSize = 4096
	homeCacheTTL  = 30 * time.Minute
)

var errNoHomeNetwork = errors.New("no joined Warpnet network serves this user")

// configuredNetworks lists the networks to join. NODE_NETWORK holds one name or
// a comma-separated list ("warpnet,testnet"), so a single-network deployment
// keeps working unchanged. Each entry can be switched off on its own with
// GATEWAY_DISABLE_<NETWORK>, so one node can be taken out of a running
// deployment without editing the list.
func configuredNetworks() []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, 2)
	for _, name := range strings.Split(envOr("NODE_NETWORK", defaultWarpnetNetwork), ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		if networkDisabled(name) {
			log.Infof("gateway: %s node disabled by %s", name, disableEnvKey(name))
			continue
		}
		out = append(out, name)
	}
	return out
}

// disableEnvKey is the per-network off switch, e.g. GATEWAY_DISABLE_TESTNET.
func disableEnvKey(network string) string {
	return "GATEWAY_DISABLE_" + strings.ToUpper(strings.ReplaceAll(network, "-", "_"))
}

// networkDisabled reports whether this network's node is switched off. Any value
// strconv.ParseBool accepts counts ("1", "true", "TRUE"); unset, empty or
// unparseable leaves the node on, so a typo cannot silently drop a network.
func networkDisabled(network string) bool {
	off, err := strconv.ParseBool(os.Getenv(disableEnvKey(network)))
	return err == nil && off
}

// connectNetworks joins every configured network. A network that cannot be
// joined is logged and skipped rather than taking the whole gateway down: the
// others still resolve and federate their own users.
func connectNetworks(ctx context.Context) []*nodeClient {
	var clients []*nodeClient
	for _, network := range configuredNetworks() {
		cli, err := connectNetwork(ctx, network)
		if err != nil {
			log.Warnf("gateway: joining %s: %v", network, err)
			continue
		}
		clients = append(clients, cli)
	}
	return clients
}

// multiNode is the gateway's Warpnet side across every joined network. It
// implements warpnetSource (resolve any handle) and nodeRequester (stream a
// route), routing per user.
type multiNode struct {
	clients []*nodeClient

	mu   sync.Mutex
	home *expirable.LRU[string, *nodeClient] // userID -> the network that serves it
}

func newMultiNode(clients []*nodeClient) *multiNode {
	return &multiNode{
		clients: clients,
		home:    expirable.NewLRU[string, *nodeClient](homeCacheSize, nil, homeCacheTTL),
	}
}

// networks names the joined networks, for logging.
func (m *multiNode) networks() string {
	names := make([]string, 0, len(m.clients))
	for _, c := range m.clients {
		names = append(names, c.network)
	}
	return strings.Join(names, ", ")
}

// locate resolves which network serves userID, remembering the answer. With a
// single network there is nothing to locate — every user is served there, and
// probing first would make an unresolvable profile fail a call that would
// otherwise have worked.
func (m *multiNode) locate(userID string) (*nodeClient, bool) {
	if len(m.clients) == 1 {
		return m.clients[0], true
	}
	if userID == "" || len(m.clients) == 0 {
		return nil, false
	}
	if c, ok := m.home.Get(userID); ok {
		return c, true
	}
	// Ask every network in parallel; the first one that knows the profile owns
	// the user. GET_USER is replicated within a network, so any member answers.
	c, _, ok := m.firstWithUser(userID)
	if !ok {
		return nil, false
	}
	m.remember(userID, c)
	return c, true
}

// firstWithUser races a profile read across the networks and returns the network
// that knows userID, with the profile it answered. Racing rather than asking one
// at a time matters on the ActivityPub hot path: a network that is unreachable or
// still discovering members takes its whole request budget to say so, and every
// lookup would wait through that before trying the network that has the user.
func (m *multiNode) firstWithUser(userID string) (*nodeClient, warpnetUser, bool) {
	type found struct {
		client *nodeClient
		user   warpnetUser
		ok     bool
	}
	results := make(chan found, len(m.clients))
	for _, c := range m.clients {
		go func(c *nodeClient) {
			u, ok := c.lookupUser(userID)
			results <- found{client: c, user: u, ok: ok}
		}(c)
	}
	for range m.clients {
		if r := <-results; r.ok {
			return r.client, r.user, true
		}
	}
	return nil, warpnetUser{}, false
}

// forget drops a user's remembered network so the next call re-locates them.
func (m *multiNode) forget(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.home.Remove(userID)
}

func (m *multiNode) remember(userID string, c *nodeClient) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if prev, ok := m.home.Get(userID); ok && prev == c {
		return
	}
	m.home.Add(userID, c)
	log.Infof("multinet: %s is served by %s", userID, c.network)
}

// GetUser resolves a handle on whichever network serves it (warpnetSource).
func (m *multiNode) GetUser(preferredUsername string) (warpnetUser, bool) {
	if c, ok := m.home.Get(preferredUsername); ok {
		if u, found := c.lookupUser(preferredUsername); found {
			return u, true
		}
		// The user is gone from the remembered network (or it went unreachable);
		// fall through and ask the others before giving up.
		m.forget(preferredUsername)
	}
	c, u, ok := m.firstWithUser(preferredUsername)
	if !ok {
		log.Warnf("multinet: no network (%s) serves user %s", m.networks(), preferredUsername)
		return warpnetUser{}, false
	}
	m.remember(preferredUsername, c)
	return u, true
}

// request streams a route that is not scoped to a user (a tweet, an image, a
// profile). Those are reads, so the networks are raced and the first answer
// wins; a write must use requestIn or requestUser instead.
func (m *multiNode) request(route string, payload any) ([]byte, error) {
	if len(m.clients) == 0 {
		return nil, fmt.Errorf("multinet: %s: %w", route, errNoHomeNetwork)
	}
	if len(m.clients) == 1 {
		return m.clients[0].request(route, payload)
	}

	type reply struct {
		bt  []byte
		err error
	}
	replies := make(chan reply, len(m.clients))
	for _, c := range m.clients {
		go func(c *nodeClient) {
			bt, err := c.request(route, payload)
			replies <- reply{bt: bt, err: err}
		}(c)
	}
	var lastErr error
	for range m.clients {
		r := <-replies
		if r.err == nil {
			return r.bt, nil
		}
		lastErr = r.err
	}
	return nil, fmt.Errorf("multinet: %s failed on every network (%s): %w", route, m.networks(), lastErr)
}

// requestUser streams a user-scoped route to the network that serves userID, and
// within it to the node that owns the user (nodeClient.requestUser). It never
// falls back to another network: that is where a follow or a follower list would
// silently go to the wrong place.
func (m *multiNode) requestUser(userID, route string, payload any) ([]byte, error) {
	c, ok := m.locate(userID)
	if !ok {
		return nil, fmt.Errorf("multinet: %s for %s: %w (%s)", route, userID, errNoHomeNetwork, m.networks())
	}
	return c.requestUser(userID, route, payload)
}

// requestIn broadcasts a route inside the network that serves localUser, keeping
// the node-level semantics of request (any member node answers) while confining
// the call to one network. Inbound Fediverse writes use it: they name a local
// user but are not owner-targeted routes.
func (m *multiNode) requestIn(localUser, route string, payload any) ([]byte, error) {
	c, ok := m.locate(localUser)
	if !ok {
		return nil, fmt.Errorf("multinet: %s for %s: %w (%s)", route, localUser, errNoHomeNetwork, m.networks())
	}
	return c.request(route, payload)
}

// networkScoped is implemented by a requester spanning several networks, which
// can confine a write to the one that serves a given user. A single-network
// requester does not need it — there is no wrong network to reach.
type networkScoped interface {
	requestIn(localUser, route string, payload any) ([]byte, error)
}

// requestForUser sends an inbound write into the network that serves localUser.
// With one network (or an unknown user) it is a plain broadcast, exactly as
// before multi-network support.
func (g *gateway) requestForUser(localUser, route string, payload any) ([]byte, error) {
	if scoped, ok := g.req.(networkScoped); ok && localUser != "" {
		return scoped.requestIn(localUser, route, payload)
	}
	return g.req.request(route, payload)
}
