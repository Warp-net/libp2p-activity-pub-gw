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

package fediverse

import (
	"crypto/rsa"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Warp-net/activity-pub-gw/internal/logring"
	"github.com/Warp-net/activity-pub-gw/internal/netguard"
	"github.com/Warp-net/activity-pub-gw/internal/warp"
	"github.com/Warp-net/warpnet/retrier"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

// RequestForUser sends a request that belongs to localUser into that user's
// network — inbound Fediverse writes, and the reads behind their own status and
// media urls. It never falls back to another network: with one network there is
// none to fall back to, and with several, falling back is the leak.
func (g *Gateway) RequestForUser(localUser, route string, payload any) ([]byte, error) {
	if scoped, ok := g.Requester.(warp.NetworkScoped); ok {
		return scoped.RequestIn(localUser, route, payload)
	}
	return g.Requester.Request(route, payload) // single-network requester: no wrong network to reach
}

// ProfileURL is a Warpnet user's actor url on this gateway.
func (g *Gateway) ProfileURL(userID string) string { return g.ActorID(userID) }

// StatusURL is a Warpnet tweet's note url on this gateway.
func (g *Gateway) StatusURL(userID, tweetID string) string {
	return g.ActorID(userID) + PathStatuses + tweetID
}

// MediaURL serves a Warpnet image through the gateway's media proxy.
func (g *Gateway) MediaURL(userID, key string) string {
	return g.baseURL() + PathMedia + EncodeMediaRef(userID, key)
}

// ProfileUser is the Warpnet user a ProfileURL on this gateway names.
func (g *Gateway) ProfileUser(rawURL string) (string, bool) {
	u, err := url.Parse(rawURL)
	if err != nil || !g.isSelfHost(u.Hostname()) {
		return "", false
	}
	rest, ok := strings.CutPrefix(u.Path, PathUsers)
	if !ok {
		return "", false
	}
	userID, _, _ := strings.Cut(rest, "/")
	return userID, userID != ""
}

// Config is what newGateway builds the ActivityPub front from.
type Config struct {
	Host string
	Key  *rsa.PrivateKey
	// Source renders local users; nil serves none.
	Source WarpnetSource
	// req reaches Warpnet; nil keeps the follower graph in memory (dev mode).
	Requester warp.Requester
	Client    *http.Client
	Logs      *logring.Ring
	LogsToken string
	// ForeignHandle names ids of other bridged networks (see gateway).
	ForeignHandle func(string) bool
	// AllowPrivateTargets lets tests federate with instances on loopback.
	AllowPrivateTargets bool
}

// New builds the ActivityPub front with its caches and retries.
func New(cfg Config) (*Gateway, error) {
	pubPEM, err := publicKeyPEM(cfg.Key)
	if err != nil {
		return nil, err
	}
	if cfg.Source == nil {
		cfg.Source = StaticSource{}
	}
	if cfg.Client == nil {
		cfg.Client = netguard.NewClient(15 * time.Second)
	}
	g := &Gateway{
		Host:          cfg.Host,
		key:           cfg.Key,
		keyPubPEM:     pubPEM,
		source:        cfg.Source,
		client:        cfg.Client,
		sem:           make(chan struct{}, maxInflightDeliveries),
		Requester:     cfg.Requester,
		foreignHandle: cfg.ForeignHandle,
		// Retry transient Mastodon HTTP failures (network errors, 429, 5xx) a few
		// times with exponential backoff; bounded so it stays within the request budget.
		retrier:             retrier.New(300*time.Millisecond, 3, retrier.ExponentialBackoff),
		getCache:            expirable.NewLRU[string, cachedGet](getCacheSize, nil, getCacheTTL),
		actorIDs:            expirable.NewLRU[string, string](actorIDsSize, nil, actorIDsTTL),
		handles:             expirable.NewLRU[string, string](actorIDsSize, nil, actorIDsTTL),
		logs:                cfg.Logs,
		logsToken:           cfg.LogsToken,
		allowPrivateTargets: cfg.AllowPrivateTargets,
	}
	// The follower graph lives in Warpnet, read and written through the
	// user's node; the store resolves follower handles back to actor urls
	// through the gateway. Without a node it falls back to memory.
	if cfg.Requester != nil {
		g.Followers = nodeFollowerStore{req: cfg.Requester, resolver: g}
	} else {
		g.Followers = newMemFollowerStore()
	}
	return g, nil
}
