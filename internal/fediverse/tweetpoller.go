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
	"context"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/warp"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

const (
	tweetPollInterval = 60 * time.Second

	// seen-set bounds: ids still present in the fetched page get their TTL
	// refreshed on every poll, so only tweets long gone from the feed expire —
	// expiry can't cause a re-publish.
	tweetSeenSize = 8192
	tweetSeenTTL  = 24 * time.Hour
)

// tweetPoller watches the bridged owner's Warpnet tweets and federates new
// original top-level posts to Fediverse followers via publish. It is stateless
// across restarts: at startup it marks existing tweets as seen, so only posts
// created afterwards are federated (no replaying history on every restart).
// The seen set is a bounded expirable LRU, never unbounded growth.
type tweetPoller struct {
	req      warp.Requester
	network  string
	owner    string
	interval time.Duration
	seen     *expirable.LRU[string, struct{}]
	publish  func(ctx context.Context, owner string, t warp.Tweet)
}

func newTweetPoller(req warp.Requester, owner string, publish func(context.Context, string, warp.Tweet)) *tweetPoller {
	return &tweetPoller{
		req:      req,
		network:  warp.NetworkOf(req),
		owner:    owner,
		interval: tweetPollInterval,
		seen:     expirable.NewLRU[string, struct{}](tweetSeenSize, nil, tweetSeenTTL),
		publish:  publish,
	}
}

func (p *tweetPoller) run(ctx context.Context) {
	seed := p.fetch() // seed: don't replay history
	for _, t := range seed {
		p.seen.Add(t.Id, struct{}{})
	}
	warp.NetLog(p.network).Infof("poller: started for %s (seeded %d existing tweets)", p.owner, len(seed))
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *tweetPoller) poll(ctx context.Context) {
	tweets := p.fetch()
	newCount, pubCount := 0, 0
	for _, t := range tweets {
		if p.seen.Contains(t.Id) {
			p.seen.Add(t.Id, struct{}{}) // refresh TTL while the id is still in the feed
			continue
		}
		p.seen.Add(t.Id, struct{}{})
		newCount++
		if warp.PublishableTweet(t, p.owner) {
			pubCount++
			p.publish(ctx, p.owner, t)
		}
	}
	warp.NetLog(p.network).Infof("poller: %s: fetched %d, new %d, publishable %d", p.owner, len(tweets), newCount, pubCount)
}

func (p *tweetPoller) fetch() []warp.Tweet {
	return warp.FetchTweets(p.req, p.owner)
}
