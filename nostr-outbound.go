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

// nostrOutbound publishes consenting Warpnet users' own posts to Nostr. A
// scan finds the consenting users of a network; each gets a poller that
// publishes new posts and edits as kind 1, vanished posts as kind 5, and the
// profile as kind 0 whenever it changes. Like tweetPoller it seeds what
// already exists, so no history is replayed and nothing is stored.

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

const (
	nostrOutboundScanInterval = 30 * time.Minute
	nostrPostPollInterval     = time.Minute
	// nostrProfileEvery polls pass between profile checks.
	nostrProfileEvery = 30
)

type nostrOutbound struct {
	ctx context.Context
	req nodeRequester
	pub *nostrPublisher

	mu      sync.Mutex
	started map[string]bool
}

func newNostrOutbound(ctx context.Context, req nodeRequester, pub *nostrPublisher) *nostrOutbound {
	return &nostrOutbound{ctx: ctx, req: req, pub: pub, started: map[string]bool{}}
}

func (o *nostrOutbound) runScanner(scanUser string) {
	for {
		o.scan(scanUser)
		select {
		case <-o.ctx.Done():
			return
		case <-time.After(nostrOutboundScanInterval):
		}
	}
}

// scan starts a poller for every consenting user; consent cannot be revoked,
// so a poller is never stopped (it skips while an account is locked).
func (o *nostrOutbound) scan(scanUser string) {
	for _, u := range nativeUsers(o.req, scanUser) {
		if o.pub.consents(u.Id) {
			o.start(u.Id)
		}
	}
}

func (o *nostrOutbound) start(owner string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.started[owner] {
		return
	}
	o.started[owner] = true
	netLog(o.pub.network).Infof("nostr: publishing %s", owner)
	go newNostrPostPoller(o.req, o.pub, owner).run(o.ctx)
}

type nostrPostPoller struct {
	feed     *tweetPoller
	pub      *nostrPublisher
	owner    string
	interval time.Duration
	// seen holds tweet versions, id plus edit time, so an edit is new.
	seen    *expirable.LRU[string, struct{}]
	last    []tweet
	profile string
	polls   int
}

func newNostrPostPoller(req nodeRequester, pub *nostrPublisher, owner string) *nostrPostPoller {
	return &nostrPostPoller{
		feed:     newTweetPoller(req, owner, nil),
		pub:      pub,
		owner:    owner,
		interval: nostrPostPollInterval,
		seen:     expirable.NewLRU[string, struct{}](tweetSeenSize, nil, tweetSeenTTL),
	}
}

func tweetVersion(t tweet) string {
	if t.UpdatedAt == nil {
		return t.Id
	}
	return t.Id + "|" + strconv.FormatInt(t.UpdatedAt.UnixNano(), 10)
}

func (p *nostrPostPoller) run(ctx context.Context) {
	p.seed()
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.poll()
		}
	}
}

func (p *nostrPostPoller) seed() {
	p.last = p.feed.fetch()
	for _, t := range p.last {
		p.seen.Add(tweetVersion(t), struct{}{})
	}
	p.profile = p.pub.PublishProfile(p.owner, p.profile)
}

func (p *nostrPostPoller) poll() {
	p.polls++
	if !p.pub.consents(p.owner) {
		return
	}
	if p.polls%nostrProfileEvery == 0 {
		p.profile = p.pub.PublishProfile(p.owner, p.profile)
	}
	cur := p.feed.fetch()
	if len(cur) == 0 {
		return // a failed read is not an empty feed
	}
	for _, t := range cur {
		v := tweetVersion(t)
		if p.seen.Contains(v) {
			p.seen.Add(v, struct{}{})
			continue
		}
		p.seen.Add(v, struct{}{})
		p.pub.PublishPost(p.owner, t)
	}
	for _, t := range vanished(p.last, cur) {
		if nostrPublishablePost(t, p.owner) {
			p.pub.DeletePost(p.owner, t.Id)
		}
	}
	p.last = cur
}

// vanished lists the posts of prev missing from cur that cur's page still
// spans: anything older than cur's oldest post may just have scrolled out.
func vanished(prev, cur []tweet) []tweet {
	present := make(map[string]bool, len(cur))
	oldest := time.Time{}
	for _, t := range cur {
		present[t.Id] = true
		if oldest.IsZero() || t.CreatedAt.Before(oldest) {
			oldest = t.CreatedAt
		}
	}
	var out []tweet
	for _, t := range prev {
		if !present[t.Id] && t.CreatedAt.After(oldest) {
			out = append(out, t)
		}
	}
	return out
}
