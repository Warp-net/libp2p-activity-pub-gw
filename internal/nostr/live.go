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
	"encoding/json"
	"sync"
	"time"

	"github.com/Warp-net/activity-pub-gw/internal/warp"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/time/rate"
)

// nostrLive holds the Warpnet -> Nostr follow graph in memory (learned from
// follow routes and rebuilt from Warpnet by a scan), keeps one live
// subscription on the followed authors and pushes their new notes to the
// followers' nodes. The same graph defines who is trusted: followed authors
// and the authors they follow.

const (
	// nostrResubscribeDelay lets a burst of follows settle into one resubscription.
	nostrResubscribeDelay = 5 * time.Second
	nostrHopRefresh       = 30 * time.Minute
	nostrScanInterval     = 30 * time.Minute
	nostrLiveMaxAuthors   = 1000
	nostrLiveMaxRelays    = 30
	nostrPushTimeout      = time.Minute
	nostrAuthorRate       = rate.Limit(1.0 / 6) // one note per 6s per author
	nostrAuthorBurst      = 5
)

type Live struct {
	bridge *Bridge
	push   func(follower string, t warp.Tweet) error

	mu      sync.Mutex
	follows map[string]map[string]struct{} // author pubkey -> Warpnet follower ids
	hop     map[string]struct{}            // authors the followed authors follow
	changed chan struct{}
	limits  *expirable.LRU[string, *rate.Limiter]
	// pushed remembers delivered notes: while a subscription is re-opened the
	// old and the new one overlap and both carry the same note.
	pushed *expirable.LRU[string, struct{}]

	resubscribeDelay time.Duration
}

func NewLive(bridge *Bridge, push func(follower string, t warp.Tweet) error) *Live {
	return &Live{
		bridge:  bridge,
		push:    push,
		follows: map[string]map[string]struct{}{},
		hop:     map[string]struct{}{},
		changed: make(chan struct{}, 1),
		limits:  expirable.NewLRU[string, *rate.Limiter](nostrCacheSize, nil, time.Hour),
		pushed:  expirable.NewLRU[string, struct{}](nostrCacheSize, nil, time.Hour),

		resubscribeDelay: nostrResubscribeDelay,
	}
}

// SetFollow records a Warpnet user following (or unfollowing) a Nostr account.
func (l *Live) SetFollow(follower, handle string, undo bool) {
	pk, ok := nostrPubKey(handle)
	if l == nil || !ok || follower == "" {
		return
	}
	l.mu.Lock()
	fs := l.follows[pk]
	_, had := fs[follower]
	switch {
	case undo && had:
		delete(fs, follower)
		if len(fs) == 0 {
			delete(l.follows, pk)
		}
	case !undo && !had:
		if fs == nil {
			fs = map[string]struct{}{}
			l.follows[pk] = fs
		}
		fs[follower] = struct{}{}
	}
	l.mu.Unlock()
	if undo == had {
		l.notify()
	}
}

// setFollowings replaces what the scan read for one Warpnet user.
func (l *Live) setFollowings(follower string, pks map[string]bool) {
	l.mu.Lock()
	dirty := false
	for pk, fs := range l.follows {
		if _, ok := fs[follower]; ok && !pks[pk] {
			delete(fs, follower)
			if len(fs) == 0 {
				delete(l.follows, pk)
			}
			dirty = true
		}
	}
	for pk := range pks {
		fs := l.follows[pk]
		if fs == nil {
			fs = map[string]struct{}{}
			l.follows[pk] = fs
		}
		if _, ok := fs[follower]; !ok {
			fs[follower] = struct{}{}
			dirty = true
		}
	}
	l.mu.Unlock()
	if dirty {
		l.notify()
	}
}

func (l *Live) notify() {
	select {
	case l.changed <- struct{}{}:
	default:
	}
}

// Trusted reports whether pk is followed by a Warpnet user or followed by
// such an account.
func (l *Live) Trusted(pk string) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.follows[pk]; ok {
		return true
	}
	_, ok := l.hop[pk]
	return ok
}

func (l *Live) authors() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, 0, len(l.follows))
	for pk := range l.follows {
		out = append(out, pk)
		if len(out) == nostrLiveMaxAuthors {
			break
		}
	}
	return out
}

func (l *Live) FollowersOf(pk string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, 0, len(l.follows[pk]))
	for f := range l.follows[pk] {
		out = append(out, f)
	}
	return out
}

// Run keeps one subscription on all followed authors, re-opened whenever the
// set changes.
func (l *Live) Run(ctx context.Context) {
	for {
		authors := l.authors()
		l.refreshHop(ctx, authors)
		subCtx, cancel := context.WithCancel(ctx)
		if len(authors) > 0 {
			relays := l.relaysFor(ctx, authors)
			go l.bridge.relays.subscribe(subCtx, relays, nostrFilter{Kinds: []int{KindNote, kindPoll}, Authors: authors}, l.onEvent)
			warp.NetLog("").Infof("nostr: following %d authors on %d relays", len(authors), len(relays))
		}
		select {
		case <-ctx.Done():
			cancel()
			return
		case <-l.changed:
			select {
			case <-ctx.Done():
			case <-time.After(l.resubscribeDelay):
			}
		case <-time.After(nostrHopRefresh):
		}
		cancel()
	}
}

func (l *Live) relaysFor(ctx context.Context, authors []string) []string {
	out := append([]string{}, l.bridge.defaults...)
	for _, pk := range authors {
		out = dedupeStrings(append(out, l.bridge.authorRelays(ctx, pk)...))
		if len(out) >= nostrLiveMaxRelays {
			return out[:nostrLiveMaxRelays]
		}
	}
	return out
}

// hopSet gathers the authors the followed authors follow.
type hopSet struct {
	mu   sync.Mutex
	keys map[string]struct{}
	sem  chan struct{}
}

// refreshHop rebuilds the one-hop trust set from the followed authors' lists.
func (l *Live) refreshHop(ctx context.Context, authors []string) {
	hop := &hopSet{keys: map[string]struct{}{}, sem: make(chan struct{}, 8)}
	var wg sync.WaitGroup
	for _, pk := range authors {
		hop.sem <- struct{}{}
		wg.Add(1)
		go l.addFollowsOf(ctx, &wg, pk, hop)
	}
	wg.Wait()
	l.mu.Lock()
	l.hop = hop.keys
	l.mu.Unlock()
}

func (l *Live) addFollowsOf(ctx context.Context, wg *sync.WaitGroup, pk string, hop *hopSet) {
	defer wg.Done()
	fl := l.bridge.followList(ctx, pk)
	hop.mu.Lock()
	for _, f := range fl {
		hop.keys[f] = struct{}{}
	}
	hop.mu.Unlock()
	<-hop.sem
}

func (l *Live) allow(pk string) bool {
	lim, ok := l.limits.Get(pk)
	if !ok {
		lim = rate.NewLimiter(nostrAuthorRate, nostrAuthorBurst)
		l.limits.Add(pk, lim)
	}
	return lim.Allow()
}

// onEvent pushes a followed author's new top-level note to every follower's
// node. Replies stay in their thread, as with Mastodon.
func (l *Live) onEvent(ev nostrEvent) {
	if droppedNostrEvent(ev, l.bridge.selfHost) {
		return
	}
	followers := l.FollowersOf(ev.PubKey)
	if len(followers) == 0 || !l.firstSight(ev.ID) {
		return
	}
	if !l.allow(ev.PubKey) {
		warp.NetLog("").Infof("nostr: %s is over its rate limit, note %s dropped", ev.PubKey, ev.ID)
		return
	}
	t, ok := nostrNoteToTweet(ev)
	if !ok || t.ParentId != nil {
		return
	}
	l.bridge.authors.Add(ev.ID, ev.PubKey)
	go l.pushToFollowers(t, followers)
}

func (l *Live) firstSight(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.pushed.Contains(id) {
		return false
	}
	l.pushed.Add(id, struct{}{})
	return true
}

func (l *Live) pushToFollowers(t warp.Tweet, followers []string) {
	ctx, cancel := context.WithTimeout(context.Background(), nostrPushTimeout)
	defer cancel()
	ts := []warp.Tweet{t}
	l.bridge.names(ctx, ts)
	for _, f := range followers {
		if err := l.push(f, ts[0]); err != nil {
			warp.NetLog("").Warnf("nostr: push %s to %s: %v", t.Id, f, err)
		}
	}
}

// RunScanner rebuilds the follow graph from Warpnet: every native user's
// followings that are Nostr accounts. Nothing is stored, so a restart
// recovers the graph from the network alone.
func (l *Live) RunScanner(ctx context.Context, req warp.Requester, scanUser string) {
	for {
		l.scan(req, scanUser)
		select {
		case <-ctx.Done():
			return
		case <-time.After(nostrScanInterval):
		}
	}
}

func (l *Live) scan(req warp.Requester, scanUser string) {
	for _, u := range warp.NativeUsers(req, scanUser) {
		fbt, ferr := req.RequestUser(u.Id, warp.RouteGetFollowings, warp.GetFollowersEvent{UserId: u.Id})
		if ferr != nil || warp.NodeResponseError(fbt) != nil {
			continue
		}
		var fr warp.FollowingsResponse
		if json.Unmarshal(fbt, &fr) != nil {
			continue
		}
		pks := map[string]bool{}
		for _, id := range fr.Followings {
			if pk, ok := nostrPubKey(id); ok {
				pks[pk] = true
			}
		}
		// An empty read is not authoritative (see outboundFederation.scan).
		if len(pks) > 0 {
			l.setFollowings(u.Id, pks)
		}
	}
}
