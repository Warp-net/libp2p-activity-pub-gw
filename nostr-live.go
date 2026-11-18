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

// nostrLive holds the Warpnet -> Nostr follow graph in memory (learned from
// follow routes and rebuilt from Warpnet by a scan), keeps one live
// subscription on the followed authors and pushes their new notes to the
// followers' nodes. The same graph defines who is trusted: followed authors
// and the authors they follow.

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/time/rate"
)

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

type nostrLive struct {
	bridge *nostrBridge
	push   func(follower string, t tweet) error

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

func newNostrLive(bridge *nostrBridge, push func(follower string, t tweet) error) *nostrLive {
	return &nostrLive{
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

// setFollow records a Warpnet user following (or unfollowing) a Nostr account.
func (l *nostrLive) setFollow(follower, handle string, undo bool) {
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
func (l *nostrLive) setFollowings(follower string, pks map[string]bool) {
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

func (l *nostrLive) notify() {
	select {
	case l.changed <- struct{}{}:
	default:
	}
}

// trusted reports whether pk is followed by a Warpnet user or followed by
// such an account.
func (l *nostrLive) trusted(pk string) bool {
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

func (l *nostrLive) authors() []string {
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

func (l *nostrLive) followersOf(pk string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, 0, len(l.follows[pk]))
	for f := range l.follows[pk] {
		out = append(out, f)
	}
	return out
}

// run keeps one subscription on all followed authors, re-opened whenever the
// set changes.
func (l *nostrLive) run(ctx context.Context) {
	for {
		authors := l.authors()
		l.refreshHop(ctx, authors)
		subCtx, cancel := context.WithCancel(ctx)
		if len(authors) > 0 {
			relays := l.relaysFor(ctx, authors)
			go l.bridge.relays.subscribe(subCtx, relays, nostrFilter{Kinds: []int{kindNote, kindPoll}, Authors: authors}, l.onEvent)
			netLog("").Infof("nostr: following %d authors on %d relays", len(authors), len(relays))
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

func (l *nostrLive) relaysFor(ctx context.Context, authors []string) []string {
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
func (l *nostrLive) refreshHop(ctx context.Context, authors []string) {
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

func (l *nostrLive) addFollowsOf(ctx context.Context, wg *sync.WaitGroup, pk string, hop *hopSet) {
	defer wg.Done()
	fl := l.bridge.followList(ctx, pk)
	hop.mu.Lock()
	for _, f := range fl {
		hop.keys[f] = struct{}{}
	}
	hop.mu.Unlock()
	<-hop.sem
}

func (l *nostrLive) allow(pk string) bool {
	lim, ok := l.limits.Get(pk)
	if !ok {
		lim = rate.NewLimiter(nostrAuthorRate, nostrAuthorBurst)
		l.limits.Add(pk, lim)
	}
	return lim.Allow()
}

// onEvent pushes a followed author's new top-level note to every follower's
// node. Replies stay in their thread, as with Mastodon.
func (l *nostrLive) onEvent(ev nostrEvent) {
	if droppedNostrEvent(ev, l.bridge.selfHost) {
		return
	}
	followers := l.followersOf(ev.PubKey)
	if len(followers) == 0 || !l.firstSight(ev.ID) {
		return
	}
	if !l.allow(ev.PubKey) {
		netLog("").Infof("nostr: %s is over its rate limit, note %s dropped", ev.PubKey, ev.ID)
		return
	}
	t, ok := nostrNoteToTweet(ev)
	if !ok || t.ParentId != nil {
		return
	}
	l.bridge.authors.Add(ev.ID, ev.PubKey)
	go l.pushToFollowers(t, followers)
}

func (l *nostrLive) firstSight(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.pushed.Contains(id) {
		return false
	}
	l.pushed.Add(id, struct{}{})
	return true
}

func (l *nostrLive) pushToFollowers(t tweet, followers []string) {
	ctx, cancel := context.WithTimeout(context.Background(), nostrPushTimeout)
	defer cancel()
	ts := []tweet{t}
	l.bridge.names(ctx, ts)
	for _, f := range followers {
		if err := l.push(f, ts[0]); err != nil {
			netLog("").Warnf("nostr: push %s to %s: %v", t.Id, f, err)
		}
	}
}

// runScanner rebuilds the follow graph from Warpnet: every native user's
// followings that are Nostr accounts. Nothing is stored, so a restart
// recovers the graph from the network alone.
func (l *nostrLive) runScanner(ctx context.Context, req nodeRequester, scanUser string) {
	for {
		l.scan(req, scanUser)
		select {
		case <-ctx.Done():
			return
		case <-time.After(nostrScanInterval):
		}
	}
}

func (l *nostrLive) scan(req nodeRequester, scanUser string) {
	limit := federationScanPageSize
	var cursor string
	for range federationScanMaxPages {
		ev := getAllUsersEvent{UserId: scanUser, Limit: &limit}
		if cursor != "" {
			ev.Cursor = &cursor
		}
		bt, err := req.request(routeGetUsers, ev)
		if err != nil {
			return
		}
		var resp usersResponse
		if json.Unmarshal(bt, &resp) != nil {
			return
		}
		for _, u := range resp.Users {
			if isBridgedNetwork(u.Network) || isBridgedUserID(u.Id) {
				continue
			}
			fbt, ferr := req.requestUser(u.Id, routeGetFollowings, getFollowersEvent{UserId: u.Id})
			if ferr != nil || nodeResponseError(fbt) != nil {
				continue
			}
			var fr followingsResponse
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
		if len(resp.Users) == 0 || resp.Cursor == "" || resp.Cursor == cursor {
			return
		}
		cursor = resp.Cursor
	}
}
