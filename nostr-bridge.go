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

// nostrBridge answers Warpnet's public read routes for npub1…@nostr.com
// accounts and their notes from public Nostr relays, next to mastodonBridge.

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Warp-net/warpnet/domain"
	"github.com/Warp-net/warpnet/event"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

var errNostrNotFound = errors.New("nostr: not found")

// defaultNostrRelays seed every lookup; author relays come from their kind
// 10002 list. purplepag.es and user.kindpag.es index profiles and relay lists.
var defaultNostrRelays = []string{
	"wss://relay.damus.io",
	"wss://relay.primal.net",
	"wss://nos.lol",
	"wss://purplepag.es",
	"wss://user.kindpag.es",
}

// nostrRelaysFromEnv lets an operator replace the default relays
// (GATEWAY_NOSTR_RELAYS, comma-separated).
func nostrRelaysFromEnv() []string {
	raw := strings.TrimSpace(os.Getenv("GATEWAY_NOSTR_RELAYS"))
	if raw == "" {
		return defaultNostrRelays
	}
	var out []string
	for _, r := range strings.Split(raw, ",") {
		if r = strings.TrimSpace(r); r != "" {
			out = append(out, r)
		}
	}
	return out
}

const (
	nostrCacheSize     = 4096
	nostrProfileTTL    = 10 * time.Minute
	nostrRelayListTTL  = 30 * time.Minute
	nostrAuthorRelays  = 5
	nostrPageSize      = 20
	nostrMaxPage       = 50
	nostrBoundedFetch  = 500
	nostrMaxFollowList = 1000
)

type nostrBridge struct {
	relays   nostrRelays
	defaults []string
	nodeID   string // gateway peer id stamped onto bridged users
	selfHost string
	// trusted reports whether an author's replies, reactions and reposts may
	// enter Warpnet (see nostrLive.trusted); nil trusts nobody.
	trusted func(pk string) bool

	profiles   *expirable.LRU[string, nostrEvent]
	relayLists *expirable.LRU[string, []string]
	follows    *expirable.LRU[string, []string]
	authors    *expirable.LRU[string, string] // event id -> author pubkey
}

func newNostrBridge(relays nostrRelays, defaults []string, nodeID, selfHost string) *nostrBridge {
	return &nostrBridge{
		relays: relays, defaults: defaults, nodeID: nodeID, selfHost: selfHost,
		profiles:   expirable.NewLRU[string, nostrEvent](nostrCacheSize, nil, nostrProfileTTL),
		relayLists: expirable.NewLRU[string, []string](nostrCacheSize, nil, nostrRelayListTTL),
		follows:    expirable.NewLRU[string, []string](nostrCacheSize, nil, nostrProfileTTL),
		authors:    expirable.NewLRU[string, string](nostrCacheSize, nil, nostrRelayListTTL),
	}
}

func (b *nostrBridge) isTrusted(pk string) bool {
	return b.trusted != nil && b.trusted(pk)
}

// authorRelays is where pk publishes (NIP-65 write relays) plus the defaults.
func (b *nostrBridge) authorRelays(ctx context.Context, pk string) []string {
	if rl, ok := b.relayLists.Get(pk); ok {
		return rl
	}
	var write []string
	evs := b.relays.fetch(ctx, b.defaults, nostrFilter{Kinds: []int{kindRelayList}, Authors: []string{pk}, Limit: 1})
	if len(evs) > 0 {
		for _, t := range evs[0].Tags {
			if len(t) < 2 || t[0] != "r" || (len(t) >= 3 && t[2] == "read") {
				continue
			}
			write = append(write, t[1])
			if len(write) == nostrAuthorRelays {
				break
			}
		}
	}
	out := dedupeStrings(append(write, b.defaults...))
	b.relayLists.Add(pk, out)
	return out
}

func (b *nostrBridge) profile(ctx context.Context, pk string) (nostrEvent, bool) {
	if p, ok := b.profiles.Get(pk); ok {
		return p, p.ID != ""
	}
	var p nostrEvent
	evs := b.relays.fetch(ctx, b.authorRelays(ctx, pk), nostrFilter{Kinds: []int{kindProfile}, Authors: []string{pk}, Limit: 1})
	if len(evs) > 0 && evs[0].PubKey == pk {
		p = evs[0]
	}
	b.profiles.Add(pk, p)
	return p, p.ID != ""
}

// names fills the display names of the authors of ts from their profiles,
// fetching the missing ones in one request.
func (b *nostrBridge) names(ctx context.Context, ts []tweet) {
	var missing []string
	for _, t := range ts {
		if pk, ok := nostrPubKey(t.UserId); ok && !b.profiles.Contains(pk) {
			missing = append(missing, pk)
		}
	}
	if missing = dedupeStrings(missing); len(missing) > 0 {
		evs := b.relays.fetch(ctx, b.defaults, nostrFilter{Kinds: []int{kindProfile}, Authors: missing, Limit: len(missing)})
		got := map[string]nostrEvent{}
		for _, ev := range evs {
			if _, seen := got[ev.PubKey]; !seen {
				got[ev.PubKey] = ev
			}
		}
		for _, pk := range missing {
			b.profiles.Add(pk, got[pk])
		}
	}
	for i := range ts {
		pk, _ := nostrPubKey(ts[i].UserId)
		if p, ok := b.profiles.Get(pk); ok && p.ID != "" {
			if u, ok := profileToUser(p, b.nodeID); ok {
				ts[i].Username = u.Username
			}
		}
	}
}

func (b *nostrBridge) GetUser(ctx context.Context, handle string) (user, error) {
	return b.getUser(ctx, handle, true)
}

func (b *nostrBridge) GetUserBrief(ctx context.Context, handle string) (user, error) {
	return b.getUser(ctx, handle, false)
}

// getUser shows any account looked up by its npub, trusted or not: the user
// asked for it by its unique id.
func (b *nostrBridge) getUser(ctx context.Context, handle string, withCounts bool) (user, error) {
	pk, ok := nostrPubKey(handle)
	if !ok {
		return user{}, errNostrNotFound
	}
	p, ok := b.profile(ctx, pk)
	if !ok || droppedNostrEvent(p, b.selfHost) {
		return user{}, errNostrNotFound
	}
	u, _ := profileToUser(p, b.nodeID)
	if !withCounts {
		return u, nil
	}
	relays := b.authorRelays(ctx, pk)
	var wg sync.WaitGroup
	wg.Add(3)
	go b.followingsInto(ctx, &wg, pk, &u.FollowingsCount)
	go b.countInto(ctx, &wg, relays, nostrFilter{Kinds: []int{kindFollowList}, Tags: map[string][]string{"p": {pk}}}, &u.FollowersCount)
	go b.countInto(ctx, &wg, relays, nostrFilter{Kinds: []int{kindNote}, Authors: []string{pk}}, &u.TweetsCount)
	wg.Wait()
	return u, nil
}

func (b *nostrBridge) followingsInto(ctx context.Context, wg *sync.WaitGroup, pk string, out *int64) {
	defer wg.Done()
	*out = int64(len(b.followList(ctx, pk)))
}

func (b *nostrBridge) countInto(ctx context.Context, wg *sync.WaitGroup, relays []string, f nostrFilter, out *int64) {
	defer wg.Done()
	*out = b.countOr(ctx, relays, f)
}

// countOr asks for a NIP-45 count and falls back to a bounded fetch, so the
// number is approximate either way.
func (b *nostrBridge) countOr(ctx context.Context, relays []string, f nostrFilter) int64 {
	if n, ok := b.relays.count(ctx, relays, f); ok {
		return n
	}
	f.Limit = nostrBoundedFetch
	return int64(len(b.relays.fetch(ctx, relays, f)))
}

// followList is pk's kind 3 follow list as hex keys.
func (b *nostrBridge) followList(ctx context.Context, pk string) []string {
	if fl, ok := b.follows.Get(pk); ok {
		return fl
	}
	var out []string
	evs := b.relays.fetch(ctx, b.authorRelays(ctx, pk), nostrFilter{Kinds: []int{kindFollowList}, Authors: []string{pk}, Limit: 1})
	if len(evs) > 0 && evs[0].PubKey == pk {
		for _, p := range tagValues(evs[0], "p") {
			if hex64.MatchString(p) {
				out = append(out, p)
			}
			if len(out) == nostrMaxFollowList {
				break
			}
		}
	}
	b.follows.Add(pk, out)
	return out
}

func (b *nostrBridge) GetFollowings(ctx context.Context, handle string, _ *string) (followingsResponse, error) {
	pk, ok := nostrPubKey(handle)
	if !ok {
		return followingsResponse{}, errNostrNotFound
	}
	return followingsResponse{FollowerId: handle, Followings: handlesOf(b.followList(ctx, pk))}, nil
}

// GetFollowers approximates the followers as the authors of kind 3 lists that
// name pk.
func (b *nostrBridge) GetFollowers(ctx context.Context, handle string, _ *string) (followersResponse, error) {
	pk, ok := nostrPubKey(handle)
	if !ok {
		return followersResponse{}, errNostrNotFound
	}
	evs := b.relays.fetch(ctx, b.authorRelays(ctx, pk), nostrFilter{
		Kinds: []int{kindFollowList}, Tags: map[string][]string{"p": {pk}}, Limit: nostrBoundedFetch,
	})
	var pks []string
	for _, ev := range evs {
		if !droppedNostrEvent(ev, b.selfHost) {
			pks = append(pks, ev.PubKey)
		}
	}
	return followersResponse{FollowingId: handle, Followers: handlesOf(dedupeStrings(pks))}, nil
}

func handlesOf(pks []string) []string {
	out := make([]string, 0, len(pks))
	for _, pk := range pks {
		if h := nostrHandle(pk); h != "" {
			out = append(out, h)
		}
	}
	return out
}

func (b *nostrBridge) GetTweetsOrReplies(ctx context.Context, ev getAllTweetsEvent) (tweetsResponse, error) {
	if ev.RootId != "" || ev.ParentId != "" {
		id := ev.ParentId
		if id == "" {
			id = ev.RootId
		}
		return b.GetReplies(ctx, id)
	}
	return b.GetTweets(ctx, ev.UserId, ev.Limit, ev.Cursor)
}

// GetTweets is an author's top-level notes, polls and reposts, newest first.
// A numeric cursor is the "until" of the next page; the node's own datastore
// cursors mean nothing here and restart from the newest.
func (b *nostrBridge) GetTweets(ctx context.Context, handle string, limit *uint64, cursor *string) (tweetsResponse, error) {
	pk, ok := nostrPubKey(handle)
	if !ok {
		return tweetsResponse{}, errNostrNotFound
	}
	n := nostrPageSize
	if limit != nil && *limit > 0 {
		n = min(int(*limit), nostrMaxPage) //nolint:gosec // bounded above
	}
	f := nostrFilter{Kinds: []int{kindNote, kindRepost, kindGenericRepost, kindPoll}, Authors: []string{pk}, Limit: n}
	if cursor != nil {
		if until, err := strconv.ParseInt(*cursor, 10, 64); err == nil && until > 0 {
			f.Until = until
		}
	}
	// Each relay returns its own n newest; merged, they cover different spans,
	// so only the n newest overall are contiguous and the cursor follows them.
	evs := b.relays.fetch(ctx, b.authorRelays(ctx, pk), f)
	if len(evs) > n {
		evs = evs[:n]
	}
	resp := tweetsResponse{UserId: handle, Tweets: []tweet{}}
	if len(evs) > 0 {
		resp.Cursor = strconv.FormatInt(evs[len(evs)-1].CreatedAt-1, 10)
	}
	for _, t := range b.render(ctx, evs, false, "") {
		if t.ParentId == nil {
			resp.Tweets = append(resp.Tweets, t)
		}
	}
	return resp, nil
}

// GetReplies is the direct replies to a note from trusted authors and the
// note's own author.
func (b *nostrBridge) GetReplies(ctx context.Context, id string) (tweetsResponse, error) {
	id = strings.TrimPrefix(id, domain.RetweetPrefix)
	resp := tweetsResponse{UserId: id, Tweets: []tweet{}}
	if !isNostrEventID(id) {
		return resp, errNostrNotFound
	}
	author, known := b.authors.Get(id)
	if !known {
		if _, err := b.GetTweet(ctx, id); err == nil {
			author, _ = b.authors.Get(id)
		}
	}
	evs := b.relays.fetch(ctx, b.relaysFor(ctx, id), nostrFilter{
		Kinds: []int{kindNote}, Tags: map[string][]string{"e": {id}}, Limit: nostrBoundedFetch,
	})
	for _, t := range b.render(ctx, evs, true, author) {
		if t.ParentId != nil && *t.ParentId == id {
			resp.Tweets = append(resp.Tweets, t)
		}
	}
	return resp, nil
}

func (b *nostrBridge) relaysFor(ctx context.Context, id string) []string {
	if pk, ok := b.authors.Get(id); ok {
		return b.authorRelays(ctx, pk)
	}
	return b.defaults
}

func (b *nostrBridge) GetTweet(ctx context.Context, id string) (tweet, error) {
	id = strings.TrimPrefix(id, domain.RetweetPrefix)
	if !isNostrEventID(id) {
		return tweet{}, errNostrNotFound
	}
	evs := b.relays.fetch(ctx, b.relaysFor(ctx, id), nostrFilter{IDs: []string{id}, Limit: 1})
	ts := b.render(ctx, evs, false, "")
	if len(ts) == 0 {
		return tweet{}, errNostrNotFound
	}
	return ts[0], nil
}

// GetTweetStats counts a note's reactions, reposts and direct replies from
// trusted authors, one reaction per author.
func (b *nostrBridge) GetTweetStats(ctx context.Context, id string) (event.TweetStatsResponse, error) {
	id = strings.TrimPrefix(id, domain.RetweetPrefix)
	resp := event.TweetStatsResponse{TweetId: domain.ID(id)}
	if !isNostrEventID(id) {
		return resp, errNostrNotFound
	}
	relays := b.relaysFor(ctx, id)
	var reactions, reposts, replies []nostrEvent
	var wg sync.WaitGroup
	wg.Add(3)
	go b.trustedReferrers(ctx, &wg, relays, id, &reactions, kindReaction)
	go b.trustedReferrers(ctx, &wg, relays, id, &reposts, kindRepost, kindGenericRepost)
	go b.trustedReferrers(ctx, &wg, relays, id, &replies, kindNote)
	wg.Wait()

	reacted := map[string]bool{}
	for _, ev := range reactions {
		emoji, ok := reactionEmoji(ev.Content)
		if !ok || reacted[ev.PubKey] || lastTagValue(ev, "e") != id {
			continue
		}
		reacted[ev.PubKey] = true
		if resp.Reactions == nil {
			resp.Reactions = map[string]uint64{}
		}
		resp.Reactions[emoji]++
		resp.ReactionsCount++
	}
	resp.RetweetsCount = uint64(len(reposts))
	for _, ev := range replies {
		if _, parent := threadRefs(ev); parent == id {
			resp.RepliesCount++
		}
	}
	return resp, nil
}

// trustedReferrers collects the events of the given kinds that reference id
// and come from trusted authors.
func (b *nostrBridge) trustedReferrers(ctx context.Context, wg *sync.WaitGroup, relays []string, id string, out *[]nostrEvent, kinds ...int) {
	defer wg.Done()
	for _, ev := range b.relays.fetch(ctx, relays, nostrFilter{Kinds: kinds, Tags: map[string][]string{"e": {id}}, Limit: nostrBoundedFetch}) {
		if b.isTrusted(ev.PubKey) && !droppedNostrEvent(ev, b.selfHost) {
			*out = append(*out, ev)
		}
	}
}

func lastTagValue(ev nostrEvent, name string) string {
	vals := tagValues(ev, name)
	if len(vals) == 0 {
		return ""
	}
	return vals[len(vals)-1]
}

// render maps events to tweets, dropping what never enters Warpnet and, when
// trustedOnly, posts by authors other than author who are not trusted. A repost becomes the reposted note stamped with
// RetweetedBy; its original is taken from the repost when signed, else fetched.
func (b *nostrBridge) render(ctx context.Context, evs []nostrEvent, trustedOnly bool, author string) []tweet {
	type slot struct {
		t        tweet
		repostOf string
	}
	slots := make([]slot, 0, len(evs))
	var missing []string
	for _, ev := range evs {
		if droppedNostrEvent(ev, b.selfHost) || trustedOnly && ev.PubKey != author && !b.isTrusted(ev.PubKey) {
			continue
		}
		b.authors.Add(ev.ID, ev.PubKey)
		target, embedded := repostTarget(ev)
		if target == "" {
			if t, ok := nostrNoteToTweet(ev); ok {
				slots = append(slots, slot{t: t})
			}
			continue
		}
		by := nostrHandle(ev.PubKey)
		if orig, ok := parseSignedEvent(embedded); ok && orig.ID == target && !droppedNostrEvent(orig, b.selfHost) {
			if t, ok := nostrNoteToTweet(orig); ok {
				b.authors.Add(orig.ID, orig.PubKey)
				t.RetweetedBy = &by
				slots = append(slots, slot{t: t})
			}
			continue
		}
		missing = append(missing, target)
		slots = append(slots, slot{t: tweet{RetweetedBy: &by}, repostOf: target})
	}
	fetched := map[string]tweet{}
	if missing = dedupeStrings(missing); len(missing) > 0 {
		for _, ev := range b.relays.fetch(ctx, b.defaults, nostrFilter{IDs: missing, Limit: len(missing)}) {
			if droppedNostrEvent(ev, b.selfHost) {
				continue
			}
			if t, ok := nostrNoteToTweet(ev); ok {
				b.authors.Add(ev.ID, ev.PubKey)
				fetched[ev.ID] = t
			}
		}
	}
	out := make([]tweet, 0, len(slots))
	for _, s := range slots {
		if s.repostOf == "" {
			out = append(out, s.t)
			continue
		}
		if t, ok := fetched[s.repostOf]; ok {
			t.RetweetedBy = s.t.RetweetedBy
			out = append(out, t)
		}
	}
	b.names(ctx, out)
	return out
}

// serves reports whether id is a Nostr account or note this bridge answers
// for; a nil bridge (Nostr off) serves nothing.
func (b *nostrBridge) serves(id string) bool {
	return b != nil && (isNostrHandle(id) || isNostrEventID(strings.TrimPrefix(id, domain.RetweetPrefix)))
}

// isReplyTo reports whether ev replies to a Nostr note. Interactions with Nostr
// content stay inside Warpnet until publishing to Nostr is enabled; they are
// acknowledged so the node keeps them.
func (b *nostrBridge) isReplyTo(ev tweet) bool {
	return ev.ParentId != nil && b.serves(*ev.ParentId) || b.serves(ev.RootId)
}

// newNostrBridge serves Nostr reads as nodeID; nil when the bridge is off.
func (g *gateway) newNostrBridge(nodeID string) *nostrBridge {
	if g.nostrRelays == nil {
		return nil
	}
	nb := newNostrBridge(g.nostrRelays, nostrRelaysFromEnv(), nodeID, g.host)
	nb.trusted = g.nostrLive.trusted
	return nb
}

// pushTimeline delivers a followed Nostr author's note to a follower's node.
func (g *gateway) pushTimeline(follower string, t tweet) error {
	if g.req == nil {
		return nil
	}
	bt, err := g.requestForUser(follower, routePostTimeline, t)
	if err == nil {
		err = nodeResponseError(bt)
	}
	return err
}
