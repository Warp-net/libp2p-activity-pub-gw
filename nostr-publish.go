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

// nostrPublisher writes a Warpnet user's actions on Nostr notes to Nostr,
// signed with the user's derived key: replies, quotes, reactions, reposts,
// their undos, deleted replies and the kind 3 follow list. Nothing is
// published for a user who has not consented at sign-up, whose account is
// locked, or who lives outside the main network; sponsored content never
// leaves Warpnet.

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/oklog/ulid/v2"
)

const (
	nostrConsentKey      = "nostr"
	nostrConsentAllow    = "allow"
	nostrActorTTL        = time.Minute
	nostrPublishTimeout  = time.Minute
	nostrPublishInFlight = 16
	nostrFollowPages     = 10
)

// homeUserReader reads a Warpnet user from the node that owns them, the only
// answer the gateway can trust for consent and the lock flag.
type homeUserReader interface {
	homeUser(userID string) (user, bool)
	followings(userID string) ([]string, bool)
}

type nostrPublisher struct {
	keys    *nostrKeys
	bridge  *nostrBridge
	home    homeUserReader
	network string
	baseURL string

	// allowed remembers consent once seen: it is given at sign-up and cannot
	// be taken back, so a later read never revokes it.
	allowed *expirable.LRU[string, struct{}]
	actors  *expirable.LRU[string, user]
	sem     chan struct{}
}

func newNostrPublisher(keys *nostrKeys, bridge *nostrBridge, home homeUserReader, network, baseURL string) *nostrPublisher {
	return &nostrPublisher{
		keys: keys, bridge: bridge, home: home, network: network, baseURL: baseURL,
		allowed: expirable.NewLRU[string, struct{}](nostrOwnedKeys, nil, 0),
		actors:  expirable.NewLRU[string, user](nostrCacheSize, nil, nostrActorTTL),
		sem:     make(chan struct{}, nostrPublishInFlight),
	}
}

// consents reports whether userID's actions may be published to Nostr.
func (p *nostrPublisher) consents(userID string) bool {
	if p.network != defaultWarpnetNetwork { // testnet stays off the public relays
		return false
	}
	u, ok := p.actors.Get(userID)
	if !ok {
		if u, ok = p.home.homeUser(userID); !ok {
			return false
		}
		p.actors.Add(userID, u)
	}
	if u.Metadata[nostrConsentKey] == nostrConsentAllow {
		p.allowed.Add(userID, struct{}{})
	}
	return p.allowed.Contains(userID) && !u.Locked
}

// signer is the key of a consenting user.
func (p *nostrPublisher) signer(userID string) ([32]byte, string, bool) {
	var sk [32]byte
	if !p.consents(userID) {
		return sk, "", false
	}
	sk, ok := p.keys.secretKey(p.network, userID)
	if !ok {
		return sk, "", false
	}
	return sk, publicKeyHex(sk), true
}

func (p *nostrPublisher) begin() (context.Context, context.CancelFunc) {
	p.sem <- struct{}{}
	return context.WithTimeout(context.Background(), nostrPublishTimeout)
}

func (p *nostrPublisher) end(cancel context.CancelFunc) {
	cancel()
	<-p.sem
}

// target reads a Nostr note to act on.
func (p *nostrPublisher) target(ctx context.Context, id string) (nostrEvent, bool) {
	id = strings.TrimPrefix(id, retweetPrefix)
	for _, ev := range p.bridge.relays.fetch(ctx, p.bridge.relaysFor(ctx, id), nostrFilter{IDs: []string{id}, Limit: 1}) {
		if ev.ID == id {
			p.bridge.authors.Add(ev.ID, ev.PubKey)
			return ev, true
		}
	}
	return nostrEvent{}, false
}

func (p *nostrPublisher) send(ctx context.Context, sk [32]byte, ev nostrEvent, relays []string) {
	ev.CreatedAt = time.Now().Unix()
	signed, ok := signEvent(sk, ev)
	if !ok {
		netLog(p.network).Warnf("nostr: cannot sign a kind %d event", ev.Kind)
		return
	}
	n := p.bridge.relays.publish(ctx, append(append([]string{}, p.bridge.defaults...), relays...), signed)
	netLog(p.network).Infof("nostr: published kind %d %s to %d relays", signed.Kind, signed.ID, n)
}

// statusURL is the gateway url of a Warpnet tweet, the NIP-48 proxy id that
// marks an event as coming from Warpnet.
func (p *nostrPublisher) statusURL(userID, tweetID string) string {
	return p.baseURL + pathUsers + userID + pathStatuses + tweetID
}

func (p *nostrPublisher) proxyTag(userID, tweetID string) []string {
	return []string{"proxy", p.statusURL(userID, tweetID), "web"}
}

// React publishes a kind 7 reaction, or deletes the user's reactions on undo.
func (p *nostrPublisher) React(actor, tweetID, emoji string, undo bool) {
	if p == nil {
		return
	}
	ctx, cancel := p.begin()
	defer p.end(cancel)
	sk, pk, ok := p.signer(actor)
	if !ok {
		return
	}
	note, ok := p.target(ctx, tweetID)
	if !ok {
		return
	}
	relays := p.bridge.authorRelays(ctx, note.PubKey)
	if undo {
		p.deleteOwn(ctx, sk, pk, note.ID, relays, kindReaction)
		return
	}
	content := emoji
	if content == defaultReaction {
		content = "+"
	}
	p.send(ctx, sk, nostrEvent{Kind: kindReaction, Content: content, Tags: [][]string{
		{"e", note.ID}, {"p", note.PubKey}, {"k", "1"},
	}}, relays)
}

// Repost publishes a kind 6 repost embedding the note, or deletes the user's
// reposts of it on undo.
func (p *nostrPublisher) Repost(actor, tweetID string, undo bool) {
	if p == nil {
		return
	}
	ctx, cancel := p.begin()
	defer p.end(cancel)
	sk, pk, ok := p.signer(actor)
	if !ok {
		return
	}
	note, ok := p.target(ctx, tweetID)
	if !ok {
		return
	}
	relays := p.bridge.authorRelays(ctx, note.PubKey)
	if undo {
		p.deleteOwn(ctx, sk, pk, note.ID, relays, kindRepost, kindGenericRepost)
		return
	}
	p.send(ctx, sk, nostrEvent{Kind: kindRepost, Content: eventJSON(note), Tags: [][]string{
		{"e", note.ID, ""}, {"p", note.PubKey},
	}}, relays)
}

// Quote publishes a Warpnet quote of a Nostr note as a kind 1 with a q tag.
func (p *nostrPublisher) Quote(ev tweet) {
	if p == nil || ev.QuotedTweetId == nil || ev.Price != nil {
		return
	}
	ctx, cancel := p.begin()
	defer p.end(cancel)
	sk, _, ok := p.signer(ev.UserId)
	if !ok {
		return
	}
	note, ok := p.target(ctx, *ev.QuotedTweetId)
	if !ok {
		return
	}
	content := strings.TrimSpace(ev.Text + "\n\nnostr:" + encodeNevent(note.ID, note.PubKey))
	p.send(ctx, sk, nostrEvent{Kind: kindNote, Content: content, Tags: [][]string{
		{"q", note.ID, "", note.PubKey}, {"p", note.PubKey}, p.proxyTag(ev.UserId, ev.Id),
	}}, p.bridge.authorRelays(ctx, note.PubKey))
}

// Reply publishes a Warpnet reply to a Nostr note as a NIP-10 kind 1.
func (p *nostrPublisher) Reply(ev tweet) {
	if p == nil || ev.ParentId == nil || ev.Price != nil || strings.TrimSpace(ev.Text) == "" {
		return
	}
	ctx, cancel := p.begin()
	defer p.end(cancel)
	sk, _, ok := p.signer(ev.UserId)
	if !ok {
		return
	}
	parent, ok := p.target(ctx, *ev.ParentId)
	if !ok {
		return
	}
	root := strings.TrimPrefix(ev.RootId, retweetPrefix)
	if !isNostrEventID(root) {
		root = parent.ID
	}
	tags := [][]string{{"e", root, "", "root"}}
	if root != parent.ID {
		tags = append(tags, []string{"e", parent.ID, "", "reply", parent.PubKey})
	}
	tags = append(tags, []string{"p", parent.PubKey}, p.proxyTag(ev.UserId, ev.Id))
	p.send(ctx, sk, nostrEvent{Kind: kindNote, Content: ev.Text, Tags: tags}, p.bridge.authorRelays(ctx, parent.PubKey))
}

// DeleteReply asks relays to drop the user's reply that carried this tweet.
func (p *nostrPublisher) DeleteReply(ev deleteTweetEvent) {
	if p == nil {
		return
	}
	parent := ev.ParentId
	if !isNostrEventID(parent) {
		parent = ev.RootId
	}
	ctx, cancel := p.begin()
	defer p.end(cancel)
	sk, pk, ok := p.signer(ev.UserId)
	if !ok {
		return
	}
	proxy := p.statusURL(ev.UserId, ev.TweetId)
	relays := p.bridge.relaysFor(ctx, parent)
	var tags [][]string
	for _, own := range p.bridge.relays.fetch(ctx, relays, nostrFilter{
		Kinds: []int{kindNote}, Authors: []string{pk}, Tags: map[string][]string{"e": {parent}}, Limit: nostrBoundedFetch,
	}) {
		if id, _ := proxiedFrom(own); id == proxy {
			tags = append(tags, []string{"e", own.ID})
		}
	}
	if len(tags) > 0 {
		p.send(ctx, sk, nostrEvent{Kind: kindDelete, Tags: append(tags, []string{"k", "1"})}, relays)
	}
}

// deleteOwn publishes a kind 5 for the user's events of kinds on note id.
func (p *nostrPublisher) deleteOwn(ctx context.Context, sk [32]byte, pk, id string, relays []string, kinds ...int) {
	var tags [][]string
	for _, own := range p.bridge.relays.fetch(ctx, append(append([]string{}, p.bridge.defaults...), relays...), nostrFilter{
		Kinds: kinds, Authors: []string{pk}, Tags: map[string][]string{"e": {id}}, Limit: nostrBoundedFetch,
	}) {
		tags = append(tags, []string{"e", own.ID})
	}
	if len(tags) == 0 {
		return
	}
	for _, k := range kinds {
		tags = append(tags, []string{"k", itoa(k)})
	}
	p.send(ctx, sk, nostrEvent{Kind: kindDelete, Tags: tags}, relays)
}

// Follow republishes the user's whole kind 3 list: the Nostr accounts they
// follow and the Warpnet users among their followings who consented too.
// handle is the account just followed or unfollowed, which the node may not
// have stored yet.
func (p *nostrPublisher) Follow(actor, handle string, undo bool) {
	if p == nil {
		return
	}
	changed, ok := nostrPubKey(handle)
	if !ok {
		return
	}
	ctx, cancel := p.begin()
	defer p.end(cancel)
	sk, _, ok := p.signer(actor)
	if !ok {
		return
	}
	ids, ok := p.home.followings(actor)
	if !ok {
		return // a partial list would unfollow everyone it misses
	}
	seen := map[string]bool{}
	var tags [][]string
	for _, id := range ids {
		pk, ok := nostrPubKey(id)
		if _, err := ulid.ParseStrict(id); !ok && err == nil && p.consents(id) {
			pk, ok = p.keys.pubKey(p.network, id)
		}
		if !ok || seen[pk] || pk == changed {
			continue
		}
		seen[pk] = true
		tags = append(tags, []string{"p", pk})
	}
	if !undo {
		tags = append(tags, []string{"p", changed})
	}
	p.send(ctx, sk, nostrEvent{Kind: kindFollowList, Tags: tags}, p.bridge.authorRelays(ctx, changed))
}

// homeUser asks the node that owns userID for the user, ignoring every other
// node's copy.
func (c *nodeClient) homeUser(userID string) (user, bool) {
	bt, owner, ok := c.askOwner(userID, routeGetUser, getUserEvent{UserId: userID})
	if !ok {
		return user{}, false
	}
	var u user
	if json.Unmarshal(bt, &u) != nil || !strings.EqualFold(u.Id, userID) || u.NodeId != owner {
		return user{}, false
	}
	return u, true
}

// askOwner sends a route to the node that owns userID only, never falling
// back to another node's copy.
func (c *nodeClient) askOwner(userID, route string, payload any) ([]byte, string, bool) {
	owner, ok := c.ownerNode(userID)
	if !ok {
		return nil, "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), perPeerTimeout)
	defer cancel()
	bt, err := c.stream(ctx, owner, route, payload)
	if err != nil || nodeResponseError(bt) != nil || isRateLimited(bt) {
		return nil, "", false
	}
	return bt, owner.String(), true
}

// followings reads all of userID's followings from their node; ok is false
// unless the whole list was read.
func (c *nodeClient) followings(userID string) ([]string, bool) {
	var out []string
	var cursor *string
	for range nostrFollowPages {
		bt, _, ok := c.askOwner(userID, routeGetFollowings, getFollowersEvent{UserId: userID, Cursor: cursor})
		if !ok {
			return nil, false
		}
		var resp followingsResponse
		if json.Unmarshal(bt, &resp) != nil {
			return nil, false
		}
		out = append(out, resp.Followings...)
		if resp.Cursor == "" || resp.Cursor == "end" || len(resp.Followings) == 0 || (cursor != nil && *cursor == resp.Cursor) {
			return out, true
		}
		next := resp.Cursor
		cursor = &next
	}
	return nil, false
}
