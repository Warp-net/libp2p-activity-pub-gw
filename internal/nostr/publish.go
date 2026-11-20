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
	"strings"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/warp"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/oklog/ulid/v2"
)

// nostrPublisher writes a Warpnet user's actions on Nostr notes to Nostr,
// signed with the user's derived key: replies, quotes, reactions, reposts,
// their undos, deleted replies and the kind 3 follow list. Nothing is
// published for a user who has not consented at sign-up, whose account is
// locked, or who lives outside the main network; sponsored content never
// leaves Warpnet.

const (
	nostrConsentKey      = "nostr"
	nostrConsentDeny     = "deny"
	nostrActorTTL        = time.Minute
	nostrPublishTimeout  = time.Minute
	nostrPublishInFlight = 16
)

// Links are the gateway's urls of Warpnet users and their content.
type Links interface {
	ProfileURL(userID string) string
	StatusURL(userID, tweetID string) string
	MediaURL(userID, key string) string
	// ProfileUser is the user id a ProfileURL names.
	ProfileUser(rawURL string) (string, bool)
}

// HomeReader reads a Warpnet user from the node that owns them, the only
// answer the gateway can trust for consent and the lock flag.
type HomeReader interface {
	HomeUser(userID string) (warp.User, bool)
	Followings(userID string) ([]string, bool)
}

type Publisher struct {
	keys    *Keys
	bridge  *Bridge
	home    HomeReader
	network string
	links   Links

	actors *expirable.LRU[string, warp.User]
	sem    chan struct{}
}

func newPublisher(keys *Keys, bridge *Bridge, home HomeReader, network string, links Links) *Publisher {
	return &Publisher{
		keys: keys, bridge: bridge, home: home, network: network, links: links,
		actors: expirable.NewLRU[string, warp.User](nostrCacheSize, nil, nostrActorTTL),
		sem:    make(chan struct{}, nostrPublishInFlight),
	}
}

// consents reports whether userID's actions may be published to Nostr: on
// unless the user opted out at sign-up.
func (p *Publisher) consents(userID string) bool {
	if p.network != warp.DefaultNetwork { // testnet stays off the public relays
		return false
	}
	u, ok := p.actors.Get(userID)
	if !ok {
		if u, ok = p.home.HomeUser(userID); !ok {
			return false
		}
		p.actors.Add(userID, u)
	}
	return u.Metadata[nostrConsentKey] != nostrConsentDeny && !u.Locked
}

// signer is the key of a consenting user.
func (p *Publisher) signer(userID string) ([32]byte, string, bool) {
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

func (p *Publisher) begin() (context.Context, context.CancelFunc) {
	p.sem <- struct{}{}
	return context.WithTimeout(context.Background(), nostrPublishTimeout)
}

func (p *Publisher) end(cancel context.CancelFunc) {
	cancel()
	<-p.sem
}

// target reads a Nostr note to act on.
func (p *Publisher) target(ctx context.Context, id string) (nostrEvent, bool) {
	id = strings.TrimPrefix(id, retweetPrefix)
	for _, ev := range p.bridge.relays.fetch(ctx, p.bridge.relaysFor(ctx, id), nostrFilter{IDs: []string{id}, Limit: 1}) {
		if ev.ID == id {
			p.bridge.authors.Add(ev.ID, ev.PubKey)
			return ev, true
		}
	}
	return nostrEvent{}, false
}

func (p *Publisher) send(ctx context.Context, sk [32]byte, ev nostrEvent, relays []string) {
	ev.CreatedAt = time.Now().Unix()
	signed, ok := signEvent(sk, ev)
	if !ok {
		warp.NetLog(p.network).Warnf("nostr: cannot sign a kind %d event", ev.Kind)
		return
	}
	n := p.bridge.relays.publish(ctx, append(append([]string{}, p.bridge.defaults...), relays...), signed)
	warp.NetLog(p.network).Infof("nostr: published kind %d %s to %d relays", signed.Kind, signed.ID, n)
}

// statusURL is the gateway url of a Warpnet tweet, the NIP-48 proxy id that
// marks an event as coming from Warpnet.
func (p *Publisher) statusURL(userID, tweetID string) string {
	return p.links.StatusURL(userID, tweetID)
}

func (p *Publisher) proxyTag(userID, tweetID string) []string {
	return []string{"proxy", p.statusURL(userID, tweetID), "web"}
}

// React publishes a kind 7 reaction, or deletes the user's reactions on undo.
func (p *Publisher) React(actor, tweetID, emoji string, undo bool) {
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
	if content == warp.DefaultReaction {
		content = "+"
	}
	p.send(ctx, sk, nostrEvent{Kind: kindReaction, Content: content, Tags: [][]string{
		{"e", note.ID}, {"p", note.PubKey}, {"k", "1"},
	}}, relays)
}

// Repost publishes a kind 6 repost embedding the note, or deletes the user's
// reposts of it on undo.
func (p *Publisher) Repost(actor, tweetID string, undo bool) {
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
func (p *Publisher) Quote(ev warp.Tweet) {
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
	p.send(ctx, sk, nostrEvent{Kind: KindNote, Content: content, Tags: [][]string{
		{"q", note.ID, "", note.PubKey}, {"p", note.PubKey}, p.proxyTag(ev.UserId, ev.Id),
	}}, p.bridge.authorRelays(ctx, note.PubKey))
}

// Reply publishes a Warpnet reply to a Nostr note as a NIP-10 kind 1.
func (p *Publisher) Reply(ev warp.Tweet) {
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
	p.send(ctx, sk, nostrEvent{Kind: KindNote, Content: ev.Text, Tags: tags}, p.bridge.authorRelays(ctx, parent.PubKey))
}

// DeleteReply asks relays to drop the user's reply that carried this tweet.
func (p *Publisher) DeleteReply(ev warp.DeleteTweetEvent) {
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
	relays := p.bridge.relaysFor(ctx, parent)
	p.deleteProxied(ctx, sk, p.statusURL(ev.UserId, ev.TweetId), relays, nostrFilter{
		Kinds: []int{KindNote}, Authors: []string{pk}, Tags: map[string][]string{"e": {parent}}, Limit: nostrBoundedFetch,
	})
}

// deleteProxied publishes a kind 5 for the user's notes matching f that
// carry the proxy id of one Warpnet tweet; an edited tweet has several.
func (p *Publisher) deleteProxied(ctx context.Context, sk [32]byte, proxy string, relays []string, f nostrFilter) {
	var tags [][]string
	for _, own := range p.bridge.relays.fetch(ctx, append(append([]string{}, p.bridge.defaults...), relays...), f) {
		if id, _ := proxiedFrom(own); id == proxy {
			tags = append(tags, []string{"e", own.ID})
		}
	}
	if len(tags) > 0 {
		p.send(ctx, sk, nostrEvent{Kind: kindDelete, Tags: append(tags, []string{"k", "1"})}, relays)
	}
}

// deleteOwn publishes a kind 5 for the user's events of kinds on note id.
func (p *Publisher) deleteOwn(ctx context.Context, sk [32]byte, pk, id string, relays []string, kinds ...int) {
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
func (p *Publisher) Follow(actor, handle string, undo bool) {
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
	ids, ok := p.home.Followings(actor)
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

// PublishPost publishes a consenting user's own top-level post as a kind 1.
// An edit arrives again under the same id and is published as a new note;
// the earlier version stays.
func (p *Publisher) PublishPost(owner string, t warp.Tweet) {
	if p == nil || !nostrPublishablePost(t, owner) {
		return
	}
	ctx, cancel := p.begin()
	defer p.end(cancel)
	sk, _, ok := p.signer(owner)
	if !ok {
		return
	}
	content := strings.TrimSpace(t.Text)
	if t.Poll != nil {
		for _, o := range t.Poll.Options {
			content += "\n• " + o
		}
	}
	for _, key := range t.ImageKeys {
		content += "\n" + p.mediaURL(owner, key)
	}
	tags := [][]string{p.proxyTag(owner, t.Id)}
	if t.QuotedTweetId != nil && isNostrEventID(*t.QuotedTweetId) {
		if note, ok := p.target(ctx, *t.QuotedTweetId); ok {
			content += "\n\nnostr:" + encodeNevent(note.ID, note.PubKey)
			tags = append(tags, []string{"q", note.ID, "", note.PubKey}, []string{"p", note.PubKey})
		}
	}
	if content = strings.TrimSpace(content); content == "" {
		return
	}
	p.send(ctx, sk, nostrEvent{Kind: KindNote, Content: content, Tags: tags}, nil)
}

// DeletePost asks relays to drop every published version of a post.
func (p *Publisher) DeletePost(owner, tweetID string) {
	if p == nil {
		return
	}
	ctx, cancel := p.begin()
	defer p.end(cancel)
	sk, pk, ok := p.signer(owner)
	if !ok {
		return
	}
	p.deleteProxied(ctx, sk, p.statusURL(owner, tweetID), nil, nostrFilter{
		Kinds: []int{KindNote}, Authors: []string{pk}, Limit: nostrBoundedFetch,
	})
}

// PublishProfile publishes the user's kind 0 and kind 10002 relay list, and
// returns the kind 0 content so the caller republishes only on a change.
func (p *Publisher) PublishProfile(owner, last string) string {
	if p == nil {
		return last
	}
	ctx, cancel := p.begin()
	defer p.end(cancel)
	sk, _, ok := p.signer(owner)
	if !ok {
		return last
	}
	u, ok := p.home.HomeUser(owner)
	if !ok {
		return last
	}
	content := p.profileContent(owner, u)
	if content == last {
		return last
	}
	p.send(ctx, sk, nostrEvent{Kind: KindProfile, Content: content, Tags: [][]string{
		{"proxy", p.links.ProfileURL(owner), "web"},
	}}, nil)
	relays := make([][]string, 0, len(p.bridge.defaults))
	for _, r := range p.bridge.defaults {
		relays = append(relays, []string{"r", r})
	}
	p.send(ctx, sk, nostrEvent{Kind: kindRelayList, Tags: relays}, nil)
	return content
}

func (p *Publisher) profileContent(owner string, u warp.User) string {
	prof := map[string]string{
		"name":         firstNonEmpty(u.Username, owner),
		"display_name": firstNonEmpty(u.Username, owner),
		"about":        u.Bio,
		"nip05":        strings.ToLower(owner) + "@" + p.bridge.selfHost,
	}
	if u.AvatarKey != "" {
		prof["picture"] = p.mediaURL(owner, u.AvatarKey)
	}
	if u.BackgroundImageKey != "" {
		prof["banner"] = p.mediaURL(owner, u.BackgroundImageKey)
	}
	if u.Website != nil && *u.Website != "" {
		prof["website"] = *u.Website
	}
	bt, _ := json.Marshal(prof)
	return string(bt)
}

// mediaURL serves a Warpnet image through the gateway's media proxy; the link
// breaks if the gateway host ever changes, which is accepted.
func (p *Publisher) mediaURL(owner, key string) string {
	return p.links.MediaURL(owner, key)
}

// nostrPublishablePost is an original top-level post by owner that is not
// sponsored; replies, retweets and paid content never leave as posts.
func nostrPublishablePost(t warp.Tweet, owner string) bool {
	return warp.PublishableTweet(t, owner) && t.Price == nil && !t.IsSponsored()
}
