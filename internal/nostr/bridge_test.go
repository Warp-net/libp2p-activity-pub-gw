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
	"strconv"
	"testing"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/nostr/nostrtest"
	"github.com/Warp-net/bridge-gateway/internal/warp"
)

func TestNostrHandles(t *testing.T) {
	k := nostrtest.NewKey()
	h := Handle(k.PK)
	if pk, ok := nostrPubKey(h); !ok || pk != k.PK {
		t.Fatalf("nostrPubKey(%q) = %q, %v", h, pk, ok)
	}
	npub, _, _ := cutAt(h)
	for id, want := range map[string]bool{
		h:                     true,
		"@" + h:               true,
		npub + "@NOSTR.COM":   true,
		npub + "@damus.io":    false,
		"alice@damus.io":      false,
		"alice@nostr.com":     false,
		"npub1xyz@nostr.com":  false,
		"01KTRA1QJ8M2W7Y4ZB6": false,
	} {
		if got := IsHandle(id); got != want {
			t.Errorf("isNostrHandle(%q) = %v, want %v", id, got, want)
		}
	}
	if !isNostrEventID(k.PK) || isNostrEventID("https://mastodon.social/users/a/statuses/1") {
		t.Error("isNostrEventID misclassifies")
	}
}

func cutAt(h string) (string, string, bool) {
	for i := range h {
		if h[i] == '@' {
			return h[:i], h[i+1:], true
		}
	}
	return h, "", false
}

func TestNostrThreadRefs(t *testing.T) {
	root, parent, other := hexID('a'), hexID('b'), hexID('c')
	for name, tc := range map[string]struct {
		tags         [][]string
		root, parent string
	}{
		"marked":         {[][]string{{"e", root, "", "root"}, {"e", other, "", "mention"}, {"e", parent, "", "reply"}}, root, parent},
		"root only":      {[][]string{{"e", root, "", "root"}}, root, root},
		"positional":     {[][]string{{"e", root}, {"e", other}, {"e", parent}}, root, parent},
		"positional one": {[][]string{{"e", root}}, root, root},
		"none":           {nil, "", ""},
	} {
		r, p := threadRefs(nostrEvent{Kind: KindNote, Tags: tc.tags})
		if r != tc.root || p != tc.parent {
			t.Errorf("%s: got %q/%q, want %q/%q", name, r, p, tc.root, tc.parent)
		}
	}
}

func hexID(c byte) string {
	b := make([]byte, 64)
	for i := range b {
		b[i] = c
	}
	return string(b)
}

func TestDroppedNostrEvent(t *testing.T) {
	for name, tc := range map[string]struct {
		ev   nostrEvent
		want bool
	}{
		"note":      {nostrEvent{Kind: KindNote}, false},
		"legacy dm": {nostrEvent{Kind: kindLegacyDM}, true},
		"gift wrap": {nostrEvent{Kind: kindGiftWrap}, true},
		"mostr":     {nostrEvent{Kind: KindNote, Tags: [][]string{{"proxy", "https://mastodon.social/users/a/statuses/1", "activitypub"}}}, true},
		"own":       {nostrEvent{Kind: KindNote, Tags: [][]string{{"proxy", "https://gw.example/users/01ABC", "web"}}}, true},
		"other web": {nostrEvent{Kind: KindNote, Tags: [][]string{{"proxy", "https://elsewhere.example/x", "web"}}}, false},
	} {
		if got := droppedNostrEvent(tc.ev, "gw.example"); got != tc.want {
			t.Errorf("%s: got %v, want %v", name, got, tc.want)
		}
	}
}

func TestNostrGetUser(t *testing.T) {
	relay := nostrtest.NewRelay(t)
	relay.Counting = true
	alice, bob := nostrtest.NewKey(), nostrtest.NewKey()
	now := time.Now().Unix()
	relay.Add(
		alice.Sign(t, KindProfile, `{"name":"alice","display_name":"Alice","about":"hi","picture":"https://img.example/a.png","banner":"http://insecure.example/b.png","website":"https://alice.example"}`, now),
		alice.Sign(t, kindFollowList, "", now, []string{"p", bob.PK}),
		bob.Sign(t, kindFollowList, "", now, []string{"p", alice.PK}),
		alice.Sign(t, KindNote, "one", now-2),
		alice.Sign(t, KindNote, "two", now-1),
	)
	b := testNostrBridge(t, relay)

	u, err := b.GetUser(testCtx(t), Handle(alice.PK))
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if u.Id != Handle(alice.PK) || u.Username != "Alice" || u.Bio != "hi" || u.Network != warp.NostrNetwork || u.NodeId != "12D3KooWGateway" {
		t.Fatalf("user = %+v", u)
	}
	if u.AvatarKey != "https://img.example/a.png" || u.BackgroundImageKey != "" || u.Website == nil || *u.Website != "https://alice.example" {
		t.Fatalf("images/site = %q %q %v", u.AvatarKey, u.BackgroundImageKey, u.Website)
	}
	if u.FollowingsCount != 1 || u.FollowersCount != 1 || u.TweetsCount != 2 {
		t.Fatalf("counts = %d/%d/%d", u.FollowingsCount, u.FollowersCount, u.TweetsCount)
	}

	if !relay.Saw(`"COUNT"`) {
		t.Fatal("counts must be asked with NIP-45 COUNT when the relay supports it")
	}
	relay.Counting = false
	b = testNostrBridge(t, relay)
	if u, _ = b.GetUser(testCtx(t), Handle(alice.PK)); u.TweetsCount != 2 || u.FollowersCount != 1 {
		t.Fatalf("bounded-fetch counts = %d/%d", u.FollowersCount, u.TweetsCount)
	}

	if _, err := b.GetUser(testCtx(t), Handle(nostrtest.NewKey().PK)); err == nil {
		t.Fatal("an npub without a profile must not resolve")
	}
	mostr := nostrtest.NewKey()
	relay.Add(mostr.Sign(t, KindProfile, `{"name":"m"}`, now, []string{"proxy", "https://mastodon.social/users/m", "activitypub"}))
	if _, err := b.GetUser(testCtx(t), Handle(mostr.PK)); err == nil {
		t.Fatal("a Mostr profile must not resolve")
	}
}

func TestNostrGetTweets(t *testing.T) {
	relay := nostrtest.NewRelay(t)
	alice, bob := nostrtest.NewKey(), nostrtest.NewKey()
	now := time.Now().Unix()
	orig := bob.Sign(t, KindNote, "original", now-100)
	unembedded := bob.Sign(t, KindNote, "fetched original", now-90)
	forged := bob.Sign(t, KindNote, "forged", now-80)
	forged.Content = "words bob never said"
	note := alice.Sign(t, KindNote, "hello https://img.example/p.jpg", now-5)
	reply := alice.Sign(t, KindNote, "a reply", now-4, []string{"e", orig.ID.Hex(), "", "root"})
	poll := alice.Sign(t, kindPoll, "best?", now-3, []string{"option", "a", "Yes"}, []string{"option", "b", "No"}, []string{"endsAt", strconv.FormatInt(now+3600, 10)})
	repost := alice.Sign(t, kindRepost, orig.String(), now-2, []string{"e", orig.ID.Hex()}, []string{"p", bob.PK})
	repostBare := alice.Sign(t, kindRepost, "", now-1, []string{"e", unembedded.ID.Hex()})
	repostForged := alice.Sign(t, kindRepost, forged.String(), now, []string{"e", forged.ID.Hex()})
	relay.Add(orig, unembedded, note, reply, poll, repost, repostBare, repostForged,
		bob.Sign(t, KindProfile, `{"name":"Bob"}`, now))
	b := testNostrBridge(t, relay)

	resp, err := b.GetTweets(testCtx(t), Handle(alice.PK), nil, nil)
	if err != nil {
		t.Fatalf("GetTweets: %v", err)
	}
	byText := map[string]warp.Tweet{}
	for _, tw := range resp.Tweets {
		byText[tw.Text] = tw
	}
	if _, ok := byText["a reply"]; ok {
		t.Error("a reply must stay out of the profile feed")
	}
	if _, ok := byText["words bob never said"]; ok {
		t.Error("a repost embedding a tampered event must not be trusted")
	}
	if tw := byText["hello"]; tw.Id != note.ID.Hex() || len(tw.ImageKeys) != 1 || tw.Network != warp.NostrNetwork {
		t.Errorf("note = %+v", tw)
	}
	if tw := byText["best?"]; tw.Poll == nil || len(tw.Poll.Options) != 2 || tw.Poll.ExpiresAt.Unix() != now+3600 {
		t.Errorf("poll = %+v", tw.Poll)
	}
	for _, text := range []string{"original", "fetched original"} {
		tw := byText[text]
		if tw.RetweetedBy == nil || *tw.RetweetedBy != Handle(alice.PK) || tw.UserId != Handle(bob.PK) || tw.Username != "Bob" {
			t.Errorf("repost %q = %+v", text, tw)
		}
	}
	if resp.Cursor != strconv.FormatInt(now-6, 10) {
		t.Errorf("cursor = %q, want just before the oldest note", resp.Cursor)
	}

	limit := uint64(2)
	cursor := strconv.FormatInt(now-3, 10)
	page, _ := b.GetTweets(testCtx(t), Handle(alice.PK), &limit, &cursor)
	if len(page.Tweets) != 1 || page.Tweets[0].Text != "best?" {
		t.Fatalf("until page = %+v", page.Tweets)
	}
}

func TestNostrRepliesAndStatsTrust(t *testing.T) {
	relay := nostrtest.NewRelay(t)
	author, friend, stranger := nostrtest.NewKey(), nostrtest.NewKey(), nostrtest.NewKey()
	now := time.Now().Unix()
	root := author.Sign(t, KindNote, "root", now-10)
	id := root.ID.Hex()
	relay.Add(root,
		author.Sign(t, KindNote, "author continues", now-9, []string{"e", id, "", "root"}),
		friend.Sign(t, KindNote, "friend replies", now-8, []string{"e", id, "", "root"}, []string{"p", author.PK}),
		stranger.Sign(t, KindNote, "spam", now-7, []string{"e", id, "", "root"}),
		friend.Sign(t, KindNote, "deeper", now-6, []string{"e", id, "", "root"}, []string{"e", hexID('d'), "", "reply"}),
		friend.Sign(t, KindNote, "via mostr", now-5, []string{"e", id, "", "root"}, []string{"proxy", "https://m.example/1", "activitypub"}),
		friend.Sign(t, kindReaction, "+", now, []string{"e", id}),
		friend.Sign(t, kindReaction, "🔥", now-1, []string{"e", id}),
		author.Sign(t, kindReaction, "🔥", now, []string{"e", id}),
		stranger.Sign(t, kindReaction, "+", now, []string{"e", id}),
		friend.Sign(t, kindRepost, "", now, []string{"e", id}),
		stranger.Sign(t, kindRepost, "", now, []string{"e", id}),
	)
	b := testNostrBridge(t, relay)
	b.trusted = trustedKeys{friend.PK: true, author.PK: true}.has

	if _, err := b.GetTweet(testCtx(t), id); err != nil {
		t.Fatalf("GetTweet: %v", err)
	}
	resp, err := b.GetReplies(testCtx(t), id)
	if err != nil {
		t.Fatalf("GetReplies: %v", err)
	}
	var texts []string
	for _, tw := range resp.Tweets {
		texts = append(texts, tw.Text)
		if tw.ParentId == nil || *tw.ParentId != id || tw.RootId != id {
			t.Errorf("reply %q has parent %v root %q", tw.Text, tw.ParentId, tw.RootId)
		}
	}
	if len(texts) != 2 {
		t.Fatalf("replies = %v, want the author's and the friend's direct replies", texts)
	}

	stats, err := b.GetTweetStats(testCtx(t), id)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.ReactionsCount != 2 || stats.Reactions[warp.DefaultReaction] != 1 || stats.Reactions["🔥"] != 1 {
		t.Errorf("reactions = %d %v, want one per trusted author", stats.ReactionsCount, stats.Reactions)
	}
	if stats.RetweetsCount != 1 || stats.RepliesCount != 2 {
		t.Errorf("reposts/replies = %d/%d", stats.RetweetsCount, stats.RepliesCount)
	}
}

func TestNostrFollowLists(t *testing.T) {
	relay := nostrtest.NewRelay(t)
	alice, bob, carol := nostrtest.NewKey(), nostrtest.NewKey(), nostrtest.NewKey()
	now := time.Now().Unix()
	relay.Add(
		alice.Sign(t, kindFollowList, "", now, []string{"p", bob.PK}, []string{"p", carol.PK}, []string{"p", "not-a-key"}),
		bob.Sign(t, kindFollowList, "", now, []string{"p", alice.PK}),
	)
	b := testNostrBridge(t, relay)
	fl, _ := b.GetFollowings(testCtx(t), Handle(alice.PK), nil)
	if len(fl.Followings) != 2 {
		t.Fatalf("followings = %v", fl.Followings)
	}
	fs, _ := b.GetFollowers(testCtx(t), Handle(alice.PK), nil)
	if len(fs.Followers) != 1 || fs.Followers[0] != Handle(bob.PK) {
		t.Fatalf("followers = %v", fs.Followers)
	}
}

func TestRelayURLGuard(t *testing.T) {
	p := NewRelayPool(nil)
	for raw, want := range map[string]bool{
		"wss://relay.damus.io":   true,
		"ws://relay.damus.io":    false,
		"wss://127.0.0.1":        false,
		"wss://10.0.0.1:7777":    false,
		"wss://localhost":        false,
		"https://relay.damus.io": false,
	} {
		if got := p.validRelayURL(raw); got != want {
			t.Errorf("validRelayURL(%q) = %v, want %v", raw, got, want)
		}
	}
}

type trustedKeys map[string]bool

func (k trustedKeys) has(pk string) bool { return k[pk] }

func TestNostrFeedCursorFollowsTheKeptNotes(t *testing.T) {
	recent, archive := nostrtest.NewRelay(t), nostrtest.NewRelay(t)
	alice := nostrtest.NewKey()
	now := time.Now().Unix()
	shared := alice.Sign(t, KindNote, "newest", now-1)
	recent.Add(shared, alice.Sign(t, KindNote, "second", now-2), alice.Sign(t, KindNote, "third", now-3))
	archive.Add(shared, alice.Sign(t, KindNote, "ancient", now-1000), alice.Sign(t, KindNote, "older", now-1001))
	b := testNostrBridge(t, recent, archive)

	limit := uint64(2)
	page, err := b.GetTweets(testCtx(t), Handle(alice.PK), &limit, nil)
	if err != nil {
		t.Fatalf("GetTweets: %v", err)
	}
	if len(page.Tweets) != 2 || page.Tweets[0].Text != "newest" || page.Tweets[1].Text != "second" {
		t.Fatalf("page = %+v", page.Tweets)
	}
	next, _ := b.GetTweets(testCtx(t), Handle(alice.PK), &limit, &page.Cursor)
	if len(next.Tweets) == 0 || next.Tweets[0].Text != "third" {
		t.Fatalf("next page = %+v, want it to continue at the third note", next.Tweets)
	}
}

func TestNostrFetchDoesNotWaitForAStalledRelay(t *testing.T) {
	fast, stalled := nostrtest.NewRelay(t), nostrtest.NewRelay(t)
	fast.Counting = true
	stalled.Stalled = true
	alice := nostrtest.NewKey()
	fast.Add(alice.Sign(t, KindNote, "hi", time.Now().Unix()))
	pool := testRelayPool()
	urls := []string{fast.URL(), stalled.URL()}

	start := time.Now()
	evs := pool.fetch(testCtx(t), urls, nostrFilter{Authors: []string{alice.PK}})
	if len(evs) != 1 {
		t.Fatalf("fetched %d events", len(evs))
	}
	if took := time.Since(start); took > 3*relayGrace {
		t.Fatalf("took %s, want the stalled relay cut off after the grace period", took)
	}
	if pool.down.Contains(stalled.URL()) {
		t.Fatal("a relay cut off by the grace period must not be marked down")
	}
}

func TestNostrWhoToFollowEntersTheFollowGraph(t *testing.T) {
	relay := nostrtest.NewRelay(t)
	entry, bob, carol, silent := nostrtest.NewKey(), nostrtest.NewKey(), nostrtest.NewKey(), nostrtest.NewKey()
	now := time.Now().Unix()
	relay.Add(
		entry.Sign(t, KindProfile, `{"name":"entry"}`, now),
		bob.Sign(t, KindProfile, `{"name":"bob"}`, now),
		carol.Sign(t, KindProfile, `{"name":"carol"}`, now),
		entry.Sign(t, kindFollowList, "", now, []string{"p", bob.PK}, []string{"p", carol.PK}, []string{"p", silent.PK}),
	)
	b := testNostrBridge(t, relay)
	b.entry = entry.PK

	got := map[string]bool{}
	for _, u := range b.WhoToFollow(testCtx(t)) {
		if u.Network != warp.NostrNetwork {
			t.Fatalf("suggested %+v, want a Nostr account", u)
		}
		got[u.Id] = true
	}
	if len(got) != 3 || !got[Handle(entry.PK)] || !got[Handle(bob.PK)] || !got[Handle(carol.PK)] {
		t.Fatalf("suggestions = %v, want the entry and the accounts it follows with a profile", got)
	}
}

func TestNostrProfileMissIsRetried(t *testing.T) {
	relay := nostrtest.NewRelay(t)
	late := nostrtest.NewKey()
	b := testNostrBridge(t, relay)
	b.prefetchProfiles(testCtx(t), []string{late.PK})
	if _, err := b.GetUser(testCtx(t), Handle(late.PK)); err == nil {
		t.Fatal("an npub without a profile must not resolve")
	}
	relay.Add(late.Sign(t, KindProfile, `{"name":"late"}`, time.Now().Unix()))
	if u, err := b.GetUser(testCtx(t), Handle(late.PK)); err != nil || u.Username != "late" {
		t.Fatalf("a profile missed once must be read again, got %+v, %v", u, err)
	}
}

func TestNostrCountSkipsARelayThatDoesNotCount(t *testing.T) {
	fast, stalled := nostrtest.NewRelay(t), nostrtest.NewRelay(t)
	fast.Counting = true
	stalled.Stalled = true
	alice := nostrtest.NewKey()
	fast.Add(alice.Sign(t, KindNote, "hi", time.Now().Unix()))
	pool := testRelayPool()
	urls := []string{fast.URL(), stalled.URL()}
	f := nostrFilter{Kinds: []int{KindNote}, Authors: []string{alice.PK}}

	short, cancel := context.WithTimeout(testCtx(t), relayGrace)
	defer cancel()
	if n, ok := pool.count(short, urls, f); !ok || n != 1 {
		t.Fatalf("first count = %d, %v", n, ok)
	}
	start := time.Now()
	if n, ok := pool.count(testCtx(t), urls, f); !ok || n != 1 {
		t.Fatalf("second count = %d, %v", n, ok)
	}
	if took := time.Since(start); took > relayGrace {
		t.Fatalf("took %s, want the relay that never counted skipped", took)
	}
}
