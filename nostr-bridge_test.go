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

import (
	"strconv"
	"testing"
	"time"
)

func TestNostrHandles(t *testing.T) {
	k := newNostrKey()
	h := k.handle()
	if pk, ok := nostrPubKey(h); !ok || pk != k.pk {
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
		if got := isNostrHandle(id); got != want {
			t.Errorf("isNostrHandle(%q) = %v, want %v", id, got, want)
		}
	}
	if !isNostrEventID(k.pk) || isNostrEventID("https://mastodon.social/users/a/statuses/1") {
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
		r, p := threadRefs(nostrEvent{Kind: kindNote, Tags: tc.tags})
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
		"note":      {nostrEvent{Kind: kindNote}, false},
		"legacy dm": {nostrEvent{Kind: kindLegacyDM}, true},
		"gift wrap": {nostrEvent{Kind: kindGiftWrap}, true},
		"mostr":     {nostrEvent{Kind: kindNote, Tags: [][]string{{"proxy", "https://mastodon.social/users/a/statuses/1", "activitypub"}}}, true},
		"own":       {nostrEvent{Kind: kindNote, Tags: [][]string{{"proxy", "https://gw.example/users/01ABC", "web"}}}, true},
		"other web": {nostrEvent{Kind: kindNote, Tags: [][]string{{"proxy", "https://elsewhere.example/x", "web"}}}, false},
	} {
		if got := droppedNostrEvent(tc.ev, "gw.example"); got != tc.want {
			t.Errorf("%s: got %v, want %v", name, got, tc.want)
		}
	}
}

func TestNostrGetUser(t *testing.T) {
	relay := newFakeRelay(t)
	relay.counting = true
	alice, bob := newNostrKey(), newNostrKey()
	now := time.Now().Unix()
	relay.add(
		alice.sign(t, kindProfile, `{"name":"alice","display_name":"Alice","about":"hi","picture":"https://img.example/a.png","banner":"http://insecure.example/b.png","website":"https://alice.example"}`, now),
		alice.sign(t, kindFollowList, "", now, []string{"p", bob.pk}),
		bob.sign(t, kindFollowList, "", now, []string{"p", alice.pk}),
		alice.sign(t, kindNote, "one", now-2),
		alice.sign(t, kindNote, "two", now-1),
	)
	b := testNostrBridge(t, relay)

	u, err := b.GetUser(testCtx(t), alice.handle())
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if u.Id != alice.handle() || u.Username != "Alice" || u.Bio != "hi" || u.Network != nostrNetwork || u.NodeId != "12D3KooWGateway" {
		t.Fatalf("user = %+v", u)
	}
	if u.AvatarKey != "https://img.example/a.png" || u.BackgroundImageKey != "" || u.Website == nil || *u.Website != "https://alice.example" {
		t.Fatalf("images/site = %q %q %v", u.AvatarKey, u.BackgroundImageKey, u.Website)
	}
	if u.FollowingsCount != 1 || u.FollowersCount != 1 || u.TweetsCount != 2 {
		t.Fatalf("counts = %d/%d/%d", u.FollowingsCount, u.FollowersCount, u.TweetsCount)
	}

	if !relay.saw(`"COUNT"`) {
		t.Fatal("counts must be asked with NIP-45 COUNT when the relay supports it")
	}
	relay.counting = false
	b = testNostrBridge(t, relay)
	if u, _ = b.GetUser(testCtx(t), alice.handle()); u.TweetsCount != 2 || u.FollowersCount != 1 {
		t.Fatalf("bounded-fetch counts = %d/%d", u.FollowersCount, u.TweetsCount)
	}

	if _, err := b.GetUser(testCtx(t), newNostrKey().handle()); err == nil {
		t.Fatal("an npub without a profile must not resolve")
	}
	mostr := newNostrKey()
	relay.add(mostr.sign(t, kindProfile, `{"name":"m"}`, now, []string{"proxy", "https://mastodon.social/users/m", "activitypub"}))
	if _, err := b.GetUser(testCtx(t), mostr.handle()); err == nil {
		t.Fatal("a Mostr profile must not resolve")
	}
}

func TestNostrGetTweets(t *testing.T) {
	relay := newFakeRelay(t)
	alice, bob := newNostrKey(), newNostrKey()
	now := time.Now().Unix()
	orig := bob.sign(t, kindNote, "original", now-100)
	unembedded := bob.sign(t, kindNote, "fetched original", now-90)
	forged := bob.sign(t, kindNote, "forged", now-80)
	forged.Content = "words bob never said"
	note := alice.sign(t, kindNote, "hello https://img.example/p.jpg", now-5)
	reply := alice.sign(t, kindNote, "a reply", now-4, []string{"e", orig.ID.Hex(), "", "root"})
	poll := alice.sign(t, kindPoll, "best?", now-3, []string{"option", "a", "Yes"}, []string{"option", "b", "No"}, []string{"endsAt", strconv.FormatInt(now+3600, 10)})
	repost := alice.sign(t, kindRepost, orig.String(), now-2, []string{"e", orig.ID.Hex()}, []string{"p", bob.pk})
	repostBare := alice.sign(t, kindRepost, "", now-1, []string{"e", unembedded.ID.Hex()})
	repostForged := alice.sign(t, kindRepost, forged.String(), now, []string{"e", forged.ID.Hex()})
	relay.add(orig, unembedded, note, reply, poll, repost, repostBare, repostForged,
		bob.sign(t, kindProfile, `{"name":"Bob"}`, now))
	b := testNostrBridge(t, relay)

	resp, err := b.GetTweets(testCtx(t), alice.handle(), nil, nil)
	if err != nil {
		t.Fatalf("GetTweets: %v", err)
	}
	byText := map[string]tweet{}
	for _, tw := range resp.Tweets {
		byText[tw.Text] = tw
	}
	if _, ok := byText["a reply"]; ok {
		t.Error("a reply must stay out of the profile feed")
	}
	if _, ok := byText["words bob never said"]; ok {
		t.Error("a repost embedding a tampered event must not be trusted")
	}
	if tw := byText["hello"]; tw.Id != note.ID.Hex() || len(tw.ImageKeys) != 1 || tw.Network != nostrNetwork {
		t.Errorf("note = %+v", tw)
	}
	if tw := byText["best?"]; tw.Poll == nil || len(tw.Poll.Options) != 2 || tw.Poll.ExpiresAt.Unix() != now+3600 {
		t.Errorf("poll = %+v", tw.Poll)
	}
	for _, text := range []string{"original", "fetched original"} {
		tw := byText[text]
		if tw.RetweetedBy == nil || *tw.RetweetedBy != alice.handle() || tw.UserId != bob.handle() || tw.Username != "Bob" {
			t.Errorf("repost %q = %+v", text, tw)
		}
	}
	if resp.Cursor != strconv.FormatInt(now-6, 10) {
		t.Errorf("cursor = %q, want just before the oldest note", resp.Cursor)
	}

	limit := uint64(2)
	cursor := strconv.FormatInt(now-3, 10)
	page, _ := b.GetTweets(testCtx(t), alice.handle(), &limit, &cursor)
	if len(page.Tweets) != 1 || page.Tweets[0].Text != "best?" {
		t.Fatalf("until page = %+v", page.Tweets)
	}
}

func TestNostrRepliesAndStatsTrust(t *testing.T) {
	relay := newFakeRelay(t)
	author, friend, stranger := newNostrKey(), newNostrKey(), newNostrKey()
	now := time.Now().Unix()
	root := author.sign(t, kindNote, "root", now-10)
	id := root.ID.Hex()
	relay.add(root,
		author.sign(t, kindNote, "author continues", now-9, []string{"e", id, "", "root"}),
		friend.sign(t, kindNote, "friend replies", now-8, []string{"e", id, "", "root"}, []string{"p", author.pk}),
		stranger.sign(t, kindNote, "spam", now-7, []string{"e", id, "", "root"}),
		friend.sign(t, kindNote, "deeper", now-6, []string{"e", id, "", "root"}, []string{"e", hexID('d'), "", "reply"}),
		friend.sign(t, kindNote, "via mostr", now-5, []string{"e", id, "", "root"}, []string{"proxy", "https://m.example/1", "activitypub"}),
		friend.sign(t, kindReaction, "+", now, []string{"e", id}),
		friend.sign(t, kindReaction, "🔥", now-1, []string{"e", id}),
		author.sign(t, kindReaction, "🔥", now, []string{"e", id}),
		stranger.sign(t, kindReaction, "+", now, []string{"e", id}),
		friend.sign(t, kindRepost, "", now, []string{"e", id}),
		stranger.sign(t, kindRepost, "", now, []string{"e", id}),
	)
	b := testNostrBridge(t, relay)
	b.trusted = func(pk string) bool { return pk == friend.pk || pk == author.pk }

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
	if stats.ReactionsCount != 2 || stats.Reactions[defaultReaction] != 1 || stats.Reactions["🔥"] != 1 {
		t.Errorf("reactions = %d %v, want one per trusted author", stats.ReactionsCount, stats.Reactions)
	}
	if stats.RetweetsCount != 1 || stats.RepliesCount != 2 {
		t.Errorf("reposts/replies = %d/%d", stats.RetweetsCount, stats.RepliesCount)
	}
}

func TestNostrFollowLists(t *testing.T) {
	relay := newFakeRelay(t)
	alice, bob, carol := newNostrKey(), newNostrKey(), newNostrKey()
	now := time.Now().Unix()
	relay.add(
		alice.sign(t, kindFollowList, "", now, []string{"p", bob.pk}, []string{"p", carol.pk}, []string{"p", "not-a-key"}),
		bob.sign(t, kindFollowList, "", now, []string{"p", alice.pk}),
	)
	b := testNostrBridge(t, relay)
	fl, _ := b.GetFollowings(testCtx(t), alice.handle(), nil)
	if len(fl.Followings) != 2 {
		t.Fatalf("followings = %v", fl.Followings)
	}
	fs, _ := b.GetFollowers(testCtx(t), alice.handle(), nil)
	if len(fs.Followers) != 1 || fs.Followers[0] != bob.handle() {
		t.Fatalf("followers = %v", fs.Followers)
	}
}

func TestRelayURLGuard(t *testing.T) {
	p := newRelayPool(nil)
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
