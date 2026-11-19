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
	"strings"
	"testing"
	"time"

	"fiatjaf.com/nostr"
	"github.com/Warp-net/warpnet/domain"
)

var pricedT = domain.Price{Amount: "1"}

const (
	testSecret = "0123456789abcdef0123456789abcdef-test"
	aliceULID  = "01KZS1PJ7ZCYGZ1AGARA8B2G9N"
	bobULID    = "01KSGHBHKG0N77T6A3RZV8W050"
	carolULID  = "01KTRA1Q83VBTES33BRQV79JN6"
)

// fakeHome stands in for users' home nodes.
type fakeHome struct {
	users      map[string]user
	follows    map[string][]string
	followsErr bool
}

func (h *fakeHome) homeUser(id string) (user, bool) {
	u, ok := h.users[id]
	return u, ok
}

func (h *fakeHome) followings(id string) ([]string, bool) {
	return h.follows[id], !h.followsErr
}

func consenting(id string) user {
	return user{Id: id, Metadata: map[string]string{nostrConsentKey: nostrConsentAllow}}
}

func testKeys(t *testing.T) *nostrKeys {
	t.Helper()
	k, err := newNostrKeys([]byte(testSecret))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

type publishFixture struct {
	relay *fakeRelay
	home  *fakeHome
	keys  *nostrKeys
	pub   *nostrPublisher
	note  nostr.Event
	other nostrKey
}

func newPublishFixture(t *testing.T) *publishFixture {
	t.Helper()
	relay := newFakeRelay(t)
	other := newNostrKey()
	note := other.sign(t, kindNote, "a nostr note", time.Now().Unix()-60)
	relay.add(note)
	home := &fakeHome{users: map[string]user{
		aliceULID: consenting(aliceULID),
		bobULID:   consenting(bobULID),
		carolULID: {Id: carolULID},
	}, follows: map[string][]string{}}
	keys := testKeys(t)
	b := testNostrBridge(t, relay)
	b.keys, b.home, b.network = keys, home, defaultWarpnetNetwork
	pub := newNostrPublisher(keys, b, home, defaultWarpnetNetwork, "https://gw.example")
	b.pub = pub
	return &publishFixture{relay: relay, home: home, keys: keys, pub: pub, note: note, other: other}
}

func (fx *publishFixture) pk(t *testing.T, id string) string {
	t.Helper()
	pk, ok := fx.keys.pubKey(defaultWarpnetNetwork, id)
	if !ok {
		t.Fatalf("no key for %s", id)
	}
	return pk
}

func tagged(ev nostr.Event, tag ...string) bool {
	for _, tg := range ev.Tags {
		if len(tg) >= len(tag) && slicesEqual(tg[:len(tag)], tag) {
			return true
		}
	}
	return false
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestNostrKeyDerivation(t *testing.T) {
	k := testKeys(t)
	a1, ok := k.pubKey("warpnet", aliceULID)
	a2, _ := k.pubKey("warpnet", strings.ToLower(aliceULID))
	aTest, _ := k.pubKey("testnet", aliceULID)
	b, _ := k.pubKey("warpnet", bobULID)
	if !ok || a1 != a2 || a1 == aTest || a1 == b || !hex64.MatchString(a1) {
		t.Fatalf("keys: %q %q %q %q", a1, a2, aTest, b)
	}
	other, _ := newNostrKeys([]byte(testSecret + "-other"))
	if o, _ := other.pubKey("warpnet", aliceULID); o == a1 {
		t.Fatal("another master must give another key")
	}
	if _, ok := k.pubKey("warpnet", "bob@mastodon.social"); ok {
		t.Fatal("only ULID accounts have keys")
	}
	if o, ok := k.owner(a1); !ok || o.userID != aliceULID || o.network != "warpnet" {
		t.Fatalf("owner = %+v %v", o, ok)
	}
	if !k.isOwn(a1, "warpnet", aliceULID) || k.isOwn(a1, "warpnet", bobULID) {
		t.Fatal("isOwn must confirm only the derived key")
	}
	sk, _ := k.secretKey("warpnet", aliceULID)
	signed, ok := signEvent(sk, nostrEvent{Kind: kindNote, Content: "x", CreatedAt: 1})
	if !ok || signed.PubKey != a1 {
		t.Fatalf("signed by %q", signed.PubKey)
	}
	if _, ok := parseSignedEvent(eventJSON(signed)); !ok {
		t.Fatal("a derived key must produce a valid BIP-340 signature")
	}
	for _, weak := range []string{"short", defaultGatewaySeed} {
		if _, err := newNostrKeys([]byte(weak)); err == nil {
			t.Errorf("secret %q must be refused", weak)
		}
	}
}

func TestNostrConsent(t *testing.T) {
	fx := newPublishFixture(t)
	if !fx.pub.consents(aliceULID) || fx.pub.consents(carolULID) || fx.pub.consents("01KZZZZZZZZZZZZZZZZZZZZZZZ") {
		t.Fatal("only users who allowed it at sign-up consent")
	}
	fx.home.users[aliceULID] = user{Id: aliceULID}
	fx.pub.actors.Purge()
	if !fx.pub.consents(aliceULID) {
		t.Fatal("consent cannot be taken back")
	}
	fx.home.users[aliceULID] = user{Id: aliceULID, Locked: true}
	fx.pub.actors.Purge()
	if fx.pub.consents(aliceULID) {
		t.Fatal("a locked account is never published")
	}
	testnet := newNostrPublisher(fx.keys, fx.pub.bridge, fx.home, "testnet", "https://gw.example")
	if testnet.consents(bobULID) {
		t.Fatal("testnet stays off the public relays")
	}
}

func TestNostrPublishReactionsAndReposts(t *testing.T) {
	fx := newPublishFixture(t)
	alice := fx.pk(t, aliceULID)
	id := fx.note.ID.Hex()

	fx.pub.React(aliceULID, id, defaultReaction, false)
	fx.pub.React(aliceULID, id, "🔥", false)
	reactions := fx.relay.published(alice, kindReaction)
	if len(reactions) != 2 || reactions[0].Content != "+" || reactions[1].Content != "🔥" ||
		!tagged(reactions[0], "e", id) || !tagged(reactions[0], "p", fx.other.pk) {
		t.Fatalf("reactions = %v", reactions)
	}
	fx.pub.React(aliceULID, id, "", true)
	del := fx.relay.published(alice, kindDelete)
	if len(del) != 1 || !tagged(del[0], "e", reactions[0].ID.Hex()) || !tagged(del[0], "e", reactions[1].ID.Hex()) || !tagged(del[0], "k", "7") {
		t.Fatalf("unreact = %v", del)
	}

	fx.pub.Repost(aliceULID, retweetID(id), false)
	reposts := fx.relay.published(alice, kindRepost)
	if len(reposts) != 1 || !tagged(reposts[0], "e", id) {
		t.Fatalf("reposts = %v", reposts)
	}
	if orig, ok := parseSignedEvent(reposts[0].Content); !ok || orig.ID != id {
		t.Fatal("a repost must embed the signed original")
	}
	fx.pub.Repost(aliceULID, id, true)
	if del = fx.relay.published(alice, kindDelete); len(del) != 2 || !tagged(del[1], "e", reposts[0].ID.Hex()) {
		t.Fatalf("unrepost = %v", del)
	}
}

func retweetID(id string) string { return retweetPrefix + id }

func TestNostrPublishRepliesQuotesAndDeletes(t *testing.T) {
	fx := newPublishFixture(t)
	alice := fx.pk(t, aliceULID)
	id := fx.note.ID.Hex()

	fx.pub.Reply(tweet{Id: "01REPLYULID0000000000000000", UserId: aliceULID, Text: "hello nostr", ParentId: &id, RootId: id})
	replies := fx.relay.published(alice, kindNote)
	if len(replies) != 1 || replies[0].Content != "hello nostr" || !tagged(replies[0], "e", id, "", "root") ||
		!tagged(replies[0], "p", fx.other.pk) || !tagged(replies[0], "proxy", "https://gw.example/users/"+aliceULID+"/statuses/01REPLYULID0000000000000000", "web") {
		t.Fatalf("reply = %v", replies)
	}
	if root, parent := threadRefs(fromLibEvent(replies[0])); root != id || parent != id {
		t.Fatalf("reply threads as %s/%s", root, parent)
	}

	fx.pub.Quote(tweet{Id: "01QUOTEULID0000000000000000", UserId: aliceULID, Text: "look", QuotedTweetId: &id})
	notes := fx.relay.published(alice, kindNote)
	if len(notes) != 2 || !tagged(notes[1], "q", id) || !strings.Contains(notes[1].Content, "nostr:nevent1") {
		t.Fatalf("quote = %v", notes[1])
	}

	fx.pub.DeleteReply(deleteTweetEvent{UserId: aliceULID, TweetId: "01REPLYULID0000000000000000", ParentId: id, RootId: id})
	del := fx.relay.published(alice, kindDelete)
	if len(del) != 1 || !tagged(del[0], "e", replies[0].ID.Hex()) || tagged(del[0], "e", notes[1].ID.Hex()) {
		t.Fatalf("delete = %v, want only the reply", del)
	}

	price := tweet{Id: "01PAIDULID00000000000000000", UserId: aliceULID, Text: "paid", ParentId: &id, RootId: id, Price: &pricedT}
	fx.pub.Reply(price)
	if len(fx.relay.published(alice, kindNote)) != 2 {
		t.Fatal("sponsored content never leaves Warpnet")
	}
}

func TestNostrPublishFollowList(t *testing.T) {
	fx := newPublishFixture(t)
	alice := fx.pk(t, aliceULID)
	dave, erin := newNostrKey(), newNostrKey()
	fx.home.follows[aliceULID] = []string{dave.handle(), bobULID, carolULID, "bob@mastodon.social"}

	fx.pub.Follow(aliceULID, erin.handle(), false)
	lists := fx.relay.published(alice, kindFollowList)
	if len(lists) != 1 {
		t.Fatalf("kind 3 = %v", lists)
	}
	got := map[string]bool{}
	for _, tg := range lists[0].Tags {
		got[tg[1]] = true
	}
	if len(got) != 3 || !got[dave.pk] || !got[erin.pk] || !got[fx.pk(t, bobULID)] {
		t.Fatalf("follow list = %v, want Nostr accounts and consenting Warpnet users only", lists[0].Tags)
	}

	fx.home.follows[aliceULID] = []string{dave.handle(), erin.handle()}
	fx.pub.Follow(aliceULID, erin.handle(), true)
	lists = fx.relay.published(alice, kindFollowList)
	if last := lists[len(lists)-1]; len(last.Tags) != 1 || last.Tags[0][1] != dave.pk {
		t.Fatalf("after unfollow = %v", last.Tags)
	}

	fx.home.followsErr = true
	fx.pub.Follow(aliceULID, dave.handle(), true)
	if len(fx.relay.published(alice, kindFollowList)) != 2 {
		t.Fatal("an unread follow list must not be republished")
	}
}

func TestNostrNothingPublishedWithoutConsent(t *testing.T) {
	fx := newPublishFixture(t)
	id := fx.note.ID.Hex()
	fx.pub.React(carolULID, id, defaultReaction, false)
	fx.pub.Repost(carolULID, id, false)
	fx.pub.Reply(tweet{Id: "01REPLYULID0000000000000000", UserId: carolULID, Text: "x", ParentId: &id, RootId: id})
	fx.pub.Follow(carolULID, newNostrKey().handle(), false)
	var nilPub *nostrPublisher
	nilPub.React(aliceULID, id, defaultReaction, false)
	carol := fx.pk(t, carolULID)
	for _, kind := range []int{kindReaction, kindRepost, kindNote, kindFollowList} {
		if evs := fx.relay.published(carol, kind); len(evs) != 0 {
			t.Fatalf("kind %d published for a user who did not consent", kind)
		}
	}
}

func TestNostrOwnKeysResolveToNativeAccounts(t *testing.T) {
	fx := newPublishFixture(t)
	b := fx.pub.bridge
	alice := fx.pk(t, aliceULID)

	u, err := b.GetUser(testCtx(t), nostrHandle(alice))
	if err != nil || u.Id != aliceULID {
		t.Fatalf("own npub = %+v, %v; want the native account", u, err)
	}

	cold := newNostrBridge(testRelayPool(), b.defaults, "gw", "gw.example")
	coldKeys := testKeys(t)
	cold.keys, cold.home, cold.network = coldKeys, fx.home, defaultWarpnetNetwork
	sk, _ := coldKeys.secretKey(defaultWarpnetNetwork, bobULID)
	coldKeys.owned.Purge()
	profile, _ := signEvent(sk, nostrEvent{Kind: kindProfile, Content: `{"name":"bob"}`, CreatedAt: time.Now().Unix(),
		Tags: [][]string{{"proxy", "https://gw.example/users/" + bobULID, "web"}}})
	lib, _ := toLibEvent(profile)
	fx.relay.add(lib)
	if u, err := cold.GetUser(testCtx(t), nostrHandle(profile.PubKey)); err != nil || u.Id != bobULID {
		t.Fatalf("own profile after a restart = %+v, %v", u, err)
	}
	forger := newNostrKey()
	fx.relay.add(forger.sign(t, kindProfile, `{"name":"fake bob"}`, time.Now().Unix(), []string{"proxy", "https://gw.example/users/" + bobULID, "web"}))
	if u, err := cold.GetUser(testCtx(t), forger.handle()); err == nil && u.Id == bobULID {
		t.Fatal("a profile merely claiming a Warpnet account must not resolve to it")
	}

	fx.relay.add(fx.other.sign(t, kindFollowList, "", time.Now().Unix(), []string{"p", alice}))
	fl, _ := b.GetFollowings(testCtx(t), fx.other.handle(), nil)
	if len(fl.Followings) != 1 || fl.Followings[0] != aliceULID {
		t.Fatalf("followings = %v, want the native id", fl.Followings)
	}
}

func TestNostrPublishingIsOffUnlessEnabled(t *testing.T) {
	g := testGateway(t)
	g.nostrRelays = testRelayPool()
	c := newTestNode(t, "pub-"+t.Name())
	if nb := g.newNostrBridge(c); nb.pub != nil {
		t.Fatal("no keys, no publishing")
	}
	g.nostrKeys = testKeys(t)
	if nb := g.newNostrBridge(c); nb.pub != nil || nb.keys == nil {
		t.Fatal("keys alone only recognise own npubs")
	}
	g.nostrPublish = true
	if nb := g.newNostrBridge(c); nb.pub == nil || nb.pub.network != c.network {
		t.Fatal("keys plus the publish switch turn writing on")
	}
}
