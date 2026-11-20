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
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"fiatjaf.com/nostr"
	"github.com/Warp-net/bridge-gateway/internal/nostr/nostrtest"
	"github.com/Warp-net/bridge-gateway/internal/warp"
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
	users      map[string]warp.User
	follows    map[string][]string
	followsErr bool
}

func (h *fakeHome) HomeUser(id string) (warp.User, bool) {
	u, ok := h.users[id]
	return u, ok
}

func (h *fakeHome) Followings(id string) ([]string, bool) {
	return h.follows[id], !h.followsErr
}

func consenting(id string) warp.User {
	return warp.User{Id: id}
}

func optedOut(id string) warp.User {
	return warp.User{Id: id, Metadata: map[string]string{nostrConsentKey: nostrConsentDeny}}
}

// fakeNodeKeys stands in for users' nodes sending their bridge keys: one per
// network and user, from a test secret.
type fakeNodeKeys struct{ secret string }

func (f fakeNodeKeys) BridgeKey(network, userID string) ([]byte, bool) {
	sum := sha256.Sum256([]byte(f.secret + "\x00" + network + "\x00" + strings.ToUpper(userID)))
	return sum[:], true
}

func testKeys(t *testing.T) *Keys {
	t.Helper()
	return newKeys(fakeNodeKeys{testSecret})
}

type publishFixture struct {
	relay *nostrtest.Relay
	home  *fakeHome
	keys  *Keys
	pub   *Publisher
	note  nostr.Event
	other nostrtest.Key
}

func newPublishFixture(t *testing.T) *publishFixture {
	t.Helper()
	relay := nostrtest.NewRelay(t)
	other := nostrtest.NewKey()
	note := other.Sign(t, KindNote, "a nostr note", time.Now().Unix()-60)
	relay.Add(note)
	home := &fakeHome{users: map[string]warp.User{
		aliceULID: consenting(aliceULID),
		bobULID:   consenting(bobULID),
		carolULID: optedOut(carolULID),
	}, follows: map[string][]string{}}
	keys := testKeys(t)
	b := testNostrBridge(t, relay)
	b.keys, b.links, b.home, b.network = keys, testLinks{}, home, warp.DefaultNetwork
	pub := newPublisher(keys, b, home, warp.DefaultNetwork, testLinks{})
	b.pub = pub
	return &publishFixture{relay: relay, home: home, keys: keys, pub: pub, note: note, other: other}
}

func (fx *publishFixture) pk(t *testing.T, id string) string {
	t.Helper()
	pk, ok := fx.keys.pubKey(warp.DefaultNetwork, id)
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
	other := newKeys(fakeNodeKeys{testSecret + "-other"})
	if o, _ := other.pubKey("warpnet", aliceULID); o == a1 {
		t.Fatal("another node must give another key")
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
	signed, ok := signEvent(sk, nostrEvent{Kind: KindNote, Content: "x", CreatedAt: 1})
	if !ok || signed.PubKey != a1 {
		t.Fatalf("signed by %q", signed.PubKey)
	}
	if _, ok := parseSignedEvent(eventJSON(signed)); !ok {
		t.Fatal("a derived key must produce a valid BIP-340 signature")
	}
	if _, ok := newKeys(nil).pubKey("warpnet", aliceULID); ok {
		t.Fatal("no node to ask, no key")
	}
}

func TestNostrConsent(t *testing.T) {
	fx := newPublishFixture(t)
	if !fx.pub.consents(aliceULID) || fx.pub.consents(carolULID) || fx.pub.consents("01KZZZZZZZZZZZZZZZZZZZZZZZ") {
		t.Fatal("everyone consents but users who opted out at sign-up")
	}
	fx.home.users[aliceULID] = optedOut(aliceULID)
	fx.pub.actors.Purge()
	if fx.pub.consents(aliceULID) {
		t.Fatal("an opt-out is honoured, however late it is read")
	}
	fx.home.users[aliceULID] = warp.User{Id: aliceULID, Locked: true}
	fx.pub.actors.Purge()
	if fx.pub.consents(aliceULID) {
		t.Fatal("a locked account is never published")
	}
	testnet := newPublisher(fx.keys, fx.pub.bridge, fx.home, "testnet", testLinks{})
	if !testnet.consents(bobULID) {
		t.Fatal("testnet users are published like the main network's")
	}
}

func TestNostrPublishReactionsAndReposts(t *testing.T) {
	fx := newPublishFixture(t)
	alice := fx.pk(t, aliceULID)
	id := fx.note.ID.Hex()

	fx.pub.React(aliceULID, id, warp.DefaultReaction, false)
	fx.pub.React(aliceULID, id, "🔥", false)
	reactions := fx.relay.Published(alice, kindReaction)
	if len(reactions) != 2 || reactions[0].Content != "+" || reactions[1].Content != "🔥" ||
		!tagged(reactions[0], "e", id) || !tagged(reactions[0], "p", fx.other.PK) {
		t.Fatalf("reactions = %v", reactions)
	}
	fx.pub.React(aliceULID, id, "", true)
	del := fx.relay.Published(alice, kindDelete)
	if len(del) != 1 || !tagged(del[0], "e", reactions[0].ID.Hex()) || !tagged(del[0], "e", reactions[1].ID.Hex()) || !tagged(del[0], "k", "7") {
		t.Fatalf("unreact = %v", del)
	}

	fx.pub.Repost(aliceULID, retweetID(id), false)
	reposts := fx.relay.Published(alice, kindRepost)
	if len(reposts) != 1 || !tagged(reposts[0], "e", id) {
		t.Fatalf("reposts = %v", reposts)
	}
	if orig, ok := parseSignedEvent(reposts[0].Content); !ok || orig.ID != id {
		t.Fatal("a repost must embed the signed original")
	}
	fx.pub.Repost(aliceULID, id, true)
	if del = fx.relay.Published(alice, kindDelete); len(del) != 2 || !tagged(del[1], "e", reposts[0].ID.Hex()) {
		t.Fatalf("unrepost = %v", del)
	}
}

func retweetID(id string) string { return retweetPrefix + id }

func TestNostrPublishRepliesQuotesAndDeletes(t *testing.T) {
	fx := newPublishFixture(t)
	alice := fx.pk(t, aliceULID)
	id := fx.note.ID.Hex()

	fx.pub.Reply(warp.Tweet{Id: "01REPLYULID0000000000000000", UserId: aliceULID, Text: "hello nostr", ParentId: &id, RootId: id})
	replies := fx.relay.Published(alice, KindNote)
	if len(replies) != 1 || replies[0].Content != "hello nostr" || !tagged(replies[0], "e", id, "", "root") ||
		!tagged(replies[0], "p", fx.other.PK) || !tagged(replies[0], "proxy", "https://gw.example/users/"+aliceULID+"/statuses/01REPLYULID0000000000000000", "web") {
		t.Fatalf("reply = %v", replies)
	}
	if root, parent := threadRefs(fromLibEvent(replies[0])); root != id || parent != id {
		t.Fatalf("reply threads as %s/%s", root, parent)
	}

	fx.pub.Quote(warp.Tweet{Id: "01QUOTEULID0000000000000000", UserId: aliceULID, Text: "look", QuotedTweetId: &id})
	notes := fx.relay.Published(alice, KindNote)
	if len(notes) != 2 || !tagged(notes[1], "q", id) || !strings.Contains(notes[1].Content, "nostr:nevent1") {
		t.Fatalf("quote = %v", notes[1])
	}

	fx.pub.DeleteReply(warp.DeleteTweetEvent{UserId: aliceULID, TweetId: "01REPLYULID0000000000000000", ParentId: id, RootId: id})
	del := fx.relay.Published(alice, kindDelete)
	if len(del) != 1 || !tagged(del[0], "e", replies[0].ID.Hex()) || tagged(del[0], "e", notes[1].ID.Hex()) {
		t.Fatalf("delete = %v, want only the reply", del)
	}

	price := warp.Tweet{Id: "01PAIDULID00000000000000000", UserId: aliceULID, Text: "paid", ParentId: &id, RootId: id, Price: &pricedT}
	fx.pub.Reply(price)
	if len(fx.relay.Published(alice, KindNote)) != 2 {
		t.Fatal("sponsored content never leaves Warpnet")
	}
}

func TestNostrPublishFollowList(t *testing.T) {
	fx := newPublishFixture(t)
	alice := fx.pk(t, aliceULID)
	dave, erin := nostrtest.NewKey(), nostrtest.NewKey()
	fx.home.follows[aliceULID] = []string{Handle(dave.PK), bobULID, carolULID, "bob@mastodon.social"}

	fx.pub.Follow(aliceULID, Handle(erin.PK), false)
	lists := fx.relay.Published(alice, kindFollowList)
	if len(lists) != 1 {
		t.Fatalf("kind 3 = %v", lists)
	}
	got := map[string]bool{}
	for _, tg := range lists[0].Tags {
		got[tg[1]] = true
	}
	if len(got) != 3 || !got[dave.PK] || !got[erin.PK] || !got[fx.pk(t, bobULID)] {
		t.Fatalf("follow list = %v, want Nostr accounts and consenting Warpnet users only", lists[0].Tags)
	}

	fx.home.follows[aliceULID] = []string{Handle(dave.PK), Handle(erin.PK)}
	fx.pub.Follow(aliceULID, Handle(erin.PK), true)
	lists = fx.relay.Published(alice, kindFollowList)
	if last := lists[len(lists)-1]; len(last.Tags) != 1 || last.Tags[0][1] != dave.PK {
		t.Fatalf("after unfollow = %v", last.Tags)
	}

	fx.home.followsErr = true
	fx.pub.Follow(aliceULID, Handle(dave.PK), true)
	if len(fx.relay.Published(alice, kindFollowList)) != 2 {
		t.Fatal("an unread follow list must not be republished")
	}
}

func TestNostrNothingPublishedWithoutConsent(t *testing.T) {
	fx := newPublishFixture(t)
	id := fx.note.ID.Hex()
	fx.pub.React(carolULID, id, warp.DefaultReaction, false)
	fx.pub.Repost(carolULID, id, false)
	fx.pub.Reply(warp.Tweet{Id: "01REPLYULID0000000000000000", UserId: carolULID, Text: "x", ParentId: &id, RootId: id})
	fx.pub.Follow(carolULID, Handle(nostrtest.NewKey().PK), false)
	var nilPub *Publisher
	nilPub.React(aliceULID, id, warp.DefaultReaction, false)
	carol := fx.pk(t, carolULID)
	for _, kind := range []int{kindReaction, kindRepost, KindNote, kindFollowList} {
		if evs := fx.relay.Published(carol, kind); len(evs) != 0 {
			t.Fatalf("kind %d published for a user who did not consent", kind)
		}
	}
}

func TestNostrOwnKeysResolveToNativeAccounts(t *testing.T) {
	fx := newPublishFixture(t)
	b := fx.pub.bridge
	alice := fx.pk(t, aliceULID)

	u, err := b.GetUser(testCtx(t), Handle(alice))
	if err != nil || u.Id != aliceULID {
		t.Fatalf("own npub = %+v, %v; want the native account", u, err)
	}

	cold := newBridge(testRelayPool(), b.defaults, "gw", "gw.example")
	coldKeys := testKeys(t)
	cold.keys, cold.links, cold.home, cold.network = coldKeys, testLinks{}, fx.home, warp.DefaultNetwork
	sk, _ := coldKeys.secretKey(warp.DefaultNetwork, bobULID)
	coldKeys.owned.Purge()
	profile, _ := signEvent(sk, nostrEvent{Kind: KindProfile, Content: `{"name":"bob"}`, CreatedAt: time.Now().Unix(),
		Tags: [][]string{{"proxy", "https://gw.example/users/" + bobULID, "web"}}})
	lib, _ := toLibEvent(profile)
	fx.relay.Add(lib)
	if u, err := cold.GetUser(testCtx(t), Handle(profile.PubKey)); err != nil || u.Id != bobULID {
		t.Fatalf("own profile after a restart = %+v, %v", u, err)
	}
	forger := nostrtest.NewKey()
	fx.relay.Add(forger.Sign(t, KindProfile, `{"name":"fake bob"}`, time.Now().Unix(), []string{"proxy", "https://gw.example/users/" + bobULID, "web"}))
	if u, err := cold.GetUser(testCtx(t), Handle(forger.PK)); err == nil && u.Id == bobULID {
		t.Fatal("a profile merely claiming a Warpnet account must not resolve to it")
	}

	fx.relay.Add(fx.other.Sign(t, kindFollowList, "", time.Now().Unix(), []string{"p", alice}))
	fl, _ := b.GetFollowings(testCtx(t), Handle(fx.other.PK), nil)
	if len(fl.Followings) != 1 || fl.Followings[0] != aliceULID {
		t.Fatalf("followings = %v, want the native id", fl.Followings)
	}
}

// testLinks mirrors the gateway's url scheme on gw.example.
type testLinks struct{}

func (testLinks) ProfileURL(userID string) string { return "https://gw.example/users/" + userID }

func (testLinks) StatusURL(userID, tweetID string) string {
	return "https://gw.example/users/" + userID + "/statuses/" + tweetID
}

func (testLinks) MediaURL(userID, key string) string {
	return "https://gw.example/media/" + userID + "/" + key
}

func (testLinks) ProfileUser(rawURL string) (string, bool) {
	rest, ok := strings.CutPrefix(rawURL, "https://gw.example/users/")
	if !ok {
		return "", false
	}
	userID, _, _ := strings.Cut(rest, "/")
	return userID, userID != ""
}
