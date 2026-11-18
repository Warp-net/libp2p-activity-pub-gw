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
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/libp2p/go-libp2p/core/peer"
)

type pushed struct {
	follower string
	t        tweet
}

// pushRecorder stands in for the follower nodes.
type pushRecorder struct {
	mu   sync.Mutex
	got  []pushed
	mark int
}

func (r *pushRecorder) push(f string, tw tweet) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, pushed{f, tw})
	return nil
}

func (r *pushRecorder) all() []pushed {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]pushed{}, r.got...)
}

func (r *pushRecorder) any() bool { return len(r.all()) > 0 }

func (r *pushRecorder) setMark() { r.mark = len(r.all()) }

func (r *pushRecorder) pastMark() bool { return len(r.all()) > r.mark }

// liveWatch waits for a live subscription on one author.
type liveWatch struct {
	relay *fakeRelay
	pk    string
}

func (w liveWatch) ready() bool { return w.relay.liveFor(w.pk) }

func TestNostrLivePushesFollowedNotes(t *testing.T) {
	relay := newFakeRelay(t)
	alice, bob, carol, stranger := newNostrKey(), newNostrKey(), newNostrKey(), newNostrKey()
	relay.add(alice.sign(t, kindFollowList, "", time.Now().Unix(), []string{"p", bob.pk}))
	b := testNostrBridge(t, relay)

	rec := &pushRecorder{}
	live := newNostrLive(b, rec.push)
	live.resubscribeDelay = 10 * time.Millisecond
	b.trusted = live.trusted

	live.setFollow("01WARPUSER", alice.handle(), false)
	live.setFollow("01WARPUSER", "alice@damus.io", false)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go live.run(ctx)
	waitFor(t, "live subscription", liveWatch{relay, alice.pk}.ready)

	if !live.trusted(alice.pk) || !live.trusted(bob.pk) || live.trusted(stranger.pk) {
		t.Fatal("trust must cover followed authors and their followings only")
	}

	at := time.Now().Unix() + 1
	relay.add(
		alice.sign(t, kindNote, "live note", at),
		alice.sign(t, kindNote, "live reply", at, []string{"e", hexID('a'), "", "root"}),
		stranger.sign(t, kindNote, "not followed", at),
	)
	waitFor(t, "push", rec.any)
	time.Sleep(100 * time.Millisecond)
	p := rec.all()
	if len(p) != 1 || p[0].follower != "01WARPUSER" || p[0].t.Text != "live note" || p[0].t.UserId != alice.handle() {
		t.Fatalf("pushes = %+v, want only the followed author's top-level note", p)
	}

	for range 2 * nostrAuthorBurst {
		relay.add(alice.sign(t, kindNote, "flood "+hexID('f')[:4]+time.Now().String(), at))
	}
	time.Sleep(200 * time.Millisecond)
	if n := len(rec.all()); n > nostrAuthorBurst {
		t.Fatalf("%d pushes, want the per-author limit (%d) to cap a flood", n, nostrAuthorBurst)
	}

	live.setFollow("01OTHER", carol.handle(), false)
	waitFor(t, "resubscription", liveWatch{relay, carol.pk}.ready)
	rec.setMark()
	relay.add(carol.sign(t, kindNote, "carol note", time.Now().Unix()+1))
	waitFor(t, "carol push", rec.pastMark)

	live.setFollow("01WARPUSER", alice.handle(), true)
	if len(live.followersOf(alice.pk)) != 0 {
		t.Fatal("unfollow must drop the follower")
	}
}

// nostrScanRequester serves a users page and per-user followings for the scan.
type nostrScanRequester struct {
	users      []user
	followings map[string][]string
}

func (s nostrScanRequester) request(route string, _ any) ([]byte, error) {
	return json.Marshal(usersResponse{Users: s.users})
}

func (s nostrScanRequester) requestUser(userID, route string, _ any) ([]byte, error) {
	return json.Marshal(followingsResponse{FollowerId: userID, Followings: s.followings[userID]})
}

func TestNostrLiveScanRebuildsFollows(t *testing.T) {
	alice, bob := newNostrKey(), newNostrKey()
	live := newNostrLive(testNostrBridge(t), nil)
	live.scan(nostrScanRequester{
		users: []user{{Id: "01NATIVE"}, {Id: alice.handle(), Network: nostrNetwork}, {Id: "01EMPTY"}},
		followings: map[string][]string{
			"01NATIVE": {alice.handle(), "bob@mastodon.social", "01WARP"},
			"01EMPTY":  {},
		},
	}, defaultOwnerHandle)
	if f := live.followersOf(alice.pk); len(f) != 1 || f[0] != "01NATIVE" {
		t.Fatalf("followers of alice = %v", f)
	}
	live.setFollow("01NATIVE", bob.handle(), false)
	live.scan(nostrScanRequester{
		users:      []user{{Id: "01NATIVE"}},
		followings: map[string][]string{"01NATIVE": {bob.handle()}},
	}, defaultOwnerHandle)
	if len(live.followersOf(alice.pk)) != 0 || len(live.followersOf(bob.pk)) != 1 {
		t.Fatal("a scan must replace what it read for a user")
	}
}

func TestNodeServerRoutesNostr(t *testing.T) {
	relay := newFakeRelay(t)
	alice := newNostrKey()
	now := time.Now().Unix()
	note := alice.sign(t, kindNote, "from nostr", now-1)
	relay.add(alice.sign(t, kindProfile, `{"name":"Alice"}`, now), note)

	g := testGateway(t)
	g.actorIDs = expirable.NewLRU[string, string](actorIDsSize, nil, actorIDsTTL)
	t.Setenv("GATEWAY_NOSTR_RELAYS", relay.url())
	g.nostrRelays = testRelayPool()
	g.nostrLive = newNostrLive(newNostrBridge(g.nostrRelays, []string{relay.url()}, "", g.host), g.pushTimeline)
	server := newTestNode(t, "srv-"+t.Name())
	client := newTestNode(t, "cli-"+t.Name())
	server.serveRoutes(g, defaultOwnerHandle)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.h.Connect(ctx, peer.AddrInfo{ID: server.h.ID(), Addrs: server.h.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	client.remember(server.h.ID())
	fx := &nodeFixture{server: server, client: client, g: g}

	var u user
	fx.call(t, routeGetUser, getUserEvent{UserId: alice.handle()}, &u)
	if u.Id != alice.handle() || u.Network != nostrNetwork || u.NodeId != server.h.ID().String() {
		t.Fatalf("user = %+v", u)
	}
	var feed tweetsResponse
	fx.call(t, routeGetTweets, getAllTweetsEvent{UserId: alice.handle()}, &feed)
	if len(feed.Tweets) != 1 || feed.Tweets[0].Id != note.ID.Hex() {
		t.Fatalf("feed = %+v", feed.Tweets)
	}
	var one tweet
	fx.call(t, routeGetTweet, getTweetEvent{TweetId: note.ID.Hex()}, &one)
	if one.Text != "from nostr" {
		t.Fatalf("tweet = %+v", one)
	}

	fx.call(t, routePostFollow, newFollowEvent{FollowerId: "01WARPUSER", FollowingId: alice.handle()}, nil)
	if f := g.nostrLive.followersOf(alice.pk); len(f) != 1 || f[0] != "01WARPUSER" {
		t.Fatalf("follow not recorded: %v", f)
	}
	parent := note.ID.Hex()
	fx.call(t, routePostReply, tweet{Id: "01REPLY", UserId: "01WARPUSER", Text: "hi", ParentId: &parent, RootId: parent}, nil)
	fx.call(t, routePostReact, reactionEvent{TweetId: parent, UserId: alice.handle(), OwnerId: "01WARPUSER"}, nil)
}
