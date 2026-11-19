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
	"net/http"
	"testing"
	"time"

	"github.com/Warp-net/activity-pub-gw/internal/fediverse"
	"github.com/Warp-net/activity-pub-gw/internal/nostr"
	"github.com/Warp-net/activity-pub-gw/internal/nostr/nostrtest"
	"github.com/Warp-net/activity-pub-gw/internal/warp"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestNodeServerRoutesNostr(t *testing.T) {
	relay := nostrtest.NewRelay(t)
	alice := nostrtest.NewKey()
	now := time.Now().Unix()
	note := alice.Sign(t, nostr.KindNote, "from nostr", now-1)
	relay.Add(alice.Sign(t, nostr.KindProfile, `{"name":"Alice"}`, now), note)

	g := newRouterTestGateway(t, fediverse.Config{})
	t.Setenv("GATEWAY_NOSTR_RELAYS", relay.URL())
	hub := testHub(g, []string{relay.URL()})
	server := newTestNode(t, "srv-"+t.Name())
	client := newTestNode(t, "cli-"+t.Name())
	server.serveRoutes(g, hub, warp.DefaultOwnerHandle)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.h.Connect(ctx, peer.AddrInfo{ID: server.h.ID(), Addrs: server.h.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	client.remember(server.h.ID())
	fx := &nodeFixture{server: server, client: client, g: g}

	var u warp.User
	fx.call(t, warp.RouteGetUser, warp.GetUserEvent{UserId: nostr.Handle(alice.PK)}, &u)
	if u.Id != nostr.Handle(alice.PK) || u.Network != warp.NostrNetwork || u.NodeId != server.h.ID().String() {
		t.Fatalf("user = %+v", u)
	}
	var feed warp.TweetsResponse
	fx.call(t, warp.RouteGetTweets, warp.GetAllTweetsEvent{UserId: nostr.Handle(alice.PK)}, &feed)
	if len(feed.Tweets) != 1 || feed.Tweets[0].Id != note.ID.Hex() {
		t.Fatalf("feed = %+v", feed.Tweets)
	}
	var one warp.Tweet
	fx.call(t, warp.RouteGetTweet, warp.GetTweetEvent{TweetId: note.ID.Hex()}, &one)
	if one.Text != "from nostr" {
		t.Fatalf("tweet = %+v", one)
	}

	fx.call(t, warp.RoutePostFollow, warp.NewFollowEvent{FollowerId: "01WARPUSER", FollowingId: nostr.Handle(alice.PK)}, nil)
	if f := hub.live.FollowersOf(alice.PK); len(f) != 1 || f[0] != "01WARPUSER" {
		t.Fatalf("follow not recorded: %v", f)
	}
	parent := note.ID.Hex()
	fx.call(t, warp.RoutePostReply, warp.Tweet{Id: "01REPLY", UserId: "01WARPUSER", Text: "hi", ParentId: &parent, RootId: parent}, nil)
	fx.call(t, warp.RoutePostReact, warp.ReactionEvent{TweetId: parent, UserId: nostr.Handle(alice.PK), OwnerId: "01WARPUSER"}, nil)
}

func TestNostrPublishingIsOffUnlessEnabled(t *testing.T) {
	g := newRouterTestGateway(t, fediverse.Config{})
	hub := testHub(g, nil)
	c := newTestNode(t, "pub-"+t.Name())
	if nb := hub.bridgeFor(c); nb.Pub != nil {
		t.Fatal("no keys, no publishing")
	}
	hub.keys = testKeys(t)
	if nb := hub.bridgeFor(c); nb.Pub != nil || nb.Keys == nil {
		t.Fatal("keys alone only recognise own npubs")
	}
	hub.publish = true
	if nb := hub.bridgeFor(c); nb.Pub == nil || nb.Pub.Network != c.network {
		t.Fatal("keys plus the publish switch turn writing on")
	}
}

// testHub is a Nostr hub over a test relay pool.
func testHub(g *fediverse.Gateway, relays []string) *nostrHub {
	pool := nostr.NewRelayPool(http.DefaultClient)
	pool.AllowInsecure = true
	hub := &nostrHub{g: g, relays: pool}
	hub.live = nostr.NewLive(nostr.NewBridge(hub.relays, relays, "", g.Host), hub.push)
	return hub
}

func testKeys(t *testing.T) *nostr.Keys {
	t.Helper()
	k, err := nostr.NewKeys([]byte("0123456789abcdef0123456789abcdef-test"), defaultGatewaySeed)
	if err != nil {
		t.Fatal(err)
	}
	return k
}
