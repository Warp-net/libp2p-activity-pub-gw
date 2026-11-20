// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Warp-net/activity-pub-gw/internal/fediverse"
	"github.com/Warp-net/activity-pub-gw/internal/fediverse/fediversetest"
	"github.com/Warp-net/activity-pub-gw/internal/nostr"
	"github.com/Warp-net/activity-pub-gw/internal/nostr/nostrtest"
	"github.com/Warp-net/activity-pub-gw/internal/warp"
	"github.com/Warp-net/warpnet/event"

	wjson "github.com/Warp-net/warpnet/json"
	"github.com/Warp-net/warpnet/security"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/libp2p/go-libp2p"

	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// newTestNode builds an in-process libp2p host with a warpnet-derived identity
// (the peer id must come from the ed25519 key the envelope is signed with, or
// the nodeserver's signature check rejects it) wrapped in a nodeClient.
func newTestNode(t *testing.T, seed string) *nodeClient {
	t.Helper()
	priv, err := security.GenerateKeyFromSeed([]byte(seed))
	if err != nil {
		t.Fatalf("seed key: %v", err)
	}
	p2pPriv, err := p2pcrypto.UnmarshalEd25519PrivateKey(priv)
	if err != nil {
		t.Fatalf("unmarshal key: %v", err)
	}
	h, err := libp2p.New(
		libp2p.Identity(p2pPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.DisableRelay(),
	)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	c := &nodeClient{
		h:          h,
		priv:       priv,
		network:    warp.DefaultNetwork,
		relays:     map[peer.ID]struct{}{},
		owner:      expirable.NewLRU[string, peer.ID](ownerCacheSize, nil, ownerCacheTTL),
		bridgeKeys: expirable.NewLRU[peer.ID, []byte](ownerCacheSize, nil, bridgeKeyTTL),

		throttled: expirable.NewLRU[string, struct{}](throttleCacheSize, nil, throttleCooldown),
		users:     expirable.NewLRU[string, fediverse.WarpnetUser](userCacheSize, nil, userCacheTTL),
	}
	c.stream = c.streamToMember
	return c
}

// nodeFixture is a gateway serving Warpnet's public routes over libp2p to a
// second in-process node, with an in-process Fediverse instance behind it.
type nodeFixture struct {
	server *nodeClient
	client *nodeClient
	g      *fediverse.Gateway
	peer   *fediversetest.Instance
	handle string
	actor  string
}

func newNodeFixture(t *testing.T) *nodeFixture {
	t.Helper()
	f := fediversetest.NewInstance(t)
	g := newRouterTestGateway(t, fediverse.Config{Client: f.Client()})
	actorURL := f.Actor("bob", nil)
	f.WebfingerFor("bob", actorURL)

	server := newTestNode(t, "srv-"+t.Name())
	client := newTestNode(t, "cli-"+t.Name())
	// The Nostr bridge is on, as in production, so Mastodon and Threads routes
	// are exercised next to it; a request it wrongly claimed would reach this
	// dead relay and fail.
	t.Setenv("GATEWAY_NOSTR_RELAYS", "ws://127.0.0.1:1")
	server.serveRoutes(g, testHub(g, nostr.RelaysFromEnv()), warp.DefaultOwnerHandle)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.h.Connect(ctx, peer.AddrInfo{ID: server.h.ID(), Addrs: server.h.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	client.remember(server.h.ID())

	return &nodeFixture{server: server, client: client, g: g, peer: f, handle: "bob@" + f.Host(), actor: actorURL}
}

// call streams a route to the gateway node and decodes the reply into out.
func (fx *nodeFixture) call(t *testing.T, route string, payload, out any) []byte {
	t.Helper()
	bt, err := fx.client.Request(route, payload)
	if err != nil {
		t.Fatalf("%s: %v", route, err)
	}
	if rerr := warp.NodeResponseError(bt); rerr != nil {
		t.Fatalf("%s: node error: %v", route, rerr)
	}
	if out != nil {
		if uerr := json.Unmarshal(bt, out); uerr != nil {
			t.Fatalf("%s: decode %s: %v", route, bt, uerr)
		}
	}
	return bt
}

// rawStream writes arbitrary bytes on a route and returns whatever comes back;
// the nodeserver drops (writes nothing to) an envelope it refuses.
func (fx *nodeFixture) rawStream(t *testing.T, route string, data []byte) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = network.WithAllowLimitedConn(ctx, route)
	s, err := fx.client.h.NewStream(ctx, fx.server.h.ID(), protocol.ID(route))
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}
	defer func() { _ = s.Close() }()
	if _, werr := s.Write(data); werr != nil {
		t.Fatalf("write: %v", werr)
	}
	if cerr := s.CloseWrite(); cerr != nil {
		t.Fatalf("close write: %v", cerr)
	}
	buf := make([]byte, 4096)
	n, _ := s.Read(buf)
	return buf[:n]
}

func TestNodeServerInfo(t *testing.T) {
	fx := newNodeFixture(t)
	var info map[string]any
	fx.call(t, event.PUBLIC_GET_INFO, nil, &info)

	if info["owner_id"] != warp.DefaultOwnerHandle {
		t.Fatalf("owner = %v, want the Mastodon entry handle so discovery seeds it", info["owner_id"])
	}
	if info["node_id"] != fx.server.h.ID().String() {
		t.Fatalf("node_id = %v", info["node_id"])
	}
	// The gateway is relay-only, so it reports itself like a NAT'd member node.
	if info["type"] != "member" {
		t.Fatalf("type = %v, want a member node", info["type"])
	}
	if info["reachability"] != float64(network.ReachabilityPrivate) {
		t.Fatalf("reachability = %v, want private (relay-only)", info["reachability"])
	}
	if info["relay_state"] != "off" {
		t.Fatalf("relay_state = %v", info["relay_state"])
	}
	if info["network"] != warp.DefaultNetwork {
		t.Fatalf("network = %v", info["network"])
	}
}

func TestNodeServerRejectsUnauthenticatedEnvelopes(t *testing.T) {
	fx := newNodeFixture(t)

	t.Run("a malformed envelope is dropped", func(t *testing.T) {
		if got := fx.rawStream(t, warp.RouteGetUser, []byte("not an envelope")); len(got) != 0 {
			t.Fatalf("response = %q, want the stream dropped", got)
		}
	})

	// An unsigned envelope must not bypass verification, like the node's own
	// auth middleware.
	t.Run("an unsigned envelope is dropped", func(t *testing.T) {
		body, err := wjson.Marshal(warp.GetUserEvent{UserId: "alice"})
		if err != nil {
			t.Fatal(err)
		}
		env, err := wjson.Marshal(warp.Message{
			Body: wjson.RawMessage(body), MessageId: "m1",
			NodeId: fx.client.h.ID().String(), Destination: warp.RouteGetUser,
			Timestamp: time.Now(), Version: "0.0.0",
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := fx.rawStream(t, warp.RouteGetUser, env); len(got) != 0 {
			t.Fatalf("response = %q, want the stream dropped", got)
		}
	})

	t.Run("a forged signature is dropped", func(t *testing.T) {
		body, err := wjson.Marshal(warp.GetUserEvent{UserId: "alice"})
		if err != nil {
			t.Fatal(err)
		}
		other, err := security.GenerateKeyFromSeed([]byte("not-the-caller"))
		if err != nil {
			t.Fatal(err)
		}
		msg := warp.Message{
			Body: wjson.RawMessage(body), MessageId: "m2",
			NodeId: fx.client.h.ID().String(), Destination: warp.RouteGetUser,
			Timestamp: time.Now(), Version: "0.0.0",
		}
		msg.Signature = security.Sign(other, msg.SigningBytes())
		env, merr := wjson.Marshal(msg)
		if merr != nil {
			t.Fatal(merr)
		}
		if got := fx.rawStream(t, warp.RouteGetUser, env); len(got) != 0 {
			t.Fatalf("response = %q, want a signature from another key refused", got)
		}
	})
}

func TestNodeServerReadRoutes(t *testing.T) {
	fx := newNodeFixture(t)
	f := fx.peer
	noteURL := f.URL("/users/bob/statuses/1")

	f.ServeDoc("/users/bob"+fediverse.PathFollowers, fediverse.ContentTypeAP, map[string]any{
		"totalItems": 2, "orderedItems": []any{"https://o.example/users/carol"},
	})
	f.ServeDoc("/users/bob/following", fediverse.ContentTypeAP, map[string]any{
		"totalItems": 1, "orderedItems": []any{"https://o.example/users/dave"},
	})
	f.ServeDoc("/users/bob/outbox", fediverse.ContentTypeAP, map[string]any{"totalItems": 3})
	f.ServeDoc("/users/bob/statuses/1", fediverse.ContentTypeAP, map[string]any{
		"type": fediverse.TypeNote, "id": noteURL, "attributedTo": fx.actor, "content": "<p>hello</p>",
		"published": "2024-01-01T00:00:00Z",
		"likes":     map[string]any{"totalItems": float64(4)},
		"shares":    map[string]any{"totalItems": float64(1)},
		"replies":   map[string]any{"totalItems": float64(2)},
	})
	f.ServeDoc("/api/v1/statuses/1/context", "application/json", map[string]any{
		"ancestors": []any{},
		"descendants": []any{map[string]any{
			"id": "2", "uri": f.URL("/users/carol/statuses/2"), "content": "<p>a reply</p>",
			"in_reply_to_id": "1", "account": map[string]any{"acct": "carol"},
		}},
	})

	t.Run("get user", func(t *testing.T) {
		var u warp.User
		fx.call(t, warp.RouteGetUser, warp.GetUserEvent{UserId: fx.handle}, &u)
		if u.Id != fx.handle || u.Network != warp.MastodonNetwork {
			t.Fatalf("user = %+v", u)
		}
		if u.FollowersCount != 2 || u.FollowingsCount != 1 || u.TweetsCount != 3 {
			t.Fatalf("counts = %d/%d/%d", u.FollowersCount, u.FollowingsCount, u.TweetsCount)
		}
	})

	t.Run("get users returns a one-row listing", func(t *testing.T) {
		var resp warp.UsersResponse
		fx.call(t, warp.RouteGetUsers, warp.GetAllUsersEvent{UserId: fx.handle}, &resp)
		if len(resp.Users) != 1 || resp.Users[0].Id != fx.handle {
			t.Fatalf("users = %+v", resp.Users)
		}
	})

	// An unknown handle must come back as an empty listing, not an error that a
	// search UI would surface.
	t.Run("get users tolerates an unknown handle", func(t *testing.T) {
		bt, err := fx.client.Request(warp.RouteGetUsers, warp.GetAllUsersEvent{UserId: "not-a-handle"})
		if err != nil {
			t.Fatal(err)
		}
		var resp warp.UsersResponse
		if uerr := json.Unmarshal(bt, &resp); uerr != nil {
			t.Fatalf("decode %s: %v", bt, uerr)
		}
		if len(resp.Users) != 0 {
			t.Fatalf("users = %+v, want none", resp.Users)
		}
	})

	t.Run("get tweet", func(t *testing.T) {
		var tw warp.Tweet
		fx.call(t, warp.RouteGetTweet, warp.GetTweetEvent{TweetId: noteURL}, &tw)
		if tw.Id != noteURL || tw.Text != "hello" {
			t.Fatalf("tweet = %+v", tw)
		}
	})

	t.Run("get tweet stats", func(t *testing.T) {
		var stats event.TweetStatsResponse
		fx.call(t, event.PUBLIC_GET_TWEET_STATS, warp.GetTweetEvent{TweetId: noteURL}, &stats)
		if stats.ReactionsCount != 4 || stats.RetweetsCount != 1 || stats.RepliesCount != 2 {
			t.Fatalf("stats = %+v", stats)
		}
		if stats.Reactions[warp.DefaultReaction] != 4 {
			t.Fatalf("stats.Reactions = %v, want the favourites as hearts", stats.Reactions)
		}
	})

	// Warpnet folded thread replies into the tweets route, keyed by parent_id or
	// root_id; the standalone replies route it used to serve is gone.
	t.Run("get tweets folded into thread replies", func(t *testing.T) {
		for _, ev := range []warp.GetAllTweetsEvent{
			{ParentId: noteURL},
			{RootId: noteURL},
		} {
			var resp warp.TweetsResponse
			fx.call(t, warp.RouteGetTweets, ev, &resp)
			if len(resp.Tweets) != 1 || resp.Tweets[0].Text != "a reply" {
				t.Fatalf("tweets = %+v", resp.Tweets)
			}
		}
	})

	t.Run("get followers and followings", func(t *testing.T) {
		var fr warp.FollowersResponse
		fx.call(t, warp.RouteGetFollowers, warp.GetFollowersEvent{UserId: fx.handle}, &fr)
		if len(fr.Followers) != 1 || fr.Followers[0] != "carol@o.example" {
			t.Fatalf("followers = %v", fr.Followers)
		}
		var fg warp.FollowingsResponse
		fx.call(t, warp.RouteGetFollowings, warp.GetFollowersEvent{UserId: fx.handle}, &fg)
		if len(fg.Followings) != 1 || fg.Followings[0] != "dave@o.example" {
			t.Fatalf("followings = %v", fg.Followings)
		}
	})

	t.Run("get image", func(t *testing.T) {
		png := []byte{0x89, 'P', 'N', 'G'}
		f.On("/img.png", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set(fediverse.HeaderContentType, "image/png")
			_, _ = w.Write(png)
		})
		var resp warp.GetImageResponse
		fx.call(t, warp.RouteGetImage, warp.GetImageEvent{Key: f.URL("/img.png")}, &resp)
		want := "data:image/png;base64," + base64.StdEncoding.EncodeToString(png)
		if resp.File != want {
			t.Fatalf("file = %q", resp.File)
		}
	})

	t.Run("post view is answered from memory", func(t *testing.T) {
		var resp event.ViewsCountResponse
		fx.call(t, event.PUBLIC_POST_VIEW, nil, &resp)
		if resp.Count != 1 {
			t.Fatalf("count = %d", resp.Count)
		}
	})
}

func TestNodeServerWriteRoutes(t *testing.T) {
	fx := newNodeFixture(t)
	f := fx.peer
	noteURL := f.URL("/users/bob/statuses/1")
	f.ServeDoc("/users/bob/statuses/1", fediverse.ContentTypeAP, map[string]any{
		"type": fediverse.TypeNote, "id": noteURL, "attributedTo": fx.actor, "content": "<p>hello</p>",
		"published": "2024-01-01T00:00:00Z",
		"likes":     map[string]any{"totalItems": float64(3)},
	})

	t.Run("react and unreact", func(t *testing.T) {
		var resp event.ReactionsCountResponse
		fx.call(t, warp.RoutePostReact, warp.ReactionEvent{
			OwnerId: "alice", TweetId: noteURL, Emoji: warp.DefaultReaction,
		}, &resp)
		if resp.Count != 4 {
			t.Fatalf("count = %d", resp.Count)
		}
		if resp.Reactions[warp.DefaultReaction] != 4 {
			t.Fatalf("reactions = %v, want the count attributed to the heart", resp.Reactions)
		}
		fx.call(t, warp.RoutePostUnreact, warp.ReactionEvent{OwnerId: "alice", TweetId: noteURL}, &resp)
		if resp.Count != 2 {
			t.Fatalf("undo count = %d", resp.Count)
		}
		got := f.Delivered()
		if len(got) != 2 || got[0].Doc["type"] != fediverse.TypeLike || got[1].Doc["type"] != fediverse.TypeUndo {
			t.Fatalf("delivered = %+v", got)
		}
	})

	// The node forwards whatever emoji the client sent, and a client that
	// predates reactions sends none — which warpnet reads as the default heart.
	// Dropping it here would lose the favourite for every such client.
	t.Run("a react with no emoji is favourited as the default heart", func(t *testing.T) {
		before := len(f.Delivered())
		var resp event.ReactionsCountResponse
		fx.call(t, warp.RoutePostReact, warp.ReactionEvent{OwnerId: "alice", TweetId: noteURL}, &resp)
		if resp.Count != 4 {
			t.Fatalf("count = %d, want the favourite counted", resp.Count)
		}
		if resp.Reactions[warp.DefaultReaction] != 4 {
			t.Fatalf("reactions = %v, want the count under the default heart", resp.Reactions)
		}
		got := f.Delivered()[before:]
		if len(got) != 1 || got[0].Doc["type"] != fediverse.TypeLike {
			t.Fatalf("delivered = %+v, want a favourite", got)
		}
	})

	// A reaction whose emoji cannot be a database key is refused, like the node
	// refuses it locally — not silently swallowed.
	t.Run("a malformed emoji is an error", func(t *testing.T) {
		bt, err := fx.client.Request(warp.RoutePostReact, warp.ReactionEvent{
			OwnerId: "alice", TweetId: noteURL, Emoji: "a/b",
		})
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		if rerr := warp.NodeResponseError(bt); rerr == nil {
			t.Fatalf("response = %s, want an error envelope", bt)
		}
	})

	// Mastodon has one reaction, the favourite, so only the default heart maps
	// onto it — any other emoji must be accepted without federating a favourite.
	t.Run("a non-heart reaction is accepted but not federated", func(t *testing.T) {
		before := len(f.Delivered())
		var resp event.ReactionsCountResponse
		fx.call(t, warp.RoutePostReact, warp.ReactionEvent{
			OwnerId: "alice", TweetId: noteURL, Emoji: "\U0001F525",
		}, &resp)
		if resp.Count != 0 {
			t.Fatalf("count = %d, want 0: no favourite was made", resp.Count)
		}
		if now := len(f.Delivered()); now != before {
			t.Fatalf("delivered %d activities, want none", now-before)
		}
	})

	t.Run("follow and unfollow", func(t *testing.T) {
		before := len(f.Delivered())
		fx.call(t, warp.RoutePostFollow, warp.NewFollowEvent{FollowerId: "alice", FollowingId: fx.handle}, nil)
		fx.call(t, warp.RoutePostUnfollow, warp.NewFollowEvent{FollowerId: "alice", FollowingId: fx.handle}, nil)
		nostrtest.WaitFor(t, "both follow activities", func() bool { return len(f.Delivered()) >= before+2 })
		got := f.Delivered()[before:]
		if got[0].Doc["type"] != fediverse.TypeFollow || got[1].Doc["type"] != fediverse.TypeUndo {
			t.Fatalf("delivered = %+v", got)
		}
	})

	// Warpnet forwards a reply over the public reply route — a reply is a tweet
	// carrying a parent. A top-level post arrives on the tweet route through
	// follower gossip and is federated in real time.
	t.Run("public reply route: a reply federates as a reply", func(t *testing.T) {
		before := len(f.Delivered())
		parent := noteURL
		var echoed warp.Tweet
		fx.call(t, warp.RoutePostReply, warp.Tweet{
			Id: "r2", ParentId: &parent, RootId: noteURL, UserId: "alice",
			Text: "threaded", CreatedAt: time.Unix(0, 0),
		}, &echoed)
		// The reply is echoed back as the stored tweet so the Warpnet UI renders it.
		if echoed.Id != "r2" || echoed.ParentId == nil || *echoed.ParentId != noteURL {
			t.Fatalf("echo = %+v", echoed)
		}
		got := f.Delivered()[before:]
		if len(got) != 1 || got[0].Doc["type"] != fediverse.TypeCreate {
			t.Fatalf("delivered = %+v", got)
		}
	})

	// A node that predates the move still forwards replies over the tweet route.
	t.Run("private tweet route: a reply still federates as a reply", func(t *testing.T) {
		before := len(f.Delivered())
		parent := noteURL
		var echoed warp.Tweet
		fx.call(t, warp.RoutePostTweet, warp.Tweet{
			Id: "r3", ParentId: &parent, RootId: noteURL, UserId: "alice",
			Text: "legacy threaded", CreatedAt: time.Unix(0, 0),
		}, &echoed)
		if echoed.Id != "r3" {
			t.Fatalf("echo = %+v", echoed)
		}
		got := f.Delivered()[before:]
		if len(got) != 1 || got[0].Doc["type"] != fediverse.TypeCreate {
			t.Fatalf("delivered = %+v", got)
		}
	})

	t.Run("private tweet route: a top-level post is echoed and federated", func(t *testing.T) {
		if err := fx.g.Followers.Add("alice", fx.actor); err != nil {
			t.Fatal(err)
		}
		before := len(f.Delivered())
		var echoed warp.Tweet
		fx.call(t, warp.RoutePostTweet, warp.Tweet{
			Id: "t1", UserId: "alice", Text: "top level", CreatedAt: time.Unix(0, 0),
		}, &echoed)
		if echoed.Id != "t1" {
			t.Fatalf("echo = %+v", echoed)
		}
		nostrtest.WaitFor(t, "the gossiped post to federate", func() bool { return len(f.Delivered()) > before })
		got := f.Delivered()[before]
		if got.Doc["type"] != fediverse.TypeCreate {
			t.Fatalf("delivered = %+v", got.Doc)
		}
		n, _ := got.Doc["object"].(map[string]any)
		if n == nil || n["id"] != fx.g.ActorID("alice")+fediverse.PathStatuses+"t1" {
			t.Fatalf("note = %+v", n)
		}
	})

	// Warpnet delivers a followed author's new tweet over the public timeline
	// route now; the gateway federates it the same way.
	t.Run("timeline route: a top-level post is echoed and federated", func(t *testing.T) {
		if err := fx.g.Followers.Add("alice", fx.actor); err != nil {
			t.Fatal(err)
		}
		before := len(f.Delivered())
		var echoed warp.Tweet
		fx.call(t, warp.RoutePostTimeline, warp.Tweet{
			Id: "t2", UserId: "alice", Text: "delivered to timeline", CreatedAt: time.Unix(0, 0),
		}, &echoed)
		if echoed.Id != "t2" {
			t.Fatalf("echo = %+v", echoed)
		}
		nostrtest.WaitFor(t, "the delivered post to federate", func() bool { return len(f.Delivered()) > before })
		got := f.Delivered()[before]
		if got.Doc["type"] != fediverse.TypeCreate {
			t.Fatalf("delivered = %+v", got.Doc)
		}
		n, _ := got.Doc["object"].(map[string]any)
		if n == nil || n["id"] != fx.g.ActorID("alice")+fediverse.PathStatuses+"t2" {
			t.Fatalf("note = %+v", n)
		}
	})

	t.Run("timeline route: a reply is acknowledged but not federated", func(t *testing.T) {
		before := len(f.Delivered())
		parent := noteURL
		var echoed warp.Tweet
		fx.call(t, warp.RoutePostTimeline, warp.Tweet{
			Id: "r4", ParentId: &parent, RootId: noteURL, UserId: "alice",
			Text: "reply via timeline", CreatedAt: time.Unix(0, 0),
		}, &echoed)
		if echoed.Id != "r4" {
			t.Fatalf("echo = %+v", echoed)
		}
		if got := f.Delivered()[before:]; len(got) != 0 {
			t.Fatalf("a reply must not federate from the timeline route, delivered = %+v", got)
		}
	})

	t.Run("retweet and unretweet", func(t *testing.T) {
		before := len(f.Delivered())
		by := "alice"
		fx.call(t, warp.RoutePostRetweet, warp.Tweet{Id: noteURL, UserId: "carol", RetweetedBy: &by}, nil)
		fx.call(t, warp.RoutePostUnretweet, warp.UnretweetEvent{RetweeterId: "alice", TweetId: noteURL}, nil)
		got := f.Delivered()[before:]
		if len(got) != 2 || got[0].Doc["type"] != fediverse.TypeAnnounce || got[1].Doc["type"] != fediverse.TypeUndo {
			t.Fatalf("delivered = %+v", got)
		}
		if got[0].Doc["actor"] != fx.g.ActorID("alice") {
			t.Fatalf("boost actor = %v, want the retweeter", got[0].Doc["actor"])
		}
	})

	t.Run("delete", func(t *testing.T) {
		before := len(f.Delivered())
		// Without thread ids there is no Mastodon note to target: acknowledge only.
		fx.call(t, warp.RouteDeleteTweet, warp.DeleteTweetEvent{UserId: "alice", TweetId: "x"}, nil)
		if len(f.Delivered()) != before {
			t.Fatal("a non-reply delete must not federate")
		}
		fx.call(t, warp.RouteDeleteTweet, warp.DeleteTweetEvent{
			UserId: "alice", TweetId: "r1", ParentId: noteURL, RootId: noteURL,
		}, nil)
		got := f.Delivered()[before:]
		if len(got) != 1 || got[0].Doc["type"] != fediverse.TypeDelete {
			t.Fatalf("delivered = %+v", got)
		}
	})
}

// A handler failure must come back as warpnet's error envelope, not a dropped
// stream — the caller has no other way to tell what went wrong.
func TestNodeServerReturnsAnErrorEnvelope(t *testing.T) {
	fx := newNodeFixture(t)
	bt, err := fx.client.Request(warp.RouteGetTweet, warp.GetTweetEvent{TweetId: fx.peer.URL("/notes/ghost")})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if rerr := warp.NodeResponseError(bt); rerr == nil {
		t.Fatalf("response = %s, want an error envelope", bt)
	}
}

func TestWrapJSON(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
	}
	h := wrapJSON(func(_ context.Context, ev payload) (any, error) {
		return "got:" + ev.Name, nil
	})

	got, err := h(context.Background(), []byte(`{"name":"alice"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got != "got:alice" {
		t.Fatalf("got %v", got)
	}

	// An empty body must decode as the zero value (PUBLIC_POST_VIEW sends none).
	got, err = h(context.Background(), nil)
	if err != nil || got != "got:" {
		t.Fatalf("empty body: (%v, %v)", got, err)
	}

	if _, err := h(context.Background(), []byte("{{{")); err == nil {
		t.Fatal("a malformed body must be an error")
	}
}

// A react answers the favourite tally attributed to the emoji the reactor named,
// so the client can repaint its chips without a second round-trip. An unreact
// names no emoji, so only the total travels.
func TestReactionsCount(t *testing.T) {
	got := reactionsCount(warp.DefaultReaction, 3)
	if got.Count != 3 || got.Reactions[warp.DefaultReaction] != 3 {
		t.Fatalf("react = %+v", got)
	}
	if got := reactionsCount("", 2); got.Count != 2 || got.Reactions != nil {
		t.Fatalf("unreact = %+v, want the total only", got)
	}
	// A zero tally must not claim an emoji with no reactions behind it.
	if got := reactionsCount(warp.DefaultReaction, 0); got.Reactions != nil {
		t.Fatalf("zero = %+v, want no breakdown", got)
	}
}

func TestRetweeterOfAndRetweetObject(t *testing.T) {
	by := "alice"
	if got := retweeterOf(warp.Tweet{UserId: "carol", RetweetedBy: &by}); got != "alice" {
		t.Fatalf("retweeter = %q, want the booster", got)
	}
	empty := ""
	if got := retweeterOf(warp.Tweet{UserId: "carol", RetweetedBy: &empty}); got != "carol" {
		t.Fatalf("retweeter = %q, want the author when RetweetedBy is blank", got)
	}
	if got := retweeterOf(warp.Tweet{UserId: "carol"}); got != "carol" {
		t.Fatalf("retweeter = %q", got)
	}

	if got := retweetObject(warp.Tweet{Id: "https://m/1", RootId: "https://m/0"}); got != "https://m/1" {
		t.Fatalf("object = %q, want the tweet id", got)
	}
	if got := retweetObject(warp.Tweet{RootId: "https://m/0"}); got != "https://m/0" {
		t.Fatalf("object = %q, want the root as fallback", got)
	}
}

func TestNodeServerWhoToFollow(t *testing.T) {
	fx := newNodeFixture(t)
	t.Setenv("GATEWAY_DISCOVERY_HOST", fx.peer.Host())
	fx.peer.ServeDoc("/api/v1/directory", "application/json", []any{map[string]any{"acct": "alice", "username": "Alice"}})
	for _, route := range []string{event.PUBLIC_GET_WHOTOFOLLOW, warp.RouteGetUsers} {
		var resp warp.UsersResponse
		fx.call(t, route, warp.GetAllUsersEvent{UserId: warp.DefaultOwnerHandle}, &resp)
		if len(resp.Users) != 1 || resp.Users[0].NodeId != fx.server.h.ID().String() {
			t.Fatalf("%s response = %+v", route, resp)
		}
	}
}

// newRouterTestGateway builds a gateway as main does, serving alice.
func newRouterTestGateway(t *testing.T, cfg fediverse.Config) *fediverse.Gateway {
	t.Helper()
	t.Setenv("GATEWAY_AP_MIRROR", "")
	key, err := fediverse.LoadOrCreateKey(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	cfg.Host, cfg.Key, cfg.AllowPrivateTargets = "gw.example", key, true
	if cfg.Source == nil {
		cfg.Source = fediverse.StaticSource{User: fediverse.WarpnetUser{ID: "alice", PreferredUsername: "alice", DisplayName: "Alice"}}
	}
	if cfg.Client == nil {
		cfg.Client = http.DefaultClient
	}
	g, err := fediverse.New(cfg)
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	return g
}

func TestNodeServerRemembersCallingMembers(t *testing.T) {
	fx := newNodeFixture(t)
	if got := fx.server.memberCandidates(warp.RouteGetUsers); len(got) != 0 {
		t.Fatalf("candidates = %v before any call", got)
	}
	fx.call(t, event.PUBLIC_GET_INFO, nil, nil)
	got := fx.server.memberCandidates(warp.RouteGetUsers)
	if len(got) != 1 || got[0] != fx.client.h.ID() {
		t.Fatalf("candidates = %v; a node that called must be asked back", got)
	}
}
