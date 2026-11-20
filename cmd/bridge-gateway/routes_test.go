// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/fediverse"
	"github.com/Warp-net/bridge-gateway/internal/fediverse/fediversetest"
	"github.com/Warp-net/bridge-gateway/internal/node"
	"github.com/Warp-net/bridge-gateway/internal/nostr"
	"github.com/Warp-net/bridge-gateway/internal/nostr/nostrtest"
	"github.com/Warp-net/bridge-gateway/internal/warp"
	"github.com/Warp-net/warpnet/event"
	wjson "github.com/Warp-net/warpnet/json"
)

// routesFixture is the gateway's route table on one node, with an in-process
// Fediverse instance behind it.
type routesFixture struct {
	routes map[string]node.HandlerFunc
	nodeID string
	g      *fediverse.Gateway
	peer   *fediversetest.Instance
	handle string
	actor  string
}

// testNode is the gateway's node as the routes see it.
type testNode struct{ id string }

func (n testNode) Request(string, any) ([]byte, error) { return nil, errors.New("no network") }
func (n testNode) RequestUser(string, string, any) ([]byte, error) {
	return nil, errors.New("no network")
}
func (n testNode) HomeUser(string) (warp.User, bool)  { return warp.User{}, false }
func (n testNode) Followings(string) ([]string, bool) { return nil, false }
func (n testNode) ID() string                         { return n.id }
func (n testNode) NetworkName() string                { return warp.DefaultNetwork }

func newRoutesFixture(t *testing.T) *routesFixture {
	t.Helper()
	f := fediversetest.NewInstance(t)
	g := newRouterTestGateway(t, fediverse.Config{Client: f.Client()})
	actorURL := f.Actor("bob", nil)
	f.WebfingerFor("bob", actorURL)

	// The Nostr bridge is on, as in production, so a request it wrongly
	// claimed would reach this dead relay and fail.
	t.Setenv("GATEWAY_NOSTR_RELAYS", "ws://127.0.0.1:1")
	n := testNode{id: "12D3KooWGatewayNode"}
	return &routesFixture{
		routes: routes(g, testHub(g), n),
		nodeID: n.id,
		g:      g,
		peer:   f,
		handle: "bob@" + f.Host(),
		actor:  actorURL,
	}
}

// request answers a route the way the node's stream handler does, with a
// handler error turned into an error envelope.
func (fx *routesFixture) request(t *testing.T, route string, payload any) []byte {
	t.Helper()
	h, ok := fx.routes[route]
	if !ok {
		t.Fatalf("%s is not served", route)
	}
	buf, err := wjson.Marshal(payload)
	if err != nil {
		t.Fatalf("%s: marshal: %v", route, err)
	}
	resp, err := h(context.Background(), buf)
	if err != nil {
		resp = event.ResponseError{Message: err.Error()}
	}
	bt, err := wjson.Marshal(resp)
	if err != nil {
		t.Fatalf("%s: marshal response: %v", route, err)
	}
	return bt
}

// call answers a route and decodes the reply into out.
func (fx *routesFixture) call(t *testing.T, route string, payload, out any) []byte {
	t.Helper()
	bt := fx.request(t, route, payload)
	if err := warp.NodeResponseError(bt); err != nil {
		t.Fatalf("%s: node error: %v", route, err)
	}
	if out != nil {
		if err := json.Unmarshal(bt, out); err != nil {
			t.Fatalf("%s: decode %s: %v", route, bt, err)
		}
	}
	return bt
}

// testHub is a Nostr hub over a test relay pool that publishes nothing.
func testHub(g *fediverse.Gateway) *nostr.Hub {
	pool := nostr.NewRelayPool(http.DefaultClient)
	pool.AllowInsecure = true
	return nostr.NewHub(pool, noBridgeKeys{}, g, g, g.Host, false)
}

type noBridgeKeys struct{}

func (noBridgeKeys) BridgeKey(string, string) ([]byte, bool) { return nil, false }

func TestRoutesReadRoutes(t *testing.T) {
	fx := newRoutesFixture(t)
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
		bt := fx.request(t, warp.RouteGetUsers, warp.GetAllUsersEvent{UserId: "not-a-handle"})
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

func TestRoutesWriteRoutes(t *testing.T) {
	fx := newRoutesFixture(t)
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
		bt := fx.request(t, warp.RoutePostReact, warp.ReactionEvent{
			OwnerId: "alice", TweetId: noteURL, Emoji: "a/b",
		})
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
func TestRoutesReturnsAnErrorEnvelope(t *testing.T) {
	fx := newRoutesFixture(t)
	bt := fx.request(t, warp.RouteGetTweet, warp.GetTweetEvent{TweetId: fx.peer.URL("/notes/ghost")})
	if rerr := warp.NodeResponseError(bt); rerr == nil {
		t.Fatalf("response = %s, want an error envelope", bt)
	}
}

func TestRoutesWhoToFollow(t *testing.T) {
	fx := newRoutesFixture(t)
	t.Setenv("GATEWAY_DISCOVERY_HOST", fx.peer.Host())
	fx.peer.ServeDoc("/api/v1/directory", "application/json", []any{map[string]any{"acct": "alice", "username": "Alice"}})
	for _, route := range []string{event.PUBLIC_GET_WHOTOFOLLOW, warp.RouteGetUsers} {
		var resp warp.UsersResponse
		fx.call(t, route, warp.GetAllUsersEvent{UserId: warp.DefaultOwnerHandle}, &resp)
		if len(resp.Users) != 1 || resp.Users[0].NodeId != fx.nodeID {
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

func TestRoutesServeNostr(t *testing.T) {
	relay := nostrtest.NewRelay(t)
	alice := nostrtest.NewKey()
	now := time.Now().Unix()
	note := alice.Sign(t, nostr.KindNote, "from nostr", now-1)
	relay.Add(alice.Sign(t, nostr.KindProfile, `{"name":"Alice"}`, now), note)

	g := newRouterTestGateway(t, fediverse.Config{})
	t.Setenv("GATEWAY_NOSTR_RELAYS", relay.URL())
	n := testNode{id: "12D3KooWGatewayNode"}
	fx := &routesFixture{routes: routes(g, testHub(g), n), nodeID: n.id, g: g}

	var u warp.User
	fx.call(t, warp.RouteGetUser, warp.GetUserEvent{UserId: nostr.Handle(alice.PK)}, &u)
	if u.Id != nostr.Handle(alice.PK) || u.Network != warp.NostrNetwork || u.NodeId != fx.nodeID {
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

	parent := note.ID.Hex()
	fx.call(t, warp.RoutePostFollow, warp.NewFollowEvent{FollowerId: "01WARPUSER", FollowingId: nostr.Handle(alice.PK)}, nil)
	fx.call(t, warp.RoutePostReply, warp.Tweet{Id: "01REPLY", UserId: "01WARPUSER", Text: "hi", ParentId: &parent, RootId: parent}, nil)
	fx.call(t, warp.RoutePostReact, warp.ReactionEvent{TweetId: parent, UserId: nostr.Handle(alice.PK), OwnerId: "01WARPUSER"}, nil)
}
