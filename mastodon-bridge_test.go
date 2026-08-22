// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"encoding/base64"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/Warp-net/warpnet/domain"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

// newBridgeFixture wires a bridge to a gateway pointed at an in-process peer.
func newBridgeFixture(t *testing.T) (*mastodonBridge, *gateway, *fakeInstance) {
	t.Helper()
	g := testGateway(t)
	g.actorIDs = expirable.NewLRU[string, string](actorIDsSize, nil, actorIDsTTL)
	f := newFakeInstance(t).attach(g)
	return newMastodonBridge(g, "node-1"), g, f
}

func TestPageCursor(t *testing.T) {
	if got := pageCursor(nil); got != "" {
		t.Fatalf("nil = %q", got)
	}
	// Warpnet paginates with the requesting node's own datastore cursor, which is
	// meaningless to the Fediverse — the gateway must restart from page one
	// rather than try to dereference it.
	local := "/TWEETS/alice/17/https://m/notes/1"
	if got := pageCursor(&local); got != "" {
		t.Fatalf("datastore cursor = %q, want it ignored", got)
	}
	page := "https://m.example/users/bob/outbox?page=2"
	if got := pageCursor(&page); got != page {
		t.Fatalf("page url = %q", got)
	}
}

func TestIsRESTStatusesURL(t *testing.T) {
	cases := map[string]bool{
		"https://m/api/v1/accounts/42/statuses?limit=40": true,
		"https://m/api/v1/accounts/42/statuses":          true,
		"https://m/users/bob/outbox?page=2":              false,
		"https://m/api/v1/statuses/100":                  false,
		"":                                               false,
	}
	for in, want := range cases {
		if got := isRESTStatusesURL(in); got != want {
			t.Errorf("isRESTStatusesURL(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestRestStatusRef(t *testing.T) {
	host, id, ok := restStatusRef("https://m.example/users/bob/statuses/100")
	if !ok || host != "m.example" || id != "100" {
		t.Fatalf("got (%q, %q, %v)", host, id, ok)
	}
	if _, id, ok := restStatusRef("https://m.example/notes/abc/"); !ok || id != "abc" {
		t.Fatalf("trailing slash: id = %q, ok = %v", id, ok)
	}
	for _, in := range []string{"/no/host", "://bad", "https://m.example/", "https://m.example"} {
		if _, _, ok := restStatusRef(in); ok {
			t.Errorf("restStatusRef(%q) unexpectedly ok", in)
		}
	}
}

func TestAcctHandle(t *testing.T) {
	if got := acctHandle("m.example", "bob"); got != "bob@m.example" {
		t.Fatalf("local acct = %q", got)
	}
	if got := acctHandle("m.example", "carol@other.example"); got != "carol@other.example" {
		t.Fatalf("remote acct = %q, want it kept as-is", got)
	}
	if got := acctHandle("m.example", ""); got != "" {
		t.Fatalf("empty acct = %q", got)
	}
}

func TestNumField(t *testing.T) {
	if got := numField(float64(5)); got != 5 {
		t.Fatalf("got %d", got)
	}
	if got := numField(float64(-1)); got != 0 {
		t.Fatalf("negative = %d", got)
	}
	if got := numField("7"); got != 0 {
		t.Fatalf("string = %d", got)
	}
	if got := numField(nil); got != 0 {
		t.Fatalf("nil = %d", got)
	}
}

func TestUndoIf(t *testing.T) {
	inner := activity{Type: typeLike, ID: "a#like-1"}
	if got := undoIf("https://gw/users/alice", inner, false); !reflect.DeepEqual(got, inner) {
		t.Fatalf("undo=false must pass the activity through, got %+v", got)
	}
	wrapped, ok := undoIf("https://gw/users/alice", inner, true).(activity)
	if !ok {
		t.Fatalf("type %T", wrapped)
	}
	if wrapped.Type != typeUndo || wrapped.Actor != "https://gw/users/alice" {
		t.Fatalf("wrapper = %+v", wrapped)
	}
	if got, isAct := wrapped.Object.(activity); !isAct || got.Type != typeLike {
		t.Fatalf("object = %+v, want the Like nested", wrapped.Object)
	}
}

func TestRestBaseTweet(t *testing.T) {
	// The quote shape is Mastodon's real one: the quoted status is nested under
	// quote.quoted_status, and the content opens with an inline "RE: <url>"
	// fallback that must not survive into the tweet text.
	t.Run("maps a status with media and a quote", func(t *testing.T) {
		got, ok := restBaseTweet("m.example", map[string]any{
			"uri":        "https://m.example/users/bob/statuses/1",
			"content":    `<p class="quote-inline">RE: <a href="https://o.example/@ann/9">https://o.example/@ann/9</a></p><p>hi</p>`,
			"created_at": "2024-01-01T00:00:00.000Z",
			"account":    map[string]any{"acct": "bob"},
			"media_attachments": []any{
				map[string]any{"type": "image", "url": "https://m.example/a.png"},
				map[string]any{"type": "video", "url": "https://m.example/a.mp4"},
			},
			"quote": map[string]any{
				"state": "accepted",
				"quoted_status": map[string]any{
					"uri":     "https://o.example/users/ann/statuses/9",
					"account": map[string]any{"acct": "ann@o.example"},
				},
			},
		})
		if !ok {
			t.Fatal("not ok")
		}
		if got.Id != "https://m.example/users/bob/statuses/1" || got.RootId != got.Id {
			t.Fatalf("ids: %+v", got)
		}
		if got.UserId != "bob@m.example" || got.Text != "hi" {
			t.Fatalf("author/text: %+v", got)
		}
		if !reflect.DeepEqual(got.ImageKeys, []string{"https://m.example/a.png"}) {
			t.Fatalf("images = %v, want only the image attachment", got.ImageKeys)
		}
		if got.QuotedTweetId == nil || *got.QuotedTweetId != "https://o.example/users/ann/statuses/9" {
			t.Fatalf("quote = %v", got.QuotedTweetId)
		}
		if got.QuotedUserId == nil || *got.QuotedUserId != "ann@o.example" {
			t.Fatalf("quoted author = %v", got.QuotedUserId)
		}
		if got.ParentId != nil {
			t.Fatalf("a quote is top-level, not a reply: parent = %v", got.ParentId)
		}
	})

	t.Run("tolerates a quote with the status inlined", func(t *testing.T) {
		got, ok := restBaseTweet("m.example", map[string]any{
			"uri": "https://m.example/users/bob/statuses/1", "content": "<p>hi</p>",
			"account": map[string]any{"acct": "bob"},
			"quote": map[string]any{
				"uri":     "https://o.example/users/ann/statuses/9",
				"account": map[string]any{"acct": "ann@o.example"},
			},
		})
		if !ok {
			t.Fatal("not ok")
		}
		if got.QuotedTweetId == nil || *got.QuotedTweetId != "https://o.example/users/ann/statuses/9" {
			t.Fatalf("quote = %v", got.QuotedTweetId)
		}
		if got.QuotedUserId == nil || *got.QuotedUserId != "ann@o.example" {
			t.Fatalf("quoted author = %v", got.QuotedUserId)
		}
	})

	t.Run("falls back to url and defaults the time", func(t *testing.T) {
		got, ok := restBaseTweet("m.example", map[string]any{
			"url": "https://m.example/@bob/2", "account": map[string]any{"acct": "bob"},
		})
		if !ok {
			t.Fatal("not ok")
		}
		if got.Id != "https://m.example/@bob/2" {
			t.Fatalf("id = %q", got.Id)
		}
		if got.CreatedAt.IsZero() {
			t.Fatal("a missing created_at must default to now")
		}
	})

	t.Run("rejects nil and id-less statuses", func(t *testing.T) {
		if _, ok := restBaseTweet("m", nil); ok {
			t.Fatal("nil status")
		}
		if _, ok := restBaseTweet("m", map[string]any{"content": "x"}); ok {
			t.Fatal("a status with neither uri nor url")
		}
	})
}

func TestRestStatusToTweetUnwrapsBoosts(t *testing.T) {
	got, ok := restStatusToTweet("m.example", map[string]any{
		"id": "9", "account": map[string]any{"acct": "booster"},
		"reblog": map[string]any{
			"uri": "https://o.example/users/ann/statuses/1", "content": "<p>original</p>",
			"account": map[string]any{"acct": "ann@o.example"},
		},
	})
	if !ok {
		t.Fatal("not ok")
	}
	if got.Id != "https://o.example/users/ann/statuses/1" || got.Text != "original" {
		t.Fatalf("boost must unwrap to the boosted status: %+v", got)
	}
	if got.RetweetedBy == nil || *got.RetweetedBy != "booster@m.example" {
		t.Fatalf("RetweetedBy = %v", got.RetweetedBy)
	}

	if _, ok := restStatusToTweet("m", nil); ok {
		t.Fatal("nil status")
	}
	if _, ok := restStatusToTweet("m", map[string]any{"reblog": map[string]any{"content": "x"}}); ok {
		t.Fatal("an unmappable reblog must not map")
	}
}

func TestRestReplyToTweet(t *testing.T) {
	const root = "https://m.example/users/alice/statuses/100"
	idToURI := map[string]string{"100": root, "101": "https://m.example/users/bob/statuses/101"}

	got, ok := restReplyToTweet("m.example", map[string]any{
		"uri": "https://m.example/users/carol/statuses/102", "in_reply_to_id": "101",
		"account": map[string]any{"acct": "carol"},
	}, root, idToURI)
	if !ok {
		t.Fatal("not ok")
	}
	if got.RootId != root {
		t.Fatalf("RootId = %q, want the thread root", got.RootId)
	}
	if got.ParentId == nil || *got.ParentId != idToURI["101"] {
		t.Fatalf("ParentId = %v, want the parent's AP uri", got.ParentId)
	}

	t.Run("an unknown in_reply_to falls back to the root", func(t *testing.T) {
		got, _ := restReplyToTweet("m.example", map[string]any{
			"uri": "https://m.example/users/carol/statuses/103", "in_reply_to_id": "999",
			"account": map[string]any{"acct": "carol"},
		}, root, idToURI)
		if got.ParentId == nil || *got.ParentId != root {
			t.Fatalf("ParentId = %v", got.ParentId)
		}
	})

	if _, ok := restReplyToTweet("m", nil, root, idToURI); ok {
		t.Fatal("nil status")
	}
}

// A thread with no replies must still answer a non-nil list: a nil slice
// marshals as JSON null, which the client cannot iterate.
func TestGetRepliesEmptyThreadIsNonNil(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	f.serveDoc("/api/v1/statuses/100/context", "application/json", map[string]any{
		"ancestors": []any{}, "descendants": []any{},
	})
	resp, err := b.GetReplies(context.Background(), f.url("/users/alice/statuses/100"))
	if err != nil {
		t.Fatalf("GetReplies: %v", err)
	}
	if resp.Tweets == nil {
		t.Fatal("an empty thread must still yield a non-nil list")
	}
	if len(resp.Tweets) != 0 {
		t.Fatalf("tweets = %+v, want none", resp.Tweets)
	}
}

func TestCollectionCount(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	ctx := context.Background()

	if got := b.collectionCount(ctx, ""); got != 0 {
		t.Fatalf("empty url = %d", got)
	}
	if got := b.collectionCount(ctx, f.url("/missing")); got != 0 {
		t.Fatalf("unreachable = %d, want 0 rather than an error", got)
	}
	f.serveDoc("/followers", contentTypeAP, map[string]any{"totalItems": 12})
	if got := b.collectionCount(ctx, f.url("/followers")); got != 12 {
		t.Fatalf("got %d", got)
	}
}

func TestGetUserCounts(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	actorURL := f.actor("bob", nil)
	f.webfingerFor("bob", actorURL)
	f.serveDoc("/users/bob"+pathFollowers, contentTypeAP, map[string]any{"totalItems": 3})
	f.serveDoc("/users/bob/following", contentTypeAP, map[string]any{"totalItems": 5})
	f.serveDoc("/users/bob/outbox", contentTypeAP, map[string]any{"totalItems": 7})

	handle := "bob@" + f.host()
	u, err := b.GetUser(context.Background(), handle)
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if u.Id != handle || u.NodeId != "node-1" {
		t.Fatalf("user = %+v", u)
	}
	if u.FollowersCount != 3 || u.FollowingsCount != 5 || u.TweetsCount != 7 {
		t.Fatalf("counts = %d/%d/%d", u.FollowersCount, u.FollowingsCount, u.TweetsCount)
	}

	t.Run("the brief form skips the count fetches", func(t *testing.T) {
		before := f.hitCount("/users/bob" + pathFollowers)
		u, err := b.GetUserBrief(context.Background(), handle)
		if err != nil {
			t.Fatal(err)
		}
		if u.FollowersCount != 0 {
			t.Fatalf("brief counts = %d, want them skipped in list contexts", u.FollowersCount)
		}
		if got := f.hitCount("/users/bob" + pathFollowers); got != before {
			t.Fatalf("followers fetched %d extra times", got-before)
		}
	})

	t.Run("an unresolvable handle fails", func(t *testing.T) {
		if _, err := b.GetUser(context.Background(), "not-a-handle"); err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("an unreachable actor fails", func(t *testing.T) {
		f.on("/.well-known/webfinger", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, contentTypeJRD, map[string]any{
				"links": []any{map[string]any{"rel": "self", "href": f.url("/users/ghost")}},
			})
		})
		if _, err := b.GetUser(context.Background(), "ghost@"+f.host()); err == nil {
			t.Fatal("expected an error")
		}
	})
}

// A handle's actor url effectively never changes, so it must be WebFingered once
// and reused — resolving the follow graph would otherwise WebFinger every row.
func TestResolveActorIDCachesTheLookup(t *testing.T) {
	_, g, f := newBridgeFixture(t)
	actorURL := f.actor("bob", nil)
	f.webfingerFor("bob", actorURL)
	handle := "bob@" + f.host()

	for range 3 {
		got, err := g.resolveActorID(context.Background(), handle)
		if err != nil {
			t.Fatal(err)
		}
		if got != actorURL {
			t.Fatalf("actor = %q", got)
		}
	}
	if n := f.hitCount("/.well-known/webfinger"); n != 1 {
		t.Fatalf("webfinger hits = %d, want the lookup cached", n)
	}
}

func TestResolveActorIDDecodesLegacyIDs(t *testing.T) {
	_, g, _ := newBridgeFixture(t)
	const actorURL = "https://m.example/users/bob"
	got, err := g.resolveActorID(context.Background(), encodeActorID(actorURL))
	if err != nil {
		t.Fatalf("legacy ap: id: %v", err)
	}
	if got != actorURL {
		t.Fatalf("got %q", got)
	}
}

func TestResolveActorIDWebfingerFailures(t *testing.T) {
	_, g, f := newBridgeFixture(t)
	ctx := context.Background()

	if _, err := g.resolveActorID(ctx, "ghost@"+f.host()); err == nil {
		t.Fatal("a 404 webfinger must fail")
	}

	f.on("/.well-known/webfinger", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, contentTypeJRD, map[string]any{
			"links": []any{
				map[string]any{"rel": "http://webfinger.net/rel/profile-page", "href": "https://x/y"},
				map[string]any{"rel": "self", "href": ""},
			},
		})
	})
	_, err := g.resolveActorID(ctx, "nolink@"+f.host())
	if err == nil || !strings.Contains(err.Error(), "no self link") {
		t.Fatalf("err = %v, want the missing self link reported", err)
	}
}

func TestGetTweetsOrRepliesDispatch(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	root := f.url("/users/alice/statuses/100")
	f.serveDoc("/api/v1/statuses/100/context", "application/json", map[string]any{
		"ancestors": []any{},
		"descendants": []any{map[string]any{
			"id": "101", "uri": f.url("/users/bob/statuses/101"), "content": "<p>reply</p>",
			"in_reply_to_id": "100", "account": map[string]any{"acct": "bob"},
		}},
	})

	// A thread-replies request carries parent_id/root_id and must come back as a
	// flat tweet list, not the reply tree.
	resp, err := b.GetTweetsOrReplies(context.Background(), getAllTweetsEvent{ParentId: root})
	if err != nil {
		t.Fatalf("by parent: %v", err)
	}
	if len(resp.Tweets) != 1 || resp.Tweets[0].Id != f.url("/users/bob/statuses/101") {
		t.Fatalf("tweets = %+v", resp.Tweets)
	}

	resp, err = b.GetTweetsOrReplies(context.Background(), getAllTweetsEvent{RootId: root})
	if err != nil {
		t.Fatalf("by root: %v", err)
	}
	if len(resp.Tweets) != 1 {
		t.Fatalf("tweets = %+v", resp.Tweets)
	}
}

func TestGetTweetsFallsBackToTheAPOutbox(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	actorURL := f.actor("bob", nil)
	f.webfingerFor("bob", actorURL)
	// No /api/v1/accounts/lookup: a non-Mastodon instance.
	f.serveDoc("/users/bob/outbox", contentTypeAP, map[string]any{
		"type": "OrderedCollection", "first": f.url("/users/bob/outbox?page=1"),
	})
	f.on("/users/bob/outbox", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery == "" {
			writeJSON(w, contentTypeAP, map[string]any{
				"type": "OrderedCollection", "first": f.url("/users/bob/outbox?page=1"),
			})
			return
		}
		writeJSON(w, contentTypeAP, map[string]any{
			"type": "OrderedCollectionPage",
			"next": f.url("/users/bob/outbox?page=2"),
			"orderedItems": []any{
				map[string]any{"type": typeCreate, "object": apNote(f.url("/notes/1"), actorURL, "<p>one</p>")},
				apNote(f.url("/notes/2"), actorURL, "<p>two</p>"),
				map[string]any{"type": typeAnnounce, "object": f.url("/notes/boosted")},
				map[string]any{"type": typeAnnounce}, // no object: skipped
				"not-an-object",
			},
		})
	})
	f.serveDoc("/notes/boosted", contentTypeAP, apNote(f.url("/notes/boosted"), f.url("/users/ann"), "<p>boosted</p>"))

	resp, err := b.GetTweets(context.Background(), "bob@"+f.host(), nil)
	if err != nil {
		t.Fatalf("GetTweets: %v", err)
	}
	if len(resp.Tweets) != 3 {
		t.Fatalf("tweets = %d (%+v), want the Create, the bare Note and the boost", len(resp.Tweets), resp.Tweets)
	}
	if resp.Cursor != f.url("/users/bob/outbox?page=2") {
		t.Fatalf("cursor = %q, want the next page url", resp.Cursor)
	}
	boost := resp.Tweets[2]
	if boost.RetweetedBy == nil || *boost.RetweetedBy != "bob@"+f.host() {
		t.Fatalf("boost = %+v, want it stamped with the booster", boost)
	}
}

func TestAPTweetsErrors(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	ctx := context.Background()

	t.Run("an actor with no outbox yields an empty timeline", func(t *testing.T) {
		f.serveDoc("/users/noout", contentTypeAP, map[string]any{"id": f.url("/users/noout")})
		f.webfingerFor("noout", f.url("/users/noout"))
		resp, err := b.apTweets(ctx, "noout@"+f.host(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Tweets) != 0 || resp.UserId != "noout@"+f.host() {
			t.Fatalf("resp = %+v", resp)
		}
	})

	t.Run("an outbox with no first page yields an empty timeline", func(t *testing.T) {
		f.serveDoc("/users/nofirst", contentTypeAP, map[string]any{
			"id": f.url("/users/nofirst"), "outbox": f.url("/users/nofirst/outbox"),
		})
		f.serveDoc("/users/nofirst/outbox", contentTypeAP, map[string]any{"type": "OrderedCollection"})
		f.webfingerFor("nofirst", f.url("/users/nofirst"))
		resp, err := b.apTweets(ctx, "nofirst@"+f.host(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Tweets) != 0 {
			t.Fatalf("resp = %+v", resp)
		}
	})

	t.Run("an unreachable outbox is an error", func(t *testing.T) {
		f.serveDoc("/users/deadout", contentTypeAP, map[string]any{
			"id": f.url("/users/deadout"), "outbox": f.url("/users/deadout/outbox"),
		})
		f.webfingerFor("deadout", f.url("/users/deadout"))
		if _, err := b.apTweets(ctx, "deadout@"+f.host(), nil); err == nil {
			t.Fatal("expected an error")
		}
	})
}

func TestGetTweetFallsBackToTheAPNote(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	noteURL := f.url("/notes/1")
	// No REST /api/v1/statuses/1, so the AP note is dereferenced instead.
	f.serveDoc("/notes/1", contentTypeAP, apNote(noteURL, f.url("/users/bob"), "<p>hi</p>"))

	got, err := b.GetTweet(context.Background(), domain.RetweetPrefix+noteURL)
	if err != nil {
		t.Fatalf("GetTweet: %v", err)
	}
	if got.Id != noteURL || got.Text != "hi" {
		t.Fatalf("tweet = %+v", got)
	}
	if got.UserId != "bob@"+f.host() {
		t.Fatalf("author = %q", got.UserId)
	}
}

func TestAPTweetUnwrapsAnActivity(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	f.serveDoc("/create/1", contentTypeAP, map[string]any{
		"type": typeCreate, "object": apNote(f.url("/notes/9"), f.url("/users/bob"), "<p>wrapped</p>"),
	})
	got, err := b.apTweet(context.Background(), f.url("/create/1"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Id != f.url("/notes/9") || got.Text != "wrapped" {
		t.Fatalf("tweet = %+v", got)
	}

	t.Run("an unreachable note is an error", func(t *testing.T) {
		if _, err := b.apTweet(context.Background(), f.url("/notes/ghost")); err == nil {
			t.Fatal("expected an error")
		}
	})
}

// A Misskey-style quote URL carries no author, so the bridge must dereference the
// quoted note — the client routes its quoted-source fetch by that id.
func TestFillQuotedAuthor(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	f.serveDoc("/notes/quoted", contentTypeAP, apNote(f.url("/notes/quoted"), f.url("/users/ann"), "<p>src</p>"))
	f.serveDoc("/notes/quoting", contentTypeAP, map[string]any{
		"type": typeNote, "id": f.url("/notes/quoting"), "attributedTo": f.url("/users/bob"),
		"content": "<p>see this</p>", "quoteUri": f.url("/notes/quoted"),
	})

	got, err := b.apTweet(context.Background(), f.url("/notes/quoting"))
	if err != nil {
		t.Fatal(err)
	}
	if got.QuotedUserId == nil || *got.QuotedUserId != "ann@"+f.host() {
		t.Fatalf("QuotedUserId = %v, want it dereferenced", got.QuotedUserId)
	}

	t.Run("a non-quote is left alone", func(t *testing.T) {
		tw := tweet{Id: "x"}
		b.fillQuotedAuthor(context.Background(), &tw)
		if tw.QuotedUserId != nil {
			t.Fatalf("QuotedUserId = %v", tw.QuotedUserId)
		}
	})

	t.Run("an already-known author is not re-fetched", func(t *testing.T) {
		q, h := f.url("/notes/quoted"), "ann@"+f.host()
		before := f.hitCount("/notes/quoted")
		tw := tweet{Id: "x", QuotedTweetId: &q, QuotedUserId: &h}
		b.fillQuotedAuthor(context.Background(), &tw)
		if got := f.hitCount("/notes/quoted"); got != before {
			t.Fatal("re-fetched a quoted note whose author was already known")
		}
	})

	t.Run("an unreachable quote is tolerated", func(t *testing.T) {
		q := f.url("/notes/ghost")
		tw := tweet{Id: "x", QuotedTweetId: &q}
		b.fillQuotedAuthor(context.Background(), &tw)
		if tw.QuotedUserId != nil {
			t.Fatalf("QuotedUserId = %v, want it left unset", tw.QuotedUserId)
		}
	})
}

func TestGetTweetStatsFallsBackToAPCollections(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	noteURL := f.url("/notes/1")
	f.serveDoc("/notes/1", contentTypeAP, map[string]any{
		"type": typeNote, "id": noteURL,
		"likes":   map[string]any{"totalItems": float64(4)},
		"shares":  map[string]any{"totalItems": float64(2)},
		"replies": map[string]any{"totalItems": float64(6)},
	})

	got, err := b.GetTweetStats(context.Background(), noteURL)
	if err != nil {
		t.Fatalf("GetTweetStats: %v", err)
	}
	if got.ReactionsCount != 4 || got.RetweetsCount != 2 || got.RepliesCount != 6 {
		t.Fatalf("stats = %+v", got)
	}
	// The AP likes collection is the favourite count, which reads back as the
	// default heart in the per-emoji breakdown the client paints chips from.
	if got.Reactions[defaultReaction] != 4 {
		t.Fatalf("stats.Reactions = %v, want 4 hearts", got.Reactions)
	}
	if string(got.TweetId) != noteURL {
		t.Fatalf("TweetId = %q", got.TweetId)
	}

	t.Run("an unreachable note is an error", func(t *testing.T) {
		if _, err := b.GetTweetStats(context.Background(), f.url("/notes/ghost")); err == nil {
			t.Fatal("expected an error")
		}
	})
}

func TestApRepliesInlineAndPaged(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	noteURL := f.url("/notes/1")
	f.serveDoc("/notes/1", contentTypeAP, map[string]any{
		"type": typeNote, "id": noteURL, "replies": f.url("/notes/1/replies"),
	})
	// first is an inline page object with the notes embedded, and a next page.
	f.serveDoc("/notes/1/replies", contentTypeAP, map[string]any{
		"first": map[string]any{
			"items": []any{apNote(f.url("/r1"), f.url("/users/bob"), "<p>r1</p>")},
			"next":  f.url("/notes/1/replies/p2"),
		},
	})
	f.serveDoc("/notes/1/replies/p2", contentTypeAP, map[string]any{
		"orderedItems": []any{apNote(f.url("/r2"), f.url("/users/carol"), "<p>r2</p>")},
	})

	resp, err := b.apReplies(context.Background(), noteURL)
	if err != nil {
		t.Fatalf("apReplies: %v", err)
	}
	if len(resp.Tweets) != 2 {
		t.Fatalf("replies = %+v, want both pages walked", resp.Tweets)
	}
}

func TestApRepliesEdgeCases(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	ctx := context.Background()

	t.Run("a note with no replies collection", func(t *testing.T) {
		f.serveDoc("/notes/noreplies", contentTypeAP, map[string]any{"type": typeNote, "id": f.url("/notes/noreplies")})
		resp, err := b.apReplies(ctx, f.url("/notes/noreplies"))
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Tweets) != 0 || resp.Tweets == nil {
			t.Fatalf("replies = %v, want an empty (non-nil) list", resp.Tweets)
		}
	})

	t.Run("a hidden replies collection is empty, not an error", func(t *testing.T) {
		f.serveDoc("/notes/hidden", contentTypeAP, map[string]any{
			"type": typeNote, "id": f.url("/notes/hidden"), "replies": f.url("/notes/hidden/replies"),
		})
		resp, err := b.apReplies(ctx, f.url("/notes/hidden"))
		if err != nil {
			t.Fatalf("err = %v, want a hidden collection treated as empty", err)
		}
		if len(resp.Tweets) != 0 {
			t.Fatalf("replies = %v", resp.Tweets)
		}
	})

	t.Run("an unreachable note is an error", func(t *testing.T) {
		if _, err := b.apReplies(ctx, f.url("/notes/ghost")); err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("the reply count is bounded", func(t *testing.T) {
		items := make([]any, 0, maxReplies+20)
		for i := range maxReplies + 20 {
			items = append(items, apNote(f.url("/rr")+string(rune('a'+i%26))+string(rune('a'+i/26)), f.url("/users/bob"), "<p>x</p>"))
		}
		f.serveDoc("/notes/many", contentTypeAP, map[string]any{
			"type": typeNote, "id": f.url("/notes/many"), "replies": f.url("/notes/many/replies"),
		})
		f.serveDoc("/notes/many/replies", contentTypeAP, map[string]any{
			"first": map[string]any{"items": items},
		})
		resp, err := b.apReplies(ctx, f.url("/notes/many"))
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Tweets) != maxReplies {
			t.Fatalf("replies = %d, want them capped at %d", len(resp.Tweets), maxReplies)
		}
	})
}

func TestContextRepliesRejectsNonContextDocuments(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	// A REST endpoint that answers with something that is not a context document
	// must fall through to the AP walk instead of returning an empty thread.
	f.serveDoc("/api/v1/statuses/1/context", "application/json", map[string]any{"error": "nope"})
	if _, ok := b.contextReplies(context.Background(), f.url("/notes/1")); ok {
		t.Fatal("a non-context document must not be accepted")
	}
	if _, ok := b.contextReplies(context.Background(), "not a url"); ok {
		t.Fatal("an unparsable note url must not be accepted")
	}
}

func TestGetFollowersAndFollowings(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	actorURL := f.actor("bob", nil)
	f.webfingerFor("bob", actorURL)
	f.serveDoc("/users/bob"+pathFollowers, contentTypeAP, map[string]any{
		"orderedItems": []any{f.url("/users/carol"), "https://o.example/users/dave"},
		"next":         f.url("/users/bob/followers?page=2"),
	})
	f.serveDoc("/users/bob/following", contentTypeAP, map[string]any{
		"items": []any{"https://o.example/users/ann"},
	})
	handle := "bob@" + f.host()

	fr, err := b.GetFollowers(context.Background(), handle, nil)
	if err != nil {
		t.Fatalf("GetFollowers: %v", err)
	}
	if fr.FollowingId != handle {
		t.Fatalf("FollowingId = %q", fr.FollowingId)
	}
	if !reflect.DeepEqual(fr.Followers, []string{"carol@" + f.host(), "dave@o.example"}) {
		t.Fatalf("followers = %v", fr.Followers)
	}
	if fr.Cursor != f.url("/users/bob/followers?page=2") {
		t.Fatalf("cursor = %q", fr.Cursor)
	}

	fg, err := b.GetFollowings(context.Background(), handle, nil)
	if err != nil {
		t.Fatalf("GetFollowings: %v", err)
	}
	if fg.FollowerId != handle {
		t.Fatalf("FollowerId = %q", fg.FollowerId)
	}
	if !reflect.DeepEqual(fg.Followings, []string{"ann@o.example"}) {
		t.Fatalf("followings = %v", fg.Followings)
	}
}

func TestFollowListEdgeCases(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	ctx := context.Background()

	t.Run("an actor without the collection yields empty", func(t *testing.T) {
		f.serveDoc("/users/nocoll", contentTypeAP, map[string]any{"id": f.url("/users/nocoll")})
		f.webfingerFor("nocoll", f.url("/users/nocoll"))
		resp, err := b.GetFollowers(ctx, "nocoll@"+f.host(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Followers) != 0 {
			t.Fatalf("followers = %v", resp.Followers)
		}
	})

	t.Run("a hidden collection yields empty, not an error", func(t *testing.T) {
		f.serveDoc("/users/hidden", contentTypeAP, map[string]any{
			"id": f.url("/users/hidden"), "followers": f.url("/users/hidden/followers"),
		})
		f.webfingerFor("hidden", f.url("/users/hidden"))
		resp, err := b.GetFollowers(ctx, "hidden@"+f.host(), nil)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(resp.Followers) != 0 {
			t.Fatalf("followers = %v", resp.Followers)
		}
	})

	t.Run("an itemless collection follows its first page", func(t *testing.T) {
		f.serveDoc("/users/paged", contentTypeAP, map[string]any{
			"id": f.url("/users/paged"), "followers": f.url("/users/paged/followers"),
		})
		f.serveDoc("/users/paged/followers", contentTypeAP, map[string]any{
			"first": f.url("/users/paged/followers?page=1"),
		})
		f.serveDoc("/users/paged/followers?page=1", contentTypeAP, nil) // path-keyed below
		f.on("/users/paged/followers", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.RawQuery == "" {
				writeJSON(w, contentTypeAP, map[string]any{"first": f.url("/users/paged/followers?page=1")})
				return
			}
			writeJSON(w, contentTypeAP, map[string]any{"orderedItems": []any{"https://o.example/users/zed"}})
		})
		f.webfingerFor("paged", f.url("/users/paged"))
		resp, err := b.GetFollowers(ctx, "paged@"+f.host(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(resp.Followers, []string{"zed@o.example"}) {
			t.Fatalf("followers = %v", resp.Followers)
		}
	})

	t.Run("a cursor page is fetched directly", func(t *testing.T) {
		f.serveDoc("/cursorpage", contentTypeAP, map[string]any{
			"orderedItems": []any{"https://o.example/users/qux"},
		})
		cursor := f.url("/cursorpage")
		resp, err := b.GetFollowers(ctx, "anything", &cursor)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(resp.Followers, []string{"qux@o.example"}) {
			t.Fatalf("followers = %v", resp.Followers)
		}
	})

	t.Run("an unreachable cursor page yields empty", func(t *testing.T) {
		cursor := f.url("/ghostpage")
		resp, err := b.GetFollowers(ctx, "anything", &cursor)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(resp.Followers) != 0 {
			t.Fatalf("followers = %v", resp.Followers)
		}
	})
}

func TestGetImage(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	png := []byte{0x89, 'P', 'N', 'G'}

	f.on("/img.png", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerContentType, "image/png")
		_, _ = w.Write(png)
	})
	// Warpnet's frontend puts the value straight into <img src>, so the wire
	// format must be a full data URL.
	got, err := b.GetImage(context.Background(), f.url("/img.png"))
	if err != nil {
		t.Fatalf("GetImage: %v", err)
	}
	want := "data:image/png;base64," + base64.StdEncoding.EncodeToString(png)
	if got.File != want {
		t.Fatalf("file = %q, want %q", got.File, want)
	}

	t.Run("a missing content type defaults to jpeg", func(t *testing.T) {
		f.on("/notype", func(w http.ResponseWriter, _ *http.Request) {
			w.Header()[headerContentType] = nil
			_, _ = w.Write(png)
		})
		got, err := b.GetImage(context.Background(), f.url("/notype"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(got.File, "data:image/jpeg;base64,") {
			t.Fatalf("file = %q", got.File)
		}
	})

	t.Run("an unreachable image is an error", func(t *testing.T) {
		if _, err := b.GetImage(context.Background(), f.url("/ghost.png")); err == nil {
			t.Fatal("expected an error")
		}
	})
}

// The default heart is the reaction Mastodon can express, so it federates as an
// AP Like — a favourite. Any other emoji has no Mastodon equivalent.
func TestReactFederatesTheHeartAndAdjustsTheCount(t *testing.T) {
	b, g, f := newBridgeFixture(t)
	actorURL := f.actor("bob", nil)
	f.serveDoc("/notes/1", contentTypeAP, map[string]any{
		"type": typeNote, "id": f.url("/notes/1"), "attributedTo": actorURL,
		"likes": map[string]any{"totalItems": float64(3)},
	})

	count, err := b.React(context.Background(), "alice", f.url("/notes/1"), defaultReaction, false)
	if err != nil {
		t.Fatalf("React: %v", err)
	}
	// The note was fetched before the Like federated, so the caller must see the
	// new value, not the stale one.
	if count != 4 {
		t.Fatalf("count = %d, want the favourite counted", count)
	}
	got := f.delivered()
	if len(got) != 1 || got[0].doc["type"] != typeLike {
		t.Fatalf("delivered = %+v", got)
	}
	if got[0].doc["actor"] != g.actorID("alice") {
		t.Fatalf("actor = %v", got[0].doc["actor"])
	}

	t.Run("an unreact wraps the Like in an Undo and decrements", func(t *testing.T) {
		// An unreact drops whichever emoji the reactor had, so it carries none
		// and must still federate.
		count, err := b.React(context.Background(), "alice", f.url("/notes/1"), "", true)
		if err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatalf("count = %d", count)
		}
		got := f.delivered()
		last := got[len(got)-1]
		if last.doc["type"] != typeUndo {
			t.Fatalf("activity = %+v", last.doc)
		}
	})

	t.Run("an unreact never underflows", func(t *testing.T) {
		f.serveDoc("/notes/zero", contentTypeAP, map[string]any{
			"type": typeNote, "id": f.url("/notes/zero"), "attributedTo": actorURL,
		})
		count, err := b.React(context.Background(), "alice", f.url("/notes/zero"), "", true)
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("count = %d, want 0 rather than an unsigned wrap", count)
		}
	})

	t.Run("a non-heart emoji is accepted but not federated", func(t *testing.T) {
		before := len(f.delivered())
		count, err := b.React(context.Background(), "alice", f.url("/notes/1"), "🔥", false)
		if err != nil {
			t.Fatalf("a reaction Mastodon cannot express must not fail: %v", err)
		}
		if count != 0 {
			t.Fatalf("count = %d, want 0: no favourite was made", count)
		}
		// Mastodon has no emoji reaction; sending a Like would book a favourite
		// the reactor never asked for.
		if now := len(f.delivered()); now != before {
			t.Fatalf("delivered %d activities, want none", now-before)
		}
	})

	t.Run("an unreachable note is an error", func(t *testing.T) {
		if _, err := b.React(context.Background(), "alice", f.url("/notes/ghost"), defaultReaction, false); err == nil {
			t.Fatal("expected an error")
		}
	})
}

func TestAnnounceFederatesTheBoost(t *testing.T) {
	b, g, f := newBridgeFixture(t)
	actorURL := f.actor("bob", nil)
	f.serveDoc("/notes/1", contentTypeAP, apNote(f.url("/notes/1"), actorURL, "<p>hi</p>"))

	if err := b.Announce(context.Background(), "alice", f.url("/notes/1"), false); err != nil {
		t.Fatalf("Announce: %v", err)
	}
	got := f.delivered()
	if len(got) != 1 || got[0].doc["type"] != typeAnnounce {
		t.Fatalf("delivered = %+v", got)
	}
	if got[0].doc["actor"] != g.actorID("alice") || got[0].doc["object"] != f.url("/notes/1") {
		t.Fatalf("activity = %+v", got[0].doc)
	}
	to, _ := got[0].doc["to"].([]any)
	if len(to) != 1 || to[0] != asPublic {
		t.Fatalf("to = %v, want the public collection", to)
	}

	t.Run("an unboost is an Undo", func(t *testing.T) {
		if err := b.Announce(context.Background(), "alice", f.url("/notes/1"), true); err != nil {
			t.Fatal(err)
		}
		got := f.delivered()
		if got[len(got)-1].doc["type"] != typeUndo {
			t.Fatalf("activity = %+v", got[len(got)-1].doc)
		}
	})

	t.Run("an unreachable note is an error", func(t *testing.T) {
		if err := b.Announce(context.Background(), "alice", f.url("/notes/ghost"), false); err == nil {
			t.Fatal("expected an error")
		}
	})
}

func TestFollowResolvesThenDelivers(t *testing.T) {
	b, g, f := newBridgeFixture(t)
	actorURL := f.actor("bob", nil)
	f.webfingerFor("bob", actorURL)

	if err := b.Follow(context.Background(), "alice", "bob@"+f.host(), false); err != nil {
		t.Fatalf("Follow: %v", err)
	}
	waitFor(t, "the Follow to be delivered", func() bool { return len(f.delivered()) > 0 })
	got := f.delivered()[0]
	if got.doc["type"] != typeFollow || got.doc["actor"] != g.actorID("alice") {
		t.Fatalf("activity = %+v", got.doc)
	}
	if got.doc["object"] != actorURL {
		t.Fatalf("object = %v", got.doc["object"])
	}

	t.Run("an unresolvable handle is an error", func(t *testing.T) {
		if err := b.Follow(context.Background(), "alice", "ghost@"+f.host(), false); err == nil {
			t.Fatal("expected an error")
		}
	})
}

func TestAuthorInboxRequiresAnAuthor(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	f.serveDoc("/notes/anon", contentTypeAP, map[string]any{"type": typeNote, "id": f.url("/notes/anon")})
	_, _, err := b.authorInbox(context.Background(), f.url("/notes/anon"))
	if err == nil || !strings.Contains(err.Error(), "attributedTo") {
		t.Fatalf("err = %v", err)
	}
}

func TestDeleteFederatesATombstone(t *testing.T) {
	b, g, f := newBridgeFixture(t)
	actorURL := f.actor("bob", nil)
	parentURL := f.url("/notes/parent")
	f.serveDoc("/notes/parent", contentTypeAP, apNote(parentURL, actorURL, "<p>parent</p>"))

	err := b.Delete(context.Background(), deleteTweetEvent{
		UserId: "alice", TweetId: "r1", ParentId: parentURL, RootId: parentURL,
	})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got := f.delivered()
	if len(got) != 1 || got[0].doc["type"] != typeDelete {
		t.Fatalf("delivered = %+v", got)
	}
	obj, _ := got[0].doc["object"].(map[string]any)
	if obj == nil || obj["type"] != typeTombstone {
		t.Fatalf("object = %+v", got[0].doc["object"])
	}
	// The Tombstone id must be byte-identical to what Reply federated, or the
	// remote instance cannot find the status to drop.
	wantID := g.actorID("alice") + pathStatuses + "r1?" + "parent=" + strings.ReplaceAll(
		strings.ReplaceAll(parentURL, ":", "%3A"), "/", "%2F")
	if obj["id"] != wantID {
		t.Fatalf("tombstone id = %v, want %q", obj["id"], wantID)
	}

	t.Run("falls back to the root when no parent is given", func(t *testing.T) {
		err := b.Delete(context.Background(), deleteTweetEvent{
			UserId: "alice", TweetId: "r2", RootId: parentURL,
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("an unreachable parent is an error", func(t *testing.T) {
		err := b.Delete(context.Background(), deleteTweetEvent{
			UserId: "alice", TweetId: "r3", ParentId: f.url("/notes/ghost"),
		})
		if err == nil {
			t.Fatal("expected an error")
		}
	})
}

func TestReplyAddressesAndThreadsTheParentAuthor(t *testing.T) {
	b, g, f := newBridgeFixture(t)
	actorURL := f.actor("bob", nil)
	parentURL := f.url("/users/bob/statuses/9")
	f.serveDoc("/users/bob/statuses/9", contentTypeAP, apNote(parentURL, actorURL, "<p>parent</p>"))

	err := b.Reply(context.Background(), tweet{
		Id: "r1", UserId: "alice", RootId: domain.ID(parentURL), Text: "nice",
	})
	if err != nil {
		t.Fatalf("Reply: %v", err)
	}
	got := f.delivered()
	if len(got) != 1 || got[0].doc["type"] != typeCreate {
		t.Fatalf("delivered = %+v", got)
	}
	n, _ := got[0].doc["object"].(map[string]any)
	if n == nil || n["inReplyTo"] != parentURL {
		t.Fatalf("note = %+v, want it threaded under the parent", n)
	}
	// The reply's own id must carry the parent so serveStatus can resolve it when
	// the remote instance dereferences the note.
	id, _ := n["id"].(string)
	if !strings.HasPrefix(id, g.actorID("alice")+pathStatuses+"r1?") || !strings.Contains(id, replyParentQuery+"=") {
		t.Fatalf("note id = %q", id)
	}
	to, _ := n["to"].([]any)
	if len(to) != 1 || to[0] != actorURL {
		t.Fatalf("to = %v, want the parent author addressed", to)
	}
	// Mastodon only notifies the author when the note also mentions them.
	tags, _ := n["tag"].([]any)
	if len(tags) != 1 {
		t.Fatalf("tag = %v, want a Mention of the parent author", tags)
	}

	t.Run("a reply with no node-assigned id still gets one", func(t *testing.T) {
		pid := parentURL
		err := b.Reply(context.Background(), tweet{
			UserId: "alice", ParentId: &pid, Text: "anon",
		})
		if err != nil {
			t.Fatal(err)
		}
		got := f.delivered()
		n, _ := got[len(got)-1].doc["object"].(map[string]any)
		if id, _ := n["id"].(string); !strings.HasPrefix(id, g.actorID("alice")+pathStatuses) {
			t.Fatalf("note id = %q", id)
		}
	})

	t.Run("an unreachable parent is an error", func(t *testing.T) {
		err := b.Reply(context.Background(), tweet{
			UserId: "alice", RootId: domain.ID(f.url("/notes/ghost")), Text: "x",
		})
		if err == nil {
			t.Fatal("expected an error")
		}
	})
}

func TestDropSelfHandles(t *testing.T) {
	b, _, _ := newBridgeFixture(t)
	got := b.dropSelfHandles([]string{"bob@m.example", "alice@gw.example", "no-at-sign"})
	// Our own users must never be offered back as foreign Mastodon profiles.
	if !reflect.DeepEqual(got, []string{"bob@m.example", "no-at-sign"}) {
		t.Fatalf("got %v", got)
	}
}

func TestGetTweetsContinuesFromACursor(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	handle := "bob@" + f.host()

	t.Run("a REST cursor continues on the REST path", func(t *testing.T) {
		f.on("/api/v1/accounts/42/statuses", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("max_id") != "100" {
				http.NotFound(w, r)
				return
			}
			writeJSON(w, "application/json", []any{map[string]any{
				"id": "99", "uri": f.url("/users/bob/statuses/99"), "content": "<p>older</p>",
				"account": map[string]any{"acct": "bob"},
			}})
		})
		cursor := f.url("/api/v1/accounts/42/statuses?max_id=100")
		resp, err := b.GetTweets(context.Background(), handle, &cursor)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Tweets) != 1 || resp.Tweets[0].Id != f.url("/users/bob/statuses/99") {
			t.Fatalf("tweets = %+v", resp.Tweets)
		}
		// Mastodon paginates by max_id, so the next cursor is the last status id.
		if !strings.Contains(resp.Cursor, "max_id=99") {
			t.Fatalf("cursor = %q", resp.Cursor)
		}
	})

	t.Run("a non-REST cursor continues on the AP outbox page", func(t *testing.T) {
		f.serveDoc("/users/bob/outbox-page2", contentTypeAP, map[string]any{
			"type":         "OrderedCollectionPage",
			"orderedItems": []any{apNote(f.url("/notes/7"), f.url("/users/bob"), "<p>page two</p>")},
		})
		cursor := f.url("/users/bob/outbox-page2")
		resp, err := b.GetTweets(context.Background(), handle, &cursor)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Tweets) != 1 || resp.Tweets[0].Text != "page two" {
			t.Fatalf("tweets = %+v", resp.Tweets)
		}
	})

	t.Run("an unreachable REST page yields an empty timeline", func(t *testing.T) {
		cursor := f.url("/api/v1/accounts/99/statuses?max_id=1")
		resp, err := b.GetTweets(context.Background(), handle, &cursor)
		if err != nil {
			t.Fatalf("err = %v, want an empty page rather than a failure", err)
		}
		if len(resp.Tweets) != 0 {
			t.Fatalf("tweets = %+v", resp.Tweets)
		}
	})
}

func TestRestTweetsFallsBackWhenLookupIsUnusable(t *testing.T) {
	b, _, f := newBridgeFixture(t)

	// A handle that is not name@instance can't be looked up over REST.
	if _, ok := b.restTweets(context.Background(), "nohandle"); ok {
		t.Fatal("a malformed handle must not resolve over REST")
	}
	// An account lookup that answers without an id is not Mastodon-compatible.
	f.serveDoc("/api/v1/accounts/lookup", "application/json", map[string]any{"acct": "bob"})
	if _, ok := b.restTweets(context.Background(), "bob@"+f.host()); ok {
		t.Fatal("a lookup without an id must fall back to the AP outbox")
	}
}

// A failed thread read must surface as an error, not as an empty thread the
// client would render as "no replies".
func TestGetTweetsOrRepliesPropagatesFailures(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	if _, err := b.GetTweetsOrReplies(context.Background(), getAllTweetsEvent{ParentId: f.url("/notes/ghost")}); err == nil {
		t.Fatal("expected an error")
	}
}

func TestGetFollowingsPropagatesFailures(t *testing.T) {
	b, _, f := newBridgeFixture(t)
	if _, err := b.GetFollowings(context.Background(), "ghost@"+f.host(), nil); err == nil {
		t.Fatal("expected an error for an unresolvable handle")
	}
	if _, err := b.GetFollowers(context.Background(), "ghost@"+f.host(), nil); err == nil {
		t.Fatal("expected an error for an unresolvable handle")
	}
}
