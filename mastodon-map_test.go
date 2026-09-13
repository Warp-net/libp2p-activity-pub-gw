// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestActorToUser(t *testing.T) {
	m := map[string]any{
		"name":      "Bob The Builder",
		"summary":   "<p>hi <b>there</b></p>",
		"icon":      map[string]any{"type": "Image", "url": "https://m.example/avatar.png"},
		"image":     []any{map[string]any{"url": "https://m.example/header.png"}},
		"published": "2024-03-04T05:06:07Z",
	}
	u := actorToUser("bob@m.example", "https://m.example/users/bob", m, "node-1")

	if u.Id != "bob@m.example" || u.Username != "Bob The Builder" {
		t.Fatalf("identity: %+v", u)
	}
	if u.Bio != "hi there" {
		t.Fatalf("bio = %q, want the tags stripped", u.Bio)
	}
	if u.NodeId != "node-1" || u.Network != mastodonNetwork {
		t.Fatalf("node/network: %+v", u)
	}
	if u.AvatarKey != "https://m.example/avatar.png" || u.BackgroundImageKey != "https://m.example/header.png" {
		t.Fatalf("images: %q / %q", u.AvatarKey, u.BackgroundImageKey)
	}
	if u.Website == nil || *u.Website != "https://m.example/users/bob" {
		t.Fatalf("website = %v", u.Website)
	}
	if !u.CreatedAt.Equal(time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)) {
		t.Fatalf("createdAt = %v", u.CreatedAt)
	}

	t.Run("falls back to preferredUsername and now", func(t *testing.T) {
		u := actorToUser("x@y", "", map[string]any{"preferredUsername": "x"}, "n")
		if u.Username != "x" {
			t.Fatalf("username = %q", u.Username)
		}
		if u.CreatedAt.IsZero() {
			t.Fatal("a missing published time must default to now, not zero")
		}
		if u.Website != nil {
			t.Fatalf("no actor url must leave website unset, got %v", u.Website)
		}
	})
}

func TestNoteToTweet(t *testing.T) {
	t.Run("rejects non-Note and id-less objects", func(t *testing.T) {
		if _, ok := noteToTweet("bob@m", map[string]any{"type": "Article", "id": "x"}); ok {
			t.Fatal("an Article must not map to a tweet")
		}
		if _, ok := noteToTweet("bob@m", map[string]any{"type": typeNote}); ok {
			t.Fatal("a Note without an id must not map to a tweet")
		}
	})

	t.Run("top-level note", func(t *testing.T) {
		n := apNote("https://m/notes/1", "https://m/users/bob", "<p>hello<br>world</p>")
		got, ok := noteToTweet("bob@m", n)
		if !ok {
			t.Fatal("not ok")
		}
		if got.Id != "https://m/notes/1" || got.RootId != got.Id {
			t.Fatalf("ids: %+v", got)
		}
		if got.ParentId != nil {
			t.Fatalf("top-level must have no parent, got %v", got.ParentId)
		}
		if got.Text != "hello\nworld" {
			t.Fatalf("text = %q, want the <br> kept as a newline", got.Text)
		}
		if got.UserId != "bob@m" || got.Network != mastodonNetwork {
			t.Fatalf("author: %+v", got)
		}
	})

	t.Run("derives the author from attributedTo when no handle is given", func(t *testing.T) {
		n := apNote("https://m.example/notes/1", "https://m.example/users/bob", "hi")
		got, _ := noteToTweet("", n)
		if got.UserId != "bob@m.example" {
			t.Fatalf("UserId = %q", got.UserId)
		}
	})

	t.Run("reply points at its parent and the parent as root", func(t *testing.T) {
		n := apNote("https://m/notes/2", "https://m/users/bob", "re")
		n["inReplyTo"] = "https://m/notes/1"
		got, _ := noteToTweet("bob@m", n)
		if got.ParentId == nil || *got.ParentId != "https://m/notes/1" {
			t.Fatalf("ParentId = %v", got.ParentId)
		}
		if got.RootId != "https://m/notes/1" {
			t.Fatalf("RootId = %q", got.RootId)
		}
	})

	t.Run("explicit quote property wins over the text fallback", func(t *testing.T) {
		n := apNote("https://m/notes/3", "https://m/users/bob", "<p>nice<br>RE: https://o.example/users/ann/statuses/9</p>")
		n["quoteUri"] = "https://o.example/users/ann/statuses/9"
		got, _ := noteToTweet("bob@m", n)
		if got.QuotedTweetId == nil || *got.QuotedTweetId != "https://o.example/users/ann/statuses/9" {
			t.Fatalf("QuotedTweetId = %v", got.QuotedTweetId)
		}
		if got.QuotedUserId == nil || *got.QuotedUserId != "ann@o.example" {
			t.Fatalf("QuotedUserId = %v", got.QuotedUserId)
		}
		if got.Text != "nice" {
			t.Fatalf("text = %q, want the RE: fallback dropped", got.Text)
		}
	})

	t.Run("collects only image attachments", func(t *testing.T) {
		n := apNote("https://m/notes/4", "https://m/users/bob", "pics")
		n["attachment"] = []any{
			map[string]any{"mediaType": "image/png", "url": "https://m/a.png"},
			map[string]any{"mediaType": "video/mp4", "url": "https://m/a.mp4"},
			map[string]any{"mediaType": "image/jpeg"}, // no url
			"not-an-object",
		}
		got, _ := noteToTweet("bob@m", n)
		if !reflect.DeepEqual(got.ImageKeys, []string{"https://m/a.png"}) {
			t.Fatalf("ImageKeys = %v", got.ImageKeys)
		}
	})

	t.Run("missing published time defaults to now", func(t *testing.T) {
		got, _ := noteToTweet("bob@m", map[string]any{"type": typeNote, "id": "https://m/notes/5"})
		if got.CreatedAt.IsZero() {
			t.Fatal("CreatedAt must not be zero")
		}
	})
}

func TestHTMLToText(t *testing.T) {
	cases := map[string]string{
		"<p>one</p><p>two</p>": "one\ntwo",
		"a<br/>b":              "a\nb",
		"a<BR>b":               "a\nb",
		"  <p> spaced </p>  ":  "spaced",
		"plain":                "plain",
		"":                     "",
		// Only tags are stripped: entities are left as the remote server wrote
		// them, so escaped markup can't be re-read as markup downstream.
		"<p>&lt;tag&gt;</p>": "&lt;tag&gt;",
	}
	for in, want := range cases {
		if got := htmlToText(in); got != want {
			t.Errorf("htmlToText(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestQuotedNoteURL(t *testing.T) {
	cases := []struct {
		name string
		note map[string]any
		want string
	}{
		{"quoteUri string", map[string]any{"quoteUri": "https://a/1"}, "https://a/1"},
		{"quoteUrl string", map[string]any{"quoteUrl": "https://a/2"}, "https://a/2"},
		{"misskey", map[string]any{"_misskey_quote": "https://a/3"}, "https://a/3"},
		{"object with href", map[string]any{"quote": map[string]any{"href": "https://a/4"}}, "https://a/4"},
		{"object with id", map[string]any{"quote": map[string]any{"id": "https://a/5"}}, "https://a/5"},
		{"http is refused", map[string]any{"quoteUri": "http://a/6"}, ""},
		{"absent", map[string]any{}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := quotedNoteURL(c.note); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestStatusAuthorHandle(t *testing.T) {
	cases := map[string]string{
		"https://m.example/users/bob/statuses/1": "bob@m.example",
		"https://m.example/@bob/1":               "bob@m.example",
		"https://m.example/notes/abc":            "", // Misskey: no author in the path
		"https://m.example/users//statuses/1":    "",
		"/relative/path":                         "",
		"://bad":                                 "",
	}
	for in, want := range cases {
		if got := statusAuthorHandle(in); got != want {
			t.Errorf("statusAuthorHandle(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSplitREQuote(t *testing.T) {
	t.Run("comment then quote", func(t *testing.T) {
		u, c, ok := splitREQuote("look at this\nRE: https://m/users/a/statuses/1")
		if !ok || u != "https://m/users/a/statuses/1" || c != "look at this" {
			t.Fatalf("got (%q, %q, %v)", u, c, ok)
		}
	})
	t.Run("quote only", func(t *testing.T) {
		u, c, ok := splitREQuote("RE: https://m/1")
		if !ok || u != "https://m/1" || c != "" {
			t.Fatalf("got (%q, %q, %v)", u, c, ok)
		}
	})
	t.Run("mid-word RE: does not match", func(t *testing.T) {
		if _, _, ok := splitREQuote("SCORE: https://m/1"); ok {
			t.Fatal("MORE:/SCORE: must not be read as a quote marker")
		}
	})
	t.Run("trailing text after the url disqualifies it", func(t *testing.T) {
		if _, _, ok := splitREQuote("RE: https://m/1 and more"); ok {
			t.Fatal("the url must end the note")
		}
	})
	t.Run("no marker", func(t *testing.T) {
		if _, _, ok := splitREQuote("just text"); ok {
			t.Fatal("unexpected match")
		}
	})
}

func TestSplitREPrefix(t *testing.T) {
	t.Run("prefix with comment", func(t *testing.T) {
		p, rest, ok := splitREPrefix("RE: https://m/1 nice one")
		if !ok || p != "https://m/1" || rest != "nice one" {
			t.Fatalf("got (%q, %q, %v)", p, rest, ok)
		}
	})
	t.Run("case insensitive, url only", func(t *testing.T) {
		p, rest, ok := splitREPrefix("re: https://m/1")
		if !ok || p != "https://m/1" || rest != "" {
			t.Fatalf("got (%q, %q, %v)", p, rest, ok)
		}
	})
	t.Run("http and short text are refused", func(t *testing.T) {
		if _, _, ok := splitREPrefix("RE: http://m/1"); ok {
			t.Fatal("http must not qualify")
		}
		if _, _, ok := splitREPrefix("hi"); ok {
			t.Fatal("short text must not qualify")
		}
	})
}

func TestStripQuoteFallback(t *testing.T) {
	if got, ok := stripQuoteFallback("comment\nRE: https://m/1"); !ok || got != "comment" {
		t.Fatalf("trailing: (%q, %v)", got, ok)
	}
	if got, ok := stripQuoteFallback("RE: https://m/1 comment"); !ok || got != "comment" {
		t.Fatalf("leading: (%q, %v)", got, ok)
	}
	if got, ok := stripQuoteFallback("plain text"); ok || got != "plain text" {
		t.Fatalf("plain: (%q, %v)", got, ok)
	}
}

func TestCollectHandles(t *testing.T) {
	b, _, _ := newBridgeFixture(t)
	ctx := context.Background()
	got := b.collectHandles(ctx, map[string]any{"orderedItems": []any{
		"https://m.example/users/bob", "https://o.example/@ann", 42,
	}})
	if !reflect.DeepEqual(got, []string{"bob@m.example", "ann@o.example"}) {
		t.Fatalf("orderedItems: %v", got)
	}
	// Servers that use "items" instead of "orderedItems" must work too.
	got = b.collectHandles(ctx, map[string]any{"items": []any{"https://m.example/users/carol"}})
	if !reflect.DeepEqual(got, []string{"carol@m.example"}) {
		t.Fatalf("items: %v", got)
	}
	if got := b.collectHandles(ctx, map[string]any{}); len(got) != 0 {
		t.Fatalf("empty page: %v", got)
	}
}

func TestAPCollectionCount(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want uint64
	}{
		{"object", map[string]any{"totalItems": float64(7)}, 7},
		{"zero stays zero", map[string]any{"totalItems": float64(0)}, 0},
		{"negative is ignored", map[string]any{"totalItems": float64(-3)}, 0},
		{"not a collection", "https://m/likes", 0},
		{"nil", nil, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := apCollectionCount(c.in); got != c.want {
				t.Fatalf("got %d, want %d", got, c.want)
			}
		})
	}
}

func TestAsImageURL(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"string", "https://m/a.png", "https://m/a.png"},
		{"object", map[string]any{"url": "https://m/b.png"}, "https://m/b.png"},
		{"array takes the first", []any{map[string]any{"url": "https://m/c.png"}}, "https://m/c.png"},
		{"empty array", []any{}, ""},
		{"other", 12, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := asImageURL(c.in); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestParseAPTime(t *testing.T) {
	if got := parseAPTime("2024-01-02T03:04:05Z"); !got.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Fatalf("got %v", got)
	}
	if got := parseAPTime(""); !got.IsZero() {
		t.Fatalf("empty must be zero, got %v", got)
	}
	if got := parseAPTime("not a time"); !got.IsZero() {
		t.Fatalf("garbage must be zero, got %v", got)
	}
}

func TestLooseAccessors(t *testing.T) {
	if got := asString(map[string]any{"id": "x"}); got != "x" {
		t.Fatalf("asString object form = %q", got)
	}
	if got := asString("y"); got != "y" {
		t.Fatalf("asString string = %q", got)
	}
	if got := asString(3); got != "" {
		t.Fatalf("asString number = %q", got)
	}
	if asMap("not a map") != nil {
		t.Fatal("asMap must be nil for a non-map")
	}
	if asSlice("not a slice") != nil {
		t.Fatal("asSlice must be nil for a non-slice")
	}
	if got := asSlice([]any{1}); len(got) != 1 {
		t.Fatalf("asSlice = %v", got)
	}
}

// TestNetworkOfHandle pins the network tag to the account's own instance.
// Threads spells itself three ways — the handle domain, the www. actor host and
// the threads.com web urls — and some accounts resolve to a numeric local part
// instead of their username, so the tag can only follow the domain.
func TestNetworkOfHandle(t *testing.T) {
	cases := map[string]string{
		"mosseri@threads.net":           threadsNetwork,
		"mosseri@www.threads.net":       threadsNetwork,
		"mosseri@THREADS.NET":           threadsNetwork,
		"@mosseri@threads.com":          threadsNetwork,
		"17841452547050663@threads.net": threadsNetwork,
		"bob@mastodon.social":           mastodonNetwork,
		"bob@notthreads.net":            mastodonNetwork,
		"bob":                           mastodonNetwork,
	}
	for handle, want := range cases {
		if got := networkOfHandle(handle); got != want {
			t.Errorf("networkOfHandle(%q) = %q, want %q", handle, got, want)
		}
	}
	if !isBridgedNetwork(threadsNetwork) || !isBridgedNetwork(mastodonNetwork) {
		t.Error("both bridged networks must count as bridged")
	}
	if isBridgedNetwork("") || isBridgedNetwork("warpnet") {
		t.Error("a Warpnet network must not count as bridged")
	}
}

// TestCanonicalHostCollapsesThreadsSpellings: only one spelling resolves, so a
// handle built from an actor url has to be collapsed onto it.
func TestCanonicalHostCollapsesThreadsSpellings(t *testing.T) {
	cases := map[string]string{
		"https://www.threads.net/ap/users/mosseri/": "mosseri@threads.net",
		"https://threads.net/ap/users/mosseri/":     "mosseri@threads.net",
		"https://www.threads.com/ap/users/mosseri/": "mosseri@threads.net",
		"https://mastodon.social/users/bob":         "bob@mastodon.social",
		"https://m.example/@bob":                    "bob@m.example",
	}
	for actorURL, want := range cases {
		if got := handleFromActorURL(actorURL); got != want {
			t.Errorf("handleFromActorURL(%q) = %q, want %q", actorURL, got, want)
		}
	}
}

func TestOpaqueLocalPart(t *testing.T) {
	cases := map[string]bool{
		"17841452547050663@threads.net": true,
		"mosseri@threads.net":           false,
		"bob2@mastodon.social":          false,
		"@mastodon.social":              false,
		"nohandle":                      false,
	}
	for handle, want := range cases {
		if got := opaqueLocalPart(handle); got != want {
			t.Errorf("opaqueLocalPart(%q) = %v, want %v", handle, got, want)
		}
	}
}

func TestRestAccountToUser(t *testing.T) {
	u := restAccountToUser("bob@threads.net", map[string]any{
		"username": "bob", "display_name": "Bob", "note": "<p>hi</p>",
		"avatar":          "https://files.example/avatars/bob.jpg",
		"header":          "https://files.example/headers/original/missing.png",
		"url":             "https://www.threads.net/@bob",
		"created_at":      "2023-12-14T00:00:00.000Z",
		"followers_count": float64(12), "following_count": float64(3), "statuses_count": float64(7),
	}, "node-1")
	if u.Id != "bob@threads.net" || u.Username != "Bob" || u.Bio != "hi" {
		t.Fatalf("user = %+v", u)
	}
	if u.Network != threadsNetwork {
		t.Errorf("network = %q, want %q", u.Network, threadsNetwork)
	}
	if u.AvatarKey == "" || u.BackgroundImageKey != "" {
		t.Errorf("images = %q / %q, want the avatar kept and the placeholder dropped", u.AvatarKey, u.BackgroundImageKey)
	}
	if u.FollowersCount != 12 || u.FollowingsCount != 3 || u.TweetsCount != 7 {
		t.Errorf("counts = %d/%d/%d", u.FollowersCount, u.FollowingsCount, u.TweetsCount)
	}
	if u.Website == nil || *u.Website != "https://www.threads.net/@bob" {
		t.Errorf("website = %v", u.Website)
	}
}
