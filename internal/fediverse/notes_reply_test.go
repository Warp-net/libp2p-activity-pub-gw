// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package fediverse

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/warp"
)

// TestBuildNoteThreadsReplies verifies a reply carries inReplyTo so Mastodon can
// thread it, while an original post does not.
func TestBuildNoteThreadsReplies(t *testing.T) {
	g := testGateway(t)
	actor := g.ActorID("alice")
	parent := "01PARENT0000000000000000000"
	root := "01ROOT000000000000000000000"

	parentID := parent
	cases := []struct {
		name string
		in   warp.Tweet
		want string
	}{
		{
			name: "top-level post has no inReplyTo",
			in:   warp.Tweet{Id: "01SELF000000000000000000000", UserId: "alice", CreatedAt: time.Unix(0, 0)},
			want: "",
		},
		{
			name: "top-level post whose RootId is itself has no inReplyTo",
			in:   warp.Tweet{Id: root, RootId: root, UserId: "alice", CreatedAt: time.Unix(0, 0)},
			want: "",
		},
		{
			name: "reply with ParentId links to the parent",
			in:   warp.Tweet{Id: "01REPLY00000000000000000000", ParentId: &parentID, RootId: root, UserId: "alice", CreatedAt: time.Unix(0, 0)},
			want: actor + PathStatuses + parent,
		},
		{
			name: "reply off the root (no ParentId) links to the root",
			in:   warp.Tweet{Id: "01REPLY20000000000000000000", RootId: root, UserId: "alice", CreatedAt: time.Unix(0, 0)},
			want: actor + PathStatuses + root,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := g.buildNote("alice", tc.in).InReplyTo; got != tc.want {
				t.Fatalf("InReplyTo = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestMentionOf verifies a federated reply carries a Mention of the parent
// author: without it Mastodon threads the reply but never notifies them.
func TestMentionOf(t *testing.T) {
	author := "https://mastodon.social/users/warpnet"
	parent := "https://mastodon.social/users/warpnet/statuses/116827782393402363"

	tags := mentionOf(author, parent)
	if len(tags) != 1 {
		t.Fatalf("tags = %+v, want one Mention", tags)
	}
	if tags[0].Type != typeMention || tags[0].Href != author {
		t.Fatalf("tag = %+v, want Mention of %q", tags[0], author)
	}
	if want := "@warpnet@mastodon.social"; tags[0].Name != want {
		t.Fatalf("Name = %q, want %q", tags[0].Name, want)
	}
	if got := mentionOf("", parent); got != nil {
		t.Fatalf("unknown author: tags = %+v, want nil", got)
	}
}

// TestServeRepliesUsesTweetsAPI verifies the replies collection is sourced from
// PUBLIC_GET_TWEETS with the note as parent_id (warpnet retired the standalone
// replies route), and that fediverse-authored replies are skipped.
func TestServeRepliesUsesTweetsAPI(t *testing.T) {
	g := testGateway(t)
	parent := "01PARENT0000000000000000000"
	native := warp.Tweet{
		Id: "01REPLY00000000000000000000", ParentId: &parent, RootId: parent,
		UserId: "alice", Username: "alice", Text: "native reply", CreatedAt: time.Unix(0, 0),
	}
	foreign := warp.Tweet{
		Id: "01REPLY20000000000000000000", ParentId: &parent, RootId: parent,
		UserId: warp.APFollowerPrefix + "xxx", Username: "bob@m", Text: "foreign reply", CreatedAt: time.Unix(0, 0),
	}
	// A reply the gateway itself ingested is attributed by handle (see
	// bridgedUserID), so the skip must catch that form too or Mastodon gets its
	// own replies back as duplicates.
	foreignByHandle := warp.Tweet{
		Id: "01REPLY30000000000000000000", ParentId: &parent, RootId: parent,
		UserId: "bob@m", Username: "bob@m", Text: "foreign reply by handle", CreatedAt: time.Unix(0, 0),
	}
	bt, _ := json.Marshal(warp.TweetsResponse{Tweets: []warp.Tweet{native, foreign, foreignByHandle}})
	fr := &fakeRequester{tweetsJSON: bt}
	g.Requester = fr

	w := httptest.NewRecorder()
	g.serveReplies(w, "alice", parent)

	if fr.lastRoute != warp.RouteGetTweets {
		t.Fatalf("route = %q, want %q", fr.lastRoute, warp.RouteGetTweets)
	}
	req, ok := fr.lastPayload.(warp.GetAllTweetsEvent)
	if !ok || req.ParentId != parent || req.UserId != "alice" {
		t.Fatalf("payload = %+v", fr.lastPayload)
	}

	var col orderedCollection
	if err := json.Unmarshal(w.Body.Bytes(), &col); err != nil {
		t.Fatalf("decode collection: %v", err)
	}
	if col.TotalItems != 1 || len(col.OrderedItems) != 1 {
		t.Fatalf("items = %d, want 1 (fediverse reply must be skipped)", col.TotalItems)
	}
}
