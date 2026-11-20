// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package handler

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Warp-net/bridge-gateway/internal/warp"
)

type servesAll struct{ FollowsFetcher }

func (servesAll) Serves(string) bool { return true }

type recordedFollow struct {
	follower, handle string
	undo             bool
}

type fakeFollowRecorder struct{ follows []recordedFollow }

func (f *fakeFollowRecorder) SetFollow(follower, handle string, undo bool) {
	f.follows = append(f.follows, recordedFollow{follower: follower, handle: handle, undo: undo})
}

type fakeFollowPublisher struct{ done chan recordedFollow }

func (f fakeFollowPublisher) Follow(actor, handle string, undo bool) {
	f.done <- recordedFollow{follower: actor, handle: handle, undo: undo}
}

func TestFollowHandlersRecordANostrFollowForTheLiveFeed(t *testing.T) {
	ev := warp.NewFollowEvent{FollowerId: "01WARPUSER", FollowingId: "npub@nostr.com"}
	buf, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	for _, undo := range []bool{false, true} {
		live := &fakeFollowRecorder{}
		pub := fakeFollowPublisher{done: make(chan recordedFollow, 1)}
		h := StreamFollowHandler(nil, servesAll{}, pub, live)
		if undo {
			h = StreamUnfollowHandler(nil, servesAll{}, pub, live)
		}
		if _, err := h(context.Background(), buf); err != nil {
			t.Fatal(err)
		}
		want := recordedFollow{follower: ev.FollowerId, handle: ev.FollowingId, undo: undo}
		if len(live.follows) != 1 || live.follows[0] != want {
			t.Fatalf("recorded = %+v, want %+v", live.follows, want)
		}
		if got := <-pub.done; got != want {
			t.Fatalf("published = %+v, want %+v", got, want)
		}
	}
}
