// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package handler

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Warp-net/bridge-gateway/internal/warp"
)

type announcement struct {
	actor, object string
	undo          bool
}

type fakeMastodonRetweets struct{ announced []announcement }

func (f *fakeMastodonRetweets) Announce(_ context.Context, localUser, objectURL string, undo bool) error {
	f.announced = append(f.announced, announcement{actor: localUser, object: objectURL, undo: undo})
	return nil
}

type servesNothing struct{}

func (servesNothing) Serves(string) bool { return false }

func TestNewReTweetHandlerAnnouncesForTheRetweeter(t *testing.T) {
	booster, blank := "alice", ""
	cases := map[string]struct {
		tweet      warp.Tweet
		wantActor  string
		wantObject string
	}{
		"the booster retweets":                {warp.Tweet{Id: "https://m/1", UserId: "carol", RetweetedBy: &booster}, "alice", "https://m/1"},
		"a blank booster falls to the author": {warp.Tweet{Id: "https://m/1", UserId: "carol", RetweetedBy: &blank}, "carol", "https://m/1"},
		"no id falls to the root":             {warp.Tweet{UserId: "carol", RootId: "https://m/0"}, "carol", "https://m/0"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			mastodon := &fakeMastodonRetweets{}
			buf, err := json.Marshal(tc.tweet)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := StreamNewReTweetHandler(mastodon, servesNothing{}, nil)(context.Background(), buf); err != nil {
				t.Fatal(err)
			}
			want := announcement{actor: tc.wantActor, object: tc.wantObject}
			if len(mastodon.announced) != 1 || mastodon.announced[0] != want {
				t.Fatalf("announced = %+v, want %+v", mastodon.announced, want)
			}
		})
	}
}
