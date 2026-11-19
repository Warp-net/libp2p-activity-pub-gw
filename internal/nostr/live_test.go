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
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/Warp-net/activity-pub-gw/internal/nostr/nostrtest"
	"github.com/Warp-net/activity-pub-gw/internal/warp"
)

type pushed struct {
	follower string
	t        warp.Tweet
}

// pushRecorder stands in for the follower nodes.
type pushRecorder struct {
	mu   sync.Mutex
	got  []pushed
	mark int
}

func (r *pushRecorder) push(f string, tw warp.Tweet) error {
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
	relay *nostrtest.Relay
	pk    string
}

func (w liveWatch) ready() bool { return w.relay.LiveFor(w.pk) }

func TestNostrLivePushesFollowedNotes(t *testing.T) {
	relay := nostrtest.NewRelay(t)
	alice, bob, carol, stranger := nostrtest.NewKey(), nostrtest.NewKey(), nostrtest.NewKey(), nostrtest.NewKey()
	relay.Add(alice.Sign(t, kindFollowList, "", time.Now().Unix(), []string{"p", bob.PK}))
	b := testNostrBridge(t, relay)

	rec := &pushRecorder{}
	live := NewLive(b, rec.push)
	live.resubscribeDelay = 10 * time.Millisecond
	b.Trusted = live.Trusted

	live.SetFollow("01WARPUSER", Handle(alice.PK), false)
	live.SetFollow("01WARPUSER", "alice@damus.io", false)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go live.Run(ctx)
	nostrtest.WaitFor(t, "live subscription", liveWatch{relay, alice.PK}.ready)

	if !live.Trusted(alice.PK) || !live.Trusted(bob.PK) || live.Trusted(stranger.PK) {
		t.Fatal("trust must cover followed authors and their followings only")
	}

	at := time.Now().Unix() + 1
	relay.Add(
		alice.Sign(t, KindNote, "live note", at),
		alice.Sign(t, KindNote, "live reply", at, []string{"e", hexID('a'), "", "root"}),
		stranger.Sign(t, KindNote, "not followed", at),
	)
	nostrtest.WaitFor(t, "push", rec.any)
	time.Sleep(100 * time.Millisecond)
	p := rec.all()
	if len(p) != 1 || p[0].follower != "01WARPUSER" || p[0].t.Text != "live note" || p[0].t.UserId != Handle(alice.PK) {
		t.Fatalf("pushes = %+v, want only the followed author's top-level note", p)
	}

	for range 2 * nostrAuthorBurst {
		relay.Add(alice.Sign(t, KindNote, "flood "+hexID('f')[:4]+time.Now().String(), at))
	}
	time.Sleep(200 * time.Millisecond)
	if n := len(rec.all()); n > nostrAuthorBurst {
		t.Fatalf("%d pushes, want the per-author limit (%d) to cap a flood", n, nostrAuthorBurst)
	}

	live.SetFollow("01OTHER", Handle(carol.PK), false)
	nostrtest.WaitFor(t, "resubscription", liveWatch{relay, carol.PK}.ready)
	rec.setMark()
	relay.Add(carol.Sign(t, KindNote, "carol note", time.Now().Unix()+1))
	nostrtest.WaitFor(t, "carol push", rec.pastMark)

	live.SetFollow("01WARPUSER", Handle(alice.PK), true)
	if len(live.FollowersOf(alice.PK)) != 0 {
		t.Fatal("unfollow must drop the follower")
	}
}

// nostrScanRequester serves a users page and per-user followings for the scan.
type nostrScanRequester struct {
	users      []warp.User
	followings map[string][]string
}

func (s nostrScanRequester) Request(route string, _ any) ([]byte, error) {
	return json.Marshal(warp.UsersResponse{Users: s.users})
}

func (s nostrScanRequester) RequestUser(userID, route string, _ any) ([]byte, error) {
	return json.Marshal(warp.FollowingsResponse{FollowerId: userID, Followings: s.followings[userID]})
}

func TestNostrLiveScanRebuildsFollows(t *testing.T) {
	alice, bob := nostrtest.NewKey(), nostrtest.NewKey()
	live := NewLive(testNostrBridge(t), nil)
	live.scan(nostrScanRequester{
		users: []warp.User{{Id: "01NATIVE"}, {Id: Handle(alice.PK), Network: warp.NostrNetwork}, {Id: "01EMPTY"}},
		followings: map[string][]string{
			"01NATIVE": {Handle(alice.PK), "bob@mastodon.social", "01WARP"},
			"01EMPTY":  {},
		},
	}, warp.DefaultOwnerHandle)
	if f := live.FollowersOf(alice.PK); len(f) != 1 || f[0] != "01NATIVE" {
		t.Fatalf("followers of alice = %v", f)
	}
	live.SetFollow("01NATIVE", Handle(bob.PK), false)
	live.scan(nostrScanRequester{
		users:      []warp.User{{Id: "01NATIVE"}},
		followings: map[string][]string{"01NATIVE": {Handle(bob.PK)}},
	}, warp.DefaultOwnerHandle)
	if len(live.FollowersOf(alice.PK)) != 0 || len(live.FollowersOf(bob.PK)) != 1 {
		t.Fatal("a scan must replace what it read for a user")
	}
}
