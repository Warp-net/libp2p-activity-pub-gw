// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package fediverse

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Warp-net/activity-pub-gw/internal/fediverse/fediversetest"
	"github.com/Warp-net/activity-pub-gw/internal/warp"
)

func TestDeliverFollow(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)
	actorURL := f.Actor("bob", nil)

	g.sendFollow("alice", actorURL)
	got := f.Delivered()
	if len(got) != 1 {
		t.Fatalf("deliveries = %d", len(got))
	}
	if got[0].Doc["type"] != TypeFollow || got[0].Doc["actor"] != g.ActorID("alice") {
		t.Fatalf("activity = %+v", got[0].Doc)
	}
	if got[0].Doc["object"] != actorURL {
		t.Fatalf("object = %v", got[0].Doc["object"])
	}
	if got[0].Sig == "" {
		t.Fatal("the Follow must be signed")
	}

	t.Run("an unfollow is delivered as Undo(Follow)", func(t *testing.T) {
		g.sendUndoFollow("alice", actorURL)
		got := f.Delivered()
		last := got[len(got)-1].Doc
		if last["type"] != TypeUndo {
			t.Fatalf("activity = %+v", last)
		}
		inner, _ := last["object"].(map[string]any)
		if inner == nil || inner["type"] != TypeFollow || inner["object"] != actorURL {
			t.Fatalf("undo object = %+v, want the Follow nested", last["object"])
		}
	})

	t.Run("an unresolvable inbox delivers nothing", func(t *testing.T) {
		before := len(f.Delivered())
		g.sendFollow("alice", f.URL("/users/ghost"))
		if got := len(f.Delivered()); got != before {
			t.Fatalf("deliveries = %d, want none", got-before)
		}
	})

	t.Run("a rejected delivery is logged, not fatal", func(t *testing.T) {
		f.Actor("grumpy", nil)
		f.On("/users/grumpy"+pathInbox, func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "blocked", http.StatusForbidden)
		})
		g.sendFollow("alice", f.URL("/users/grumpy")) // must not panic
	})
}

// scanFeeder serves paged user listings plus a per-user follower list.
type scanFeeder struct {
	mu        sync.Mutex
	userPages [][]byte
	call      int
	cursors   []string
	followers map[string][]byte
	fallback  []byte
}

func (s *scanFeeder) Request(route string, payload any) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch route {
	case warp.RouteGetUsers:
		if ev, ok := payload.(warp.GetAllUsersEvent); ok {
			c := ""
			if ev.Cursor != nil {
				c = *ev.Cursor
			}
			s.cursors = append(s.cursors, c)
		}
		i := s.call
		s.call++
		if i >= len(s.userPages) {
			return s.userPages[len(s.userPages)-1], nil
		}
		return s.userPages[i], nil
	case warp.RouteGetFollowers:
		if ev, ok := payload.(warp.GetFollowersEvent); ok {
			if bt, has := s.followers[string(ev.UserId)]; has {
				return bt, nil
			}
		}
		return s.fallback, nil
	}
	return []byte(`["accepted"]`), nil
}

func (s *scanFeeder) RequestUser(_, route string, payload any) ([]byte, error) {
	return s.Request(route, payload)
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	bt, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return bt
}

func TestScanPagesAndSkipsBridgedUsers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	withFollower := mustJSON(t, warp.FollowersResponse{
		Followers: []string{"bob@mastodon.social"},
	})
	empty := mustJSON(t, warp.FollowersResponse{})

	feeder := &scanFeeder{
		userPages: [][]byte{
			mustJSON(t, warp.UsersResponse{
				Users:  []warp.User{{Id: "alice"}, {Id: "bridged@m.example", Network: warp.MastodonNetwork}},
				Cursor: "p2",
			}),
			mustJSON(t, warp.UsersResponse{Users: []warp.User{{Id: "carol"}}}), // empty cursor ends the walk
		},
		followers: map[string][]byte{
			"alice": withFollower,
			"carol": withFollower,
		},
		fallback: empty,
	}

	g := testGateway(t)
	g.actorIDs = nil // force the resolver down the decode/webfinger path only
	o := NewOutboundFederation(ctx, feeder, g)
	o.scan("scanner")

	if !o.federating("alice") || !o.federating("carol") {
		t.Fatal("both pages' users with an ap: follower must be federated")
	}
	// A bridged account already lives in Mastodon; federating it would loop.
	if o.federating("bridged@m.example") {
		t.Fatal("a bridged account must never be federated back")
	}
	if len(feeder.cursors) != 2 || feeder.cursors[0] != "" || feeder.cursors[1] != "p2" {
		t.Fatalf("cursors = %v, want the second page requested with p2", feeder.cursors)
	}
}

func TestScanStopsOnARepeatedCursor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	feeder := &scanFeeder{
		userPages: [][]byte{mustJSON(t, warp.UsersResponse{Users: []warp.User{{Id: "alice"}}, Cursor: "same"})},
		fallback:  mustJSON(t, warp.FollowersResponse{}),
	}
	o := NewOutboundFederation(ctx, feeder, testGateway(t))
	o.scan("scanner")

	if feeder.call > 3 {
		t.Fatalf("user pages fetched %d times, want the walk to stop on the repeated cursor", feeder.call)
	}
}

func TestScanSurvivesBadResponses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("a transport error ends the scan quietly", func(t *testing.T) {
		o := NewOutboundFederation(ctx, errRequester{err: context.DeadlineExceeded}, testGateway(t))
		o.scan("scanner") // must not panic
		if o.federating("alice") {
			t.Fatal("nothing should have started")
		}
	})

	t.Run("a malformed listing ends the scan quietly", func(t *testing.T) {
		o := NewOutboundFederation(ctx, rawRequester{body: []byte("not json")}, testGateway(t))
		o.scan("scanner")
	})

	t.Run("an empty user page ends the walk", func(t *testing.T) {
		feeder := &scanFeeder{
			userPages: [][]byte{mustJSON(t, warp.UsersResponse{Cursor: "next"})},
			fallback:  mustJSON(t, warp.FollowersResponse{}),
		}
		o := NewOutboundFederation(ctx, feeder, testGateway(t))
		o.scan("scanner")
		if feeder.call != 1 {
			t.Fatalf("pages fetched = %d, want the walk to stop on an empty page", feeder.call)
		}
	})
}

// RunScanner must scan immediately (so federation resumes on startup) and return
// when the app context is cancelled.
func TestRunScannerScansThenStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	feeder := &scanFeeder{
		userPages: [][]byte{mustJSON(t, warp.UsersResponse{Users: []warp.User{{Id: "alice"}}})},
		followers: map[string][]byte{"alice": mustJSON(t, warp.FollowersResponse{
			Followers: []string{"bob@mastodon.social"},
		})},
		fallback: mustJSON(t, warp.FollowersResponse{}),
	}
	o := NewOutboundFederation(ctx, feeder, testGateway(t))

	done := make(chan struct{})
	go func() { defer close(done); o.RunScanner("scanner") }()

	waitFor(t, "the first scan to federate alice", func() bool { return o.federating("alice") })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runScanner did not return after its context was cancelled")
	}
}

func TestOutboundStartStopIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g := testGateway(t)
	fr := &fakeTweetsRequester{}
	o := NewOutboundFederation(ctx, fr, g)

	if o.federating("alice") {
		t.Fatal("nothing federates before start")
	}
	o.Start("")
	if o.federating("") {
		t.Fatal("an empty user must be ignored")
	}

	o.Start("alice")
	o.Start("alice") // repeated follows must not spawn duplicate pollers
	if !o.federating("alice") {
		t.Fatal("alice should be federating")
	}

	o.stop("alice")
	if o.federating("alice") {
		t.Fatal("alice should have stopped")
	}
	o.stop("alice") // stopping twice is a no-op
}
