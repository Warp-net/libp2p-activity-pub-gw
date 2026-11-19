// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package fediverse

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Warp-net/activity-pub-gw/internal/warp"
)

// The poller must seed existing tweets at startup so a restart doesn't replay
// history to every Fediverse follower, then federate only what arrives after.
func TestTweetPollerRunSeedsThenFederates(t *testing.T) {
	fr := &liveTweets{tweets: []warp.Tweet{{Id: "old", UserId: "alice"}}}
	var mu sync.Mutex
	var published []string

	p := newTweetPoller(fr, "alice", func(_ context.Context, _ string, tw warp.Tweet) {
		mu.Lock()
		published = append(published, tw.Id)
		mu.Unlock()
	})
	p.interval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.run(ctx) }()

	waitFor(t, "the existing tweet to be seeded", func() bool { return p.seen.Contains("old") })

	fr.add(warp.Tweet{Id: "new", UserId: "alice"})
	waitFor(t, "the new tweet to be federated", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(published) > 0
	})

	mu.Lock()
	got := append([]string(nil), published...)
	mu.Unlock()
	for _, id := range got {
		if id == "old" {
			t.Fatal("history was replayed; every follower would be re-notified on restart")
		}
	}
	if got[0] != "new" {
		t.Fatalf("published = %v", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after its context was cancelled")
	}
}

func TestTweetPollerFetchFailures(t *testing.T) {
	t.Run("a transport error yields no tweets", func(t *testing.T) {
		p := newTweetPoller(errRequester{err: errors.New("down")}, "alice", nil)
		if got := p.fetch(); got != nil {
			t.Fatalf("tweets = %v", got)
		}
	})

	t.Run("a malformed response yields no tweets", func(t *testing.T) {
		p := newTweetPoller(rawRequester{body: []byte("not json")}, "alice", nil)
		if got := p.fetch(); got != nil {
			t.Fatalf("tweets = %v", got)
		}
	})

	// A failed poll must not publish anything, so a blip can't fan garbage out.
	t.Run("a failed poll publishes nothing", func(t *testing.T) {
		var published int
		p := newTweetPoller(errRequester{err: errors.New("down")}, "alice",
			func(context.Context, string, warp.Tweet) { published++ })
		p.poll(context.Background())
		if published != 0 {
			t.Fatalf("published = %d", published)
		}
	})
}

// An id still present in the feed keeps its TTL refreshed, so expiry can never
// cause an old post to be federated a second time.
func TestTweetPollerRefreshesSeenIDs(t *testing.T) {
	fr := &fakeTweetsRequester{tweets: []warp.Tweet{{Id: "t1", UserId: "alice"}}}
	var published int
	p := newTweetPoller(fr, "alice", func(context.Context, string, warp.Tweet) { published++ })

	p.poll(context.Background())
	if published != 1 {
		t.Fatalf("first poll published %d, want 1", published)
	}
	for range 3 {
		p.poll(context.Background())
	}
	if published != 1 {
		t.Fatalf("published = %d, want no re-publish while the id stays in the feed", published)
	}
	if !p.seen.Contains("t1") {
		t.Fatal("the id fell out of the seen set")
	}
}

func TestFollowPollerErrors(t *testing.T) {
	newPoller := func(req warp.Requester) *followPoller {
		return newFollowPoller(req, stubResolver{}, "owner", func(string) {}, func(string) {})
	}

	t.Run("a transport error propagates", func(t *testing.T) {
		boom := errors.New("down")
		if err := newPoller(errRequester{err: boom}).poll(); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
	})

	// An error envelope decodes as an empty following list, which would look like
	// the owner unfollowed everyone.
	t.Run("a node error envelope propagates", func(t *testing.T) {
		p := newPoller(rawRequester{body: []byte(`{"code":500,"message":"node down"}`)})
		if err := p.poll(); err == nil {
			t.Fatal("expected the error envelope to be reported")
		}
	})

	t.Run("a malformed response propagates", func(t *testing.T) {
		if err := newPoller(rawRequester{body: []byte("[[[")}).poll(); err == nil {
			t.Fatal("expected a decode error")
		}
	})
}

// A read failure is far likelier than every follow vanishing at once, so an
// empty round must be skipped rather than mass-unfollowing on remote instances.
func TestFollowPollerSkipsAnEmptyBlip(t *testing.T) {
	fr := &fakeRequester{}
	var unfollowed []string
	p := newFollowPoller(fr, stubResolver{}, "owner",
		func(string) {}, func(a string) { unfollowed = append(unfollowed, a) })

	fr.followingsJSON = mustJSON(t, warp.FollowingsResponse{
		Followings: []string{encodeActorID("https://m/users/bob")},
	})
	if err := p.poll(); err != nil { // baseline
		t.Fatal(err)
	}

	fr.followingsJSON = mustJSON(t, warp.FollowingsResponse{})
	if err := p.poll(); err != nil {
		t.Fatal(err)
	}
	if len(unfollowed) != 0 {
		t.Fatalf("unfollowed = %v, want the empty round skipped", unfollowed)
	}
	if len(p.known) != 1 {
		t.Fatalf("known = %v, want the baseline kept", p.known)
	}
}

// An unresolvable following must be skipped, not treated as an unfollow.
func TestFollowPollerSkipsUnresolvableFollowings(t *testing.T) {
	fr := &fakeRequester{}
	var followed []string
	p := newFollowPoller(fr, stubResolver{}, "owner",
		func(a string) { followed = append(followed, a) }, func(string) {})

	fr.followingsJSON = mustJSON(t, warp.FollowingsResponse{Followings: []string{"@"}})
	if err := p.poll(); err != nil {
		t.Fatal(err)
	}
	if len(p.known) != 0 {
		t.Fatalf("known = %v, want the unresolvable id skipped", p.known)
	}
	if len(followed) != 0 {
		t.Fatalf("followed = %v", followed)
	}
}

func TestFollowPollerRunStops(t *testing.T) {
	fr := &countingRequester{body: mustJSON(t, warp.FollowingsResponse{})}
	p := newFollowPoller(fr, stubResolver{}, "owner", func(string) {}, func(string) {})
	p.interval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.run(ctx) }()
	waitFor(t, "the first poll", func() bool { return fr.count() > 0 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after its context was cancelled")
	}
}

// liveTweets is a tweet feed a test can extend while the poller reads it.
type liveTweets struct {
	mu     sync.Mutex
	tweets []warp.Tweet
}

func (l *liveTweets) add(t warp.Tweet) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tweets = append(l.tweets, t)
}

func (l *liveTweets) Request(route string, _ any) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if route == warp.RouteGetTweets {
		return json.Marshal(warp.TweetsResponse{Tweets: l.tweets})
	}
	return []byte(`["accepted"]`), nil
}

func (l *liveTweets) RequestUser(_, route string, payload any) ([]byte, error) {
	return l.Request(route, payload)
}

// countingRequester answers with a fixed body and counts the calls.
type countingRequester struct {
	body []byte
	n    atomic.Int32
}

func (c *countingRequester) Request(string, any) ([]byte, error) {
	c.n.Add(1)
	return c.body, nil
}

func (c *countingRequester) RequestUser(_, route string, payload any) ([]byte, error) {
	return c.Request(route, payload)
}

func (c *countingRequester) count() int { return int(c.n.Load()) }
