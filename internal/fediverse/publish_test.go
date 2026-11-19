// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package fediverse

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Warp-net/activity-pub-gw/internal/fediverse/fediversetest"
	"github.com/Warp-net/activity-pub-gw/internal/warp"
)

func TestDeliverToFollowersFansOut(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)
	bob := f.Actor("bob", nil)
	carol := f.Actor("carol", nil)
	for _, a := range []string{bob, carol} {
		if err := g.Followers.Add("alice", a); err != nil {
			t.Fatal(err)
		}
	}

	g.deliverToFollowers(context.Background(), "alice", activity{Type: TypeCreate}, "test")
	got := f.Delivered()
	if len(got) != 2 {
		t.Fatalf("deliveries = %d, want one per follower", len(got))
	}
	paths := map[string]bool{}
	for _, d := range got {
		paths[d.Path] = true
	}
	if !paths["/users/bob"+pathInbox] || !paths["/users/carol"+pathInbox] {
		t.Fatalf("delivered to %v", paths)
	}
}

func TestDeliverToFollowersEdgeCases(t *testing.T) {
	t.Run("no followers delivers nothing", func(t *testing.T) {
		g := testGateway(t)
		f := attachFake(fediversetest.NewInstance(t), g)
		g.deliverToFollowers(context.Background(), "alice", activity{Type: TypeCreate}, "test")
		if len(f.Delivered()) != 0 {
			t.Fatal("unexpected delivery")
		}
	})

	t.Run("a store failure aborts the fan-out", func(t *testing.T) {
		g := testGateway(t)
		f := attachFake(fediversetest.NewInstance(t), g)
		g.Followers = errFollowerStore{err: errors.New("node down")}
		g.deliverToFollowers(context.Background(), "alice", activity{Type: TypeCreate}, "test")
		if len(f.Delivered()) != 0 {
			t.Fatal("unexpected delivery")
		}
	})

	t.Run("one unreachable follower does not block the others", func(t *testing.T) {
		g := testGateway(t)
		f := attachFake(fediversetest.NewInstance(t), g)
		good := f.Actor("good", nil)
		if err := g.Followers.Add("alice", f.URL("/users/ghost")); err != nil {
			t.Fatal(err)
		}
		if err := g.Followers.Add("alice", good); err != nil {
			t.Fatal(err)
		}
		g.deliverToFollowers(context.Background(), "alice", activity{Type: TypeCreate}, "test")
		got := f.Delivered()
		if len(got) != 1 || got[0].Path != "/users/good"+pathInbox {
			t.Fatalf("delivered = %+v, want only the reachable follower", got)
		}
	})

	t.Run("a rejected inbox is logged, not fatal", func(t *testing.T) {
		g := testGateway(t)
		f := attachFake(fediversetest.NewInstance(t), g)
		f.Actor("grumpy", nil)
		f.On("/users/grumpy"+pathInbox, func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "no", http.StatusForbidden)
		})
		if err := g.Followers.Add("alice", f.URL("/users/grumpy")); err != nil {
			t.Fatal(err)
		}
		g.deliverToFollowers(context.Background(), "alice", activity{Type: TypeCreate}, "test")
	})

	// A cancelled context must stop queueing new deliveries and wait out the
	// in-flight ones rather than leaking goroutines.
	t.Run("a cancelled context stops the fan-out", func(t *testing.T) {
		g := testGateway(t)
		f := attachFake(fediversetest.NewInstance(t), g)
		for _, name := range []string{"a", "b", "c"} {
			if err := g.Followers.Add("alice", f.Actor(name, nil)); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		done := make(chan struct{})
		go func() {
			defer close(done)
			g.deliverToFollowers(ctx, "alice", activity{Type: TypeCreate}, "test")
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("deliverToFollowers did not return on a cancelled context")
		}
	})
}

func TestPublishNoteBuildsACreate(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)
	if err := g.Followers.Add("alice", f.Actor("bob", nil)); err != nil {
		t.Fatal(err)
	}

	g.publishNote(context.Background(), "alice", warp.Tweet{
		Id: "t1", UserId: "alice", Text: "hello", CreatedAt: time.Unix(0, 0),
	})
	got := f.Delivered()
	if len(got) != 1 {
		t.Fatalf("deliveries = %d", len(got))
	}
	if got[0].Doc["type"] != TypeCreate {
		t.Fatalf("activity = %+v", got[0].Doc)
	}
	n, _ := got[0].Doc["object"].(map[string]any)
	if n == nil || n["id"] != g.ActorID("alice")+PathStatuses+"t1" {
		t.Fatalf("note = %+v, want a deterministic status id", n)
	}
	// The activity id is derived from the note id so a duplicate delivery from
	// the poller is deduplicated by the peer.
	if got[0].Doc["id"] != g.ActorID("alice")+PathStatuses+"t1/activity" {
		t.Fatalf("activity id = %v", got[0].Doc["id"])
	}
}

func TestFederateTweetAsyncDeliversWithoutBlocking(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)
	if err := g.Followers.Add("alice", f.Actor("bob", nil)); err != nil {
		t.Fatal(err)
	}

	// The libp2p gossip ack must not wait on Fediverse delivery.
	g.FederateTweetAsync(warp.Tweet{Id: "t9", UserId: "alice", Text: "gossiped", CreatedAt: time.Unix(0, 0)})
	waitFor(t, "the gossiped tweet to be federated", func() bool { return len(f.Delivered()) == 1 })

	n, _ := f.Delivered()[0].Doc["object"].(map[string]any)
	if n == nil || n["id"] != g.ActorID("alice")+PathStatuses+"t9" {
		t.Fatalf("note = %+v", n)
	}
}

func TestSendActorUpdate(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)
	if err := g.Followers.Add("alice", f.Actor("bob", nil)); err != nil {
		t.Fatal(err)
	}

	g.sendActorUpdate(context.Background(), "alice")
	got := f.Delivered()
	if len(got) != 1 {
		t.Fatalf("deliveries = %d", len(got))
	}
	if got[0].Doc["type"] != typeUpdate {
		t.Fatalf("activity = %+v", got[0].Doc)
	}
	// Mastodon only re-fetches a remote actor on an Update, so the refreshed
	// profile must ride along in the object.
	obj, _ := got[0].Doc["object"].(map[string]any)
	if obj == nil || obj["type"] != "Person" || obj["preferredUsername"] != "alice" {
		t.Fatalf("object = %+v", got[0].Doc["object"])
	}
	pk, _ := obj["publicKey"].(map[string]any)
	if pk == nil || !strings.Contains(pk["publicKeyPem"].(string), "BEGIN PUBLIC KEY") {
		t.Fatalf("publicKey = %+v", obj["publicKey"])
	}

	t.Run("an unknown user pushes nothing", func(t *testing.T) {
		before := len(f.Delivered())
		g.sendActorUpdate(context.Background(), "nobody")
		if got := len(f.Delivered()); got != before {
			t.Fatal("an unresolvable user must not produce an Update")
		}
	})
}

// The delivery semaphore is the only bound on concurrent outbound requests, so a
// wide fan-out must never exceed it.
func TestDeliveryFanOutRespectsTheSemaphore(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)

	var mu sync.Mutex
	var inFlight, peak int
	for i := range 12 {
		name := "f" + string(rune('a'+i))
		actorURL := f.Actor(name, nil)
		f.On("/users/"+name+pathInbox, func(w http.ResponseWriter, _ *http.Request) {
			mu.Lock()
			inFlight++
			if inFlight > peak {
				peak = inFlight
			}
			mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			mu.Lock()
			inFlight--
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		})
		if err := g.Followers.Add("alice", actorURL); err != nil {
			t.Fatal(err)
		}
	}

	g.deliverToFollowers(context.Background(), "alice", activity{Type: TypeCreate}, "test")
	mu.Lock()
	defer mu.Unlock()
	if peak > cap(g.sem) {
		t.Fatalf("peak concurrency = %d, want at most the semaphore's %d", peak, cap(g.sem))
	}
	if peak == 0 {
		t.Fatal("nothing was delivered")
	}
}
