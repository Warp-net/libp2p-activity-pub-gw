// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// inboxFixture wires a gateway to an in-process peer instance whose actor "bob"
// publishes the gateway's own key, so a request signed here verifies there —
// the whole inbound path (fetch key, verify, dispatch) runs for real.
type inboxFixture struct {
	g      *gateway
	peer   *fakeInstance
	actor  string
	keyID  string
	inbox  string
	posted func() []delivery
}

func newInboxFixture(t *testing.T) *inboxFixture {
	t.Helper()
	g := testGateway(t)
	peer := newFakeInstance(t).attach(g)
	actorURL := peer.actor("bob", &g.key.PublicKey)
	return &inboxFixture{
		g: g, peer: peer, actor: actorURL,
		keyID: actorURL + "#main-key", inbox: actorURL + pathInbox,
		posted: peer.delivered,
	}
}

// post builds a signed inbound activity aimed at the gateway's shared inbox.
func (fx *inboxFixture) post(t *testing.T, doc map[string]any) *http.Request {
	t.Helper()
	return fx.postAs(t, fx.keyID, doc)
}

func (fx *inboxFixture) postAs(t *testing.T, keyID string, doc map[string]any) *http.Request {
	t.Helper()
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, "https://gw.example"+pathInbox, strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	if err := signRequest(req, keyID, fx.g.key, body); err != nil {
		t.Fatalf("sign: %v", err)
	}
	return req
}

// waitFor polls cond until it holds or the deadline passes; the inbox dispatches
// its work to goroutines bounded by the delivery semaphore.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestHandleInboxRejectsBadRequests(t *testing.T) {
	fx := newInboxFixture(t)

	t.Run("only POST is accepted", func(t *testing.T) {
		w := httptest.NewRecorder()
		fx.g.handleInbox(w, httptest.NewRequest(http.MethodGet, pathInbox, nil), "")
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d", w.Code)
		}
	})

	t.Run("an unreadable body is a bad request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, pathInbox, errReader{})
		w := httptest.NewRecorder()
		fx.g.handleInbox(w, req, "")
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d", w.Code)
		}
	})

	t.Run("an unsigned delivery is unauthorized", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, pathInbox, strings.NewReader(`{"type":"Follow"}`))
		w := httptest.NewRecorder()
		fx.g.handleInbox(w, req, "")
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d", w.Code)
		}
	})

	t.Run("a signed body that is not JSON is a bad request", func(t *testing.T) {
		body := []byte("not json")
		req, err := http.NewRequest(http.MethodPost, "https://gw.example"+pathInbox, strings.NewReader(string(body)))
		if err != nil {
			t.Fatal(err)
		}
		if err := signRequest(req, fx.keyID, fx.g.key, body); err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		fx.g.handleInbox(w, req, "")
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d", w.Code)
		}
	})

	// An authenticated peer must not be able to speak for another actor.
	t.Run("an actor that does not match the signer is forbidden", func(t *testing.T) {
		req := fx.post(t, map[string]any{
			"type": typeFollow, "actor": "https://elsewhere.example/users/mallory",
			"object": "https://gw.example/users/alice",
		})
		w := httptest.NewRecorder()
		fx.g.handleInbox(w, req, "")
		if w.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", w.Code)
		}
	})
}

func TestHandleInboxFollow(t *testing.T) {
	t.Run("accepts, delivers a signed Accept and records the follower", func(t *testing.T) {
		fx := newInboxFixture(t)
		var followed []string
		var mu sync.Mutex
		fx.g.onFollowed = func(u string) { mu.Lock(); followed = append(followed, u); mu.Unlock() }

		req := fx.post(t, map[string]any{
			"type": typeFollow, "actor": fx.actor, "object": "https://gw.example/users/alice",
		})
		w := httptest.NewRecorder()
		fx.g.handleInbox(w, req, "")
		if w.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", w.Code)
		}

		waitFor(t, "the Accept to be delivered", func() bool { return len(fx.posted()) > 0 })
		got := fx.posted()[0]
		if got.path != "/users/bob"+pathInbox {
			t.Fatalf("delivered to %q, want bob's inbox", got.path)
		}
		if got.doc["type"] != "Accept" || got.doc["actor"] != "https://gw.example/users/alice" {
			t.Fatalf("activity = %+v", got.doc)
		}
		// Accept must echo the original Follow so the peer can match it.
		obj, _ := got.doc["object"].(map[string]any)
		if obj == nil || obj["type"] != typeFollow {
			t.Fatalf("object = %+v, want the Follow echoed back", got.doc["object"])
		}
		if got.sig == "" {
			t.Fatal("the Accept must be signed")
		}

		// onFollowed is the last step of acceptFollow, so waiting on it also
		// settles the follower write that precedes it.
		waitFor(t, "outbound federation to start", func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(followed) > 0
		})
		mu.Lock()
		startedFor := append([]string(nil), followed...)
		mu.Unlock()
		if len(startedFor) != 1 || startedFor[0] != "alice" {
			t.Fatalf("onFollowed = %v, want outbound federation started for alice", startedFor)
		}
		urls, _ := fx.g.followers.List("alice")
		if len(urls) != 1 || urls[0] != fx.actor {
			t.Fatalf("followers = %v, want the remote actor recorded", urls)
		}
	})

	// Without this the shared inbox is a signing oracle and an unbounded
	// follower-state sink for attacker-chosen usernames.
	t.Run("a Follow targeting an unknown local user is refused", func(t *testing.T) {
		fx := newInboxFixture(t)
		req := fx.post(t, map[string]any{
			"type": typeFollow, "actor": fx.actor, "object": "https://gw.example/users/nobody",
		})
		w := httptest.NewRecorder()
		fx.g.handleInbox(w, req, "")
		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", w.Code)
		}
		if len(fx.posted()) != 0 {
			t.Fatal("nothing must be signed for an unhosted user")
		}
	})

	// A personal inbox names the user in the path; the object need not repeat it.
	t.Run("falls back to the inbox owner when the object names no user", func(t *testing.T) {
		fx := newInboxFixture(t)
		req := fx.post(t, map[string]any{"type": typeFollow, "actor": fx.actor})
		w := httptest.NewRecorder()
		fx.g.handleInbox(w, req, "alice")
		if w.Code != http.StatusAccepted {
			t.Fatalf("status = %d", w.Code)
		}
		waitFor(t, "the Accept", func() bool { return len(fx.posted()) > 0 })
	})

	t.Run("a saturated delivery pool asks the peer to retry", func(t *testing.T) {
		fx := newInboxFixture(t)
		for range cap(fx.g.sem) {
			fx.g.sem <- struct{}{}
		}
		req := fx.post(t, map[string]any{
			"type": typeFollow, "actor": fx.actor, "object": "https://gw.example/users/alice",
		})
		w := httptest.NewRecorder()
		fx.g.handleInbox(w, req, "")
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503 so the peer retries", w.Code)
		}
	})
}

func TestHandleInboxForwardsTranslatedActivities(t *testing.T) {
	t.Run("a Like reaches the node", func(t *testing.T) {
		fx := newInboxFixture(t)
		fr := &syncRequester{}
		fx.g.req = fr

		req := fx.post(t, map[string]any{
			"type": typeLike, "actor": fx.actor,
			"object": "https://gw.example/users/alice/statuses/t1",
		})
		w := httptest.NewRecorder()
		fx.g.handleInbox(w, req, "")
		if w.Code != http.StatusAccepted {
			t.Fatalf("status = %d", w.Code)
		}
		waitFor(t, "the favourite to be forwarded", func() bool { return fr.route() == routePostReact })
		ev, ok := fr.payload().(reactionEvent)
		if !ok {
			t.Fatalf("payload type %T", fr.lastPayload)
		}
		if string(ev.TweetId) != "t1" || string(ev.UserId) != "alice" {
			t.Fatalf("payload = %+v", ev)
		}
		if string(ev.OwnerId) != "bob@"+fx.peer.host() {
			t.Fatalf("OwnerId = %q, want the reactor's handle", ev.OwnerId)
		}
		if ev.Emoji != defaultReaction {
			t.Fatalf("Emoji = %q, want the default heart a favourite maps to", ev.Emoji)
		}
	})

	t.Run("an untranslatable activity is acknowledged, not handled", func(t *testing.T) {
		fx := newInboxFixture(t)
		fr := &syncRequester{}
		fx.g.req = fr
		req := fx.post(t, map[string]any{"type": "Flag", "actor": fx.actor})
		w := httptest.NewRecorder()
		fx.g.handleInbox(w, req, "")
		if w.Code != http.StatusAccepted {
			t.Fatalf("status = %d", w.Code)
		}
		if fr.route() != "" {
			t.Fatalf("route = %q, want nothing forwarded", fr.route())
		}
	})

	// Without a node there is nowhere to forward to; the peer must still be told
	// to stop retrying.
	t.Run("acknowledged when no node is connected", func(t *testing.T) {
		fx := newInboxFixture(t)
		req := fx.post(t, map[string]any{
			"type": typeLike, "actor": fx.actor,
			"object": "https://gw.example/users/alice/statuses/t1",
		})
		w := httptest.NewRecorder()
		fx.g.handleInbox(w, req, "")
		if w.Code != http.StatusAccepted {
			t.Fatalf("status = %d", w.Code)
		}
	})

	t.Run("a saturated pool drops the activity with 503", func(t *testing.T) {
		fx := newInboxFixture(t)
		fx.g.req = &syncRequester{}
		for range cap(fx.g.sem) {
			fx.g.sem <- struct{}{}
		}
		req := fx.post(t, map[string]any{
			"type": typeLike, "actor": fx.actor,
			"object": "https://gw.example/users/alice/statuses/t1",
		})
		w := httptest.NewRecorder()
		fx.g.handleInbox(w, req, "")
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", w.Code)
		}
	})

	// A node that rejects the forward must not turn into a 5xx for the peer:
	// the activity was accepted, the failure is ours to log.
	t.Run("a node error is logged, not surfaced", func(t *testing.T) {
		fx := newInboxFixture(t)
		fx.g.req = errRequester{err: errors.New("node down")}
		req := fx.post(t, map[string]any{
			"type": typeLike, "actor": fx.actor,
			"object": "https://gw.example/users/alice/statuses/t1",
		})
		w := httptest.NewRecorder()
		fx.g.handleInbox(w, req, "")
		if w.Code != http.StatusAccepted {
			t.Fatalf("status = %d", w.Code)
		}
	})
}

// The full wire path: a signed delivery over real HTTP through the route table,
// including the rate-limit middleware and the shared-inbox handler.
func TestSharedInboxEndToEnd(t *testing.T) {
	g := testGateway(t)
	peer := newFakeInstance(t).attach(g)
	actorURL := peer.actor("bob", &g.key.PublicKey)

	gw := httptest.NewServer(g.routes())
	defer gw.Close()

	body, err := json.Marshal(map[string]any{
		"type": typeFollow, "actor": actorURL, "object": "https://gw.example/users/alice",
	})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, gw.URL+pathInbox, strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(headerContentType, contentTypeAP)
	if err := signRequest(req, actorURL+"#main-key", g.key, body); err != nil {
		t.Fatal(err)
	}

	resp, err := gw.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		bt, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d: %s", resp.StatusCode, bt)
	}
	waitFor(t, "the Accept to be delivered", func() bool { return len(peer.delivered()) > 0 })
}

func TestAcceptFollowFailures(t *testing.T) {
	t.Run("a missing remote actor is dropped", func(t *testing.T) {
		fx := newInboxFixture(t)
		fx.g.acceptFollow("alice", "", map[string]any{"type": typeFollow})
		if len(fx.posted()) != 0 {
			t.Fatal("nothing must be delivered without an actor")
		}
	})

	t.Run("an unresolvable inbox stops before delivery", func(t *testing.T) {
		fx := newInboxFixture(t)
		fx.g.acceptFollow("alice", fx.peer.url("/users/ghost"), map[string]any{"type": typeFollow})
		if len(fx.posted()) != 0 {
			t.Fatal("unexpected delivery")
		}
		if urls, _ := fx.g.followers.List("alice"); len(urls) != 0 {
			t.Fatal("a follower must not be recorded when the Accept never went out")
		}
	})

	t.Run("a rejected Accept does not record the follower", func(t *testing.T) {
		fx := newInboxFixture(t)
		fx.peer.on("/users/bob"+pathInbox, func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "blocked domain", http.StatusForbidden)
		})
		fx.g.acceptFollow("alice", fx.actor, map[string]any{"type": typeFollow})
		if urls, _ := fx.g.followers.List("alice"); len(urls) != 0 {
			t.Fatalf("followers = %v, want none after a rejected Accept", urls)
		}
	})

	t.Run("a store failure still counts as accepted", func(t *testing.T) {
		fx := newInboxFixture(t)
		fx.g.followers = errFollowerStore{err: errors.New("node down")}
		var followed []string
		fx.g.onFollowed = func(u string) { followed = append(followed, u) }

		fx.g.acceptFollow("alice", fx.actor, map[string]any{"type": typeFollow})
		if len(fx.posted()) != 1 {
			t.Fatalf("deliveries = %d, want the Accept out", len(fx.posted()))
		}
		if len(followed) != 1 {
			t.Fatalf("onFollowed = %v, want federation started anyway", followed)
		}
	})
}

func TestHandleDeleteAcknowledges(t *testing.T) {
	fx := newInboxFixture(t)
	fr := &syncRequester{}
	fx.g.req = fr
	req := fx.post(t, map[string]any{
		"type": typeDelete, "actor": fx.actor,
		"object": map[string]any{"id": "https://m/notes/1", "type": typeTombstone},
	})
	w := httptest.NewRecorder()
	fx.g.handleInbox(w, req, "")
	// Deletion is an owner-only route, so the gateway acknowledges to stop peer
	// retries rather than pretending to act.
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d", w.Code)
	}
	if fr.route() != "" {
		t.Fatalf("route = %q, want no forward", fr.route())
	}
}

func TestStringField(t *testing.T) {
	m := map[string]any{
		"str": "https://m/1",
		"obj": map[string]any{"id": "https://m/2"},
		"bad": map[string]any{"type": "Note"},
		"num": 3,
	}
	cases := map[string]string{
		"str":     "https://m/1",
		"obj":     "https://m/2",
		"bad":     "",
		"num":     "",
		"missing": "",
	}
	for key, want := range cases {
		if got := stringField(m, key); got != want {
			t.Errorf("stringField(%q) = %q, want %q", key, got, want)
		}
	}
}

// errReader fails on the first read, standing in for a truncated connection.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("broken pipe") }

// syncRequester records the last forwarded route/payload. The inbox forwards on
// its own goroutine, so the recorder a test polls must be synchronised.
type syncRequester struct {
	mu          sync.Mutex
	lastRoute   string
	lastPayload any
}

func (s *syncRequester) request(route string, payload any) ([]byte, error) {
	s.mu.Lock()
	s.lastRoute, s.lastPayload = route, payload
	s.mu.Unlock()
	return []byte(`["accepted"]`), nil
}

func (s *syncRequester) requestUser(_, route string, payload any) ([]byte, error) {
	return s.request(route, payload)
}

func (s *syncRequester) route() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRoute
}

func (s *syncRequester) payload() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastPayload
}
