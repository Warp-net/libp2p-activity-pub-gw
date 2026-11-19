// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package fediverse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Warp-net/activity-pub-gw/internal/fediverse/fediversetest"
	"github.com/Warp-net/activity-pub-gw/internal/netguard"
	"github.com/Warp-net/activity-pub-gw/internal/warp"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

// getJSON fetches path off srv and decodes the body into v.
func getJSON(t *testing.T, srv *httptest.Server, path string, v any) *http.Response {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if v != nil {
		if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
	}
	return resp
}

func TestHandleInstanceActor(t *testing.T) {
	g := testGateway(t)
	srv := httptest.NewServer(g.Routes())
	defer srv.Close()

	var a actor
	resp := getJSON(t, srv, pathActor, &a)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get(HeaderContentType); ct != ContentTypeAP {
		t.Fatalf("content-type = %q", ct)
	}
	// Peers re-fetch the instance actor to verify every signed request; without a
	// cache hint that becomes a fetch storm against /actor.
	if cc := resp.Header.Get("Cache-Control"); cc != "max-age=3600" {
		t.Fatalf("cache-control = %q", cc)
	}
	if a.Type != "Application" || a.PreferredUsername != instanceActorName {
		t.Fatalf("actor = %+v", a)
	}
	if a.ID != g.instanceActorID() || a.PublicKey.ID != g.instanceKeyID() {
		t.Fatalf("ids: %q / %q", a.ID, a.PublicKey.ID)
	}
	if !strings.Contains(a.PublicKey.PublicKeyPEM, "BEGIN PUBLIC KEY") {
		t.Fatal("the instance actor must publish the gateway signing key")
	}
	if a.Endpoints == nil || a.Endpoints.SharedInbox != g.baseURL()+pathInbox {
		t.Fatalf("endpoints = %+v", a.Endpoints)
	}
}

// The keyId actor must be webfinger-resolvable or peers re-resolve it on every
// signed request.
func TestWebFingerResolvesTheInstanceActor(t *testing.T) {
	g := testGateway(t)
	srv := httptest.NewServer(g.Routes())
	defer srv.Close()

	var jrd webFingerJRD
	resp := getJSON(t, srv, "/.well-known/webfinger?resource=acct:"+instanceActorName+"@gw.example", &jrd)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(jrd.Links) != 1 || jrd.Links[0].Href != g.instanceActorID() {
		t.Fatalf("jrd = %+v", jrd)
	}
}

func TestWebFingerRejectsBadResources(t *testing.T) {
	srv := httptest.NewServer(testGateway(t).Routes())
	defer srv.Close()

	cases := map[string]int{
		"/.well-known/webfinger?resource=alice":           http.StatusBadRequest, // no @
		"/.well-known/webfinger":                          http.StatusBadRequest,
		"/.well-known/webfinger?resource=acct:alice@else": http.StatusNotFound, // another domain
	}
	for path, want := range cases {
		resp := getJSON(t, srv, path, nil)
		if resp.StatusCode != want {
			t.Errorf("%s = %d, want %d", path, resp.StatusCode, want)
		}
	}
}

func TestHandleInstanceActorSub(t *testing.T) {
	g := testGateway(t)
	srv := httptest.NewServer(g.Routes())
	defer srv.Close()

	for _, sub := range []string{"outbox", "followers", "following"} {
		var col orderedCollection
		resp := getJSON(t, srv, pathActor+"/"+sub, &col)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d", sub, resp.StatusCode)
		}
		if col.Type != "OrderedCollection" || col.TotalItems != 0 || len(col.OrderedItems) != 0 {
			t.Fatalf("%s = %+v, want an empty collection", sub, col)
		}
		if col.ID != g.baseURL()+pathActor+"/"+sub {
			t.Fatalf("%s id = %q", sub, col.ID)
		}
	}

	if resp := getJSON(t, srv, pathActor+"/nope", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown sub-collection = %d, want 404", resp.StatusCode)
	}
}

func TestHandleUsersDispatch(t *testing.T) {
	g := testGateway(t)
	srv := httptest.NewServer(g.Routes())
	defer srv.Close()

	cases := map[string]int{
		"/users/alice":            http.StatusOK,
		"/users/alice/":           http.StatusOK, // trailing slash is the actor too
		"/users/alice/outbox":     http.StatusOK,
		"/users/alice/followers":  http.StatusOK,
		"/users/alice/following":  http.StatusOK,
		"/users/bob":              http.StatusNotFound, // unknown user
		"/users/alice/nope":       http.StatusNotFound,
		"/users/alice/statuses":   http.StatusNotFound, // no status id
		"/users/alice/statuses/":  http.StatusNotFound,
		"/users/alice/statuses/1": http.StatusNotFound, // no node wired
		"/users/alice/inbox":      http.StatusMethodNotAllowed,
	}
	for path, want := range cases {
		resp := getJSON(t, srv, path, nil)
		if resp.StatusCode != want {
			t.Errorf("GET %s = %d, want %d", path, resp.StatusCode, want)
		}
	}
}

func TestServeFollowers(t *testing.T) {
	g := testGateway(t)
	if err := g.Followers.Add("alice", "https://m.example/users/bob"); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(g.Routes())
	defer srv.Close()

	var col orderedCollection
	getJSON(t, srv, "/users/alice/followers", &col)
	if col.TotalItems != 1 || len(col.OrderedItems) != 1 {
		t.Fatalf("collection = %+v", col)
	}
	if col.OrderedItems[0] != "https://m.example/users/bob" {
		t.Fatalf("item = %v", col.OrderedItems[0])
	}
	if col.ID != g.ActorID("alice")+PathFollowers {
		t.Fatalf("id = %q", col.ID)
	}
}

// errFollowerStore fails every read, standing in for an unreachable owner node.
type errFollowerStore struct{ err error }

func (e errFollowerStore) Add(string, string) error      { return e.err }
func (e errFollowerStore) List(string) ([]string, error) { return nil, e.err }

func TestServeFollowersToleratesAStoreError(t *testing.T) {
	g := testGateway(t)
	g.Followers = errFollowerStore{err: errors.New("node down")}
	w := httptest.NewRecorder()
	g.serveFollowers(w, "alice")

	var col orderedCollection
	if err := json.Unmarshal(w.Body.Bytes(), &col); err != nil {
		t.Fatal(err)
	}
	if col.TotalItems != 0 || col.OrderedItems == nil {
		t.Fatalf("collection = %+v, want an empty (never null) list", col)
	}
}

func TestServeOutbox(t *testing.T) {
	parent := "01PARENT0000000000000000000"
	by := "bob@m"
	tweets := []warp.Tweet{
		{Id: "t1", UserId: "alice", CreatedAt: time.Unix(0, 0)},                    // publishable
		{Id: "t2", UserId: "alice", ParentId: &parent, CreatedAt: time.Unix(0, 0)}, // reply: filtered
		{Id: "t3", UserId: "alice", RetweetedBy: &by, CreatedAt: time.Unix(0, 0)},  // boost: filtered
		{Id: "t4", UserId: "carol", CreatedAt: time.Unix(0, 0)},                    // someone else's
	}

	t.Run("inlines only original top-level posts", func(t *testing.T) {
		g := testGateway(t)
		bt, _ := json.Marshal(warp.TweetsResponse{Tweets: tweets})
		g.Requester = &fakeRequester{tweetsJSON: bt}

		w := httptest.NewRecorder()
		g.serveOutbox(w, "alice")
		var col orderedCollection
		if err := json.Unmarshal(w.Body.Bytes(), &col); err != nil {
			t.Fatal(err)
		}
		// totalItems must match what is actually served, or Mastodon shows
		// "Posts: N" over a short tab.
		if col.TotalItems != 1 || len(col.OrderedItems) != 1 {
			t.Fatalf("collection = %d items / totalItems %d, want 1", len(col.OrderedItems), col.TotalItems)
		}
		item, _ := json.Marshal(col.OrderedItems[0])
		var act activity
		if err := json.Unmarshal(item, &act); err != nil {
			t.Fatal(err)
		}
		if act.Type != TypeCreate || act.Actor != g.ActorID("alice") {
			t.Fatalf("item = %+v, want a Create by alice", act)
		}
	})

	t.Run("no node serves an empty collection", func(t *testing.T) {
		g := testGateway(t)
		w := httptest.NewRecorder()
		g.serveOutbox(w, "alice")
		var col orderedCollection
		if err := json.Unmarshal(w.Body.Bytes(), &col); err != nil {
			t.Fatal(err)
		}
		if col.TotalItems != 0 || col.ID != g.ActorID("alice")+"/outbox" {
			t.Fatalf("collection = %+v", col)
		}
	})

	t.Run("a node error serves an empty collection", func(t *testing.T) {
		g := testGateway(t)
		g.Requester = errRequester{err: errors.New("unreachable")}
		w := httptest.NewRecorder()
		g.serveOutbox(w, "alice")
		var col orderedCollection
		if err := json.Unmarshal(w.Body.Bytes(), &col); err != nil {
			t.Fatal(err)
		}
		if col.TotalItems != 0 {
			t.Fatalf("collection = %+v", col)
		}
	})

	t.Run("a malformed node response serves an empty collection", func(t *testing.T) {
		g := testGateway(t)
		g.Requester = rawRequester{body: []byte("not json")}
		w := httptest.NewRecorder()
		g.serveOutbox(w, "alice")
		var col orderedCollection
		if err := json.Unmarshal(w.Body.Bytes(), &col); err != nil {
			t.Fatal(err)
		}
		if col.TotalItems != 0 {
			t.Fatalf("collection = %+v", col)
		}
	})
}

func TestServeFollowing(t *testing.T) {
	t.Run("warpnet follows resolve locally, fediverse follows to their actor url", func(t *testing.T) {
		g := testGateway(t)
		g.actorIDs = expirable.NewLRU[string, string](actorIDsSize, nil, actorIDsTTL)
		g.actorIDs.Add("bob@m.example", "https://m.example/users/bob")
		bt, _ := json.Marshal(warp.FollowingsResponse{Followings: []string{
			"01KSGHBHKG0N77T6A3RZV8WSH5", // native warpnet user
			"bob@m.example",              // bridged handle, resolved via the cache
			"@",                          // unresolvable id: skipped, not fatal
		}})
		g.Requester = &fakeRequester{followingsJSON: bt}

		w := httptest.NewRecorder()
		g.serveFollowing(w, "alice")
		var col orderedCollection
		if err := json.Unmarshal(w.Body.Bytes(), &col); err != nil {
			t.Fatal(err)
		}
		if col.TotalItems != 2 {
			t.Fatalf("items = %v, want the native and the resolvable bridged follow", col.OrderedItems)
		}
		if col.OrderedItems[0] != g.ActorID("01KSGHBHKG0N77T6A3RZV8WSH5") {
			t.Fatalf("native follow = %v, want our own actor url", col.OrderedItems[0])
		}
		if col.OrderedItems[1] != "https://m.example/users/bob" {
			t.Fatalf("bridged follow = %v", col.OrderedItems[1])
		}
	})

	t.Run("no node, node error and bad json all serve an empty collection", func(t *testing.T) {
		for name, g := range map[string]*Gateway{
			"no node":  testGateway(t),
			"error":    testGateway(t),
			"bad json": testGateway(t),
		} {
			switch name {
			case "error":
				g.Requester = errRequester{err: errors.New("x")}
			case "bad json":
				g.Requester = rawRequester{body: []byte("{{{")}
			}
			w := httptest.NewRecorder()
			g.serveFollowing(w, "alice")
			var col orderedCollection
			if err := json.Unmarshal(w.Body.Bytes(), &col); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if col.TotalItems != 0 {
				t.Fatalf("%s: collection = %+v", name, col)
			}
		}
	})
}

func TestServeRepliesWithoutANode(t *testing.T) {
	g := testGateway(t)
	w := httptest.NewRecorder()
	g.serveReplies(w, "alice", "t1")
	var col orderedCollection
	if err := json.Unmarshal(w.Body.Bytes(), &col); err != nil {
		t.Fatal(err)
	}
	if col.TotalItems != 0 || col.ID != g.ActorID("alice")+PathStatuses+"t1/replies" {
		t.Fatalf("collection = %+v", col)
	}

	t.Run("a node error serves empty", func(t *testing.T) {
		g := testGateway(t)
		g.Requester = errRequester{err: errors.New("x")}
		w := httptest.NewRecorder()
		g.serveReplies(w, "alice", "t1")
		var col orderedCollection
		if err := json.Unmarshal(w.Body.Bytes(), &col); err != nil {
			t.Fatal(err)
		}
		if col.TotalItems != 0 {
			t.Fatalf("collection = %+v", col)
		}
	})

	t.Run("bad json serves empty", func(t *testing.T) {
		g := testGateway(t)
		g.Requester = rawRequester{body: []byte("nope")}
		w := httptest.NewRecorder()
		g.serveReplies(w, "alice", "t1")
		var col orderedCollection
		if err := json.Unmarshal(w.Body.Bytes(), &col); err != nil {
			t.Fatal(err)
		}
		if col.TotalItems != 0 {
			t.Fatalf("collection = %+v", col)
		}
	})
}

func TestNodeInfo(t *testing.T) {
	g := testGateway(t)
	srv := httptest.NewServer(g.Routes())
	defer srv.Close()

	var links nodeInfoLinks
	getJSON(t, srv, "/.well-known/nodeinfo", &links)
	if len(links.Links) != 1 || links.Links[0].Href != g.baseURL()+"/nodeinfo/2.0" {
		t.Fatalf("links = %+v", links)
	}

	var info map[string]any
	getJSON(t, srv, "/nodeinfo/2.0", &info)
	if info["version"] != "2.0" {
		t.Fatalf("version = %v", info["version"])
	}
	sw, _ := info["software"].(map[string]any)
	if sw["name"] != "warpnet-fediverse-gateway" || sw["version"] != version {
		t.Fatalf("software = %+v", sw)
	}
	if info["openRegistrations"] != false {
		t.Fatalf("openRegistrations = %v", info["openRegistrations"])
	}
}

func TestWriteJSONReportsUnmarshalableValues(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, ContentTypeAP, math.Inf(1)) // JSON cannot represent Inf
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

func TestLogRequestsRecordsTheStatus(t *testing.T) {
	h := logRequests(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "gone", http.StatusGone)
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
	if w.Code != http.StatusGone {
		t.Fatalf("status = %d, want the wrapper to pass it through", w.Code)
	}

	// A handler that never calls WriteHeader is logged as 200.
	sw := &statusWriter{ResponseWriter: httptest.NewRecorder(), status: http.StatusOK}
	if sw.status != http.StatusOK {
		t.Fatalf("default status = %d", sw.status)
	}
	sw.WriteHeader(http.StatusTeapot)
	if sw.status != http.StatusTeapot {
		t.Fatalf("status = %d", sw.status)
	}
}

func TestUserFromActorURL(t *testing.T) {
	cases := map[string]string{
		"https://gw.example/users/alice":            "alice",
		"https://gw.example/users/alice/statuses/1": "alice",
		"https://gw.example/users/alice/":           "alice",
		"https://other.example/users/bob":           "bob", // path-shaped, host is not checked here
		"https://gw.example/@alice":                 "",
		"":                                          "",
	}
	for in, want := range cases {
		if got := userFromActorURL(in); got != want {
			t.Errorf("userFromActorURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRandomToken(t *testing.T) {
	a, b := warp.RandomToken(), warp.RandomToken()
	if len(a) != 16 {
		t.Fatalf("token = %q, want 16 hex chars", a)
	}
	if a == b {
		t.Fatal("tokens must not repeat; activity ids are derived from them")
	}
}

func TestIsSelfHostAndURL(t *testing.T) {
	g := testGateway(t) // host gw.example
	if !g.isSelfHost("GW.EXAMPLE") {
		t.Fatal("host matching must be case-insensitive")
	}
	if g.isSelfHost("other.example") {
		t.Fatal("a foreign host must not be self")
	}
	if !g.isSelfURL("https://gw.example/users/alice") {
		t.Fatal("our own actor url must be self")
	}
	if g.isSelfURL("https://other.example/users/alice") {
		t.Fatal("a remote url must not be self")
	}
	if g.isSelfURL("://nope") {
		t.Fatal("an unparsable url must not read as self")
	}

	t.Run("an empty host never matches", func(t *testing.T) {
		empty := &Gateway{}
		if empty.isSelfHost("") {
			t.Fatal("an unconfigured gateway must not claim every host")
		}
	})
}

func TestCheckRemoteURLAppliesTheSSRFGuardWhenNotInTestMode(t *testing.T) {
	g := testGateway(t)
	g.allowPrivateTargets = false

	if err := g.checkRemoteURL("http://example.com/x"); !errors.Is(err, netguard.ErrInsecureURL) {
		t.Fatalf("http: err = %v", err)
	}
	if err := g.checkRemoteURL("https://127.0.0.1/x"); !errors.Is(err, netguard.ErrBlockedHost) {
		t.Fatalf("loopback: err = %v", err)
	}
	if err := g.checkRemoteURL(g.baseURL() + "/users/alice"); !errors.Is(err, errSelfTarget) {
		t.Fatalf("self: err = %v", err)
	}
	if err := g.checkRemoteURL("https://mastodon.social/users/bob"); err != nil {
		t.Fatalf("a public https url must pass: %v", err)
	}
}

func TestFetchKey(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)
	actorURL := f.Actor("bob", &g.key.PublicKey)

	t.Run("resolves the actor's signing key", func(t *testing.T) {
		got, err := g.fetchKey(context.Background(), actorURL+"#main-key")
		if err != nil {
			t.Fatalf("fetchKey: %v", err)
		}
		if !got.Equal(&g.key.PublicKey) {
			t.Fatal("wrong key")
		}
	})

	t.Run("tolerates publicKey as an array", func(t *testing.T) {
		pem, perr := publicKeyPEM(g.key)
		if perr != nil {
			t.Fatal(perr)
		}
		f.ServeDoc("/users/arr", ContentTypeAP, map[string]any{
			"id": f.URL("/users/arr"),
			"publicKey": []any{map[string]any{
				"id": f.URL("/users/arr") + "#main-key", "publicKeyPem": pem,
			}},
		})
		if _, err := g.fetchKey(context.Background(), f.URL("/users/arr")+"#main-key"); err != nil {
			t.Fatalf("array form: %v", err)
		}
	})

	t.Run("an actor without a key is malformed", func(t *testing.T) {
		f.ServeDoc("/users/nokey", ContentTypeAP, map[string]any{"id": f.URL("/users/nokey")})
		if _, err := g.fetchKey(context.Background(), f.URL("/users/nokey")); !errors.Is(err, errActorMalformed) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("an empty publicKeyPem is malformed", func(t *testing.T) {
		f.ServeDoc("/users/emptypem", ContentTypeAP, map[string]any{
			"id": f.URL("/users/emptypem"), "publicKey": map[string]any{"id": "k"},
		})
		if _, err := g.fetchKey(context.Background(), f.URL("/users/emptypem")); !errors.Is(err, errActorMalformed) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("an empty publicKey array is malformed", func(t *testing.T) {
		f.ServeDoc("/users/emptyarr", ContentTypeAP, map[string]any{
			"id": f.URL("/users/emptyarr"), "publicKey": []any{},
		})
		if _, err := g.fetchKey(context.Background(), f.URL("/users/emptyarr")); !errors.Is(err, errActorMalformed) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("an unreachable actor fails", func(t *testing.T) {
		if _, err := g.fetchKey(context.Background(), f.URL("/users/ghost")+"#main-key"); err == nil {
			t.Fatal("expected an error for a 404 actor")
		}
	})
}

func TestFetchActorErrors(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)
	f.On("/notjson", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(HeaderContentType, ContentTypeAP)
		_, _ = w.Write([]byte("<html>"))
	})

	if _, err := g.fetchActor(context.Background(), f.URL("/missing")); !errors.Is(err, errRemoteStatus) {
		t.Fatalf("404: err = %v", err)
	}
	if _, err := g.fetchActor(context.Background(), f.URL("/notjson")); err == nil {
		t.Fatal("a non-JSON actor document must fail")
	}
}

func TestRemoteInbox(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)
	ctx := context.Background()

	t.Run("prefers the shared inbox", func(t *testing.T) {
		f.ServeDoc("/users/shared", ContentTypeAP, map[string]any{
			"id": f.URL("/users/shared"), "inbox": f.URL("/users/shared/inbox"),
			"endpoints": map[string]any{"sharedInbox": f.URL("/inbox")},
		})
		got, err := g.remoteInbox(ctx, f.URL("/users/shared"))
		if err != nil {
			t.Fatal(err)
		}
		if got != f.URL("/inbox") {
			t.Fatalf("inbox = %q, want the shared one", got)
		}
	})

	t.Run("falls back to the personal inbox", func(t *testing.T) {
		actorURL := f.Actor("solo", nil)
		got, err := g.remoteInbox(ctx, actorURL)
		if err != nil {
			t.Fatal(err)
		}
		if got != actorURL+pathInbox {
			t.Fatalf("inbox = %q", got)
		}
	})

	t.Run("an empty sharedInbox falls through", func(t *testing.T) {
		f.ServeDoc("/users/emptyshared", ContentTypeAP, map[string]any{
			"id": f.URL("/users/emptyshared"), "inbox": f.URL("/users/emptyshared/inbox"),
			"endpoints": map[string]any{"sharedInbox": ""},
		})
		got, err := g.remoteInbox(ctx, f.URL("/users/emptyshared"))
		if err != nil {
			t.Fatal(err)
		}
		if got != f.URL("/users/emptyshared/inbox") {
			t.Fatalf("inbox = %q", got)
		}
	})

	t.Run("an actor with no inbox is malformed", func(t *testing.T) {
		f.ServeDoc("/users/noinbox", ContentTypeAP, map[string]any{"id": f.URL("/users/noinbox")})
		if _, err := g.remoteInbox(ctx, f.URL("/users/noinbox")); !errors.Is(err, errActorMalformed) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("an unreachable actor fails", func(t *testing.T) {
		if _, err := g.remoteInbox(ctx, f.URL("/users/ghost")); err == nil {
			t.Fatal("expected an error")
		}
	})
}

func TestAPGetJSONAndArray(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)
	ctx := context.Background()

	f.ServeDoc("/obj", "application/json", map[string]any{"a": 1})
	f.ServeDoc("/arr", "application/json", []any{map[string]any{"id": "1"}})
	f.On("/broken", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("{{{")) })

	if m, err := g.apGetJSON(ctx, f.URL("/obj"), "application/json"); err != nil || m["a"] != float64(1) {
		t.Fatalf("obj = %v, %v", m, err)
	}
	if a, err := g.apGetArray(ctx, f.URL("/arr"), "application/json"); err != nil || len(a) != 1 {
		t.Fatalf("arr = %v, %v", a, err)
	}

	if _, err := g.apGetJSON(ctx, f.URL("/missing"), "application/json"); !errors.Is(err, errRemoteStatus) {
		t.Fatalf("json 404: err = %v", err)
	}
	if _, err := g.apGetArray(ctx, f.URL("/missing"), "application/json"); !errors.Is(err, errRemoteStatus) {
		t.Fatalf("array 404: err = %v", err)
	}
	if _, err := g.apGetJSON(ctx, f.URL("/broken"), "application/json"); err == nil {
		t.Fatal("malformed JSON must fail")
	}
	if _, err := g.apGetArray(ctx, f.URL("/obj"), "application/json"); err == nil {
		t.Fatal("an object is not an array")
	}
	// A blocked URL must never reach the network.
	if _, err := g.apGetJSON(ctx, g.baseURL()+"/x", "application/json"); !errors.Is(err, errSelfTarget) {
		t.Fatalf("self: err = %v", err)
	}
	if _, err := g.apGetArray(ctx, g.baseURL()+"/x", "application/json"); !errors.Is(err, errSelfTarget) {
		t.Fatalf("self array: err = %v", err)
	}
}

func TestFetchMedia(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)
	ctx := context.Background()

	f.On("/img.png", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(HeaderContentType, "image/png")
		_, _ = w.Write([]byte{0x89, 'P', 'N', 'G'})
	})

	mime, data, err := g.fetchMedia(ctx, f.URL("/img.png"))
	if err != nil {
		t.Fatalf("fetchMedia: %v", err)
	}
	if mime != "image/png" || len(data) != 4 {
		t.Fatalf("mime = %q, %d bytes", mime, len(data))
	}
	if _, _, err := g.fetchMedia(ctx, f.URL("/missing.png")); !errors.Is(err, errRemoteStatus) {
		t.Fatalf("404: err = %v", err)
	}
}

// A media file above the limit must be an error, never silently truncated:
// nodes cache whatever bytes we hand them, so half an image would be served
// as the real thing until its TTL expires.
func TestFetchMediaRejectsOversizedFiles(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)
	ctx := context.Background()

	f.On("/at-limit.png", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(HeaderContentType, "image/png")
		_, _ = w.Write(bytes.Repeat([]byte{0xAB}, maxMediaBytes))
	})
	f.On("/oversize.png", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(HeaderContentType, "image/png")
		_, _ = w.Write(bytes.Repeat([]byte{0xAB}, maxMediaBytes+1))
	})

	_, data, err := g.fetchMedia(ctx, f.URL("/at-limit.png"))
	if err != nil {
		t.Fatalf("at-limit fetch: %v", err)
	}
	if len(data) != maxMediaBytes {
		t.Fatalf("at-limit: got %d bytes, want %d", len(data), maxMediaBytes)
	}

	if _, _, err := g.fetchMedia(ctx, f.URL("/oversize.png")); !errors.Is(err, errBodyTooLarge) {
		t.Fatalf("oversize: err = %v, want errBodyTooLarge", err)
	}
}

func TestPostSignedSurfacesTheRemoteRejection(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)
	f.On("/reject", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "signature verification failed", http.StatusUnauthorized)
	})

	err := g.postSigned(context.Background(), "alice", f.URL("/reject"), activity{Type: TypeCreate})
	if err == nil {
		t.Fatal("expected an error")
	}
	// Mastodon explains inbox rejections in the body; without it a failed
	// delivery is undiagnosable.
	if !strings.Contains(err.Error(), "signature verification failed") {
		t.Fatalf("err = %v, want the peer's explanation included", err)
	}
	if !errors.Is(err, errRemoteStatus) {
		t.Fatalf("err = %v, want errRemoteStatus", err)
	}
}

func TestPostSignedDeliversASignedActivity(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)
	f.Inbox("/inbox")

	doc := activity{Context: asContext, ID: "x#1", Type: TypeCreate, Actor: g.ActorID("alice")}
	if err := g.postSigned(context.Background(), "alice", f.URL("/inbox"), doc); err != nil {
		t.Fatalf("postSigned: %v", err)
	}
	got := f.Delivered()
	if len(got) != 1 {
		t.Fatalf("deliveries = %d", len(got))
	}
	if got[0].Doc["type"] != TypeCreate {
		t.Fatalf("delivered = %+v", got[0].Doc)
	}
	if !strings.Contains(got[0].Sig, g.keyID("alice")) {
		t.Fatalf("signature = %q, want it keyed to the local user's actor", got[0].Sig)
	}
}

func TestRetryableStatus(t *testing.T) {
	for _, code := range []int{http.StatusTooManyRequests, 500, 502, 503} {
		if !retryableStatus(code) {
			t.Errorf("%d should be retried", code)
		}
	}
	for _, code := range []int{200, 301, 400, 403, 404, 410, 422} {
		if retryableStatus(code) {
			t.Errorf("%d should not be retried", code)
		}
	}
}

func TestSignGetIsANoOpWithoutAKey(t *testing.T) {
	g := &Gateway{Host: "gw.example"}
	req, err := http.NewRequest(http.MethodGet, "https://m/users/bob", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.signGet(req); err != nil {
		t.Fatalf("signGet: %v", err)
	}
	if req.Header.Get("Signature") != "" {
		t.Fatal("a keyless gateway must not pretend to sign")
	}
}

// A long rejection body must be truncated, not dumped whole into the log line.
func TestPostSignedTruncatesTheRejectionSnippet(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)
	long := strings.Repeat("x", 1000)
	f.On("/verbose", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, long, http.StatusBadRequest)
	})

	err := g.postSigned(context.Background(), "alice", f.URL("/verbose"), activity{Type: TypeCreate})
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), long) {
		t.Fatal("the whole body was included; the snippet must be capped")
	}
	if !strings.Contains(err.Error(), strings.Repeat("x", 300)) {
		t.Fatalf("err = %v, want the first 300 bytes kept", err)
	}
}

// A doc that cannot be marshalled must fail before anything is sent.
func TestPostSignedRejectsAnUnmarshalableDocument(t *testing.T) {
	g := testGateway(t)
	f := attachFake(fediversetest.NewInstance(t), g)
	f.Inbox("/inbox")
	if err := g.postSigned(context.Background(), "alice", f.URL("/inbox"), make(chan int)); err == nil {
		t.Fatal("expected a marshal error")
	}
	if len(f.Delivered()) != 0 {
		t.Fatal("nothing must reach the peer")
	}
}

// A requester that spans no networks (the single-node client, or a test fake)
// keeps the plain broadcast behaviour.
func TestRequestForUserFallsBackToBroadcast(t *testing.T) {
	fake := &fakeRequester{}
	g := testGateway(t)
	g.Requester = fake

	if _, err := g.RequestForUser("alice", warp.RoutePostReact, warp.ReactionEvent{}); err != nil {
		t.Fatalf("broadcast fallback: %v", err)
	}
	if fake.lastRoute != warp.RoutePostReact {
		t.Fatalf("route = %q, want the reaction broadcast", fake.lastRoute)
	}
}
