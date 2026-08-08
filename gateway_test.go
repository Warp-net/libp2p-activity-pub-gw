//nolint:all
package main

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Warp-net/warpnet/domain"
	"github.com/Warp-net/warpnet/retrier"
	"github.com/hashicorp/golang-lru/v2/expirable"
	log "github.com/sirupsen/logrus"
)

func testGateway(t *testing.T) *gateway {
	t.Helper()
	key, err := loadOrCreateKey(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	pub, err := publicKeyPEM(key)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}
	return &gateway{
		host:                "gw.example",
		key:                 key,
		keyPubPEM:           pub,
		source:              staticSource{user: warpnetUser{ID: "alice", PreferredUsername: "alice", DisplayName: "Alice"}},
		client:              http.DefaultClient,
		sem:                 make(chan struct{}, 4),
		followers:           newMemFollowerStore(),
		allowPrivateTargets: true,
	}
}

func TestWebFinger(t *testing.T) {
	srv := httptest.NewServer(testGateway(t).routes())
	defer srv.Close()

	t.Run("known user", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/.well-known/webfinger?resource=acct:alice@gw.example")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		var jrd webFingerJRD
		if err := json.NewDecoder(resp.Body).Decode(&jrd); err != nil {
			t.Fatal(err)
		}
		if len(jrd.Links) != 1 || jrd.Links[0].Href != "https://gw.example/users/alice" {
			t.Fatalf("unexpected jrd: %+v", jrd)
		}
	})

	t.Run("unknown user", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/.well-known/webfinger?resource=acct:bob@gw.example")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
	})
}

func TestActorDocument(t *testing.T) {
	srv := httptest.NewServer(testGateway(t).routes())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/users/alice")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if ct := resp.Header.Get("Content-Type"); ct != contentTypeAP {
		t.Fatalf("content-type = %q", ct)
	}
	var a actor
	if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		t.Fatal(err)
	}
	if a.ID != "https://gw.example/users/alice" {
		t.Fatalf("id = %q", a.ID)
	}
	if a.Inbox != "https://gw.example/users/alice/inbox" {
		t.Fatalf("inbox = %q", a.Inbox)
	}
	if !strings.Contains(a.PublicKey.PublicKeyPEM, "BEGIN PUBLIC KEY") {
		t.Fatalf("actor is missing a public key PEM")
	}
}

func TestHTTPSignatureRoundTrip(t *testing.T) {
	g := testGateway(t)
	body := []byte(`{"type":"Follow","actor":"https://remote/users/bob"}`)

	req, err := http.NewRequest(http.MethodPost, "https://gw.example/users/alice/inbox", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	if err := signRequest(req, g.keyID("alice"), g.key, body); err != nil {
		t.Fatalf("sign: %v", err)
	}

	pubKey := func(string) (*rsa.PublicKey, error) { return &g.key.PublicKey, nil }

	if _, err := verifyRequest(req, body, pubKey); err != nil {
		t.Fatalf("verify: %v", err)
	}

	t.Run("tampered body fails", func(t *testing.T) {
		if _, err := verifyRequest(req, []byte(`{"type":"Undo"}`), pubKey); err == nil {
			t.Fatal("expected digest mismatch, got nil")
		}
	})
}

func TestMemFollowerStore(t *testing.T) {
	s := newMemFollowerStore()
	if err := s.Add("alice", "https://m/users/bob"); err != nil {
		t.Fatal(err)
	}
	if err := s.Add("alice", "https://m/users/bob"); err != nil { // idempotent
		t.Fatal(err)
	}
	if got, _ := s.List("alice"); len(got) != 1 || got[0] != "https://m/users/bob" {
		t.Fatalf("want 1 follower, got %+v", got)
	}
}

func TestBuildCreateNote(t *testing.T) {
	g := testGateway(t)
	a := g.buildCreateNote("alice", tweet{Id: "t1", Text: "hello <fedi>", CreatedAt: time.Unix(1700000000, 0), UserId: "alice", ImageKeys: []string{"k1"}})
	if a.Type != "Create" || a.Actor != "https://gw.example/users/alice" {
		t.Fatalf("bad create: %+v", a)
	}
	n, ok := a.Object.(note)
	if !ok {
		t.Fatalf("object is not a note: %T", a.Object)
	}
	if n.ID != "https://gw.example/users/alice/statuses/t1" {
		t.Fatalf("bad note id: %s", n.ID)
	}
	if !strings.Contains(n.Content, "&lt;fedi&gt;") {
		t.Fatalf("content not html-escaped: %s", n.Content)
	}
	if len(n.To) == 0 || n.To[0] != asPublic {
		t.Fatalf("note not addressed to public: %+v", n.To)
	}
	if len(n.Attachment) != 1 || n.Attachment[0].Type != "Document" {
		t.Fatalf("attachment: %+v", n.Attachment)
	}
	if u, k, ok := decodeMediaRef(strings.TrimPrefix(n.Attachment[0].URL, "https://gw.example/media/")); !ok || u != "alice" || k != "k1" {
		t.Fatalf("attachment ref: %s", n.Attachment[0].URL)
	}
}

func TestPublishNoteFanout(t *testing.T) {
	g := testGateway(t)
	var mu sync.Mutex
	hits := map[string]int{}
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if name, ok := strings.CutPrefix(r.URL.Path, "/users/"); ok {
			// actor document carrying this follower's inbox
			writeJSON(w, contentTypeAP, map[string]any{
				"id":    srv.URL + "/users/" + name,
				"inbox": srv.URL + "/inbox/" + name,
			})
			return
		}
		mu.Lock()
		hits[r.URL.Path]++
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	g.client = srv.Client()

	_ = g.followers.Add("alice", srv.URL+"/users/bob")
	_ = g.followers.Add("alice", srv.URL+"/users/carol")

	g.publishNote(context.Background(), "alice", tweet{Id: "t1", Text: "hello fedi", CreatedAt: time.Now()})

	mu.Lock()
	defer mu.Unlock()
	if hits["/inbox/bob"] != 1 || hits["/inbox/carol"] != 1 {
		t.Fatalf("fanout mismatch: %+v", hits)
	}
}

// TestBridgeDeleteFederatesTombstone verifies deleting a Warpnet reply to a
// Mastodon note posts an AP Delete(Tombstone) for the reply's note id to the
// parent author's inbox.
func TestBridgeDeleteFederatesTombstone(t *testing.T) {
	g := testGateway(t)
	var got map[string]any
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/note":
			writeJSON(w, contentTypeAP, map[string]any{
				"id": srv.URL + "/note", "type": "Note", "attributedTo": srv.URL + "/users/bob",
			})
		case r.URL.Path == "/users/bob":
			writeJSON(w, contentTypeAP, map[string]any{
				"id": srv.URL + "/users/bob", "inbox": srv.URL + "/inbox/bob",
			})
		case r.URL.Path == "/inbox/bob" && r.Method == http.MethodPost:
			_ = json.NewDecoder(r.Body).Decode(&got)
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	g.client = srv.Client()

	b := newMastodonBridge(g, "node1")
	if err := b.Delete(context.Background(), deleteTweetEvent{
		UserId: "alice", TweetId: "01REPLY00000000000000000000", ParentId: srv.URL + "/note",
	}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if got["type"] != typeDelete || got["actor"] != g.actorID("alice") {
		t.Fatalf("activity = %+v", got)
	}
	obj, _ := got["object"].(map[string]any)
	wantID := g.actorID("alice") + pathStatuses + "01REPLY00000000000000000000" +
		"?" + url.Values{replyParentQuery: {srv.URL + "/note"}}.Encode()
	if obj["type"] != typeTombstone || obj["id"] != wantID {
		t.Fatalf("object = %+v, want Tombstone id %s", got["object"], wantID)
	}
}

// TestPublishNoteFederatesTopLevelPost verifies a top-level owner tweet is
// federated to the author's Fediverse followers as a Create(Note) with a stable,
// tweet-derived id — the delivery path the gossip push handler drives for BUG-1.
func TestPublishNoteFederatesTopLevelPost(t *testing.T) {
	g := testGateway(t)
	var got map[string]any
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/users/bob":
			writeJSON(w, contentTypeAP, map[string]any{
				"id": srv.URL + "/users/bob", "inbox": srv.URL + "/inbox/bob",
			})
		case r.URL.Path == "/inbox/bob" && r.Method == http.MethodPost:
			_ = json.NewDecoder(r.Body).Decode(&got)
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	g.client = srv.Client()
	if err := g.followers.Add("alice", srv.URL+"/users/bob"); err != nil {
		t.Fatalf("add follower: %v", err)
	}

	g.publishNote(context.Background(), "alice", tweet{Id: "01POST0000000000000000000000", UserId: "alice", Text: "hi fediverse"})

	if got["type"] != typeCreate || got["actor"] != g.actorID("alice") {
		t.Fatalf("activity = %+v", got)
	}
	obj, _ := got["object"].(map[string]any)
	wantID := g.actorID("alice") + pathStatuses + "01POST0000000000000000000000"
	if obj["type"] != typeNote || obj["id"] != wantID {
		t.Fatalf("object = %+v, want Note id %s", got["object"], wantID)
	}
}

// TestBridgeReplyEmbedsParentInNoteID verifies a federated reply's note id
// carries the parent url (so serveStatus can later resolve it), the note threads
// to the real parent, and the Create id stays query-free.
func TestBridgeReplyEmbedsParentInNoteID(t *testing.T) {
	g := testGateway(t)
	var got map[string]any
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/note":
			writeJSON(w, contentTypeAP, map[string]any{
				"id": srv.URL + "/note", "type": "Note", "attributedTo": srv.URL + "/users/bob",
			})
		case r.URL.Path == "/users/bob":
			writeJSON(w, contentTypeAP, map[string]any{
				"id": srv.URL + "/users/bob", "inbox": srv.URL + "/inbox/bob",
			})
		case r.URL.Path == "/inbox/bob" && r.Method == http.MethodPost:
			_ = json.NewDecoder(r.Body).Decode(&got)
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	g.client = srv.Client()

	b := newMastodonBridge(g, "node1")
	parent := srv.URL + "/note"
	pid := parent
	if err := b.Reply(context.Background(), tweet{
		UserId: "alice", Id: "r1", Text: "thank you!", RootId: parent, ParentId: &pid,
	}); err != nil {
		t.Fatalf("Reply: %v", err)
	}

	base := g.actorID("alice") + pathStatuses + "r1"
	obj, _ := got["object"].(map[string]any)
	wantNote := base + "?" + url.Values{replyParentQuery: {parent}}.Encode()
	if obj["id"] != wantNote {
		t.Fatalf("note id = %v, want %v", obj["id"], wantNote)
	}
	if obj["inReplyTo"] != parent {
		t.Fatalf("inReplyTo = %v, want %v", obj["inReplyTo"], parent)
	}
	if got["id"] != base+"#create" {
		t.Fatalf("create id = %v, want %v", got["id"], base+"#create")
	}
}

// TestScanKeepsFederatingOnEmptyFollowerBlip guards the fix for federation
// stopping on a transient read: a user-scoped follower read falls back to a
// broadcast when the owner node is unreachable (a node restart), and a non-owner
// node answers with an empty list. Stopping on that single empty read killed
// federation, and posts made while stopped are seeded as already-published when
// it resumes — they never reach the Fediverse.
func TestScanKeepsFederatingOnEmptyFollowerBlip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	usersJSON, _ := json.Marshal(usersResponse{Users: []user{{Id: "alice"}}})
	withFollower, _ := json.Marshal(followersResponse{
		Followers: []string{encodeActorID("https://mastodon.social/users/warpnet")},
	})
	empty, _ := json.Marshal(followersResponse{})

	req := &scanRequester{users: usersJSON, followers: withFollower}
	o := newOutboundFederation(ctx, req, testGateway(t))

	o.scan("alice")
	if !o.federating("alice") {
		t.Fatal("a user with an ap: follower must be federated")
	}

	req.followers = empty // owner node unreachable — broadcast answers empty
	o.scan("alice")
	if !o.federating("alice") {
		t.Fatal("stopped federating on a single empty read")
	}

	o.scan("alice") // still empty — a real unfollow
	if o.federating("alice") {
		t.Fatal("a repeated empty read must stop federation")
	}
}

// scanRequester serves scan(): the user page and a swappable follower list.
type scanRequester struct {
	users     []byte
	followers []byte
}

func (r *scanRequester) request(route string, _ any) ([]byte, error) {
	switch route {
	case routeGetUsers:
		return r.users, nil
	case routeGetFollowers:
		return r.followers, nil
	}
	return []byte(`["accepted"]`), nil
}

func (r *scanRequester) requestUser(_, route string, payload any) ([]byte, error) {
	return r.request(route, payload)
}

type fakeRequester struct {
	lastRoute      string
	lastPayload    any
	followersJSON  []byte
	followingsJSON []byte
	imageFile      string
	tweet          tweet
	tweetsJSON     []byte
}

func (f *fakeRequester) request(route string, payload any) ([]byte, error) {
	f.lastRoute = route
	f.lastPayload = payload
	switch route {
	case routeGetFollowers:
		return f.followersJSON, nil
	case routeGetFollowings:
		return f.followingsJSON, nil
	case routeGetImage:
		bt, _ := json.Marshal(getImageResponse{File: f.imageFile})
		return bt, nil
	case routeGetTweet:
		bt, _ := json.Marshal(f.tweet)
		return bt, nil
	case routeGetTweets:
		if f.tweetsJSON != nil {
			return f.tweetsJSON, nil
		}
	}
	return []byte(`["accepted"]`), nil
}

func (f *fakeRequester) requestUser(_, route string, payload any) ([]byte, error) {
	return f.request(route, payload)
}

// stubResolver stands in for the gateway's handle -> actor url lookup.
type stubResolver struct{ byID map[string]string }

func (r stubResolver) resolveActorID(_ context.Context, id string) (string, error) {
	if url, ok := r.byID[id]; ok {
		return url, nil
	}
	return decodeActorID(id) // legacy "ap:" ids need no lookup
}

func TestNodeFollowerStore(t *testing.T) {
	const actor = "https://mastodon.social/users/bob"
	const legacy = "https://mastodon.social/users/carol"
	fr := &fakeRequester{}
	s := nodeFollowerStore{req: fr, resolver: stubResolver{byID: map[string]string{
		"bob@mastodon.social": actor,
	}}}

	if err := s.Add("owner1", actor); err != nil {
		t.Fatal(err)
	}
	ev, ok := fr.lastPayload.(newFollowEvent)
	if !ok {
		t.Fatalf("follow payload type %T", fr.lastPayload)
	}
	if ev.FollowingId != "owner1" {
		t.Fatalf("following id = %q", ev.FollowingId)
	}
	// Warpnet stores the handle: the "ap:" encoding must not leak out of the
	// gateway, so a follower renders as a profile there instead of a raw id.
	if ev.FollowerId != "bob@mastodon.social" {
		t.Fatalf("follower id = %q, want the bob@mastodon.social handle", ev.FollowerId)
	}

	// List resolves handles, still decodes follow graphs recorded as "ap:" before
	// the switch, and skips native Warpnet ids.
	resp := followersResponse{Followers: []string{
		"bob@mastodon.social",
		encodeActorID(legacy),
		"01KSGHBHKG0N77T6A3RZV8WSH5", // native ULID — must be skipped
	}}
	fr.followersJSON, _ = json.Marshal(resp)

	urls, err := s.List("owner1")
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 2 || urls[0] != actor || urls[1] != legacy {
		t.Fatalf("list mismatch: %+v", urls)
	}
}

func strptr(s string) *string { return &s }

func TestPublishableTweet(t *testing.T) {
	cases := []struct {
		tw   tweet
		want bool
	}{
		{tweet{UserId: "alice"}, true},
		{tweet{UserId: "bob"}, false},                                 // not the owner
		{tweet{UserId: "alice", RetweetedBy: strptr("alice")}, false}, // retweet
		{tweet{UserId: "alice", ParentId: strptr("t0")}, false},       // reply
		{tweet{UserId: "alice", ParentId: strptr("")}, true},          // empty parent = top-level
	}
	for i, c := range cases {
		if got := publishableTweet(c.tw, "alice"); got != c.want {
			t.Errorf("case %d: got %v want %v", i, got, c.want)
		}
	}
}

type fakeTweetsRequester struct {
	tweets []tweet
}

func (f *fakeTweetsRequester) request(route string, _ any) ([]byte, error) {
	if route == routeGetTweets {
		bt, _ := json.Marshal(tweetsResponse{Tweets: f.tweets})
		return bt, nil
	}
	return []byte(`["accepted"]`), nil
}

func (f *fakeTweetsRequester) requestUser(_, route string, payload any) ([]byte, error) {
	return f.request(route, payload)
}

func TestTweetPollerSeedAndDedup(t *testing.T) {
	fr := &fakeTweetsRequester{tweets: []tweet{{Id: "t1", UserId: "alice"}}}
	var published []string
	p := newTweetPoller(fr, "alice", func(_ context.Context, _ string, tw tweet) {
		published = append(published, tw.Id)
	})

	// seed marks existing tweets as seen (no history replay)
	for _, tw := range p.fetch() {
		p.seen.Add(tw.Id, struct{}{})
	}

	// a new tweet arrives → published once
	fr.tweets = append(fr.tweets, tweet{Id: "t2", UserId: "alice"})
	p.poll(context.Background())
	if len(published) != 1 || published[0] != "t2" {
		t.Fatalf("expected only t2 published, got %v", published)
	}

	// polling again republishes nothing
	published = nil
	p.poll(context.Background())
	if len(published) != 0 {
		t.Fatalf("expected no republish, got %v", published)
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	rl := newRateLimitersWith(1000, 8, time.Second)
	var served int
	h := rl.middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served++
		w.WriteHeader(http.StatusOK)
	}))
	do := func(path, addr string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.RemoteAddr = addr
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	// weight-1 requests pass until the 8-unit budget is spent
	for i := 0; i < 8; i++ {
		if w := do("/nodeinfo/2.0", "203.0.113.1:1000"); w.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i+1, w.Code)
		}
	}
	w := do("/nodeinfo/2.0", "203.0.113.1:1000")
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("over budget: status = %d, want 429", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Fatal("over budget: missing Retry-After header")
	}
	if served != 8 {
		t.Fatalf("served = %d, want 8", served)
	}

	// another client IP has its own budget
	if w := do("/nodeinfo/2.0", "203.0.113.2:1000"); w.Code != http.StatusOK {
		t.Fatalf("other client: status = %d, want 200", w.Code)
	}

	// a weight-8 media request exhausts a fresh client's budget in one hit
	if w := do(pathMedia+"x", "203.0.113.3:1000"); w.Code != http.StatusOK {
		t.Fatalf("media: status = %d, want 200", w.Code)
	}
	if w := do("/nodeinfo/2.0", "203.0.113.3:1000"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("media client over budget: status = %d, want 429", w.Code)
	}
}

// TestRateLimitGlobalRecovers guards the fix for the global limiter latching
// locked forever: the middleware fast-429s a locked limiter without calling
// Limit(), which is the only thing that expires old tasks, so once the global
// budget is spent the whole data plane stays 429'd until restart. The drain must
// replace the stuck limiter so it recovers.
func TestRateLimitGlobalRecovers(t *testing.T) {
	window := 30 * time.Millisecond
	rl := newRateLimitersWith(4, 1000, window) // tiny global budget, roomy per-client
	h := rl.middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	do := func() int {
		r := httptest.NewRequest(http.MethodGet, "/nodeinfo/2.0", nil) // weight 1, data-plane
		r.RemoteAddr = "203.0.113.9:1000"
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}

	for i := 0; i < 4; i++ { // spend the global budget
		if code := do(); code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i+1, code)
		}
	}
	if code := do(); code != http.StatusTooManyRequests {
		t.Fatalf("over global budget: status = %d, want 429", code)
	}

	// The window has long passed, but without anything calling Limit() the
	// limiter never reclaims the weight — this is the latch bug.
	time.Sleep(window * 3)
	if code := do(); code != http.StatusTooManyRequests {
		t.Fatalf("without drain: status = %d, want still-locked 429", code)
	}

	// The drain goroutine advances the window and unlocks the data plane.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rl.drain(ctx)

	recovered := false
	for i := 0; i < 40; i++ { // up to ~2s (drainInterval is 1s)
		if do() == http.StatusOK {
			recovered = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !recovered {
		t.Fatal("global limiter never recovered after drain")
	}
}

// TestRateLimitClientRecovers guards the same latch fix for per-client limiters:
// a client that trips its budget is fast-429'd without calling Limit(), and its
// LRU entry's TTL is refreshed on every request, so it would stay locked forever.
// The drain must replace the stuck client limiter so it recovers.
func TestRateLimitClientRecovers(t *testing.T) {
	window := 30 * time.Millisecond
	rl := newRateLimitersWith(1_000_000, 4, window) // roomy global, tiny per-client
	h := rl.middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	do := func() int {
		r := httptest.NewRequest(http.MethodGet, "/nodeinfo/2.0", nil) // weight 1
		r.RemoteAddr = "203.0.113.7:1000"
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}

	for i := 0; i < 4; i++ { // spend the client's budget
		if code := do(); code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i+1, code)
		}
	}
	if code := do(); code != http.StatusTooManyRequests {
		t.Fatalf("over client budget: status = %d, want 429", code)
	}

	// The window has passed, but the client keeps 429'ing and its TTL is
	// refreshed, so nothing reclaims its weight — the latch bug.
	time.Sleep(window * 3)
	if code := do(); code != http.StatusTooManyRequests {
		t.Fatalf("without drain: status = %d, want still-locked 429", code)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rl.drain(ctx)

	recovered := false
	for i := 0; i < 40; i++ { // up to ~2s (drainInterval is 1s)
		if do() == http.StatusOK {
			recovered = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !recovered {
		t.Fatal("client limiter never recovered after drain")
	}
}

// TestRateLimitIdleClientStaysUnlocked guards the queue fix: with idle gaps
// longer than the window, every queued task expires on each call, and the
// library's default queue then fails to drop them (CutOffBefore no-ops when
// start == len). Limit() re-subtracts the same weights on the next call, the
// unsigned total underflows to ~4e9 and IsLocked() latches — 429ing a client
// that spent 5 of its 1000 units. That is what broke follow/reply delivery:
// Mastodon got a 429 fetching our actor document and could not verify the
// signature ("Unable to fetch key JSON").
func TestRateLimitIdleClientStaysUnlocked(t *testing.T) {
	window := 20 * time.Millisecond
	rl := newRateLimitersWith(1000, 1000, window) // both budgets roomy
	h := rl.middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	do := func() int {
		r := httptest.NewRequest(http.MethodGet, "/users/alice", nil) // weight 2
		r.RemoteAddr = "203.0.113.11:1000"
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}

	for i := 0; i < 6; i++ {
		if code := do(); code != http.StatusOK {
			t.Fatalf("request %d after idle gaps: status = %d, want 200", i+1, code)
		}
		time.Sleep(window * 2) // let the whole window expire between requests
	}
	if l := rl.client("203.0.113.11"); l.IsLocked() {
		t.Fatal("client limiter locked while far under budget")
	}
}

func TestRequestWeight(t *testing.T) {
	cases := map[string]uint32{
		"/nodeinfo/2.0":              weightStatic,
		"/.well-known/nodeinfo":      weightStatic,
		"/.well-known/webfinger?r=x": weightActor,
		"/users/alice":               weightActor,
		"/users/alice/outbox":        weightCollection,
		"/users/alice/statuses/1":    weightCollection,
		"/users/alice/inbox":         weightInbox,
		"/inbox":                     weightInbox,
		"/media/abc":                 weightMedia,
	}
	for path, want := range cases {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		if got := requestWeight(r); got != want {
			t.Errorf("requestWeight(%s) = %d, want %d", path, got, want)
		}
	}
}

func TestTranslateInbound(t *testing.T) {
	g := testGateway(t) // host gw.example
	actor := "https://m/users/bob"
	status := "https://gw.example/users/alice/statuses/t1"

	route, payload, ok := g.translateInbound(map[string]any{"type": "Like", "actor": actor, "object": status})
	if !ok || route != routePostReact {
		t.Fatalf("favourite: route=%q ok=%v", route, ok)
	}
	// owner_id is the reactor and user_id the reacted tweet's author — the
	// direction the node's reaction handler and the client use. With them swapped
	// the node books it as the author reacting to their own tweet, so the author
	// gets no notification and the reaction is streamed back to the gateway.
	react := payload.(reactionEvent)
	if react.TweetId != "t1" || react.UserId != "alice" {
		t.Fatalf("reaction event: %+v", react)
	}
	// A Mastodon favourite is the default heart: warpnet stores reactions per
	// emoji, so an empty one would read back as a bare like on an old client but
	// leave the heart chip unpainted on a current one.
	if react.Emoji != domain.DefaultReaction {
		t.Fatalf("favourite emoji = %q, want the default heart", react.Emoji)
	}
	// A Fediverse actor is attributed by its "name@instance" handle — the id its
	// bridged profile resolves under. An "ap:<base64url>" id here has no profile
	// to open, so the client renders the raw id.
	if react.OwnerId != "bob@m" {
		t.Fatalf("reactor id = %q, want the bob@m handle", react.OwnerId)
	}

	route, payload, ok = g.translateInbound(map[string]any{
		"type": "Create", "actor": actor,
		"object": map[string]any{
			"type": "Note", "id": "https://m/users/bob/statuses/9",
			"content": "<p>hi there</p>", "inReplyTo": status,
		},
	})
	if !ok || route != routePostTweet {
		t.Fatalf("reply: route=%q ok=%v", route, ok)
	}
	reply := payload.(tweet)
	if reply.RootId != "t1" || reply.ParentId == nil || *reply.ParentId != "t1" || reply.Text != "hi there" {
		t.Fatalf("reply event: %+v", reply)
	}
	if reply.ParentUserId == nil || *reply.ParentUserId != "alice" {
		t.Fatalf("reply parent_user_id = %v, want alice", reply.ParentUserId)
	}
	if reply.Username != "bob@m" {
		t.Fatalf("reply username = %q, want bob@m", reply.Username)
	}
	// The reply's author id must be the handle too, not the ActivityPub id: the
	// thread showed "@ap:aHR0cHM6..." next to the name because they disagreed.
	if reply.UserId != "bob@m" {
		t.Fatalf("reply user_id = %q, want the bob@m handle", reply.UserId)
	}
	// And its own id is the note url, so the node can ask us for its stats.
	if reply.Id != "https://m/users/bob/statuses/9" {
		t.Fatalf("reply id = %q, want the note id", reply.Id)
	}

	// Quote-post convention: no inReplyTo, text opens with "RE: <status URL>".
	route, payload, ok = g.translateInbound(map[string]any{
		"type": "Create", "actor": actor,
		"object": map[string]any{"type": "Note", "content": "<p>RE: <a href=\"" + status + "\">" + status + "</a> nice post</p>"},
	})
	if !ok || route != routePostTweet {
		t.Fatalf("RE reply: route=%q ok=%v", route, ok)
	}
	reply = payload.(tweet)
	if reply.RootId != "t1" || reply.ParentId == nil || *reply.ParentId != "t1" || reply.Text != "nice post" {
		t.Fatalf("RE reply event: %+v", reply)
	}

	// RE: pointing at a foreign status is not ours to thread.
	if _, _, ok := g.translateInbound(map[string]any{
		"type": "Create", "actor": actor,
		"object": map[string]any{"type": "Note", "content": "<p>RE: https://evil/users/x/statuses/9 hi</p>"},
	}); ok {
		t.Fatal("foreign RE quote should be unhandled")
	}

	// Quote of a local status (Misskey wire: quoteUri + "RE:" text fallback)
	// maps to a quote retweet.
	route, payload, ok = g.translateInbound(map[string]any{
		"type": "Create", "actor": actor,
		"object": map[string]any{
			"type": "Note", "quoteUri": status,
			"content": "<p>hot take<br>RE: <a href=\"" + status + "\">" + status + "</a></p>",
		},
	})
	if !ok || route != routePostRetweet {
		t.Fatalf("quote: route=%q ok=%v", route, ok)
	}
	q := payload.(tweet)
	if q.Id != "t1" || q.QuotedTweetId == nil || *q.QuotedTweetId != "t1" || q.QuotedUserId == nil || *q.QuotedUserId != "alice" {
		t.Fatalf("quote event: %+v", q)
	}
	if q.Text != "hot take" || q.RetweetedBy == nil || q.UserId != *q.RetweetedBy {
		t.Fatalf("quote comment/author: %+v", q)
	}
	if *q.RetweetedBy != "bob@m" {
		t.Fatalf("quoter id = %q, want the bob@m handle", *q.RetweetedBy)
	}

	// Quote property with a leading "RE:" fallback (Mastodon wire form)
	// drops the fallback from the comment too.
	route, payload, ok = g.translateInbound(map[string]any{
		"type": "Create", "actor": actor,
		"object": map[string]any{
			"type": "Note", "quote": status,
			"content": "<p>RE: <a href=\"" + status + "\">" + status + "</a></p><p>great idea</p>",
		},
	})
	if !ok || route != routePostRetweet {
		t.Fatalf("leading-fallback quote: route=%q ok=%v", route, ok)
	}
	if q := payload.(tweet); q.Text != "great idea" || q.QuotedTweetId == nil || *q.QuotedTweetId != "t1" {
		t.Fatalf("leading-fallback quote event: %+v", q)
	}

	// Same via the text fallback alone (no quoteUri).
	route, payload, ok = g.translateInbound(map[string]any{
		"type": "Create", "actor": actor,
		"object": map[string]any{"type": "Note", "content": "<p>nice<br>RE: " + status + "</p>"},
	})
	if !ok || route != routePostRetweet {
		t.Fatalf("text-fallback quote: route=%q ok=%v", route, ok)
	}
	if q := payload.(tweet); q.Text != "nice" || q.QuotedTweetId == nil || *q.QuotedTweetId != "t1" {
		t.Fatalf("text-fallback quote event: %+v", q)
	}

	// A quote of a foreign status is not ours to store.
	if _, _, ok := g.translateInbound(map[string]any{
		"type": "Create", "actor": actor,
		"object": map[string]any{
			"type": "Note", "quoteUri": "https://evil/users/x/statuses/9",
			"content": "<p>look</p>",
		},
	}); ok {
		t.Fatal("foreign quote should be unhandled")
	}

	if route, _, ok := g.translateInbound(map[string]any{
		"type": "Undo", "actor": actor,
		"object": map[string]any{"type": "Follow", "object": "https://gw.example/users/alice"},
	}); !ok || route != routePostUnfollow {
		t.Fatalf("undo follow: route=%q ok=%v", route, ok)
	}

	if route, _, ok := g.translateInbound(map[string]any{
		"type": "Undo", "actor": actor,
		"object": map[string]any{"type": "Like", "object": status},
	}); !ok || route != routePostUnreact {
		t.Fatalf("undo favourite: route=%q ok=%v", route, ok)
	}

	if route, payload, ok := g.translateInbound(map[string]any{
		"type": "Undo", "actor": actor,
		"object": map[string]any{"type": "Announce", "object": status},
	}); !ok || route != routePostUnretweet {
		t.Fatalf("undo announce: route=%q ok=%v", route, ok)
	} else if ur := payload.(unretweetEvent); ur.TweetId != "t1" {
		t.Fatalf("unretweet event: %+v", ur)
	}

	route, payload, ok = g.translateInbound(map[string]any{"type": "Announce", "actor": actor, "object": status})
	if !ok || route != routePostRetweet {
		t.Fatalf("announce: route=%q ok=%v", route, ok)
	}
	rt := payload.(tweet)
	if rt.Id != "t1" || rt.RetweetedBy == nil {
		t.Fatalf("retweet event: %+v", rt)
	}
	if *rt.RetweetedBy != "bob@m" {
		t.Fatalf("booster id = %q, want the bob@m handle", *rt.RetweetedBy)
	}

	// Foreign-host objects and unhandled types are rejected.
	if _, _, ok := g.translateInbound(map[string]any{"type": "Like", "actor": actor, "object": "https://evil/users/x/statuses/9"}); ok {
		t.Fatal("foreign-host like should be unhandled")
	}
	if _, _, ok := g.translateInbound(map[string]any{"type": "Delete", "actor": actor, "object": status}); ok {
		t.Fatal("delete should not translate to a node route")
	}
}

func TestNoteToTweetREPrefix(t *testing.T) {
	parent := "https://m/users/bob/statuses/42"

	tw, ok := noteToTweet("alice@m", map[string]any{
		"type": "Note", "id": "https://m/users/alice/statuses/1",
		"content": "<p>RE: <a href=\"" + parent + "\">" + parent + "</a> agreed!</p>",
	})
	if !ok {
		t.Fatal("note should map")
	}
	if tw.ParentId == nil || *tw.ParentId != parent || tw.RootId != parent {
		t.Fatalf("RE quote should thread under %s: %+v", parent, tw)
	}
	if tw.Text != "agreed!" {
		t.Fatalf("text = %q, want prefix stripped", tw.Text)
	}

	// Bare quote with no commentary keeps its original text.
	tw, _ = noteToTweet("alice@m", map[string]any{
		"type": "Note", "id": "https://m/users/alice/statuses/2",
		"content": "<p>RE: " + parent + "</p>",
	})
	if tw.ParentId == nil || *tw.ParentId != parent || tw.Text != "RE: "+parent {
		t.Fatalf("bare RE quote: %+v", tw)
	}

	// An explicit inReplyTo wins; the text is left alone.
	tw, _ = noteToTweet("alice@m", map[string]any{
		"type": "Note", "id": "https://m/users/alice/statuses/3",
		"content": "<p>RE: https://other/users/x/statuses/9</p>", "inReplyTo": parent,
	})
	if tw.ParentId == nil || *tw.ParentId != parent || tw.Text != "RE: https://other/users/x/statuses/9" {
		t.Fatalf("inReplyTo should win: %+v", tw)
	}

	// Non-URL and plain-word "RE:" texts stay top-level.
	for _, content := range []string{"<p>RE: what you said</p>", "<p>REALLY good</p>", "<p>RE: http://insecure/1</p>"} {
		tw, _ = noteToTweet("alice@m", map[string]any{
			"type": "Note", "id": "https://m/users/alice/statuses/4", "content": content,
		})
		if tw.ParentId != nil {
			t.Fatalf("%q should not thread: %+v", content, tw)
		}
	}
}

func TestNoteToTweetQuote(t *testing.T) {
	quoted := "https://misskey.example/notes/9abc"

	// Misskey wire form: explicit quote property + glued "RE:" text fallback.
	tw, ok := noteToTweet("alice@mi", map[string]any{
		"type": "Note", "id": "https://misskey.example/notes/1",
		"content":  "<p>great point!<br><br>RE: <a href=\"" + quoted + "\">" + quoted + "</a></p>",
		"quoteUri": quoted,
	})
	if !ok {
		t.Fatal("note should map")
	}
	if tw.QuotedTweetId == nil || *tw.QuotedTweetId != quoted {
		t.Fatalf("quoted tweet id: %+v", tw)
	}
	if tw.Text != "great point!" {
		t.Fatalf("text = %q, want fallback stripped", tw.Text)
	}
	if tw.ParentId != nil {
		t.Fatalf("a quote is not a reply: %+v", tw)
	}
	if tw.QuotedUserId != nil {
		t.Fatalf("/notes/ URL carries no author, want nil quoted_user_id: %+v", tw)
	}

	// Text-only trailing fallback; a Mastodon-shaped URL fills quoted_user_id.
	mQuoted := "https://m/users/bob/statuses/42"
	tw, _ = noteToTweet("alice@m", map[string]any{
		"type": "Note", "id": "https://m/users/alice/statuses/2",
		"content": "<p>согласен</p><p>RE: <a href=\"" + mQuoted + "\">" + mQuoted + "</a></p>",
	})
	if tw.QuotedTweetId == nil || *tw.QuotedTweetId != mQuoted || tw.Text != "согласен" {
		t.Fatalf("text-fallback quote: %+v", tw)
	}
	if tw.QuotedUserId == nil || *tw.QuotedUserId != "bob@m" {
		t.Fatalf("quoted_user_id: %+v", tw.QuotedUserId)
	}

	// Mastodon wire form: explicit quote property + leading "RE:" fallback
	// before the comment.
	tw, _ = noteToTweet("alice@m", map[string]any{
		"type": "Note", "id": "https://m/users/alice/statuses/3",
		"content": "<p>RE: <a href=\"" + mQuoted + "\">" + mQuoted + "</a></p><p>This is a great idea!</p>",
		"quote":   mQuoted,
	})
	if tw.QuotedTweetId == nil || *tw.QuotedTweetId != mQuoted || tw.ParentId != nil {
		t.Fatalf("leading-fallback quote: %+v", tw)
	}
	if tw.Text != "This is a great idea!" {
		t.Fatalf("text = %q, want leading fallback stripped", tw.Text)
	}

	// A mid-word "RE:" (MORE:) or a URL not closing the note is no quote.
	for _, content := range []string{
		"<p>read MORE: https://m/users/x/statuses/9</p>",
		"<p>intro<br>RE: https://m/users/x/statuses/9 and more words</p>",
	} {
		tw, _ = noteToTweet("alice@m", map[string]any{
			"type": "Note", "id": "https://m/users/alice/statuses/3", "content": content,
		})
		if tw.QuotedTweetId != nil {
			t.Fatalf("%q should not quote: %+v", content, tw)
		}
	}
}

func TestHandleDeleteStub(t *testing.T) {
	g := testGateway(t)
	w := httptest.NewRecorder()
	g.handleDelete(w, map[string]any{
		"type": "Delete", "actor": "https://m/users/bob",
		"object": "https://m/users/bob/statuses/9",
	})
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", w.Code)
	}
}

func TestHandleFromActorURL(t *testing.T) {
	cases := map[string]string{
		"https://mastodon.social/users/bob": "bob@mastodon.social",
		"https://example.com/@alice":        "alice@example.com",
		"https://example.com/users/carol/":  "carol@example.com",
		"justname":                          "justname",
	}
	for in, want := range cases {
		if got := handleFromActorURL(in); got != want {
			t.Errorf("handleFromActorURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSafeClientRedirect(t *testing.T) {
	c := newSafeClient(time.Second)
	if c.CheckRedirect == nil {
		t.Fatal("expected a CheckRedirect guard")
	}
	priv, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1/x", nil)
	if err := c.CheckRedirect(priv, nil); err == nil {
		t.Fatal("private redirect target should be rejected")
	}
	pub, _ := http.NewRequest(http.MethodGet, "https://example.com/y", nil)
	if err := c.CheckRedirect(pub, nil); err != nil {
		t.Fatalf("public https redirect should pass: %v", err)
	}
	if err := c.CheckRedirect(pub, make([]*http.Request, maxRedirects)); err == nil {
		t.Fatal("overlong redirect chain should be rejected")
	}
}

func TestIsBlockedIP(t *testing.T) {
	for _, s := range []string{"127.0.0.1", "10.0.0.1", "192.168.1.1", "169.254.1.1", "::1", "0.0.0.0"} {
		if !isBlockedIP(netip.MustParseAddr(s)) {
			t.Errorf("%s should be blocked", s)
		}
	}
	for _, s := range []string{"1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"} {
		if isBlockedIP(netip.MustParseAddr(s)) {
			t.Errorf("%s should be allowed", s)
		}
	}
}

func TestSafeClientBlocksPrivateDial(t *testing.T) {
	// The dialer's Control must reject a connection to a loopback IP (DNS
	// rebinding defence) before any TCP connect happens.
	c := newSafeClient(2 * time.Second)
	if _, err := c.Get("http://127.0.0.1:9/"); err == nil {
		t.Fatal("dial to a loopback IP should be blocked")
	}
}

func TestFollowPollerDiff(t *testing.T) {
	fr := &fakeRequester{}
	var followed, unfollowed []string
	p := newFollowPoller(fr, stubResolver{byID: map[string]string{
		"bob@mastodon.social": "https://mastodon.social/users/bob",
	}}, "owner",
		func(a string) { followed = append(followed, a) },
		func(a string) { unfollowed = append(unfollowed, a) },
	)
	enc := func(urls ...string) []byte {
		ids := []string{"01KSGHBHKG0N77T6A3RZV8WSH5"} // a Warpnet following must be ignored
		for _, u := range urls {
			ids = append(ids, encodeActorID(u))
		}
		bt, _ := json.Marshal(followingsResponse{Followings: ids})
		return bt
	}

	fr.followingsJSON = enc("https://m/users/bob")
	if err := p.poll(); err != nil {
		t.Fatal(err)
	}
	if len(followed) != 0 || len(unfollowed) != 0 {
		t.Fatalf("baseline must fire nothing: f=%v u=%v", followed, unfollowed)
	}

	fr.followingsJSON = enc("https://m/users/carol")
	if err := p.poll(); err != nil {
		t.Fatal(err)
	}
	if len(followed) != 1 || followed[0] != "https://m/users/carol" {
		t.Fatalf("followed=%v", followed)
	}
	if len(unfollowed) != 1 || unfollowed[0] != "https://m/users/bob" {
		t.Fatalf("unfollowed=%v", unfollowed)
	}
}

func TestHandleMedia(t *testing.T) {
	raw := []byte{0x89, 'P', 'N', 'G'}
	b64 := base64.StdEncoding.EncodeToString(raw)
	for name, file := range map[string]string{
		"data url": "data:image/png;base64," + b64,
		"legacy":   "image/png," + b64,
	} {
		t.Run(name, func(t *testing.T) {
			g := testGateway(t)
			g.req = &fakeRequester{imageFile: file}

			srv := httptest.NewServer(g.routes())
			defer srv.Close()

			resp, err := http.Get(srv.URL + "/media/" + encodeMediaRef("alice", "img1"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			if ct := resp.Header.Get("Content-Type"); ct != "image/png" {
				t.Fatalf("content-type = %q", ct)
			}
			got, _ := io.ReadAll(resp.Body)
			if !bytes.Equal(got, raw) {
				t.Fatalf("body mismatch: %v", got)
			}
		})
	}
}

func TestServeStatus(t *testing.T) {
	g := testGateway(t)
	g.req = &fakeRequester{tweet: tweet{
		Id: "t1", UserId: "alice", Text: "hi <there>", CreatedAt: time.Unix(1700000000, 0),
	}}

	srv := httptest.NewServer(g.routes())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/users/alice/statuses/t1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != contentTypeAP {
		t.Fatalf("content-type = %q", ct)
	}
	var n note
	if err := json.NewDecoder(resp.Body).Decode(&n); err != nil {
		t.Fatal(err)
	}
	if n.ID != "https://gw.example/users/alice/statuses/t1" || n.Type != typeNote {
		t.Fatalf("note id/type: %+v", n)
	}
	if n.Context != asContext || !strings.Contains(n.Content, "&lt;there&gt;") {
		t.Fatalf("note context/content: %+v", n)
	}

	t.Run("missing tweet 404s", func(t *testing.T) {
		g2 := testGateway(t)
		g2.req = &fakeRequester{} // empty tweet (Id == "")
		srv2 := httptest.NewServer(g2.routes())
		defer srv2.Close()
		resp, err := http.Get(srv2.URL + "/users/alice/statuses/nope")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
	})

	t.Run("reply status passes parent and threads", func(t *testing.T) {
		g3 := testGateway(t)
		parent := "https://mastodon.xyz/users/NGIZero/statuses/116760411158384997"
		pid := parent
		fr := &fakeRequester{tweet: tweet{
			Id: "r1", UserId: "alice", Text: "thank you!", ParentId: &pid, RootId: parent,
			CreatedAt: time.Unix(1700000000, 0),
		}}
		g3.req = fr
		srv3 := httptest.NewServer(g3.routes())
		defer srv3.Close()

		want := "https://gw.example/users/alice/statuses/r1?" + url.Values{replyParentQuery: {parent}}.Encode()
		resp, err := http.Get(srv3.URL + "/users/alice/statuses/r1?" + url.Values{replyParentQuery: {parent}}.Encode())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		// the node is asked with parent_id so it can find the reply in the thread
		gt, ok := fr.lastPayload.(getTweetEvent)
		if !ok || gt.ParentId != parent || gt.TweetId != "r1" {
			t.Fatalf("payload = %+v", fr.lastPayload)
		}
		var n note
		if err := json.NewDecoder(resp.Body).Decode(&n); err != nil {
			t.Fatal(err)
		}
		if n.ID != want {
			t.Fatalf("note id = %q, want %q", n.ID, want)
		}
		if n.InReplyTo != parent {
			t.Fatalf("inReplyTo = %q, want %q", n.InReplyTo, parent)
		}
	})
}

func TestValidateRemoteURL(t *testing.T) {
	for _, u := range []string{
		"https://mastodon.social/users/x",
		"https://example.com/inbox",
	} {
		if err := validateRemoteURL(u); err != nil {
			t.Errorf("expected ok for %s, got %v", u, err)
		}
	}
	for _, u := range []string{
		"http://example.com/x",  // not https
		"https://127.0.0.1/x",   // loopback
		"https://localhost/x",   // localhost
		"https://10.0.0.1/x",    // private
		"https://169.254.1.1/x", // link-local
		"https://[::1]/x",       // ipv6 loopback
		"https:///x",            // no host
	} {
		if err := validateRemoteURL(u); err == nil {
			t.Errorf("expected error for %s", u)
		}
	}
}

func TestSendRetry(t *testing.T) {
	g := testGateway(t)
	g.retrier = retrier.New(time.Millisecond, 3, retrier.NoBackoff)

	t.Run("4xx is not retried", func(t *testing.T) {
		var n int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			atomic.AddInt32(&n, 1)
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()
		if _, err := g.apGetJSON(context.Background(), srv.URL, contentTypeAP); err == nil {
			t.Fatal("want error")
		}
		if got := atomic.LoadInt32(&n); got != 1 {
			t.Fatalf("attempts = %d, want 1", got)
		}
	})

	t.Run("5xx retries then exhausts", func(t *testing.T) {
		var n int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			atomic.AddInt32(&n, 1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer srv.Close()
		if _, err := g.apGetJSON(context.Background(), srv.URL, contentTypeAP); err == nil {
			t.Fatal("want error")
		}
		if got := atomic.LoadInt32(&n); got != 3 {
			t.Fatalf("attempts = %d, want 3", got)
		}
	})

	t.Run("succeeds after a transient 503", func(t *testing.T) {
		var n int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if atomic.AddInt32(&n, 1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer srv.Close()
		m, err := g.apGetJSON(context.Background(), srv.URL, contentTypeAP)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if m["ok"] != true {
			t.Fatalf("body = %v", m)
		}
		if got := atomic.LoadInt32(&n); got != 2 {
			t.Fatalf("attempts = %d, want 2", got)
		}
	})
}

func TestGetRepliesDereferencesURIItems(t *testing.T) {
	g := testGateway(t)
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/note":
			writeJSON(w, contentTypeAP, map[string]any{
				"type": "Note", "id": srv.URL + "/note", "replies": srv.URL + "/note/replies",
			})
		case r.URL.Path == "/note/replies" && r.URL.RawQuery == "":
			// Mastodon-style: first is a page URL.
			writeJSON(w, contentTypeAP, map[string]any{
				"type": "Collection", "id": srv.URL + "/note/replies",
				"first": srv.URL + "/note/replies?page=true",
			})
		case r.URL.Path == "/note/replies" && r.URL.RawQuery == "page=true":
			// items are note URIs (strings), as Mastodon emits them.
			writeJSON(w, contentTypeAP, map[string]any{
				"type": "CollectionPage", "id": srv.URL + "/note/replies?page=true",
				"items": []any{srv.URL + "/r1", srv.URL + "/r2"},
			})
		case r.URL.Path == "/r1" || r.URL.Path == "/r2":
			writeJSON(w, contentTypeAP, map[string]any{
				"type": "Note", "id": srv.URL + r.URL.Path,
				"attributedTo": srv.URL + "/users/bob", "content": "reply " + r.URL.Path,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	g.client = srv.Client()

	b := newMastodonBridge(g, "node1")
	resp, err := b.GetReplies(context.Background(), srv.URL+"/note")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(resp.Tweets) != 2 {
		t.Fatalf("replies = %d, want 2: %+v", len(resp.Tweets), resp.Tweets)
	}
}

// GetReplies must prefer the Mastodon REST context endpoint and serve one
// thread level per call: the context lists the whole subtree flattened, but
// only direct children are the note's replies — a nested reply is served when
// the client asks for its own parent's replies.
func TestGetRepliesUsesMastodonContext(t *testing.T) {
	g := testGateway(t)
	descendants := []any{}
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/statuses/100/context", "/api/v1/statuses/101/context":
			// Mastodon returns the same flattened subtree for every status of
			// this thread that has descendants below it.
			writeJSON(w, "application/json", map[string]any{
				"ancestors": []any{}, "descendants": descendants,
			})
			return
		}
		http.NotFound(w, r) // no AP /replies route: only the context path must be used
	}))
	defer srv.Close()
	descendants = []any{
		map[string]any{
			"id": "101", "uri": srv.URL + "/users/bob/statuses/101",
			"content": "<p>first reply</p>", "created_at": "2024-01-01T00:00:00.000Z",
			"in_reply_to_id": "100", "account": map[string]any{"acct": "bob"},
		},
		map[string]any{
			"id": "102", "uri": srv.URL + "/users/carol/statuses/102",
			"content": "<p>nested</p>", "created_at": "2024-01-01T00:01:00.000Z",
			"in_reply_to_id": "101", "account": map[string]any{"acct": "carol@other.example"},
		},
	}
	g.client = srv.Client()
	host := strings.TrimPrefix(srv.URL, "https://")

	b := newMastodonBridge(g, "node1")
	root := srv.URL + "/users/alice/statuses/100"
	resp, err := b.GetReplies(context.Background(), root)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(resp.Tweets) != 1 {
		t.Fatalf("replies = %d, want the direct child only: %+v", len(resp.Tweets), resp.Tweets)
	}
	r0 := resp.Tweets[0]
	if r0.Id != srv.URL+"/users/bob/statuses/101" {
		t.Errorf("r0.Id = %q, want the AP uri", r0.Id)
	}
	if r0.UserId != "bob@"+host { // local acct completed with the instance host
		t.Errorf("r0.UserId = %q, want bob@%s", r0.UserId, host)
	}
	if r0.RootId != root {
		t.Errorf("r0.RootId = %q, want %q", r0.RootId, root)
	}
	if r0.ParentId == nil || *r0.ParentId != root { // in_reply_to 100 -> root URL
		t.Errorf("r0.ParentId = %v, want %q", r0.ParentId, root)
	}

	// Walking one level deeper (bob's reply as the parent) surfaces the
	// nested reply, parented at bob's uri.
	nested, err := b.GetReplies(context.Background(), r0.Id)
	if err != nil {
		t.Fatalf("nested: %v", err)
	}
	if len(nested.Tweets) != 1 {
		t.Fatalf("nested replies = %d, want 1: %+v", len(nested.Tweets), nested.Tweets)
	}
	r1 := nested.Tweets[0]
	if r1.UserId != "carol@other.example" { // already a full handle: keep as-is
		t.Errorf("r1.UserId = %q, want carol@other.example", r1.UserId)
	}
	if r1.ParentId == nil || *r1.ParentId != r0.Id {
		t.Errorf("r1.ParentId = %v, want bob's uri", r1.ParentId)
	}
}

// A federated Warpnet reply comes back from the context as a status hosted by
// the gateway itself; GetReplies must hand it to the node in native shape —
// bare tweet/user ids, no foreign network tag — so the node dedupes it against
// its own thread index.
func TestGetRepliesNativizesOwnStatuses(t *testing.T) {
	g := testGateway(t)
	self := g.baseURL()
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/statuses/100/context" {
			writeJSON(w, "application/json", map[string]any{
				"ancestors": []any{},
				"descendants": []any{map[string]any{
					"id":  "201",
					"uri": self + "/users/01KTRAUSER/statuses/01KZH0TWEET?parent=x",
					"content": "<p>from warpnet</p>", "created_at": "2024-01-01T00:00:00.000Z",
					"in_reply_to_id": "100",
					"account": map[string]any{
						"acct": "01KTRAUSER@selfhost", "display_name": "Vadim",
					},
				}},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	g.client = srv.Client()

	b := newMastodonBridge(g, "node1")
	root := srv.URL + "/users/alice/statuses/100"
	resp, err := b.GetReplies(context.Background(), root)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(resp.Tweets) != 1 {
		t.Fatalf("replies = %d, want 1: %+v", len(resp.Tweets), resp.Tweets)
	}
	r0 := resp.Tweets[0]
	if r0.Id != "01KZH0TWEET" {
		t.Errorf("Id = %q, want the bare warpnet tweet id", r0.Id)
	}
	if r0.UserId != "01KTRAUSER" {
		t.Errorf("UserId = %q, want the bare warpnet user id", r0.UserId)
	}
	if r0.Username != "Vadim" {
		t.Errorf("Username = %q, want the display name", r0.Username)
	}
	if r0.Network != "" {
		t.Errorf("Network = %q, want native (empty)", r0.Network)
	}
}

// GetTweets must load the timeline from the Mastodon REST statuses endpoint in
// one page, unwrapping boosts and embedding media, and page by max_id.
func TestGetTweetsUsesMastodonREST(t *testing.T) {
	g := testGateway(t)
	var statusesQuery string
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/accounts/lookup":
			writeJSON(w, "application/json", map[string]any{"id": "42", "acct": "alice"})
		case "/api/v1/accounts/42/statuses":
			statusesQuery = r.URL.RawQuery
			writeJSON(w, "application/json", []any{
				map[string]any{
					"id": "100", "uri": srv.URL + "/users/alice/statuses/100",
					"content": "<p>hello</p>", "created_at": "2024-01-01T00:00:00.000Z",
					"account":           map[string]any{"acct": "alice"},
					"media_attachments": []any{map[string]any{"type": "image", "url": srv.URL + "/img.png"}},
				},
				map[string]any{
					"id": "101", "uri": srv.URL + "/users/alice/statuses/101",
					"content": "", "created_at": "2024-01-01T00:01:00.000Z",
					"account": map[string]any{"acct": "alice"},
					"reblog": map[string]any{
						"id": "200", "uri": srv.URL + "/users/bob/statuses/200",
						"content": "<p>boosted</p>", "created_at": "2024-01-01T00:02:00.000Z",
						"account": map[string]any{"acct": "bob@other.example"},
					},
				},
			})
		default:
			http.NotFound(w, r) // no webfinger/outbox: only the REST path must be used
		}
	}))
	defer srv.Close()
	g.client = srv.Client()
	host := strings.TrimPrefix(srv.URL, "https://")

	b := newMastodonBridge(g, "node1")
	resp, err := b.GetTweets(context.Background(), "alice@"+host, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(resp.Tweets) != 2 {
		t.Fatalf("tweets = %d, want 2: %+v", len(resp.Tweets), resp.Tweets)
	}
	t0 := resp.Tweets[0]
	if t0.Id != srv.URL+"/users/alice/statuses/100" || t0.RootId != t0.Id {
		t.Errorf("t0 id/root = %q/%q, want top-level 100", t0.Id, t0.RootId)
	}
	if len(t0.ImageKeys) != 1 || t0.ImageKeys[0] != srv.URL+"/img.png" {
		t.Errorf("t0.ImageKeys = %v, want the media url", t0.ImageKeys)
	}
	t1 := resp.Tweets[1]
	if t1.Id != srv.URL+"/users/bob/statuses/200" { // boost unwrapped to the boosted status
		t.Errorf("t1.Id = %q, want boosted 200", t1.Id)
	}
	if t1.RetweetedBy == nil || *t1.RetweetedBy != "alice@"+host {
		t.Errorf("t1.RetweetedBy = %v, want alice@%s", t1.RetweetedBy, host)
	}
	if t1.UserId != "bob@other.example" {
		t.Errorf("t1.UserId = %q, want the boosted author", t1.UserId)
	}
	if !strings.Contains(resp.Cursor, "max_id=101") {
		t.Errorf("cursor = %q, want max_id=101", resp.Cursor)
	}
	// The profile timeline (Posts tab) must exclude replies, mirroring warpnet's
	// own timeline keyspace which never stores replies.
	if !strings.Contains(statusesQuery, "exclude_replies=true") {
		t.Errorf("statuses query = %q, want exclude_replies=true", statusesQuery)
	}
}

// A PUBLIC_GET_TWEETS request carrying root_id/parent_id is a thread-replies
// request (warpnet folded replies into this route): the gateway must return the
// note's replies as a flat TweetsResponse, not resolve an empty user handle.
func TestGetTweetsOrRepliesServesThreadReplies(t *testing.T) {
	g := testGateway(t)
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/statuses/100/context" {
			writeJSON(w, "application/json", map[string]any{
				"ancestors": []any{},
				"descendants": []any{
					map[string]any{
						"id": "101", "uri": srv.URL + "/users/bob/statuses/101",
						"content": "<p>a reply</p>", "created_at": "2024-01-01T00:00:00.000Z",
						"in_reply_to_id": "100", "account": map[string]any{"acct": "bob"},
					},
				},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	g.client = srv.Client()
	host := strings.TrimPrefix(srv.URL, "https://")

	b := newMastodonBridge(g, "node1")
	root := srv.URL + "/users/alice/statuses/100"
	resp, err := b.GetTweetsOrReplies(context.Background(), getAllTweetsEvent{RootId: root})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(resp.Tweets) != 1 {
		t.Fatalf("tweets = %d, want 1 (flat replies): %+v", len(resp.Tweets), resp.Tweets)
	}
	r0 := resp.Tweets[0]
	if r0.Id != srv.URL+"/users/bob/statuses/101" {
		t.Errorf("r0.Id = %q, want the reply uri", r0.Id)
	}
	if r0.UserId != "bob@"+host {
		t.Errorf("r0.UserId = %q, want bob@%s", r0.UserId, host)
	}
	if r0.RootId != root {
		t.Errorf("r0.RootId = %q, want %q", r0.RootId, root)
	}
}

// GetTweet and GetTweetStats must read a single status (and its real counts)
// from the Mastodon REST status endpoint.
func TestGetTweetAndStatsUseMastodonREST(t *testing.T) {
	g := testGateway(t)
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/statuses/100" {
			writeJSON(w, "application/json", map[string]any{
				"id": "100", "uri": srv.URL + "/users/alice/statuses/100",
				"content": "<p>hi</p>", "created_at": "2024-01-01T00:00:00.000Z",
				"account":       map[string]any{"acct": "alice"},
				"replies_count": float64(3), "reblogs_count": float64(2), "favourites_count": float64(5),
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	g.client = srv.Client()
	host := strings.TrimPrefix(srv.URL, "https://")
	b := newMastodonBridge(g, "node1")
	noteURL := srv.URL + "/users/alice/statuses/100"

	tw, err := b.GetTweet(context.Background(), noteURL)
	if err != nil {
		t.Fatalf("GetTweet err: %v", err)
	}
	if tw.Id != noteURL || tw.Text != "hi" || tw.UserId != "alice@"+host {
		t.Errorf("tweet = %+v, want mapped REST status", tw)
	}

	stats, err := b.GetTweetStats(context.Background(), noteURL)
	if err != nil {
		t.Fatalf("GetTweetStats err: %v", err)
	}
	if stats.ReactionsCount != 5 || stats.RetweetsCount != 2 || stats.RepliesCount != 3 {
		t.Errorf("stats = %+v, want favourites=5 boosts=2 replies=3", stats)
	}
	// Every favourite reads back as the default heart, and the client paints its
	// reaction chips from this breakdown — without it a favourited status shows
	// no chip at all despite the non-zero count.
	if stats.Reactions[domain.DefaultReaction] != 5 {
		t.Errorf("stats.Reactions = %v, want 5 hearts", stats.Reactions)
	}
}

// The /logs endpoint must stay disabled without a token, reject a wrong token,
// and otherwise serve the buffered log lines (via query param or Bearer header).
func TestLogsEndpoint(t *testing.T) {
	g := testGateway(t)
	ring := newLogRing(10)
	_ = ring.Fire(&log.Entry{Time: time.Now(), Level: log.InfoLevel, Message: "hello world"})
	g.logs = ring

	g.logsToken = ""
	rec := httptest.NewRecorder()
	g.handleLogs(rec, httptest.NewRequest(http.MethodGet, "/logs", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("no-token code = %d, want 404", rec.Code)
	}

	g.logsToken = "sekret"
	rec = httptest.NewRecorder()
	g.handleLogs(rec, httptest.NewRequest(http.MethodGet, "/logs?token=nope", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad-token code = %d, want 401", rec.Code)
	}

	rec = httptest.NewRecorder()
	g.handleLogs(rec, httptest.NewRequest(http.MethodGet, "/logs?token=sekret", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "hello world") {
		t.Fatalf("query-token code=%d body=%q", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/logs", nil)
	req.Header.Set("Authorization", "Bearer sekret")
	g.handleLogs(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "hello world") {
		t.Fatalf("bearer code=%d body=%q", rec.Code, rec.Body.String())
	}

	// The standalone listener must expose /logs and nothing else.
	h := g.logsHandler()
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/logs?token=sekret", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("logsHandler /logs code = %d, want 200", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/users/alice", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("logsHandler /users code = %d, want 404 (only /logs is served)", rec.Code)
	}
}

// Every outbound fetch a request fans out to must be logged under one trace id,
// indented and numbered, so the log maps a libp2p request to its REST calls.
func TestTracedFetchesShareOneID(t *testing.T) {
	g := testGateway(t)
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/accounts/lookup":
			writeJSON(w, "application/json", map[string]any{"id": "42"})
		case "/api/v1/accounts/42/statuses":
			writeJSON(w, "application/json", []any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	g.client = srv.Client()

	var buf bytes.Buffer
	prevOut, prevLvl := log.StandardLogger().Out, log.GetLevel()
	log.SetOutput(&buf)
	log.SetLevel(log.InfoLevel)
	defer func() { log.SetOutput(prevOut); log.SetLevel(prevLvl) }()

	ctx, tr := startTrace(context.Background())
	b := newMastodonBridge(g, "node1")
	if _, err := b.GetTweets(ctx, "alice@"+strings.TrimPrefix(srv.URL, "https://"), nil); err != nil {
		t.Fatalf("GetTweets: %v", err)
	}
	if tr.calls.Load() != 2 { // lookup + statuses
		t.Fatalf("traced calls = %d, want 2", tr.calls.Load())
	}
	out := buf.String()
	if !strings.Contains(out, "  ["+tr.id+"] #1 GET") || !strings.Contains(out, "["+tr.id+"] #2 GET") {
		t.Errorf("log missing padded, id-tagged fetch lines:\n%s", out)
	}
}

// The short-TTL GET cache must collapse repeated fetches of the same URL (the
// burst a timeline render triggers) onto a single network round-trip.
func TestSignedGetCacheDedupes(t *testing.T) {
	g := testGateway(t)
	g.getCache = expirable.NewLRU[string, cachedGet](getCacheSize, nil, getCacheTTL)
	var hits int32
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/statuses/100" {
			atomic.AddInt32(&hits, 1)
			writeJSON(w, "application/json", map[string]any{
				"id": "100", "uri": srv.URL + "/users/alice/statuses/100",
				"content": "<p>hi</p>", "account": map[string]any{"acct": "alice"},
				"replies_count": float64(1), "reblogs_count": float64(0), "favourites_count": float64(0),
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	g.client = srv.Client()
	b := newMastodonBridge(g, "node1")
	noteURL := srv.URL + "/users/alice/statuses/100"

	for i := 0; i < 3; i++ {
		if _, err := b.GetTweet(context.Background(), noteURL); err != nil {
			t.Fatalf("GetTweet %d: %v", i, err)
		}
	}
	if _, err := b.GetTweetStats(context.Background(), noteURL); err != nil {
		t.Fatalf("GetTweetStats: %v", err)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("origin hits = %d, want 1 (cache should dedupe)", got)
	}
}

// Concurrent identical signed GETs must collapse onto a single upstream
// round-trip via single-flight. The 1s getCache only dedupes sequential bursts
// (it fills after a request returns), so a reply thread that fans out
// overlapping author/stats fetches relies on single-flight. getCache is left
// nil so only single-flight can do the deduping here.
func TestSignedGetSingleFlightDedupes(t *testing.T) {
	g := testGateway(t)
	g.getCache = nil
	release := make(chan struct{})
	var hits int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		<-release // hold the in-flight request open so callers must coalesce
		writeJSON(w, "application/json", map[string]any{"ok": true})
	}))
	defer srv.Close()
	g.client = srv.Client()

	const n = 8
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = g.signedGet(context.Background(), srv.URL+"/thing", "application/json")
		}()
	}
	// Give every goroutine time to enter the single flight before releasing the
	// one in-flight request; with getCache nil, any that didn't coalesce would
	// hit the origin separately.
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("origin hits = %d, want 1 (single-flight should collapse concurrent GETs)", got)
	}
}

// Warpnet paginates GET_TWEETS with the requesting node's own datastore cursor
// (e.g. "/TWEETS/<user>/<seq>/<noteURL>"), not the AP "next" URL the gateway
// returned. The gateway must ignore such a cursor and serve the first outbox
// page rather than dereference it (which fails the https check, breaking the
// whole response).
func TestGetTweetsIgnoresDatastoreCursor(t *testing.T) {
	g := testGateway(t)
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/.well-known/webfinger":
			writeJSON(w, contentTypeJRD, map[string]any{
				"links": []any{map[string]any{"rel": "self", "href": srv.URL + "/users/alice"}},
			})
		case r.URL.Path == "/users/alice":
			writeJSON(w, contentTypeAP, map[string]any{
				"type": "Person", "id": srv.URL + "/users/alice", "outbox": srv.URL + "/outbox",
			})
		case r.URL.Path == "/outbox" && r.URL.RawQuery == "":
			writeJSON(w, contentTypeAP, map[string]any{
				"type": "OrderedCollection", "id": srv.URL + "/outbox",
				"first": srv.URL + "/outbox?page=true",
			})
		case r.URL.Path == "/outbox" && r.URL.RawQuery == "page=true":
			writeJSON(w, contentTypeAP, map[string]any{
				"type": "OrderedCollectionPage", "id": srv.URL + "/outbox?page=true",
				"orderedItems": []any{map[string]any{
					"type": typeNote, "id": srv.URL + "/statuses/1",
					"attributedTo": srv.URL + "/users/alice", "content": "hello",
				}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	g.client = srv.Client()

	b := newMastodonBridge(g, "node1")
	handle := "alice@" + strings.TrimPrefix(srv.URL, "https://")
	cursor := "/TWEETS/" + handle + "/9223372035074277473/" + srv.URL + "/statuses/1"
	resp, err := b.GetTweets(context.Background(), handle, &cursor)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(resp.Tweets) != 1 || resp.Tweets[0].Id != srv.URL+"/statuses/1" {
		t.Fatalf("tweets = %+v, want first-page note", resp.Tweets)
	}
}

// A federated reply id carries "?parent=<encoded url>"; the query must not leak
// into the tweet id or inbound Like/Announce/Undo against it are dropped.
func TestParseLocalStatusReplyParentQuery(t *testing.T) {
	g := testGateway(t) // host gw.example
	parent := "https://mastodon.social/users/bob/statuses/123"
	statusURL := "https://gw.example/users/alice/statuses/r1?" + url.Values{replyParentQuery: {parent}}.Encode()
	owner, tweetID, ok := g.parseLocalStatus(statusURL)
	if !ok || owner != "alice" || tweetID != "r1" {
		t.Fatalf("parseLocalStatus(%q) = %q, %q, %v", statusURL, owner, tweetID, ok)
	}
}

// A Fediverse follower reaches warpnet as an "ap:" id (base64url of the actor
// URL), and warpnet asks for that profile with the id verbatim. The bridge must
// decode it to the actor URL instead of trying to WebFinger it as a handle,
// otherwise the UI renders the raw id.
func TestGetUserResolvesAPFollowerID(t *testing.T) {
	g := testGateway(t)
	var srv *httptest.Server
	var webfingered bool
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/users/warpnet":
			writeJSON(w, contentTypeAP, map[string]any{
				"id": srv.URL + "/users/warpnet", "type": "Person",
				"preferredUsername": "warpnet", "name": "Warpnet",
				"summary": "<p>bio</p>",
			})
		case "/.well-known/webfinger":
			webfingered = true
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	g.client = srv.Client()

	b := newMastodonBridge(g, "node1")
	id := encodeActorID(srv.URL + "/users/warpnet")
	u, err := b.GetUserBrief(context.Background(), id)
	if err != nil {
		t.Fatalf("GetUserBrief(%q): %v", id, err)
	}
	if webfingered {
		t.Errorf("ap: id was webfingered; it already carries the actor URL")
	}
	if u.Username != "Warpnet" {
		t.Errorf("Username = %q, want the resolved actor name", u.Username)
	}
	if u.Website == nil || *u.Website != srv.URL+"/users/warpnet" {
		t.Errorf("Website = %v, want the actor URL", u.Website)
	}
	if u.Id != id {
		t.Errorf("Id = %q, want the requested id %q", u.Id, id)
	}
}

// The gateway must never resolve itself through the Fediverse: our own users are
// served natively by Warpnet, so looping a request out to Mastodon and back here
// would return a local user as a foreign Mastodon account.
func TestSelfLoopRefused(t *testing.T) {
	g := testGateway(t) // host gw.example
	b := newMastodonBridge(g, "node1")
	ctx := context.Background()

	t.Run("own handle is not resolvable", func(t *testing.T) {
		if _, err := b.GetUser(ctx, "alice@gw.example"); !errors.Is(err, errSelfTarget) {
			t.Errorf("GetUser: err = %v, want errSelfTarget", err)
		}
		if _, err := b.GetTweets(ctx, "alice@gw.example", nil); !errors.Is(err, errSelfTarget) {
			t.Errorf("GetTweets: err = %v, want errSelfTarget", err)
		}
		if _, err := b.GetFollowers(ctx, "alice@GW.EXAMPLE", nil); !errors.Is(err, errSelfTarget) {
			t.Errorf("GetFollowers: err = %v, want errSelfTarget", err)
		}
		if err := b.Follow(ctx, "alice", "alice@gw.example", false); !errors.Is(err, errSelfTarget) {
			t.Errorf("Follow: err = %v, want errSelfTarget", err)
		}
		if _, err := b.GetUser(ctx, encodeActorID(g.actorID("alice"))); !errors.Is(err, errSelfTarget) {
			t.Errorf("GetUser(ap: self): err = %v, want errSelfTarget", err)
		}
	})

	t.Run("own urls are neither fetched nor delivered to", func(t *testing.T) {
		if _, err := g.fetchActor(ctx, g.actorID("alice")); !errors.Is(err, errSelfTarget) {
			t.Errorf("fetchActor: err = %v, want errSelfTarget", err)
		}
		if _, err := g.apGetJSON(ctx, g.baseURL()+"/.well-known/webfinger", contentTypeJRD); !errors.Is(err, errSelfTarget) {
			t.Errorf("apGetJSON: err = %v, want errSelfTarget", err)
		}
		if _, _, err := g.fetchMedia(ctx, g.baseURL()+pathMedia+"x"); !errors.Is(err, errSelfTarget) {
			t.Errorf("fetchMedia: err = %v, want errSelfTarget", err)
		}
		if err := g.postSigned(ctx, "alice", g.baseURL()+pathInbox, activity{}); !errors.Is(err, errSelfTarget) {
			t.Errorf("postSigned: err = %v, want errSelfTarget", err)
		}
		// A status of ours is not a Mastodon note either (Like/Reply/Delete).
		if _, err := b.React(ctx, "alice", g.actorID("alice")+pathStatuses+"1", domain.DefaultReaction, false); !errors.Is(err, errSelfTarget) {
			t.Errorf("React: err = %v, want errSelfTarget", err)
		}
	})

	// A Warpnet user who follows a Fediverse account shows up in that account's
	// follower list as our own actor URL; it must not be offered as a profile.
	t.Run("own actor is dropped from a remote follower list", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/.well-known/webfinger":
				writeJSON(w, contentTypeJRD, map[string]any{
					"links": []any{map[string]any{"rel": "self", "href": "https://" + r.Host + "/users/bob"}},
				})
			case "/users/bob":
				writeJSON(w, contentTypeAP, map[string]any{
					"id": "https://" + r.Host + "/users/bob", "followers": "https://" + r.Host + "/users/bob/followers",
				})
			case "/users/bob/followers":
				writeJSON(w, contentTypeAP, map[string]any{
					"orderedItems": []any{g.actorID("alice"), "https://other.example/users/carol"},
				})
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()
		g.client = srv.Client()
		defer func() { g.client = http.DefaultClient }()

		resp, err := b.GetFollowers(ctx, "bob@"+strings.TrimPrefix(srv.URL, "https://"), nil)
		if err != nil {
			t.Fatalf("GetFollowers: %v", err)
		}
		if len(resp.Followers) != 1 || resp.Followers[0] != "carol@other.example" {
			t.Errorf("Followers = %v, want only the remote follower", resp.Followers)
		}
	})
}
