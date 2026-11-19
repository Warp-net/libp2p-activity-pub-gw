// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package fediversetest

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Instance is an in-process Fediverse server: a TLS httptest instance that
// answers the ActivityPub and Mastodon-REST endpoints the gateway dereferences,
// records what was fetched, and captures signed inbox deliveries. It keeps the
// federation tests hermetic — no test ever leaves the process.
type Instance struct {
	srv *httptest.Server

	mu       sync.Mutex
	handlers map[string]http.HandlerFunc
	hits     map[string]int
	posts    []Delivery
}

// Delivery is one activity a peer inbox received.
type Delivery struct {
	Path string
	Doc  map[string]any
	Sig  string
}

func NewInstance(t *testing.T) *Instance {
	t.Helper()
	f := &Instance{handlers: map[string]http.HandlerFunc{}, hits: map[string]int{}}
	f.srv = httptest.NewTLSServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *Instance) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.hits[r.URL.Path]++
	h, ok := f.handlers[r.URL.Path]
	f.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	h(w, r)
}

// Client trusts this instance's TLS cert.
func (f *Instance) Client() *http.Client { return f.srv.Client() }

func (f *Instance) URL(path string) string { return f.srv.URL + path }
func (f *Instance) Host() string           { return strings.TrimPrefix(f.srv.URL, "https://") }

func (f *Instance) On(path string, h http.HandlerFunc) *Instance {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers[path] = h
	return f
}

// ServeDoc answers path with v encoded as JSON under contentType.
func (f *Instance) ServeDoc(path, contentType string, v any) *Instance {
	return f.On(path, func(w http.ResponseWriter, _ *http.Request) {
		fakeJSON(w, contentType, v)
	})
}

func (f *Instance) HitCount(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits[path]
}

// Inbox registers path as an inbox that accepts and records deliveries.
func (f *Instance) Inbox(path string) *Instance {
	return f.On(path, func(w http.ResponseWriter, r *http.Request) {
		var doc map[string]any
		_ = json.NewDecoder(r.Body).Decode(&doc)
		f.mu.Lock()
		f.posts = append(f.posts, Delivery{Path: r.URL.Path, Doc: doc, Sig: r.Header.Get("Signature")})
		f.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	})
}

func (f *Instance) Delivered() []Delivery {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Delivery, len(f.posts))
	copy(out, f.posts)
	return out
}

// Actor registers a minimal actor document at /users/{name} plus its inbox, and
// returns the actor URL. pub, when non-nil, is published as the actor's signing
// key so a signature made with it verifies against this document.
func (f *Instance) Actor(name string, pub *rsa.PublicKey) string {
	id := f.URL("/users/" + name)
	doc := map[string]any{
		"@context":          []any{"https://www.w3.org/ns/activitystreams", "https://w3id.org/security/v1"},
		"id":                id,
		"type":              "Person",
		"preferredUsername": name,
		"inbox":             id + "/inbox",
		"outbox":            id + "/outbox",
		"followers":         id + "/followers",
		"following":         id + "/following",
	}
	if pub != nil {
		if der, err := x509.MarshalPKIXPublicKey(pub); err == nil {
			doc["publicKey"] = map[string]any{
				"id": id + "#main-key", "owner": id,
				"publicKeyPem": string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})),
			}
		}
	}
	f.ServeDoc("/users/"+name, fakeContentTypeAP, doc)
	f.Inbox("/users/" + name + "/inbox")
	return id
}

// WebfingerFor makes handle "{name}@{host}" resolvable to the given actor URL.
func (f *Instance) WebfingerFor(name, actorURL string) *Instance {
	return f.On("/.well-known/webfinger", func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Query().Get("resource"), name) {
			http.NotFound(w, r)
			return
		}
		fakeJSON(w, "application/jrd+json", map[string]any{
			"subject": "acct:" + name + "@" + f.Host(),
			"links":   []any{map[string]any{"rel": "self", "type": fakeContentTypeAP, "href": actorURL}},
		})
	})
}

// Note is a minimal ActivityPub Note document.
func Note(id, author, content string) map[string]any {
	return map[string]any{
		"type": "Note", "id": id, "attributedTo": author,
		"content": content, "published": "2024-01-01T00:00:00Z",
	}
}

const fakeContentTypeAP = "application/activity+json"

func fakeJSON(w http.ResponseWriter, contentType string, v any) {
	bt, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "marshal", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	_, _ = w.Write(bt)
}
