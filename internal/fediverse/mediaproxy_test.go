// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package fediverse

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Warp-net/bridge-gateway/internal/warp"
)

func TestEncodeDecodeMediaRef(t *testing.T) {
	// The key is a data URL, so the reference must survive ':' ',' and '/'.
	const user, key = "01USER0000000000000000000", "image/png,AAAA/BBB+CCC"
	ref := EncodeMediaRef(user, key)
	if strings.ContainsAny(ref, "/+=") {
		t.Fatalf("ref %q must stay a single URL path segment", ref)
	}
	gotUser, gotKey, ok := decodeMediaRef(ref)
	if !ok || gotUser != user || gotKey != key {
		t.Fatalf("round-trip = (%q, %q, %v)", gotUser, gotKey, ok)
	}
}

func TestDecodeMediaRefRejectsGarbage(t *testing.T) {
	if _, _, ok := decodeMediaRef("!!!not base64!!!"); ok {
		t.Fatal("bad base64 must not decode")
	}
	// Valid base64 without the separator is not a media reference.
	if _, _, ok := decodeMediaRef(base64.RawURLEncoding.EncodeToString([]byte("nosep"))); ok {
		t.Fatal("a ref without the separator must not decode")
	}
}

// errRequester fails every route, standing in for an unreachable node.
type errRequester struct{ err error }

func (e errRequester) Request(string, any) ([]byte, error) { return nil, e.err }
func (e errRequester) RequestUser(_, route string, payload any) ([]byte, error) {
	return e.Request(route, payload)
}

// rawRequester answers every route with a fixed body.
type rawRequester struct{ body []byte }

func (r rawRequester) Request(string, any) ([]byte, error) { return r.body, nil }
func (r rawRequester) RequestUser(_, route string, payload any) ([]byte, error) {
	return r.Request(route, payload)
}

func TestHandleMediaErrors(t *testing.T) {
	png := []byte{0x89, 'P', 'N', 'G'}
	ref := EncodeMediaRef("alice", "avatar-key")

	t.Run("no node", func(t *testing.T) {
		g := testGateway(t)
		w := httptest.NewRecorder()
		g.handleMedia(w, httptest.NewRequest(http.MethodGet, PathMedia+ref, nil))
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", w.Code)
		}
	})

	t.Run("undecodable reference", func(t *testing.T) {
		g := testGateway(t)
		g.Requester = &fakeRequester{}
		w := httptest.NewRecorder()
		g.handleMedia(w, httptest.NewRequest(http.MethodGet, PathMedia+"!!!", nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", w.Code)
		}
	})

	t.Run("empty key", func(t *testing.T) {
		g := testGateway(t)
		g.Requester = &fakeRequester{}
		w := httptest.NewRecorder()
		g.handleMedia(w, httptest.NewRequest(http.MethodGet, PathMedia+EncodeMediaRef("alice", ""), nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", w.Code)
		}
	})

	t.Run("node error", func(t *testing.T) {
		g := testGateway(t)
		g.Requester = errRequester{err: errors.New("no node")}
		w := httptest.NewRecorder()
		g.handleMedia(w, httptest.NewRequest(http.MethodGet, PathMedia+ref, nil))
		if w.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502", w.Code)
		}
	})

	t.Run("empty file in the response", func(t *testing.T) {
		g := testGateway(t)
		g.Requester = &fakeRequester{imageFile: ""}
		w := httptest.NewRecorder()
		g.handleMedia(w, httptest.NewRequest(http.MethodGet, PathMedia+ref, nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", w.Code)
		}
	})

	t.Run("a file with no mime separator", func(t *testing.T) {
		g := testGateway(t)
		g.Requester = &fakeRequester{imageFile: "no-separator-here"}
		w := httptest.NewRecorder()
		g.handleMedia(w, httptest.NewRequest(http.MethodGet, PathMedia+ref, nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", w.Code)
		}
	})

	t.Run("undecodable base64 payload", func(t *testing.T) {
		g := testGateway(t)
		g.Requester = &fakeRequester{imageFile: "data:image/png;base64,!!!!"}
		w := httptest.NewRecorder()
		g.handleMedia(w, httptest.NewRequest(http.MethodGet, PathMedia+ref, nil))
		if w.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502", w.Code)
		}
	})

	t.Run("a malformed node envelope", func(t *testing.T) {
		g := testGateway(t)
		g.Requester = rawRequester{body: []byte("not json")}
		w := httptest.NewRecorder()
		g.handleMedia(w, httptest.NewRequest(http.MethodGet, PathMedia+ref, nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", w.Code)
		}
	})

	t.Run("the bare <mime>,<base64> form is tolerated", func(t *testing.T) {
		g := testGateway(t)
		g.Requester = &fakeRequester{imageFile: "image/png," + base64.StdEncoding.EncodeToString(png)}
		w := httptest.NewRecorder()
		g.handleMedia(w, httptest.NewRequest(http.MethodGet, PathMedia+ref, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d", w.Code)
		}
		if ct := w.Header().Get(HeaderContentType); ct != "image/png" {
			t.Fatalf("content-type = %q", ct)
		}
		if !strings.EqualFold(w.Body.String(), string(png)) {
			t.Fatalf("body = %q", w.Body.String())
		}
	})

	t.Run("the requested key reaches the node", func(t *testing.T) {
		g := testGateway(t)
		fr := &fakeRequester{imageFile: "data:image/png;base64," + base64.StdEncoding.EncodeToString(png)}
		g.Requester = fr
		w := httptest.NewRecorder()
		g.handleMedia(w, httptest.NewRequest(http.MethodGet, PathMedia+ref, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d", w.Code)
		}
		ev, ok := fr.lastPayload.(warp.GetImageEvent)
		if !ok {
			t.Fatalf("payload type %T", fr.lastPayload)
		}
		if ev.UserId != "alice" || ev.Key != "avatar-key" {
			t.Fatalf("payload = %+v", ev)
		}
		if fr.lastRoute != warp.RouteGetImage {
			t.Fatalf("route = %q", fr.lastRoute)
		}
	})
}

// A round-trip through the real route table: the actor's avatar URL the gateway
// publishes must be fetchable at /media and return the node's bytes.
func TestMediaURLFromActorResolves(t *testing.T) {
	g := testGateway(t)
	png := []byte{0x89, 'P', 'N', 'G'}
	g.Requester = &fakeRequester{imageFile: "data:image/png;base64," + base64.StdEncoding.EncodeToString(png)}
	g.source = StaticSource{User: WarpnetUser{
		ID: "alice", PreferredUsername: "alice", Avatar: "avatar-key",
	}}

	srv := httptest.NewServer(g.Routes())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/users/alice")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var a actor
	if derr := json.NewDecoder(resp.Body).Decode(&a); derr != nil {
		t.Fatal(derr)
	}
	if a.Icon == nil {
		t.Fatal("the actor must advertise an icon when the profile has an avatar")
	}
	mediaPath := strings.TrimPrefix(a.Icon.URL, g.baseURL())

	imgResp, err := http.Get(srv.URL + mediaPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = imgResp.Body.Close() }()
	if imgResp.StatusCode != http.StatusOK {
		t.Fatalf("media status = %d for %s", imgResp.StatusCode, mediaPath)
	}
}
