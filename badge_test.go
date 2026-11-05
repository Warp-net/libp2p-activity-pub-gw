// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleStatic(t *testing.T) {
	srv := httptest.NewServer(testGateway(t).routes())
	defer srv.Close()

	t.Run("serves the embedded badge", func(t *testing.T) {
		resp, err := http.Get(srv.URL + warpnetBadgePath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if ct := resp.Header.Get(headerContentType); !strings.HasPrefix(ct, "image/png") {
			t.Fatalf("content-type = %q — Mastodon reads it when caching the emoji", ct)
		}
		if cc := resp.Header.Get("Cache-Control"); cc != "public, max-age=86400" {
			t.Fatalf("cache-control = %q", cc)
		}
	})

	t.Run("serves the sponsored teaser", func(t *testing.T) {
		resp, err := http.Get(srv.URL + sponsoredTeaserPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if ct := resp.Header.Get(headerContentType); !strings.HasPrefix(ct, "image/png") {
			t.Fatalf("content-type = %q", ct)
		}
	})

	t.Run("unknown static path is 404", func(t *testing.T) {
		resp, err := http.Get(srv.URL + pathStatic + "nope.png")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
	})
}

func TestBadgedSummary(t *testing.T) {
	if got := badgedSummary(""); got != warpnetBioPrefix {
		t.Fatalf("empty bio = %q, want just the notice", got)
	}
	got := badgedSummary(`a <script>alert(1)</script> bio`)
	if !strings.HasPrefix(got, warpnetBioPrefix) {
		t.Fatalf("the notice must come first: %q", got)
	}
	if strings.Contains(got, "<script>") {
		t.Fatalf("the bio must be escaped, Mastodon renders the summary as HTML: %q", got)
	}
	if !strings.Contains(got, "&lt;script&gt;") {
		t.Fatalf("escaped bio missing: %q", got)
	}
}

func TestWarpnetActorTag(t *testing.T) {
	g := testGateway(t)
	tag := g.warpnetActorTag()
	if tag.Type != "Emoji" || tag.Name != warpnetEmojiShortcode {
		t.Fatalf("tag = %+v", tag)
	}
	if tag.ID != "https://gw.example/emoji/warpnet" {
		t.Fatalf("id = %q", tag.ID)
	}
	if tag.Icon == nil || tag.Icon.URL != "https://gw.example"+warpnetBadgePath || tag.Icon.MediaType != "image/png" {
		t.Fatalf("icon = %+v", tag.Icon)
	}
}

func TestWarpnetNetworkField(t *testing.T) {
	f := warpnetNetworkField()
	if f.Type != "PropertyValue" || f.Name != "Network" {
		t.Fatalf("field = %+v", f)
	}
	if !strings.Contains(f.Value, "https://warpnet.site") {
		t.Fatalf("value = %q", f.Value)
	}
}
