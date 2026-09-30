// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"encoding/json"
	"testing"
)

// Whatever a sponsored tweet carries when it reaches the gateway, the Fediverse
// gets the teaser picture and nothing else.
func TestBuildNoteFederatesASponsoredTweetAsItsTeaser(t *testing.T) {
	g := testGateway(t)
	var tw tweet
	wire := `{"id":"t1","user_id":"alice","text":"the paid words","image_keys":["k1"],` +
		`"created_at":"2026-09-29T10:00:00Z","price":{"amount":"1.5","units":1500000}}`
	if err := json.Unmarshal([]byte(wire), &tw); err != nil {
		t.Fatal(err)
	}

	n := g.buildNote("alice", tw)
	if n.Content != "" {
		t.Fatalf("content = %q, want none", n.Content)
	}
	if len(n.Attachment) != 1 || n.Attachment[0].URL != "https://gw.example"+sponsoredTeaserPath {
		t.Fatalf("attachment = %+v, want the teaser picture only", n.Attachment)
	}
}
