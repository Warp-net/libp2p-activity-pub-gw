// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"strings"
	"testing"
)

func TestParseLocalStatus(t *testing.T) {
	g := testGateway(t) // host gw.example
	base := g.baseURL() + pathUsers

	t.Run("accepts our own status urls", func(t *testing.T) {
		cases := []struct {
			in          string
			owner, twID string
		}{
			{base + "alice" + pathStatuses + "t1", "alice", "t1"},
			{base + "alice" + pathStatuses + "t1/replies", "alice", "t1"},
			{base + "alice" + pathStatuses + "t1#frag", "alice", "t1"},
			// A federated reply id carries "?parent=..."; the query must be dropped
			// before the path is split.
			{base + "alice" + pathStatuses + "t1?parent=https%3A%2F%2Fm%2F1", "alice", "t1"},
		}
		for _, c := range cases {
			owner, id, ok := g.parseLocalStatus(c.in)
			if !ok || owner != c.owner || id != c.twID {
				t.Errorf("parseLocalStatus(%q) = (%q, %q, %v)", c.in, owner, id, ok)
			}
		}
	})

	t.Run("refuses anything that is not ours", func(t *testing.T) {
		for _, in := range []string{
			"https://other.example/users/alice/statuses/t1", // another host
			base + "alice",                // no status
			base + pathStatuses + "t1",    // no owner
			base + "alice" + pathStatuses, // no id
			"",
		} {
			if _, _, ok := g.parseLocalStatus(in); ok {
				t.Errorf("parseLocalStatus(%q) unexpectedly ok", in)
			}
		}
	})
}

func TestIngestedNoteID(t *testing.T) {
	// Keying by the note's own AP id keeps the reply resolvable and makes a
	// redelivery update the same row instead of adding a second one.
	if got := ingestedNoteID(map[string]any{"id": "https://m/notes/1"}); got != "https://m/notes/1" {
		t.Fatalf("got %q", got)
	}
	if got := ingestedNoteID(map[string]any{"url": "https://m/@bob/1"}); got != "https://m/@bob/1" {
		t.Fatalf("url fallback = %q", got)
	}
	got := ingestedNoteID(map[string]any{})
	if len(got) != 16 {
		t.Fatalf("last-resort id = %q, want a random token", got)
	}
	if other := ingestedNoteID(map[string]any{}); other == got {
		t.Fatal("the fallback id must not repeat")
	}
}

func TestBridgedUserID(t *testing.T) {
	// A handle renders as a profile in Warpnet; a raw "ap:<base64url>" id does not,
	// so the handle is preferred wherever one can be derived.
	if got := bridgedUserID("https://m.example/users/bob"); got != "bob@m.example" {
		t.Fatalf("got %q", got)
	}
	if got := bridgedUserID("https://m.example/@bob"); got != "bob@m.example" {
		t.Fatalf("got %q", got)
	}
	// An actor url with no derivable handle falls back to the reversible encoding.
	raw := "not-a-url"
	got := bridgedUserID(raw)
	if !strings.HasPrefix(got, apFollowerPrefix) {
		t.Fatalf("got %q, want the encoded fallback", got)
	}
	decoded, err := decodeActorID(got)
	if err != nil || decoded != raw {
		t.Fatalf("fallback does not round-trip: (%q, %v)", decoded, err)
	}
}

func TestHandleFromActorURLFallsBackToTheRawURL(t *testing.T) {
	cases := map[string]string{
		"https://m.example/users/bob": "bob@m.example",
		"https://m.example/@bob":      "bob@m.example",
		"https://m.example/":          "https://m.example/", // no name segment
		"/relative":                   "/relative",          // no host
		"://bad":                      "://bad",
	}
	for in, want := range cases {
		if got := handleFromActorURL(in); got != want {
			t.Errorf("handleFromActorURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTranslateInboundRejectsIncompleteActivities(t *testing.T) {
	g := testGateway(t)
	status := g.actorID("alice") + pathStatuses + "t1"

	cases := map[string]map[string]any{
		"no actor":                     {"type": typeLike, "object": status},
		"unknown type":                 {"type": "Flag", "actor": "https://m/users/bob"},
		"like of a foreign status":     {"type": typeLike, "actor": "https://m/users/bob", "object": "https://o/1"},
		"announce of a foreign status": {"type": typeAnnounce, "actor": "https://m/users/bob", "object": "https://o/1"},
		"create without an object":     {"type": typeCreate, "actor": "https://m/users/bob"},
		"create of an unrelated note":  {"type": typeCreate, "actor": "https://m/users/bob", "object": map[string]any{"content": "hi"}},
		"undo without an object":       {"type": typeUndo, "actor": "https://m/users/bob"},
		"undo of an unknown type":      {"type": typeUndo, "actor": "https://m/users/bob", "object": map[string]any{"type": "Flag"}},
		"undo follow without a target": {
			"type": typeUndo, "actor": "https://m/users/bob",
			"object": map[string]any{"type": typeFollow, "object": "https://other/x"},
		},
		"undo like of a foreign status": {
			"type": typeUndo, "actor": "https://m/users/bob",
			"object": map[string]any{"type": typeLike, "object": "https://o/1"},
		},
		"undo announce of a foreign status": {
			"type": typeUndo, "actor": "https://m/users/bob",
			"object": map[string]any{"type": typeAnnounce, "object": "https://o/1"},
		},
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if route, _, _, ok := g.translateInbound(raw); ok {
				t.Fatalf("translated to %q, want it refused", route)
			}
		})
	}
}

// Every translated activity must name the local Warpnet user it targets: with
// several networks joined that user is what confines the write to the one
// network that has them (see gateway.requestForUser). A missing one would let an
// inbound favourite or reply fan out to every network and land in the wrong one.
func TestTranslateInboundReportsTheTargetedLocalUser(t *testing.T) {
	g := testGateway(t) // host gw.example
	const actor = "https://m/users/bob"
	const status = "https://gw.example/users/alice/statuses/t1"

	cases := map[string]map[string]any{
		"favourite": {"type": typeLike, "actor": actor, "object": status},
		"boost":     {"type": typeAnnounce, "actor": actor, "object": status},
		"reply": {"type": typeCreate, "actor": actor, "object": map[string]any{
			"type": typeNote, "id": "https://m/users/bob/statuses/9",
			"content": "hi", "inReplyTo": status,
		}},
		"quote": {"type": typeCreate, "actor": actor, "object": map[string]any{
			"type": typeNote, "id": "https://m/users/bob/statuses/9",
			"content": "look", "quoteUrl": status,
		}},
		"unfollow": {"type": typeUndo, "actor": actor, "object": map[string]any{
			"type": typeFollow, "object": "https://gw.example/users/alice",
		}},
		"unfavourite": {"type": typeUndo, "actor": actor, "object": map[string]any{
			"type": typeLike, "object": status,
		}},
		"unboost": {"type": typeUndo, "actor": actor, "object": map[string]any{
			"type": typeAnnounce, "object": status,
		}},
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			route, _, localUser, ok := g.translateInbound(raw)
			if !ok {
				t.Fatalf("not translated")
			}
			if localUser != "alice" {
				t.Fatalf("%s -> %s: local user = %q, want alice", name, route, localUser)
			}
		})
	}
}
