// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"strings"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

func fire(t *testing.T, lr *logRing, msg string) {
	t.Helper()
	if err := lr.Fire(&log.Entry{Time: time.Unix(0, 0).UTC(), Level: log.InfoLevel, Message: msg}); err != nil {
		t.Fatalf("Fire: %v", err)
	}
}

func TestLogRing(t *testing.T) {
	t.Run("hooks every level", func(t *testing.T) {
		if got := newLogRing(2).Levels(); len(got) != len(log.AllLevels) {
			t.Fatalf("levels = %v, want all of them so nothing is missed", got)
		}
	})

	t.Run("empty ring has no lines", func(t *testing.T) {
		if got := newLogRing(4).lines(""); len(got) != 0 {
			t.Fatalf("lines = %v", got)
		}
	})

	t.Run("partial ring returns what was written, oldest first", func(t *testing.T) {
		lr := newLogRing(4)
		fire(t, lr, "one")
		fire(t, lr, "two")
		got := lr.lines("")
		if len(got) != 2 {
			t.Fatalf("lines = %v", got)
		}
		if !strings.HasSuffix(got[0], "one") || !strings.HasSuffix(got[1], "two") {
			t.Fatalf("order = %v", got)
		}
		if !strings.Contains(got[0], "info") {
			t.Fatalf("the level must be rendered: %q", got[0])
		}
	})

	t.Run("a full ring drops the oldest lines", func(t *testing.T) {
		lr := newLogRing(3)
		for _, m := range []string{"a", "b", "c", "d", "e"} {
			fire(t, lr, m)
		}
		got := lr.lines("")
		if len(got) != 3 {
			t.Fatalf("lines = %d, want the ring size", len(got))
		}
		for i, want := range []string{"c", "d", "e"} {
			if !strings.HasSuffix(got[i], want) {
				t.Fatalf("lines = %v, want the last three in order", got)
			}
		}
	})

	t.Run("exactly full wraps to the start", func(t *testing.T) {
		lr := newLogRing(2)
		fire(t, lr, "a")
		fire(t, lr, "b")
		got := lr.lines("")
		if len(got) != 2 || !strings.HasSuffix(got[0], "a") || !strings.HasSuffix(got[1], "b") {
			t.Fatalf("lines = %v", got)
		}
	})

	t.Run("concurrent writers and readers stay consistent", func(t *testing.T) {
		lr := newLogRing(64)
		var wg sync.WaitGroup
		for i := range 32 {
			wg.Add(2)
			go func() { defer wg.Done(); fire(t, lr, "w") }()
			go func() { defer wg.Done(); _ = lr.lines(""); _ = i }()
		}
		wg.Wait()
		if got := len(lr.lines("")); got != 32 {
			t.Fatalf("lines = %d, want 32", got)
		}
	})
}

// The gateway runs a node per network in one process, so its logs interleave.
// A reader must be able to take one network apart from the other — while still
// seeing the lines that belong to no network (the ActivityPub surface, the
// Mastodon bridge), since those carry the context around a failure.
func TestLogRingSeparatesNetworks(t *testing.T) {
	lr := newLogRing(16)
	fireOn := func(network, msg string) {
		e := log.WithField(logFieldNetwork, network)
		if network == "" {
			e = log.NewEntry(log.StandardLogger())
		}
		e.Message = msg
		e.Level = log.InfoLevel
		if err := lr.Fire(e); err != nil {
			t.Fatalf("fire: %v", err)
		}
	}
	fireOn("warpnet", "mainnet dialled a member")
	fireOn("testnet", "testnet dialled a member")
	fireOn("", "http: GET /users/alice -> 200")

	joined := func(lines []string) string { return strings.Join(lines, "\n") }

	t.Run("a network gets its own lines plus the shared ones", func(t *testing.T) {
		got := joined(lr.lines("warpnet"))
		if !strings.Contains(got, "mainnet dialled") {
			t.Fatalf("warpnet's own line missing:\n%s", got)
		}
		if strings.Contains(got, "testnet dialled") {
			t.Fatalf("the other network leaked in:\n%s", got)
		}
		if !strings.Contains(got, "http: GET") {
			t.Fatalf("network-independent line dropped:\n%s", got)
		}
	})

	t.Run("the tag is rendered so a line says where it came from", func(t *testing.T) {
		if got := joined(lr.lines("testnet")); !strings.Contains(got, "[testnet] testnet dialled") {
			t.Fatalf("missing [network] tag:\n%s", got)
		}
	})

	t.Run("unfiltered returns every network", func(t *testing.T) {
		if got := lr.lines(""); len(got) != 3 {
			t.Fatalf("lines = %d, want all 3", len(got))
		}
	})
}
