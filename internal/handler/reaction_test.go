// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package handler

import (
	"context"
	"testing"

	"github.com/Warp-net/bridge-gateway/internal/warp"
)

// The tally is attributed to the emoji the reactor named, so the client
// repaints its chips without a second round trip; an unreact names none.
func TestBuildReactionsCount(t *testing.T) {
	got := buildReactionsCount(warp.DefaultReaction, 3)
	if got.Count != 3 || got.Reactions[warp.DefaultReaction] != 3 {
		t.Fatalf("react = %+v", got)
	}
	if got := buildReactionsCount("", 2); got.Count != 2 || got.Reactions != nil {
		t.Fatalf("unreact = %+v, want the total only", got)
	}
	if got := buildReactionsCount(warp.DefaultReaction, 0); got.Reactions != nil {
		t.Fatalf("zero = %+v, want no breakdown", got)
	}
}

func TestReactionHandlersRefuseAMalformedBody(t *testing.T) {
	for name, h := range map[string]func(context.Context, []byte) (any, error){
		"react":   StreamReactionHandler(nil, nil, nil),
		"unreact": StreamUnreactionHandler(nil, nil, nil),
	} {
		if _, err := h(context.Background(), []byte("{{{")); err == nil {
			t.Fatalf("%s: a malformed body must be an error", name)
		}
	}
}
