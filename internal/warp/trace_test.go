// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package warp

import (
	"context"
	"testing"
	"time"
)

func TestStartTrace(t *testing.T) {
	ctx, tr := StartTrace(context.Background())
	if tr == nil || len(tr.ID) != 8 {
		t.Fatalf("trace = %+v, want an 8-char id", tr)
	}
	if got := traceFrom(ctx); got != tr {
		t.Fatalf("traceFrom returned %v, want the trace startTrace stamped", got)
	}
	// Each request gets its own id so interleaved pipelines stay distinguishable.
	_, other := StartTrace(context.Background())
	if other.ID == tr.ID {
		t.Fatal("two traces share an id")
	}
}

func TestTraceFromUntracedContext(t *testing.T) {
	if got := traceFrom(context.Background()); got != nil {
		t.Fatalf("traceFrom = %v, want nil outside a traced request", got)
	}
	// logFetch must be a no-op rather than panic when the context has no trace
	// (fetches also happen outside a libp2p request, e.g. outbound delivery).
	LogFetch(context.Background(), "GET", "https://m/x", 200, time.Second, false)
}

func TestLogFetchCountsCalls(t *testing.T) {
	ctx, tr := StartTrace(context.Background())
	LogFetch(ctx, "GET", "https://m/a", 200, 5*time.Millisecond, false)
	LogFetch(ctx, "GET", "https://m/a", 200, 0, true)
	if got := tr.Calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want every fetch counted under the request", got)
	}
}
