// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/Warp-net/bridge-gateway/internal/fediverse"
)

// The version string is stamped into the outbound User-Agent and nodeinfo, so it
// must stay a parseable semver as the patch level is bumped every commit.
func TestGatewayVersionIsSemver(t *testing.T) {
	if _, err := semver.NewVersion(version); err != nil {
		t.Fatalf("version %q: %v", version, err)
	}
	fediverse.SetVersion(version)
	if want := "bridge-gateway/" + version; fediverse.UserAgent() != want {
		t.Fatalf("userAgent = %q, want %q", fediverse.UserAgent(), want)
	}
}
