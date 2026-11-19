// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/Warp-net/activity-pub-gw/internal/fediverse"
	"github.com/Warp-net/activity-pub-gw/internal/warp"
)

func TestEnvOr(t *testing.T) {
	const key = "GATEWAY_TEST_ENV_OR"
	if got := warp.EnvOr(key, "fallback"); got != "fallback" {
		t.Fatalf("unset = %q, want the default", got)
	}
	t.Setenv(key, "value")
	if got := warp.EnvOr(key, "fallback"); got != "value" {
		t.Fatalf("set = %q", got)
	}
	t.Setenv(key, "") // an empty value counts as unset
	if got := warp.EnvOr(key, "fallback"); got != "fallback" {
		t.Fatalf("empty = %q, want the default", got)
	}
}

// The version string is stamped into the outbound User-Agent and nodeinfo, so it
// must stay a parseable semver as the patch level is bumped every commit.
func TestGatewayVersionIsSemver(t *testing.T) {
	if _, err := semver.NewVersion(gatewayVersion); err != nil {
		t.Fatalf("gatewayVersion %q: %v", gatewayVersion, err)
	}
	fediverse.SetVersion(gatewayVersion)
	if want := "warpnet-gateway/" + gatewayVersion; fediverse.UserAgent() != want {
		t.Fatalf("userAgent = %q, want %q", fediverse.UserAgent(), want)
	}
}
