// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package warp

import "testing"

func TestEnvOr(t *testing.T) {
	const key = "GATEWAY_TEST_ENV_OR"
	if got := EnvOr(key, "fallback"); got != "fallback" {
		t.Fatalf("unset = %q, want the default", got)
	}
	t.Setenv(key, "value")
	if got := EnvOr(key, "fallback"); got != "value" {
		t.Fatalf("set = %q", got)
	}
	t.Setenv(key, "") // an empty value counts as unset
	if got := EnvOr(key, "fallback"); got != "fallback" {
		t.Fatalf("empty = %q, want the default", got)
	}
}
