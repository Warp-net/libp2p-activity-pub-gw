// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"testing"

	"github.com/Warp-net/bridge-gateway/internal/warp"
)

func TestConfiguredNetworks(t *testing.T) {
	t.Run("unset joins the default network alone", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", "")
		if got := ConfiguredNetworks(); len(got) != 1 || got[0] != warp.DefaultNetwork {
			t.Fatalf("networks = %v, want just %s", got, warp.DefaultNetwork)
		}
	})

	t.Run("a list joins every network, in order", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", "warpnet,testnet")
		got := ConfiguredNetworks()
		if len(got) != 2 || got[0] != "warpnet" || got[1] != "testnet" {
			t.Fatalf("networks = %v", got)
		}
	})

	t.Run("whitespace and duplicates are ignored", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", " warpnet , testnet ,,warpnet")
		got := ConfiguredNetworks()
		if len(got) != 2 || got[0] != "warpnet" || got[1] != "testnet" {
			t.Fatalf("networks = %v, want each network once", got)
		}
	})
}

// Each node has its own off switch, so one can be taken out of a running
// deployment without rewriting NODE_NETWORK.
func TestNetworkDisableFlags(t *testing.T) {
	t.Run("disables just the named network", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", "warpnet,testnet")
		t.Setenv("GATEWAY_DISABLE_TESTNET", "true")
		if got := ConfiguredNetworks(); len(got) != 1 || got[0] != "warpnet" {
			t.Fatalf("networks = %v, want testnet dropped", got)
		}
	})

	t.Run("works for either node", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", "warpnet,testnet")
		t.Setenv("GATEWAY_DISABLE_WARPNET", "1")
		if got := ConfiguredNetworks(); len(got) != 1 || got[0] != "testnet" {
			t.Fatalf("networks = %v, want warpnet dropped", got)
		}
	})

	t.Run("disabling every network leaves none", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", "warpnet,testnet")
		t.Setenv("GATEWAY_DISABLE_WARPNET", "true")
		t.Setenv("GATEWAY_DISABLE_TESTNET", "true")
		if got := ConfiguredNetworks(); len(got) != 0 {
			t.Fatalf("networks = %v, want none", got)
		}
	})

	// A typo must not silently drop a network — only a real boolean switches
	// a node off.
	t.Run("an unparseable value keeps the node on", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", "warpnet,testnet")
		t.Setenv("GATEWAY_DISABLE_TESTNET", "maybe")
		if got := ConfiguredNetworks(); len(got) != 2 {
			t.Fatalf("networks = %v, want both kept", got)
		}
	})

	t.Run("false keeps the node on", func(t *testing.T) {
		t.Setenv("NODE_NETWORK", "testnet")
		t.Setenv("GATEWAY_DISABLE_TESTNET", "false")
		if got := ConfiguredNetworks(); len(got) != 1 {
			t.Fatalf("networks = %v, want testnet kept", got)
		}
	})
}
