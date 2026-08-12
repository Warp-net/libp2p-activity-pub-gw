/*

 Warpnet - Decentralized Social Network
 Copyright (C) 2025 Vadim Filin, https://github.com/Warp-net,
 <github.com.mecdy@passmail.net>

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <https://www.gnu.org/licenses/>.

WarpNet is provided “as is” without warranty of any kind, either expressed or implied.
Use at your own risk. The maintainers shall not be liable for any damages or data loss
resulting from the use or misuse of this software.
*/

// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

// The Warpnet wire framing — the gateway opens a plain libp2p stream and writes
// warpnet's signed event.Message envelope (signed with warpnet's security.Sign).
// PSK derivation and the message/event DTOs come from warpnet's own packages
// (security, event, domain); only the bootstrap peer list is kept local.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"
	"time"

	wjson "github.com/Warp-net/warpnet/json"
	"github.com/Warp-net/warpnet/security"
	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const defaultWarpnetNetwork = "warpnet"

// defaultGatewaySeed derives the gateway's stable libp2p identity (warpnet pins
// the resulting peer id as mastodon.GatewayNodeID, so it is not configurable).
// The same identity is used on every network: they are PSK-isolated, so the peer
// id can (and must) be the one warpnet expects on each of them.
const defaultGatewaySeed = "warpnet-activitypub-gateway"

// p2pListenByNetwork is each network's libp2p listen address. The gateway joins
// every configured network in one process, so each node needs its own port. They
// are relay-only and never advertised (ForceReachabilityPrivate), so the numbers
// matter only in that they must not clash — see TestP2PListenAddrsAreDistinct.
var p2pListenByNetwork = map[string]string{
	defaultWarpnetNetwork: "/ip4/0.0.0.0/tcp/4040",
	"testnet":             "/ip4/0.0.0.0/tcp/4041",
}

// defaultOwnerHandle is the Mastodon account the gateway advertises as its node
// owner (warpnet pins it as mastodon.EntryHandle, so it is not configurable) so
// Warpnet discovery seeds it as the entry point into the Fediverse. The gateway
// has no Warpnet user of its own.
const defaultOwnerHandle = "warpnet@mastodon.social"

// bootstrapByNetwork lists the public entry nodes per network (the network's
// relays — the gateway's DHT bootstrap; member nodes are found via the DHT).
var bootstrapByNetwork = map[string][]string{
	defaultWarpnetNetwork: {
		"/ip4/207.154.221.44/tcp/4001/p2p/12D3KooWMKZFrp1BDKg9amtkv5zWnLhuUXN32nhqMvbtMdV2hz7j",
		"/ip4/207.154.221.44/tcp/4002/p2p/12D3KooWSjbYrsVoXzJcEtmgJLMVCbPXMzJmNN1JkEZB9LJ2rnmU",
		"/ip4/207.154.221.44/tcp/4003/p2p/12D3KooWNXSGyfTuYc3JznW48jay73BtQgHszWfPpyF581EWcpGJ",
		"/ip4/130.94.88.38/tcp/4011/p2p/12D3KooWNW7nbLpbsEVJ86JN6c1zXRDKGCbqmLfhitFCPccRv2YW",
	},
	"testnet": {
		"/ip4/207.154.221.44/tcp/4011/p2p/12D3KooWMKZFrp1BDKg9amtkv5zWnLhuUXN32nhqMvbtMdV2hz7j",
		"/ip4/207.154.221.44/tcp/4022/p2p/12D3KooWSjbYrsVoXzJcEtmgJLMVCbPXMzJmNN1JkEZB9LJ2rnmU",
		"/ip4/207.154.221.44/tcp/4033/p2p/12D3KooWNXSGyfTuYc3JznW48jay73BtQgHszWfPpyF581EWcpGJ",
	},
}

// streamSend opens a libp2p stream on the route's protocol ID, writes the signed
// message envelope, and reads the full response — warpnet's stream framing.
func streamSend(ctx context.Context, h host.Host, p peer.ID, priv ed25519.PrivateKey, route string, payload any) ([]byte, error) {
	var body []byte
	if payload != nil {
		if b, ok := payload.([]byte); ok {
			body = b
		} else {
			var err error
			if body, err = wjson.Marshal(payload); err != nil {
				return nil, fmt.Errorf("stream: marshal payload: %w", err)
			}
		}
	}

	// Allow the stream over a limited (circuit-relay) connection — member nodes
	// behind NAT are reachable only via a relay, which NewStream otherwise rejects.
	ctx = network.WithAllowLimitedConn(ctx, route)
	s, err := h.NewStream(ctx, p, protocol.ID(route))
	if err != nil {
		return nil, fmt.Errorf("stream: new: %w", err)
	}
	defer func() { _ = s.Close() }()

	msg := message{
		Body:        wjson.RawMessage(body),
		MessageId:   uuid.New().String(),
		NodeId:      h.ID().String(),
		Destination: route,
		Timestamp:   time.Now(),
		Version:     "0.0.0",
	}
	msg.Signature = security.Sign(priv, msg.SigningBytes())
	data, err := wjson.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("stream: marshal envelope: %w", err)
	}

	rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
	if _, err := rw.Write(data); err != nil {
		return nil, fmt.Errorf("stream: write: %w", err)
	}
	if err := rw.Flush(); err != nil {
		return nil, fmt.Errorf("stream: flush: %w", err)
	}
	_ = s.CloseWrite()

	buf := bytes.NewBuffer(nil)
	if _, err := buf.ReadFrom(rw); err != nil {
		return nil, fmt.Errorf("stream: read: %w", err)
	}
	return buf.Bytes(), nil
}
