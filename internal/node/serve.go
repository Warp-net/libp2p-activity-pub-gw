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

package node

import (
	"context"
	"io"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/Warp-net/bridge-gateway/internal/warp"
	"github.com/Warp-net/warpnet/core/warpnet"
	"github.com/Warp-net/warpnet/event"
	wjson "github.com/Warp-net/warpnet/json"
	"github.com/Warp-net/warpnet/security"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	maxRequestBytes = 1 << 20
	handlerTimeout  = 90 * time.Second
)

// HandlerFunc answers one route from the request body the caller signed.
type HandlerFunc func(ctx context.Context, buf []byte) (any, error)

// Serve answers the routes as an ordinary member node owned by ownerHandle.
// The node info route is the node's own.
func (n *Node) Serve(ownerHandle string, handlers map[string]HandlerFunc) {
	n.host.SetStreamHandler(protocol.ID(event.PUBLIC_GET_INFO), n.streamHandler(event.PUBLIC_GET_INFO,
		func(context.Context, []byte) (any, error) {
			return n.nodeInfo(ownerHandle), nil
		},
	))
	for route, h := range handlers {
		n.host.SetStreamHandler(protocol.ID(route), n.streamHandler(route, h))
	}
	warp.NetLog(n.network).Infof("nodeserver: serving %d public routes as %s (owner %s)", len(handlers)+1, n.host.ID(), ownerHandle)
}

// streamHandler verifies the envelope against the caller's peer key, like the
// node's auth middleware, before the handler sees it.
func (n *Node) streamHandler(route string, h HandlerFunc) network.StreamHandler {
	return func(s network.Stream) {
		defer func() { _ = s.Close() }()

		data, err := io.ReadAll(io.LimitReader(s, maxRequestBytes))
		if err != nil {
			warp.NetLog(n.network).Warnf("nodeserver: %s: read: %v", route, err)
			return
		}
		var msg warp.Message
		if err := wjson.Unmarshal(data, &msg); err != nil {
			warp.NetLog(n.network).Warnf("nodeserver: %s: bad envelope: %v", route, err)
			return
		}
		if msg.Signature == "" {
			warp.NetLog(n.network).Warnf("nodeserver: %s: signature missing", route)
			return
		}
		if conn := s.Conn(); conn != nil {
			from := conn.RemotePeer()
			if err := security.VerifySignature(warpnet.FromIDToPubKey(from), msg.SigningBytes(), msg.Signature); err != nil {
				warp.NetLog(n.network).Warnf("nodeserver: %s: signature from %s invalid: %v", route, from, err)
				return
			}
			// A node that writes here is a member to ask back, even before the
			// DHT surfaces it.
			n.remember(from)
			var be warp.BridgeEnvelope
			if wjson.Unmarshal(data, &be) == nil {
				n.rememberBridgeKey(from, be.BridgeKey)
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
		defer cancel()
		ctx, tr := warp.StartTrace(ctx)
		start := time.Now()
		resp, err := h(ctx, msg.Body)
		warp.NetLog(n.network).Infof("[%s] libp2p %s: %d REST calls in %s", tr.ID, route, tr.Calls.Load(), time.Since(start).Round(time.Millisecond))
		if err != nil {
			warp.NetLog(n.network).Warnf("nodeserver: %s: %v", route, err)
			resp = event.ResponseError{Message: err.Error()}
		}
		out, err := wjson.Marshal(resp)
		if err != nil {
			warp.NetLog(n.network).Errorf("nodeserver: %s: marshal response: %v", route, err)
			return
		}
		if _, err := s.Write(out); err != nil {
			warp.NetLog(n.network).Warnf("nodeserver: %s: write: %v", route, err)
		}
	}
}

// nodeInfo reports private reachability, like a NATed member: the gateway is
// reachable through the relays only.
func (n *Node) nodeInfo(ownerHandle string) warpnet.NodeInfo {
	addrs := make([]string, 0, len(n.host.Addrs()))
	for _, a := range n.host.Addrs() {
		addrs = append(addrs, a.String())
	}
	return warpnet.NodeInfo{
		Type:         warpnet.MemberNode,
		OwnerId:      ownerHandle,
		ID:           n.host.ID(),
		Version:      semver.MustParse(wireVersion),
		Addresses:    addrs,
		StartTime:    time.Now(),
		RelayState:   warpnet.RelayStatusOff,
		Reachability: warpnet.ReachabilityPrivate,
		Network:      n.network,
	}
}
