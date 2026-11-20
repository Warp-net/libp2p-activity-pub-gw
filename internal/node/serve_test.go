// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/warp"
	"github.com/Warp-net/warpnet/event"
	wjson "github.com/Warp-net/warpnet/json"
	"github.com/Warp-net/warpnet/security"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const errorRoute = "/test/error/0.0.0"

// serveFixture is a node serving routes to a second in-process node.
type serveFixture struct {
	server *Node
	client *Node
}

func newServeFixture(t *testing.T) *serveFixture {
	t.Helper()
	server := newTestNode(t, "srv-"+t.Name())
	client := newTestNode(t, "cli-"+t.Name())
	server.Serve(warp.DefaultOwnerHandle, map[string]HandlerFunc{
		warp.RouteGetUser: func(_ context.Context, buf []byte) (any, error) {
			var ev warp.GetUserEvent
			if err := wjson.Unmarshal(buf, &ev); err != nil {
				return nil, err
			}
			return warp.User{Id: ev.UserId}, nil
		},
		errorRoute: func(context.Context, []byte) (any, error) {
			return nil, errors.New("not found")
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.host.Connect(ctx, peer.AddrInfo{ID: server.host.ID(), Addrs: server.host.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	client.remember(server.host.ID())
	return &serveFixture{server: server, client: client}
}

// rawStream writes arbitrary bytes on a route; a refused envelope gets no
// answer at all.
func (fx *serveFixture) rawStream(t *testing.T, route string, data []byte) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = network.WithAllowLimitedConn(ctx, route)
	s, err := fx.client.host.NewStream(ctx, fx.server.host.ID(), protocol.ID(route))
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}
	defer func() { _ = s.Close() }()
	if _, err := s.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}
	buf := make([]byte, 4096)
	n, _ := s.Read(buf)
	return buf[:n]
}

func signedEnvelope(t *testing.T, signer []byte, from, bridgeKey string) []byte {
	t.Helper()
	body, err := wjson.Marshal(warp.GetUserEvent{UserId: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	msg := warp.Message{
		Body: wjson.RawMessage(body), MessageId: "m-" + bridgeKey[:4], NodeId: from,
		Destination: warp.RouteGetUser, Timestamp: time.Now(), Version: wireVersion,
	}
	msg.Signature = security.Sign(signer, msg.SigningBytes())
	env, err := wjson.Marshal(struct {
		warp.Message
		BridgeKey string `json:"bridge_key"`
	}{Message: msg, BridgeKey: bridgeKey})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestServeAnswersTheNodeInfo(t *testing.T) {
	fx := newServeFixture(t)
	bt, err := fx.client.Request(event.PUBLIC_GET_INFO, nil)
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	var info map[string]any
	if err := json.Unmarshal(bt, &info); err != nil {
		t.Fatalf("decode %s: %v", bt, err)
	}
	if info["owner_id"] != warp.DefaultOwnerHandle {
		t.Fatalf("owner = %v, want the Mastodon entry handle so discovery seeds it", info["owner_id"])
	}
	if info["node_id"] != fx.server.ID() {
		t.Fatalf("node_id = %v", info["node_id"])
	}
	if info["type"] != "member" {
		t.Fatalf("type = %v, want a member node", info["type"])
	}
	if info["reachability"] != float64(network.ReachabilityPrivate) {
		t.Fatalf("reachability = %v, want private (relay-only)", info["reachability"])
	}
	if info["relay_state"] != "off" {
		t.Fatalf("relay_state = %v", info["relay_state"])
	}
	if info["network"] != warp.DefaultNetwork {
		t.Fatalf("network = %v", info["network"])
	}
}

func TestServeRejectsUnauthenticatedEnvelopes(t *testing.T) {
	fx := newServeFixture(t)
	body, err := wjson.Marshal(warp.GetUserEvent{UserId: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	unsigned := warp.Message{
		Body: wjson.RawMessage(body), MessageId: "m1",
		NodeId: fx.client.ID(), Destination: warp.RouteGetUser,
		Timestamp: time.Now(), Version: wireVersion,
	}

	t.Run("a malformed envelope is dropped", func(t *testing.T) {
		if got := fx.rawStream(t, warp.RouteGetUser, []byte("not an envelope")); len(got) != 0 {
			t.Fatalf("response = %q, want the stream dropped", got)
		}
	})

	t.Run("an unsigned envelope is dropped", func(t *testing.T) {
		env, err := wjson.Marshal(unsigned)
		if err != nil {
			t.Fatal(err)
		}
		if got := fx.rawStream(t, warp.RouteGetUser, env); len(got) != 0 {
			t.Fatalf("response = %q, want the stream dropped", got)
		}
	})

	t.Run("a forged signature is dropped", func(t *testing.T) {
		other, err := security.GenerateKeyFromSeed([]byte("not-the-caller"))
		if err != nil {
			t.Fatal(err)
		}
		forged := unsigned
		forged.Signature = security.Sign(other, forged.SigningBytes())
		env, err := wjson.Marshal(forged)
		if err != nil {
			t.Fatal(err)
		}
		if got := fx.rawStream(t, warp.RouteGetUser, env); len(got) != 0 {
			t.Fatalf("response = %q, want a signature from another key refused", got)
		}
	})
}

func TestServeAnswersAHandlerErrorWithAnErrorEnvelope(t *testing.T) {
	fx := newServeFixture(t)
	bt, err := fx.client.Request(errorRoute, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if err := warp.NodeResponseError(bt); err == nil {
		t.Fatalf("response = %s, want an error envelope", bt)
	}
}

func TestServeRemembersCallingMembers(t *testing.T) {
	fx := newServeFixture(t)
	if got := fx.server.memberCandidates(warp.RouteGetUsers); len(got) != 0 {
		t.Fatalf("candidates = %v before any call", got)
	}
	if _, err := fx.client.Request(event.PUBLIC_GET_INFO, nil); err != nil {
		t.Fatal(err)
	}
	got := fx.server.memberCandidates(warp.RouteGetUsers)
	if len(got) != 1 || got[0] != fx.client.host.ID() {
		t.Fatalf("candidates = %v; a node that called must be asked back", got)
	}
}

func TestBridgeKeyComesFromTheUsersOwnNode(t *testing.T) {
	const user = "01KZS1PJ7ZCYGZ1AGARA8B2G9N"
	fx := newServeFixture(t)
	from := fx.client.host.ID()
	own, err := fx.client.host.Peerstore().PrivKey(from).Raw()
	if err != nil {
		t.Fatal(err)
	}
	forger, _ := security.GenerateKeyFromSeed([]byte("not-the-sender"))

	fx.rawStream(t, warp.RouteGetUser, signedEnvelope(t, forger, from.String(), strings.Repeat("cd", 32)))
	if _, ok := fx.server.bridgeKeys.Get(from); ok {
		t.Fatal("a key in an envelope that fails its signature must be dropped")
	}

	key := strings.Repeat("ab", 32)
	fx.rawStream(t, warp.RouteGetUser, signedEnvelope(t, own, from.String(), key))
	fx.server.owners.Add(user, from)
	got, ok := fx.server.BridgeKey(user)
	if !ok || hex.EncodeToString(got) != key {
		t.Fatalf("bridge key = %x, %v; want the one the user's node sent", got, ok)
	}
	fx.server.owners.Add(user, fx.server.host.ID())
	if _, ok := fx.server.BridgeKey(user); ok {
		t.Fatal("another node's key must not stand for the user")
	}
}
