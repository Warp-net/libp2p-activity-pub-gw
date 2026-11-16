// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	wjson "github.com/Warp-net/warpnet/json"
	"github.com/Warp-net/warpnet/security"
	"github.com/libp2p/go-libp2p/core/peer"
)

// signingEnvelope frames a body/timestamp/route the way the wire does, so a
// test can ask what bytes the signature covers.
func signingEnvelope(body []byte, ts time.Time, route string) message {
	return message{Body: wjson.RawMessage(body), Timestamp: ts, Destination: route}
}

// The gateway signs with warpnet's own Message.SigningBytes: the raw body,
// the timestamp as decimal Unix nanoseconds, then the destination route.
// Pinned here because a warpnet bump that changed the framing would silently
// get every envelope the gateway sends rejected by the node — and every one
// it receives dropped.
func TestSigningBytesFraming(t *testing.T) {
	ts := time.Unix(0, 1700000000123456789)
	if got := string(signingEnvelope([]byte("body"), ts, "/route").SigningBytes()); got != "body1700000000123456789/route" {
		t.Fatalf("SigningBytes = %q", got)
	}
	if got := string(signingEnvelope(nil, ts, "").SigningBytes()); got != "1700000000123456789" {
		t.Fatalf("empty body = %q", got)
	}
}

// The signature a stream carries must verify against the peer id the receiver
// derives it from — the contract nodeserver's auth check relies on.
func TestStreamSignatureVerifiesAgainstThePeerKey(t *testing.T) {
	priv, err := security.GenerateKeyFromSeed([]byte("sig-check"))
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"user_id":"alice"}`)
	ts := time.Now()
	const route = "/public/get/user/0.0.0"
	sig := security.Sign(priv, signingEnvelope(body, ts, route).SigningBytes())
	pub, ok := ed25519.PrivateKey(priv).Public().(ed25519.PublicKey)
	if !ok {
		t.Fatal("not an ed25519 key")
	}

	if verr := security.VerifySignature(pub, signingEnvelope(body, ts, route).SigningBytes(), sig); verr != nil {
		t.Fatalf("verify: %v", verr)
	}
	if verr := security.VerifySignature(pub, signingEnvelope([]byte("tampered"), ts, route).SigningBytes(), sig); verr == nil {
		t.Fatal("a tampered body must not verify")
	}
	if verr := security.VerifySignature(pub, signingEnvelope(body, ts.Add(time.Second), route).SigningBytes(), sig); verr == nil {
		t.Fatal("a replaced timestamp must not verify")
	}
	if verr := security.VerifySignature(pub, signingEnvelope(body, ts, "/private/post/block/0.0.0").SigningBytes(), sig); verr == nil {
		t.Fatal("a repointed route must not verify")
	}
}

// The gateway's libp2p identity is derived from a fixed seed: warpnet pins the
// resulting peer id, so a change here orphans every bridged Mastodon user.
func TestGatewayIdentityIsDeterministic(t *testing.T) {
	a, err := security.GenerateKeyFromSeed([]byte(defaultGatewaySeed))
	if err != nil {
		t.Fatal(err)
	}
	b, err := security.GenerateKeyFromSeed([]byte(defaultGatewaySeed))
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.PrivateKey(a).Equal(ed25519.PrivateKey(b)) {
		t.Fatal("the seeded identity is not stable across derivations")
	}
}

func TestBootstrapEntriesParse(t *testing.T) {
	if len(bootstrapByNetwork) == 0 {
		t.Fatal("no networks configured")
	}
	for network, addrs := range bootstrapByNetwork {
		if len(addrs) == 0 {
			t.Errorf("%s has no entry peers", network)
		}
		for _, s := range addrs {
			if _, err := peer.AddrInfoFromString(s); err != nil {
				t.Errorf("%s: bootstrap %q does not parse: %v", network, s, err)
			}
		}
	}
}

func TestStreamSendRejectsAnUnmarshalablePayload(t *testing.T) {
	c := newTestNode(t, "marshal-fail")
	// A channel cannot be JSON-encoded; the failure must surface before any
	// stream is opened.
	_, err := streamSend(context.Background(), c.h, c.h.ID(), c.priv, "/route", make(chan int))
	if err == nil {
		t.Fatal("expected a marshal error")
	}
}

func TestStreamSendReportsAnUndialablePeer(t *testing.T) {
	c := newTestNode(t, "undialable")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	other := newTestNode(t, "never-connected")
	if _, err := streamSend(ctx, c.h, other.h.ID(), c.priv, "/route", nil); err == nil {
		t.Fatal("expected a dial error for a peer with no known addresses")
	}
}
