// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package fediverse

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

// signedInboxRequest builds a request signed by key, the shape an inbound
// ActivityPub delivery has.
func signedInboxRequest(t *testing.T, key *rsa.PrivateKey, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "https://gw.example/inbox", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	if err := signRequest(req, "https://remote/users/bob#main-key", key, body); err != nil {
		t.Fatalf("sign: %v", err)
	}
	return req
}

func TestSignRequestSetsSignedHeaders(t *testing.T) {
	key := testGateway(t).key
	body := []byte(`{"type":"Create"}`)
	req := signedInboxRequest(t, key, body)

	if req.Header.Get(headerDate) == "" {
		t.Fatal("Date must be set so the peer can apply its replay window")
	}
	if req.Header.Get("Digest") == "" {
		t.Fatal("a request with a body must carry a Digest")
	}
	_, headers, sig, err := parseSignatureHeader(req.Header.Get("Signature"))
	if err != nil {
		t.Fatalf("parse own signature: %v", err)
	}
	if sig == "" {
		t.Fatal("empty signature")
	}
	for _, want := range append(minSignedHeaders, digestHeader) {
		if !slicesContains(headers, want) {
			t.Fatalf("header %q not signed; signed set = %v", want, headers)
		}
	}

	t.Run("a body-less request signs no digest", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, "https://remote/users/bob", nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := signRequest(req, "k", key, nil); err != nil {
			t.Fatalf("sign: %v", err)
		}
		if req.Header.Get("Digest") != "" {
			t.Fatal("a GET must not carry a Digest")
		}
	})

	t.Run("an explicit Date is preserved", func(t *testing.T) {
		fixed := time.Now().UTC().Add(-time.Minute).Format(http.TimeFormat)
		req, err := http.NewRequest(http.MethodGet, "https://remote/users/bob", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(headerDate, fixed)
		if err := signRequest(req, "k", key, nil); err != nil {
			t.Fatal(err)
		}
		if got := req.Header.Get(headerDate); got != fixed {
			t.Fatalf("Date = %q, want the caller's %q", got, fixed)
		}
	})
}

func TestVerifyRequestPolicy(t *testing.T) {
	g := testGateway(t)
	body := []byte(`{"type":"Create"}`)
	fetch := func(string) (*rsa.PublicKey, error) { return &g.key.PublicKey, nil }

	t.Run("missing Signature header", func(t *testing.T) {
		req := signedInboxRequest(t, g.key, body)
		req.Header.Del("Signature")
		if _, err := verifyRequest(req, body, fetch); !errors.Is(err, errNoSignatureHeader) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("an unparsable Signature header", func(t *testing.T) {
		req := signedInboxRequest(t, g.key, body)
		req.Header.Set("Signature", "garbage")
		if _, err := verifyRequest(req, body, fetch); !errors.Is(err, errIncompleteSignature) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("an incomplete signed header set", func(t *testing.T) {
		req := signedInboxRequest(t, g.key, body)
		req.Header.Set("Signature", `keyId="k",headers="date",signature="AAAA"`)
		err := verifyRequest2(req, body, fetch)
		if !errors.Is(err, errIncompleteSignature) {
			t.Fatalf("err = %v, want the minimum header set enforced", err)
		}
	})

	t.Run("a missing Date header", func(t *testing.T) {
		req := signedInboxRequest(t, g.key, body)
		req.Header.Del(headerDate)
		if _, err := verifyRequest(req, body, fetch); !errors.Is(err, errIncompleteSignature) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("an unparsable Date header", func(t *testing.T) {
		req := signedInboxRequest(t, g.key, body)
		req.Header.Set(headerDate, "yesterday")
		if _, err := verifyRequest(req, body, fetch); !errors.Is(err, errIncompleteSignature) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("a stale Date is refused in both directions", func(t *testing.T) {
		for _, skew := range []time.Duration{-2 * maxClockSkew, 2 * maxClockSkew} {
			req := signedInboxRequest(t, g.key, body)
			req.Header.Set(headerDate, time.Now().Add(skew).UTC().Format(http.TimeFormat))
			if _, err := verifyRequest(req, body, fetch); !errors.Is(err, errStaleRequest) {
				t.Fatalf("skew %v: err = %v, want errStaleRequest", skew, err)
			}
		}
	})

	t.Run("a body not bound by a digest", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, "https://gw.example/inbox", strings.NewReader(string(body)))
		if err != nil {
			t.Fatal(err)
		}
		if err := signRequest(req, "k", g.key, nil); err != nil { // signs no digest
			t.Fatal(err)
		}
		if _, err := verifyRequest(req, body, fetch); !errors.Is(err, errIncompleteSignature) {
			t.Fatalf("err = %v, want an unbound body refused", err)
		}
	})

	t.Run("a tampered digest", func(t *testing.T) {
		req := signedInboxRequest(t, g.key, body)
		req.Header.Set("Digest", "SHA-256=AAAA")
		if _, err := verifyRequest(req, body, fetch); !errors.Is(err, errDigestMismatch) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("an unresolvable key", func(t *testing.T) {
		req := signedInboxRequest(t, g.key, body)
		boom := errors.New("no such actor")
		_, err := verifyRequest(req, body, func(string) (*rsa.PublicKey, error) { return nil, boom })
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want the fetch error wrapped", err)
		}
	})

	t.Run("the wrong key", func(t *testing.T) {
		other, kerr := rsa.GenerateKey(rand.Reader, 2048)
		if kerr != nil {
			t.Fatal(kerr)
		}
		req := signedInboxRequest(t, g.key, body)
		if _, err := verifyRequest(req, body, func(string) (*rsa.PublicKey, error) { return &other.PublicKey, nil }); err == nil {
			t.Fatal("a signature must not verify under an unrelated key")
		}
	})

	t.Run("a valid signature returns the keyId", func(t *testing.T) {
		req := signedInboxRequest(t, g.key, body)
		keyID, err := verifyRequest(req, body, fetch)
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
		if keyID != "https://remote/users/bob#main-key" {
			t.Fatalf("keyID = %q — the caller binds the activity's actor to it", keyID)
		}
	})
}

// verifyRequest2 drops the keyId so the error assertions above read cleanly.
func verifyRequest2(req *http.Request, body []byte, fetch func(string) (*rsa.PublicKey, error)) error {
	_, err := verifyRequest(req, body, fetch)
	return err
}

func TestParseSignatureHeader(t *testing.T) {
	t.Run("full header", func(t *testing.T) {
		keyID, headers, sig, err := parseSignatureHeader(
			`keyId="https://m/users/bob#main-key",algorithm="rsa-sha256",headers="(request-target) Host Date Digest",signature="Zm9v"`)
		if err != nil {
			t.Fatal(err)
		}
		if keyID != "https://m/users/bob#main-key" || sig != "Zm9v" {
			t.Fatalf("keyID=%q sig=%q", keyID, sig)
		}
		want := []string{"(request-target)", "host", "date", "digest"}
		if !reflect.DeepEqual(headers, want) {
			t.Fatalf("headers = %v, want %v lowercased", headers, want)
		}
	})

	t.Run("headers default to date per the draft", func(t *testing.T) {
		_, headers, _, err := parseSignatureHeader(`keyId="k",signature="s"`)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(headers, []string{dateHeader}) {
			t.Fatalf("headers = %v", headers)
		}
	})

	t.Run("a missing keyId or signature is incomplete", func(t *testing.T) {
		for _, in := range []string{`signature="s"`, `keyId="k"`, ``, `nonsense`} {
			if _, _, _, err := parseSignatureHeader(in); !errors.Is(err, errIncompleteSignature) {
				t.Fatalf("%q: err = %v", in, err)
			}
		}
	})
}

func TestParseRSAPublicKeyPEM(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 1024) //nolint:gosec // short key keeps the test fast
	if err != nil {
		t.Fatal(err)
	}

	t.Run("PKIX", func(t *testing.T) {
		pemStr, perr := publicKeyPEM(key)
		if perr != nil {
			t.Fatal(perr)
		}
		got, gerr := parseRSAPublicKeyPEM(pemStr)
		if gerr != nil {
			t.Fatalf("parse: %v", gerr)
		}
		if !got.Equal(&key.PublicKey) {
			t.Fatal("round-trip mismatch")
		}
	})

	t.Run("PKCS#1", func(t *testing.T) {
		block := &pem.Block{Type: "RSA PUBLIC KEY", Bytes: x509.MarshalPKCS1PublicKey(&key.PublicKey)}
		got, gerr := parseRSAPublicKeyPEM(string(pem.EncodeToMemory(block)))
		if gerr != nil {
			t.Fatalf("parse: %v", gerr)
		}
		if !got.Equal(&key.PublicKey) {
			t.Fatal("round-trip mismatch")
		}
	})

	t.Run("not PEM", func(t *testing.T) {
		if _, err := parseRSAPublicKeyPEM("hello"); !errors.Is(err, errBadPublicKey) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("a non-RSA key is refused", func(t *testing.T) {
		ec, eerr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if eerr != nil {
			t.Fatal(eerr)
		}
		der, merr := x509.MarshalPKIXPublicKey(&ec.PublicKey)
		if merr != nil {
			t.Fatal(merr)
		}
		block := &pem.Block{Type: "PUBLIC KEY", Bytes: der}
		if _, err := parseRSAPublicKeyPEM(string(pem.EncodeToMemory(block))); !errors.Is(err, errBadPublicKey) {
			t.Fatalf("err = %v, want an ECDSA key refused", err)
		}
	})

	t.Run("PEM carrying garbage", func(t *testing.T) {
		block := &pem.Block{Type: "PUBLIC KEY", Bytes: []byte("nope")}
		if _, err := parseRSAPublicKeyPEM(string(pem.EncodeToMemory(block))); !errors.Is(err, errBadPublicKey) {
			t.Fatalf("err = %v", err)
		}
	})
}

func slicesContains(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
}
