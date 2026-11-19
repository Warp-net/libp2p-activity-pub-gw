// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package fediverse

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestLoadOrCreateKey(t *testing.T) {
	t.Run("generates and persists on first run", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "key.pem")
		key, err := LoadOrCreateKey(path)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if key.N.BitLen() != rsaKeyBits {
			t.Fatalf("bits = %d, want %d", key.N.BitLen(), rsaKeyBits)
		}
		info, serr := os.Stat(path)
		if serr != nil {
			t.Fatalf("the key must be written to disk: %v", serr)
		}
		// Windows has no Unix permission bits — Go reduces the mode to a
		// read-only flag — so 0600 is unrepresentable there.
		if runtime.GOOS != "windows" {
			if perm := info.Mode().Perm(); perm != 0o600 {
				t.Fatalf("mode = %v, want 0600 — the signing key must not be world-readable", perm)
			}
		}

		// Signature stability across restarts is the whole point: reloading must
		// return the same key, not mint a new one.
		again, err := LoadOrCreateKey(path)
		if err != nil {
			t.Fatalf("reload: %v", err)
		}
		if !again.Equal(key) {
			t.Fatal("reloading returned a different key; existing followers could no longer verify us")
		}
	})

	t.Run("rejects a file that is not PEM", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "key.pem")
		if err := os.WriteFile(path, []byte("garbage"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadOrCreateKey(path)
		if !errors.Is(err, errNotPEMFile) {
			t.Fatalf("err = %v, want errNotPEMFile", err)
		}
	})

	t.Run("rejects PEM that is not a PKCS#1 private key", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "key.pem")
		block := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: []byte("not a key")}
		if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadOrCreateKey(path); err == nil {
			t.Fatal("expected a parse error")
		}
	})

	t.Run("reports a read error that is not a missing file", func(t *testing.T) {
		dir := t.TempDir() // a directory reads as EISDIR, not ENOENT
		if _, err := LoadOrCreateKey(dir); err == nil {
			t.Fatal("expected a read error")
		}
	})

	t.Run("reports an unwritable destination", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing-dir", "key.pem")
		if _, err := LoadOrCreateKey(path); err == nil {
			t.Fatal("expected a write error")
		}
	})
}

func TestPublicKeyPEM(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 1024) //nolint:gosec // short key keeps the test fast
	if err != nil {
		t.Fatal(err)
	}
	out, err := publicKeyPEM(key)
	if err != nil {
		t.Fatalf("publicKeyPEM: %v", err)
	}
	if !strings.HasPrefix(out, "-----BEGIN PUBLIC KEY-----") {
		t.Fatalf("not a PUBLIC KEY block: %q", out)
	}
	block, _ := pem.Decode([]byte(out))
	if block == nil {
		t.Fatal("output is not decodable PEM")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !parsed.(*rsa.PublicKey).Equal(&key.PublicKey) {
		t.Fatal("the encoded key is not the one we passed in")
	}
}
