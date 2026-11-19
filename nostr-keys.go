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

// Custodial Nostr keys of Warpnet users, computed on the fly from one
// gateway secret and never stored: sk = HMAC-SHA256(master,
// "warpnet/nostr/v1\x00" + network + "\x00" + ULID), retried with a counter
// while it is not a valid secp256k1 scalar.

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/oklog/ulid/v2"
)

const (
	nostrKeyDomain = "warpnet/nostr/v1"
	// nostrSecretMinLen keeps a guessable master out: every key follows from it.
	nostrSecretMinLen = 32
	nostrOwnedKeys    = 1 << 16
)

var errWeakNostrSecret = errors.New("nostr: GATEWAY_NOSTR_SECRET must be at least 32 bytes and not the public gateway seed")

type nostrKeys struct {
	master []byte
	// owned maps a derived pubkey back to the Warpnet account it belongs to,
	// so the gateway's own keys resolve to native accounts, not Nostr copies.
	owned *expirable.LRU[string, ownedKey]
}

type ownedKey struct{ network, userID string }

// nostrKeysFromEnv reads the master secret from GATEWAY_NOSTR_SECRET; nil
// when it is unset. The value is never logged.
func nostrKeysFromEnv() (*nostrKeys, error) {
	secret := os.Getenv("GATEWAY_NOSTR_SECRET")
	if secret == "" {
		return nil, nil //nolint:nilnil // unset: no keys, not an error
	}
	return newNostrKeys([]byte(secret))
}

func newNostrKeys(master []byte) (*nostrKeys, error) {
	if len(master) < nostrSecretMinLen || string(master) == defaultGatewaySeed {
		return nil, errWeakNostrSecret
	}
	return &nostrKeys{
		master: append([]byte{}, master...),
		owned:  expirable.NewLRU[string, ownedKey](nostrOwnedKeys, nil, 0),
	}, nil
}

// secretKey derives a Warpnet user's key; only ULID accounts have one.
func (k *nostrKeys) secretKey(network, userID string) ([32]byte, bool) {
	var sk [32]byte
	if k == nil || network == "" {
		return sk, false
	}
	if _, err := ulid.ParseStrict(userID); err != nil {
		return sk, false
	}
	userID = strings.ToUpper(userID)
	msg := nostrKeyDomain + "\x00" + network + "\x00" + userID
	for counter := 0; ; counter++ {
		m := msg
		if counter > 0 {
			m += "\x00" + strconv.Itoa(counter)
		}
		mac := hmac.New(sha256.New, k.master)
		mac.Write([]byte(m))
		copy(sk[:], mac.Sum(nil))
		var s btcec.ModNScalar
		if overflow := s.SetByteSlice(sk[:]); !overflow && !s.IsZero() {
			break
		}
	}
	k.owned.Add(publicKeyHex(sk), ownedKey{network: network, userID: userID})
	return sk, true
}

// pubKey is a Warpnet user's Nostr public key.
func (k *nostrKeys) pubKey(network, userID string) (string, bool) {
	sk, ok := k.secretKey(network, userID)
	if !ok {
		return "", false
	}
	return publicKeyHex(sk), true
}

// owner names the Warpnet account behind one of the gateway's own keys,
// among the keys derived since start.
func (k *nostrKeys) owner(pk string) (ownedKey, bool) {
	if k == nil {
		return ownedKey{}, false
	}
	return k.owned.Get(pk)
}

// isOwn checks a claim that pk belongs to userID by deriving it again.
func (k *nostrKeys) isOwn(pk, network, userID string) bool {
	derived, ok := k.pubKey(network, userID)
	return ok && hmac.Equal([]byte(derived), []byte(pk))
}
