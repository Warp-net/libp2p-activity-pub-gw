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

package nostr

// Nostr keys of Warpnet users. Each user's node derives a bridge key from its
// own identity and sends it to the gateway with its messages; the gateway turns
// it into the user's secp256k1 key, so every user has their own npub.

import (
	"crypto/sha256"
	"crypto/subtle"
	"strings"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/oklog/ulid/v2"
)

const (
	nostrKeyDomain = "warpnet/nostr/v1"
	nostrOwnedKeys = 1 << 16
	nostrPubKeyTTL = time.Hour
)

// KeySource gives the bridge key a user's own node sent.
type KeySource interface {
	BridgeKey(network, userID string) ([]byte, bool)
}

type Keys struct {
	source KeySource
	// owned maps a user's pubkey back to the Warpnet account it belongs to,
	// so the gateway's own users resolve to native accounts, not Nostr copies.
	owned *expirable.LRU[string, ownedKey]
	pubs  *expirable.LRU[string, string]
}

type ownedKey struct{ network, userID string }

func NewKeys(source KeySource) *Keys {
	return &Keys{
		source: source,
		owned:  expirable.NewLRU[string, ownedKey](nostrOwnedKeys, nil, 0),
		pubs:   expirable.NewLRU[string, string](nostrOwnedKeys, nil, nostrPubKeyTTL),
	}
}

// secretKey is a Warpnet user's key, from their node's bridge key; only ULID
// accounts have one.
func (k *Keys) secretKey(network, userID string) ([32]byte, bool) {
	var sk [32]byte
	if k == nil || k.source == nil || network == "" {
		return sk, false
	}
	if _, err := ulid.ParseStrict(userID); err != nil {
		return sk, false
	}
	userID = strings.ToUpper(userID)
	bridge, ok := k.source.BridgeKey(network, userID)
	if !ok {
		return sk, false
	}
	sk = nostrSecret(bridge)
	pk := publicKeyHex(sk)
	k.owned.Add(pk, ownedKey{network: network, userID: userID})
	k.pubs.Add(network+"\x00"+userID, pk)
	return sk, true
}

// nostrSecret maps a bridge key to a valid secp256k1 scalar.
func nostrSecret(bridge []byte) [32]byte {
	for counter := byte(0); ; counter++ {
		sk := sha256.Sum256(append(append([]byte(nostrKeyDomain), bridge...), counter))
		var s btcec.ModNScalar
		if overflow := s.SetByteSlice(sk[:]); !overflow && !s.IsZero() {
			return sk
		}
	}
}

// pubKey is a Warpnet user's Nostr public key.
func (k *Keys) pubKey(network, userID string) (string, bool) {
	if k == nil {
		return "", false
	}
	if pk, ok := k.pubs.Get(network + "\x00" + strings.ToUpper(userID)); ok {
		return pk, true
	}
	sk, ok := k.secretKey(network, userID)
	if !ok {
		return "", false
	}
	return publicKeyHex(sk), true
}

// owner names the Warpnet account behind one of the users' keys seen since
// start.
func (k *Keys) owner(pk string) (ownedKey, bool) {
	if k == nil {
		return ownedKey{}, false
	}
	return k.owned.Get(pk)
}

// isOwn checks a claim that pk belongs to userID against the user's node.
func (k *Keys) isOwn(pk, network, userID string) bool {
	known, ok := k.pubKey(network, userID)
	return ok && subtle.ConstantTimeCompare([]byte(known), []byte(pk)) == 1
}
