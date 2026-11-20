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

package fediverse

import (
	"encoding/base64"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/warp"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

// followerStore records, per local (bridged) user, the remote Fediverse actors
// that follow them. The production path (nodeFollowerStore) keeps the follow
// graph in Warpnet via the existing follow routes; memFollowerStore is an
// in-memory dev fallback when no Warpnet node is configured. Either way the
// gateway persists only its keys, never Warpnet content.
type followerStore interface {
	Add(localUser, actorURL string) error
	List(localUser string) ([]string, error)
}

var errNotAPFollower = errors.New("not an AP follower id")

// encodeActorID encodes a remote actor URL into a Warpnet follower id that is
// safe as a datastore key segment (base64url has no '/') and reversible, so the
// gateway can recover the actor URL from a follower list.
func encodeActorID(actorURL string) string {
	return warp.APFollowerPrefix + base64.RawURLEncoding.EncodeToString([]byte(actorURL))
}

func decodeActorID(id string) (string, error) {
	enc, ok := strings.CutPrefix(id, warp.APFollowerPrefix)
	if !ok {
		return "", errNotAPFollower
	}
	bt, err := base64.RawURLEncoding.DecodeString(enc)
	if err != nil {
		return "", err
	}
	return string(bt), nil
}

// memFollowerStore bounds: dev-only data, so size/TTL eviction losing a
// follower list is acceptable — production keeps the graph in Warpnet.
const (
	memFollowerSize = 1024
	memFollowerTTL  = 24 * time.Hour
)

// memFollowerStore is an in-memory dev fallback used only when the gateway has
// no Warpnet node connection. Nothing touches the disk (see CLAUDE.md); the
// expirable LRU keeps it bounded. The mutex makes Add's read-modify-write atomic.
type memFollowerStore struct {
	mu   sync.Mutex
	data *expirable.LRU[string, []string] // localUser -> actor URLs
}

func newMemFollowerStore() *memFollowerStore {
	return &memFollowerStore{data: expirable.NewLRU[string, []string](memFollowerSize, nil, memFollowerTTL)}
}

func (s *memFollowerStore) Add(localUser, actorURL string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	urls, _ := s.data.Get(localUser)
	if slices.Contains(urls, actorURL) {
		return nil
	}
	s.data.Add(localUser, append(urls, actorURL))
	return nil
}

func (s *memFollowerStore) List(localUser string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src, _ := s.data.Get(localUser)
	out := make([]string, len(src))
	copy(out, src)
	return out, nil
}
