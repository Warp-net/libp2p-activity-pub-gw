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
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/warp"

	log "github.com/sirupsen/logrus"
)

// nodeFollowerStore keeps the AP follow graph in Warpnet by reusing the
// existing follow routes: PUBLIC_POST_FOLLOW records "remote actor follows
// owner" on the owner's node, PUBLIC_GET_FOLLOWERS reads them back. Remote
// actor URLs travel as base64url follower ids (see encodeActorID), so the
// gateway itself stores no follower state.
type nodeFollowerStore struct {
	req      warp.Requester
	resolver actorResolver
}

// actorResolver turns a stored follower id into the actor url the gateway
// delivers to. Warpnet only ever sees the "name@instance" handle; the encoding
// of older "ap:" ids stays an implementation detail behind this.
type actorResolver interface {
	resolveActorID(ctx context.Context, id string) (string, error)
	canonicalHandle(ctx context.Context, actorURL string) string
}

const (
	followersPageSize = uint64(100)
	followersMaxPages = 100

	// resolveFollowerTimeout bounds the WebFinger lookup a handle needs; the
	// result is cached, so this is paid once per follower per cache window.
	resolveFollowerTimeout = 10 * time.Second
)

// followerID is the Warpnet id a Fediverse follower is stored under. It is
// resolved through the actor document where the url alone would yield an
// unresolvable handle (see gateway.canonicalHandle); the store's own interface
// carries no context, so the lookup gets the same bounded one a handle
// resolution gets elsewhere.
func (s nodeFollowerStore) followerID(actorURL string) string {
	if s.resolver == nil {
		return bridgedUserID(actorURL)
	}
	ctx, cancel := context.WithTimeout(context.Background(), resolveFollowerTimeout)
	defer cancel()
	if handle := s.resolver.canonicalHandle(ctx, actorURL); strings.Contains(handle, "@") {
		return handle
	}
	return bridgedUserID(actorURL)
}

func (s nodeFollowerStore) Add(localUser, actorURL string) error {
	bt, err := s.req.RequestUser(localUser, warp.RoutePostFollow, warp.NewFollowEvent{
		FollowerId:  s.followerID(actorURL),
		FollowingId: localUser,
	})
	if err != nil {
		return err
	}
	return warp.NodeResponseError(bt)
}

func (s nodeFollowerStore) List(localUser string) ([]string, error) {
	limit := followersPageSize
	var cursor string
	urls := make([]string, 0, limit)
	for range followersMaxPages {
		ev := warp.GetFollowersEvent{UserId: localUser, Limit: &limit}
		if cursor != "" {
			ev.Cursor = &cursor
		}
		bt, err := s.req.RequestUser(localUser, warp.RouteGetFollowers, ev)
		if err != nil {
			return nil, err
		}
		if err := warp.NodeResponseError(bt); err != nil {
			return nil, err
		}
		var resp warp.FollowersResponse
		if err := json.Unmarshal(bt, &resp); err != nil {
			return nil, err
		}
		for _, id := range resp.Followers {
			if !warp.IsBridgedUserID(id) {
				continue // native Warpnet follower, not a Fediverse actor
			}
			ctx, cancel := context.WithTimeout(context.Background(), resolveFollowerTimeout)
			actorURL, rerr := s.resolver.resolveActorID(ctx, id)
			cancel()
			if rerr != nil {
				log.Warnf("followers: resolving %s: %v", id, rerr)
				continue
			}
			urls = append(urls, actorURL)
		}
		if len(resp.Followers) == 0 || resp.Cursor == "" || resp.Cursor == cursor {
			break
		}
		cursor = resp.Cursor
	}
	return urls, nil
}
