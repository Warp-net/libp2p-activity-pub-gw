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

package handler

import (
	"context"

	"github.com/Warp-net/bridge-gateway/internal/node"
	"github.com/Warp-net/bridge-gateway/internal/warp"
	"github.com/Warp-net/warpnet/json"
)

type FollowsFetcher interface {
	GetFollowers(ctx context.Context, handle string, cursor *string) (warp.FollowersResponse, error)
	GetFollowings(ctx context.Context, handle string, cursor *string) (warp.FollowingsResponse, error)
}

type MastodonFollowStorer interface {
	FollowsFetcher
	Follow(ctx context.Context, localUser, followingHandle string, undo bool) error
}

type NostrFollowsFetcher interface {
	FollowsFetcher
	Serves(id string) bool
}

type NostrFollowPublisher interface {
	Follow(actor, handle string, undo bool)
}

// NostrFollowRecorder keeps the follow graph the live Nostr feed delivers by.
type NostrFollowRecorder interface {
	SetFollow(follower, handle string, undo bool)
}

func StreamGetFollowersHandler(mastodonBridge MastodonFollowStorer, nostrBridge NostrFollowsFetcher) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.GetFollowersEvent
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if nostrBridge.Serves(ev.UserId) {
			return nostrBridge.GetFollowers(ctx, ev.UserId, ev.Cursor)
		}
		return mastodonBridge.GetFollowers(ctx, ev.UserId, ev.Cursor)
	}
}

func StreamGetFollowingsHandler(mastodonBridge MastodonFollowStorer, nostrBridge NostrFollowsFetcher) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.GetFollowersEvent
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if nostrBridge.Serves(ev.UserId) {
			return nostrBridge.GetFollowings(ctx, ev.UserId, ev.Cursor)
		}
		return mastodonBridge.GetFollowings(ctx, ev.UserId, ev.Cursor)
	}
}

func StreamFollowHandler(
	mastodonBridge MastodonFollowStorer,
	nostrBridge NostrFollowsFetcher,
	nostrPublisher NostrFollowPublisher,
	nostrLive NostrFollowRecorder,
) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.NewFollowEvent
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if nostrBridge.Serves(ev.FollowingId) {
			nostrLive.SetFollow(ev.FollowerId, ev.FollowingId, false)
			go nostrPublisher.Follow(ev.FollowerId, ev.FollowingId, false)
			return struct{}{}, nil
		}
		return struct{}{}, mastodonBridge.Follow(ctx, ev.FollowerId, ev.FollowingId, false)
	}
}

func StreamUnfollowHandler(
	mastodonBridge MastodonFollowStorer,
	nostrBridge NostrFollowsFetcher,
	nostrPublisher NostrFollowPublisher,
	nostrLive NostrFollowRecorder,
) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.NewFollowEvent
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if nostrBridge.Serves(ev.FollowingId) {
			nostrLive.SetFollow(ev.FollowerId, ev.FollowingId, true)
			go nostrPublisher.Follow(ev.FollowerId, ev.FollowingId, true)
			return struct{}{}, nil
		}
		return struct{}{}, mastodonBridge.Follow(ctx, ev.FollowerId, ev.FollowingId, true)
	}
}
