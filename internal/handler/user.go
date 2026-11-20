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

type UserFetcher interface {
	GetUser(ctx context.Context, handle string) (warp.User, error)
	GetUserBrief(ctx context.Context, handle string) (warp.User, error)
}

type MastodonUserFetcher interface {
	UserFetcher
	GetWhoToFollow(ctx context.Context, ev warp.GetAllUsersEvent) (warp.UsersResponse, error)
}

type NostrUserFetcher interface {
	UserFetcher
	WhoToFollow(ctx context.Context) []warp.User
	Serves(id string) bool
}

func StreamGetUserHandler(mastodonBridge MastodonUserFetcher, nostrBridge NostrUserFetcher) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.GetUserEvent
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if nostrBridge.Serves(ev.UserId) {
			return nostrBridge.GetUser(ctx, ev.UserId)
		}
		return mastodonBridge.GetUser(ctx, ev.UserId)
	}
}

// StreamGetUsersHandler answers the owner's listing with who-to-follow
// candidates, so Warpnet's user refresh stores their profiles, and any other
// handle with its one profile.
func StreamGetUsersHandler(
	ownerHandle string,
	mastodonBridge MastodonUserFetcher,
	nostrBridge NostrUserFetcher,
) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.GetAllUsersEvent
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if ev.UserId == ownerHandle {
			return getWhoToFollow(ctx, mastodonBridge, nostrBridge, ev)
		}
		fetcher := UserFetcher(mastodonBridge)
		if nostrBridge.Serves(ev.UserId) {
			fetcher = nostrBridge
		}
		u, err := fetcher.GetUserBrief(ctx, ev.UserId)
		if err != nil {
			return warp.UsersResponse{}, nil //nolint:nilerr // an unknown handle lists nobody
		}
		return warp.UsersResponse{Users: []warp.User{u}}, nil
	}
}

func StreamGetWhoToFollowHandler(mastodonBridge MastodonUserFetcher, nostrBridge NostrUserFetcher) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.GetAllUsersEvent
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		return getWhoToFollow(ctx, mastodonBridge, nostrBridge, ev)
	}
}

// getWhoToFollow puts the Nostr suggestions in front of the first Mastodon page.
func getWhoToFollow(
	ctx context.Context,
	mastodonBridge MastodonUserFetcher,
	nostrBridge NostrUserFetcher,
	ev warp.GetAllUsersEvent,
) (warp.UsersResponse, error) {
	resp, err := mastodonBridge.GetWhoToFollow(ctx, ev)
	if ev.Cursor != nil && *ev.Cursor != "" {
		return resp, err
	}
	resp.Users = append(nostrBridge.WhoToFollow(ctx), resp.Users...)
	return resp, nil
}
