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

type MastodonRetweetStorer interface {
	Announce(ctx context.Context, localUser, objectURL string, undo bool) error
}

type NostrRetweetedTweetFetcher interface {
	Serves(id string) bool
}

type NostrRetweetPublisher interface {
	Quote(ev warp.Tweet)
	Repost(actor, tweetID string, undo bool)
}

func StreamNewReTweetHandler(
	mastodonBridge MastodonRetweetStorer,
	nostrBridge NostrRetweetedTweetFetcher,
	nostrPublisher NostrRetweetPublisher,
) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.Tweet
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if ev.QuotedTweetId != nil && nostrBridge.Serves(*ev.QuotedTweetId) {
			go nostrPublisher.Quote(ev)
			return ev, nil
		}

		retweeterId := ev.UserId
		if ev.RetweetedBy != nil && *ev.RetweetedBy != "" {
			retweeterId = *ev.RetweetedBy
		}
		tweetId := ev.Id
		if tweetId == "" {
			tweetId = ev.RootId
		}
		if nostrBridge.Serves(tweetId) {
			go nostrPublisher.Repost(retweeterId, tweetId, false)
			return ev, nil
		}
		return ev, mastodonBridge.Announce(ctx, retweeterId, tweetId, false)
	}
}

func StreamUnretweetHandler(
	mastodonBridge MastodonRetweetStorer,
	nostrBridge NostrRetweetedTweetFetcher,
	nostrPublisher NostrRetweetPublisher,
) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.UnretweetEvent
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if nostrBridge.Serves(ev.TweetId) {
			go nostrPublisher.Repost(ev.RetweeterId, ev.TweetId, true)
			return struct{}{}, nil
		}
		return struct{}{}, mastodonBridge.Announce(ctx, ev.RetweeterId, ev.TweetId, true)
	}
}
