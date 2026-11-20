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

import (
	"github.com/Warp-net/bridge-gateway/internal/fediverse"
	"github.com/Warp-net/bridge-gateway/internal/handler"
	"github.com/Warp-net/bridge-gateway/internal/node"
	"github.com/Warp-net/bridge-gateway/internal/nostr"
	"github.com/Warp-net/bridge-gateway/internal/warp"
	"github.com/Warp-net/warpnet/event"
)

// routes are the Warpnet routes the gateway answers on node n. A nil hub (Nostr
// off) leaves a nil Nostr bridge, which serves nothing.
func routes(gateway *fediverse.Gateway, nostrHub *nostr.Hub, n nostr.HubNode) map[string]node.HandlerFunc {
	mastodonBridge := fediverse.NewMastodonBridge(gateway, n.ID())
	var (
		nostrBridge    *nostr.Bridge
		nostrPublisher *nostr.Publisher
	)
	if nostrHub != nil {
		nostrBridge, nostrPublisher = nostrHub.NewBridge(n)
	}

	return map[string]node.HandlerFunc{
		event.PUBLIC_GET_WHOTOFOLLOW: handler.StreamGetWhoToFollowHandler(mastodonBridge, nostrBridge),
		warp.RouteGetUser:            handler.StreamGetUserHandler(mastodonBridge, nostrBridge),
		warp.RouteGetUsers:           handler.StreamGetUsersHandler(warp.DefaultOwnerHandle, mastodonBridge, nostrBridge),

		warp.RouteGetTweets:          handler.StreamGetTweetsHandler(mastodonBridge, nostrBridge),
		warp.RouteGetTweet:           handler.StreamGetTweetHandler(mastodonBridge, nostrBridge),
		event.PUBLIC_GET_TWEET_STATS: handler.StreamGetTweetStatsHandler(mastodonBridge, nostrBridge),
		event.PUBLIC_POST_VIEW:       handler.StreamViewTweetHandler(),
		warp.RoutePostTweet:          handler.StreamNewTweetHandler(mastodonBridge, nostrBridge, nostrPublisher, gateway),
		warp.RoutePostReply:          handler.StreamNewReplyHandler(mastodonBridge, nostrBridge, nostrPublisher),
		warp.RoutePostTimeline:       handler.StreamTimelineHandler(gateway),
		warp.RouteDeleteTweet:        handler.StreamDeleteTweetHandler(mastodonBridge, nostrBridge, nostrPublisher),

		warp.RouteGetFollowers:  handler.StreamGetFollowersHandler(mastodonBridge, nostrBridge),
		warp.RouteGetFollowings: handler.StreamGetFollowingsHandler(mastodonBridge, nostrBridge),
		warp.RoutePostFollow:    handler.StreamFollowHandler(mastodonBridge, nostrBridge, nostrPublisher, nostrHub),
		warp.RoutePostUnfollow:  handler.StreamUnfollowHandler(mastodonBridge, nostrBridge, nostrPublisher, nostrHub),

		warp.RoutePostReact:   handler.StreamReactionHandler(mastodonBridge, nostrBridge, nostrPublisher),
		warp.RoutePostUnreact: handler.StreamUnreactionHandler(mastodonBridge, nostrBridge, nostrPublisher),

		warp.RoutePostRetweet:   handler.StreamNewReTweetHandler(mastodonBridge, nostrBridge, nostrPublisher),
		warp.RoutePostUnretweet: handler.StreamUnretweetHandler(mastodonBridge, nostrBridge, nostrPublisher),

		warp.RouteGetImage: handler.StreamGetImageHandler(mastodonBridge),
	}
}
