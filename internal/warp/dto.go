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

package warp

// The gateway's wire contract is now warpnet's own: route protocol IDs come
// from event.PUBLIC_*, and payloads/responses are warpnet's event/domain types
// (aliased to the gateway's local names so call sites stay terse). domain.ID is
// a string alias, so these are byte-for-byte the shapes the node speaks.

import (
	"errors"
	"unicode"
	"unicode/utf8"

	"github.com/Warp-net/warpnet/domain"
	"github.com/Warp-net/warpnet/event"
)

// Public-route protocol IDs (each is also the libp2p protocol string).
const (
	RouteGetUser       = event.PUBLIC_GET_USER
	RouteGetUsers      = event.PUBLIC_GET_USERS
	RouteGetTweet      = event.PUBLIC_GET_TWEET
	RouteGetTweets     = event.PUBLIC_GET_TWEETS
	RouteGetFollowers  = event.PUBLIC_GET_FOLLOWERS
	RouteGetFollowings = event.PUBLIC_GET_FOLLOWINGS
	RouteGetImage      = event.PUBLIC_GET_IMAGE
	RoutePostFollow    = event.PUBLIC_POST_FOLLOW
	RoutePostUnfollow  = event.PUBLIC_POST_UNFOLLOW
	// RoutePostReact/routePostUnreact are warpnet's reaction routes; they
	// replaced the binary like/unlike ones. A reaction carries an emoji, and
	// the heart (defaultReaction) is the one that maps to a Mastodon
	// favourite — see mastodonBridge.React.
	RoutePostReact     = event.PUBLIC_POST_REACT
	RoutePostUnreact   = event.PUBLIC_POST_UNREACT
	RoutePostRetweet   = event.PUBLIC_POST_RETWEET
	RoutePostUnretweet = event.PUBLIC_POST_UNRETWEET
	RoutePostTweet     = event.PRIVATE_POST_TWEET
	// RoutePostTimeline is warpnet's follower-delivery route: a followed
	// author's new tweet arrives here (PRIVATE_POST_TWEET now strictly means
	// "the owner composes"; the gateway is the Fediverse followers' node).
	RoutePostTimeline = event.PUBLIC_POST_TIMELINE
	RoutePostReply    = event.PUBLIC_POST_REPLY
	// RouteDeleteTweet is the route warpnet forwards a reply deletion over to
	// the parent author's node.
	RouteDeleteTweet = event.PRIVATE_DELETE_TWEET
)

// Wire envelope + domain payloads (warpnet's own types).
type (
	Message = event.Message
	Tweet   = domain.Tweet
	User    = domain.User
)

// Request/response event payloads (warpnet's own types). GetAllTweetsEvent and
// GetTweetEvent now carry root_id/parent_id themselves, so the thread ids the
// gateway needs no longer have to be decoded into local shadow structs.
type (
	GetUserEvent       = event.GetUserEvent
	GetAllUsersEvent   = event.GetAllUsersEvent
	UsersResponse      = event.UsersResponse
	GetTweetEvent      = event.GetTweetEvent
	GetAllTweetsEvent  = event.GetAllTweetsEvent
	TweetsResponse     = event.TweetsResponse
	GetFollowersEvent  = event.GetAllTweetsEvent // {user_id, cursor}; same shape for followers/followings
	FollowersResponse  = event.FollowersResponse
	FollowingsResponse = event.FollowingsResponse
	NewFollowEvent     = event.NewFollowEvent
	ReactionEvent      = event.ReactionEvent
	UnretweetEvent     = event.UnretweetEvent
	DeleteTweetEvent   = event.DeleteTweetEvent
	GetImageEvent      = event.GetImageEvent
	GetImageResponse   = event.GetImageResponse
)

const (
	DefaultReaction  = "❤️"
	maxReactionRunes = 8
)

func NormalizeReaction(emoji string) (string, error) {
	if emoji == "" {
		return DefaultReaction, nil
	}
	if !utf8.ValidString(emoji) {
		return "", errors.New("reaction: not a valid utf-8 string")
	}
	if utf8.RuneCountInString(emoji) > maxReactionRunes {
		return "", errors.New("reaction: too long")
	}
	for _, r := range emoji {
		if r == '/' || unicode.IsSpace(r) || unicode.IsControl(r) {
			return "", errors.New("reaction: forbidden character")
		}
	}
	return emoji, nil
}
