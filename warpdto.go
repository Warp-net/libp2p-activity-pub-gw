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
	routeGetUser       = event.PUBLIC_GET_USER
	routeGetUsers      = event.PUBLIC_GET_USERS
	routeGetTweet      = event.PUBLIC_GET_TWEET
	routeGetTweets     = event.PUBLIC_GET_TWEETS
	routeGetFollowers  = event.PUBLIC_GET_FOLLOWERS
	routeGetFollowings = event.PUBLIC_GET_FOLLOWINGS
	routeGetImage      = event.PUBLIC_GET_IMAGE
	routePostFollow    = event.PUBLIC_POST_FOLLOW
	routePostUnfollow  = event.PUBLIC_POST_UNFOLLOW
	// routePostReact/routePostUnreact are warpnet's reaction routes; they
	// replaced the binary like/unlike ones. A reaction carries an emoji, and
	// the heart (defaultReaction) is the one that maps to a Mastodon
	// favourite — see mastodonBridge.React.
	routePostReact     = event.PUBLIC_POST_REACT
	routePostUnreact   = event.PUBLIC_POST_UNREACT
	routePostRetweet   = event.PUBLIC_POST_RETWEET
	routePostUnretweet = event.PUBLIC_POST_UNRETWEET
	routePostTweet     = event.PRIVATE_POST_TWEET
	routePostReply     = event.PUBLIC_POST_REPLY
	// routeDeleteTweet is the route warpnet forwards a reply deletion over to
	// the parent author's node.
	routeDeleteTweet = event.PRIVATE_DELETE_TWEET
)

// Wire envelope + domain payloads (warpnet's own types).
type (
	message = event.Message
	tweet   = domain.Tweet
	user    = domain.User
)

// Request/response event payloads (warpnet's own types). GetAllTweetsEvent and
// GetTweetEvent now carry root_id/parent_id themselves, so the thread ids the
// gateway needs no longer have to be decoded into local shadow structs.
type (
	getUserEvent       = event.GetUserEvent
	getAllUsersEvent   = event.GetAllUsersEvent
	usersResponse      = event.UsersResponse
	getTweetEvent      = event.GetTweetEvent
	getAllTweetsEvent  = event.GetAllTweetsEvent
	tweetsResponse     = event.TweetsResponse
	getFollowersEvent  = event.GetAllTweetsEvent // {user_id, cursor}; same shape for followers/followings
	followersResponse  = event.FollowersResponse
	followingsResponse = event.FollowingsResponse
	newFollowEvent     = event.NewFollowEvent
	reactionEvent      = event.ReactionEvent
	unretweetEvent     = event.UnretweetEvent
	deleteTweetEvent   = event.DeleteTweetEvent
	getImageEvent      = event.GetImageEvent
	getImageResponse   = event.GetImageResponse
)

const (
	defaultReaction  = "❤️"
	maxReactionRunes = 8
)

func normalizeReaction(emoji string) (string, error) {
	if emoji == "" {
		return defaultReaction, nil
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
