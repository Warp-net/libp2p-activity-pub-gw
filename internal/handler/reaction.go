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
	"github.com/Warp-net/warpnet/event"
	"github.com/Warp-net/warpnet/json"
)

type MastodonReactionStorer interface {
	React(ctx context.Context, localUser, objectURL, emoji string, undo bool) (uint64, error)
}

type NostrReactedTweetFetcher interface {
	Serves(id string) bool
}

type NostrReactionPublisher interface {
	React(actor, tweetID, emoji string, undo bool)
}

// StreamReactionHandler federates a reaction on a bridged tweet as a Like,
// which only the default heart maps onto. A client that predates reactions
// names no emoji, which means that heart.
func StreamReactionHandler(
	mastodonBridge MastodonReactionStorer,
	nostrBridge NostrReactedTweetFetcher,
	nostrPublisher NostrReactionPublisher,
) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.ReactionEvent
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		emoji, err := warp.NormalizeReaction(ev.Emoji)
		if err != nil {
			return nil, err
		}
		if nostrBridge.Serves(ev.TweetId) {
			go nostrPublisher.React(ev.OwnerId, ev.TweetId, emoji, false)
			return buildReactionsCount(emoji, 0), nil
		}
		count, err := mastodonBridge.React(ctx, ev.OwnerId, ev.TweetId, emoji, false)
		return buildReactionsCount(emoji, count), err
	}
}

// StreamUnreactionHandler drops whichever reaction the user had, so it names
// no emoji and always federates as an Undo(Like).
func StreamUnreactionHandler(
	mastodonBridge MastodonReactionStorer,
	nostrBridge NostrReactedTweetFetcher,
	nostrPublisher NostrReactionPublisher,
) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.ReactionEvent
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if nostrBridge.Serves(ev.TweetId) {
			go nostrPublisher.React(ev.OwnerId, ev.TweetId, "", true)
			return buildReactionsCount("", 0), nil
		}
		count, err := mastodonBridge.React(ctx, ev.OwnerId, ev.TweetId, "", true)
		return buildReactionsCount("", count), err
	}
}

// buildReactionsCount attributes the favourite tally to the emoji, so the
// client repaints the chip without another round trip.
func buildReactionsCount(emoji string, count uint64) event.ReactionsCountResponse {
	resp := event.ReactionsCountResponse{Count: count}
	if emoji != "" && count > 0 {
		resp.Reactions = map[string]uint64{emoji: count}
	}
	return resp
}
