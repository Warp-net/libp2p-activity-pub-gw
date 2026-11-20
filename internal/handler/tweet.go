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

type TweetFetcher interface {
	GetTweetsOrReplies(ctx context.Context, ev warp.GetAllTweetsEvent) (warp.TweetsResponse, error)
	GetTweet(ctx context.Context, id string) (warp.Tweet, error)
	GetTweetStats(ctx context.Context, id string) (event.TweetStatsResponse, error)
}

type MastodonTweetStorer interface {
	TweetFetcher
	Reply(ctx context.Context, ev warp.Tweet) error
	Delete(ctx context.Context, ev warp.DeleteTweetEvent) error
}

type NostrTweetFetcher interface {
	TweetFetcher
	Serves(id string) bool
	IsReplyTo(ev warp.Tweet) bool
}

type NostrTweetPublisher interface {
	Reply(ev warp.Tweet)
	DeleteReply(ev warp.DeleteTweetEvent)
}

type TweetFederator interface {
	FederateTweetAsync(t warp.Tweet)
}

func StreamGetTweetsHandler(mastodonBridge MastodonTweetStorer, nostrBridge NostrTweetFetcher) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.GetAllTweetsEvent
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if nostrBridge.Serves(ev.UserId) || nostrBridge.Serves(ev.RootId) || nostrBridge.Serves(ev.ParentId) {
			return nostrBridge.GetTweetsOrReplies(ctx, ev)
		}
		return mastodonBridge.GetTweetsOrReplies(ctx, ev)
	}
}

func StreamGetTweetHandler(mastodonBridge MastodonTweetStorer, nostrBridge NostrTweetFetcher) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.GetTweetEvent
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if nostrBridge.Serves(ev.TweetId) {
			return nostrBridge.GetTweet(ctx, ev.TweetId)
		}
		return mastodonBridge.GetTweet(ctx, ev.TweetId)
	}
}

func StreamGetTweetStatsHandler(mastodonBridge MastodonTweetStorer, nostrBridge NostrTweetFetcher) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.GetTweetEvent
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if nostrBridge.Serves(ev.TweetId) {
			return nostrBridge.GetTweetStats(ctx, ev.TweetId)
		}
		return mastodonBridge.GetTweetStats(ctx, ev.TweetId)
	}
}

// StreamViewTweetHandler acknowledges a view: the bridged networks count none.
func StreamViewTweetHandler() node.HandlerFunc {
	return func(context.Context, []byte) (any, error) {
		return event.ViewsCountResponse{Count: 1}, nil
	}
}

// StreamNewTweetHandler federates the owner's own top-level tweet and sends a
// reply to the bridged thread it answers. The tweet is echoed back as the node
// stored it, so the Warpnet UI renders it.
func StreamNewTweetHandler(
	mastodonBridge MastodonTweetStorer,
	nostrBridge NostrTweetFetcher,
	nostrPublisher NostrTweetPublisher,
	gateway TweetFederator,
) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.Tweet
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if !ev.IsReply() {
			if warp.PublishableTweet(ev, ev.UserId) {
				gateway.FederateTweetAsync(ev)
			}
			return ev, nil
		}
		if nostrBridge.IsReplyTo(ev) {
			go nostrPublisher.Reply(ev)
			return ev, nil
		}
		return ev, mastodonBridge.Reply(ctx, ev)
	}
}

func StreamNewReplyHandler(
	mastodonBridge MastodonTweetStorer,
	nostrBridge NostrTweetFetcher,
	nostrPublisher NostrTweetPublisher,
) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.Tweet
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if nostrBridge.IsReplyTo(ev) {
			go nostrPublisher.Reply(ev)
			return ev, nil
		}
		return ev, mastodonBridge.Reply(ctx, ev)
	}
}

// StreamTimelineHandler receives a followed author's new tweet as their
// followers' node. Replies live in their thread, so only a top-level tweet
// federates; everything is acknowledged so the sender does not retry.
func StreamTimelineHandler(gateway TweetFederator) node.HandlerFunc {
	return func(_ context.Context, buf []byte) (any, error) {
		var ev warp.Tweet
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if !ev.IsReply() && warp.PublishableTweet(ev, ev.UserId) {
			gateway.FederateTweetAsync(ev)
		}
		return ev, nil
	}
}

// StreamDeleteTweetHandler federates a reply deletion, which Warpnet forwards
// to the parent author's node. Only a reply targets a bridged note.
func StreamDeleteTweetHandler(
	mastodonBridge MastodonTweetStorer,
	nostrBridge NostrTweetFetcher,
	nostrPublisher NostrTweetPublisher,
) node.HandlerFunc {
	return func(ctx context.Context, buf []byte) (any, error) {
		var ev warp.DeleteTweetEvent
		if err := json.Unmarshal(buf, &ev); err != nil {
			return nil, err
		}
		if nostrBridge.Serves(ev.ParentId) || nostrBridge.Serves(ev.RootId) {
			go nostrPublisher.DeleteReply(ev)
			return struct{}{}, nil
		}
		if ev.ParentId == "" && ev.RootId == "" {
			return struct{}{}, nil
		}
		return struct{}{}, mastodonBridge.Delete(ctx, ev)
	}
}
