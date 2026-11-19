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
	"context"
	"io"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/Warp-net/activity-pub-gw/internal/fediverse"
	"github.com/Warp-net/activity-pub-gw/internal/warp"
	"github.com/Warp-net/warpnet/core/warpnet"
	"github.com/Warp-net/warpnet/event"

	// nodeserver is the libp2p transport for the Mastodon -> Warpnet direction: it
	// answers Warpnet's public /public routes. To a Warpnet node the gateway is an
	// ordinary member peer — it reports node info with a hardcoded Mastodon owner
	// and resolves every request through the mastodonBridge. It verifies the
	// caller's signature (like the node's auth middleware) but holds no domain
	// logic itself, and does NOT register the spoof-challenge route (it is not a
	// Warpnet-codebase node; discovery tolerates that).

	wjson "github.com/Warp-net/warpnet/json"
	"github.com/Warp-net/warpnet/security"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	// maxRequestBytes bounds an inbound request envelope (the written response,
	// e.g. an image, is not capped here).
	maxRequestBytes = 1 << 20
	// apRequestTimeout bounds the ActivityPub work to answer one node request.
	apRequestTimeout = 90 * time.Second
)

// routeHandler answers one public route from the signed request body.
type routeHandler func(ctx context.Context, body []byte) (any, error)

// serveRoutes registers the gateway as a libp2p server for Warpnet's public
// routes, delegating all domain work to the bridge. ownerHandle is the Mastodon
// account advertised as the node owner.
func (c *nodeClient) serveRoutes(g *fediverse.Gateway, hub *nostrHub, ownerHandle string) {
	b := fediverse.NewMastodonBridge(g, c.h.ID().String())
	nb := hub.bridgeFor(c)

	handlers := map[string]routeHandler{
		event.PUBLIC_GET_INFO: c.infoHandler(ownerHandle),
		event.PUBLIC_GET_WHOTOFOLLOW: wrapJSON(func(ctx context.Context, ev warp.GetAllUsersEvent) (any, error) {
			return b.GetWhoToFollow(ctx, ev)
		}),

		warp.RouteGetUser: wrapJSON(func(ctx context.Context, ev warp.GetUserEvent) (any, error) {
			if nb.Serves(string(ev.UserId)) {
				return nb.GetUser(ctx, string(ev.UserId))
			}
			return b.GetUser(ctx, string(ev.UserId))
		}),
		warp.RouteGetUsers: wrapJSON(func(ctx context.Context, ev warp.GetAllUsersEvent) (any, error) {
			if string(ev.UserId) == ownerHandle {
				return b.GetWhoToFollow(ctx, ev)
			}
			getBrief := b.GetUserBrief
			if nb.Serves(string(ev.UserId)) {
				getBrief = nb.GetUserBrief
			}
			u, err := getBrief(ctx, string(ev.UserId)) // list context: skip count fetches
			if err != nil {
				return event.UsersResponse{}, nil //nolint:nilerr // unknown handle -> empty
			}
			return event.UsersResponse{Users: []warp.User{u}}, nil
		}),
		warp.RouteGetTweets: wrapJSON(func(ctx context.Context, ev warp.GetAllTweetsEvent) (any, error) {
			if nb.Serves(ev.UserId) || nb.Serves(ev.RootId) || nb.Serves(ev.ParentId) {
				return nb.GetTweetsOrReplies(ctx, ev)
			}
			return b.GetTweetsOrReplies(ctx, ev)
		}),
		warp.RouteGetTweet: wrapJSON(func(ctx context.Context, ev warp.GetTweetEvent) (any, error) {
			if nb.Serves(string(ev.TweetId)) {
				return nb.GetTweet(ctx, string(ev.TweetId))
			}
			return b.GetTweet(ctx, string(ev.TweetId))
		}),
		event.PUBLIC_GET_TWEET_STATS: wrapJSON(func(ctx context.Context, ev warp.GetTweetEvent) (any, error) {
			if nb.Serves(string(ev.TweetId)) {
				return nb.GetTweetStats(ctx, string(ev.TweetId))
			}
			return b.GetTweetStats(ctx, string(ev.TweetId))
		}),
		warp.RouteGetFollowers: wrapJSON(func(ctx context.Context, ev warp.GetFollowersEvent) (any, error) {
			if nb.Serves(ev.UserId) {
				return nb.GetFollowers(ctx, ev.UserId, ev.Cursor)
			}
			return b.GetFollowers(ctx, string(ev.UserId), ev.Cursor)
		}),
		warp.RouteGetFollowings: wrapJSON(func(ctx context.Context, ev warp.GetFollowersEvent) (any, error) {
			if nb.Serves(ev.UserId) {
				return nb.GetFollowings(ctx, ev.UserId, ev.Cursor)
			}
			return b.GetFollowings(ctx, string(ev.UserId), ev.Cursor)
		}),
		warp.RouteGetImage: wrapJSON(func(ctx context.Context, ev warp.GetImageEvent) (any, error) {
			return b.GetImage(ctx, ev.Key)
		}),
		event.PUBLIC_POST_VIEW: func(context.Context, []byte) (any, error) {
			return event.ViewsCountResponse{Count: 1}, nil
		},

		// A Warpnet reaction on a bridged status federates as an AP Like — a
		// Mastodon favourite — which only the default heart maps onto (React
		// drops any other emoji). The per-emoji breakdown mirrors it so the
		// client repaints the heart chip without a second round-trip.
		//
		// The emoji is normalized here, at the wire boundary: the node forwards
		// whatever the client sent, and a client that predates reactions sends
		// none — which means the default heart, i.e. a favourite.
		warp.RoutePostReact: wrapJSON(func(ctx context.Context, ev warp.ReactionEvent) (any, error) {
			emoji, err := warp.NormalizeReaction(ev.Emoji)
			if err != nil {
				return nil, err
			}
			if nb.Serves(string(ev.TweetId)) {
				go nb.Pub.React(string(ev.OwnerId), string(ev.TweetId), emoji, false)
				return reactionsCount(emoji, 0), nil
			}
			count, rerr := b.React(ctx, string(ev.OwnerId), string(ev.TweetId), emoji, false)
			return reactionsCount(emoji, count), rerr
		}),
		// An unreact drops whichever reaction the user had, so it names no emoji
		// and always federates as an Undo(Like).
		warp.RoutePostUnreact: wrapJSON(func(ctx context.Context, ev warp.ReactionEvent) (any, error) {
			if nb.Serves(string(ev.TweetId)) {
				go nb.Pub.React(string(ev.OwnerId), string(ev.TweetId), "", true)
				return reactionsCount("", 0), nil
			}
			count, err := b.React(ctx, string(ev.OwnerId), string(ev.TweetId), "", true)
			return reactionsCount("", count), err
		}),
		warp.RoutePostFollow: wrapJSON(func(ctx context.Context, ev warp.NewFollowEvent) (any, error) {
			if nb.Serves(string(ev.FollowingId)) {
				hub.setFollow(string(ev.FollowerId), string(ev.FollowingId), false)
				go nb.Pub.Follow(string(ev.FollowerId), string(ev.FollowingId), false)
				return struct{}{}, nil
			}
			return struct{}{}, b.Follow(ctx, string(ev.FollowerId), string(ev.FollowingId), false)
		}),
		warp.RoutePostUnfollow: wrapJSON(func(ctx context.Context, ev warp.NewFollowEvent) (any, error) {
			if nb.Serves(string(ev.FollowingId)) {
				hub.setFollow(string(ev.FollowerId), string(ev.FollowingId), true)
				go nb.Pub.Follow(string(ev.FollowerId), string(ev.FollowingId), true)
				return struct{}{}, nil
			}
			return struct{}{}, b.Follow(ctx, string(ev.FollowerId), string(ev.FollowingId), true)
		}),
		warp.RoutePostTweet: wrapJSON(func(ctx context.Context, ev warp.Tweet) (any, error) {
			if !ev.IsReply() {
				if warp.PublishableTweet(ev, ev.UserId) {
					g.FederateTweetAsync(ev)
				}
				return ev, nil
			}
			// Echo the reply back as the tweet the node stored, so the Warpnet
			// UI renders it.
			if nb.IsReplyTo(ev) {
				go nb.Pub.Reply(ev)
				return ev, nil
			}
			return ev, b.Reply(ctx, ev)
		}),
		warp.RoutePostReply: wrapJSON(func(ctx context.Context, ev warp.Tweet) (any, error) {
			if nb.IsReplyTo(ev) {
				go nb.Pub.Reply(ev)
				return ev, nil
			}
			return ev, b.Reply(ctx, ev)
		}),
		// A followed author's new tweet, delivered to this node as their
		// followers' node. Replies live in their thread, not the timeline, so
		// only a top-level post federates; everything is acknowledged so the
		// sender doesn't retry.
		warp.RoutePostTimeline: wrapJSON(func(ctx context.Context, ev warp.Tweet) (any, error) {
			if !ev.IsReply() && warp.PublishableTweet(ev, ev.UserId) {
				g.FederateTweetAsync(ev)
			}
			return ev, nil
		}),
		// Warpnet forwards a reply deletion to the parent author's node over the
		// private delete route; federate it as an AP Delete. Only a reply (a
		// parent set) targets a Mastodon note; anything else is acknowledged.
		warp.RouteDeleteTweet: wrapJSON(func(ctx context.Context, ev warp.DeleteTweetEvent) (any, error) {
			if nb.Serves(ev.ParentId) || nb.Serves(ev.RootId) {
				go nb.Pub.DeleteReply(ev)
				return struct{}{}, nil
			}
			if ev.ParentId == "" && ev.RootId == "" {
				return struct{}{}, nil
			}
			return struct{}{}, b.Delete(ctx, ev)
		}),
		warp.RoutePostRetweet: wrapJSON(func(ctx context.Context, ev warp.Tweet) (any, error) {
			if ev.QuotedTweetId != nil && nb.Serves(*ev.QuotedTweetId) {
				go nb.Pub.Quote(ev)
				return ev, nil
			}
			if nb.Serves(retweetObject(ev)) {
				go nb.Pub.Repost(retweeterOf(ev), retweetObject(ev), false)
				return ev, nil
			}
			return ev, b.Announce(ctx, retweeterOf(ev), retweetObject(ev), false)
		}),
		warp.RoutePostUnretweet: wrapJSON(func(ctx context.Context, ev warp.UnretweetEvent) (any, error) {
			if nb.Serves(string(ev.TweetId)) {
				go nb.Pub.Repost(string(ev.RetweeterId), string(ev.TweetId), true)
				return struct{}{}, nil
			}
			return struct{}{}, b.Announce(ctx, string(ev.RetweeterId), string(ev.TweetId), true)
		}),
	}

	for route, h := range handlers {
		c.h.SetStreamHandler(protocol.ID(route), c.streamHandler(route, h))
	}
	warp.NetLog(c.network).Infof("nodeserver: serving %d public routes as %s (owner %s)", len(handlers), c.h.ID(), ownerHandle)
}

// streamHandler reads the signed request envelope, verifies it against the
// caller's peer key (mirrors the node's auth middleware), dispatches, and writes
// the response.
func (c *nodeClient) streamHandler(route string, h routeHandler) network.StreamHandler {
	return func(s network.Stream) {
		defer func() { _ = s.Close() }()

		data, err := io.ReadAll(io.LimitReader(s, maxRequestBytes))
		if err != nil {
			warp.NetLog(c.network).Warnf("nodeserver: %s: read: %v", route, err)
			return
		}
		var msg warp.Message
		if uerr := wjson.Unmarshal(data, &msg); uerr != nil {
			warp.NetLog(c.network).Warnf("nodeserver: %s: bad envelope: %v", route, uerr)
			return
		}
		// Signature is mandatory, like the node's auth middleware: an unsigned
		// envelope must not bypass verification.
		if msg.Signature == "" {
			warp.NetLog(c.network).Warnf("nodeserver: %s: signature missing", route)
			return
		}
		if conn := s.Conn(); conn != nil {
			pub := warpnet.FromIDToPubKey(conn.RemotePeer())
			if verr := security.VerifySignature(pub, msg.SigningBytes(), msg.Signature); verr != nil {
				warp.NetLog(c.network).Warnf("nodeserver: %s: signature from %s invalid: %v", route, conn.RemotePeer(), verr)
				return
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), apRequestTimeout)
		defer cancel()
		// Trace the whole pipeline: every outbound REST/AP fetch this libp2p
		// request fans out to is logged indented under one id, then the summary
		// line below reports the fetch count and total wall-clock.
		ctx, tr := warp.StartTrace(ctx)
		start := time.Now()
		resp, herr := h(ctx, msg.Body)
		warp.NetLog(c.network).Infof("[%s] libp2p %s: %d REST calls in %s", tr.ID, route, tr.Calls.Load(), time.Since(start).Round(time.Millisecond))
		if herr != nil {
			warp.NetLog(c.network).Warnf("nodeserver: %s: %v", route, herr)
			resp = event.ResponseError{Message: herr.Error()}
		}
		out, merr := wjson.Marshal(resp)
		if merr != nil {
			warp.NetLog(c.network).Errorf("nodeserver: %s: marshal response: %v", route, merr)
			return
		}
		if _, werr := s.Write(out); werr != nil {
			warp.NetLog(c.network).Warnf("nodeserver: %s: write: %v", route, werr)
		}
	}
}

// infoHandler reports the gateway as a member node owned by ownerHandle. It is
// reachable only via relays (ForceReachabilityPrivate), so it reports private
// reachability, like a NAT'd member node.
func (c *nodeClient) infoHandler(ownerHandle string) routeHandler {
	ver, _ := semver.NewVersion("0.0.0")
	networkName := c.network // this node's own network, not the whole NODE_NETWORK list
	return func(context.Context, []byte) (any, error) {
		addrs := make([]string, 0, len(c.h.Addrs()))
		for _, a := range c.h.Addrs() {
			addrs = append(addrs, a.String())
		}
		return warpnet.NodeInfo{
			Type:         warpnet.MemberNode,
			OwnerId:      ownerHandle,
			ID:           c.h.ID(),
			Version:      ver,
			Addresses:    addrs,
			StartTime:    time.Now(),
			RelayState:   warpnet.RelayStatusOff,
			Reachability: warpnet.ReachabilityPrivate,
			Network:      networkName,
		}, nil
	}
}

// wrapJSON adapts a typed handler to a routeHandler by decoding the request body
// into T first.
func wrapJSON[T any](h func(context.Context, T) (any, error)) routeHandler {
	return func(ctx context.Context, body []byte) (any, error) {
		var ev T
		if len(body) > 0 {
			if err := wjson.Unmarshal(body, &ev); err != nil {
				return nil, err
			}
		}
		return h(ctx, ev)
	}
}

// reactionsCount answers a react/unreact with the status's favourite tally,
// attributed to the emoji the reactor named (an unreact names none, so the
// breakdown is left off and only the total travels).
func reactionsCount(emoji string, count uint64) event.ReactionsCountResponse {
	resp := event.ReactionsCountResponse{Count: count}
	if emoji != "" && count > 0 {
		resp.Reactions = map[string]uint64{emoji: count}
	}
	return resp
}

func retweeterOf(ev warp.Tweet) string {
	if ev.RetweetedBy != nil && *ev.RetweetedBy != "" {
		return *ev.RetweetedBy
	}
	return ev.UserId
}

func retweetObject(ev warp.Tweet) string {
	if ev.Id != "" {
		return ev.Id
	}
	return ev.RootId
}
