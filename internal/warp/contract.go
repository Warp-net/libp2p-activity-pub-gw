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

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/Warp-net/warpnet/event"
	log "github.com/sirupsen/logrus"
)

// NetworkScoped is implemented by a requester spanning several networks, which
// can confine a write to the one that serves a given user. A single-network
// requester does not need it — there is no wrong network to reach.
type NetworkScoped interface {
	RequestIn(localUser, route string, payload any) ([]byte, error)
}

// LogFieldNetwork tags a log line with the network it came from. The gateway
// runs a node per network in one process, so without it a failure cannot be
// attributed to one; logRing renders it and /logs?network= filters on it.
const LogFieldNetwork = "net"

// NetLog returns a logger tagged with network. An empty name yields an untagged
// logger, for the parts that serve every network alike (the ActivityPub surface,
// the Mastodon bridge).
func NetLog(network string) *log.Entry {
	if network == "" {
		return log.NewEntry(log.StandardLogger())
	}
	return log.WithField(LogFieldNetwork, network)
}

// networkNamer is implemented by a requester bound to a single network, so the
// components it drives can tag their logs with it without being handed the name.
type networkNamer interface {
	NetworkName() string
}

// NetworkOf names the network a requester is bound to, or "" when it spans
// several (or is a test fake).
func NetworkOf(req Requester) string {
	if n, ok := req.(networkNamer); ok {
		return n.NetworkName()
	}
	return ""
}

// Requester is the subset of nodeClient that the follower store needs.
// requestUser targets the node that owns userID (for routes that are
// authoritative only there, e.g. follows/followers); request broadcasts to any
// member node (for replicated routes and as the owner-resolution fallback).
type Requester interface {
	Request(route string, payload any) ([]byte, error)
	RequestUser(userID, route string, payload any) ([]byte, error)
}

// NodeResponseError reports a node handler failure: warpnet streams
// event.ResponseError as an ordinary response, so a nil transport error says
// nothing about success and an unchecked body decodes as an empty result.
func NodeResponseError(bt []byte) error {
	var possibleError struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(bt, &possibleError); err != nil {
		return nil // not an error envelope — let the caller decode it
	}
	if possibleError.Message == "" {
		return nil
	}
	return fmt.Errorf("node: %d %s", possibleError.Code, possibleError.Message)
}

// PublishableTweet reports whether a tweet should be federated outbound: an
// original top-level post authored by the owner. Retweets and replies are
// skipped for now (inReplyTo / Announce mapping is a later step).
func PublishableTweet(t Tweet, owner string) bool {
	if t.UserId != owner || t.RetweetedBy != nil {
		return false
	}
	return t.ParentId == nil || *t.ParentId == ""
}

// APFollowerPrefix tags follower ids that encode a remote ActivityPub actor, so
// AP followers can be told apart from native Warpnet user ids (which are valid
// base64url too). The ':' is safe as a key segment — the datastore delimiter is
// '/'.
const APFollowerPrefix = "ap:"

// IsBridgedUserID reports whether a Warpnet user id denotes a Fediverse account
// rather than a Warpnet member: either a gateway-minted "ap:" id (follow graph)
// or a "name@instance" handle (activity attribution, see bridgedUserID).
// Warpnet's own ids are ULIDs, so neither form collides with them.
func IsBridgedUserID(id string) bool {
	return strings.HasPrefix(id, APFollowerPrefix) || strings.Contains(id, "@")
}

// MastodonNetwork tags bridged users/tweets so Warpnet treats them as a foreign
// network; mirrors warpnet's own "mastodon" User.Network value on the wire.
const MastodonNetwork = "mastodon"

// ThreadsNetwork tags accounts and posts bridged in from Meta's Threads. They
// travel the same ActivityPub path as Mastodon ones, but Warpnet stores the
// network per account and the two servers behave differently enough to be worth
// telling apart: Threads serves no REST API and no browsable outbox, so its
// posts are read through a mirror (see mastodonBridge.mirrorTweets).
const ThreadsNetwork = "threads"

// IsBridgedNetwork reports whether a Warpnet User.Network tag names a network
// bridged in through this gateway rather than Warpnet itself.
func IsBridgedNetwork(network string) bool {
	return network == MastodonNetwork || network == ThreadsNetwork || network == NostrNetwork
}

// NostrNetwork tags users and posts bridged in from Nostr.
const NostrNetwork = "nostr"

// NativeUsers pages through the Warpnet users a network knows, skipping
// accounts bridged in from elsewhere.
func NativeUsers(req Requester, scanUser string) []User {
	limit := ScanPageSize
	var cursor string
	var out []User
	for range ScanMaxPages {
		ev := GetAllUsersEvent{UserId: scanUser, Limit: &limit}
		if cursor != "" {
			ev.Cursor = &cursor
		}
		bt, err := req.Request(RouteGetUsers, ev)
		if err != nil {
			return out
		}
		var resp UsersResponse
		if json.Unmarshal(bt, &resp) != nil {
			return out
		}
		for _, u := range resp.Users {
			if !IsBridgedNetwork(u.Network) && !IsBridgedUserID(u.Id) {
				out = append(out, u)
			}
		}
		if len(resp.Users) == 0 || resp.Cursor == "" || resp.Cursor == cursor {
			return out
		}
		cursor = resp.Cursor
	}
	return out
}

const DefaultNetwork = "warpnet"

func IsRateLimited(bt []byte) bool {
	var resp struct {
		Code int `json:"code"`
	}
	if json.Unmarshal(bt, &resp) != nil {
		return false
	}
	return resp.Code == event.RateLimitErrorCode
}

const (
	ScanPageSize = uint64(100)
	ScanMaxPages = 100
)

// FetchTweets reads the newest page of owner's tweets from their node; nil
// when it cannot be read.
func FetchTweets(req Requester, owner string) []Tweet {
	bt, err := req.RequestUser(owner, RouteGetTweets, GetAllTweetsEvent{UserId: owner})
	if err != nil {
		NetLog(NetworkOf(req)).Errorf("poller: get tweets for %s: %v", owner, err)
		return nil
	}
	var resp TweetsResponse
	if jerr := json.Unmarshal(bt, &resp); jerr != nil {
		NetLog(NetworkOf(req)).Errorf("poller: decode tweets: %v", jerr)
		return nil
	}
	return resp.Tweets
}

func EnvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// DefaultOwnerHandle is the Mastodon account the gateway advertises as its node
// owner (warpnet pins it as mastodon.EntryHandle, so it is not configurable) so
// Warpnet discovery seeds it as the entry point into the Fediverse. The gateway
// has no Warpnet user of its own.
const DefaultOwnerHandle = "warpnet@mastodon.social"

func RandomToken() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
