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
	"net/url"
	"path"
	"strings"
	"time"
)

const (
	keyType   = "type"
	keyObject = "object"
	keyActor  = "actor"
)

// translateInbound maps a verified inbound ActivityPub activity to the Warpnet
// route + event the gateway should send to the owner's node, reusing Warpnet's
// existing handlers. Remote actors travel as ap:-prefixed base64url ids (the
// follower scheme); the owner and tweet are recovered from our own URLs.
// Delete is not handled yet (it needs an AP-id -> Warpnet-id mapping).
//
// It also reports the local Warpnet user the activity is aimed at. With several
// networks joined that user picks the one network the write belongs in — see
// gateway.requestForUser.
func (g *gateway) translateInbound(raw map[string]any) (string, any, string, bool) {
	actor, _ := raw[keyActor].(string)
	if actor == "" {
		return "", nil, "", false
	}

	switch raw[keyType] {
	case typeLike:
		owner, tweetID, ok := g.parseLocalStatus(stringField(raw, keyObject))
		if !ok {
			return "", nil, "", false
		}
		// OwnerId is the reactor, UserId the reacted tweet's author (the
		// direction the node's reaction handler and the client both use).
		// Swapping them books the reaction as the author reacting to their own
		// tweet: no notification, and the node forwards it straight back to us.
		// A Mastodon favourite is the default heart — the only reaction the
		// Fediverse side can express.
		return routePostReact, reactionEvent{
			TweetId: tweetID, UserId: owner, OwnerId: bridgedUserID(actor),
			Emoji: defaultReaction,
		}, owner, true

	case typeAnnounce:
		owner, tweetID, ok := g.parseLocalStatus(stringField(raw, keyObject))
		if !ok {
			return "", nil, "", false
		}
		by := bridgedUserID(actor)
		return routePostRetweet, tweet{
			Id:          tweetID,
			RootId:      tweetID,
			UserId:      owner,
			RetweetedBy: &by,
			CreatedAt:   time.Now(),
		}, owner, true

	case typeCreate:
		obj, _ := raw[keyObject].(map[string]any)
		if obj == nil {
			return "", nil, "", false
		}
		text := htmlToText(stringField(obj, "content"))
		owner, parentID, ok := g.parseLocalStatus(stringField(obj, "inReplyTo"))
		if !ok {
			// Quote of one of our statuses — an explicit quote property, or
			// the Misskey-style trailing "RE: <url>" fallback after a comment
			// — maps to a Warpnet quote retweet.
			qURL := quotedNoteURL(obj)
			qu, comment, sok := splitREQuote(text)
			if qURL == "" && sok && comment != "" {
				qURL = qu
			}
			if qOwner, tweetID, qok := g.parseLocalStatus(qURL); qok {
				if stripped, ok := stripQuoteFallback(text); ok && stripped != "" {
					text = stripped
				}
				by := bridgedUserID(actor)
				return routePostRetweet, tweet{
					Id:            tweetID,
					CreatedAt:     time.Now(),
					UserId:        by,
					Username:      handleFromActorURL(actor),
					Text:          text,
					RetweetedBy:   &by,
					QuotedTweetId: &tweetID,
					QuotedUserId:  &qOwner,
				}, qOwner, true
			}
			// Quote-post convention: the text opens with
			// "RE: <our status URL>" — treat it as a reply to that status.
			parentURL, rest, reOK := splitREPrefix(text)
			if !reOK {
				return "", nil, "", false
			}
			if owner, parentID, ok = g.parseLocalStatus(parentURL); !ok {
				return "", nil, "", false
			}
			if rest != "" {
				text = rest
			}
		}
		pid, powner := parentID, owner
		// A reply is a tweet carrying a parent, sent on warpnet's public reply
		// route (the private tweet route it used to ride is now owner-only).
		// parent_user_id is what routes the reply to the parent author's node
		// and lets that node raise the reply notification.
		return routePostReply, tweet{
			CreatedAt:    time.Now(),
			Id:           ingestedNoteID(obj),
			ParentId:     &pid,
			ParentUserId: &powner,
			RootId:       parentID,
			Text:         text,
			UserId:       bridgedUserID(actor),
			Username:     handleFromActorURL(actor),
		}, powner, true

	case typeUndo:
		obj, _ := raw[keyObject].(map[string]any)
		if obj == nil {
			return "", nil, "", false
		}
		switch obj[keyType] {
		case typeFollow:
			owner := userFromActorURL(stringField(obj, keyObject))
			if owner == "" {
				return "", nil, "", false
			}
			return routePostUnfollow, newFollowEvent{
				FollowerId: bridgedUserID(actor), FollowingId: owner,
			}, owner, true
		case typeLike:
			owner, tweetID, ok := g.parseLocalStatus(stringField(obj, keyObject))
			if !ok {
				return "", nil, "", false
			}
			// Unreact drops whatever emoji the reactor had, so it carries none.
			return routePostUnreact, reactionEvent{
				TweetId: tweetID, UserId: owner, OwnerId: bridgedUserID(actor),
			}, owner, true
		case typeAnnounce:
			owner, tweetID, ok := g.parseLocalStatus(stringField(obj, keyObject))
			if !ok {
				return "", nil, "", false
			}
			// The event carries no local user (only the boosted tweet), but the
			// owner of the boosted status still picks the network to undo it in.
			return routePostUnretweet, unretweetEvent{
				TweetId: tweetID, RetweeterId: bridgedUserID(actor),
			}, owner, true
		}
	}
	return "", nil, "", false
}

// parseLocalStatus extracts the owner username and tweet id from one of our own
// status URLs (https://host/users/{user}/statuses/{id}).
func (g *gateway) parseLocalStatus(statusURL string) (owner, tweetID string, ok bool) {
	// A federated reply id carries "?parent=..." (see b.Reply); the encoded parent
	// contains no bare '/', so drop the query/fragment before splitting the path.
	statusURL, _, _ = strings.Cut(statusURL, "#")
	statusURL, _, _ = strings.Cut(statusURL, "?")
	rest, found := strings.CutPrefix(statusURL, g.baseURL()+pathUsers)
	if !found {
		return "", "", false
	}
	owner, after, found := strings.Cut(rest, pathStatuses)
	if !found || owner == "" || after == "" {
		return "", "", false
	}
	tweetID = after
	if i := strings.IndexByte(tweetID, '/'); i >= 0 {
		tweetID = tweetID[:i]
	}
	return owner, tweetID, true
}

// ingestedNoteID is the Warpnet id for a note we ingest: the note's own AP id,
// which keeps it resolvable afterwards. The node asks its author's home node —
// us — for that reply's stats, and a random token resolves to nothing there
// ("remote URL must be https" on every thread open); the note url reaches the
// remote status. It also makes a redelivery of the same note key to the same
// reply instead of adding a second row. Bridged tweets read off a profile are
// already keyed by their status url (see restBaseTweet).
func ingestedNoteID(note map[string]any) string {
	if id := stringField(note, "id"); id != "" {
		return id
	}
	if u := stringField(note, "url"); u != "" {
		return u
	}
	return randomToken()
}

// bridgedUserID is the Warpnet user id for a Fediverse actor whose activity we
// hand to a node: the "name@instance" handle, which is exactly the id the
// bridged profile resolves under, so a liker, booster or reply author opens as a
// profile instead of showing a raw "ap:<base64url>" id. The encoded actor url is
// the fallback for an actor url no handle can be derived from. (The follow graph
// keeps using encodeActorID: the gateway decodes those ids back to inboxes.)
func bridgedUserID(actorURL string) string {
	handle := handleFromActorURL(actorURL)
	if !strings.Contains(handle, "@") {
		return encodeActorID(actorURL)
	}
	return handle
}

// handleFromActorURL turns a remote actor URL (https://host/users/bob or
// https://host/@bob) into a readable "bob@host" handle, falling back to the raw
// URL when it can't be parsed.
func handleFromActorURL(actorURL string) string {
	u, err := url.Parse(actorURL)
	if err != nil || u.Host == "" {
		return actorURL
	}
	name := strings.TrimPrefix(path.Base(u.Path), "@")
	if name == "" || name == "." || name == "/" {
		return actorURL
	}
	return name + "@" + u.Host
}
