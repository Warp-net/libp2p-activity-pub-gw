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
	"encoding/json"
	"html"
	"net/http"
	"net/url"
	"time"

	log "github.com/sirupsen/logrus"
)

const asPublic = "https://www.w3.org/ns/activitystreams#Public"

// buildNote renders a Warpnet tweet as an ActivityPub Note authored by
// localUser. The Note id is deterministic (.../statuses/{id}) so serveStatus
// can resolve it back to the tweet without local storage.
func (g *gateway) buildNote(localUser string, t tweet) note {
	actorID := g.actorID(localUser)
	followers := actorID + pathFollowers

	n := note{
		ID:           actorID + pathStatuses + t.Id,
		Type:         typeNote,
		AttributedTo: actorID,
		Content:      "<p>" + html.EscapeString(t.Text) + "</p>",
		Published:    t.CreatedAt.UTC().Format(time.RFC3339),
		Replies:      actorID + pathStatuses + t.Id + "/replies",
		To:           []string{asPublic},
		Cc:           []string{followers},
	}
	// Link a reply back to the status it answers so Mastodon threads it into the
	// conversation instead of showing a detached top-level post — without this a
	// Warpnet reply loses all thread context on the Fediverse. The parent shares
	// this author's status space for a self-thread; serveReplies overrides
	// InReplyTo with the exact parent URL for a cross-author reply.
	if parent := replyParentID(t); parent != "" {
		n.InReplyTo = actorID + pathStatuses + parent
	}
	for _, key := range t.ImageKeys {
		n.Attachment = append(n.Attachment, attachment{
			Type: typeDocument,
			URL:  g.baseURL() + pathMedia + encodeMediaRef(t.UserId, key),
		})
	}
	return n
}

// replyParentID returns the id of the status a tweet replies to — its immediate
// parent, or the thread root when the reply hangs directly off the root — or ""
// for an original top-level post. It mirrors domain.Tweet's threading fields
// (ParentId is the parent TWEET id, nil for top-level; RootId is the thread root).
func replyParentID(t tweet) string {
	if t.ParentId != nil && *t.ParentId != "" {
		return *t.ParentId
	}
	if t.RootId != "" && t.RootId != t.Id {
		return t.RootId
	}
	return ""
}

// buildCreateNote wraps a Warpnet tweet as an ActivityPub Create(Note) authored
// by localUser, addressed to the public and the author's followers.
func (g *gateway) buildCreateNote(localUser string, t tweet) activity {
	n := g.buildNote(localUser, t)
	return activity{
		Context: asContext,
		ID:      n.ID + "/activity",
		Type:    typeCreate,
		Actor:   n.AttributedTo,
		Object:  n,
		To:      n.To,
		Cc:      n.Cc,
	}
}

// serveStatus resolves one of our deterministic Note ids back to the Warpnet
// tweet (PUBLIC_GET_TWEET) and renders it as a standalone Note, so peers can
// dereference, reply to, and boost the gateway's posts. Needs a node.
func (g *gateway) serveStatus(w http.ResponseWriter, r *http.Request, user, tweetID string) {
	if g.req == nil {
		http.NotFound(w, r)
		return
	}
	req := getTweetEvent{TweetId: tweetID, UserId: user}
	// A reply carries its parent url on the id (see b.Reply); pass it so the node
	// can look the reply up in the parent's thread index instead of the timeline.
	if parent := r.URL.Query().Get(replyParentQuery); parent != "" {
		req.ParentId = parent
	}
	// Read it from the network that serves this user, never from the others: the
	// status url names its owner, and a blind fan-out would let another network
	// answer for them.
	bt, err := g.requestForUser(user, routeGetTweet, req)
	if err != nil {
		log.Warnf("status: fetch %s/%s: %v", user, tweetID, err)
		http.NotFound(w, r)
		return
	}
	var t tweet
	if jerr := json.Unmarshal(bt, &t); jerr != nil || t.Id == "" {
		http.NotFound(w, r)
		return
	}
	n := g.buildNote(user, t)
	n.Context = asContext
	// For a reply, make the served note match what b.Reply federated: its id
	// carries the parent (so it equals the url the remote instance dereferenced)
	// and InReplyTo is the real parent url, not buildNote's local status path.
	if parent := r.URL.Query().Get(replyParentQuery); parent != "" {
		n.ID = g.actorID(user) + pathStatuses + t.Id + "?" + url.Values{replyParentQuery: {parent}}.Encode()
		n.InReplyTo = parent
	}
	writeJSON(w, contentTypeAP, n)
}
