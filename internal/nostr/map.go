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

package nostr

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Warp-net/activity-pub-gw/internal/warp"
	"github.com/Warp-net/warpnet/domain"
)

// nostrHandleHost labels a Nostr account in its Warpnet id, npub1…@nostr.com.
// It is only a label: the gateway never contacts this host.
const nostrHandleHost = "nostr.com"

const (
	KindProfile       = 0
	KindNote          = 1
	kindFollowList    = 3
	kindLegacyDM      = 4
	kindDelete        = 5
	kindRepost        = 6
	kindReaction      = 7
	kindGenericRepost = 16
	kindPollResponse  = 1018
	kindGiftWrap      = 1059
	kindPoll          = 1068
	kindRelayList     = 10002
)

const (
	// nostrTextLimit matches the node's timeline byte limit.
	nostrTextLimit     = 2048
	pollMinOptions     = 2
	pollMaxOptions     = 4
	pollOptionRunes    = 25
	pollDefaultTTL     = 7 * 24 * time.Hour
	mostrProxyProtocol = "activitypub"
)

// Handle is the Warpnet id of a Nostr account.
func Handle(pkHex string) string {
	npub, ok := encodeNpub(pkHex)
	if !ok {
		return ""
	}
	return npub + "@" + nostrHandleHost
}

// nostrPubKey returns the hex key of an npub1…@nostr.com id. Any other domain
// is never Nostr, so alice@damus.io stays a Mastodon handle.
func nostrPubKey(id string) (string, bool) {
	name, host, ok := strings.Cut(strings.TrimPrefix(id, "@"), "@")
	if !ok || !strings.EqualFold(host, nostrHandleHost) || !strings.HasPrefix(name, "npub1") {
		return "", false
	}
	return decodeNpub(strings.ToLower(name))
}

func IsHandle(id string) bool {
	_, ok := nostrPubKey(id)
	return ok
}

var hex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)

// isNostrEventID reports whether a tweet id is a Nostr event id; bridged
// Fediverse ids are https urls and Warpnet's are ULIDs.
func isNostrEventID(id string) bool {
	return hex64.MatchString(id)
}

func tagValue(ev nostrEvent, name string) string {
	for _, t := range ev.Tags {
		if len(t) >= 2 && t[0] == name {
			return t[1]
		}
	}
	return ""
}

func tagValues(ev nostrEvent, name string) []string {
	var out []string
	for _, t := range ev.Tags {
		if len(t) >= 2 && t[0] == name && t[1] != "" {
			out = append(out, t[1])
		}
	}
	return out
}

// proxiedFrom returns the NIP-48 proxy tag's protocol and id, if any.
func proxiedFrom(ev nostrEvent) (id, protocol string) {
	for _, t := range ev.Tags {
		if len(t) >= 3 && t[0] == "proxy" {
			return t[1], t[2]
		}
	}
	return "", ""
}

// droppedNostrEvent filters what never enters Warpnet: direct messages (they
// are not supported), Fediverse posts relayed by Mostr (they arrive directly
// over ActivityPub already) and the gateway's own Warpnet posts coming back.
func droppedNostrEvent(ev nostrEvent, selfHost string) bool {
	if ev.Kind == kindLegacyDM || ev.Kind == kindGiftWrap {
		return true
	}
	id, protocol := proxiedFrom(ev)
	if protocol == mostrProxyProtocol {
		return true
	}
	if selfHost != "" && id != "" {
		if u, err := url.Parse(id); err == nil && strings.EqualFold(u.Hostname(), selfHost) {
			return true
		}
	}
	return false
}

type nostrProfile struct {
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
	About       string `json:"about"`
	Picture     string `json:"picture"`
	Banner      string `json:"banner"`
	Website     string `json:"website"`
}

// profileToUser maps a kind 0 event to a Warpnet user homed on the gateway.
func profileToUser(ev nostrEvent, nodeID string) (warp.User, bool) {
	handle := Handle(ev.PubKey)
	if ev.Kind != KindProfile || handle == "" {
		return warp.User{}, false
	}
	var p nostrProfile
	_ = json.Unmarshal([]byte(ev.Content), &p)
	u := warp.User{
		Id:                 handle,
		Username:           firstNonEmpty(p.DisplayName, p.Name, shortNpub(handle)),
		Bio:                p.About,
		NodeId:             nodeID,
		Network:            warp.NostrNetwork,
		AvatarKey:          httpsURL(p.Picture),
		BackgroundImageKey: httpsURL(p.Banner),
		CreatedAt:          time.Unix(ev.CreatedAt, 0),
	}
	if site := httpsURL(p.Website); site != "" {
		u.Website = &site
	}
	return u, true
}

func shortNpub(handle string) string {
	npub, _, _ := strings.Cut(handle, "@")
	if len(npub) > 16 {
		return npub[:12] + "…" + npub[len(npub)-4:]
	}
	return npub
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	return ""
}

func httpsURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if u, err := url.Parse(raw); err == nil && u.Scheme == "https" && u.Host != "" {
		return raw
	}
	return ""
}

// threadRefs reads a reply's root and parent per NIP-10: marked e tags first,
// then the deprecated positional form (first = root, last = parent).
func threadRefs(ev nostrEvent) (root, parent string) {
	var positional []string
	for _, t := range ev.Tags {
		if len(t) < 2 || t[0] != "e" || !isNostrEventID(t[1]) {
			continue
		}
		marker := ""
		if len(t) >= 4 {
			marker = t[3]
		}
		switch marker {
		case "root":
			root = t[1]
		case "reply":
			parent = t[1]
		case "mention":
		default:
			positional = append(positional, t[1])
		}
	}
	if root == "" && parent == "" && len(positional) > 0 {
		root, parent = positional[0], positional[len(positional)-1]
	}
	if parent == "" {
		parent = root
	}
	if root == "" {
		root = parent
	}
	return root, parent
}

var imageURLRe = regexp.MustCompile(`https://[^\s]+\.(?i:png|jpe?g|gif|webp|avif)(\?[^\s]*)?`)

// nostrNoteToTweet maps a kind 1 note (or a kind 1068 poll) to a tweet.
func nostrNoteToTweet(ev nostrEvent) (warp.Tweet, bool) {
	handle := Handle(ev.PubKey)
	if handle == "" || (ev.Kind != KindNote && ev.Kind != kindPoll) {
		return warp.Tweet{}, false
	}
	t := warp.Tweet{
		Id:        ev.ID,
		RootId:    ev.ID,
		UserId:    handle,
		Username:  handle,
		CreatedAt: time.Unix(ev.CreatedAt, 0),
		Network:   warp.NostrNetwork,
	}
	text := ev.Content
	for _, u := range imageURLRe.FindAllString(text, -1) {
		t.ImageKeys = append(t.ImageKeys, u)
		text = strings.Replace(text, u, "", 1)
	}
	if ev.Kind == kindPoll {
		text = pollInto(&t, ev, text)
	} else if root, parent := threadRefs(ev); parent != "" {
		p, pu := parent, parentAuthor(ev, parent)
		t.RootId, t.ParentId = root, &p
		if pu != "" {
			t.ParentUserId = &pu
		}
	}
	if q := tagValue(ev, "q"); isNostrEventID(q) {
		t.QuotedTweetId = &q
	}
	t.Text = truncateUTF8(strings.TrimSpace(text), nostrTextLimit)
	return t, t.Text != "" || len(t.ImageKeys) > 0
}

// parentAuthor finds the parent's author: the pubkey on the parent's e tag,
// else the last p tag.
func parentAuthor(ev nostrEvent, parent string) string {
	for _, t := range ev.Tags {
		if len(t) >= 5 && t[0] == "e" && t[1] == parent && hex64.MatchString(t[4]) {
			return Handle(t[4])
		}
	}
	ps := tagValues(ev, "p")
	if len(ps) > 0 && hex64.MatchString(ps[len(ps)-1]) {
		return Handle(ps[len(ps)-1])
	}
	return ""
}

// pollInto maps a NIP-88 poll onto a Warpnet poll, or appends its options to
// the text when Warpnet cannot hold them.
func pollInto(t *warp.Tweet, ev nostrEvent, text string) string {
	var opts []string
	valid := true
	for _, tag := range ev.Tags {
		if len(tag) >= 3 && tag[0] == "option" {
			label := strings.TrimSpace(tag[2])
			opts = append(opts, label)
			if label == "" || utf8.RuneCountInString(label) > pollOptionRunes {
				valid = false
			}
		}
	}
	if len(opts) == 0 {
		return text
	}
	if valid && len(opts) >= pollMinOptions && len(opts) <= pollMaxOptions {
		expires := time.Unix(ev.CreatedAt, 0).Add(pollDefaultTTL)
		if end, err := strconv.ParseInt(tagValue(ev, "endsAt"), 10, 64); err == nil && end > 0 {
			expires = time.Unix(end, 0)
		}
		t.Poll = &domain.Poll{Options: opts, ExpiresAt: expires}
		return text
	}
	for _, o := range opts {
		text += "\n• " + o
	}
	return text
}

// repostTarget returns the reposted event id and, when the repost embeds it,
// the reposted event itself.
func repostTarget(ev nostrEvent) (string, string) {
	if ev.Kind != kindRepost && ev.Kind != kindGenericRepost {
		return "", ""
	}
	id := tagValue(ev, "e")
	if !isNostrEventID(id) {
		return "", ""
	}
	return id, ev.Content
}

// reactionEmoji maps a kind 7 content to a Warpnet reaction; "+" is a like.
func reactionEmoji(content string) (string, bool) {
	switch content {
	case "", "+":
		return warp.DefaultReaction, true
	case "-":
		return "", false
	}
	emoji, err := warp.NormalizeReaction(content)
	return emoji, err == nil && !strings.Contains(emoji, ":")
}

// retweetPrefix marks a Warpnet retweet id wrapping the original's.
const retweetPrefix = domain.RetweetPrefix

func itoa(n int) string { return strconv.Itoa(n) }

func truncateUTF8(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	s = s[:limit]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}
