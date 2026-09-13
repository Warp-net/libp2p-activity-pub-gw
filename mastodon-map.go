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

// mastodon_map holds the pure conversion from loosely-typed ActivityPub
// documents to Warpnet's domain types. These functions have no I/O and no
// dependency on the gateway, so they are trivially unit-testable.

import (
	"context"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"

	stripper "github.com/grokify/html-strip-tags-go"
	log "github.com/sirupsen/logrus"
)

// mastodonNetwork tags bridged users/tweets so Warpnet treats them as a foreign
// network; mirrors warpnet's own "mastodon" User.Network value on the wire.
const mastodonNetwork = "mastodon"

// threadsNetwork tags accounts and posts bridged in from Meta's Threads. They
// travel the same ActivityPub path as Mastodon ones, but Warpnet stores the
// network per account and the two servers behave differently enough to be worth
// telling apart: Threads serves no REST API and no browsable outbox, so its
// posts are read through a mirror (see mastodonBridge.mirrorTweets).
const threadsNetwork = "threads"

// threadsHost is the one spelling Threads' own WebFinger answers for, and so
// the one every handle is canonicalized onto.
const threadsHost = "threads.net"

// isThreadsHost reports whether an instance host is Threads. It spells itself
// several ways — the handle domain is threads.net, actor urls carry the www.
// host, and the web urls moved to threads.com — so the tag is decided on the
// registrable name alone.
func isThreadsHost(host string) bool {
	host = strings.TrimPrefix(strings.ToLower(host), "www.")
	return host == threadsHost || host == "threads.com"
}

// opaqueLocalPart reports whether a handle's local part is an opaque id rather
// than a username. Threads serves some actors under a numeric id
// (…/ap/users/17841452547050663/) and then answers WebFinger for the username
// only, 404ing that id — so the handle has to come from the actor document
// instead of the url.
func opaqueLocalPart(handle string) bool {
	name, _, ok := strings.Cut(handle, "@")
	if !ok || name == "" {
		return false
	}
	for _, r := range name {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// canonicalHost is the spelling of an instance host a handle carries. Only the
// canonical one resolves: Threads answers WebFinger for name@threads.net and
// 404s the same account under any other spelling, so a handle built from an
// actor url's host would be unresolvable half the time.
func canonicalHost(host string) string {
	if isThreadsHost(host) {
		return threadsHost
	}
	return host
}

// networkOfHandle names the foreign network a bridged handle belongs to. It
// follows the account's own instance and never the server the posts were read
// from: a Threads account read through a Mastodon mirror is still on Threads.
func networkOfHandle(handle string) string {
	_, instance, ok := strings.Cut(strings.TrimPrefix(handle, "@"), "@")
	if ok && isThreadsHost(instance) {
		return threadsNetwork
	}
	return mastodonNetwork
}

// isBridgedNetwork reports whether a Warpnet User.Network tag names a network
// bridged in through this gateway rather than Warpnet itself.
func isBridgedNetwork(network string) bool {
	return network == mastodonNetwork || network == threadsNetwork
}

// restAccountToUser renders a Mastodon REST account as a Warpnet user — the
// mirror's view of an account whose own server would not answer (see
// mastodonBridge.mirrorUser). It carries the same fields actorToUser builds,
// plus the counts that cost three extra collection fetches over ActivityPub, and
// images the instance has already cached rather than links into the origin's CDN.
func restAccountToUser(handle string, acc map[string]any, nodeID string) user {
	name := asString(acc["display_name"])
	if name == "" {
		name = asString(acc["username"])
	}
	u := user{
		Id:                 handle,
		Username:           name,
		Bio:                stripper.StripTags(asString(acc["note"])),
		NodeId:             nodeID,
		Network:            networkOfHandle(handle),
		AvatarKey:          restImageURL(acc["avatar"]),
		BackgroundImageKey: restImageURL(acc["header"]),
		CreatedAt:          parseAPTime(asString(acc["created_at"])),
		FollowersCount:     int64(numField(acc["followers_count"])),
		FollowingsCount:    int64(numField(acc["following_count"])),
		TweetsCount:        int64(numField(acc["statuses_count"])),
	}
	if u.CreatedAt.IsZero() {
		u.CreatedAt = time.Now()
	}
	if site := asString(acc["url"]); site != "" {
		u.Website = &site
	}
	return u
}

// restImageURL drops the placeholder Mastodon serves in place of an absent
// avatar or header; passed on, it would paint a broken default over the
// client's own.
func restImageURL(v any) string {
	u := asString(v)
	if strings.Contains(u, "/missing.png") {
		return ""
	}
	return u
}

// actorToUser renders an ActivityPub actor document as a Warpnet user. handle is
// the WebFinger id (and the Warpnet user id); nodeID is the gateway peer that
// serves it.
func actorToUser(handle, actorURL string, m map[string]any, nodeID string) user {
	name, _ := m["name"].(string)
	if name == "" {
		name, _ = m["preferredUsername"].(string)
	}
	u := user{
		Id:                 handle,
		Username:           name,
		Bio:                stripper.StripTags(asString(m["summary"])),
		NodeId:             nodeID,
		Network:            networkOfHandle(handle),
		AvatarKey:          asImageURL(m["icon"]),
		BackgroundImageKey: asImageURL(m["image"]),
		CreatedAt:          parseAPTime(asString(m["published"])),
	}
	if u.CreatedAt.IsZero() {
		u.CreatedAt = time.Now()
	}
	if actorURL != "" {
		site := actorURL
		u.Website = &site
	}
	return u
}

// noteToTweet maps an ActivityPub Note to a Warpnet tweet. Non-Note objects are
// skipped (ok=false).
func noteToTweet(authorHandle string, note map[string]any) (tweet, bool) {
	if t, _ := note["type"].(string); t != typeNote {
		return tweet{}, false
	}
	id := asString(note["id"])
	if id == "" {
		return tweet{}, false
	}
	username := authorHandle
	if username == "" {
		username = handleFromActorURL(asString(note["attributedTo"]))
	}
	t := tweet{
		Id: id,
		// Warpnet convention: top-level tweets carry RootId = own id and no
		// ParentId; replies point both at the parent (AP gives only the
		// immediate inReplyTo, not the thread root).
		RootId:    id,
		Text:      htmlToText(asString(note["content"])),
		UserId:    username,
		Username:  username,
		CreatedAt: parseAPTime(asString(note["published"])),
		Network:   networkOfHandle(username),
	}
	if parent := asString(note["inReplyTo"]); parent != "" {
		t.RootId = parent
		t.ParentId = &parent
	}
	if q := quotedNoteURL(note); q != "" {
		setQuoted(&t, q)
		if comment, ok := stripQuoteFallback(t.Text); ok && comment != "" {
			t.Text = comment // drop the "RE: <url>" text fallback; the embedded preview shows the source
		}
	} else if q, comment, ok := splitREQuote(t.Text); ok && comment != "" {
		setQuoted(&t, q)
		t.Text = comment
	} else if t.ParentId == nil {
		if parent, rest, ok := splitREPrefix(t.Text); ok {
			t.RootId = parent
			t.ParentId = &parent
			if rest != "" {
				t.Text = rest
			}
		}
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	for _, a := range asSlice(note["attachment"]) {
		att := asMap(a)
		if att == nil {
			continue
		}
		if mt, _ := att["mediaType"].(string); !strings.HasPrefix(mt, "image/") {
			continue
		}
		if u := asString(att["url"]); u != "" {
			t.ImageKeys = append(t.ImageKeys, u)
		}
	}
	return t, true
}

var lineBreakTags = regexp.MustCompile(`(?i)<br\s*/?>|</p>`)

// htmlToText flattens a note's HTML content to plain text, keeping <br> and
// paragraph breaks as newlines so line-oriented conventions (the "RE:" quote
// fallback) survive tag stripping instead of gluing onto the comment.
func htmlToText(html string) string {
	return strings.TrimSpace(stripper.StripTags(lineBreakTags.ReplaceAllString(html, "\n")))
}

// quotedNoteURL reads the quote properties Fediverse servers attach to a quote
// post (Misskey-family quoteUri/quoteUrl/_misskey_quote, Mastodon/FEP quote),
// tolerating the string, {"id"} and {"href"} forms.
func quotedNoteURL(note map[string]any) string {
	for _, k := range []string{"quoteUri", "quoteUrl", "_misskey_quote", "quote"} {
		u := asString(note[k])
		if u == "" {
			u = asString(asMap(note[k])["href"])
		}
		if strings.HasPrefix(u, "https://") {
			return u
		}
	}
	return ""
}

// setQuoted marks t as a quote of the note at qURL, deriving quoted_user_id
// from the URL when it carries the author (the client routes its quoted-source
// fetch by that id); URLs without an author are resolved later by the bridge.
func setQuoted(t *tweet, qURL string) {
	t.QuotedTweetId = &qURL
	if h := statusAuthorHandle(qURL); h != "" {
		t.QuotedUserId = &h
	}
}

// statusAuthorHandle derives the author's "name@host" handle from a status URL
// that embeds the author (/users/{name}/statuses/{id} or /@{name}/{id}); ""
// when it doesn't (e.g. Misskey's /notes/{id}).
func statusAuthorHandle(statusURL string) string {
	u, err := url.Parse(statusURL)
	if err != nil || u.Host == "" {
		return ""
	}
	segs := strings.Split(strings.Trim(u.Path, "/"), "/")
	switch {
	case len(segs) >= 3 && segs[0] == "users" && segs[1] != "" && segs[2] == "statuses":
		return segs[1] + "@" + u.Host
	case len(segs) >= 2 && len(segs[0]) > 1 && segs[0][0] == '@':
		return segs[0][1:] + "@" + u.Host
	}
	return ""
}

// splitREQuote parses the Misskey-style quote fallback — text closing with an
// "RE: <status URL>" line appended after the comment (comment is "" when the
// fallback is the whole text). The URL must end the note; a mid-word "RE:"
// (MORE:, SCORE:) does not match.
func splitREQuote(text string) (quotedURL, comment string, ok bool) {
	trimmed := strings.TrimSpace(text)
	i := strings.LastIndex(trimmed, "RE: https://")
	if i < 0 || (i > 0 && !unicode.IsSpace(rune(trimmed[i-1]))) {
		return "", "", false
	}
	quotedURL = trimmed[i+len("RE: "):]
	if strings.IndexFunc(quotedURL, unicode.IsSpace) >= 0 {
		return "", "", false
	}
	return quotedURL, strings.TrimSpace(trimmed[:i]), true
}

// stripQuoteFallback removes the "RE: <status URL>" text fallback a quoting
// server adds to a quote post's content — trailing (Misskey) or leading
// (Mastodon) — returning the remaining comment.
func stripQuoteFallback(text string) (string, bool) {
	if _, comment, ok := splitREQuote(text); ok {
		return comment, true
	}
	if _, rest, ok := splitREPrefix(text); ok {
		return rest, true
	}
	return text, false
}

// splitREPrefix parses the Fediverse quote-post convention — a note whose text
// starts with "RE: <status URL>" instead of carrying inReplyTo — returning the
// quoted status URL and the remaining text. Only https URLs qualify (matching
// the gateway's fetch guard).
func splitREPrefix(text string) (parentURL, rest string, ok bool) {
	trimmed := strings.TrimSpace(text)
	if len(trimmed) < 3 || !strings.EqualFold(trimmed[:3], "re:") {
		return "", "", false
	}
	parentURL = strings.TrimSpace(trimmed[3:])
	if i := strings.IndexFunc(parentURL, unicode.IsSpace); i >= 0 {
		parentURL, rest = parentURL[:i], strings.TrimSpace(parentURL[i:])
	}
	if !strings.HasPrefix(parentURL, "https://") {
		return "", "", false
	}
	return parentURL, rest, true
}

// collectHandles maps a collection page's actor-URL items to Fediverse handles.
// An item whose url carries an opaque id is named through its actor document:
// a handle built from such a url resolves to nothing, so the row it produces
// fails to load and silently disappears from the list. Threads serves its actors
// that way, and so does Mastodon for accounts created after the switch — three
// of the first six entries of a real following collection.
//
// Only the opaque ones cost a fetch (canonicalHandle), and they run concurrently:
// a page holds a dozen items and one Threads actor alone answers in seconds.
func (b *mastodonBridge) collectHandles(ctx context.Context, page map[string]any) []string {
	items := asSlice(page["orderedItems"])
	if len(items) == 0 {
		items = asSlice(page["items"])
	}
	named := make([]string, len(items))
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for i, it := range items {
		u := asString(it)
		if u == "" {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			named[i] = b.ap.canonicalHandle(ctx, u)
		}()
	}
	wg.Wait()
	out := make([]string, 0, len(named))
	for _, h := range named {
		if h != "" {
			out = append(out, h)
		}
	}
	return out
}

// apCollectionCount reads totalItems off an AP Collection value.
func apCollectionCount(v any) uint64 {
	m := asMap(v)
	if m == nil {
		return 0
	}
	if n, ok := m["totalItems"].(float64); ok && n > 0 {
		return uint64(n)
	}
	return 0
}

// asImageURL extracts the url of an AP image (icon/image), tolerating the bare
// string, object, and array forms.
func asImageURL(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case map[string]any:
		return asString(x["url"])
	case []any:
		if len(x) > 0 {
			return asImageURL(x[0])
		}
	}
	return ""
}

func parseAPTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		log.Debugf("mastodon: bad published time %q: %v", s, err)
		return time.Time{}
	}
	return t
}

// asString reads a string from a loosely-typed AP value, tolerating the
// {"id": "..."} object form.
func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case map[string]any:
		id, _ := x["id"].(string)
		return id
	}
	return ""
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}
