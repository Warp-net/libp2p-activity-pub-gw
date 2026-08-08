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

// mastodonBridge converts between Warpnet and the Fediverse over ActivityPub:
// reads resolve a WebFinger handle to a remote actor and render it into Warpnet
// domain types; writes federate a Warpnet action (like, follow, reply, boost)
// as a signed activity delivered to the target author's inbox. It depends only
// on apTransport, so the conversion logic is isolated from the HTTP/libp2p
// machinery on the gateway.

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/Warp-net/warpnet/domain"
	"github.com/Warp-net/warpnet/event"
)

// apTransport is the ActivityPub HTTP surface the bridge needs; *gateway
// implements it (reusing its SSRF-hardened client and signing keys).
type apTransport interface {
	apGetJSON(ctx context.Context, rawURL, accept string) (map[string]any, error)
	resolveActorID(ctx context.Context, id string) (string, error)
	apGetArray(ctx context.Context, rawURL, accept string) ([]any, error)
	fetchActor(ctx context.Context, actorURL string) (map[string]any, error)
	remoteInbox(ctx context.Context, actorURL string) (string, error)
	actorID(user string) string
	postSigned(ctx context.Context, localUser, target string, doc any) error
	deliverFollow(localUser, remoteActorURL string, undo bool)
	fetchMedia(ctx context.Context, rawURL string) (mimeType string, data []byte, err error)
	isSelfHost(host string) bool
}

type mastodonBridge struct {
	ap     apTransport
	nodeID string // gateway peer id stamped onto bridged users
}

func newMastodonBridge(ap apTransport, nodeID string) *mastodonBridge {
	return &mastodonBridge{ap: ap, nodeID: nodeID}
}

// resolveHandle resolves "name@instance" to its actor URL via WebFinger. An
// "ap:" id (a remote actor Warpnet learned about through the follow graph)
// already carries its actor URL, so it is decoded instead of WebFingered.
// Handles hosted by the gateway itself are refused: they are Warpnet users the
// network already serves natively, and resolving them here would present a local
// user as a foreign Mastodon account (a request looping back on itself).
func (b *mastodonBridge) resolveHandle(ctx context.Context, handle string) (string, error) {
	return b.ap.resolveActorID(ctx, handle)
}

// resolveActorID is the single place a bridged user id becomes an actor url. It
// takes the "name@instance" handle every bridged id now uses, and still decodes
// the legacy "ap:" form so follow graphs recorded before the switch keep
// resolving. Handles hosted by the gateway itself are refused: they are Warpnet
// users the network already serves natively, and resolving them here would
// present a local user as a foreign Mastodon account (a request looping back on
// itself).
func (g *gateway) resolveActorID(ctx context.Context, handle string) (string, error) {
	if actorURL, derr := decodeActorID(handle); derr == nil {
		if u, perr := url.Parse(actorURL); perr == nil && g.isSelfHost(u.Hostname()) {
			return "", fmt.Errorf("mastodon: actor %s is served by this gateway: %w", actorURL, errSelfTarget)
		}
		return actorURL, nil
	}
	name, instance, ok := strings.Cut(strings.TrimPrefix(handle, "@"), "@")
	if !ok || name == "" || instance == "" {
		return "", fmt.Errorf("mastodon: %q is not a name@instance handle", handle)
	}
	if g.isSelfHost(instance) {
		return "", fmt.Errorf("mastodon: %q is a Warpnet user served by this gateway: %w", handle, errSelfTarget)
	}
	if g.actorIDs != nil {
		if cached, ok := g.actorIDs.Get(handle); ok {
			return cached, nil
		}
	}
	wf := "https://" + instance + "/.well-known/webfinger?resource=acct:" + name + "@" + instance
	doc, err := g.apGetJSON(ctx, wf, contentTypeJRD)
	if err != nil {
		return "", fmt.Errorf("mastodon: webfinger %s: %w", handle, err)
	}
	for _, l := range asSlice(doc["links"]) {
		link := asMap(l)
		if link == nil || asString(link["rel"]) != "self" {
			continue
		}
		if href, _ := link["href"].(string); href != "" {
			if g.actorIDs != nil {
				g.actorIDs.Add(handle, href)
			}
			return href, nil
		}
	}
	return "", fmt.Errorf("mastodon: webfinger %s: no self link", handle)
}

// --- reads (Mastodon -> Warpnet) ---

// GetUser resolves a handle to a full profile including follower/following/
// tweet counts (3 extra collection fetches). Use it for a direct profile view.
func (b *mastodonBridge) GetUser(ctx context.Context, handle string) (user, error) {
	return b.getUser(ctx, handle, true)
}

// GetUserBrief resolves a handle without the count fetches — for list contexts
// (who-to-follow, search) where counts aren't shown, avoiding 3 fetches per row.
func (b *mastodonBridge) GetUserBrief(ctx context.Context, handle string) (user, error) {
	return b.getUser(ctx, handle, false)
}

func (b *mastodonBridge) getUser(ctx context.Context, handle string, withCounts bool) (user, error) {
	actorURL, err := b.resolveHandle(ctx, handle)
	if err != nil {
		return user{}, err
	}
	m, err := b.ap.fetchActor(ctx, actorURL)
	if err != nil {
		return user{}, err
	}
	u := actorToUser(handle, actorURL, m, b.nodeID)
	if !withCounts {
		return u, nil
	}
	// The three collection fetches run in parallel so one slow endpoint does
	// not eat the whole nodeserver request budget.
	counts := make([]int64, 3)
	var wg sync.WaitGroup
	for i, coll := range []string{asString(m["followers"]), asString(m["following"]), asString(m["outbox"])} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			counts[i] = b.collectionCount(ctx, coll)
		}()
	}
	wg.Wait()
	u.FollowersCount, u.FollowingsCount, u.TweetsCount = counts[0], counts[1], counts[2]
	return u, nil
}

// collectionCount reads totalItems off an AP collection URL (followers,
// following, outbox); 0 on any miss.
func (b *mastodonBridge) collectionCount(ctx context.Context, collURL string) int64 {
	if collURL == "" {
		return 0
	}
	m, err := b.ap.apGetJSON(ctx, collURL, contentTypeAP)
	if err != nil {
		return 0
	}
	return int64(apCollectionCount(m)) //nolint:gosec
}

// pageCursor returns the cursor as a Fediverse page URL, or "" when it is not
// one. Warpnet paginates the gateway's read routes with the requesting node's
// own datastore cursor (e.g. "/TWEETS/<user>/<seq>/<noteURL>"), not the AP
// "next" URL the gateway returned, so a non-https cursor is meaningless to the
// Fediverse: the gateway restarts from the first page rather than dereference
// it (which would otherwise fail the https SSRF guard).
func pageCursor(cursor *string) string {
	if cursor == nil || !strings.HasPrefix(*cursor, "https://") {
		return ""
	}
	return *cursor
}

// GetTweetsOrReplies serves the PUBLIC_GET_TWEETS route. Warpnet folded thread
// replies into it: a plain profile request carries only a userId (handle),
// while a thread-replies request carries root_id/parent_id — the note whose
// replies are wanted.
func (b *mastodonBridge) GetTweetsOrReplies(ctx context.Context, ev getAllTweetsEvent) (tweetsResponse, error) {
	if ev.RootId != "" || ev.ParentId != "" {
		id := ev.ParentId
		if id == "" {
			id = ev.RootId
		}
		return b.GetReplies(ctx, id)
	}
	return b.GetTweets(ctx, ev.UserId, ev.Cursor)
}

// GetTweets renders a remote actor's timeline as Warpnet tweets. Mastodon-family
// instances serve a full status page (counts, boosts, media inline) from one
// REST call, avoiding the per-item dereferences the AP outbox needs; others fall
// back to the outbox. cursor, when set, continues whichever source produced it.
func (b *mastodonBridge) GetTweets(ctx context.Context, handle string, cursor *string) (tweetsResponse, error) {
	if pc := pageCursor(cursor); pc != "" {
		if isRESTStatusesURL(pc) {
			resp, _ := b.restTweetsPage(ctx, handle, pc)
			return resp, nil
		}
		return b.apTweets(ctx, handle, cursor)
	}
	if resp, ok := b.restTweets(ctx, handle); ok {
		return resp, nil
	}
	return b.apTweets(ctx, handle, cursor)
}

// isRESTStatusesURL reports whether a pagination cursor points at the Mastodon
// REST account-statuses endpoint (so pagination continues on the REST path).
func isRESTStatusesURL(u string) bool {
	return strings.Contains(u, "/api/v1/accounts/") && strings.Contains(u, "/statuses")
}

// restTweets loads a handle's first status page over the Mastodon REST API,
// resolving the account id via /accounts/lookup. ok is false for non-Mastodon
// instances, so the caller falls back to the AP outbox.
func (b *mastodonBridge) restTweets(ctx context.Context, handle string) (tweetsResponse, bool) {
	name, instance, ok := strings.Cut(strings.TrimPrefix(handle, "@"), "@")
	if !ok || name == "" || instance == "" {
		return tweetsResponse{}, false
	}
	look, err := b.ap.apGetJSON(ctx, "https://"+instance+"/api/v1/accounts/lookup?acct="+url.QueryEscape(name+"@"+instance), "application/json")
	if err != nil {
		return tweetsResponse{}, false
	}
	accID := asString(look["id"])
	if accID == "" {
		return tweetsResponse{}, false
	}
	// exclude_replies mirrors warpnet's own profile timeline, which is served
	// from the author's timeline keyspace and never contains replies; the Posts
	// tab must show only top-level posts (thread replies come from GetReplies).
	return b.restTweetsPage(ctx, handle, "https://"+instance+"/api/v1/accounts/"+accID+"/statuses?limit=40&exclude_replies=true")
}

// restTweetsPage fetches one REST status page and maps it, deriving the next
// cursor from the last status id (Mastodon paginates by max_id).
func (b *mastodonBridge) restTweetsPage(ctx context.Context, handle, pageURL string) (tweetsResponse, bool) {
	arr, err := b.ap.apGetArray(ctx, pageURL, "application/json")
	if err != nil {
		return tweetsResponse{UserId: handle}, false
	}
	host := ""
	if u, perr := url.Parse(pageURL); perr == nil {
		host = u.Host
	}
	resp := tweetsResponse{UserId: handle}
	lastID := ""
	for _, it := range arr {
		s := asMap(it)
		if s == nil {
			continue
		}
		if sid := asString(s["id"]); sid != "" {
			lastID = sid
		}
		// exclude_replies leaves self-replies (thread continuations) in the
		// page; a Warpnet profile carries only top-level posts, so a reply
		// must never surface as a standalone tweet — it stays reachable
		// through its parent's thread.
		if asString(s["in_reply_to_id"]) != "" {
			continue
		}
		if t, ok := restStatusToTweet(host, s); ok {
			resp.Tweets = append(resp.Tweets, t)
		}
	}
	if lastID != "" && len(arr) > 0 {
		if u, perr := url.Parse(pageURL); perr == nil {
			q := u.Query()
			q.Set("max_id", lastID)
			u.RawQuery = q.Encode()
			resp.Cursor = u.String()
		}
	}
	return resp, true
}

// apTweets renders a remote actor's outbox as Warpnet tweets (fallback). cursor,
// when set, is the next OrderedCollectionPage URL.
func (b *mastodonBridge) apTweets(ctx context.Context, handle string, cursor *string) (tweetsResponse, error) {
	pageURL := pageCursor(cursor)
	if pageURL == "" {
		actorURL, err := b.resolveHandle(ctx, handle)
		if err != nil {
			return tweetsResponse{}, err
		}
		actor, err := b.ap.fetchActor(ctx, actorURL)
		if err != nil {
			return tweetsResponse{}, err
		}
		outbox := asString(actor["outbox"])
		if outbox == "" {
			return tweetsResponse{UserId: handle}, nil
		}
		ob, err := b.ap.apGetJSON(ctx, outbox, contentTypeAP)
		if err != nil {
			return tweetsResponse{}, err
		}
		if pageURL = asString(ob["first"]); pageURL == "" {
			return tweetsResponse{UserId: handle}, nil
		}
	}

	page, err := b.ap.apGetJSON(ctx, pageURL, contentTypeAP)
	if err != nil {
		return tweetsResponse{}, err
	}
	resp := tweetsResponse{UserId: handle, Cursor: asString(page["next"])}
	resp.Tweets = topLevelOnly(b.resolveTimelineItems(ctx, handle, asSlice(page["orderedItems"])))
	return resp, nil
}

// topLevelOnly drops replies from a profile timeline: the AP outbox lists them
// alongside posts, but a Warpnet profile carries only top-level tweets.
func topLevelOnly(ts []tweet) []tweet {
	out := ts[:0]
	for _, t := range ts {
		if t.ParentId == nil || *t.ParentId == "" {
			out = append(out, t)
		}
	}
	return out
}

// resolveTimelineItems renders one outbox page's items into tweets concurrently,
// preserving order. Each item may trigger its own fetches (a boost dereferences
// the boosted note, a quote resolves its author), so rendering them serially
// would serialize a round-trip per item; a bounded pool keeps the fan-out gentle.
func (b *mastodonBridge) resolveTimelineItems(ctx context.Context, handle string, items []any) []tweet {
	const maxConcurrent = 8
	out := make([]tweet, len(items))
	ok := make([]bool, len(items))
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	for i, it := range items {
		obj := asMap(it)
		if obj == nil {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if t, good := b.activityToTweet(ctx, handle, obj); good {
				out[i], ok[i] = t, true
			}
		}()
	}
	wg.Wait()
	tweets := make([]tweet, 0, len(items))
	for i := range items {
		if ok[i] {
			tweets = append(tweets, out[i])
		}
	}
	return tweets
}

// activityToTweet turns one outbox item (Create wrapping a Note, a bare Note, or
// an Announce boost) into a tweet.
func (b *mastodonBridge) activityToTweet(ctx context.Context, handle string, obj map[string]any) (tweet, bool) {
	if asString(obj["type"]) == typeAnnounce {
		boosted := asString(obj["object"])
		if boosted == "" {
			return tweet{}, false
		}
		bm, err := b.ap.apGetJSON(ctx, boosted, contentTypeAP)
		if err != nil {
			return tweet{}, false
		}
		t, ok := noteToTweet(handleFromActorURL(asString(bm["attributedTo"])), bm)
		if ok {
			by := handle
			t.RetweetedBy = &by
			b.fillQuotedAuthor(ctx, &t)
		}
		return t, ok
	}
	note := obj
	if inner := asMap(obj["object"]); inner != nil {
		note = inner
	}
	t, ok := noteToTweet(handle, note)
	if ok {
		b.fillQuotedAuthor(ctx, &t)
	}
	return t, ok
}

// fillQuotedAuthor resolves quoted_user_id for a quote whose quoted status URL
// does not embed the author (e.g. Misskey's /notes/{id}) — the client routes
// its quoted-source fetch by that id. Best-effort, one dereference.
func (b *mastodonBridge) fillQuotedAuthor(ctx context.Context, t *tweet) {
	if t.QuotedTweetId == nil || t.QuotedUserId != nil {
		return
	}
	m, err := b.ap.apGetJSON(ctx, *t.QuotedTweetId, contentTypeAP)
	if err != nil {
		return
	}
	if inner := asMap(m["object"]); inner != nil {
		m = inner
	}
	if author := asString(m["attributedTo"]); author != "" {
		h := handleFromActorURL(author)
		t.QuotedUserId = &h
	}
}

// GetTweet fetches a single status by its id. Mastodon-family instances serve
// the full status (counts, boost, media inline) from one REST call; others fall
// back to dereferencing the AP Note.
func (b *mastodonBridge) GetTweet(ctx context.Context, noteURL string) (tweet, error) {
	noteURL = strings.TrimPrefix(noteURL, domain.RetweetPrefix)
	if host, id, ok := restStatusRef(noteURL); ok {
		if m, err := b.ap.apGetJSON(ctx, "https://"+host+"/api/v1/statuses/"+id, "application/json"); err == nil {
			if t, ok := restStatusToTweet(host, m); ok {
				return t, nil
			}
		}
	}
	return b.apTweet(ctx, noteURL)
}

// apTweet dereferences a single AP Note (fallback for non-Mastodon instances).
func (b *mastodonBridge) apTweet(ctx context.Context, noteURL string) (tweet, error) {
	m, err := b.ap.apGetJSON(ctx, noteURL, contentTypeAP)
	if err != nil {
		return tweet{}, err
	}
	if inner := asMap(m["object"]); inner != nil {
		m = inner
	}
	t, ok := noteToTweet(handleFromActorURL(asString(m["attributedTo"])), m)
	if ok {
		b.fillQuotedAuthor(ctx, &t)
	}
	return t, nil
}

// GetReplies returns a Note's replies as the flat tweet list warpnet's folded
// reply route (PUBLIC_GET_TWEETS with root_id/parent_id) answers with — the
// reply tree it used to return is gone from the wire contract. Mastodon-family
// instances expose the whole thread via one REST call
// (/api/v1/statuses/{id}/context), so try that first — it is a single
// unauthenticated request that returns every descendant as a full status,
// instead of dereferencing each reply URI from the slow, often-partial
// ActivityPub replies collection. Non-Mastodon servers (no /context) fall back
// to the AP walk.
func (b *mastodonBridge) GetReplies(ctx context.Context, noteURL string) (tweetsResponse, error) {
	noteURL = strings.TrimPrefix(noteURL, domain.RetweetPrefix)
	if resp, ok := b.contextReplies(ctx, noteURL); ok {
		return resp, nil
	}
	resp, err := b.apReplies(ctx, noteURL)
	for i := range resp.Tweets {
		b.nativizeOwnStatus(&resp.Tweets[i], nil)
	}
	return resp, err
}

// maxReplies bounds how many replies GetReplies returns from either path.
const maxReplies = 50

// contextReplies fetches the whole thread via the Mastodon REST context endpoint
// and flattens its descendants into replies. ok is false when the instance is
// not Mastodon-compatible (no /context), so the caller falls back to AP.
func (b *mastodonBridge) contextReplies(ctx context.Context, noteURL string) (tweetsResponse, bool) {
	host, id, ok := restStatusRef(noteURL)
	if !ok {
		return tweetsResponse{}, false
	}
	ctxURL := "https://" + host + "/api/v1/statuses/" + id + "/context"
	m, err := b.ap.apGetJSON(ctx, ctxURL, "application/json")
	if err != nil {
		return tweetsResponse{}, false // not Mastodon / not reachable -> AP fallback
	}
	desc, hasDesc := m["descendants"]
	if _, hasAnc := m["ancestors"]; !hasDesc && !hasAnc {
		return tweetsResponse{}, false // not a context document
	}
	items := asSlice(desc)
	idToURI := map[string]string{id: noteURL}
	resp := tweetsResponse{Tweets: []tweet{}}
	for _, it := range items {
		if len(resp.Tweets) >= maxReplies {
			break
		}
		s := asMap(it)
		// The context lists the note's whole subtree flattened; only direct
		// children are this note's replies — deeper levels are served when
		// the client walks the thread one parent at a time.
		if s == nil || asString(s["in_reply_to_id"]) != id {
			continue
		}
		if t, ok := restReplyToTweet(host, s, noteURL, idToURI); ok {
			b.nativizeOwnStatus(&t, asMap(s["account"]))
			resp.Tweets = append(resp.Tweets, t)
		}
	}
	return resp, true
}

// nativizeOwnStatus rewrites a status hosted by this gateway — a federated
// Warpnet reply — back into its native Warpnet shape: bare tweet/user ids and
// no foreign network tag, so the asking node lines it up with (and dedupes
// against) the copy in its own thread index. account is the REST account of
// the status for a display name; nil is fine.
func (b *mastodonBridge) nativizeOwnStatus(t *tweet, account map[string]any) {
	if t.ParentId != nil {
		if _, pid, ok := b.ownStatusRef(*t.ParentId); ok {
			t.ParentId = &pid
		}
	}
	owner, id, ok := b.ownStatusRef(t.Id)
	if !ok {
		return
	}
	t.Id = id
	t.UserId = owner
	t.Network = ""
	t.Username = owner
	if name := asString(account["display_name"]); name != "" {
		t.Username = name
	}
}

// ownStatusRef splits a status URL hosted by this gateway
// (https://<self>/users/{user}/statuses/{id}[?parent=...]) into its Warpnet
// owner and tweet ids; ok is false for foreign URLs.
func (b *mastodonBridge) ownStatusRef(rawURL string) (owner, tweetID string, ok bool) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" || !b.ap.isSelfHost(u.Hostname()) {
		return "", "", false
	}
	rest, found := strings.CutPrefix(u.Path, pathUsers)
	if !found {
		return "", "", false
	}
	owner, tweetID, found = strings.Cut(rest, pathStatuses)
	if !found || owner == "" || tweetID == "" {
		return "", "", false
	}
	if i := strings.IndexByte(tweetID, '/'); i >= 0 {
		tweetID = tweetID[:i]
	}
	return owner, tweetID, true
}

// restStatusRef splits an AP note/status URL into the instance host and the
// status id (its last path segment) for building Mastodon REST URLs. ok is false
// for URLs without a host or id.
func restStatusRef(noteURL string) (host, id string, ok bool) {
	u, err := url.Parse(noteURL)
	if err != nil || u.Host == "" {
		return "", "", false
	}
	id = path.Base(strings.TrimRight(u.Path, "/"))
	if id == "" || id == "." || id == "/" {
		return "", "", false
	}
	return u.Host, id, true
}

// acctHandle completes a REST account's bare acct (local accounts) into a
// name@host handle; remote accounts already carry the @host.
func acctHandle(host, acct string) string {
	if acct == "" || strings.Contains(acct, "@") {
		return acct
	}
	return acct + "@" + host
}

// restBaseTweet maps the fields common to every Mastodon REST status (distinct
// from an AP Note) into a tweet, defaulting to the top-level convention
// (RootId = own id, no ParentId). host completes a local account's handle.
func restBaseTweet(host string, s map[string]any) (tweet, bool) {
	if s == nil {
		return tweet{}, false
	}
	uri := asString(s["uri"])
	if uri == "" {
		uri = asString(s["url"])
	}
	if uri == "" {
		return tweet{}, false
	}
	handle := acctHandle(host, asString(asMap(s["account"])["acct"]))
	t := tweet{
		Id:        uri,
		RootId:    uri,
		Text:      htmlToText(asString(s["content"])),
		UserId:    handle,
		Username:  handle,
		CreatedAt: parseAPTime(asString(s["created_at"])),
		Network:   mastodonNetwork,
	}
	for _, a := range asSlice(s["media_attachments"]) {
		att := asMap(a)
		if att == nil || asString(att["type"]) != "image" {
			continue
		}
		if mu := asString(att["url"]); mu != "" {
			t.ImageKeys = append(t.ImageKeys, mu)
		}
	}
	if q := asMap(s["quote"]); q != nil {
		if qu := asString(q["uri"]); qu != "" {
			t.QuotedTweetId = &qu
			if qh := acctHandle(host, asString(asMap(q["account"])["acct"])); qh != "" {
				t.QuotedUserId = &qh
			}
		}
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	return t, true
}

// restStatusToTweet maps a standalone REST status (timeline / single tweet),
// unwrapping a boost into the boosted status stamped with RetweetedBy.
func restStatusToTweet(host string, s map[string]any) (tweet, bool) {
	if s == nil {
		return tweet{}, false
	}
	if rb := asMap(s["reblog"]); rb != nil {
		t, ok := restStatusToTweet(host, rb)
		if !ok {
			return tweet{}, false
		}
		if by := acctHandle(host, asString(asMap(s["account"])["acct"])); by != "" {
			t.RetweetedBy = &by
		}
		return t, true
	}
	return restBaseTweet(host, s)
}

// restReplyToTweet maps a REST status that is a reply: it points RootId at the
// thread root and resolves in_reply_to_id to the parent's AP note URL via idToURI.
func restReplyToTweet(host string, s map[string]any, rootURL string, idToURI map[string]string) (tweet, bool) {
	t, ok := restBaseTweet(host, s)
	if !ok {
		return tweet{}, false
	}
	t.RootId = rootURL
	parent := rootURL
	if irid := asString(s["in_reply_to_id"]); irid != "" {
		if puri, has := idToURI[irid]; has {
			parent = puri
		}
	}
	t.ParentId = &parent
	return t, true
}

// apReplies reads a Note's replies collection over ActivityPub, walking a
// bounded number of pages and dereferencing items that are note URIs. Used as
// the fallback for instances without the Mastodon REST context endpoint.
func (b *mastodonBridge) apReplies(ctx context.Context, noteURL string) (tweetsResponse, error) {
	m, err := b.ap.apGetJSON(ctx, noteURL, contentTypeAP)
	if err != nil {
		return tweetsResponse{}, err
	}
	resp := tweetsResponse{Tweets: []tweet{}}
	repliesURL := asString(m["replies"])
	if repliesURL == "" {
		return resp, nil
	}
	coll, err := b.ap.apGetJSON(ctx, repliesURL, contentTypeAP)
	if err != nil {
		return resp, nil //nolint:nilerr // hidden/absent replies -> empty, not an error
	}
	page := asMap(coll["first"])
	pageURL := ""
	if page == nil {
		pageURL = asString(coll["first"])
	}
	// Walk a bounded number of pages: Mastodon's replies collection lists items
	// as note URIs (strings), so each is dereferenced; some servers inline the
	// note objects instead. Bounded so a long thread can't run unbounded fetches.
	const maxPages = 5
	for p := 0; p < maxPages && len(resp.Tweets) < maxReplies; p++ {
		if page == nil {
			if pageURL == "" {
				break
			}
			page, _ = b.ap.apGetJSON(ctx, pageURL, contentTypeAP)
			if page == nil {
				break
			}
		}
		items := asSlice(page["items"])
		if len(items) == 0 {
			items = asSlice(page["orderedItems"]) // Mastodon uses items; others orderedItems
		}
		if room := maxReplies - len(resp.Tweets); len(items) > room {
			items = items[:room] // never dereference more than we can keep
		}
		resp.Tweets = append(resp.Tweets, b.resolveReplyItems(ctx, items)...)
		pageURL = asString(page["next"])
		page = nil
	}
	return resp, nil
}

// resolveReplyItems dereferences one reply page's items concurrently, preserving
// order. Each item is usually a note URI needing its own fetch (plus a possible
// quoted-author fetch), so a long thread would otherwise serialize dozens of
// round-trips; a bounded pool keeps the fan-out gentle on the remote instance.
func (b *mastodonBridge) resolveReplyItems(ctx context.Context, items []any) []tweet {
	const maxConcurrent = 8
	out := make([]tweet, len(items))
	ok := make([]bool, len(items))
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	for i, it := range items {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			note := asMap(it)
			if note == nil {
				// item is a note URI (Mastodon's usual form) — dereference it.
				if u := asString(it); u != "" {
					note, _ = b.ap.apGetJSON(ctx, u, contentTypeAP)
				}
			}
			if note == nil {
				return
			}
			if t, good := noteToTweet(handleFromActorURL(asString(note["attributedTo"])), note); good {
				b.fillQuotedAuthor(ctx, &t)
				out[i], ok[i] = t, true
			}
		}()
	}
	wg.Wait()
	replies := make([]tweet, 0, len(items))
	for i := range items {
		if ok[i] {
			replies = append(replies, out[i])
		}
	}
	return replies
}

// GetTweetStats reads the favourite/boost/reply counts for a status. Mastodon's
// REST status carries them directly
// (favourites_count/reblogs_count/replies_count) — which the AP Note does not
// federate (likes/shares are usually absent, so the AP path reports 0) — so
// prefer REST and fall back to the AP collections.
//
// MyReaction is deliberately left empty: the gateway is stateless and holds no
// Warpnet user's own reaction. The node that asked overwrites the field from its
// local store before handing the stats to its client.
func (b *mastodonBridge) GetTweetStats(ctx context.Context, noteURL string) (event.TweetStatsResponse, error) {
	noteURL = strings.TrimPrefix(noteURL, domain.RetweetPrefix)
	if host, id, ok := restStatusRef(noteURL); ok {
		if m, err := b.ap.apGetJSON(ctx, "https://"+host+"/api/v1/statuses/"+id, "application/json"); err == nil {
			if _, isStatus := m["replies_count"]; isStatus {
				return tweetStats(noteURL,
					numField(m["favourites_count"]),
					numField(m["reblogs_count"]),
					numField(m["replies_count"]),
				), nil
			}
		}
	}
	m, err := b.ap.apGetJSON(ctx, noteURL, contentTypeAP)
	if err != nil {
		return event.TweetStatsResponse{}, err
	}
	if inner := asMap(m["object"]); inner != nil {
		m = inner
	}
	return tweetStats(noteURL,
		apCollectionCount(m["likes"]),
		apCollectionCount(m["shares"]),
		apCollectionCount(m["replies"]),
	), nil
}

// tweetStats builds the stats response for a bridged status. Every Mastodon
// favourite reads back as the default heart, so the per-emoji breakdown the
// client paints its reaction chips from holds exactly that one entry — without
// it a favourited status would render no chip at all despite a non-zero count.
func tweetStats(noteURL string, favourites, boosts, replies uint64) event.TweetStatsResponse {
	resp := event.TweetStatsResponse{
		TweetId:        domain.ID(noteURL),
		ReactionsCount: favourites,
		RetweetsCount:  boosts,
		RepliesCount:   replies,
	}
	if favourites > 0 {
		resp.Reactions = map[string]uint64{domain.DefaultReaction: favourites}
	}
	return resp
}

// numField reads a JSON number (Mastodon REST count) as a uint64; 0 otherwise.
func numField(v any) uint64 {
	if f, ok := v.(float64); ok && f > 0 {
		return uint64(f)
	}
	return 0
}

func (b *mastodonBridge) GetFollowers(ctx context.Context, handle string, cursor *string) (followersResponse, error) {
	ids, next, err := b.followList(ctx, handle, cursor, "followers")
	if err != nil {
		return followersResponse{}, err
	}
	return followersResponse{FollowingId: handle, Followers: ids, Cursor: next}, nil
}

func (b *mastodonBridge) GetFollowings(ctx context.Context, handle string, cursor *string) (followingsResponse, error) {
	ids, next, err := b.followList(ctx, handle, cursor, "following")
	if err != nil {
		return followingsResponse{}, err
	}
	return followingsResponse{FollowerId: handle, Followings: ids, Cursor: next}, nil
}

// followList resolves the actor's follower/following collection to handles.
// Instances that hide the member list yield an empty result.
func (b *mastodonBridge) followList(ctx context.Context, handle string, cursor *string, field string) ([]string, string, error) {
	pageURL := pageCursor(cursor)
	if pageURL == "" {
		actorURL, err := b.resolveHandle(ctx, handle)
		if err != nil {
			return nil, "", err
		}
		actor, err := b.ap.fetchActor(ctx, actorURL)
		if err != nil {
			return nil, "", err
		}
		coll := asString(actor[field])
		if coll == "" {
			return []string{}, "", nil
		}
		page, perr := b.ap.apGetJSON(ctx, coll, contentTypeAP)
		if perr != nil {
			return []string{}, "", nil //nolint:nilerr // hidden collection -> empty, not an error
		}
		hasItems := len(asSlice(page["orderedItems"])) > 0 || len(asSlice(page["items"])) > 0
		if first := asString(page["first"]); first != "" && !hasItems {
			pageURL = first
		} else {
			return b.dropSelfHandles(collectHandles(page)), asString(page["next"]), nil
		}
	}
	page, err := b.ap.apGetJSON(ctx, pageURL, contentTypeAP)
	if err != nil {
		return []string{}, "", nil //nolint:nilerr // hidden collection -> empty, not an error
	}
	return b.dropSelfHandles(collectHandles(page)), asString(page["next"]), nil
}

// dropSelfHandles removes handles hosted by the gateway. A Warpnet user who
// follows a Fediverse account appears in that account's follower list as our own
// actor URL, so listing it would offer the user a Mastodon profile of themselves
// — the loop back into Warpnet through the Fediverse.
func (b *mastodonBridge) dropSelfHandles(handles []string) []string {
	out := make([]string, 0, len(handles))
	for _, h := range handles {
		if _, instance, ok := strings.Cut(h, "@"); ok && b.ap.isSelfHost(instance) {
			continue
		}
		out = append(out, h)
	}
	return out
}

func (b *mastodonBridge) GetImage(ctx context.Context, rawURL string) (getImageResponse, error) {
	mime, data, err := b.ap.fetchMedia(ctx, rawURL)
	if err != nil {
		return getImageResponse{}, err
	}
	if mime == "" {
		mime = "image/jpeg"
	}
	// Warpnet stores and serves images as full data URLs (the frontend puts the
	// value straight into <img src>), so mirror that format on the wire.
	return getImageResponse{File: "data:" + mime + ";base64," + base64.StdEncoding.EncodeToString(data)}, nil
}

// --- writes (Warpnet -> Mastodon) ---

// React federates a Warpnet reaction (or its undo) as an AP Like — a Mastodon
// favourite — and returns the status's favourite count. emoji must already be
// normalized (see domain.NormalizeReaction); an undo names none.
//
// Mastodon has one reaction, the favourite, so only the default heart maps onto
// it; emoji reactions are a Pleroma/Misskey extension the Mastodon inbox would
// reject. Any other emoji is therefore accepted and dropped rather than
// federated as a favourite the reactor did not intend.
func (b *mastodonBridge) React(ctx context.Context, localUser, objectURL, emoji string, undo bool) (uint64, error) {
	if !undo && emoji != domain.DefaultReaction {
		return 0, nil
	}
	note, inbox, err := b.authorInbox(ctx, objectURL)
	if err != nil {
		return 0, err
	}
	actorID := b.ap.actorID(localUser)
	like := activity{Context: asContext, ID: actorID + "#like-" + randomToken(), Type: typeLike, Actor: actorID, Object: asString(note["id"])}
	if derr := b.ap.postSigned(ctx, localUser, inbox, undoIf(actorID, like, undo)); derr != nil {
		return 0, derr
	}
	// The note was fetched before the Like federated, so its likes count does
	// not include this action yet — adjust so the caller sees the new value.
	count := apCollectionCount(note["likes"])
	if undo {
		if count > 0 {
			count--
		}
		return count, nil
	}
	return count + 1, nil
}

// Announce federates a boost (or its undo) of objectURL.
func (b *mastodonBridge) Announce(ctx context.Context, localUser, objectURL string, undo bool) error {
	note, inbox, err := b.authorInbox(ctx, objectURL)
	if err != nil {
		return err
	}
	actorID := b.ap.actorID(localUser)
	announce := activity{Context: asContext, ID: actorID + "#announce-" + randomToken(), Type: typeAnnounce, Actor: actorID, Object: asString(note["id"]), To: []string{asPublic}}
	return b.ap.postSigned(ctx, localUser, inbox, undoIf(actorID, announce, undo))
}

// Follow federates a follow (or its undo) of a Mastodon handle.
func (b *mastodonBridge) Follow(ctx context.Context, localUser, followingHandle string, undo bool) error {
	actorURL, err := b.resolveHandle(ctx, followingHandle)
	if err != nil {
		return err
	}
	b.ap.deliverFollow(localUser, actorURL, undo)
	return nil
}

// mentionOf builds the Mention tag for the replied-to author. href is what
// Mastodon resolves the mention by; name is the @handle it renders, derived
// from the parent status url.
func mentionOf(authorActorURL, parentURL string) []mentionTag {
	if authorActorURL == "" {
		return nil
	}
	m := mentionTag{Type: typeMention, Href: authorActorURL}
	if handle := statusAuthorHandle(parentURL); handle != "" {
		m.Name = "@" + handle
	}
	return []mentionTag{m}
}

// Reply federates a Warpnet reply as a Create(Note) inReplyTo the parent. The
// reply arrives as a tweet carrying a parent — warpnet's folded reply shape.
func (b *mastodonBridge) Reply(ctx context.Context, ev tweet) error {
	parentURL := ev.RootId
	if ev.ParentId != nil && *ev.ParentId != "" {
		parentURL = *ev.ParentId
	}
	obj, inbox, err := b.authorInbox(ctx, parentURL)
	if err != nil {
		return err
	}
	// Address the parent author (To) with the public collection in Cc, so the
	// reply is delivered/notified to them and shown publicly, not just threaded.
	author := asString(obj["attributedTo"])
	localUser := ev.UserId
	actorID := b.ap.actorID(localUser)
	// Reuse the node-assigned reply id in the deterministic /statuses/{id}
	// scheme: retries then dedupe on the remote side, and the id stays
	// dereferenceable via serveStatus instead of dangling.
	noteID := ev.Id
	if noteID == "" {
		noteID = randomToken()
	}
	noteURL := actorID + pathStatuses + noteID
	// Carry the parent url on the reply's own id: the node keys replies under
	// their parent, so serveStatus needs it to resolve this note when a remote
	// instance dereferences the id (a bare id only resolves a top-level tweet).
	replyURL := noteURL + "?" + url.Values{replyParentQuery: {parentURL}}.Encode()
	n := note{
		Context:      asContext,
		ID:           replyURL,
		Type:         typeNote,
		AttributedTo: actorID,
		Content:      ev.Text,
		Published:    time.Now().UTC().Format(time.RFC3339),
		InReplyTo:    parentURL,
		To:           []string{author},
		Cc:           []string{asPublic},
		Tag:          mentionOf(author, parentURL),
	}
	create := activity{Context: asContext, ID: noteURL + "#create", Type: typeCreate, Actor: actorID, Object: n, To: []string{author}, Cc: []string{asPublic}}
	return b.ap.postSigned(ctx, localUser, inbox, create)
}

// Delete federates the deletion of a Warpnet reply to a Mastodon note as an AP
// Delete(Tombstone) addressed to the parent author, mirroring the Create that
// Reply sent. The deleted Note id is the same deterministic /statuses/{id} url.
func (b *mastodonBridge) Delete(ctx context.Context, ev deleteTweetEvent) error {
	parentURL := ev.RootId
	if ev.ParentId != "" {
		parentURL = ev.ParentId
	}
	obj, inbox, err := b.authorInbox(ctx, parentURL)
	if err != nil {
		return err
	}
	author := asString(obj["attributedTo"])
	localUser := ev.UserId
	actorID := b.ap.actorID(localUser)
	noteURL := actorID + pathStatuses + ev.TweetId
	// The Tombstone id must be byte-identical to the note id b.Reply federated
	// (which carries the parent url), or Mastodon can't find the status to drop.
	replyURL := noteURL + "?" + url.Values{replyParentQuery: {parentURL}}.Encode()
	del := activity{
		Context: asContext,
		ID:      noteURL + "#delete",
		Type:    typeDelete,
		Actor:   actorID,
		Object:  tombstone{ID: replyURL, Type: typeTombstone},
		To:      []string{author},
		Cc:      []string{asPublic},
	}
	return b.ap.postSigned(ctx, localUser, inbox, del)
}

// authorInbox fetches an object (Note) once and resolves its author's inbox,
// returning the fetched note so callers can also read its counts.
func (b *mastodonBridge) authorInbox(ctx context.Context, objectURL string) (map[string]any, string, error) {
	m, err := b.ap.apGetJSON(ctx, strings.TrimPrefix(objectURL, domain.RetweetPrefix), contentTypeAP)
	if err != nil {
		return nil, "", err
	}
	if inner := asMap(m["object"]); inner != nil {
		m = inner
	}
	author := asString(m["attributedTo"])
	if author == "" {
		return nil, "", fmt.Errorf("mastodon: object %s has no attributedTo", objectURL)
	}
	inbox, err := b.ap.remoteInbox(ctx, author)
	return m, inbox, err
}

// undoIf wraps an activity in an Undo when undo is set.
func undoIf(actorID string, inner activity, undo bool) any {
	if !undo {
		return inner
	}
	return activity{Context: asContext, ID: actorID + "#undo-" + randomToken(), Type: typeUndo, Actor: actorID, Object: inner}
}
