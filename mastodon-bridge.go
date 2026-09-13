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
	"github.com/hashicorp/golang-lru/v2/expirable"
	log "github.com/sirupsen/logrus"
)

// apTransport is the ActivityPub HTTP surface the bridge needs; *gateway
// implements it (reusing its SSRF-hardened client and signing keys).
type apTransport interface {
	apGetJSON(ctx context.Context, rawURL, accept string) (map[string]any, error)
	resolveActorID(ctx context.Context, id string) (string, error)
	canonicalHandle(ctx context.Context, actorURL string) string
	rememberHandle(actorURL, handle string)
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

	// refs remembers the Mastodon REST status a canonical note url was read as.
	// A status read from a mirror is reachable only under the mirror's own id,
	// which the page that produced it hands out alongside the uri and nothing
	// else ever would — without it a mirrored thread could not be opened.
	refs *expirable.LRU[string, statusRef]

	// misses remembers that a host has no answer for an account, so a REST API
	// that is not there (Threads 404s every /api/v1 path) and a mirror that does
	// not know the account are each probed once rather than on every render —
	// the client re-polls every followed handle every 75s.
	misses *expirable.LRU[string, struct{}]
}

// statusRef locates a status on a Mastodon REST API: the instance serving it and
// its id there.
type statusRef struct{ host, id string }

const (
	restRefsSize = 4096
	restRefsTTL  = 30 * time.Minute
	restMissSize = 2048
	restMissTTL  = 10 * time.Minute
)

func newMastodonBridge(ap apTransport, nodeID string) *mastodonBridge {
	return &mastodonBridge{
		ap: ap, nodeID: nodeID,
		refs:   expirable.NewLRU[string, statusRef](restRefsSize, nil, restRefsTTL),
		misses: expirable.NewLRU[string, struct{}](restMissSize, nil, restMissTTL),
	}
}

func (b *mastodonBridge) rememberRef(uri, host, id string) {
	if b.refs == nil || uri == "" || host == "" || id == "" {
		return
	}
	b.refs.Add(uri, statusRef{host: host, id: id})
}

// restRef locates the Mastodon REST status representing a note: the copy on the
// instance it was actually read from when one was seen, otherwise the status on
// the note's own host.
func (b *mastodonBridge) restRef(noteURL string) (statusRef, bool) {
	if b.refs != nil {
		if r, ok := b.refs.Get(noteURL); ok {
			return r, true
		}
	}
	host, id, ok := restStatusRef(noteURL)
	return statusRef{host: host, id: id}, ok
}

// restAccount resolves a handle to its account object on apiHost's Mastodon REST
// API, always naming the full name@instance acct so a mirror answers for the
// remote account and not a local namesake. A host that does not answer for the
// account is remembered for a while (see misses).
func (b *mastodonBridge) restAccount(ctx context.Context, handle, apiHost string) (map[string]any, bool) {
	name, instance, ok := strings.Cut(strings.TrimPrefix(handle, "@"), "@")
	if !ok || name == "" || instance == "" {
		return nil, false
	}
	acct := name + "@" + instance
	key := apiHost + " " + acct
	if b.misses != nil {
		if _, missed := b.misses.Get(key); missed {
			return nil, false
		}
	}
	acc, err := b.ap.apGetJSON(ctx, "https://"+apiHost+"/api/v1/accounts/lookup?acct="+url.QueryEscape(acct), "application/json")
	if err != nil || asString(acc["id"]) == "" {
		if b.misses != nil {
			b.misses.Add(key, struct{}{})
		}
		return nil, false
	}
	b.rememberAccountHandle(acc)
	return acc, true
}

// rememberAccountHandle records a REST account's actor url and handle together.
// It is the cheap way to name an actor served under an opaque id: the pairing is
// right there in every account object, where reading it off the actor itself
// would mean a signed fetch the origin can refuse.
func (b *mastodonBridge) rememberAccountHandle(acc map[string]any) {
	uri, handle := asString(acc["uri"]), asString(acc["acct"])
	if uri == "" || !strings.Contains(handle, "@") {
		return
	}
	b.ap.rememberHandle(uri, handle)
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

// canonicalHandle resolves an actor url to the handle Warpnet stores as the user
// id. The url alone is not always enough: Threads serves some actors under a
// numeric id (…/ap/users/17841452547050663/) that its own WebFinger then refuses
// to resolve, so a handle read off the path would be a dead id — nothing could
// ever turn it back into an actor url. The actor document carries
// preferredUsername, which is the local part WebFinger does answer for.
//
// Only an opaque local part is dereferenced, so an ordinary handle costs no
// fetch; on the inbound path the actor was just fetched to verify the signature,
// so even that one is served from cache. A failed fetch degrades to the
// url-derived handle rather than dropping the activity.
func (g *gateway) canonicalHandle(ctx context.Context, actorURL string) string {
	naive := handleFromActorURL(actorURL)
	if !opaqueLocalPart(naive) {
		return naive
	}
	u, perr := url.Parse(actorURL)
	if perr != nil || u.Host == "" {
		return naive
	}
	if g.handles != nil {
		if handle, ok := g.handles.Get(actorURL); ok {
			return handle
		}
	}
	// A Mastodon-family host names the account behind an opaque id over its
	// public REST API, unauthenticated and in a fraction of the time a signed
	// actor fetch takes. It is tried first because the actor fetch needs our own
	// actor to be dereferenceable by the peer, and a peer in secure mode refuses
	// it outright when it is not.
	opaque := path.Base(strings.TrimRight(u.Path, "/"))
	if handle := g.restHandleByID(ctx, u.Host, opaque); handle != "" {
		g.rememberHandle(actorURL, handle)
		return handle
	}
	m, err := g.fetchActor(ctx, actorURL)
	if err != nil {
		log.Warnf("mastodon: canonical handle for %s: %v", actorURL, err)
		return naive
	}
	name := asString(m["preferredUsername"])
	if name == "" {
		return naive
	}
	handle := name + "@" + canonicalHost(u.Host)
	g.rememberHandle(actorURL, handle)
	return handle
}

// restHandleByID names the account an opaque actor id belongs to through the
// host's Mastodon REST API. Empty when the host serves none (Threads 404s) or
// does not know the id.
func (g *gateway) restHandleByID(ctx context.Context, host, id string) string {
	if host == "" || id == "" || id == "." || id == "/" {
		return ""
	}
	m, err := g.apGetJSON(ctx, "https://"+host+"/api/v1/accounts/"+url.PathEscape(id), "application/json")
	if err != nil {
		return ""
	}
	acct := asString(m["acct"])
	if acct == "" {
		return ""
	}
	if strings.Contains(acct, "@") {
		return acct // a remote account the host knows, already a full handle
	}
	return acct + "@" + canonicalHost(host)
}

// rememberHandle records an actor url -> handle pairing learned elsewhere, so
// canonicalHandle can answer without dereferencing the actor. It matters for
// Threads: it serves some actors under an opaque id and gates every fetch behind
// a signature it is free to reject, and a mirror hands out the pairing anyway.
func (g *gateway) rememberHandle(actorURL, handle string) {
	if g.handles == nil || actorURL == "" || handle == "" {
		return
	}
	g.handles.Add(actorURL, handle)
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
	if mirrorFirst(handle) {
		if mu, ok := b.mirrorUser(ctx, handle); ok {
			return mu, nil
		}
	}
	// A list context skips the three collection fetches, which leaves the counts
	// at zero — and the asking node stores whatever it is handed, so those zeros
	// land on top of the real numbers and the profile reads "0 Followers" until
	// something refreshes it. The account's own REST API carries the profile and
	// its counts in the single request the brief path already budgets for, so
	// nothing is skipped and nothing is zeroed.
	if !withCounts {
		if hu, ok := b.hostUser(ctx, handle); ok {
			return hu, nil
		}
	}
	u, err := b.apUser(ctx, handle, withCounts)
	if err == nil {
		return u, nil
	}
	// The account's own server did not answer — Threads hides an actor behind a
	// signed fetch it can refuse, and its collections carry no members anyway.
	// An instance that federates with it holds a copy of the profile, and its
	// counts in the same single request where ActivityPub needs three more.
	if mu, ok := b.mirrorUser(ctx, handle); ok {
		return mu, nil
	}
	return user{}, err
}

// apUser reads a profile from the account's own server over ActivityPub.
func (b *mastodonBridge) apUser(ctx context.Context, handle string, withCounts bool) (user, error) {
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
// back to the outbox, and an account whose own server serves neither is read
// from a mirror (mirrorTweets). cursor, when set, continues whichever source
// produced it.
func (b *mastodonBridge) GetTweets(ctx context.Context, handle string, cursor *string) (tweetsResponse, error) {
	if pc := pageCursor(cursor); pc != "" {
		if isRESTStatusesURL(pc) {
			resp, _ := b.restTweetsPage(ctx, handle, pc)
			return resp, nil
		}
		return b.apTweets(ctx, handle, cursor)
	}
	if mirrorFirst(handle) {
		if mirrored, ok := b.mirrorTweets(ctx, handle); ok {
			return mirrored, nil
		}
	}
	if resp, ok := b.restTweets(ctx, handle); ok {
		return resp, nil
	}
	resp, err := b.apTweets(ctx, handle, cursor)
	if err == nil && len(resp.Tweets) > 0 {
		return resp, nil
	}
	// The account's own server yielded nothing: Threads serves no REST API and
	// answers its outbox with a bare count, so this is the only reachable
	// source. The mirror is a last resort, never a preference — an instance
	// that does serve its own posts is always authoritative over a copy.
	if mirrored, ok := b.mirrorTweets(ctx, handle); ok {
		return mirrored, nil
	}
	return resp, err
}

// defaultMirrorHost is the instance asked for posts an account's own server will
// not serve. It is the one Warpnet already seeds as its entry into the Fediverse
// (warpnet's mastodon.EntryHandle), so the deployment depends on no new third
// party; GATEWAY_AP_MIRROR repoints it, and an empty value switches the fallback
// off entirely.
const defaultMirrorHost = "mastodon.social"

func mirrorHost() string { return envOr("GATEWAY_AP_MIRROR", defaultMirrorHost) }

// mirrorTweets reads a handle's posts from the mirror instance rather than the
// account's own. A Mastodon instance that federates with the account holds its
// posts and serves them over REST, and every status carries the canonical uri on
// the account's own server — so reads come from the copy while replies, likes
// and follows still go to the real thing.
//
// ok is false when no mirror is configured, when the mirror is the account's own
// instance (restTweets already asked it), or when the mirror does not know the
// account: it only holds accounts someone there follows, which is the standing
// limit of this path.
// mirrorFirst reports whether a handle's own server is known not to serve its
// posts or profile, so the mirror is asked before it rather than after. Threads
// answers its outbox and its follow collections with bare counts and serves no
// REST API: going to the origin first costs five requests and six seconds to
// learn nothing, and hands back an avatar url on Meta's CDN that expires within
// days, where the mirror's copy is stable. The origin stays the fallback.
func mirrorFirst(handle string) bool {
	_, instance, ok := strings.Cut(strings.TrimPrefix(handle, "@"), "@")
	return ok && isThreadsHost(instance)
}

// hollowFollowCollections reports whether a handle's server answers its
// follower and following collections with a bare count and no members. Threads
// does, and unlike its posts this cannot be read from a mirror either: an
// instance that federates with it holds the account but not its graph, and
// answers both REST endpoints with an empty array. Measured against the live
// gateway, asking the origin costs three seconds per tab to return nothing, so
// the only thing left to fix is not to ask. The counts still show — those come
// from the profile, not from here.
func hollowFollowCollections(handle string) bool {
	_, instance, ok := strings.Cut(strings.TrimPrefix(handle, "@"), "@")
	return ok && isThreadsHost(instance)
}

func (b *mastodonBridge) mirrorTweets(ctx context.Context, handle string) (tweetsResponse, bool) {
	mirror, ok := mirrorFor(handle)
	if !ok {
		return tweetsResponse{}, false
	}
	return b.restTweetsFrom(ctx, handle, mirror)
}

// mirrorFor names the instance to ask about a handle when its own server will
// not answer. There is none when the fallback is switched off or when the mirror
// is the account's own instance — that one has already been asked.
func mirrorFor(handle string) (string, bool) {
	_, instance, ok := strings.Cut(strings.TrimPrefix(handle, "@"), "@")
	if !ok || instance == "" {
		return "", false
	}
	mirror := mirrorHost()
	if mirror == "" || strings.EqualFold(mirror, instance) {
		return "", false
	}
	return mirror, true
}

// restUser reads a profile from apiHost's Mastodon REST API. The one request
// carries the profile and its counts, where ActivityPub needs an actor fetch
// plus a collection each for followers, followings and posts.
func (b *mastodonBridge) restUser(ctx context.Context, handle, apiHost string) (user, bool) {
	acc, ok := b.restAccount(ctx, handle, apiHost)
	if !ok {
		return user{}, false
	}
	return restAccountToUser(handle, acc, b.nodeID), true
}

// mirrorUser reads a profile from the mirror instance, under the same conditions
// as mirrorTweets.
func (b *mastodonBridge) mirrorUser(ctx context.Context, handle string) (user, bool) {
	mirror, ok := mirrorFor(handle)
	if !ok {
		return user{}, false
	}
	return b.restUser(ctx, handle, mirror)
}

// hostUser reads a profile from the account's own instance.
func (b *mastodonBridge) hostUser(ctx context.Context, handle string) (user, bool) {
	_, instance, ok := strings.Cut(strings.TrimPrefix(handle, "@"), "@")
	if !ok || instance == "" {
		return user{}, false
	}
	return b.restUser(ctx, handle, instance)
}

// isRESTStatusesURL reports whether a pagination cursor points at the Mastodon
// REST account-statuses endpoint (so pagination continues on the REST path).
func isRESTStatusesURL(u string) bool {
	return strings.Contains(u, "/api/v1/accounts/") && strings.Contains(u, "/statuses")
}

// restTweets loads a handle's first status page from the account's own instance
// over the Mastodon REST API. ok is false for non-Mastodon instances, so the
// caller falls back to the AP outbox.
func (b *mastodonBridge) restTweets(ctx context.Context, handle string) (tweetsResponse, bool) {
	_, instance, ok := strings.Cut(strings.TrimPrefix(handle, "@"), "@")
	if !ok || instance == "" {
		return tweetsResponse{}, false
	}
	return b.restTweetsFrom(ctx, handle, instance)
}

// restTweetsFrom loads a handle's first status page over the Mastodon REST API
// of apiHost, resolving the account id via /accounts/lookup. apiHost is the
// account's own instance, or a mirror that federates with it. The lookup always
// names the full name@instance acct, so a mirror answers for the remote account
// and not for a local namesake. ok is false when apiHost serves no Mastodon REST
// API or does not know the account.
func (b *mastodonBridge) restTweetsFrom(ctx context.Context, handle, apiHost string) (tweetsResponse, bool) {
	acc, ok := b.restAccount(ctx, handle, apiHost)
	if !ok {
		return tweetsResponse{}, false
	}
	accID := asString(acc["id"])
	// exclude_replies mirrors warpnet's own profile timeline, which is served
	// from the author's timeline keyspace and never contains replies; the Posts
	// tab must show only top-level posts (thread replies come from GetReplies).
	return b.restTweetsPage(ctx, handle, "https://"+apiHost+"/api/v1/accounts/"+accID+"/statuses?limit=40&exclude_replies=true")
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
			// The page is the only place a status's canonical uri and its id on
			// this instance appear together; a mirrored thread is unreachable
			// without the pairing.
			b.rememberRef(asString(s["uri"]), host, sid)
		}
		b.rememberAccountHandle(asMap(s["account"]))
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
		t, ok := noteToTweet(b.ap.canonicalHandle(ctx, asString(bm["attributedTo"])), bm)
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
		h := b.ap.canonicalHandle(ctx, author)
		t.QuotedUserId = &h
	}
}

// GetTweet fetches a single status by its id. Mastodon-family instances serve
// the full status (counts, boost, media inline) from one REST call; others fall
// back to dereferencing the AP Note.
func (b *mastodonBridge) GetTweet(ctx context.Context, noteURL string) (tweet, error) {
	noteURL = strings.TrimPrefix(noteURL, domain.RetweetPrefix)
	if ref, ok := b.restRef(noteURL); ok {
		if m, err := b.ap.apGetJSON(ctx, "https://"+ref.host+"/api/v1/statuses/"+ref.id, "application/json"); err == nil {
			if t, ok := restStatusToTweet(ref.host, m); ok {
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
	t, ok := noteToTweet(b.ap.canonicalHandle(ctx, asString(m["attributedTo"])), m)
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
	ref, ok := b.restRef(noteURL)
	if !ok {
		return tweetsResponse{}, false
	}
	host, id := ref.host, ref.id
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
		if s == nil {
			continue
		}
		b.rememberRef(asString(s["uri"]), host, asString(s["id"]))
		b.rememberAccountHandle(asMap(s["account"]))
		// The context lists the note's whole subtree flattened; only direct
		// children are this note's replies — deeper levels are served when
		// the client walks the thread one parent at a time, and the pairing
		// remembered above is what lets it.
		if asString(s["in_reply_to_id"]) != id {
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
		Network:   networkOfHandle(handle),
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
		// Mastodon nests the quoted status under quoted_status and keeps only the
		// acceptance state on the quote itself; other servers inline the status.
		qs := asMap(q["quoted_status"])
		if qs == nil {
			qs = q
		}
		if qu := asString(qs["uri"]); qu != "" {
			t.QuotedTweetId = &qu
			if qh := acctHandle(host, asString(asMap(qs["account"])["acct"])); qh != "" {
				t.QuotedUserId = &qh
			}
		}
		// Drop the "RE: <url>" fallback Mastodon inlines into a quote's content:
		// left in, it renders as a bare link line that reads like a stray reply
		// sitting in the timeline. Mirrors noteToTweet on the ActivityPub path.
		if comment, stripped := stripQuoteFallback(t.Text); stripped && comment != "" {
			t.Text = comment
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
			if t, good := noteToTweet(b.ap.canonicalHandle(ctx, asString(note["attributedTo"])), note); good {
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
	if ref, ok := b.restRef(noteURL); ok {
		if m, err := b.ap.apGetJSON(ctx, "https://"+ref.host+"/api/v1/statuses/"+ref.id, "application/json"); err == nil {
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
		resp.Reactions = map[string]uint64{defaultReaction: favourites}
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
	if hollowFollowCollections(handle) {
		return []string{}, "", nil
	}
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
			return b.dropSelfHandles(b.collectHandles(ctx, page)), asString(page["next"]), nil
		}
	}
	page, err := b.ap.apGetJSON(ctx, pageURL, contentTypeAP)
	if err != nil {
		return []string{}, "", nil //nolint:nilerr // hidden collection -> empty, not an error
	}
	return b.dropSelfHandles(b.collectHandles(ctx, page)), asString(page["next"]), nil
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
// normalized (see normalizeReaction); an undo names none.
//
// Mastodon has one reaction, the favourite, so only the default heart maps onto
// it; emoji reactions are a Pleroma/Misskey extension the Mastodon inbox would
// reject. Any other emoji is therefore accepted and dropped rather than
// federated as a favourite the reactor did not intend.
func (b *mastodonBridge) React(ctx context.Context, localUser, objectURL, emoji string, undo bool) (uint64, error) {
	if !undo && emoji != defaultReaction {
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
