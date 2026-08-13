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
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"

	"github.com/Warp-net/warpnet/retrier"
	"github.com/hashicorp/golang-lru/v2/expirable"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

const (
	contentTypeAP  = "application/activity+json"
	contentTypeJRD = "application/jrd+json"
	asContext      = "https://www.w3.org/ns/activitystreams"
	secContext     = "https://w3id.org/security/v1"

	maxBodyBytes = 1 << 20

	// maxMediaBytes bounds a media (attachment) download. Mastodon serves
	// original images up to 16 MB; capping lower silently truncated large
	// PNGs and nodes cached the broken bytes.
	maxMediaBytes = 16 << 20

	// maxInflightDeliveries bounds concurrent outbound Accept deliveries so a
	// burst of inbound Follow activities can't spawn unbounded goroutines.
	maxInflightDeliveries = 16

	// maxRedirects caps redirect hops for outbound federation fetches.
	maxRedirects = 5

	pathUsers = "/users/"
	pathInbox = "/inbox"
	pathActor = "/actor"
	// instanceActorName is the preferredUsername of the gateway instance actor
	// (served at /actor); it is webfinger-resolvable so peers can cache it.
	instanceActorName = "warpnet-gw"
	pathFollowers     = "/followers"
	pathStatuses      = "/statuses/"
	pathMedia         = "/media/"
	// pathPprof is the Go runtime profiling subtree (net/http/pprof), gated by
	// the same token as /logs.
	pathPprof = "/debug/pprof/"
	// replyParentQuery carries a reply note's parent url on its own status id, so
	// serveStatus can hand the node the thread key it needs to resolve a reply
	// (the node stores replies under their parent, not in the author's timeline).
	replyParentQuery = "parent"

	headerContentType = "Content-Type"
)

// userAgent identifies the gateway on outbound Fediverse requests; some
// instances and CDNs reject the default Go user-agent.
const userAgent = "warpnet-gateway/" + gatewayVersion

var (
	errActorMalformed   = errors.New("actor document malformed")
	errRemoteStatus     = errors.New("remote returned error status")
	errInsecureURL      = errors.New("remote URL must be https")
	errBlockedHost      = errors.New("remote URL host is not allowed")
	errTooManyRedirects = errors.New("too many redirects")
	errSelfTarget       = errors.New("remote URL targets this gateway")
	errBodyTooLarge     = errors.New("remote body exceeds the size limit")
)

// gateway is the ActivityPub front for one bridged Warpnet user. Documents are
// rendered on demand from source; the state it keeps on disk is the RSA signing
// key and the follower store (followers.go). Warpnet content is never stored
// here.
type gateway struct {
	host       string // public hostname, e.g. name.tailnet.ts.net (no scheme)
	key        *rsa.PrivateKey
	keyPubPEM  string
	source     warpnetSource
	client     *http.Client
	retrier    retrier.Retrier // retries transient Mastodon HTTP failures; nil = single attempt
	sem        chan struct{}   // bounds concurrent Accept deliveries
	followers  followerStore
	req        nodeRequester          // connector to the owner's node; nil in dev/no-node mode
	onFollowed func(localUser string) // starts outbound federation for a user; nil without a node
	limits     *rateLimiters          // weighted per-IP + global rate limiting; built lazily by routes()

	// getCache deduplicates outbound signed GETs for a very short window so the
	// burst of per-tweet/per-actor fetches a single timeline render triggers
	// collapses onto one round-trip. Transient, in memory, 1s TTL — not storage.
	getCache *expirable.LRU[string, cachedGet]

	// actorIDs caches handle -> actor url so resolving the follow graph (a
	// followers page, a note fan-out) doesn't WebFinger every follower on every
	// call. Transient, in memory — the graph itself lives in Warpnet.
	actorIDs *expirable.LRU[string, string]

	// sf collapses concurrent identical signed GETs onto a single upstream
	// round-trip. getCache only dedupes sequential bursts (it fills after a
	// request returns), so the overlapping author/stats/context fetches a reply
	// thread fans out would each miss the cache and hit the remote independently.
	sf singleflight.Group

	// logs buffers recent log lines for the /logs endpoint; logsToken gates it
	// (empty token = endpoint disabled). Both are in-memory only.
	logs      *logRing
	logsToken string

	// allowPrivateTargets disables the SSRF guard's loopback/private-range
	// rejection for outbound delivery. Test-only; never set in main.go.
	allowPrivateTargets bool
}

// cachedGet is a memoized signed-GET response (final, non-retryable).
type cachedGet struct {
	status int
	body   []byte
}

const (
	// getCacheTTL is intentionally tiny: it only collapses the fan-out of a
	// single request burst, never serves stale data across user actions.
	getCacheTTL  = time.Second
	getCacheSize = 2048

	// A handle's actor url effectively never changes, so this may be long; it is
	// bounded and in memory only.
	actorIDsTTL  = time.Hour
	actorIDsSize = 2048
)

func (g *gateway) baseURL() string            { return "https://" + g.host }
func (g *gateway) actorID(user string) string { return g.baseURL() + pathUsers + user }
func (g *gateway) keyID(user string) string   { return g.actorID(user) + "#main-key" }

// instanceActorID is the gateway's own ActivityPub actor (an Application). It
// signs outbound authorized-fetch GETs: secure-mode peers dereference
// instanceKeyID to fetch the gateway signing key and verify the fetch. It is
// served from memory, independent of any Warpnet user.
func (g *gateway) instanceActorID() string { return g.baseURL() + pathActor }
func (g *gateway) instanceKeyID() string   { return g.instanceActorID() + "#main-key" }

// signGet signs an outbound GET as the gateway instance actor so secure-mode
// (authorized-fetch) peers answer it; a no-op when no signing key is present.
func (g *gateway) signGet(req *http.Request) error {
	if g.key == nil {
		return nil
	}
	return signRequest(req, g.instanceKeyID(), g.key, nil)
}

func (g *gateway) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/webfinger", g.handleWebFinger)
	mux.HandleFunc("/.well-known/nodeinfo", g.handleNodeInfoLinks)
	mux.HandleFunc("/nodeinfo/2.0", g.handleNodeInfo)
	mux.HandleFunc(pathUsers, g.handleUsers)
	mux.HandleFunc(pathActor, g.handleInstanceActor)
	mux.HandleFunc(pathActor+"/", g.handleInstanceActorSub)
	mux.HandleFunc(pathInbox, g.handleSharedInbox)
	mux.HandleFunc(pathMedia, g.handleMedia)
	mux.HandleFunc(pathStatic, g.handleStatic)
	mux.HandleFunc("/logs", g.handleLogs)
	mux.Handle(pathPprof, g.pprofHandler())
	if g.limits == nil {
		g.limits = newRateLimiters()
	}
	return logRequests(g.limits.middleware(mux))
}

// logRequests logs every inbound request (method, path, status, user-agent) so
// it's visible what Mastodon actually fetches — the actor, /media, the outbox, etc.
func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		log.Infof("http: %s %s -> %d in %s (ua=%q)", r.Method, r.URL.RequestURI(), sw.status, time.Since(start).Round(time.Millisecond), r.UserAgent())
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// logsHandler is the handler for the optional standalone /logs listener
// (GATEWAY_LOGS_ADDR): it exposes only the debug surface (/logs and
// /debug/pprof), never the federation surface.
func (g *gateway) logsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/logs", g.handleLogs)
	mux.Handle(pathPprof, g.pprofHandler())
	return mux
}

// authorizeDebug gates the operator surface (/logs, /debug/pprof) on
// GATEWAY_LOGS_TOKEN, supplied as ?token= or a Bearer header: without a
// configured token the surface is disabled (404), so the Funnel-exposed gateway
// never leaks it publicly by default. It writes the response when it denies.
func (g *gateway) authorizeDebug(w http.ResponseWriter, r *http.Request) bool {
	if g.logsToken == "" {
		http.NotFound(w, r)
		return false
	}
	provided := r.URL.Query().Get("token")
	if provided == "" {
		provided = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	}
	if subtle.ConstantTimeCompare([]byte(provided), []byte(g.logsToken)) != 1 {
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

// pprofHandler serves the Go runtime profiles under /debug/pprof/ (heap,
// goroutine, CPU profile, trace) behind the same token as /logs, so the running
// gateway can be profiled without a debug build or a restart.
func (g *gateway) pprofHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(pathPprof, pprof.Index) // also serves the named profiles
	mux.HandleFunc(pathPprof+"cmdline", pprof.Cmdline)
	mux.HandleFunc(pathPprof+"profile", pprof.Profile)
	mux.HandleFunc(pathPprof+"symbol", pprof.Symbol)
	mux.HandleFunc(pathPprof+"trace", pprof.Trace)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !g.authorizeDebug(w, r) {
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// handleLogs serves the in-memory log ring as text/plain, gated like the rest of
// the debug surface (see authorizeDebug).
//
// ?network=<name> reads one Warpnet network apart from the others: its own lines
// plus the ones belonging to no network. Without it, every network is returned.
func (g *gateway) handleLogs(w http.ResponseWriter, r *http.Request) {
	if g.logs == nil {
		http.NotFound(w, r)
		return
	}
	if !g.authorizeDebug(w, r) {
		return
	}
	w.Header().Set(headerContentType, "text/plain; charset=utf-8")
	for _, line := range g.logs.lines(r.URL.Query().Get("network")) {
		_, _ = io.WriteString(w, line+"\n")
	}
}

func (g *gateway) handleWebFinger(w http.ResponseWriter, r *http.Request) {
	acct := strings.TrimPrefix(r.URL.Query().Get("resource"), "acct:")
	at := strings.LastIndexByte(acct, '@')
	if at < 0 {
		http.Error(w, "bad resource", http.StatusBadRequest)
		return
	}
	user, domain := acct[:at], acct[at+1:]
	if domain != g.host {
		http.NotFound(w, r)
		return
	}
	// The gateway instance actor must be webfinger-resolvable: peers webfinger
	// the keyId actor after fetching it, and a 404 makes them re-resolve on
	// every signed request (a fetch storm against /actor).
	if user == instanceActorName {
		writeJSON(w, contentTypeJRD, webFingerJRD{
			Subject: "acct:" + user + "@" + g.host,
			Links:   []webFingerLink{{Rel: "self", Type: contentTypeAP, Href: g.instanceActorID()}},
		})
		return
	}
	if _, ok := g.source.GetUser(user); !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, contentTypeJRD, webFingerJRD{
		Subject: "acct:" + user + "@" + g.host,
		Links: []webFingerLink{{
			Rel:  "self",
			Type: contentTypeAP,
			Href: g.actorID(user),
		}},
	})
}

// handleUsers serves the actor document at /users/{user} and dispatches the
// per-actor sub-collections and inbox.
func (g *gateway) handleUsers(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, pathUsers), "/")
	user := parts[0]
	wu, ok := g.source.GetUser(user)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 1 || parts[1] == "" {
		g.serveActor(w, wu)
		return
	}
	switch parts[1] {
	case "inbox":
		g.handleInbox(w, r, user)
	case "outbox":
		g.serveOutbox(w, user)
	case "followers":
		g.serveFollowers(w, user)
	case "following":
		g.serveFollowing(w, user)
	case "statuses":
		if len(parts) < 3 || parts[2] == "" {
			http.NotFound(w, r)
			return
		}
		if len(parts) >= 4 && parts[3] == "replies" {
			g.serveReplies(w, user, parts[2])
			return
		}
		g.serveStatus(w, r, user, parts[2])
	default:
		http.NotFound(w, r)
	}
}

func (g *gateway) serveActor(w http.ResponseWriter, wu warpnetUser) {
	writeJSON(w, contentTypeAP, g.buildActor(wu))
}

// handleInstanceActor serves the gateway's own Application actor. It carries the
// gateway signing key so peers can verify our authorized-fetch GETs; it is not
// a Warpnet user and resolves without a network round-trip.
func (g *gateway) handleInstanceActor(w http.ResponseWriter, _ *http.Request) {
	id := g.instanceActorID()
	// The instance actor and its key are static — let peers cache them instead
	// of re-fetching on every signed request.
	w.Header().Set("Cache-Control", "max-age=3600")
	writeJSON(w, contentTypeAP, actor{
		Context:           []any{asContext, secContext},
		ID:                id,
		Type:              "Application",
		PreferredUsername: instanceActorName,
		Inbox:             g.baseURL() + pathInbox,
		Outbox:            id + "/outbox",
		Followers:         id + pathFollowers,
		Following:         id + "/following",
		PublicKey: publicKey{
			ID:           g.instanceKeyID(),
			Owner:        id,
			PublicKeyPEM: g.keyPubPEM,
		},
		Endpoints: &actorEndpoints{SharedInbox: g.baseURL() + pathInbox},
	})
}

// handleInstanceActorSub serves the instance actor's advertised collections as
// empty (it federates nothing of its own); anything else under /actor/ is 404.
func (g *gateway) handleInstanceActorSub(w http.ResponseWriter, r *http.Request) {
	switch strings.TrimPrefix(r.URL.Path, pathActor+"/") {
	case "outbox", "followers", "following":
		g.serveEmptyCollection(w, g.baseURL()+r.URL.Path)
	default:
		http.NotFound(w, r)
	}
}

// buildActor renders the actor document. Shared by serveActor (served on GET)
// and the outbound Update(Person) that refreshes followers' cached profile.
func (g *gateway) buildActor(wu warpnetUser) actor {
	id := g.actorID(wu.PreferredUsername)
	name := wu.DisplayName
	if name == "" {
		name = wu.PreferredUsername
	}
	a := actor{
		// The extra context object defines toot:Emoji and schema:PropertyValue so
		// the badge emoji and the "Network" profile field are recognized.
		Context: []any{asContext, secContext, map[string]string{
			"toot":          "http://joinmastodon.org/ns#",
			"Emoji":         "toot:Emoji",
			"schema":        "http://schema.org#",
			"PropertyValue": "schema:PropertyValue",
			"value":         "schema:value",
		}},
		ID:                id,
		Type:              "Person",
		PreferredUsername: wu.PreferredUsername,
		Name:              name + " " + warpnetEmojiShortcode,
		Summary:           badgedSummary(wu.Summary),
		Tag:               []emojiTag{g.warpnetActorTag()},
		Attachment:        []propertyValue{warpnetNetworkField()},
		Inbox:             id + pathInbox,
		Outbox:            id + "/outbox",
		Followers:         id + pathFollowers,
		Following:         id + "/following",
		PublicKey: publicKey{
			ID:           g.keyID(wu.PreferredUsername),
			Owner:        id,
			PublicKeyPEM: g.keyPubPEM,
		},
		Endpoints: &actorEndpoints{SharedInbox: g.baseURL() + pathInbox},
	}
	if wu.Avatar != "" {
		a.Icon = &attachment{Type: "Image", URL: g.baseURL() + pathMedia + encodeMediaRef(wu.PreferredUsername, wu.Avatar)}
	}
	if wu.Background != "" {
		a.Image = &attachment{Type: "Image", URL: g.baseURL() + pathMedia + encodeMediaRef(wu.PreferredUsername, wu.Background)}
	}
	return a
}

func (g *gateway) serveEmptyCollection(w http.ResponseWriter, id string) {
	writeJSON(w, contentTypeAP, orderedCollection{
		Context:      asContext,
		ID:           id,
		Type:         "OrderedCollection",
		TotalItems:   0,
		OrderedItems: []any{},
	})
}

func (g *gateway) serveFollowers(w http.ResponseWriter, user string) {
	urls, err := g.followers.List(user)
	if err != nil {
		log.Errorf("followers: list %s: %v", user, err)
	}
	items := make([]any, 0, len(urls))
	for _, u := range urls {
		items = append(items, u)
	}
	writeJSON(w, contentTypeAP, orderedCollection{
		Context:      asContext,
		ID:           g.actorID(user) + pathFollowers,
		Type:         "OrderedCollection",
		TotalItems:   len(items),
		OrderedItems: items,
	})
}

// serveOutbox renders the user's Warpnet posts (PUBLIC_GET_TWEETS) as an
// OrderedCollection of Create(Note) so they appear on the Mastodon profile.
func (g *gateway) serveOutbox(w http.ResponseWriter, userID string) {
	id := g.actorID(userID) + "/outbox"
	if g.req == nil {
		g.serveEmptyCollection(w, id)
		return
	}
	bt, err := g.req.requestUser(userID, routeGetTweets, getAllTweetsEvent{UserId: userID})
	if err != nil {
		log.Warnf("outbox: fetch %s: %v", userID, err)
		g.serveEmptyCollection(w, id)
		return
	}
	var resp tweetsResponse
	if jerr := json.Unmarshal(bt, &resp); jerr != nil {
		g.serveEmptyCollection(w, id)
		return
	}
	items := make([]any, 0, len(resp.Tweets))
	for _, t := range resp.Tweets {
		if !publishableTweet(t, userID) { // own original top-level posts, matching outbound federation
			continue
		}
		items = append(items, g.buildCreateNote(userID, t))
	}

	// totalItems must match the collection we actually serve: the outbox inlines
	// only publishable (original top-level) posts, so its count is len(items). The
	// user's raw TweetsCount includes replies/retweets that are filtered out here,
	// so reporting it produced "Posts: N" with an empty/short Posts tab.
	writeJSON(w, contentTypeAP, orderedCollection{
		Context:      asContext,
		ID:           id,
		Type:         "OrderedCollection",
		TotalItems:   len(items),
		OrderedItems: items,
	})
}

// serveFollowing renders who the user follows (PUBLIC_GET_FOLLOWINGS): fediverse
// follows resolve to their actor URL, Warpnet follows to their gateway actor.
func (g *gateway) serveFollowing(w http.ResponseWriter, user string) {
	id := g.actorID(user) + "/following"
	if g.req == nil {
		g.serveEmptyCollection(w, id)
		return
	}
	bt, err := g.req.requestUser(user, routeGetFollowings, getFollowersEvent{UserId: user})
	if err != nil {
		log.Warnf("following: fetch %s: %v", user, err)
		g.serveEmptyCollection(w, id)
		return
	}
	var resp followingsResponse
	if jerr := json.Unmarshal(bt, &resp); jerr != nil {
		g.serveEmptyCollection(w, id)
		return
	}
	items := make([]any, 0, len(resp.Followings))
	for _, fid := range resp.Followings {
		if !isBridgedUserID(fid) {
			items = append(items, g.actorID(fid)) // a Warpnet user we serve ourselves
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), resolveFollowerTimeout)
		actorURL, rerr := g.resolveActorID(ctx, fid)
		cancel()
		if rerr != nil {
			log.Warnf("following: resolving %s: %v", fid, rerr)
			continue
		}
		items = append(items, actorURL)
	}
	writeJSON(w, contentTypeAP, orderedCollection{
		Context:      asContext,
		ID:           id,
		Type:         "OrderedCollection",
		TotalItems:   len(items),
		OrderedItems: items,
	})
}

// serveReplies renders the replies to a Note as an OrderedCollection of Notes
// for thread context. Warpnet folded replies into the tweets API, so the direct
// replies come from PUBLIC_GET_TWEETS with the note as parent_id. Only
// Warpnet-authored replies are inlined; fediverse replies (ap: ids) are omitted
// — Mastodon has its own.
func (g *gateway) serveReplies(w http.ResponseWriter, user, tweetID string) {
	id := g.actorID(user) + pathStatuses + tweetID + "/replies"
	if g.req == nil {
		g.serveEmptyCollection(w, id)
		return
	}
	bt, err := g.req.requestUser(user, routeGetTweets, getAllTweetsEvent{UserId: user, ParentId: tweetID})
	if err != nil {
		log.Warnf("replies: fetch %s/%s: %v", user, tweetID, err)
		g.serveEmptyCollection(w, id)
		return
	}
	var resp tweetsResponse
	if jerr := json.Unmarshal(bt, &resp); jerr != nil {
		g.serveEmptyCollection(w, id)
		return
	}
	parentURL := g.actorID(user) + pathStatuses + tweetID
	items := make([]any, 0, len(resp.Tweets))
	for _, t := range resp.Tweets {
		if t.Id == "" || isBridgedUserID(t.UserId) {
			continue // skip fediverse-authored replies; Mastodon already has them
		}
		n := g.buildNote(t.UserId, t)
		n.InReplyTo = parentURL // these are direct replies to (user, tweetID)
		items = append(items, n)
	}
	writeJSON(w, contentTypeAP, orderedCollection{
		Context:      asContext,
		ID:           id,
		Type:         "OrderedCollection",
		TotalItems:   len(items),
		OrderedItems: items,
	})
}

func (g *gateway) handleNodeInfoLinks(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, "application/json", nodeInfoLinks{Links: []nodeInfoLink{{
		Rel:  "http://nodeinfo.diaspora.software/ns/schema/2.0",
		Href: g.baseURL() + "/nodeinfo/2.0",
	}}})
}

func (g *gateway) handleNodeInfo(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, "application/json", map[string]any{
		"version":           "2.0",
		"software":          map[string]any{"name": "warpnet-fediverse-gateway", "version": gatewayVersion},
		"protocols":         []string{"activitypub"},
		"usage":             map[string]any{"users": map[string]any{"total": 1}},
		"openRegistrations": false,
		"services":          map[string]any{"inbound": []any{}, "outbound": []any{}},
		"metadata":          map[string]any{},
	})
}

func writeJSON(w http.ResponseWriter, contentType string, v any) {
	bt, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "marshal", http.StatusInternalServerError)
		return
	}
	w.Header().Set(headerContentType, contentType)
	_, _ = w.Write(bt)
}

// validateRemoteURL is the SSRF guard for dereferencing attacker-supplied
// actor/key URLs: it requires https, a host, and rejects localhost plus literal
// IPs in loopback/private/link-local/unspecified ranges.
// TODO(prod): also guard hostnames that resolve into those ranges (DNS
// rebinding) at dial time before exposing the gateway publicly.
func validateRemoteURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse url %q: %w", raw, err)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("url %q: %w", raw, errInsecureURL)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("url %q has no host: %w", raw, errBlockedHost)
	}
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return fmt.Errorf("url %q targets localhost: %w", raw, errBlockedHost)
	}
	if addr, perr := netip.ParseAddr(host); perr == nil && isBlockedIP(addr) {
		return fmt.Errorf("url %q targets a disallowed address: %w", raw, errBlockedHost)
	}
	return nil
}

// isSelfHost reports whether host is this gateway's own public hostname.
func (g *gateway) isSelfHost(host string) bool {
	return g.host != "" && strings.EqualFold(host, g.host)
}

// isSelfURL reports whether raw points back at this gateway.
func (g *gateway) isSelfURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && g.isSelfHost(u.Hostname())
}

// checkRemoteURL guards every outbound federation fetch/delivery. Besides the
// SSRF checks it refuses URLs pointing back at this gateway: our own users are
// served from Warpnet, so dereferencing ourselves over ActivityPub would loop a
// local user back in as a foreign Mastodon account (and let a peer bounce our
// own activities back into Warpnet through the inbox).
func (g *gateway) checkRemoteURL(raw string) error {
	if g.isSelfURL(raw) {
		return fmt.Errorf("url %q: %w", raw, errSelfTarget)
	}
	if g.allowPrivateTargets {
		return nil
	}
	return validateRemoteURL(raw)
}

// isBlockedIP reports whether ip is in a range outbound federation must never
// reach (loopback, private, link-local, multicast, unspecified).
func isBlockedIP(ip netip.Addr) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast()
}

// newSafeClient builds the HTTP client for outbound federation, hardened against
// SSRF on attacker-supplied actor/key URLs: CheckRedirect re-applies the URL
// guard to every redirect hop (and caps the chain), and the dialer's Control
// validates the *resolved* IP at connect time, closing DNS-rebinding that the
// hostname checks can't see. Signatures aren't re-applied across redirects, so a
// redirected signed fetch fails at the target — failing closed.
func newSafeClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	dialer.Control = func(_, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return err
		}
		if ip, perr := netip.ParseAddr(host); perr == nil && isBlockedIP(ip) {
			return fmt.Errorf("dial %s: %w", address, errBlockedHost)
		}
		return nil
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = dialer.DialContext
	// A federation gateway talks to a handful of instances but fans many fetches
	// at each (a thread pulls the note, its stats, every reply and their authors).
	// The stdlib default keeps only 2 idle connections per host, so concurrent
	// fetches force fresh TLS handshakes every batch; pool enough to reuse them.
	tr.MaxIdleConnsPerHost = 32
	return &http.Client{
		Timeout:   timeout,
		Transport: tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return errTooManyRedirects
			}
			return validateRemoteURL(req.URL.String())
		},
	}
}

// fetchActor dereferences a remote actor document, signing the GET so it
// works against instances running in authorized-fetch / secure mode.
// retryableStatus reports whether an HTTP status warrants a retry: 429 and 5xx
// are transient (peer overloaded / temporarily unavailable); 4xx are not.
func retryableStatus(code int) bool {
	return code == http.StatusTooManyRequests || code >= 500
}

// sendRetry issues the request built by newReq and reads its response, retrying
// transient failures (network errors, 429, 5xx) via the gateway retrier.
// newReq must build a fresh request each call (the body is consumed per
// attempt). A non-retryable status returns nil error with the status/body so
// the caller can render its own message; an exhausted/transport error is
// returned as err. A success body larger than limit is an error, never a
// silent truncation — half an image is worse than none.
func (g *gateway) sendRetry(ctx context.Context, limit int64, newReq func() (*http.Request, error)) (status int, body []byte, header http.Header, err error) {
	do := func() error {
		req, rerr := newReq()
		if rerr != nil {
			return fmt.Errorf("%w: %w", rerr, retrier.ErrStopTrying)
		}
		if req.Header.Get("User-Agent") == "" {
			req.Header.Set("User-Agent", userAgent)
		}
		resp, rerr := g.client.Do(req) //nolint:gosec // SSRF-guarded by validateRemoteURL + safe client
		if rerr != nil {
			return rerr // network error: retry
		}
		defer func() { _ = resp.Body.Close() }()
		status, header = resp.StatusCode, resp.Header
		body, rerr = io.ReadAll(io.LimitReader(resp.Body, limit+1))
		if rerr != nil {
			return rerr // truncated/transient read: retry
		}
		if status >= 300 && retryableStatus(status) {
			return fmt.Errorf("status %d: %w", status, errRemoteStatus)
		}
		if status < 300 && int64(len(body)) > limit {
			return fmt.Errorf("%w: %w", errBodyTooLarge, retrier.ErrStopTrying)
		}
		return nil
	}
	if g.retrier == nil {
		return status, body, header, do()
	}
	return status, body, header, g.retrier.Try(ctx, do)
}

// signedGet issues a signed GET, memoizing the final response in getCache for a
// 1s window keyed by accept+URL. The body is returned to each caller for its own
// unmarshal, so the shared cache never hands out a mutable decoded document.
func (g *gateway) signedGet(ctx context.Context, rawURL, accept string) (int, []byte, error) {
	key := accept + " " + rawURL
	if g.getCache != nil {
		if c, ok := g.getCache.Get(key); ok {
			logFetch(ctx, "GET", rawURL, c.status, 0, true)
			return c.status, c.body, nil
		}
	}
	v, err, _ := g.sf.Do(key, func() (any, error) {
		// A concurrent flight may have populated the cache while we queued.
		if g.getCache != nil {
			if c, ok := g.getCache.Get(key); ok {
				logFetch(ctx, "GET", rawURL, c.status, 0, true)
				return c, nil
			}
		}
		start := time.Now()
		status, bt, _, ferr := g.sendRetry(ctx, maxBodyBytes, func() (*http.Request, error) {
			req, rerr := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
			if rerr != nil {
				return nil, rerr
			}
			req.Header.Set("Accept", accept)
			return req, g.signGet(req)
		})
		if ferr != nil {
			logFetch(ctx, "GET", rawURL, 0, time.Since(start), false)
			return cachedGet{}, ferr
		}
		logFetch(ctx, "GET", rawURL, status, time.Since(start), false)
		if g.getCache != nil {
			g.getCache.Add(key, cachedGet{status: status, body: bt})
		}
		return cachedGet{status: status, body: bt}, nil
	})
	if err != nil {
		return 0, nil, err
	}
	c := v.(cachedGet)
	return c.status, c.body, nil
}

func (g *gateway) fetchActor(ctx context.Context, actorURL string) (map[string]any, error) {
	if err := g.checkRemoteURL(actorURL); err != nil {
		return nil, err
	}
	// G704: dereferencing remote actor URLs is intrinsic to ActivityPub
	// federation; validateRemoteURL enforces https, full SSRF hardening is a
	// documented production TODO.
	status, bt, err := g.signedGet(ctx, actorURL, contentTypeAP)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", actorURL, err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("fetch %s: status %d: %w", actorURL, status, errRemoteStatus)
	}
	var m map[string]any
	if err := json.Unmarshal(bt, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// fetchKey resolves a keyId (actorURL#main-key) to its RSA public key.
func (g *gateway) fetchKey(ctx context.Context, keyID string) (*rsa.PublicKey, error) {
	actorURL := strings.SplitN(keyID, "#", 2)[0]
	m, err := g.fetchActor(ctx, actorURL)
	if err != nil {
		return nil, err
	}
	pk, ok := m["publicKey"].(map[string]any)
	if !ok {
		// Some servers send publicKey as an array; take the first object.
		if arr, isArr := m["publicKey"].([]any); isArr && len(arr) > 0 {
			pk, ok = arr[0].(map[string]any)
		}
	}
	if !ok || pk == nil {
		return nil, fmt.Errorf("actor %s has no publicKey: %w", actorURL, errActorMalformed)
	}
	pemStr, _ := pk["publicKeyPem"].(string)
	if pemStr == "" {
		return nil, fmt.Errorf("actor %s has no publicKeyPem: %w", actorURL, errActorMalformed)
	}
	return parseRSAPublicKeyPEM(pemStr)
}

// remoteInbox returns the best inbox URL for a remote actor (sharedInbox if
// advertised, otherwise the personal inbox).
func (g *gateway) remoteInbox(ctx context.Context, actorURL string) (string, error) {
	m, err := g.fetchActor(ctx, actorURL)
	if err != nil {
		return "", err
	}
	if ep, ok := m["endpoints"].(map[string]any); ok {
		if si, ok := ep["sharedInbox"].(string); ok && si != "" {
			return si, nil
		}
	}
	if inbox, ok := m["inbox"].(string); ok && inbox != "" {
		return inbox, nil
	}
	return "", fmt.Errorf("actor %s has no inbox: %w", actorURL, errActorMalformed)
}

// apGetJSON fetches and decodes an arbitrary AP/JRD JSON document, reusing the
// SSRF guard. Actor documents go through fetchActor (it signs for
// authorized-fetch instances); this is for WebFinger and AP collections.
func (g *gateway) apGetJSON(ctx context.Context, rawURL, accept string) (map[string]any, error) {
	if err := g.checkRemoteURL(rawURL); err != nil {
		return nil, err
	}
	status, bt, err := g.signedGet(ctx, rawURL, accept)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", rawURL, err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("fetch %s: status %d: %w", rawURL, status, errRemoteStatus)
	}
	var m map[string]any
	if err := json.Unmarshal(bt, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// apGetArray is apGetJSON for endpoints that return a top-level JSON array (the
// Mastodon REST list endpoints, e.g. an account's statuses).
func (g *gateway) apGetArray(ctx context.Context, rawURL, accept string) ([]any, error) {
	if err := g.checkRemoteURL(rawURL); err != nil {
		return nil, err
	}
	status, bt, err := g.signedGet(ctx, rawURL, accept)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", rawURL, err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("fetch %s: status %d: %w", rawURL, status, errRemoteStatus)
	}
	var a []any
	if err := json.Unmarshal(bt, &a); err != nil {
		return nil, err
	}
	return a, nil
}

// fetchMedia downloads a remote media URL (SSRF-guarded), returning its
// content type and bytes.
func (g *gateway) fetchMedia(ctx context.Context, rawURL string) (string, []byte, error) {
	if err := g.checkRemoteURL(rawURL); err != nil {
		return "", nil, err
	}
	start := time.Now()
	status, bt, header, err := g.sendRetry(ctx, maxMediaBytes, func() (*http.Request, error) {
		req, rerr := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if rerr != nil {
			return nil, rerr
		}
		return req, g.signGet(req)
	})
	logFetch(ctx, "GET media", rawURL, status, time.Since(start), false)
	if err != nil {
		return "", nil, fmt.Errorf("media %s: %w", rawURL, err)
	}
	if status != http.StatusOK {
		return "", nil, fmt.Errorf("media %s: status %d: %w", rawURL, status, errRemoteStatus)
	}
	return header.Get(headerContentType), bt, nil
}

// postSigned delivers a signed POST of doc to target, as localUser.
func (g *gateway) postSigned(ctx context.Context, localUser, target string, doc any) error {
	if err := g.checkRemoteURL(target); err != nil {
		return err
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	// G704: target is a federation peer inbox; validateRemoteURL (above) enforces
	// https and newSafeClient guards redirects + the resolved dial IP. A retried
	// delivery re-signs (fresh Date) and re-sends the same activity id, which the
	// peer deduplicates — safe to retry on transient (429/5xx) failures.
	status, respBody, _, err := g.sendRetry(ctx, maxBodyBytes, func() (*http.Request, error) {
		req, rerr := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
		if rerr != nil {
			return nil, rerr
		}
		req.Header.Set(headerContentType, contentTypeAP)
		return req, signRequest(req, g.keyID(localUser), g.key, body)
	})
	// Include a snippet of the peer's response: Mastodon explains inbox
	// rejections in the body (e.g. signature/verification, blocked domain),
	// which is what we need to diagnose a failed delivery — keep it even when
	// retries were exhausted on a 429/5xx (err != nil but a body was read).
	snippet := strings.TrimSpace(string(respBody))
	if len(snippet) > 300 {
		snippet = snippet[:300]
	}
	if err != nil {
		if snippet != "" {
			return fmt.Errorf("deliver to %s: %w: %s", target, err, snippet)
		}
		return fmt.Errorf("deliver to %s: %w", target, err)
	}
	if status >= 300 {
		return fmt.Errorf("deliver to %s: status %d: %w: %s", target, status, errRemoteStatus, snippet)
	}
	return nil
}

func randomToken() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// userFromActorURL extracts the username from one of our own actor URLs
// (https://host/users/NAME).
func userFromActorURL(u string) string {
	_, rest, ok := strings.Cut(u, pathUsers)
	if !ok {
		return ""
	}
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		rest = rest[:i]
	}
	return rest
}
