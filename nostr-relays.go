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

// relayPool is the only code that touches the Nostr library: the bridge speaks
// in nostrEvent/nostrFilter, so a breaking change upstream stays in this file.

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"fiatjaf.com/nostr"
	"fiatjaf.com/nostr/nip19"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

type nostrEvent struct {
	ID        string
	PubKey    string
	CreatedAt int64
	Kind      int
	Tags      [][]string
	Content   string
}

type nostrFilter struct {
	IDs     []string
	Authors []string
	Kinds   []int
	Tags    map[string][]string
	Since   int64
	Until   int64
	Limit   int
}

// nostrRelays is the relay surface the bridge needs.
type nostrRelays interface {
	// fetch returns the stored events matching f, newest first, deduplicated
	// across relays.
	fetch(ctx context.Context, urls []string, f nostrFilter) []nostrEvent
	// count asks relays for a NIP-45 count; ok is false when none answered.
	count(ctx context.Context, urls []string, f nostrFilter) (int64, bool)
	// subscribe streams new events matching f until ctx ends, reconnecting.
	subscribe(ctx context.Context, urls []string, f nostrFilter, on func(nostrEvent))
}

const (
	relayDialTimeout  = 7 * time.Second
	relayEOSETimeout  = 6 * time.Second
	relayDownTTL      = time.Minute
	relayMaxPerFetch  = 500
	relayRetryBackoff = 30 * time.Second
	relayIdleTTL      = 10 * time.Minute
)

type relayPool struct {
	client *http.Client
	// allowInsecure lets tests dial a plain ws:// relay on loopback.
	allowInsecure bool

	mu     sync.Mutex
	conns  map[string]*nostr.Relay
	used   map[string]time.Time
	active map[string]int
	down   *expirable.LRU[string, struct{}]
}

func newRelayPool(client *http.Client) *relayPool {
	return &relayPool{
		client: client,
		conns:  map[string]*nostr.Relay{},
		used:   map[string]time.Time{},
		active: map[string]int{},
		down:   expirable.NewLRU[string, struct{}](1024, nil, relayDownTTL),
	}
}

// validRelayURL accepts only wss:// relays on public hosts: author relay lists
// are user-supplied, so they get the same SSRF guard as ActivityPub fetches.
func (p *relayPool) validRelayURL(raw string) bool {
	if p.allowInsecure && strings.HasPrefix(raw, "ws://") {
		return true
	}
	rest, ok := strings.CutPrefix(raw, "wss://")
	return ok && validateRemoteURL("https://"+rest) == nil
}

// relay hands out a connection to raw under its normalized url, which the
// caller hands back to release; connections nobody used for relayIdleTTL are
// closed.
func (p *relayPool) relay(ctx context.Context, raw string) (*nostr.Relay, string, bool) {
	url := nostr.NormalizeURL(raw)
	if !p.validRelayURL(url) || p.down.Contains(url) {
		return nil, "", false
	}
	p.mu.Lock()
	p.sweep()
	if cur, ok := p.conns[url]; ok && cur.IsConnected() {
		p.active[url]++
		p.mu.Unlock()
		return cur, url, true
	}
	p.mu.Unlock()

	r := nostr.NewRelay(context.Background(), url, nostr.RelayOptions{NoticeHandler: ignoreNotice})
	dctx, cancel := context.WithTimeout(ctx, relayDialTimeout)
	err := r.ConnectWithClient(dctx, p.client)
	cancel()
	if err != nil {
		p.down.Add(url, struct{}{})
		netLog("").Debugf("nostr: relay %s: %v", url, err)
		return nil, "", false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if cur, raced := p.conns[url]; raced && cur.IsConnected() {
		_ = r.Close()
		r = cur
	}
	p.conns[url] = r
	p.active[url]++
	return r, url, true
}

func (p *relayPool) release(url string) {
	p.mu.Lock()
	p.active[url]--
	p.used[url] = time.Now()
	p.mu.Unlock()
}

func ignoreNotice(*nostr.Relay, string) {}

func (p *relayPool) sweep() {
	for url, r := range p.conns {
		if p.active[url] == 0 && time.Since(p.used[url]) > relayIdleTTL {
			_ = r.Close()
			delete(p.conns, url)
			delete(p.used, url)
			delete(p.active, url)
		}
	}
}

// eventSet collects events from several relays, deduplicated by id.
type eventSet struct {
	mu   sync.Mutex
	seen map[string]bool
	out  []nostrEvent
}

func (s *eventSet) add(ev nostr.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id := ev.ID.Hex(); !s.seen[id] {
		s.seen[id] = true
		s.out = append(s.out, fromLibEvent(ev))
	}
}

func newestFirst(a, b nostrEvent) int {
	return int(b.CreatedAt - a.CreatedAt)
}

func (p *relayPool) fetch(ctx context.Context, urls []string, f nostrFilter) []nostrEvent {
	filter, ok := toLibFilter(f)
	if !ok {
		return nil
	}
	set := &eventSet{seen: map[string]bool{}}
	var wg sync.WaitGroup
	for _, u := range dedupeStrings(urls) {
		wg.Add(1)
		go p.fetchFrom(ctx, &wg, u, filter, set)
	}
	wg.Wait()
	slices.SortFunc(set.out, newestFirst)
	return set.out
}

// fetchFrom reads one relay's stored events into set until EOSE.
func (p *relayPool) fetchFrom(ctx context.Context, wg *sync.WaitGroup, raw string, filter nostr.Filter, set *eventSet) {
	defer wg.Done()
	r, url, ok := p.relay(ctx, raw)
	if !ok {
		return
	}
	defer p.release(url)
	sub, err := r.Subscribe(ctx, filter, nostr.SubscriptionOptions{MaxWaitForEOSE: relayEOSETimeout})
	if err != nil {
		return
	}
	defer sub.Unsub()
	for n := 0; n < relayMaxPerFetch; n++ {
		select {
		case ev, open := <-sub.Events:
			if !open {
				return
			}
			set.add(ev)
		case <-sub.EndOfStoredEvents:
			return
		case <-sub.ClosedReason:
			return
		case <-sub.Context.Done():
			return
		case <-ctx.Done():
			return
		}
	}
}

// countResult keeps the largest count any relay reported.
type countResult struct {
	mu       sync.Mutex
	best     int64
	answered bool
}

func (p *relayPool) count(ctx context.Context, urls []string, f nostrFilter) (int64, bool) {
	filter, ok := toLibFilter(f)
	if !ok {
		return 0, false
	}
	res := &countResult{}
	var wg sync.WaitGroup
	for _, u := range dedupeStrings(urls) {
		wg.Add(1)
		go p.countAt(ctx, &wg, u, filter, res)
	}
	wg.Wait()
	return res.best, res.answered
}

func (p *relayPool) countAt(ctx context.Context, wg *sync.WaitGroup, raw string, filter nostr.Filter, res *countResult) {
	defer wg.Done()
	r, url, ok := p.relay(ctx, raw)
	if !ok {
		return
	}
	defer p.release(url)
	cctx, cancel := context.WithTimeout(ctx, relayEOSETimeout)
	defer cancel()
	n, _, err := r.Count(cctx, filter, nostr.SubscriptionOptions{})
	if err != nil {
		return
	}
	res.mu.Lock()
	res.answered = true
	res.best = max(res.best, int64(n))
	res.mu.Unlock()
}

// liveDelivery hands each live event to on once, whichever relays carry it.
type liveDelivery struct {
	mu   sync.Mutex
	seen *expirable.LRU[string, struct{}]
	on   func(nostrEvent)
}

func (d *liveDelivery) deliver(ev nostr.Event) {
	id := ev.ID.Hex()
	d.mu.Lock()
	dup := d.seen.Contains(id)
	d.seen.Add(id, struct{}{})
	d.mu.Unlock()
	if !dup {
		d.on(fromLibEvent(ev))
	}
}

func (p *relayPool) subscribe(ctx context.Context, urls []string, f nostrFilter, on func(nostrEvent)) {
	d := &liveDelivery{seen: expirable.NewLRU[string, struct{}](4096, nil, time.Hour), on: on}
	var wg sync.WaitGroup
	for _, u := range dedupeStrings(urls) {
		wg.Add(1)
		go p.keepFollowing(ctx, &wg, u, f, d)
	}
	wg.Wait()
}

// keepFollowing re-opens one relay's live subscription until ctx ends.
func (p *relayPool) keepFollowing(ctx context.Context, wg *sync.WaitGroup, url string, f nostrFilter, d *liveDelivery) {
	defer wg.Done()
	for ctx.Err() == nil {
		p.follow(ctx, url, f, d)
		select {
		case <-ctx.Done():
		case <-time.After(relayRetryBackoff):
		}
	}
}

// follow keeps one live subscription on one relay until it drops or ctx ends.
func (p *relayPool) follow(ctx context.Context, raw string, f nostrFilter, d *liveDelivery) {
	if f.Since == 0 {
		f.Since = time.Now().Unix()
	}
	filter, ok := toLibFilter(f)
	if !ok {
		return
	}
	r, url, ok := p.relay(ctx, raw)
	if !ok {
		return
	}
	defer p.release(url)
	sub, err := r.Subscribe(ctx, filter, nostr.SubscriptionOptions{})
	if err != nil {
		return
	}
	defer sub.Unsub()
	for {
		select {
		case ev, open := <-sub.Events:
			if !open {
				return
			}
			d.deliver(ev)
		case <-sub.ClosedReason:
			return
		case <-sub.Context.Done():
			return
		case <-r.Context().Done():
			return
		case <-ctx.Done():
			return
		}
	}
}

func toLibFilter(f nostrFilter) (nostr.Filter, bool) {
	out := nostr.Filter{Limit: f.Limit, Since: nostr.Timestamp(f.Since), Until: nostr.Timestamp(f.Until)}
	for _, id := range f.IDs {
		v, err := nostr.IDFromHex(id)
		if err != nil {
			return nostr.Filter{}, false
		}
		out.IDs = append(out.IDs, v)
	}
	for _, a := range f.Authors {
		v, err := nostr.PubKeyFromHex(a)
		if err != nil {
			return nostr.Filter{}, false
		}
		out.Authors = append(out.Authors, v)
	}
	for _, k := range f.Kinds {
		out.Kinds = append(out.Kinds, nostr.Kind(k)) //nolint:gosec // kinds are small constants
	}
	if len(f.Tags) > 0 {
		out.Tags = nostr.TagMap(f.Tags)
	}
	return out, true
}

func fromLibEvent(ev nostr.Event) nostrEvent {
	tags := make([][]string, 0, len(ev.Tags))
	for _, t := range ev.Tags {
		tags = append(tags, []string(t))
	}
	return nostrEvent{
		ID:        ev.ID.Hex(),
		PubKey:    ev.PubKey.Hex(),
		CreatedAt: int64(ev.CreatedAt),
		Kind:      int(ev.Kind),
		Tags:      tags,
		Content:   ev.Content,
	}
}

// encodeNpub renders a hex public key as a bech32 npub.
func encodeNpub(pkHex string) (string, bool) {
	pk, err := nostr.PubKeyFromHex(pkHex)
	if err != nil {
		return "", false
	}
	return nip19.EncodeNpub(pk), true
}

// decodeNpub returns the hex public key behind an npub.
func decodeNpub(npub string) (string, bool) {
	prefix, v, err := nip19.Decode(npub)
	if err != nil || prefix != "npub" {
		return "", false
	}
	pk, ok := v.(nostr.PubKey)
	if !ok {
		return "", false
	}
	return pk.Hex(), true
}

func dedupeStrings(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// parseSignedEvent decodes an event embedded in another one (a repost's
// content) and keeps it only when its id and signature check out: the
// reposter could otherwise put any words in the original author's mouth.
func parseSignedEvent(raw string) (nostrEvent, bool) {
	var ev nostr.Event
	if raw == "" || ev.UnmarshalJSON([]byte(raw)) != nil || !ev.CheckID() || !ev.VerifySignature() {
		return nostrEvent{}, false
	}
	return fromLibEvent(ev), true
}
