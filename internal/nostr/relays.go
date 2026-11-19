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
	"context"
	"encoding/hex"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"fiatjaf.com/nostr"
	"fiatjaf.com/nostr/nip19"
	"github.com/Warp-net/activity-pub-gw/internal/netguard"
	"github.com/Warp-net/activity-pub-gw/internal/warp"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

// relayPool is the only code that touches the Nostr library: the bridge speaks
// in nostrEvent/nostrFilter, so a breaking change upstream stays in this file.

type nostrEvent struct {
	ID        string
	PubKey    string
	CreatedAt int64
	Kind      int
	Tags      [][]string
	Content   string
	Sig       string
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

// Relays is the relay surface the bridge needs.
type Relays interface {
	// fetch returns the stored events matching f, newest first, deduplicated
	// across relays.
	fetch(ctx context.Context, urls []string, f nostrFilter) []nostrEvent
	// count asks relays for a NIP-45 count; ok is false when none answered.
	count(ctx context.Context, urls []string, f nostrFilter) (int64, bool)
	// subscribe streams new events matching f until ctx ends, reconnecting.
	subscribe(ctx context.Context, urls []string, f nostrFilter, on func(nostrEvent))
	// publish sends a signed event and reports how many relays accepted it.
	publish(ctx context.Context, urls []string, ev nostrEvent) int
}

const (
	relayDialTimeout = 4 * time.Second
	relayEOSETimeout = 5 * time.Second
	// relayGrace is how long the rest get once half the relays have answered
	// in full: the relays are redundant, so the slowest is not worth waiting
	// for, but the first to answer is often an index relay with nothing to say.
	relayGrace        = time.Second
	relayDownTTL      = time.Minute
	relayMaxPerFetch  = 500
	relayRetryBackoff = 30 * time.Second
	relayIdleTTL      = 10 * time.Minute
)

type RelayPool struct {
	client *http.Client
	// AllowInsecure lets tests dial a plain ws:// relay on loopback.
	AllowInsecure bool

	mu     sync.Mutex
	conns  map[string]*nostr.Relay
	used   map[string]time.Time
	active map[string]int
	down   *expirable.LRU[string, struct{}]
}

func NewRelayPool(client *http.Client) *RelayPool {
	return &RelayPool{
		client: client,
		conns:  map[string]*nostr.Relay{},
		used:   map[string]time.Time{},
		active: map[string]int{},
		down:   expirable.NewLRU[string, struct{}](1024, nil, relayDownTTL),
	}
}

// validRelayURL accepts only wss:// relays on public hosts: author relay lists
// are user-supplied, so they get the same SSRF guard as ActivityPub fetches.
func (p *RelayPool) validRelayURL(raw string) bool {
	if p.AllowInsecure && strings.HasPrefix(raw, "ws://") {
		return true
	}
	rest, ok := strings.CutPrefix(raw, "wss://")
	return ok && netguard.ValidateURL("https://"+rest) == nil
}

// relay hands out a connection to raw under its normalized url, which the
// caller hands back to release; connections nobody used for relayIdleTTL are
// closed.
func (p *RelayPool) relay(ctx context.Context, raw string) (*nostr.Relay, string, bool) {
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
		if ctx.Err() == nil { // a dial cut short by the caller says nothing about the relay
			p.down.Add(url, struct{}{})
		}
		warp.NetLog("").Debugf("nostr: relay %s: %v", url, err)
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

func (p *RelayPool) release(url string) {
	p.mu.Lock()
	p.active[url]--
	p.used[url] = time.Now()
	p.mu.Unlock()
}

func ignoreNotice(*nostr.Relay, string) {}

func (p *RelayPool) sweep() {
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

func (p *RelayPool) fetch(ctx context.Context, urls []string, f nostrFilter) []nostrEvent {
	filter, ok := toLibFilter(f)
	if !ok {
		return nil
	}
	urls = dedupeStrings(urls)
	set := &eventSet{seen: map[string]bool{}}
	fctx, cancel := context.WithTimeout(ctx, relayEOSETimeout)
	defer cancel()
	answered := make(chan struct{}, len(urls))
	var wg sync.WaitGroup
	for _, u := range urls {
		wg.Add(1)
		go p.fetchFrom(fctx, &wg, u, filter, set, answered)
	}
	awaitRelays(fctx, cancel, &wg, answered, (len(urls)+1)/2)
	set.mu.Lock()
	defer set.mu.Unlock()
	slices.SortFunc(set.out, newestFirst)
	return append([]nostrEvent{}, set.out...)
}

// awaitRelays returns once every relay is done, or relayGrace after quorum of
// them answered in full, cancelling the stragglers.
func awaitRelays(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, answered chan struct{}, quorum int) {
	all := make(chan struct{})
	go closeWhenDone(wg, all)
	for n := 0; n < quorum; n++ {
		select {
		case <-all:
			return
		case <-ctx.Done():
			cancel()
			<-all
			return
		case <-answered:
		}
	}
	select {
	case <-all:
		return
	case <-ctx.Done():
	case <-time.After(relayGrace):
	}
	cancel()
	<-all
}

func closeWhenDone(wg *sync.WaitGroup, ch chan struct{}) {
	wg.Wait()
	close(ch)
}

// fetchFrom reads one relay's stored events into set until EOSE, then
// reports the relay as answered.
func (p *RelayPool) fetchFrom(ctx context.Context, wg *sync.WaitGroup, raw string, filter nostr.Filter, set *eventSet, answered chan<- struct{}) {
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
			answered <- struct{}{}
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

func (p *RelayPool) count(ctx context.Context, urls []string, f nostrFilter) (int64, bool) {
	filter, ok := toLibFilter(f)
	if !ok {
		return 0, false
	}
	urls = dedupeStrings(urls)
	res := &countResult{}
	cctx, cancel := context.WithTimeout(ctx, relayEOSETimeout)
	defer cancel()
	var wg sync.WaitGroup
	for _, u := range urls {
		wg.Add(1)
		go p.countAt(cctx, &wg, u, filter, res)
	}
	wg.Wait()
	res.mu.Lock()
	defer res.mu.Unlock()
	return res.best, res.answered
}

// countAt asks one relay; counts take the largest answer, so every relay is
// waited for within the budget.
func (p *RelayPool) countAt(ctx context.Context, wg *sync.WaitGroup, raw string, filter nostr.Filter, res *countResult) {
	defer wg.Done()
	r, url, ok := p.relay(ctx, raw)
	if !ok {
		return
	}
	defer p.release(url)
	n, _, err := r.Count(ctx, filter, nostr.SubscriptionOptions{})
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

func (p *RelayPool) subscribe(ctx context.Context, urls []string, f nostrFilter, on func(nostrEvent)) {
	d := &liveDelivery{seen: expirable.NewLRU[string, struct{}](4096, nil, time.Hour), on: on}
	var wg sync.WaitGroup
	for _, u := range dedupeStrings(urls) {
		wg.Add(1)
		go p.keepFollowing(ctx, &wg, u, f, d)
	}
	wg.Wait()
}

// keepFollowing re-opens one relay's live subscription until ctx ends.
func (p *RelayPool) keepFollowing(ctx context.Context, wg *sync.WaitGroup, url string, f nostrFilter, d *liveDelivery) {
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
func (p *RelayPool) follow(ctx context.Context, raw string, f nostrFilter, d *liveDelivery) {
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
		Sig:       nostr.HexEncodeToString(ev.Sig[:]),
	}
}

func toLibEvent(ev nostrEvent) (nostr.Event, bool) {
	out := nostr.Event{CreatedAt: nostr.Timestamp(ev.CreatedAt), Kind: nostr.Kind(ev.Kind), Content: ev.Content} //nolint:gosec // kinds are small
	for _, t := range ev.Tags {
		out.Tags = append(out.Tags, nostr.Tag(t))
	}
	if ev.ID != "" {
		id, err := nostr.IDFromHex(ev.ID)
		if err != nil {
			return nostr.Event{}, false
		}
		out.ID = id
	}
	if ev.PubKey != "" {
		pk, err := nostr.PubKeyFromHex(ev.PubKey)
		if err != nil {
			return nostr.Event{}, false
		}
		out.PubKey = pk
	}
	if ev.Sig != "" {
		sig, err := hex.DecodeString(ev.Sig)
		if err != nil || len(sig) != len(out.Sig) {
			return nostr.Event{}, false
		}
		copy(out.Sig[:], sig)
	}
	return out, true
}

// signEvent stamps ev with the key's pubkey, id and BIP-340 signature.
func signEvent(sk [32]byte, ev nostrEvent) (nostrEvent, bool) {
	lib, ok := toLibEvent(ev)
	if !ok || lib.Sign(sk) != nil {
		return nostrEvent{}, false
	}
	return fromLibEvent(lib), true
}

// publicKeyHex is the x-only public key of sk.
func publicKeyHex(sk [32]byte) string {
	return nostr.GetPublicKey(sk).Hex()
}

// eventJSON is the canonical JSON of a signed event, as a repost embeds it.
func eventJSON(ev nostrEvent) string {
	lib, ok := toLibEvent(ev)
	if !ok {
		return ""
	}
	return lib.String()
}

// encodeNevent renders an event reference for a quote's text.
func encodeNevent(id, authorHex string) string {
	eid, err := nostr.IDFromHex(id)
	if err != nil {
		return ""
	}
	pk, _ := nostr.PubKeyFromHex(authorHex)
	return nip19.EncodeNevent(eid, nil, pk)
}

func (p *RelayPool) publish(ctx context.Context, urls []string, ev nostrEvent) int {
	lib, ok := toLibEvent(ev)
	if !ok {
		return 0
	}
	urls = dedupeStrings(urls)
	res := &countResult{}
	var wg sync.WaitGroup
	for _, u := range urls {
		wg.Add(1)
		go p.publishTo(ctx, &wg, u, lib, res)
	}
	wg.Wait()
	return int(res.best)
}

func (p *RelayPool) publishTo(ctx context.Context, wg *sync.WaitGroup, raw string, ev nostr.Event, res *countResult) {
	defer wg.Done()
	r, url, ok := p.relay(ctx, raw)
	if !ok {
		return
	}
	defer p.release(url)
	pctx, cancel := context.WithTimeout(ctx, relayEOSETimeout)
	defer cancel()
	if err := r.Publish(pctx, ev); err != nil {
		warp.NetLog("").Debugf("nostr: publish %s to %s: %v", ev.ID.Hex(), url, err)
		return
	}
	res.mu.Lock()
	res.best++
	res.mu.Unlock()
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
