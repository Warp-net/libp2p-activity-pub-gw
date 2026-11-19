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

package nostrtest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"fiatjaf.com/nostr"
	"github.com/coder/websocket"
)

// Relay is an in-process NIP-01 relay: it answers REQ from its stored
// events, keeps subscriptions open for events added later, and answers COUNT
// only when counting is enabled.
type Relay struct {
	t        *testing.T
	srv      *httptest.Server
	Counting bool
	// Stalled relays accept requests and never answer them.
	Stalled bool

	mu     sync.Mutex
	events []nostr.Event
	subs   map[*fakeSub]struct{}
	reqs   []string
}

type fakeSub struct {
	id      string
	filters []nostr.Filter
	send    func(any)
}

func NewRelay(t *testing.T) *Relay {
	t.Helper()
	r := &Relay{t: t, subs: map[*fakeSub]struct{}{}}
	r.srv = httptest.NewServer(http.HandlerFunc(r.serve))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *Relay) URL() string { return "ws" + strings.TrimPrefix(r.srv.URL, "http") }

func (r *Relay) Add(evs ...nostr.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ev := range evs {
		r.events = append(r.events, ev)
		for s := range r.subs {
			for _, f := range s.filters {
				if f.Matches(ev) {
					s.send([]any{"EVENT", s.id, ev})
					break
				}
			}
		}
	}
}

func (r *Relay) Saw(substr string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, req := range r.reqs {
		if strings.Contains(req, substr) {
			return true
		}
	}
	return false
}

// LiveFor reports whether a subscription for new events (one with a since)
// follows the author pk.
func (r *Relay) LiveFor(pk string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for s := range r.subs {
		for _, f := range s.filters {
			for _, a := range f.Authors {
				if f.Since > 0 && a.Hex() == pk {
					return true
				}
			}
		}
	}
	return false
}

// fakeConn serializes writes to one client.
type fakeConn struct {
	ctx context.Context
	c   *websocket.Conn
	mu  sync.Mutex
}

func (fc *fakeConn) send(v any) {
	bt, _ := json.Marshal(v)
	fc.mu.Lock()
	defer fc.mu.Unlock()
	_ = fc.c.Write(fc.ctx, websocket.MessageText, bt)
}

func (r *Relay) dropSubs(mine map[string]*fakeSub) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range mine {
		delete(r.subs, s)
	}
}

func libNewestFirst(a, b nostr.Event) int { return int(b.CreatedAt - a.CreatedAt) }

func (r *Relay) serve(w http.ResponseWriter, req *http.Request) {
	c, err := websocket.Accept(w, req, nil)
	if err != nil {
		return
	}
	ctx := req.Context()
	conn := &fakeConn{ctx: ctx, c: c}
	send := conn.send
	mine := map[string]*fakeSub{}
	defer r.dropSubs(mine)
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		var msg []json.RawMessage
		if json.Unmarshal(data, &msg) != nil || len(msg) < 2 {
			continue
		}
		var typ, id string
		_ = json.Unmarshal(msg[0], &typ)
		_ = json.Unmarshal(msg[1], &id)
		var filters []nostr.Filter
		for _, raw := range msg[2:] {
			var f nostr.Filter
			if f.UnmarshalJSON(raw) == nil {
				filters = append(filters, f)
			}
		}
		r.mu.Lock()
		r.reqs = append(r.reqs, string(data))
		r.mu.Unlock()
		if r.Stalled {
			continue
		}
		switch typ {
		case "EVENT":
			var ev nostr.Event
			if ev.UnmarshalJSON(msg[1]) != nil || !ev.VerifySignature() {
				send([]any{"OK", "", false, "invalid: bad signature"})
				continue
			}
			r.Add(ev)
			send([]any{"OK", ev.ID.Hex(), true, ""})
		case "REQ":
			for _, ev := range r.stored(filters) {
				send([]any{"EVENT", id, ev})
			}
			send([]any{"EOSE", id})
			s := &fakeSub{id: id, filters: filters, send: send}
			r.mu.Lock()
			r.subs[s] = struct{}{}
			r.mu.Unlock()
			mine[id] = s
		case "COUNT":
			if !r.Counting {
				send([]any{"CLOSED", id, "unsupported: no NIP-45"})
				continue
			}
			send([]any{"COUNT", id, map[string]int{"count": len(r.stored(filters))}})
		case "CLOSE":
			if s, ok := mine[id]; ok {
				r.mu.Lock()
				delete(r.subs, s)
				r.mu.Unlock()
				delete(mine, id)
			}
		}
	}
}

func (r *Relay) stored(filters []nostr.Filter) []nostr.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []nostr.Event
	for _, f := range filters {
		var matched []nostr.Event
		for _, ev := range r.events {
			if f.Matches(ev) {
				matched = append(matched, ev)
			}
		}
		slices.SortFunc(matched, libNewestFirst)
		if f.Limit > 0 && len(matched) > f.Limit {
			matched = matched[:f.Limit]
		}
		out = append(out, matched...)
	}
	return out
}

// Key is a test author.
type Key struct {
	sk nostr.SecretKey
	PK string
}

func NewKey() Key {
	sk := nostr.Generate()
	return Key{sk: sk, PK: sk.Public().Hex()}
}

func (k Key) Sign(t *testing.T, kind int, content string, at int64, tags ...[]string) nostr.Event {
	t.Helper()
	ev := nostr.Event{Kind: nostr.Kind(kind), Content: content, CreatedAt: nostr.Timestamp(at)} //nolint:gosec // test kinds
	for _, tag := range tags {
		ev.Tags = append(ev.Tags, nostr.Tag(tag))
	}
	if err := ev.Sign(k.sk); err != nil {
		t.Fatalf("sign: %v", err)
	}
	return ev
}

// Published returns the events of kind the relay was sent by pk.
func (r *Relay) Published(pk string, kind int) []nostr.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []nostr.Event
	for _, ev := range r.events {
		if ev.PubKey.Hex() == pk && int(ev.Kind) == kind {
			out = append(out, ev)
		}
	}
	return out
}

func WaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
