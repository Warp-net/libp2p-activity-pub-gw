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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/nostr/nostrtest"
	"github.com/Warp-net/bridge-gateway/internal/warp"
)

// fakeFeed serves a user's timeline page, swappable between polls.
type fakeFeed struct {
	mu    sync.Mutex
	page  []warp.Tweet
	users []warp.User
}

func (f *fakeFeed) set(ts ...warp.Tweet) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.page = ts
}

func (f *fakeFeed) Request(string, any) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return json.Marshal(warp.UsersResponse{Users: f.users})
}

func (f *fakeFeed) setUsers(us ...warp.User) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.users = us
}

func (f *fakeFeed) RequestUser(userID, _ string, _ any) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return json.Marshal(warp.TweetsResponse{UserId: userID, Tweets: f.page})
}

func post(id, text string, at time.Time) warp.Tweet {
	return warp.Tweet{Id: id, RootId: id, UserId: aliceULID, Text: text, CreatedAt: at}
}

func TestVanishedOnlyWithinTheCurrentPage(t *testing.T) {
	now := time.Now()
	old, mid, newest := post("old", "", now.Add(-3*time.Hour)), post("mid", "", now.Add(-2*time.Hour)), post("new", "", now.Add(-time.Hour))
	if got := vanished([]warp.Tweet{newest, mid, old}, []warp.Tweet{newest, old}); len(got) != 1 || got[0].Id != "mid" {
		t.Fatalf("vanished = %v, want the deleted middle post", got)
	}
	if got := vanished([]warp.Tweet{mid, old}, []warp.Tweet{newest, mid}); len(got) != 0 {
		t.Fatalf("vanished = %v, a post that scrolled out of the page is not deleted", got)
	}
}

func TestNostrPostPollerPublishesPostsEditsAndDeletes(t *testing.T) {
	fx := newPublishFixture(t)
	alice := fx.pk(t, aliceULID)
	site := "https://alice.example"
	fx.home.users[aliceULID] = warp.User{Id: aliceULID, Username: "Alice", Bio: "hi", AvatarKey: "avatar-key", Website: &site}
	feed := &fakeFeed{}
	now := time.Now()
	existing := post("01EXISTING", "already there", now.Add(-time.Hour))
	feed.set(existing)
	p := newNostrPostPoller(feed, fx.pub, aliceULID)

	p.seed()
	if notes := fx.relay.Published(alice, KindNote); len(notes) != 0 {
		t.Fatal("existing posts are not replayed")
	}
	profiles := fx.relay.Published(alice, KindProfile)
	if len(profiles) != 1 || !tagged(profiles[0], "proxy", "https://gw.example/users/"+aliceULID, "web") {
		t.Fatalf("kind 0 = %v", profiles)
	}
	var prof map[string]string
	_ = json.Unmarshal([]byte(profiles[0].Content), &prof)
	if prof["name"] != "Alice" || prof["about"] != "hi" || prof["website"] != site ||
		prof["nip05"] != strings.ToLower(aliceULID)+"@gw.example" || !strings.HasPrefix(prof["picture"], "https://gw.example/media/") {
		t.Fatalf("profile = %v", prof)
	}
	if len(fx.relay.Published(alice, kindRelayList)) != 1 {
		t.Fatal("the relay list goes out with the profile")
	}

	fresh := post("01FRESH", "new post", now)
	fresh.ImageKeys = []string{"img-key"}
	reply := post("01REPLY", "a reply", now)
	parent := "01EXISTING"
	reply.ParentId = &parent
	paid := post("01PAID", "paid", now)
	paid.Price = &pricedT
	feed.set(paid, reply, fresh, existing)
	p.poll()
	notes := fx.relay.Published(alice, KindNote)
	if len(notes) != 1 || !strings.HasPrefix(notes[0].Content, "new post\nhttps://gw.example/media/") ||
		!tagged(notes[0], "proxy", "https://gw.example/users/"+aliceULID+"/statuses/01FRESH", "web") {
		t.Fatalf("notes = %v, want only the new top-level post", notes)
	}

	edited := fresh
	at := now.Add(time.Minute)
	edited.UpdatedAt = &at
	edited.Text = "new post, edited"
	feed.set(paid, reply, edited, existing)
	p.poll()
	if notes = fx.relay.Published(alice, KindNote); len(notes) != 2 || !strings.HasPrefix(notes[1].Content, "new post, edited") {
		t.Fatalf("an edit must go out as a new note: %v", notes)
	}
	p.poll()
	if len(fx.relay.Published(alice, KindNote)) != 2 {
		t.Fatal("an unchanged post is published once")
	}

	feed.set(existing)
	p.poll()
	del := fx.relay.Published(alice, kindDelete)
	if len(del) != 1 || !tagged(del[0], "e", notes[0].ID.Hex()) || !tagged(del[0], "e", notes[1].ID.Hex()) {
		t.Fatalf("delete = %v, want every version of the vanished post", del)
	}

	feed.set()
	p.poll()
	if len(fx.relay.Published(alice, kindDelete)) != 1 {
		t.Fatal("a failed read must not delete everything")
	}
}

func TestNostrOutboundStartsConsentingUsersOnce(t *testing.T) {
	fx := newPublishFixture(t)
	feed := &fakeFeed{users: []warp.User{{Id: aliceULID}, optedOut(carolULID), {Id: "bob@mastodon.social", Network: warp.MastodonNetwork}}}
	o := newOutbound(testCtx(t), feed, fx.pub)
	o.scan(warp.DefaultOwnerHandle)
	o.scan(warp.DefaultOwnerHandle)
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.started) != 1 || !o.started[aliceULID] {
		t.Fatalf("started = %v, want only the consenting user", o.started)
	}
}

// startedUser reports whether the outbound has a poller for a user.
type startedUser struct {
	o  *nostrOutbound
	id string
}

func (s startedUser) ok() bool {
	s.o.mu.Lock()
	defer s.o.mu.Unlock()
	return s.o.started[s.id]
}

func TestNostrOutboundRetriesAnUnreachableNetworkSoon(t *testing.T) {
	fx := newPublishFixture(t)
	feed := &fakeFeed{}
	o := newOutbound(testCtx(t), feed, fx.pub)
	o.retry = 10 * time.Millisecond
	go o.RunScanner(warp.DefaultOwnerHandle)

	time.Sleep(30 * time.Millisecond)
	feed.setUsers(warp.User{Id: aliceULID})
	nostrtest.WaitFor(t, "a scan soon after the network answers", startedUser{o, aliceULID}.ok)
}

func TestNostrJSON(t *testing.T) {
	fx := newPublishFixture(t)
	srv := httptest.NewServer(newNIP05Handler(publisherList{fx.pub}.all))
	defer srv.Close()

	code, body := nostrJSONName(t, srv.URL, strings.ToLower(aliceULID))
	if code != http.StatusOK || body[strings.ToLower(aliceULID)] != fx.pk(t, aliceULID) {
		t.Fatalf("alice = %d %v", code, body)
	}
	for _, name := range []string{strings.ToLower(carolULID), "not-a-ulid", ""} {
		if code, _ := nostrJSONName(t, srv.URL, name); code != http.StatusNotFound {
			t.Errorf("%q = %d, want 404", name, code)
		}
	}
}

func nostrJSONName(t *testing.T, base, name string) (int, map[string]string) {
	t.Helper()
	resp, err := http.Get(base + "/.well-known/nostr.json?name=" + name)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Names map[string]string `json:"names"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	_ = resp.Body.Close()
	return resp.StatusCode, body.Names
}

// publisherList serves a fixed set of publishers to the NIP-05 handler.
type publisherList []*Publisher

func (l publisherList) all() []*Publisher { return l }
