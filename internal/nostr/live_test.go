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
	"testing"

	"github.com/Warp-net/bridge-gateway/internal/nostr/nostrtest"
	"github.com/Warp-net/bridge-gateway/internal/warp"
)

// nostrScanRequester serves a users page and per-user followings for the scan.
type nostrScanRequester struct {
	users      []warp.User
	followings map[string][]string
}

func (s nostrScanRequester) Request(route string, _ any) ([]byte, error) {
	return json.Marshal(warp.UsersResponse{Users: s.users})
}

func (s nostrScanRequester) RequestUser(userID, route string, _ any) ([]byte, error) {
	return json.Marshal(warp.FollowingsResponse{FollowerId: userID, Followings: s.followings[userID]})
}

func TestNostrLiveScanRebuildsFollows(t *testing.T) {
	alice, bob := nostrtest.NewKey(), nostrtest.NewKey()
	live := newLive(testNostrBridge(t), nil)
	live.scan(nostrScanRequester{
		users: []warp.User{{Id: "01NATIVE"}, {Id: Handle(alice.PK), Network: warp.NostrNetwork}, {Id: "01EMPTY"}},
		followings: map[string][]string{
			"01NATIVE": {Handle(alice.PK), "bob@mastodon.social", "01WARP"},
			"01EMPTY":  {},
		},
	}, warp.DefaultOwnerHandle)
	if f := live.FollowersOf(alice.PK); len(f) != 1 || f[0] != "01NATIVE" {
		t.Fatalf("followers of alice = %v", f)
	}
	live.SetFollow("01NATIVE", Handle(bob.PK), false)
	live.scan(nostrScanRequester{
		users:      []warp.User{{Id: "01NATIVE"}},
		followings: map[string][]string{"01NATIVE": {Handle(bob.PK)}},
	}, warp.DefaultOwnerHandle)
	if len(live.FollowersOf(alice.PK)) != 0 || len(live.FollowersOf(bob.PK)) != 1 {
		t.Fatal("a scan must replace what it read for a user")
	}
}
