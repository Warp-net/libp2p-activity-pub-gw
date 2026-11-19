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
	"strings"

	"github.com/oklog/ulid/v2"
)

// nip05Handler serves NIP-05 for consenting Warpnet users: a user id is
// <ulid>@<gateway host>, lower-cased as NIP-05 names are.
type nip05Handler struct {
	publishers func() []*Publisher
}

func NewNIP05Handler(publishers func() []*Publisher) http.Handler {
	return nip05Handler{publishers: publishers}
}

func (h nip05Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	name := strings.ToLower(r.URL.Query().Get("name"))
	if _, err := ulid.ParseStrict(name); err != nil {
		http.NotFound(w, r)
		return
	}
	for _, pub := range h.publishers() {
		if !pub.consents(strings.ToUpper(name)) {
			continue
		}
		pk, ok := pub.keys.pubKey(pub.Network, name)
		if !ok {
			continue
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"names":  map[string]string{name: pk},
			"relays": map[string][]string{pk: pub.bridge.defaults},
		})
		return
	}
	http.NotFound(w, r)
}
