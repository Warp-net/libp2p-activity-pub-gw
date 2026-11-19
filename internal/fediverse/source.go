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

package fediverse

// WarpnetUser is the minimal user shape the gateway renders into an actor.
type WarpnetUser struct {
	ID                string
	PreferredUsername string
	DisplayName       string
	Summary           string
	Avatar            string // domain.User.AvatarKey ("<mime>,<base64>"); empty if none
	Background        string // domain.User.BackgroundImageKey; empty if none
}

// WarpnetSource resolves the Warpnet user a given actor handle maps to.
//
// multiNode (multinet.go) is what the gateway runs: it resolves ANY requested
// handle live via PUBLIC_GET_USER, on whichever joined network has the user.
// nodeSource is the same read against one network. staticSource is a single-user
// dev fallback, used only when no network is reachable.
type WarpnetSource interface {
	GetUser(preferredUsername string) (WarpnetUser, bool)
}

type StaticSource struct {
	User WarpnetUser
}

func (s StaticSource) GetUser(preferredUsername string) (WarpnetUser, bool) {
	if preferredUsername != s.User.PreferredUsername {
		return WarpnetUser{}, false
	}
	return s.User, true
}
