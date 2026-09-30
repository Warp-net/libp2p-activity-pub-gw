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
	"embed"
	"html"
	"net/http"
)

// staticFS holds the Warpnet logo served as the actor's custom emoji badge and
// the picture a sponsored tweet federates as.
//
//go:embed static/warpnet.png static/sponsored.png
var staticFS embed.FS

const (
	pathStatic = "/static/"

	// warpnetEmojiShortcode is the custom-emoji shortcode appended to the
	// display name; Mastodon replaces it with the Warpnet logo inline.
	warpnetEmojiShortcode = ":warpnet:"
	warpnetBadgePath      = pathStatic + "warpnet.png"

	// warpnetBioPrefix is prepended (as HTML) to the bio so a Mastodon visitor
	// sees the account belongs to the Warpnet network, not a Mastodon instance.
	warpnetBioPrefix = "<p>🛸 This account lives on <strong>Warpnet</strong> — a decentralized " +
		"P2P social network. You're viewing it across the Warpnet⇄Fediverse gateway.</p>"
)

// handleStatic serves the embedded badge image (Content-Type is set from the
// extension, which Mastodon reads when caching the custom emoji).
func (g *gateway) handleStatic(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "public, max-age=86400")
	http.FileServer(http.FS(staticFS)).ServeHTTP(w, r)
}

// badgedSummary prefixes the user's bio with the Warpnet notice. The original
// bio is HTML-escaped (Warpnet bios are plain text; the summary is rendered as
// HTML by Mastodon).
func badgedSummary(bio string) string {
	if bio == "" {
		return warpnetBioPrefix
	}
	return warpnetBioPrefix + "<p>" + html.EscapeString(bio) + "</p>"
}

// warpnetActorTag returns the custom-emoji tag for the Warpnet badge.
func (g *gateway) warpnetActorTag() emojiTag {
	return emojiTag{
		Type: "Emoji",
		ID:   g.baseURL() + "/emoji/warpnet",
		Name: warpnetEmojiShortcode,
		Icon: &emojiIcon{Type: "Image", MediaType: "image/png", URL: g.baseURL() + warpnetBadgePath},
	}
}

// warpnetNetworkField returns the "Network" profile metadata row.
func warpnetNetworkField() propertyValue {
	return propertyValue{
		Type:  "PropertyValue",
		Name:  "Network",
		Value: `<a href="https://warpnet.site" rel="nofollow noopener" target="_blank">Warpnet</a>`,
	}
}
