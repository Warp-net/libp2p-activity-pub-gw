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
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// logRing is a bounded in-memory ring buffer of the most recent log lines,
// installed as a logrus hook so the /logs endpoint can serve them. Transient
// and in memory only (never on disk) — it keeps the gateway stateless.
type logRing struct {
	mu   sync.Mutex
	buf  []logLine
	size int
	next int
	full bool
}

// logLine is one buffered line and the Warpnet network it came from, so the
// networks the gateway runs side by side can be read apart. An empty network
// means the line belongs to none of them (the ActivityPub surface, the Mastodon
// bridge) and shows up whichever network is being read.
type logLine struct {
	text    string
	network string
}

func newLogRing(size int) *logRing {
	return &logRing{buf: make([]logLine, size), size: size}
}

// Levels/Fire implement logrus.Hook: every emitted entry is formatted and stored.
func (lr *logRing) Levels() []log.Level { return log.AllLevels }

func (lr *logRing) Fire(e *log.Entry) error {
	network, _ := e.Data[logFieldNetwork].(string)
	tag := ""
	if network != "" {
		tag = "[" + network + "] "
	}
	line := logLine{
		text:    fmt.Sprintf("%s %-5s %s%s", e.Time.Format(time.DateTime), e.Level.String(), tag, e.Message),
		network: network,
	}
	lr.mu.Lock()
	lr.buf[lr.next] = line
	lr.next = (lr.next + 1) % lr.size
	if lr.next == 0 {
		lr.full = true
	}
	lr.mu.Unlock()
	return nil
}

// lines returns the buffered log lines oldest-first. A non-empty network keeps
// only that network's lines plus the untagged ones, so one network can be read
// without the other's traffic in the way; an empty network returns everything.
func (lr *logRing) lines(network string) []string {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	ordered := make([]logLine, 0, lr.size)
	if !lr.full {
		ordered = append(ordered, lr.buf[:lr.next]...)
	} else {
		ordered = append(ordered, lr.buf[lr.next:]...)
		ordered = append(ordered, lr.buf[:lr.next]...)
	}

	out := make([]string, 0, len(ordered))
	for _, l := range ordered {
		if network != "" && l.network != "" && l.network != network {
			continue
		}
		out = append(out, l.text)
	}
	return out
}
