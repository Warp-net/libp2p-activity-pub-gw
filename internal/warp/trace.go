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

package warp

import (
	"context"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

// Trace correlates every outbound fetch a single libp2p request fans out to,
// so the log shows one request's whole REST pipeline (indented under it, each
// timed) sharing one id, then a summary line with the total pipeline duration.
type Trace struct {
	ID    string
	Calls atomic.Int32
}

type traceCtxKey struct{}

// StartTrace stamps ctx with a fresh trace and returns it plus the trace so the
// caller can log the closing summary.
func StartTrace(ctx context.Context) (context.Context, *Trace) {
	tr := &Trace{ID: RandomToken()[:8]}
	return context.WithValue(ctx, traceCtxKey{}, tr), tr
}

func traceFrom(ctx context.Context) *Trace {
	tr, _ := ctx.Value(traceCtxKey{}).(*Trace)
	return tr
}

// LogFetch records one outbound fetch under the current request's trace: indented
// beneath the libp2p line, numbered, and timed (cached marks a cache hit).
func LogFetch(ctx context.Context, method, url string, status int, took time.Duration, cached bool) {
	tr := traceFrom(ctx)
	if tr == nil {
		return
	}
	n := tr.Calls.Add(1)
	tag := ""
	if cached {
		tag = " (cached)"
	}
	log.Infof("  [%s] #%d %s %s%s -> %d in %s", tr.ID, n, method, url, tag, status, took.Round(time.Millisecond))
}
