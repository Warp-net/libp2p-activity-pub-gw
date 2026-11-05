// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

const (
	directoryLimit        = uint64(80)
	directoryTimeout      = 5 * time.Second
	directoryCursorPrefix = "directory:"
)

// GetWhoToFollow supplies public discovery candidates, without an OAuth account.
func (b *mastodonBridge) GetWhoToFollow(ctx context.Context, ev getAllUsersEvent) (usersResponse, error) {
	resp := usersResponse{Users: []user{}, Cursor: "end"}
	host, configured := os.LookupEnv("GATEWAY_DISCOVERY_HOST")
	if !configured {
		host = defaultMirrorHost
	}
	if host == "" || (ev.Cursor != nil && *ev.Cursor == "end") {
		return resp, nil
	}
	limit := uint64(40)
	if ev.Limit != nil && *ev.Limit > 0 {
		limit = min(*ev.Limit, directoryLimit)
	}
	var offset uint64
	if ev.Cursor != nil && *ev.Cursor != "" {
		raw, ok := strings.CutPrefix(*ev.Cursor, directoryCursorPrefix)
		var err error
		offset, err = strconv.ParseUint(raw, 10, 64)
		if !ok || err != nil || offset > ^uint64(0)-limit {
			return resp, fmt.Errorf("directory: invalid cursor")
		}
	}
	u := url.URL{Scheme: "https", Host: host, Path: "/api/v1/directory"}
	q := url.Values{"order": {"active"}, "local": {"false"}, "limit": {strconv.FormatUint(limit, 10)}, "offset": {strconv.FormatUint(offset, 10)}}
	u.RawQuery = q.Encode()
	key := u.String()
	if cached, ok := b.recommendations.Get(key); ok {
		return cached, nil
	}
	ctx, cancel := context.WithTimeout(ctx, directoryTimeout)
	defer cancel()
	arr, err := b.ap.apGetArray(ctx, key, "application/json")
	if err != nil {
		return resp, err
	}
	seen := map[string]bool{}
	for _, item := range arr[:min(uint64(len(arr)), limit)] {
		acc := asMap(item)
		handle := acctHandle(host, asString(acc["acct"]))
		if handle == "" || seen[handle] || acc["suspended"] == true || acc["moved"] != nil {
			continue
		}
		name, instance, ok := strings.Cut(handle, "@")
		_, nativeIDErr := ulid.ParseStrict(name)
		if !ok || name == "" || instance == "" || strings.Contains(instance, "@") || nativeIDErr == nil || b.ap.isSelfHost(instance) {
			continue
		}
		seen[handle] = true
		b.rememberAccountHandle(acc)
		resp.Users = append(resp.Users, restAccountToUser(handle, acc, b.nodeID))
	}
	if uint64(len(arr)) >= limit {
		resp.Cursor = directoryCursorPrefix + strconv.FormatUint(offset+limit, 10)
	}
	b.recommendations.Add(key, resp)
	return resp, nil
}
