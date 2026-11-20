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

package node

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/warp"
	wjson "github.com/Warp-net/warpnet/json"
	"github.com/Warp-net/warpnet/security"
	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const wireVersion = "0.0.0"

// sendStream writes warpnet's signed message envelope on the route's protocol
// and reads the whole response.
func sendStream(ctx context.Context, h host.Host, p peer.ID, privKey ed25519.PrivateKey, route string, payload any) ([]byte, error) {
	body, isRaw := payload.([]byte)
	if !isRaw && payload != nil {
		var err error
		if body, err = wjson.Marshal(payload); err != nil {
			return nil, fmt.Errorf("stream: marshal payload: %w", err)
		}
	}

	// NATed members are reachable over a relay only.
	ctx = network.WithAllowLimitedConn(ctx, route)
	s, err := h.NewStream(ctx, p, protocol.ID(route))
	if err != nil {
		return nil, fmt.Errorf("stream: new: %w", err)
	}
	defer func() { _ = s.Close() }()

	msg := warp.Message{
		Body:        wjson.RawMessage(body),
		MessageId:   uuid.New().String(),
		NodeId:      h.ID().String(),
		Destination: route,
		Timestamp:   time.Now(),
		Version:     wireVersion,
	}
	msg.Signature = security.Sign(privKey, msg.SigningBytes())
	data, err := wjson.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("stream: marshal envelope: %w", err)
	}

	rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
	if _, err := rw.Write(data); err != nil {
		return nil, fmt.Errorf("stream: write: %w", err)
	}
	if err := rw.Flush(); err != nil {
		return nil, fmt.Errorf("stream: flush: %w", err)
	}
	_ = s.CloseWrite()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rw); err != nil {
		return nil, fmt.Errorf("stream: read: %w", err)
	}
	return buf.Bytes(), nil
}
