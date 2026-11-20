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
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/Warp-net/bridge-gateway/internal/warp"
	"github.com/libp2p/go-libp2p/core/peer"
	log "github.com/sirupsen/logrus"
)

const identitySeedDomain = "warpnet/gateway/p2p/v1"

var errNoEntryPeers = errors.New("no Warpnet entry peers (check NODE_NETWORK)")

// listenByNetwork gives every network its own port: all of them are joined in
// one process. The nodes are relay-only and never advertise these addresses.
var listenByNetwork = map[string]string{
	warp.DefaultNetwork: "/ip4/0.0.0.0/tcp/4040",
	"testnet":           "/ip4/0.0.0.0/tcp/4041",
}

// bootstrapByNetwork lists each network's relays, the DHT entry points.
var bootstrapByNetwork = map[string][]string{
	warp.DefaultNetwork: {
		"/ip4/207.154.221.44/tcp/4001/p2p/12D3KooWMKZFrp1BDKg9amtkv5zWnLhuUXN32nhqMvbtMdV2hz7j",
		"/ip4/207.154.221.44/tcp/4002/p2p/12D3KooWSjbYrsVoXzJcEtmgJLMVCbPXMzJmNN1JkEZB9LJ2rnmU",
		"/ip4/207.154.221.44/tcp/4003/p2p/12D3KooWNXSGyfTuYc3JznW48jay73BtQgHszWfPpyF581EWcpGJ",
		"/ip4/130.94.88.38/tcp/4011/p2p/12D3KooWNW7nbLpbsEVJ86JN6c1zXRDKGCbqmLfhitFCPccRv2YW",
	},
	"testnet": {
		"/ip4/207.154.221.44/tcp/4011/p2p/12D3KooWMKZFrp1BDKg9amtkv5zWnLhuUXN32nhqMvbtMdV2hz7j",
		"/ip4/207.154.221.44/tcp/4022/p2p/12D3KooWSjbYrsVoXzJcEtmgJLMVCbPXMzJmNN1JkEZB9LJ2rnmU",
		"/ip4/207.154.221.44/tcp/4033/p2p/12D3KooWNXSGyfTuYc3JznW48jay73BtQgHszWfPpyF581EWcpGJ",
	},
}

// ConfiguredNetworks reads NODE_NETWORK, one name or a comma-separated list,
// and drops every network switched off by its GATEWAY_DISABLE_<NETWORK>.
func ConfiguredNetworks() []string {
	seen := make(map[string]struct{})
	networks := make([]string, 0, len(listenByNetwork))
	for _, name := range strings.Split(warp.EnvOr("NODE_NETWORK", warp.DefaultNetwork), ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, isDuplicate := seen[name]; isDuplicate {
			continue
		}
		seen[name] = struct{}{}
		if isNetworkDisabled(name) {
			log.Infof("gateway: %s node disabled by %s", name, disableEnvKey(name))
			continue
		}
		networks = append(networks, name)
	}
	return networks
}

func disableEnvKey(network string) string {
	return "GATEWAY_DISABLE_" + strings.ToUpper(strings.ReplaceAll(network, "-", "_"))
}

// isNetworkDisabled takes only a real boolean as an off switch, so a typo
// cannot silently drop a network.
func isNetworkDisabled(network string) bool {
	isOff, err := strconv.ParseBool(os.Getenv(disableEnvKey(network)))
	return err == nil && isOff
}

// IdentitySeed derives the libp2p identity from the secret signing key, so
// every gateway has its own stable peer id. All networks share it: they are
// PSK-isolated.
func IdentitySeed(key *rsa.PrivateKey) string {
	sum := sha256.Sum256(append([]byte(identitySeedDomain), x509.MarshalPKCS1PrivateKey(key)...))
	return hex.EncodeToString(sum[:])
}

func parseEntries(network string) ([]peer.AddrInfo, error) {
	var entries []peer.AddrInfo
	for _, s := range bootstrapByNetwork[network] {
		ai, err := peer.AddrInfoFromString(s)
		if err != nil {
			warp.NetLog(network).Warnf("nodeclient: bad bootstrap %q: %v", s, err)
			continue
		}
		entries = append(entries, *ai)
	}
	if len(entries) == 0 {
		return nil, errNoEntryPeers
	}
	return entries, nil
}
