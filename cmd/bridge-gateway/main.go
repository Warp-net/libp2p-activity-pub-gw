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

// Command bridge-gateway bridges Warpnet with the Fediverse and Nostr. It joins
// every network named in NODE_NETWORK as an ordinary member node, serves any
// Warpnet user as an ActivityPub actor and resolves bridged accounts live, so
// it keeps nothing but its keys on disk. The public host is self-hosted via
// embedded Tailscale Funnel.
//
// Configuration is environment-only: importing the libp2p stack pulls in
// warpnet's config.init, whose pflag.Parse would clash with a flag set here.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/fediverse"
	"github.com/Warp-net/bridge-gateway/internal/logring"
	"github.com/Warp-net/bridge-gateway/internal/netguard"
	"github.com/Warp-net/bridge-gateway/internal/node"
	"github.com/Warp-net/bridge-gateway/internal/nostr"
	"github.com/Warp-net/bridge-gateway/internal/warp"
	log "github.com/sirupsen/logrus"
	"tailscale.com/tsnet"
)

const version = "0.1.132"

// Developer's switches, not deployment config.
const (
	isNostrEnabled    = true
	isNostrPublishing = true
)

const (
	logRingSize       = 2000
	readHeaderTimeout = 10 * time.Second
	shutdownTimeout   = 5 * time.Second
)

func main() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true, TimestampFormat: time.DateTime})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
	logs := logring.NewRing(logRingSize)
	log.AddHook(logs)
	log.Infof("bridge-gateway: starting version %s", version)
	fediverse.SetVersion(version)

	// The persisted Dir pins the *.ts.net hostname: a new one orphans every
	// Fediverse follower.
	ts := &tsnet.Server{
		Hostname: warp.EnvOr("GATEWAY_FUNNEL_HOSTNAME", "warpnet-gw"),
		Dir:      warp.EnvOr("GATEWAY_FUNNEL_DIR", "fediverse-gateway-tsnet"),
		AuthKey:  os.Getenv("TS_AUTHKEY"),
		UserLogf: log.Infof,
		Logf:     log.Debugf,
	}
	status, err := ts.Up(context.Background())
	if err != nil {
		log.Fatalf("bridge-gateway: tailscale funnel: %v", err)
	}
	if status.Self == nil || status.Self.DNSName == "" {
		log.Fatalln("bridge-gateway: tailscale funnel: node has no DNS name (enable MagicDNS + HTTPS for the tailnet)")
	}
	host := strings.TrimSuffix(status.Self.DNSName, ".")
	log.Infof("bridge-gateway: tailscale funnel node up as %s", host)

	key, err := fediverse.LoadOrCreateKey(warp.EnvOr("GATEWAY_KEY", "fediverse-gateway-key.pem"))
	if err != nil {
		log.Fatalf("bridge-gateway: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	seed := node.IdentitySeed(key)
	var nodes []*node.Node
	for _, network := range node.ConfiguredNetworks() {
		n, err := node.NewNode(ctx, network, seed)
		if err != nil {
			log.Warnf("bridge-gateway: joining %s: %v", network, err)
			continue
		}
		nodes = append(nodes, n)
	}

	cfg := fediverse.Config{
		Host:          host,
		Key:           key,
		Logs:          logs,
		LogsToken:     os.Getenv("GATEWAY_LOGS_TOKEN"),
		ForeignHandle: nostr.IsHandle,
	}
	var multiNode *node.MultiNode
	if len(nodes) == 0 {
		log.Warnln("bridge-gateway: no Warpnet network reachable; serving the static profile only")
	} else {
		multiNode = node.NewMultiNode(nodes)
		cfg.Source, cfg.Requester = multiNode, multiNode
		log.Infof("bridge-gateway: joined Warpnet (%s); any user is resolvable via the network", multiNode.Networks())
	}
	gateway, err := fediverse.New(cfg)
	if err != nil {
		log.Fatalf("bridge-gateway: %v", err)
	}

	if multiNode != nil {
		var nostrHub *nostr.Hub
		if isNostrEnabled {
			nostrHub = nostr.NewHub(nostr.NewRelayPool(netguard.NewClient(0)), multiNode, gateway, gateway, host, isNostrPublishing)
			log.Infof("bridge-gateway: publishing to Nostr %v", isNostrPublishing)
			gateway.Handle("/.well-known/nostr.json", nostrHub.NIP05Handler())
		}

		// Outbound federation follows the graph stored in Warpnet: a user is
		// federated once they gain a Fediverse follower, and a restart resumes
		// it from the network alone.
		federations := make(map[*node.Node]*fediverse.OutboundFederation, len(nodes))
		for _, n := range nodes {
			n.Serve(warp.DefaultOwnerHandle, routes(gateway, nostrHub, n))
			federations[n] = fediverse.NewOutboundFederation(ctx, n, gateway)
			go federations[n].RunScanner(warp.DefaultOwnerHandle)
		}
		if nostrHub != nil {
			nostrHub.Run(ctx, warp.DefaultOwnerHandle)
		}
		gateway.OnFollowed = func(localUser string) {
			n, ok := multiNode.Locate(localUser)
			if !ok {
				log.Warnf("bridge-gateway: cannot federate %s: no joined network serves it", localUser)
				return
			}
			federations[n].Start(localUser)
		}
	}

	srv := &http.Server{Handler: gateway.Routes(), ReadHeaderTimeout: readHeaderTimeout}
	// A locked limiter never reclaims its window by itself: without the drain
	// the first burst over budget would 429 everything until a restart.
	go gateway.Limits.Drain(ctx)

	go func() {
		log.Infof("bridge-gateway: serving Warpnet users at https://%s/users/{id}", host)
		ln, err := ts.ListenFunnel("tcp", ":443")
		if err != nil {
			log.Fatalf("bridge-gateway: tailscale funnel: %v", err)
		}
		log.Infof("bridge-gateway: serving public https://%s via Tailscale Funnel", host)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("bridge-gateway: serve: %v", err)
		}
	}()

	// The debug listener serves /logs and /debug/pprof only, behind the token.
	var debugSrv *http.Server
	if addr := os.Getenv("GATEWAY_LOGS_ADDR"); addr != "" {
		debugSrv = &http.Server{Addr: addr, Handler: gateway.LogsHandler(), ReadHeaderTimeout: readHeaderTimeout}
		go func() {
			log.Infof("bridge-gateway: serving /logs and /debug/pprof on %s", addr)
			if err := debugSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Errorf("bridge-gateway: logs listener: %v", err)
			}
		}()
	}

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-interruptChan

	log.Infoln("bridge-gateway: shutting down...")
	cancel()
	for _, n := range nodes {
		_ = n.Close()
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
	if debugSrv != nil {
		_ = debugSrv.Shutdown(shutdownCtx)
	}
	_ = ts.Close()
}
