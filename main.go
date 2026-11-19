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

// Command fediverse-gateway is a thin ActivityPub gateway that lets Warpnet
// users be discovered and followed from Mastodon / the Fediverse. It is agnostic
// to node, user, and network: it joins every network named in NODE_NETWORK
// (one libp2p node each, e.g. "warpnet,testnet") through their bootstrap nodes
// and resolves any requested user via the public routes, on whichever network
// has them. Outbound post/follow federation follows the graph — it starts for a
// user once they gain a Fediverse follower, and is never pinned to a configured
// user.
//
// Implemented: WebFinger, an actor document with an RSA public key, an inbox
// that verifies HTTP signatures and answers Follow with a signed Accept
// (persisting the follower), outbound Create(Note) fan-out, and a libp2p
// connector to the Warpnet network.
//
// Configuration is environment-only and intentionally minimal: GATEWAY_KEY,
// GATEWAY_FUNNEL_DIR, GATEWAY_FUNNEL_HOSTNAME, TS_AUTHKEY, GATEWAY_AP_MIRROR
// (the instance asked for posts an account's own server will not serve), and the
// standard NODE_NETWORK (one name or a comma-separated list, each network's node
// also switchable on its own via GATEWAY_DISABLE_<NETWORK>). It does NOT use CLI
// flags: importing the libp2p stack pulls in config.init's pflag.Parse, which
// would clash with a second flag set, and every other Warpnet node is
// env-configured too.
//
// The gateway keeps only keys on disk (RSA signing key); profile/followers
// live in Warpnet. The public endpoint is self-hosted via embedded Tailscale
// Funnel, which terminates TLS and pins the hostname.
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

	"github.com/Warp-net/activity-pub-gw/internal/fediverse"
	"github.com/Warp-net/activity-pub-gw/internal/logring"
	"github.com/Warp-net/activity-pub-gw/internal/netguard"
	"github.com/Warp-net/activity-pub-gw/internal/nostr"
	"github.com/Warp-net/activity-pub-gw/internal/warp"

	log "github.com/sirupsen/logrus"
	"tailscale.com/tsnet"
)

const gatewayVersion = "0.1.118"

// logRingSize is how many recent log lines the /logs endpoint retains in memory.
const logRingSize = 2000

const fatalFmt = "gateway: %v"

func main() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true, TimestampFormat: time.DateTime})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
	// Mirror logs into an in-memory ring so the /logs endpoint can serve them.
	logs := logring.NewRing(logRingSize)
	log.AddHook(logs)
	log.Infof("gateway: starting version %s", gatewayVersion)
	fediverse.SetVersion(gatewayVersion)

	keyPath := warp.EnvOr("GATEWAY_KEY", "fediverse-gateway-key.pem")

	// Self-host the public endpoint via embedded Tailscale Funnel: the gateway
	// becomes its own tailnet node and ListenFunnel (below) serves public HTTPS
	// with an auto-provisioned *.ts.net cert, deriving host from the node. The
	// persisted Dir keeps the hostname stable across restarts (a rotating name
	// orphans existing followers).
	ts := &tsnet.Server{
		Hostname: warp.EnvOr("GATEWAY_FUNNEL_HOSTNAME", "warpnet-gw"),
		Dir:      warp.EnvOr("GATEWAY_FUNNEL_DIR", "fediverse-gateway-tsnet"),
		AuthKey:  os.Getenv("TS_AUTHKEY"),
		UserLogf: log.Infof,
		Logf:     log.Debugf,
	}
	st, uerr := ts.Up(context.Background())
	if uerr != nil {
		log.Fatalf("gateway: tailscale funnel: %v", uerr)
	}
	if st.Self == nil || st.Self.DNSName == "" {
		log.Fatalln("gateway: tailscale funnel: node has no DNS name (enable MagicDNS + HTTPS for the tailnet)")
	}
	host := strings.TrimSuffix(st.Self.DNSName, ".")
	log.Infof("gateway: tailscale funnel node up as %s", host)

	key, err := fediverse.LoadOrCreateKey(keyPath)
	if err != nil {
		log.Fatalf(fatalFmt, err)
	}

	appCtx, appCancel := context.WithCancel(context.Background())

	// Join every network in NODE_NETWORK at once — one libp2p node each — and
	// serve them behind the single ActivityPub host. A handle carries no network,
	// so multiNode locates each user on the network that has them.
	var src fediverse.WarpnetSource = fediverse.StaticSource{} // empty fallback when no network is reachable
	var nodes *multiNode
	if os.Getenv("GATEWAY_P2P_SEED") == "" {
		log.Warnf("gateway: GATEWAY_P2P_SEED is unset; running under the public legacy identity, which nodes trust with nothing secret")
	}
	clients := connectNetworks(appCtx)
	if len(clients) == 0 {
		log.Warnln("gateway: no Warpnet network reachable; serving the static profile only")
	} else {
		nodes = newMultiNode(clients)
		src = nodes
		log.Infof("gateway: joined Warpnet (%s); any user is resolvable via the network", nodes.networks())
	}

	var req warp.Requester
	if nodes != nil {
		req = nodes
	}
	g, err := fediverse.New(fediverse.Config{
		Host:          host,
		Key:           key,
		Source:        src,
		Requester:     req,
		Logs:          logs,
		LogsToken:     os.Getenv("GATEWAY_LOGS_TOKEN"),
		ForeignHandle: nostr.IsHandle,
	})
	if err != nil {
		log.Fatalf(fatalFmt, err)
	}

	var hub *nostrHub
	if os.Getenv("GATEWAY_DISABLE_NOSTR") == "" {
		hub = &nostrHub{g: g, relays: nostr.NewRelayPool(netguard.NewClient(0))}
		keys := nodeKeys{}
		for _, c := range clients {
			keys[c.network] = c
		}
		hub.keys = nostr.NewKeys(keys)
		// Writing to Nostr waits for the node-facing sender check; until it is
		// in, GATEWAY_NOSTR_PUBLISH stays off.
		hub.publish = os.Getenv("GATEWAY_NOSTR_PUBLISH") == "true"
		log.Infof("gateway: publishing to Nostr %v", hub.publish)
		nb := nostr.NewBridge(hub.relays, nostr.RelaysFromEnv(), "", host)
		hub.live = nostr.NewLive(nb, hub.push)
		nb.Trusted = hub.live.Trusted
		g.Handle("/.well-known/nostr.json", nostr.NewNIP05Handler(hub.publishers))
		go hub.live.Run(appCtx)
		if nodes != nil {
			for _, c := range clients {
				go hub.live.RunScanner(appCtx, c, warp.DefaultOwnerHandle)
			}
		}
	}

	// Serve Warpnet's public /public routes over libp2p (Mastodon -> Warpnet) on
	// every joined network: the gateway joins each as an ordinary member peer that
	// advertises a Mastodon account as its node owner (so discovery seeds it) and
	// resolves every user/tweet/image request live from the Fediverse via
	// ActivityPub. The Fediverse side is network-independent, so all nodes serve
	// the same handlers.
	for _, c := range clients {
		c.serveRoutes(g, hub, warp.DefaultOwnerHandle)
	}
	for _, pub := range hub.publishers() {
		for _, c := range clients {
			if c.network == pub.Network {
				go nostr.NewOutbound(appCtx, c, pub).RunScanner(warp.DefaultOwnerHandle)
			}
		}
	}

	// Outbound federation follows the graph: when a Warpnet user gains a
	// Fediverse follower (an accepted inbound Follow), start federating that
	// user's new posts and follows. It is never pinned to a specific user.
	// The federated set is derived from the follow graph stored in Warpnet
	// (users with an ap: follower), so the gateway keeps no local state and
	// federation resumes after a restart from the network alone.
	//
	// One federation per network: the scan pages through the users of the network
	// it runs on, and a user's pollers must read from their own network.
	if nodes != nil {
		feds := make(map[*nodeClient]*fediverse.OutboundFederation, len(clients))
		for _, c := range clients {
			of := fediverse.NewOutboundFederation(appCtx, c, g)
			feds[c] = of
			go of.RunScanner(warp.DefaultOwnerHandle)
		}
		g.OnFollowed = func(localUser string) {
			c, ok := nodes.locate(localUser)
			if !ok {
				log.Warnf("gateway: cannot federate %s: no joined network serves it", localUser)
				return
			}
			feds[c].Start(localUser)
		}
	}

	srv := &http.Server{
		Handler:           g.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	// Keep the global rate limiter's sliding window advancing: without this it
	// stays locked forever once the budget is first exceeded (the middleware
	// fast-429s a locked limiter without ever calling Limit, which is what
	// reclaims expired weight), taking the whole data plane down until restart.
	go g.Limits.Drain(appCtx)

	go func() {
		log.Infof("gateway: serving Warpnet users at https://%s/users/{id}", host)
		ln, lerr := ts.ListenFunnel("tcp", ":443")
		if lerr != nil {
			log.Fatalf("gateway: tailscale funnel: %v", lerr)
		}
		log.Infof("gateway: serving public https://%s via Tailscale Funnel", host)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("gateway: serve: %v", err)
		}
	}()

	// Optional standalone debug listener on a plain-HTTP port (e.g. ":8080"),
	// for reaching the logs and the profiles on the host's own address instead of
	// the Funnel hostname. Serves only /logs and /debug/pprof, both gated by
	// GATEWAY_LOGS_TOKEN.
	var logsSrv *http.Server
	if addr := os.Getenv("GATEWAY_LOGS_ADDR"); addr != "" {
		logsSrv = &http.Server{Addr: addr, Handler: g.LogsHandler(), ReadHeaderTimeout: 10 * time.Second}
		go func() {
			log.Infof("gateway: serving /logs and /debug/pprof on %s", addr)
			if err := logsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Errorf("gateway: logs listener: %v", err)
			}
		}()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Infoln("gateway: shutting down...")
	appCancel()
	for _, c := range clients {
		c.close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	if logsSrv != nil {
		_ = logsSrv.Shutdown(ctx)
	}
	_ = ts.Close()
}
