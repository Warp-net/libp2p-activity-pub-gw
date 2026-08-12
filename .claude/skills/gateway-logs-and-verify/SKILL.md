---
name: gateway-logs-and-verify
description: Use this skill whenever an activity-pub-gw change or Fediverse bridging bug needs real evidence — any task phrased as "get the gateway logs", "why didn't the reply/like/follow reach Mastodon", "is it federating", "check it end-to-end on the testnet gateway", "verify locally before pushing", "did the deploy actually take", or when a Warpnet↔Fediverse symptom (reply invisible, 404 on a status, follow not applied) needs to be traced through the running gateway rather than guessed from code. It documents how to pull the gateway's live `/logs`, read them, verify against the real Mastodon thread, and run the local build/test + pinned-warpnet drift check. Do NOT use it to design a new public route (that is warpnet-add-handler in the node repo) — use it to diagnose and prove an existing bridge behaves correctly.
---

# Getting gateway logs and verifying activity-pub-gw

`go test` proves the package compiles and the unit logic holds. It does **not** prove
that the gateway federates a real activity, that a note is dereferenceable by a remote
instance, or that the running droplet is even on your code. The gateway is a stateless
bridge — the truth lives in its live logs and in the actual Mastodon thread, not in the
source. This skill is the runbook for pulling both.

## 1. Pull the gateway's `/logs`

The gateway mirrors its recent log lines into an in-memory ring and serves them at
`/logs`, gated by `GATEWAY_LOGS_TOKEN` (as `?token=` **or** an `Authorization: Bearer`
header). It is a plain `text/plain` dump of the last ~2000 lines. Two listeners expose it:

- **Standalone plain-HTTP listener** — `GATEWAY_LOGS_ADDR`, bound on the droplet host
  (testnet `:4080` via `network_mode: host`, mainnet `:4081` published from the container):
  `http://<droplet-ip>:4080/logs?token=$GATEWAY_LOGS_TOKEN`
- **Tailscale Funnel public HTTPS** (:443) — `https://<GATEWAY_FUNNEL_HOSTNAME>.<tailnet>.ts.net/logs?token=$GATEWAY_LOGS_TOKEN`
  (testnet host `warpnet-gw-testnet.<tailnet>.ts.net`, mainnet `warpnet-gw.<tailnet>.ts.net`;
  the prefix is in `deploy/docker-compose-<network>.yml`, the public host is also visible on
  Mastodon as the `@user@<host>` domain).

Two deployments share the droplet, so pick the port/host of the one you are debugging: the
testnet-only one (`:4080`, `warpnet-gw-testnet`) and the mainnet one (`:4081`, `warpnet-gw`),
which joins mainnet **and** testnet in a single process — one libp2p node each. Its logs
therefore carry both networks; `multinet:` lines say which network serves a given user, and
`nodeclient <peer>: joined Warpnet (<network>)` appears once per node at startup. The droplet
ip lives in `.github/workflows/build-deploy-<network>.yaml` and the funnel prefix in
`deploy/docker-compose-<network>.yml`. **Never commit the token value** — pass it at call
time.

### Reaching it from a restricted sandbox

If outbound egress goes through an **HTTPS-CONNECT-only** agent proxy (direct sockets
blocked, absolute-form HTTP proxying rejected), the plain-HTTP `:4080` listener needs a
forced CONNECT tunnel — send plain HTTP *inside* the tunnel:

```sh
curl -sS --proxytunnel -x "$HTTPS_PROXY" \
  "http://<droplet-ip>:4080/logs?token=$GATEWAY_LOGS_TOKEN" -o gwlogs.txt
```

`--noproxy '*'` (direct) times out; `-x` without `--proxytunnel` gets a 405 "this proxy
only accepts HTTPS CONNECT tunnels". The Funnel HTTPS URL works with a normal
`-x "$HTTPS_PROXY"` (it is real TLS on :443).

Fallbacks: `docker logs --since 30m fediverse-gateway-testnet` on the droplet
(`make ssh-do` from the warpnet repo). The Docker remote API on :2375 also carries logs
but a safety classifier may block raw-daemon access — prefer `/logs`.

## 2. Read the logs

Anchors, and what they mean:

- `gateway: starting version X.Y.Z` — **the running build.** If it does not match your
  merged `gatewayVersion`, the droplet was not redeployed. Deploy is manual
  (`workflow_dispatch` "Build & Deploy Testnet"); merging `main` does **not** auto-deploy.
- `nodeserver: serving N public routes as <peer> (owner <handle>)` — the gateway's libp2p
  server is up; `<peer>` is what a Warpnet node discovers as the bridged user's node.
- `[<id>] libp2p <route>: N REST calls in <t>` — one inbound libp2p request and how many
  outbound REST fetches it fanned out to. `0 REST calls` on `/private/post/tweet` is a
  **non-reply** correctly skipped (top-level owner tweets arrive here via gossip; only a
  tweet with a parent federates).
- `  [<id>] #n GET <url> -> <status>` — a **traced outbound fetch** (the `authorInbox`
  GETs, thread reads, etc.), indented under its request.
- `http: GET|POST <path> -> <status> (ua="Mastodon/… +https://<instance>/")` — the
  gateway's **inbound HTTP access log**: what a remote instance fetched from us and the
  result. A `GET /users/<u>/statuses/<id> -> 404` is a remote instance failing to
  dereference one of our notes.

**Trap:** `postSigned` (the outbound `POST` to a peer inbox) is **not** traced — there is
no `#n POST … inbox` line even on success. Absence of a POST line ≠ no delivery. A failed
delivery surfaces only as a `warning nodeserver: <route>: deliver to <inbox>: status …`
line. No such warning ⇒ the `Create`/`Delete` was accepted (2xx).

## 3. Verify federation against the real Mastodon thread

The gateway logs prove *we* sent it; the Fediverse proves it *landed*. Query the parent
instance's public API directly (through the proxy — it is HTTPS):

```sh
curl -sS -x "$HTTPS_PROXY" \
  "https://<instance>/api/v1/statuses/<parent-id>/context" | jq '.descendants[] | {uri, acct: .account.acct, content}'
```

A Warpnet reply shows up as a descendant with
`acct: <userid>@<gateway-host>` and `display: <name>`. If it is there but the same reply
is missing on *another* instance, that other instance could not dereference it — confirm
with `GET https://<gateway-host>/users/<user>/statuses/<id>` (200 vs 404). A reply status
carries its parent on its own id (`…/statuses/<id>?parent=<parentURL>`); dereferencing
must return `200` with `inReplyTo` equal to the real parent url and an `id` equal to the
fetched url.

## 4. Verify locally

The module is a single package. The full gate is:

```sh
go build ./...
go vet ./...
go test .
```

Bump `gatewayVersion` in `main.go` on every commit (see CLAUDE.md) — it is the only way
to tell from `/logs` what is actually running after a deploy.

### Pinned-warpnet drift is the usual root cause

`go.mod` pins an **old** `github.com/Warp-net/warpnet` (a `v1.0.1-0.2026…` pseudo-version).
The gateway speaks the node's own `event.*`/`domain.*` types, so when the live node moves
ahead, routes and DTOs drift. Diff the pinned source against the checked-out node:

```sh
W=$(go env GOMODCACHE)/github.com/'!warp-net'/warpnet@<pinned-version>
C=<path-to-warpnet-checkout>
diff <(sort "$W/event/paths.go") <(sort "$C/event/paths.go")   # routes added/removed
# repeat for event/event.go and domain/warpnet.go for DTO field drift
```

`wjson`/jsoniter decodes with unknown-field tolerance, so **extra** fields the node sends
are harmless; the danger is a field the pinned struct lacks that the gateway needs to
populate (e.g. a reply's `parent_id`) — decode into a small local request struct with the
needed json tags instead of the stale pinned type. Removed routes the gateway still calls
(`request(route, …)`) fail at runtime, not compile time — grep every `request`/`requestUser`
route against the current `paths.go`.
