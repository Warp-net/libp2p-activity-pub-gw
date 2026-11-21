# bridge-gateway

A thin bridge between Warpnet and the outside networks: it makes Warpnet users
discoverable and followable from Mastodon / the Fediverse and Nostr, and brings
their accounts into Warpnet. It is agnostic to node, user, and network: it
joins Warpnet through the network's bootstrap nodes and resolves any requested
user via the public routes.

> [!WARNING]  
> The gateway was entirely vibecoded because the ActivityPub feature is nice to
> have but not necessary for Warpnet. Besides, ActivityPub is considered obsolete
> and goes against Warpnet's principles.

## Run your own gateway

Anyone can run a gateway. Its libp2p peer id and its public host come from the
state it creates on first start, so a new gateway never collides with the
official one; Warpnet users opt into yours by its peer id.

### 1. Prepare Tailscale

The gateway gets its public HTTPS host from an embedded Tailscale Funnel node:
no domain, no certificates, no open inbound ports. In a free Tailscale account:

1. In **DNS**, turn on **MagicDNS** and **HTTPS certificates**.
2. In **Access controls**, grant Funnel to your nodes:
   ```jsonc
   "nodeAttrs": [{ "target": ["autogroup:member"], "attr": ["funnel"] }]
   ```
3. In **Settings → Keys**, generate a reusable, non-ephemeral auth key
   (`tskey-auth-…`).

[DEPLOY.md](DEPLOY.md) walks through these screens step by step.

### 2. Start it

With Docker, from the repository root:

```sh
docker build -f Dockerfile.bridge-gateway -t bridge-gateway .
docker run -d --name bridge-gateway --restart always --network host \
  -v bridge-gateway-data:/data \
  -e TS_AUTHKEY=tskey-auth-... \
  -e GATEWAY_FUNNEL_HOSTNAME=my-gateway \
  bridge-gateway
```

Without Docker (Go 1.26):

```sh
go build -o bridge-gateway ./cmd/bridge-gateway
TS_AUTHKEY=tskey-auth-... GATEWAY_KEY=./data/key.pem GATEWAY_FUNNEL_DIR=./data/tsnet ./bridge-gateway
```

The `/data` volume (or the two paths above) is the gateway's only state: the
RSA key that signs its ActivityPub requests and seeds its peer id, and the
Tailscale node that owns its `*.ts.net` host. Back it up. Losing it gives the
gateway a new host and a new peer id: Fediverse followers are orphaned and
Warpnet users have to point at the gateway again. Never run two gateways from
copies of the same state, since they would fight over one peer id.

### 3. Find its address and peer id

```sh
docker logs bridge-gateway 2>&1 | grep -E "funnel node up|serving .* public routes"
```

```
level=info msg="bridge-gateway: tailscale funnel node up as my-gateway.tailXXXX.ts.net"
level=info msg="nodeserver: serving 21 public routes as 12D3KooW... (owner ...)" net=warpnet
```

The `*.ts.net` name is the gateway's host on the Fediverse: a Warpnet user
`<user-id>` becomes `@<user-id>@my-gateway.tailXXXX.ts.net`. The `12D3KooW…`
value is its peer id.

### 4. Point Warpnet at it

Each Warpnet user who wants your gateway opens **Settings → Bridge gateway**
in their node, enters the peer id as **Gateway node id**, saves and restarts
the node. From then on their node bridges Mastodon, Threads and Nostr through
your gateway and hands it the key that signs their Nostr posts.

### Configuration

| Variable | Purpose | Default |
| --- | --- | --- |
| `TS_AUTHKEY` | Tailscale auth key; needed only until the node state exists | none |
| `GATEWAY_FUNNEL_HOSTNAME` | first label of the public `*.ts.net` host | `warpnet-gw` |
| `GATEWAY_KEY` | path of the RSA signing key, created if missing | `/data/key.pem` in Docker |
| `GATEWAY_FUNNEL_DIR` | directory of the Tailscale node state | `/data/tsnet` in Docker |
| `NODE_NETWORK` | Warpnet networks to join, e.g. `warpnet,testnet` | `warpnet` |
| `GATEWAY_DISABLE_<NETWORK>` | `true` takes one joined network out | unset |
| `GATEWAY_DISCOVERY_HOST` | Mastodon instance for who-to-follow; empty disables it | `mastodon.social` |
| `GATEWAY_AP_MIRROR` | instance asked for posts the author's server will not serve; empty disables it | `mastodon.social` |
| `GATEWAY_NOSTR_RELAYS` | comma-separated `wss://` relays replacing the defaults | damus, primal, nos.lol, purplepag.es, user.kindpag.es |
| `GATEWAY_LOGS_TOKEN` | token for `/logs` and `/debug/pprof`; unset hides them | unset |
| `GATEWAY_LOGS_ADDR` | extra plain-HTTP listener for `/logs` and `/debug/pprof`, e.g. `:4080` | unset |

The gateway listens for Warpnet on TCP 4040 (`warpnet`) and 4041 (`testnet`);
`--network host` keeps those reachable from other nodes. Users whose actions
the gateway publishes to Nostr are the ones who did not opt out at sign-up; it
serves their NIP-05 names at `https://<host>/.well-known/nostr.json`.
