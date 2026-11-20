# Development Guidelines

## Statelessness (hard rule)
The gateway MUST stay stateless. The ONLY data allowed on disk:
- the RSA signing key (`GATEWAY_KEY`) — HTTP-signature stability;
- the Tailscale Funnel node state (`GATEWAY_FUNNEL_DIR`) — stable hostname.

Do NOT persist anything else: no users, tweets, followers, federation sets,
caches, or registries. All such data lives in Warpnet (via the public routes)
or in the Fediverse (via ActivityPub) and must be derived from there at
runtime. If a feature seems to need local storage, derive the data from the
network instead or ask before adding it.

## Code Changes
- Make the smallest possible changes required to solve the task.
- Avoid refactoring or unrelated edits.
- Do not add fat comment blocks.
- Go filenames use hyphens, not underscores.

## Versioning
- Increment the patch version in the `version` const (`cmd/bridge-gateway/main.go`) on
  every commit.
