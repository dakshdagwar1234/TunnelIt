# TunnelIt

A reverse-tunneling tool, built from raw TCP sockets, that exposes a local HTTP
server to the public internet — the core mechanism behind tools like ngrok and
SSH reverse tunnels, implemented from scratch with `asyncio`.

No tunneling library is used. The relay, framing protocol, multiplexing,
routing, auth, and reconnect logic are all custom.

## The problem

A server running on `localhost:3000` can't receive requests from the public
internet — it sits behind NAT with no inbound port. TunnelIt solves this the
way SSH reverse tunnels do: the local machine dials *out* to a public relay
first, and the relay reuses that already-open connection to push requests
back in.

```
Internet  →  Relay (public IP)  ↔  persistent TCP tunnel  ↔  Tunnel Client (laptop)  →  localhost:3000
```

The client always connects outbound. Nothing has to reach the laptop directly.

## Architecture

| Component | Responsibility |
|---|---|
| **Relay** (`relay/server.py`) | Listens on a tunnel port (clients register here) and a public port (external traffic arrives here). Routes each public request to the correct tunnel by subdomain, and matches each response back to the request that caused it. |
| **Client** (`client/client.py`) | Dials the relay once, keeps the connection open, and for each request it receives, forwards it to the local app and sends the response back down the same tunnel. |
| **Protocol** (`protocol/basic.py`) | A length-prefixed, request-ID-tagged framing format for messages sent over the tunnel — TCP is a byte stream, not a message protocol, so this layer defines where one logical message ends and the next begins. |

## What's implemented, phase by phase

1. **Basic tunnel** — one relay, one client, one request at a time, over a persistent async TCP connection.
2. **Framing protocol** — `[4-byte length][4-byte request ID][payload]`, unit-tested against coalesced, split, and byte-by-byte partial reads.
3. **Multiplexing** — many requests share one tunnel connection concurrently. Each request gets an `asyncio.Future`; a single reader loop on the tunnel matches incoming responses back to the correct Future by request ID, so responses can arrive out of order safely.
4. **Multiple tunnels** — the relay routes by subdomain, parsed from the HTTP `Host` header (the same mechanism nginx uses for virtual hosting), so one relay can serve many independent tunnels at once.
5. **Authentication** — tunnel registration requires a shared secret token; the relay rejects unauthorized registrations before a tunnel is created.
6. **Reliability** — the client auto-reconnects with exponential backoff (1s → 2s → 4s → 8s, capped), sends periodic heartbeats so dead connections are detected proactively, and in-flight requests fail fast with a `502` if the tunnel drops rather than being silently retried (retrying a non-idempotent request risks duplicating side effects).
7. **Deployment** — the relay runs on a cloud VM with a real public IP; tested end to end from an external network, including serving a full production app (not just a test page) through the tunnel.

## Running it locally

```bash
# terminal 1: a local app to expose
python -m http.server 3000

# terminal 2: the relay
python -m relay.server

# terminal 3: the tunnel client
python -m client.client --local-port 3000 --subdomain abc --token <token-from-relay/tokens.py>

# terminal 4: simulate a public request
curl -i -H "Host: abc.tunnelit.local" localhost:8080/
```

Run the unit tests:
```bash
python -m pytest tests/ -v
```

## Benchmarks

Measured with `bench/bench.py` against `bench/slow_server.py` (a stand-in
backend with a fixed artificial delay, so concurrency's effect is visible).

| Setup | Result |
|---|---|
| Local relay, sequential vs concurrent | 20 sequential requests: 4.2s. 50 concurrent requests, same tunnel: 0.3s. |
| Deployed relay (Azure, 1 vCPU) | 0 errors up to 30 concurrent requests; failures begin between 30–40, consistent with CPU throttling on a free-tier burstable VM, not a protocol issue. |

The artificial delay (0.2s) is synthetic, used to make the multiplexing
benefit measurable — it isn't a claim about raw throughput.

## Known limitations

- One TCP connection per tunnel; no connection pooling to the relay.
- No chunked/streaming request or response bodies — a full message is read before forwarding.
- No WebSocket support (the framing protocol assumes discrete request/response pairs).
- Auth is a single shared secret per relay, not per-user tokens or accounts — adequate for this project's scope, not for a multi-tenant service.
- Subdomain routing requires wildcard DNS (or, for this deployment, a free DNS provider like DuckDNS) pointed at the relay; it isn't auto-provisioned.

## Stack

Python, `asyncio`, raw TCP sockets, `pytest`. Deployed on an Azure VM.