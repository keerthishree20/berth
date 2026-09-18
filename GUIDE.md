# Berth — Complete Project Guide

## Table of Contents
1. [What is Berth?](#what-is-berth)
2. [Quick Start](#quick-start)
3. [Core Concepts](#core-concepts)
4. [Architecture](#architecture)
5. [Life of a Request](#life-of-a-request)
6. [Code Walkthrough](#code-walkthrough)
7. [Configuration](#configuration)
8. [Admin Endpoints](#admin-endpoints)
9. [Testing Strategy](#testing-strategy)
10. [Benchmarks](#benchmarks)
11. [Extending Berth](#extending-berth)
12. [Troubleshooting](#troubleshooting)

---

## What is Berth?

Berth is an HTTP/1.1 reverse proxy and load balancer written on Python's asyncio, using only the
standard library. Clients connect to Berth, and Berth forwards each request to one of several backend
servers.

It does what a production load balancer does, at small scale:
- chooses a backend with one of four strategies,
- keeps sticky sessions with a consistent hash ring,
- reuses backend connections from a pool,
- stops sending traffic to a failing backend with a circuit breaker,
- notices recovery with active health checks,
- retries safe requests on a different backend,
- refuses ambiguous HTTP that could be used for request smuggling.

It was benchmarked head to head against nginx and loses, by about 4.5 times on one core. The README
explains why and what the comparison uncovered.

---

## Quick Start

Requires Python 3.10 or newer. On this machine `python3` is 3.6, so the Makefile uses `python3.12`.

```bash
make install     # .venv with test tools; Berth itself has no dependencies
make test        # the full suite, over real sockets
```

Proxy two backends:

```bash
python -m http.server 9001 &        # any two HTTP servers will do
python -m http.server 9002 &
.venv/bin/python -m berth.cli --backend 127.0.0.1:9001 --backend 127.0.0.1:9002 --health-path /
curl localhost:8080/
curl localhost:8081/stats
```

`--health-path /` matters here, because `http.server` has no `/health` route and would otherwise be
marked unhealthy.

---

## Core Concepts

### Reverse proxy
A server that accepts client requests and forwards them to backends. Clients never talk to the
backends directly.

### Balancing strategy
| strategy | behaviour |
|---|---|
| `least_connections` (default) | the backend with the fewest requests in flight, adjusted for weight |
| `round_robin` | strict rotation, respecting weight |
| `consistent_hash` | the same key always goes to the same backend |
| `random` | a uniform pick, kept as the baseline |

### Consistent hashing
Plain `hash(key) % n` remaps most keys when a backend is added or removed. A hash ring places each
backend at 160 points around a circle, and a key belongs to the next point clockwise. Removing one of
eight backends moves only its own keys. Unhealthy backends stay on the ring, and their keys walk on
to the next healthy owner, so they come back when it recovers.

### Circuit breaker
A per-backend state machine:
- **Closed.** Normal. Failures are counted.
- **Open.** After `failure_threshold` failures in a row, no traffic is sent for `cooldown_s`.
- **Half-open.** A few probe requests go through. `success_threshold` successes close it. One
  failure reopens it.

### Active health checks
The breaker notices failure through real traffic, but an open breaker sends no traffic, so it cannot
notice recovery. A background checker requests `health_path` on each backend and marks it healthy
or unhealthy after a run of results, resetting the breaker on recovery.

### Retries
Only for idempotent methods such as GET, only before any response byte reached the client, and only
on a different backend. A failed POST gets a 502 instead, because retrying it could charge a card
twice.

### Request smuggling
When two servers disagree about where an HTTP message ends, an attacker can hide a second request
inside the first. Berth refuses ambiguous messages with a 400: both `Content-Length` and chunked
encoding, two different `Content-Length` values, or a header name with trailing whitespace.

---

## Architecture

```
   clients                      Berth (one asyncio process)
 ┌─────────┐   :8080   ┌──────────────────────────────────────┐
 │ browser │──────────►│ Proxy._handle_client                  │
 │ curl    │           │   http1.read_request                  │
 └─────────┘           │   Balancer.choose ── HashRing         │
                       │   Backend.acquire  ── connection pool │
                       │   CircuitBreaker per backend          │
                       │   stream body both ways               │
                       └──────┬───────────────┬────────────────┘
                              │               │
                    ┌─────────▼───┐     ┌─────▼───────┐
                    │ backend A   │     │ backend B   │
                    └─────────────┘     └─────────────┘
                              ▲               ▲
                              └─ HealthChecker (own connections)

   :8081  admin: /stats (JSON), /metrics (Prometheus), /health
```

Everything runs on one event loop and one core.

---

## Life of a Request

1. `Proxy._handle_client` accepts a connection and calls `http1.read_request`. Malformed or
   ambiguous requests get a 400.
2. `Balancer.choose(key)` picks an available backend. For consistent hashing the key is the
   configured header, or the client IP. A backend is unavailable if its breaker is open or it failed
   health checks.
3. The in-flight count rises **before** the first `await`. When it rose later, twenty concurrent
   requests all saw every backend at zero and all picked the same one.
4. `Backend.acquire` returns a pooled keep-alive connection, checked for liveness, or opens a new one.
5. `_upstream_request` rewrites headers: hop-by-hop headers are removed, and `X-Forwarded-For` is
   appended to.
6. The request is sent and the response streamed back to the client.
7. A failure while reading from the backend counts against the backend. A failure while writing to
   the client counts as a client disconnect and costs the backend nothing. Mixing these up was the
   bug the nginx benchmark found.
8. On a backend failure before any byte reached the client, an idempotent request is retried on
   another backend.
9. The connection returns to the pool if it can be reused.

---

## Code Walkthrough

| file | responsibility |
|---|---|
| `berth/http1.py` | `Headers` with cached lowercase names, `read_request`, `read_response`, `framing_of`, `stream_body`, smuggling refusals, hop-by-hop stripping |
| `berth/hashring.py` | `HashRing` with virtual nodes. `get(key)`, `get_preference(key, count)` and `distribution()` |
| `berth/circuit.py` | `CircuitBreaker` with `allows()`, `succeeded()`, `failed()` and the three `State`s |
| `berth/backend.py` | `Backend`: address, weight, pool, in-flight count, latency, breaker, counters |
| `berth/balancer.py` | `Balancer.choose()` for the four strategies, `others()` for retry order, `key_from()` |
| `berth/health.py` | `HealthChecker` with `check_once`, `sweep` and the run loop |
| `berth/proxy.py` | `Proxy`: accept, choose, forward, stream, retry, and the admin server |
| `berth/metrics.py` | request counts, a latency window with percentiles, Prometheus text |
| `berth/config.py` | `Config` and `BackendConfig` with validation and JSON loading |
| `berth/cli.py` | command-line parsing |

---

## Configuration

### Command line
```
berth --backend host:port [--backend host:port ...]
      --listen 127.0.0.1:8080 --admin-port 8081
      --strategy least_connections|round_robin|consistent_hash|random
      --hash-header x-session-id
      --retries 1 --health-path /health --access-log -v
berth --config berth.json
```

### JSON file
Every field of `Config` can be set. Unknown keys are rejected rather than ignored.

```json
{
  "backends": [
    {"name": "a", "host": "127.0.0.1", "port": 9001, "weight": 2},
    {"name": "b", "host": "127.0.0.1", "port": 9002}
  ],
  "listen_port": 8080,
  "strategy": "consistent_hash",
  "hash_key": "header",
  "hash_header": "x-session-id",
  "health_path": "/health",
  "failure_threshold": 5,
  "cooldown_s": 5.0
}
```

### Settings worth knowing
| field | default | meaning |
|---|---|---|
| `pool_size` | 32 | idle connections kept per backend |
| `connect_timeout_s`, `request_timeout_s` | 2, 30 | timeouts to the backend |
| `retries` | 1 | extra attempts on other backends, idempotent requests only |
| `failure_threshold`, `cooldown_s`, `success_threshold` | 5, 5.0, 2 | circuit breaker |
| `health_interval_s`, `unhealthy_after`, `healthy_after` | 2.0, 2, 2 | health checks |
| `hash_replicas` | 160 | points per backend on the ring |

Backend names must be unique, because they key the hash ring. The admin port must differ from the
listen port.

---

## Admin Endpoints

| path | returns |
|---|---|
| `/stats` | JSON with overall and per-backend counters, circuit state, pooled connections, latency percentiles, client disconnects |
| `/metrics` | the same in Prometheus text format |
| `/health` | whether Berth itself is up |

---

## Testing Strategy

Every test uses real sockets and real backends that can be told to refuse, hang, truncate a
response, or fail health checks. Nothing mocks the transport.

| file | area |
|---|---|
| `tests/test_proxy.py` | forwarding, balancing, failure, health, pooling, retries, admin, client disconnects |
| `tests/test_http1.py` | parsing, framing, smuggling cases |
| `tests/test_hashring.py` | distribution and how many keys move when membership changes |
| `tests/test_circuit.py` | the breaker state machine |
| `tests/support.py` | the controllable test backends |

---

## Benchmarks

```bash
make bench     # head to head against nginx with pinned cores. Needs Docker and wrk
```

Results, the fairness rules and the profile are in the README. Quote the headline as Berth on one
core against nginx on one core, and mention that latencies were measured at saturation.

---

## Extending Berth

Left out on purpose:
- **TLS.** Terminate it in front, or add `ssl=` to the listener.
- **Multiple processes.** The biggest throughput lever. Needs `SO_REUSEPORT` and per-process breaker
  state.
- **HTTP/2 and WebSockets.** Upgrades are refused rather than half-supported.
- **Rate limiting, config reload, uvloop.**

---

## Troubleshooting

### Every request returns 503
No backend is available. Check `/stats`: either the breakers are open or health checks are failing.
If your backends have no `/health` route, pass `--health-path` with a path they do serve.

### `ValueError: admin_port and listen_port must differ`
Pick a different `--admin-port`, or `0` to disable the admin server.

### Sticky sessions do not stick
Consistent hashing keys on the client IP unless you pass `--hash-header`. Everyone behind one
gateway shares an IP.

### A POST failed and was not retried
Correct behaviour. Only idempotent methods are retried.

### Requests get 400
The request was ambiguous or malformed, for example both `Content-Length` and
`Transfer-Encoding: chunked`. Fix the client.
