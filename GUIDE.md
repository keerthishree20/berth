# Berth — Complete Project Guide

A complete guide from zero to a working reverse proxy and load balancer. Covers every feature, every
design decision and the reason behind it, with the real code. It is self-contained: you can paste it
into any AI chat and ask questions about the project without sharing the repository.

**Repository:** https://github.com/keerthishree20/berth
**All projects:** https://github.com/keerthishree20

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Tech Stack & Why](#2-tech-stack--why)
3. [Project Setup from Scratch](#3-project-setup-from-scratch)
4. [Core Ideas in Plain Words](#4-core-ideas-in-plain-words)
5. [Project Structure](#5-project-structure)
6. [Life of a Request](#6-life-of-a-request)
7. [HTTP Parsing & Request Smuggling](#7-http-parsing--request-smuggling)
8. [Hop-by-Hop Headers & X-Forwarded-For](#8-hop-by-hop-headers--x-forwarded-for)
9. [Choosing a Backend: Four Strategies](#9-choosing-a-backend-four-strategies)
10. [Consistent Hashing](#10-consistent-hashing)
11. [Circuit Breaker](#11-circuit-breaker)
12. [Active Health Checks](#12-active-health-checks)
13. [Retries](#13-retries)
14. [Connection Pooling](#14-connection-pooling)
15. [Client Disconnects (the Bug the Benchmark Found)](#15-client-disconnects-the-bug-the-benchmark-found)
16. [Metrics & the Admin Port](#16-metrics--the-admin-port)
17. [Configuration](#17-configuration)
18. [Command Line](#18-command-line)
19. [Testing](#19-testing)
20. [Benchmark Against nginx](#20-benchmark-against-nginx)
21. [Deliberately Not Built](#21-deliberately-not-built)
22. [Troubleshooting](#22-troubleshooting)
23. [Complete Feature Summary](#23-complete-feature-summary)

---

## 1. Project Overview

Berth is an **HTTP reverse proxy and load balancer** written on Python's asyncio, using only the
standard library. Clients connect to Berth, and Berth forwards each request to one of several backend
servers, the same job nginx or HAProxy do in front of a website.

It does what a production load balancer does, at small scale:
- chooses a backend with one of four strategies,
- keeps sticky sessions with a consistent hash ring,
- reuses backend connections from a pool,
- stops sending traffic to a failing backend with a circuit breaker,
- notices recovery with active health checks,
- retries safe requests on a different backend,
- refuses ambiguous HTTP that attackers use for request smuggling.

It was benchmarked head to head against nginx and **loses, by about 4.5 times on one core**. That
result, and the bug the comparison uncovered, are both in this guide.

**Status:** complete. 92 tests pass, all over real sockets.

---

## 2. Tech Stack & Why

| Technology | Role | Why We Chose It |
|---|---|---|
| **Python 3.10+ asyncio** | Networking | thousands of connections on one thread with readable code |
| **Standard library only** | Runtime | no dependency; every protocol decision is visible in the code |
| **Hand-written HTTP/1.1 parser** | Protocol | full control over how ambiguous messages are refused |
| **blake2b** | Hash ring | fast and not a broken hash like md5 |
| **pytest** | Tests | real sockets and real misbehaving backends, no mocks |
| **Docker + wrk** | Benchmark | runs nginx and the load generator with pinned cores |

---

## 3. Project Setup from Scratch

```bash
git clone https://github.com/keerthishree20/berth.git
cd berth
make install     # .venv with test tools (python3.12; python3 here is 3.6)
make test        # all 92 tests, about 5 seconds
```

### Try it with two test servers
```bash
python3.12 -m http.server 9001 &
python3.12 -m http.server 9002 &
.venv/bin/python -m berth.cli --backend 127.0.0.1:9001 --backend 127.0.0.1:9002 --health-path /
curl localhost:8080/
curl localhost:8081/stats
```

`--health-path /` matters here, because `http.server` has no `/health` route and the backends would
otherwise be marked unhealthy.

---

## 4. Core Ideas in Plain Words

| Idea | Meaning |
|---|---|
| **Reverse proxy** | a server that takes client requests and passes them to backend servers |
| **Load balancing** | spreading requests across several backends |
| **Sticky session** | the same user always goes to the same backend |
| **Circuit breaker** | stop sending traffic to a backend that keeps failing, then test it carefully |
| **Health check** | a background request to see if a backend is alive |
| **Idempotent request** | one that is safe to repeat, like GET; a POST that charges a card is not |
| **Keep-alive** | reusing one TCP connection for many requests |
| **Request smuggling** | an attack where two servers disagree about where one request ends |

---

## 5. Project Structure

```
berth/
  http1.py     parsing, framing, hop-by-hop headers, smuggling refusals
  hashring.py  HashRing: consistent hashing with virtual nodes
  circuit.py   CircuitBreaker: closed, open, half-open
  backend.py   Backend: address, weight, connection pool, counters, breaker
  balancer.py  Balancer: the four strategies and their fallbacks
  health.py    HealthChecker: active health checks
  proxy.py     Proxy: accept, choose, forward, stream, retry; the admin server
  metrics.py   counters, latency window, Prometheus output
  config.py    Config and BackendConfig, validation, JSON loading
  cli.py       the command line
bench/
  versus_nginx.py   the head-to-head benchmark, cores pinned
tests/
  support.py        controllable test backends (refuse, hang, truncate, fail health)
  test_proxy.py     end to end
  test_http1.py     parsing and smuggling cases
  test_hashring.py  distribution and remapping
  test_circuit.py   the breaker state machine
```

---

## 6. Life of a Request

1. `Proxy._handle_client` accepts a connection and reads a request with `http1.read_request`.
   Ambiguous or malformed requests get `400`.
2. The body is read into memory if it is at most 1 MiB (so a retry has something to resend), or
   streamed through if larger.
3. `Balancer.choose(key)` picks an available backend.
4. If the request is idempotent and not streamed, up to `retries` other backends are lined up.
5. For each attempt, the backend's circuit breaker must allow it.
6. `_forward` takes a pooled connection, rewrites the headers, sends the request, and streams the
   response back.
7. Success is recorded on the backend. Failures are recorded and the next attempt tried.
8. If every attempt fails: `502`. If nothing is available at all: `503`.

The key part of `_serve_one`:

```python
retryable = streaming is None and head.method.upper() in IDEMPOTENT
backend = self.balancer.choose(key)
attempts = [backend]
if retryable and self.config.retries:
    attempts += list(self.balancer.others(backend, key))[:self.config.retries]

for attempt_no, target in enumerate(attempts):
    if not target.breaker.allows():
        continue
    try:
        status = await self._forward(target, head, body, streaming, client_ip, writer, keep_alive)
    except (OSError, asyncio.TimeoutError, http1.ProtocolError) as exc:
        target.record_failure()            # the backend's fault: try the next one
        continue
    except _ClientGone:
        self.metrics.client_disconnects += 1
        return False                       # the client left; not the backend's fault
    except _ResponseAlreadyStarted:
        target.record_failure()
        return False                       # bytes already sent; cannot honestly retry
    target.record_success(elapsed)
    return client_wants_keep_alive

writer.write(http1.simple_response(502, ...))
```

---

## 7. HTTP Parsing & Request Smuggling

`berth/http1.py` refuses ambiguity instead of guessing.

### Where does the body end?
```python
def framing_of(headers: Headers) -> tuple[str, int]:
    encoding = headers.get("transfer-encoding", "")
    chunked = "chunked" in encoding.lower()
    length = headers.get("content-length")
    if chunked and length is not None:
        raise ProtocolError("message carries both Transfer-Encoding and Content-Length")
    if chunked:
        return "chunked", 0
    if length is not None:
        if len(headers.get_all("content-length")) > 1:
            raise ProtocolError("more than one Content-Length")
        return "length", int(length)
    return "eof", 0
```

### Header names with spaces
```python
name, separator, value = line.partition(":")
if not separator or not name or name != name.strip():
    raise ProtocolError(f"malformed header line: {line!r}")
```

### Why refuse instead of choosing?
Request smuggling works when two servers disagree about where a message ends: the proxy sees one
request, the backend sees two, and the second is the attacker's. Each of these gets a `400`:
- both `Content-Length` and `Transfer-Encoding: chunked`,
- two different `Content-Length` values,
- a non-numeric length,
- a header name with trailing whitespace (some parsers strip it and some do not),
- too many headers.

### The `Headers` class
Keeps headers in order, allows repeats, and caches lowercase names. Caching mattered: a profile found
1.68 million `str.lower` calls in a 15,000-request sample.

---

## 8. Hop-by-Hop Headers & X-Forwarded-For

Some headers describe only one connection and must not be passed along:

```python
def without_hop_by_hop(self) -> "Headers":
    nominated = {token.strip().lower()
                 for value in self.get_all("connection")
                 for token in value.split(",") if token.strip()}
    drop = HOP_BY_HOP | nominated if nominated else HOP_BY_HOP
    ...
```

That includes any header the `Connection` header itself names. `X-Forwarded-For` is **appended to**,
not replaced, so the original client address survives a chain of proxies.

---

## 9. Choosing a Backend: Four Strategies

```python
def choose(self, key: str | None = None) -> Backend:
    healthy = self.available()          # breaker not open, health check passing
    if not healthy:
        raise NoBackendAvailable(...)
    if self.strategy == "consistent_hash" and key is not None:
        return self._by_key(key, healthy)
    if self.strategy == "least_connections":
        return min(healthy, key=lambda b: (b.in_flight / b.weight, b.name))
    if self.strategy == "random":
        return self._rng.choice(healthy)
    return self._next_round_robin(healthy)
```

| Strategy | Behaviour | When a backend slows down |
|---|---|---|
| `least_connections` (default) | fewest requests in flight, adjusted for weight | gets less work automatically |
| `round_robin` | strict rotation, respecting weight | keeps getting its full share |
| `consistent_hash` | same key, same backend | its keys stay put |
| `random` | uniform pick | the baseline the others must beat |

### A subtle bug that was fixed
The in-flight count must rise **before the first `await`**. When it rose after the backend connection
was acquired, twenty simultaneous requests all saw every backend at zero and all picked the same one.

### Why the least-connections test is careful
One burst of requests arrives before anything finishes, so every backend looks idle and
least-connections just alternates. It only differs from round robin under **sustained** load, where
the fast backend keeps freeing slots. The test drives it that way, with round robin as a control.

---

## 10. Consistent Hashing

`berth/hashring.py`:

```python
def _hash(value: str) -> int:
    return int.from_bytes(hashlib.blake2b(value.encode(), digest_size=8).digest(), "big")

def get(self, key: str) -> str | None:
    at = bisect.bisect(self._points, _hash(key))
    return self._owners[at % len(self._owners)]
```

Each backend sits at **160 points** around a ring. A key belongs to the next point clockwise.

### Why not `hash(key) % n`?
Modulo remaps almost every key when a backend is added or removed: every sticky session moves and
every warm cache goes cold. The test measures it: going from eight backends to seven under modulo
moves over 80% of keys. The ring moves only the keys of the backend that left, about one in eight.

### Why 160 points?
One point per backend divides the circle so unevenly that one node gets far more than its share.

### When a backend is down
Unhealthy backends **stay on the ring**. A key walks clockwise to the next healthy owner:

```python
for name in self._ring.get_preference(key, len(self._ring)):
    if name in usable:
        return self._by_name[name]
```

So while a backend is down, all its keys land on the **same** substitute, whose cache warms up,
instead of scattering. When it recovers, its keys come straight back.

### What is the key?
The header named by `--hash-header` (for example a session cookie or tenant id), or the client IP.
Client IP is a poor fallback: everyone behind one office gateway shares it.

---

## 11. Circuit Breaker

`berth/circuit.py`, one per backend:

```
CLOSED ──(failure_threshold failures in a row)──► OPEN
OPEN   ──(cooldown_s passes)──────────────────► HALF_OPEN
HALF_OPEN ──(success_threshold successes)──────► CLOSED
HALF_OPEN ──(one failure)──────────────────────► OPEN
```

```python
def allows(self) -> bool:
    if self.state is State.CLOSED:
        return True
    if self.state is State.OPEN:
        if self.clock() - self.opened_at < self.cooldown_s:
            self.rejected += 1
            return False
        self._enter_half_open()
    if self._probes >= self.probe_limit:     # at most 2 trial requests at once
        self.rejected += 1
        return False
    self._probes += 1
    return True

def failed(self) -> None:
    if self.state is State.HALF_OPEN:
        self._trip()                         # one failed probe is enough
        return
    self.failures += 1
    if self.failures >= self.failure_threshold:
        self._trip()
```

### Why half-open matters
Going straight from open to closed would send full load at a backend that has proved nothing. A
couple of probe requests test it first.

---

## 12. Active Health Checks

The breaker notices failure through real traffic, but an **open breaker sends no traffic**, so it can
never notice recovery. `berth/health.py` fixes that with background requests to `health_path`:

```python
def record(self, backend, ok):
    ...
    if not ok and backend.healthy and -streak >= self.unhealthy_after:
        backend.healthy = False
    elif ok and not backend.healthy and streak >= self.healthy_after:
        backend.healthy = True
        backend.breaker.reset()     # it is answering again; don't wait out the cooldown
```

- It needs a **run** of results (2 by default) before flipping, so one dropped packet does not bounce a
  healthy backend in and out.
- Health checks use their **own** connections. A pooled connection could make a dead backend look alive.

---

## 13. Retries

Only when all three are true:
1. the method is idempotent: `GET`, `HEAD`, `OPTIONS`, `TRACE`, `PUT`, `DELETE`,
2. no response byte has reached the client yet,
3. there is a **different** backend to try.

A failed `POST` gets `502` instead. Retrying it is how a proxy silently charges a card twice, and a
test says so.

Request bodies up to 1 MiB are kept so a retry can resend them. Larger bodies stream straight through
and give up retries, because you cannot replay what you did not keep.

---

## 14. Connection Pooling

```python
async def acquire(self) -> PooledConnection:
    now = time.monotonic()
    while self._pool:
        connection = self._pool.popleft()
        if now - connection.idle_since > self.idle_timeout_s or not connection.usable:
            connection.close()                 # too old or closed by the backend
            continue
        self.connections_reused += 1
        return connection
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(self.host, self.port), self.connect_timeout_s)
    # TCP_NODELAY: a proxy writes a whole request head then waits, exactly what Nagle delays
    ...
```

- Idle keep-alive connections are kept per backend, up to `pool_size` (32).
- A pooled connection is checked before reuse, because a backend can close an idle one at any moment.
- In the tests, twenty requests over one client connection open at most five backend connections.

---

## 15. Client Disconnects (the Bug the Benchmark Found)

The first benchmark reported Berth at 6,362 requests a second, **faster** than the final result. It
was wrong. wrk also reported 95,547 non-2xx responses: Berth was returning 503 quickly, and a proxy
that rejects everything quickly looks fast.

**Cause.** When a wrk run ends, it drops all its connections, often while the proxy is still writing
responses. Those write failures were caught by the same handler as a backend failing, and recorded
against the backend. A few dozen tripped the circuit breaker on both healthy backends. The warm-up run
had done exactly this, so the measured run started with every backend locked out.

**Why it matters in real life.** Clients hang up constantly: phones drop signal, browsers navigate
away, upstream load balancers time out. A proxy that blames its backends for that can take a healthy
cluster offline during an ordinary traffic spike.

**Fix.** A failure mid-response now has one of two owners:
- **reading** from the backend fails → the backend's fault, recorded against it,
- **writing** to the client fails → `_ClientGone`, counted as a client disconnect, costs the backend
  nothing.

A regression test aborts eight connections mid-response against a breaker with a threshold of two.
It was confirmed to fail on the old code before the fix.

---

## 16. Metrics & the Admin Port

The admin port (default 8081) serves:

| Path | Returns |
|---|---|
| `/stats` or `/` | JSON: overall and per-backend counters, circuit state, pooled connections, latency percentiles, client disconnects, retries |
| `/metrics` | the same in Prometheus text format |
| `/health` | whether Berth itself is up |

`--admin-port 0` disables it. `--access-log` logs one line per request.

---

## 17. Configuration

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

### All settings and defaults
| Field | Default | Meaning |
|---|---|---|
| `listen_host`, `listen_port` | 127.0.0.1, 8080 | where clients connect |
| `strategy` | `least_connections` | one of the four strategies |
| `hash_key`, `hash_header`, `hash_replicas` | `client_ip`, `x-session-id`, 160 | consistent hashing |
| `health_path`, `health_interval_s`, `health_timeout_s` | `/health`, 2.0, 1.0 | health checks |
| `unhealthy_after`, `healthy_after` | 2, 2 | results needed to flip |
| `pool_size`, `idle_timeout_s` | 32, 30.0 | connection pool |
| `connect_timeout_s`, `request_timeout_s` | 2.0, 30.0 | backend timeouts |
| `retries` | 1 | extra backends to try, idempotent only |
| `failure_threshold`, `cooldown_s`, `success_threshold` | 5, 5.0, 2 | circuit breaker |
| `admin_port`, `access_log` | 8081, false | admin server and logging |

Backend names must be unique, because they key the hash ring. The admin port must differ from the
listen port.

---

## 18. Command Line

```
berth --backend host:port [--backend host:port ...]
      --listen 127.0.0.1:8080 --admin-port 8081
      --strategy least_connections|round_robin|consistent_hash|random
      --hash-header x-session-id
      --retries 1 --health-path /health --access-log -v
berth --config berth.json
```

---

## 19. Testing

All 92 tests use real sockets and real backends from `tests/support.py` that can be told to refuse
connections, hang, truncate a response, or fail health checks. Nothing mocks the transport, because
what a proxy does when a connection misbehaves is the whole subject.

| Area | Tests |
|---|---:|
| End to end: forwarding, balancing, failure, health, pooling, admin | 32 |
| HTTP parsing, framing and smuggling cases | 37 |
| Hash ring, including remapping on membership change | 13 |
| Circuit breaker state machine | 10 |

---

## 20. Benchmark Against nginx

Same machine, same backends, cores pinned. Intel Core i5-11320H, Linux 6.8, Python 3.12, nginx 1.31.
`wrk -t2 -c64 -d15s`, 3-byte body, two backends. Reproduce with `make bench` (needs Docker).

| Setup | Requests/s | p50 | p90 | p99 |
|---|---:|---:|---:|---:|
| Direct to backend, no proxy | 176,643 | 0.32 ms | 0.42 ms | 0.60 ms |
| nginx, 4 workers on 4 cores | 70,269 | 0.74 ms | 2.03 ms | 4.65 ms |
| nginx, 1 worker on 1 core | 23,725 | 2.64 ms | 3.10 ms | 3.93 ms |
| **Berth, 1 process on 1 core** | **5,299** | **12.0 ms** | **13.2 ms** | **14.8 ms** |

**nginx wins: 4.5× like for like, 13× as it is normally deployed.**

### How the comparison was kept fair
- The backend is never the bottleneck (the direct row answers 33× faster than Berth forwards).
- Cores are pinned; the headline compares one Berth process with one nginx worker.
- The load generator has its own cores.
- nginx is configured properly: upstream keep-alive, no access log, least-connections.
- **Latencies are at saturation**: wrk pushes as hard as possible, so Berth's 12 ms median is mostly
  queueing. At a load it can keep up with, latency is far lower.

### Why nginx is faster
- The Python interpreter and event loop: coroutine switches, buffer copies and header objects in
  Python, where nginx does it all in C.
- One core: asyncio is single-threaded; nginx scales across workers.

Two profiled hot spots were fixed: cached lowercase header names, and one `asyncio.timeout` scope
instead of two `wait_for` calls. Together: 4,894 to about 5,200 requests a second, roughly 7%. Closing
the rest of the gap needs a different runtime, not tidier Python.

---

## 21. Deliberately Not Built

| Feature | Why not |
|---|---|
| TLS | terminate it in front, or add `ssl=` to the listener |
| HTTP/2 and WebSockets | upgrades are refused rather than half-supported |
| multiple processes | the biggest throughput lever; needs `SO_REUSEPORT` and per-process breaker state |
| uvloop | likely faster, but deliberately not measured, so no number is claimed |
| rate limiting, config reload | out of scope |

---

## 22. Troubleshooting

### Every request returns 503
No backend is available. Check `/stats`: breakers open, or health checks failing. If your backends
have no `/health` route, pass `--health-path` with one they do serve.

### `ValueError: admin_port and listen_port must differ`
Pick another `--admin-port`, or `0` to disable it.

### Sticky sessions do not stick
Consistent hashing uses the client IP unless you pass `--hash-header`.

### A POST failed and was not retried
Correct. Only idempotent methods are retried.

### Requests get 400
The request was ambiguous or malformed, for example both a length and chunked encoding.

---

## 23. Complete Feature Summary

### All Features Built

| # | Feature | Type | Key Files |
|---|---|---|---|
| 1 | HTTP/1.1 parsing and streaming | Protocol | `http1.py` |
| 2 | Request smuggling refusals | Security | `http1.py` |
| 3 | Hop-by-hop stripping, X-Forwarded-For | Protocol | `http1.py`, `proxy.py` |
| 4 | Four balancing strategies with weights | Routing | `balancer.py` |
| 5 | Consistent hash ring with 160 virtual nodes | Routing | `hashring.py` |
| 6 | Circuit breaker with half-open probes | Resilience | `circuit.py` |
| 7 | Active health checks with hysteresis | Resilience | `health.py` |
| 8 | Safe retries on another backend | Resilience | `proxy.py` |
| 9 | Keep-alive connection pool | Performance | `backend.py` |
| 10 | Client-disconnect attribution | Resilience | `proxy.py` |
| 11 | Stats, Prometheus and health endpoints | Ops | `metrics.py`, `proxy.py` |
| 12 | JSON config with validation | Config | `config.py` |
| 13 | Command line | Tooling | `cli.py` |
| 14 | Real-socket test suite | Testing | `tests/` |
| 15 | nginx benchmark with pinned cores | Tooling | `bench/versus_nginx.py` |

### Data Flow Architecture

```
Client ──TCP :8080──► Proxy._handle_client
  └── http1.read_request ──► framing_of (400 on ambiguity)
        └── Balancer.choose(key)
              ├── consistent_hash ──► HashRing.get_preference ──► first healthy
              ├── least_connections ──► min(in_flight / weight)
              └── round_robin / random
        └── breaker.allows()? ──► Backend.acquire() (pooled, TCP_NODELAY)
              └── send request (hop-by-hop stripped, X-Forwarded-For appended)
              └── stream response back to client
                    ├── backend read fails ──► record_failure ──► retry if idempotent
                    ├── client write fails ──► client_disconnects (backend not blamed)
                    └── success ──► record_success, return connection to pool

HealthChecker (background) ──► GET health_path on own connections ──► healthy / unhealthy ──► reset breaker
Admin :8081 ──► /stats (JSON)  /metrics (Prometheus)  /health
```

### Tech Stack at a Glance

```
Language:   Python 3.10+ (standard library only)
Networking: asyncio streams, hand-written HTTP/1.1
Routing:    least connections, round robin, random, consistent hashing (blake2b, 160 vnodes)
Resilience: circuit breaker, active health checks, safe retries, connection pooling
Testing:    pytest over real sockets with misbehaving test backends
Benchmark:  wrk and nginx in Docker, cores pinned
```
