# Berth

An HTTP reverse proxy and load balancer on asyncio. Health checks, consistent
hashing for sticky routing, connection pooling, and a circuit breaker per
backend. No dependencies outside the standard library.

```bash
berth --backend 127.0.0.1:9001 --backend 127.0.0.1:9002
curl localhost:8081/stats
```

It loses to nginx by about four and a half times on one core. The numbers, the
reasons, and the bug the comparison uncovered are all below.

---

## Measured against nginx

Same machine, same backends, cores pinned. Intel Core i5-11320H, 8 threads,
Linux 6.8, Python 3.12.13, nginx 1.31.5. Reproduce with `make bench`.

`wrk -t2 -c64 -d15s`, a 3 byte body, two backends:

| Setup | Requests/s | p50 | p90 | p99 |
|---|---:|---:|---:|---:|
| Direct to backend, no proxy | 176,643 | 0.32 ms | 0.42 ms | 0.60 ms |
| nginx, 4 workers on 4 cores | 70,269 | 0.74 ms | 2.03 ms | 4.65 ms |
| nginx, 1 worker on 1 core | 23,725 | 2.64 ms | 3.10 ms | 3.93 ms |
| **Berth, 1 process on 1 core** | **5,299** | **12.0 ms** | **13.2 ms** | **14.8 ms** |

**nginx wins, by 4.5 times like for like and 13 times as it is normally
deployed.** That was the expected result and the brief for this project was to
report it rather than hide it.

### How the comparison was kept fair

- **The backend is never the bottleneck.** Both proxies forward to nginx serving
  a fixed body, which the direct row shows answering 33 times faster than Berth
  can forward.
- **Cores are pinned.** Berth is one Python process, so the headline row is nginx
  restricted to one worker on the same core. Four workers is reported too,
  because leaving it out would flatter Berth.
- **The load generator has its own cores**, and the backends have theirs.
- **nginx is configured competently**: upstream keep-alive, no access log, the
  same least-connections balancing Berth uses by default.
- **Latencies are at saturation.** wrk drives each proxy as hard as it will go,
  so Berth's 12 ms median is mostly time spent queued. At a load it can keep up
  with, its latency is far lower. wrk cannot hold a fixed rate to show that, and
  no number is quoted for it.

### Why nginx is faster

A profile of Berth under load says where the time goes:

- **The interpreter and the event loop.** Every request passes through several
  coroutine switches, stream buffer copies, and header objects built in Python.
  nginx does the same work in C with no garbage collector and no per-object
  overhead.
- **One core.** asyncio runs on a single thread. nginx scales across workers
  almost linearly, which is the gap between its two rows.

Two hot spots in that profile were pure overhead, so they were fixed and
measured:

| Change | Why | Effect |
|---|---|---|
| Cache lowercased header names | 1.68 million `str.lower` calls in a 15,000 request sample, about 110 per request | combined |
| One `asyncio.timeout` scope instead of two `wait_for` calls | each `wait_for` builds a task and a timer | combined |
| **Both together** | | **4,894 to about 5,200 requests/s, roughly 7%** |

Seven percent is what the profile predicted and it is all there was to find at
that level. Closing the rest of the gap needs a different runtime rather than
tidier Python.

---

## The bug the benchmark found

The first run reported Berth at 6,362 requests per second, faster than the
final result. It was wrong. wrk also reported 95,547 non-2xx responses, which is
nearly every request in the run. Berth was returning 503 quickly, and a proxy
that rejects everything quickly looks fast.

**Cause.** When a wrk run ends it drops all its connections, often while the
proxy is still writing responses into them. Those write failures were caught by
the same handler as a backend failing mid-response, and recorded against the
backend. A few dozen of them tripped the circuit breaker on both healthy
backends. The warm-up run had done exactly this, so the measured run started
with every backend locked out.

**Why it matters beyond a benchmark.** Clients hang up constantly in production:
mobile connections drop, browsers navigate away, load balancers upstream time
out. A proxy that counts those against its backends can take a healthy cluster
out of service during an ordinary traffic spike.

**Fix.** A failure mid-response now has one of two owners. Reading the body is
the backend's side and still counts against it. Writing the body is the client's
side, is counted as a client disconnect, and costs the backend nothing. There is
a regression test that aborts eight connections mid-response against a breaker
with a threshold of two. It was confirmed to fail on the old behaviour before the
fix went in.

---

## How it works

### Choosing a backend

| Strategy | Behaviour | When a backend slows down |
|---|---|---|
| `least_connections` (default) | fewest requests in flight, adjusted for weight | receives less work automatically |
| `round_robin` | strict rotation, respects weight | keeps receiving its full share |
| `consistent_hash` | same key, same backend | its keys stay put; see below |
| `random` | uniform pick | here as the baseline the others must beat |

The least-connections test is careful about one detail. A single burst of
requests arrives before anything finishes, so every backend reads as idle and
least-connections correctly alternates. The strategy only diverges from round
robin under sustained load, where the fast backend keeps freeing slots, and the
test drives it that way with round robin as a control.

The in-flight count also has to rise before the first `await`. When it rose
after the backend connection was acquired, twenty concurrent requests all saw
every backend at zero and all picked the same one.

### Consistent hashing

Plain modulo hashing remaps almost every key when a backend is added or removed.
Every sticky session moves and every warm cache goes cold at once. The test
suite measures this directly: going from eight backends to seven under modulo
hashing moves over 80% of keys.

A ring moves only the keys that were on the backend that left, about one in
eight here, and none of the others. Each backend sits at 160 points on the ring,
because a single point per backend divides the circle so unevenly that one node
ends up far over its share. That is tested too.

Unhealthy backends stay on the ring. Removing them would remap their keys, then
remap them back on recovery. Instead a key walks clockwise to the next healthy
owner, so while a backend is down all of its keys land on the same substitute
and that one cache warms up rather than every cache diluting.

### Circuit breaker

Closed, open, half-open. A run of failures opens it and all traffic to that
backend stops. After a cooldown, half-open lets a couple of probe requests
through. Enough successes close it; one failure reopens it.

Half-open is the part that matters. Going straight from open to closed would
send full load at a backend that has proved nothing.

### Active health checks

The breaker notices a backend failing because real requests fail. It cannot
notice one recovering, since an open breaker sends it nothing. Active checks
close that loop with traffic nobody is waiting on. A recovered backend is marked
healthy and has its breaker reset, instead of sitting idle for the rest of its
cooldown.

Checks need a run of results before flipping state, so one dropped packet does
not bounce a healthy backend out of the pool. They use their own connections,
because a pooled connection could make a dead backend look alive.

### Retries

Only for idempotent methods, only before any response byte has reached the
client, and only on a different backend. A POST that fails gets a 502. Retrying
it is how a proxy silently charges a card twice, and there is a test that says
so.

Request bodies up to 1 MiB are held so a retry has something to send. Larger ones
stream straight through and give up retries, since you cannot replay what you did
not keep.

### Pooling

Idle keep-alive connections are kept per backend and reused. In the tests,
twenty requests over one client connection open at most five backend
connections. A pooled connection is checked before reuse, because a backend can
close an idle connection at any moment, and a backend that closes after every
response is handled by opening a fresh one.

### HTTP correctness

The parser refuses rather than resolves ambiguity. A message with both
`Content-Length` and `Transfer-Encoding: chunked`, two different
`Content-Length` values, or a header name with trailing whitespace gets a 400.
Each is a known request smuggling vector: two hops disagreeing about where a
message ends. Hop-by-hop headers are stripped, including any the `Connection`
header nominates. `X-Forwarded-For` is appended to rather than replaced, so the
original client survives a chain of proxies.

---

## Tests

92 tests, all over real sockets with real backends that can be told to refuse,
hang, truncate a response or fail health checks. None mock the transport,
because what a proxy does when a connection misbehaves is the whole subject.

| Area | Tests |
|---|---:|
| End to end: forwarding, balancing, failure, health, pooling, admin | 32 |
| HTTP parsing, framing and smuggling cases | 37 |
| Hash ring, including remapping on membership change | 13 |
| Circuit breaker state machine | 10 |

```
$ .venv/bin/python -m pytest -q
92 passed in 5.11s
```

## Running it

```bash
make install
make test
make bench      # needs Docker for nginx and wrk
```

```
berth --backend host:port [--backend host:port ...]
      --listen 127.0.0.1:8080 --admin-port 8081
      --strategy least_connections|round_robin|consistent_hash|random
      --hash-header x-session-id
      --retries 1 --health-path /health --access-log
berth --config berth.json
```

The admin port serves `/stats` as JSON, `/metrics` for Prometheus, and
`/health`. Every counter is per backend as well as overall, including circuit
state, pooled connections and client disconnects.

## Not built

- **TLS.** Terminate it in front, or add `ssl=` to the listener. Out of scope.
- **HTTP/2 and WebSockets.** Upgrades are refused rather than half-supported.
- **Multiple processes.** The largest single lever on throughput, and the one
  that would close most of the gap to nginx's four-worker row. It needs
  `SO_REUSEPORT` and per-process state for the breakers, which is its own design.
- **uvloop.** Likely faster, and deliberately not measured here, so no number is
  claimed.
- **Rate limiting.**
- **Configuration reload without a restart.**

## Layout

```
berth/
  http1.py     parsing, framing, hop-by-hop headers, smuggling refusals
  hashring.py  consistent hashing with virtual nodes
  circuit.py   closed, open, half-open
  backend.py   a backend, its connection pool, its counters
  balancer.py  the four strategies and their fallbacks
  health.py    active health checks
  proxy.py     the server: accept, choose, forward, stream, retry
  metrics.py   counters, latency window, Prometheus output
  config.py    configuration and validation
  cli.py       the command line
bench/
  versus_nginx.py   the head to head, cores pinned
tests/              92 tests, real sockets throughout
```
