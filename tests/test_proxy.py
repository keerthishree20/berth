"""End to end, over real sockets.

Every test here starts real backend servers and a real proxy and talks to it
with a real client. Mocking the transport would test the proxy's opinion of how
a connection behaves, when what it does when one misbehaves is the entire
subject.
"""

from __future__ import annotations

import asyncio

import pytest

from .conftest import wait_until
from .support import ChunkedBackend, Client, one_shot

pytestmark = pytest.mark.timeout(60)


# ---------------------------------------------------------------- forwarding


async def test_a_request_reaches_a_backend_and_the_response_comes_back(backends, make_proxy):
    pool = await backends(1, body=b"hello from the backend")
    proxy = await make_proxy(pool)

    answer = await one_shot("127.0.0.1", proxy.port, "/hello")

    assert answer.status == 200
    assert answer.body == b"hello from the backend"
    assert pool[0].received[-1].target == "/hello"


async def test_the_method_and_path_are_passed_through_untouched(backends, make_proxy):
    pool = await backends(1)
    proxy = await make_proxy(pool)

    async with Client("127.0.0.1", proxy.port) as client:
        await client.request("DELETE", "/things/7?force=1")

    received = pool[0].received[-1]
    assert received.method == "DELETE"
    assert received.target == "/things/7?force=1"


async def test_a_request_body_is_forwarded(backends, make_proxy):
    pool = await backends(1)
    proxy = await make_proxy(pool)

    async with Client("127.0.0.1", proxy.port) as client:
        await client.request("POST", "/submit", body=b'{"a":1}')

    assert pool[0].received[-1].body == b'{"a":1}'


async def test_forwarding_headers_are_added(backends, make_proxy):
    pool = await backends(1)
    proxy = await make_proxy(pool)

    await one_shot("127.0.0.1", proxy.port, "/")

    headers = pool[0].received[-1].headers
    assert headers.get("x-forwarded-for") == "127.0.0.1"
    assert headers.get("x-real-ip") == "127.0.0.1"
    assert headers.get("x-forwarded-proto") == "http"
    assert headers.get("x-forwarded-host") is not None


async def test_an_existing_forwarded_chain_is_appended_to_not_replaced(backends, make_proxy):
    """Losing the chain loses the original client, which is the one thing the
    header exists to carry."""
    pool = await backends(1)
    proxy = await make_proxy(pool)

    async with Client("127.0.0.1", proxy.port) as client:
        await client.request(headers=[("X-Forwarded-For", "203.0.113.9")])

    assert pool[0].received[-1].headers.get("x-forwarded-for") == "203.0.113.9, 127.0.0.1"


async def test_hop_by_hop_headers_do_not_reach_the_backend(backends, make_proxy):
    pool = await backends(1)
    proxy = await make_proxy(pool)

    async with Client("127.0.0.1", proxy.port) as client:
        await client.request(headers=[("Keep-Alive", "timeout=5"), ("X-Keep", "yes")])

    headers = pool[0].received[-1].headers
    assert headers.get("keep-alive") is None
    assert headers.get("x-keep") == "yes"


async def test_a_chunked_response_is_relayed(backends, make_proxy):
    pool = await backends(1, cls=ChunkedBackend)
    proxy = await make_proxy(pool)

    answer = await one_shot("127.0.0.1", proxy.port, "/stream")

    assert answer.status == 200
    assert answer.body == b"onetwothree"


async def test_a_backend_error_status_is_passed_through_unchanged(backends, make_proxy):
    """A 500 from a backend is a real answer. Turning it into a 502 would hide
    the difference between an application error and a broken backend."""
    pool = await backends(1, status=500, body=b"application blew up")
    proxy = await make_proxy(pool)

    answer = await one_shot("127.0.0.1", proxy.port, "/")
    assert answer.status == 500
    assert answer.body == b"application blew up"


async def test_a_malformed_request_gets_a_400(backends, make_proxy):
    pool = await backends(1)
    proxy = await make_proxy(pool)

    reader, writer = await asyncio.open_connection("127.0.0.1", proxy.port)
    writer.write(b"GET / HTTP/1.1\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n")
    await writer.drain()
    assert b"400" in await reader.read(200)
    writer.close()


# ------------------------------------------------------------------ balancing


async def test_round_robin_spreads_requests(backends, make_proxy):
    pool = await backends(3)
    proxy = await make_proxy(pool, strategy="round_robin")

    async with Client("127.0.0.1", proxy.port) as client:
        for _ in range(30):
            await client.request()

    counts = [len(b.received) for b in pool]
    assert sum(counts) == 30
    assert all(count == 10 for count in counts), counts


async def test_least_connections_avoids_the_slow_backend(backends, make_proxy):
    """Round robin would keep feeding the slow one its full share.

    The load has to be sustained for this to show. A single burst arrives before
    anything finishes, every backend reads as equally idle, and least-connections
    rightly alternates. The difference appears once the fast backend keeps
    freeing its slots while the slow one is still holding its own.
    """
    slow, fast = await backends(2)
    slow.delay_s = 0.25
    proxy = await make_proxy([slow, fast], strategy="least_connections")

    async def worker():
        for _ in range(10):
            await one_shot("127.0.0.1", proxy.port, "/")

    await asyncio.gather(*(worker() for _ in range(4)))
    assert len(fast.received) + len(slow.received) == 40
    assert len(fast.received) > 2 * len(slow.received), (
        f"fast {len(fast.received)}, slow {len(slow.received)}")


async def test_round_robin_does_not_avoid_the_slow_backend(backends, make_proxy):
    """The control for the test above. Same load, and the slow backend still
    gets its half."""
    slow, fast = await backends(2)
    slow.delay_s = 0.05
    proxy = await make_proxy([slow, fast], strategy="round_robin")

    async def worker():
        for _ in range(10):
            await one_shot("127.0.0.1", proxy.port, "/")

    await asyncio.gather(*(worker() for _ in range(4)))
    assert abs(len(fast.received) - len(slow.received)) <= 2


async def test_consistent_hashing_keeps_a_session_on_one_backend(backends, make_proxy):
    pool = await backends(3)
    proxy = await make_proxy(pool, strategy="consistent_hash",
                             hash_key="header", hash_header="x-session-id")

    async with Client("127.0.0.1", proxy.port) as client:
        for _ in range(12):
            await client.request(headers=[("X-Session-Id", "user-77")])

    served = [b for b in pool if b.received]
    assert len(served) == 1, "a sticky session was split across backends"
    assert len(served[0].received) == 12


async def test_different_sessions_reach_different_backends(backends, make_proxy):
    pool = await backends(4)
    proxy = await make_proxy(pool, strategy="consistent_hash",
                             hash_key="header", hash_header="x-session-id")

    async with Client("127.0.0.1", proxy.port) as client:
        for i in range(80):
            await client.request(headers=[("X-Session-Id", f"user-{i}")])

    assert sum(1 for b in pool if b.received) >= 3


# -------------------------------------------------------------------- failure


async def test_a_dead_backend_is_retried_elsewhere(backends, make_proxy):
    """The client sees a normal response. It never learns a backend died."""
    broken, healthy = await backends(2)
    broken.refuse = True
    healthy.body = b"served by the survivor"
    proxy = await make_proxy([broken, healthy], strategy="round_robin", retries=1)

    answers = [await one_shot("127.0.0.1", proxy.port, "/") for _ in range(6)]

    assert all(a.status == 200 for a in answers)
    assert all(a.body == b"served by the survivor" for a in answers)
    assert proxy.metrics.retries > 0


async def test_a_post_is_not_retried(backends, make_proxy):
    """Retrying a POST is how a proxy silently duplicates a payment. It gets a
    502 instead."""
    broken, healthy = await backends(2)
    broken.refuse = True
    proxy = await make_proxy([broken, healthy], strategy="round_robin", retries=1)

    statuses = []
    for _ in range(6):
        async with Client("127.0.0.1", proxy.port) as client:
            statuses.append((await client.request("POST", "/pay", body=b"amount=100")).status)

    assert 502 in statuses, "a failed POST should surface, not be replayed"
    assert len(broken.received) > 0


async def test_repeated_failures_trip_the_breaker_and_stop_the_traffic(backends, make_proxy):
    broken, healthy = await backends(2)
    broken.refuse = True
    proxy = await make_proxy([broken, healthy], strategy="round_robin",
                             failure_threshold=3, cooldown_s=30.0, retries=1,
                             health_interval_s=30.0)

    for _ in range(12):
        await one_shot("127.0.0.1", proxy.port, "/")

    assert proxy.balancer.get("b0").breaker.is_open
    before = len(broken.received)
    for _ in range(6):
        await one_shot("127.0.0.1", proxy.port, "/")
    assert len(broken.received) == before, "an open breaker should send nothing"


async def test_with_every_backend_down_the_client_gets_a_503(backends, make_proxy):
    pool = await backends(2)
    for backend in pool:
        backend.refuse = True
    proxy = await make_proxy(pool, failure_threshold=1, cooldown_s=30.0,
                             health_interval_s=30.0, retries=1)

    for _ in range(4):
        await one_shot("127.0.0.1", proxy.port, "/")
    answer = await one_shot("127.0.0.1", proxy.port, "/")

    assert answer.status == 503
    assert proxy.metrics.rejected_no_backend > 0


async def test_a_backend_that_dies_mid_response_does_not_produce_a_lie(backends, make_proxy):
    """Once bytes are on the way there is no honest retry: the client already
    has part of an answer. The connection ends instead of a second one being
    spliced onto the first."""
    pool = await backends(1)
    pool[0].truncate = True
    proxy = await make_proxy(pool, retries=1)

    reader, writer = await asyncio.open_connection("127.0.0.1", proxy.port)
    writer.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
    await writer.drain()
    raw = await asyncio.wait_for(reader.read(4096), 10)
    writer.close()

    assert raw.count(b"HTTP/1.1") == 1, "two responses were spliced together"
    assert proxy.metrics.upstream_errors > 0


async def test_clients_hanging_up_never_trip_a_healthy_backend(backends, make_proxy):
    """The regression the nginx benchmark found.

    A load generator closes all its connections when it finishes, often while
    the proxy is still writing responses to them. Those write failures used to
    be recorded against the backend, the breaker tripped, and a perfectly
    healthy backend was locked out, turning every following request into a 503.
    Clients hang up all the time in production. It must cost the backend
    nothing.
    """
    pool = await backends(1, body=b"x" * (4 * 1024 * 1024))
    proxy = await make_proxy(pool, failure_threshold=2, cooldown_s=600.0,
                             health_interval_s=600.0, retries=0)

    for _ in range(8):
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy.port)
        writer.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        await writer.drain()
        await reader.read(1024)       # take the head and a little body, then leave
        writer.transport.abort()      # a hard reset, not a polite close
        await asyncio.sleep(0.05)

    backend = proxy.balancer.get("b0")
    assert not backend.breaker.is_open, backend.snapshot()
    assert backend.failures == 0
    assert proxy.metrics.upstream_errors == 0
    assert proxy.metrics.client_disconnects > 0

    pool[0].body = b"still here"
    answer = await one_shot("127.0.0.1", proxy.port, "/")
    assert answer.status == 200
    assert answer.body == b"still here"


async def test_a_hanging_backend_times_out_rather_than_hanging_the_client(backends, make_proxy):
    slow, healthy = await backends(2)
    slow.hang = True
    proxy = await make_proxy([slow, healthy], strategy="round_robin",
                             request_timeout_s=0.4, retries=1, health_interval_s=30.0)

    answer = await asyncio.wait_for(one_shot("127.0.0.1", proxy.port, "/"), 10)
    assert answer.status in (200, 502)


# ------------------------------------------------------------ health checking


async def test_an_unhealthy_backend_is_taken_out_of_rotation(backends, make_proxy):
    sick, healthy = await backends(2)
    sick.healthy = False
    proxy = await make_proxy([sick, healthy], strategy="round_robin",
                             health_interval_s=0.05, unhealthy_after=2)

    await wait_until(lambda: not proxy.balancer.get("b0").healthy,
                     what="the sick backend to be marked down")
    before = len(sick.received)
    for _ in range(6):
        await one_shot("127.0.0.1", proxy.port, "/")

    assert len(sick.received) == before
    assert len(healthy.received) >= 6


async def test_a_recovered_backend_comes_back(backends, make_proxy):
    """The loop a circuit breaker cannot close on its own: with no traffic going
    to a backend, nothing tells it the backend is answering again."""
    sick, healthy = await backends(2)
    sick.healthy = False
    proxy = await make_proxy([sick, healthy], strategy="round_robin",
                             health_interval_s=0.05, unhealthy_after=2, healthy_after=2)

    await wait_until(lambda: not proxy.balancer.get("b0").healthy, what="down")
    sick.healthy = True
    await wait_until(lambda: proxy.balancer.get("b0").healthy, what="back up")

    for _ in range(6):
        await one_shot("127.0.0.1", proxy.port, "/")
    assert len(sick.received) > 0


async def test_recovery_also_resets_the_breaker(backends, make_proxy):
    sick, healthy = await backends(2)
    sick.refuse = True
    sick.healthy = False
    proxy = await make_proxy([sick, healthy], strategy="round_robin",
                             failure_threshold=1, cooldown_s=600.0,
                             health_interval_s=0.05, retries=1)

    await one_shot("127.0.0.1", proxy.port, "/")
    await wait_until(lambda: not proxy.balancer.get("b0").healthy, what="down")

    sick.refuse = False
    sick.healthy = True
    await wait_until(lambda: proxy.balancer.get("b0").healthy, what="back up")
    assert not proxy.balancer.get("b0").breaker.is_open, (
        "a 600 second cooldown would otherwise keep a working backend idle")


# ------------------------------------------------------------------- pooling


async def test_backend_connections_are_reused(backends, make_proxy):
    pool = await backends(1)
    proxy = await make_proxy(pool)

    async with Client("127.0.0.1", proxy.port) as client:
        for _ in range(20):
            await client.request()

    backend = proxy.balancer.get("b0")
    assert backend.connections_reused >= 15, backend.snapshot()
    assert backend.connections_opened <= 5


async def test_a_backend_that_closes_after_each_response_is_handled(backends, make_proxy):
    """Not every backend keeps connections alive. The proxy has to notice and
    open a new one rather than writing into a closed socket."""
    pool = await backends(1)
    pool[0].close_after_response = True
    proxy = await make_proxy(pool)

    async with Client("127.0.0.1", proxy.port) as client:
        answers = [await client.request() for _ in range(8)]

    assert all(a.status == 200 for a in answers)
    assert proxy.balancer.get("b0").connections_opened >= 8


async def test_the_client_connection_stays_open_across_requests(backends, make_proxy):
    pool = await backends(1)
    proxy = await make_proxy(pool)

    async with Client("127.0.0.1", proxy.port) as client:
        for i in range(10):
            answer = await client.request(target=f"/{i}")
            assert answer.status == 200
    assert [r.target for r in pool[0].received] == [f"/{i}" for i in range(10)]


async def test_concurrent_clients_are_all_served(backends, make_proxy):
    pool = await backends(3, delay_s=0.01)
    proxy = await make_proxy(pool, strategy="least_connections")

    answers = await asyncio.gather(*(one_shot("127.0.0.1", proxy.port, f"/{i}")
                                     for i in range(60)))
    assert all(a.status == 200 for a in answers)
    assert sum(len(b.received) for b in pool) == 60


# --------------------------------------------------------------------- admin


async def test_the_admin_port_reports_stats(backends, make_proxy):
    pool = await backends(2)
    proxy = await make_proxy(pool)
    await one_shot("127.0.0.1", proxy.port, "/")

    answer = await one_shot("127.0.0.1", proxy.admin_port, "/stats")
    assert answer.status == 200
    assert b'"backends"' in answer.body
    assert b'"latency_ms"' in answer.body


async def test_the_admin_port_serves_prometheus(backends, make_proxy):
    pool = await backends(1)
    proxy = await make_proxy(pool)
    await one_shot("127.0.0.1", proxy.port, "/")

    answer = await one_shot("127.0.0.1", proxy.admin_port, "/metrics")
    assert answer.status == 200
    assert b"berth_requests_total" in answer.body
    assert b'berth_backend_up{backend="b0"}' in answer.body


async def test_an_unknown_admin_path_is_a_404(backends, make_proxy):
    proxy = await make_proxy(await backends(1))
    answer = await one_shot("127.0.0.1", proxy.admin_port, "/nope")
    assert answer.status == 404


async def test_latency_percentiles_are_recorded(backends, make_proxy):
    pool = await backends(1, delay_s=0.01)
    proxy = await make_proxy(pool)
    for _ in range(20):
        await one_shot("127.0.0.1", proxy.port, "/")

    latency = proxy.metrics.snapshot()["latency_ms"]
    assert latency["samples"] == 20
    assert latency["p50"] >= 10
    assert latency["p99"] >= latency["p50"]
