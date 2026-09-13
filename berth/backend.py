"""A backend, its connection pool, and the counters that describe it.

Connection pooling is the least glamorous part of a proxy and often the largest
single win. Without it every proxied request pays for a TCP handshake, and under
load the machine also runs out of ephemeral ports and fills with sockets in
TIME_WAIT. With it, a busy proxy holds a handful of connections open per backend
and reuses them for the life of the process.
"""

from __future__ import annotations

import asyncio
import collections
import time
from dataclasses import dataclass, field

from .circuit import CircuitBreaker


@dataclass
class PooledConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    idle_since: float = field(default_factory=time.monotonic)

    @property
    def usable(self) -> bool:
        # A backend can close an idle keep-alive connection at any moment, so a
        # pooled connection is a guess until it is checked.
        return not self.writer.is_closing() and not self.reader.at_eof()

    def close(self) -> None:
        try:
            self.writer.close()
        except Exception:  # noqa: BLE001 - closing a dead socket is not news
            pass


class Backend:
    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        *,
        weight: int = 1,
        pool_size: int = 32,
        idle_timeout_s: float = 30.0,
        connect_timeout_s: float = 2.0,
        breaker: CircuitBreaker | None = None,
    ):
        self.name = name
        self.host = host
        self.port = port
        self.weight = max(weight, 1)
        self.pool_size = pool_size
        self.idle_timeout_s = idle_timeout_s
        self.connect_timeout_s = connect_timeout_s
        self.breaker = breaker or CircuitBreaker()

        self.healthy = True
        self.in_flight = 0
        self.requests = 0
        self.failures = 0
        self.total_latency_s = 0.0
        self.connections_opened = 0
        self.connections_reused = 0

        self._pool: collections.deque[PooledConnection] = collections.deque()

    # ------------------------------------------------------------------ status

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def available(self) -> bool:
        """Healthy, and the breaker is willing to let something through."""
        return self.healthy and not self.breaker.is_open

    @property
    def mean_latency_ms(self) -> float:
        return (self.total_latency_s / self.requests * 1000) if self.requests else 0.0

    @property
    def pooled(self) -> int:
        return len(self._pool)

    def snapshot(self) -> dict[str, object]:
        return {
            "name": self.name,
            "address": self.address,
            "healthy": self.healthy,
            "weight": self.weight,
            "in_flight": self.in_flight,
            "requests": self.requests,
            "failures": self.failures,
            "mean_latency_ms": round(self.mean_latency_ms, 2),
            "pooled_connections": self.pooled,
            "connections_opened": self.connections_opened,
            "connections_reused": self.connections_reused,
            "circuit": self.breaker.snapshot(),
        }

    # -------------------------------------------------------------------- pool

    async def acquire(self) -> PooledConnection:
        """A connection to this backend, reused if one is sitting idle."""
        now = time.monotonic()
        while self._pool:
            connection = self._pool.popleft()
            if now - connection.idle_since > self.idle_timeout_s or not connection.usable:
                connection.close()
                continue
            self.connections_reused += 1
            return connection

        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port), self.connect_timeout_s)
        # Disable Nagle: a proxy writes a complete request head in one go and
        # then waits, which is exactly the pattern Nagle delays.
        try:
            writer.get_extra_info("socket").setsockopt(
                __import__("socket").IPPROTO_TCP, __import__("socket").TCP_NODELAY, 1)
        except Exception:  # noqa: BLE001 - not every transport has a socket
            pass
        self.connections_opened += 1
        return PooledConnection(reader, writer)

    def release(self, connection: PooledConnection, *, reusable: bool) -> None:
        if not reusable or len(self._pool) >= self.pool_size or not connection.usable:
            connection.close()
            return
        connection.idle_since = time.monotonic()
        self._pool.append(connection)

    def close_pool(self) -> None:
        while self._pool:
            self._pool.popleft().close()

    # ----------------------------------------------------------------- outcome

    def record_success(self, latency_s: float) -> None:
        self.requests += 1
        self.total_latency_s += latency_s
        self.breaker.succeeded()

    def record_failure(self) -> None:
        self.requests += 1
        self.failures += 1
        self.breaker.failed()

    def __repr__(self) -> str:
        state = "up" if self.available else "down"
        return f"<Backend {self.name} {self.address} {state}>"
