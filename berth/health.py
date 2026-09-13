"""Active health checking.

The circuit breaker already notices a backend failing, because it sees real
requests fail. What it cannot do is notice one recovering: once it is open, no
traffic goes there, so nothing tells it the backend came back. Its half-open
probe is a guess made with a real customer's request.

Active checks close that loop with traffic nobody is waiting on. A backend that
recovers is marked healthy and has its breaker reset before any user request is
risked on it.

Checks run on their own connections rather than pooled ones. A pooled connection
that a backend has quietly stopped serving would make a dead backend look alive
for as long as the socket stays open.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Iterable

from .backend import Backend
from .http1 import ProtocolError, RequestHead, Headers, read_response

log = logging.getLogger("berth.health")


class HealthChecker:
    def __init__(
        self,
        backends: Iterable[Backend],
        *,
        path: str = "/health",
        interval_s: float = 2.0,
        timeout_s: float = 1.0,
        unhealthy_after: int = 2,
        healthy_after: int = 2,
        on_change: Callable[[Backend, bool], None] | None = None,
    ):
        self.backends = list(backends)
        self.path = path
        self.interval_s = interval_s
        self.timeout_s = timeout_s
        self.unhealthy_after = unhealthy_after
        self.healthy_after = healthy_after
        self.on_change = on_change

        self._streaks: dict[str, int] = {b.name: 0 for b in self.backends}
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self.checks = 0
        self.transitions = 0

    async def check_once(self, backend: Backend) -> bool:
        """One probe. Returns whether the backend answered acceptably."""
        writer = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(backend.host, backend.port), self.timeout_s)
            head = RequestHead("GET", self.path, "HTTP/1.1", Headers([
                ("Host", backend.address),
                ("User-Agent", "berth-health/1.0"),
                ("Connection", "close"),
            ]))
            writer.write(head.encode())
            await asyncio.wait_for(writer.drain(), self.timeout_s)
            response = await asyncio.wait_for(read_response(reader), self.timeout_s)
            # Any 2xx or 3xx counts. Insisting on exactly 200 makes the check
            # brittle for no gain.
            return 200 <= response.status < 400
        except (OSError, asyncio.TimeoutError, ProtocolError):
            return False
        finally:
            if writer is not None:
                try:
                    writer.close()
                except Exception:  # noqa: BLE001
                    pass

    def record(self, backend: Backend, ok: bool) -> None:
        """Apply one result, flipping the backend only after a run of them.

        A single failed probe is noise: a dropped packet, a garbage collection
        pause. Requiring a run of them is what keeps a healthy backend from
        flapping out of the pool and back.
        """
        streak = self._streaks.get(backend.name, 0)
        streak = streak + 1 if ok else min(streak, 0) - 1
        if ok and streak < 0:
            streak = 1
        self._streaks[backend.name] = streak

        if not ok and backend.healthy and -streak >= self.unhealthy_after:
            backend.healthy = False
            self.transitions += 1
            log.warning("backend %s marked unhealthy", backend.name)
            if self.on_change:
                self.on_change(backend, False)
        elif ok and not backend.healthy and streak >= self.healthy_after:
            backend.healthy = True
            # The breaker was tripped by real failures against a backend that is
            # now answering. Leaving it open would keep the backend idle until
            # its cooldown elapses for no reason.
            backend.breaker.reset()
            self.transitions += 1
            log.info("backend %s marked healthy", backend.name)
            if self.on_change:
                self.on_change(backend, True)

    async def sweep(self) -> None:
        results = await asyncio.gather(
            *(self.check_once(backend) for backend in self.backends),
            return_exceptions=True)
        for backend, result in zip(self.backends, results):
            self.record(backend, result is True)
        self.checks += 1

    async def run(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                await self.sweep()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - a checker must not die
                log.exception("health sweep failed")
            elapsed = time.monotonic() - started
            try:
                await asyncio.wait_for(self._stop.wait(),
                                       max(self.interval_s - elapsed, 0.01))
            except asyncio.TimeoutError:
                pass

    def start(self) -> None:
        if self._task is None:
            self._stop.clear()
            self._task = asyncio.create_task(self.run(), name="berth-health")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
