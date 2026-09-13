"""Choosing a backend.

Four strategies, and the interesting question is not which is fastest but what
each one does when a backend disappears.

* **round robin** spreads evenly and ignores how long requests take, so one slow
  backend still receives its full share and its queue grows without bound.
* **least connections** sends work to whoever has least in flight, which tracks
  actual capacity rather than assuming every request costs the same. A good
  default.
* **consistent hash** sends the same key to the same backend, which is what
  sticky sessions and warm caches need, and moves only that backend's share of
  keys when it leaves. See `hashring`.
* **random** is here as the baseline the others have to beat, and as a reminder
  that with enough backends it is not actually terrible.

Every strategy skips backends that are unhealthy or whose breaker is open, and
every one falls back rather than failing when its first choice is unavailable.
"""

from __future__ import annotations

import itertools
import random
from typing import Callable, Iterable, Sequence

from .backend import Backend
from .hashring import HashRing

Strategy = str
STRATEGIES = ("round_robin", "least_connections", "consistent_hash", "random")


class NoBackendAvailable(RuntimeError):
    """Every backend is unhealthy, tripped, or there are none configured."""


class Balancer:
    def __init__(self, backends: Sequence[Backend], *, strategy: Strategy = "least_connections",
                 hash_replicas: int = 160, rng: random.Random | None = None):
        if strategy not in STRATEGIES:
            raise ValueError(
                f"unknown strategy {strategy!r}; choose one of {', '.join(STRATEGIES)}")
        self.backends = list(backends)
        self.strategy = strategy
        self._by_name = {b.name: b for b in self.backends}
        self._rng = rng or random.Random()
        # The ring holds every backend, healthy or not. Removing a sick backend
        # from it would remap its keys onto neighbours and then remap them back
        # on recovery, which is the churn the ring exists to avoid. Health is
        # handled by walking to the next owner instead.
        self._ring = HashRing((b.name for b in self.backends), replicas=hash_replicas)
        self._cycle = self._weighted_cycle()

    def _weighted_cycle(self):
        entries: list[Backend] = []
        for backend in self.backends:
            entries.extend([backend] * backend.weight)
        return itertools.cycle(entries) if entries else itertools.cycle([None])

    # ------------------------------------------------------------------ choose

    def available(self) -> list[Backend]:
        return [b for b in self.backends if b.available]

    def choose(self, key: str | None = None) -> Backend:
        """One backend, or `NoBackendAvailable` if there is genuinely nothing."""
        healthy = self.available()
        if not healthy:
            raise NoBackendAvailable(
                f"none of {len(self.backends)} backends is available"
                if self.backends else "no backends configured")

        if self.strategy == "consistent_hash" and key is not None:
            return self._by_key(key, healthy)
        if self.strategy == "least_connections":
            return min(healthy, key=lambda b: (b.in_flight / b.weight, b.name))
        if self.strategy == "random":
            return self._rng.choice(healthy)
        return self._next_round_robin(healthy)

    def _by_key(self, key: str, healthy: list[Backend]) -> Backend:
        """The key's owner, or the next one clockwise that can take it.

        Walking the ring rather than rehashing keeps the substitute stable: while
        a backend is down its keys all land on the same replacement, so that
        replacement's cache warms up instead of the load scattering.
        """
        usable = {b.name for b in healthy}
        for name in self._ring.get_preference(key, len(self._ring)):
            if name in usable:
                return self._by_name[name]
        return healthy[0]

    def _next_round_robin(self, healthy: list[Backend]) -> Backend:
        usable = {b.name for b in healthy}
        for _ in range(len(self.backends) * max((b.weight for b in self.backends), default=1) + 1):
            candidate = next(self._cycle)
            if candidate is not None and candidate.name in usable:
                return candidate
        return healthy[0]

    def others(self, exclude: Backend, key: str | None = None) -> Iterable[Backend]:
        """Backends to try after one has failed, best first."""
        healthy = [b for b in self.available() if b is not exclude]
        if not healthy:
            return []
        if self.strategy == "consistent_hash" and key is not None:
            order = {name: position for position, name
                     in enumerate(self._ring.get_preference(key, len(self._ring)))}
            return sorted(healthy, key=lambda b: order.get(b.name, len(order)))
        return sorted(healthy, key=lambda b: (b.in_flight / b.weight, b.name))

    # ------------------------------------------------------------- membership

    def add(self, backend: Backend) -> None:
        self.backends.append(backend)
        self._by_name[backend.name] = backend
        self._ring.add(backend.name)
        self._cycle = self._weighted_cycle()

    def remove(self, name: str) -> bool:
        backend = self._by_name.pop(name, None)
        if backend is None:
            return False
        self.backends = [b for b in self.backends if b.name != name]
        self._ring.remove(name)
        self._cycle = self._weighted_cycle()
        backend.close_pool()
        return True

    def get(self, name: str) -> Backend | None:
        return self._by_name.get(name)

    def __len__(self) -> int:
        return len(self.backends)


def key_from(header_value: str | None, client_ip: str, mode: str) -> str:
    """What a consistent-hash route should be keyed on.

    A header is the honest choice when there is one: a session cookie or a
    tenant id identifies the thing whose locality matters. Client address is the
    fallback, and it is a poor one, because everyone behind the same corporate
    gateway hashes to the same backend.
    """
    if mode == "header" and header_value:
        return header_value
    return client_ip
