"""Consistent hashing.

The property that matters is not that keys are spread evenly. It is what happens
when the set of backends changes. Plain modulo hashing sends key `k` to
`hash(k) % n`, and changing `n` remaps almost every key at once: every sticky
session lands on a different backend, every warm cache is cold, and a single
backend leaving takes the whole cluster's hit rate with it.

A ring fixes that. Backends are placed at points around a circle and a key goes
to the first backend clockwise from its own position. Removing a backend only
affects the keys that were sitting in its arc, which is roughly `1/n` of them.
Everything else does not move at all.

The catch is that `n` points on a circle divide it very unevenly. Each backend
is therefore placed at many points, and with enough of them the arcs even out.
That is the only job `replicas` does, and it is the difference between a ring
that balances and one that does not.
"""

from __future__ import annotations

import bisect
import hashlib
from typing import Iterable, Sequence

#: Points per backend. Load spread improves as roughly 1/sqrt(replicas), so this
#: is the usual place people under-provision: 16 replicas leaves visible skew,
#: 160 does not, and the ring is only walked with a binary search either way.
DEFAULT_REPLICAS = 160


def _hash(value: str) -> int:
    # blake2b rather than md5: same speed here, and nobody has to explain in a
    # review why a broken hash is fine because it is "not used for security".
    return int.from_bytes(hashlib.blake2b(value.encode(), digest_size=8).digest(), "big")


class HashRing:
    def __init__(self, nodes: Iterable[str] = (), *, replicas: int = DEFAULT_REPLICAS):
        if replicas < 1:
            raise ValueError("replicas must be at least 1")
        self.replicas = replicas
        self._points: list[int] = []          # sorted positions on the ring
        self._owners: list[str] = []          # the node at each position
        self._nodes: set[str] = set()
        for node in nodes:
            self.add(node)

    def __len__(self) -> int:
        return len(self._nodes)

    def __contains__(self, node: str) -> bool:
        return node in self._nodes

    def nodes(self) -> list[str]:
        return sorted(self._nodes)

    def add(self, node: str) -> None:
        if node in self._nodes:
            return
        self._nodes.add(node)
        for replica in range(self.replicas):
            point = _hash(f"{node}#{replica}")
            at = bisect.bisect(self._points, point)
            self._points.insert(at, point)
            self._owners.insert(at, node)

    def remove(self, node: str) -> bool:
        if node not in self._nodes:
            return False
        self._nodes.discard(node)
        keep = [(p, o) for p, o in zip(self._points, self._owners) if o != node]
        self._points = [p for p, _ in keep]
        self._owners = [o for _, o in keep]
        return True

    def get(self, key: str) -> str | None:
        """The backend that owns this key."""
        if not self._points:
            return None
        at = bisect.bisect(self._points, _hash(key))
        return self._owners[at % len(self._owners)]

    def get_preference(self, key: str, count: int) -> list[str]:
        """The owner, then the next distinct backends clockwise.

        The fallback order for when the first choice is unhealthy. Walking the
        ring rather than picking at random keeps the substitute stable too, so a
        backend failing does not scatter its keys across the whole cluster.
        """
        if not self._points or count <= 0:
            return []
        start = bisect.bisect(self._points, _hash(key))
        chosen: list[str] = []
        for step in range(len(self._points)):
            node = self._owners[(start + step) % len(self._owners)]
            if node not in chosen:
                chosen.append(node)
                if len(chosen) == min(count, len(self._nodes)):
                    break
        return chosen

    def distribution(self, keys: Sequence[str]) -> dict[str, int]:
        """How many of these keys each backend owns. Used by the tests and by
        anyone deciding whether `replicas` is high enough."""
        counts = {node: 0 for node in self._nodes}
        for key in keys:
            owner = self.get(key)
            if owner is not None:
                counts[owner] += 1
        return counts

    def __repr__(self) -> str:
        return f"<HashRing {len(self._nodes)} nodes, {self.replicas} replicas each>"
