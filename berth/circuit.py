"""Circuit breaker.

A backend that is failing does not need more traffic sent at it. Worse, every
request that will eventually fail still occupies a connection and a timeout's
worth of waiting, so a single sick backend can consume the proxy's capacity and
take healthy backends down with it. The breaker stops that by refusing to try.

Three states, and the middle one is the whole point:

    closed ──── failures reach the threshold ────► open
      ▲                                             │
      │                                    cooldown elapses
      │                                             ▼
      └──── enough probes succeed ──────────── half-open
                                                    │
                              any probe fails ──────┘  (back to open)

Half-open is what makes recovery automatic. Going straight from open back to
closed would send the full load at a backend that has proved nothing; half-open
lets a couple of requests through and decides based on what happens to them.
"""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field


class State(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    #: Consecutive failures that trip the breaker.
    failure_threshold: int = 5
    #: How long to refuse everything before letting a probe through.
    cooldown_s: float = 5.0
    #: Consecutive successful probes needed to close it again.
    success_threshold: int = 2
    #: Probes allowed at once while half-open. More than a couple defeats the
    #: purpose: the point is to risk a little traffic, not all of it.
    probe_limit: int = 2
    #: Injected so tests can move time without sleeping through it.
    clock: "callable" = time.monotonic

    state: State = field(default=State.CLOSED, init=False)
    failures: int = field(default=0, init=False)
    successes: int = field(default=0, init=False)
    opened_at: float = field(default=0.0, init=False)
    trips: int = field(default=0, init=False)
    rejected: int = field(default=0, init=False)
    _probes: int = field(default=0, init=False)

    def allows(self) -> bool:
        """Whether a request may be sent. Call `succeeded` or `failed` after."""
        if self.state is State.CLOSED:
            return True

        if self.state is State.OPEN:
            if self.clock() - self.opened_at < self.cooldown_s:
                self.rejected += 1
                return False
            self._enter_half_open()

        if self._probes >= self.probe_limit:
            self.rejected += 1
            return False
        self._probes += 1
        return True

    def succeeded(self) -> None:
        self.failures = 0
        if self.state is State.HALF_OPEN:
            self._probes = max(self._probes - 1, 0)
            self.successes += 1
            if self.successes >= self.success_threshold:
                self.state = State.CLOSED
                self.successes = 0
                self._probes = 0

    def failed(self) -> None:
        if self.state is State.HALF_OPEN:
            # One failed probe is enough. The backend was given its chance.
            self._trip()
            return
        self.failures += 1
        if self.failures >= self.failure_threshold:
            self._trip()

    def _trip(self) -> None:
        if self.state is not State.OPEN:
            self.trips += 1
        self.state = State.OPEN
        self.opened_at = self.clock()
        self.failures = 0
        self.successes = 0
        self._probes = 0

    def _enter_half_open(self) -> None:
        self.state = State.HALF_OPEN
        self.successes = 0
        self._probes = 0

    def reset(self) -> None:
        self.state = State.CLOSED
        self.failures = 0
        self.successes = 0
        self._probes = 0

    @property
    def is_open(self) -> bool:
        return self.state is State.OPEN

    def snapshot(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "failures": self.failures,
            "trips": self.trips,
            "rejected": self.rejected,
        }
