"""Counters and a latency window.

Percentiles come from a bounded ring of recent samples rather than every
observation ever taken. Keeping them all would make the proxy's memory grow with
its uptime, and an average since process start stops describing the present
after the first incident anyway.
"""

from __future__ import annotations

import collections
import time
from dataclasses import dataclass, field


@dataclass
class Metrics:
    window: int = 4096

    started_at: float = field(default_factory=time.monotonic)
    requests: int = 0
    responses: int = 0
    client_errors: int = 0        # bad requests, before a backend was involved
    upstream_errors: int = 0      # a backend refused, timed out, or died
    client_disconnects: int = 0   # the client hung up mid-response; not a backend fault
    retries: int = 0
    rejected_no_backend: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    by_status: collections.Counter = field(default_factory=collections.Counter)
    _latencies: collections.deque = field(default_factory=lambda: collections.deque(maxlen=4096))

    def __post_init__(self) -> None:
        self._latencies = collections.deque(maxlen=self.window)

    def observe(self, status: int, latency_s: float) -> None:
        self.responses += 1
        self.by_status[status] += 1
        self._latencies.append(latency_s)

    def percentile(self, fraction: float) -> float:
        if not self._latencies:
            return 0.0
        ordered = sorted(self._latencies)
        at = min(int(fraction * len(ordered)), len(ordered) - 1)
        return ordered[at]

    @property
    def uptime_s(self) -> float:
        return time.monotonic() - self.started_at

    def snapshot(self) -> dict[str, object]:
        return {
            "uptime_s": round(self.uptime_s, 1),
            "requests": self.requests,
            "responses": self.responses,
            "requests_per_s": round(self.responses / self.uptime_s, 1) if self.uptime_s else 0.0,
            "client_errors": self.client_errors,
            "upstream_errors": self.upstream_errors,
            "client_disconnects": self.client_disconnects,
            "retries": self.retries,
            "rejected_no_backend": self.rejected_no_backend,
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
            "latency_ms": {
                "p50": round(self.percentile(0.50) * 1000, 2),
                "p90": round(self.percentile(0.90) * 1000, 2),
                "p99": round(self.percentile(0.99) * 1000, 2),
                "max": round(max(self._latencies, default=0.0) * 1000, 2),
                "samples": len(self._latencies),
            },
            "by_status": dict(sorted(self.by_status.items())),
        }


def prometheus(snapshot: dict, backends: list[dict]) -> str:
    lines: list[str] = []

    def gauge(name: str, help_text: str, value, labels: str = "") -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} gauge")
        lines.append(f"{name}{labels} {value}")

    gauge("berth_requests_total", "Requests accepted from clients", snapshot["requests"])
    gauge("berth_responses_total", "Responses returned to clients", snapshot["responses"])
    gauge("berth_upstream_errors_total", "Backend failures", snapshot["upstream_errors"])
    gauge("berth_retries_total", "Requests retried on another backend", snapshot["retries"])
    for quantile in ("p50", "p90", "p99"):
        gauge(f"berth_latency_{quantile}_ms",
              f"End to end latency, {quantile} over the recent window",
              snapshot["latency_ms"][quantile])
    for backend in backends:
        label = f'{{backend="{backend["name"]}"}}'
        gauge("berth_backend_up", "1 when the backend is healthy and its breaker is closed",
              1 if backend["healthy"] and backend["circuit"]["state"] != "open" else 0, label)
        gauge("berth_backend_in_flight", "Requests currently at this backend",
              backend["in_flight"], label)
        gauge("berth_backend_requests_total", "Requests sent to this backend",
              backend["requests"], label)
        gauge("berth_backend_failures_total", "Requests that failed at this backend",
              backend["failures"], label)
        gauge("berth_backend_pooled_connections", "Idle pooled connections",
              backend["pooled_connections"], label)
    return "\n".join(lines) + "\n"
