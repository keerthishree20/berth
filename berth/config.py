"""Configuration, from a JSON file or a dictionary."""

from __future__ import annotations

import json
import pathlib
from dataclasses import dataclass, field

from .balancer import STRATEGIES


@dataclass(frozen=True)
class BackendConfig:
    name: str
    host: str
    port: int
    weight: int = 1


@dataclass(frozen=True)
class Config:
    backends: tuple[BackendConfig, ...]
    listen_host: str = "127.0.0.1"
    listen_port: int = 8080
    strategy: str = "least_connections"

    #: What a consistent-hash route keys on: "client_ip" or "header".
    hash_key: str = "client_ip"
    hash_header: str = "x-session-id"
    hash_replicas: int = 160

    #: Active health checking. Passive failure counting happens regardless, in
    #: the circuit breaker; this is the part that notices a backend recovering
    #: while no traffic is being sent to it.
    health_path: str = "/health"
    health_interval_s: float = 2.0
    health_timeout_s: float = 1.0
    unhealthy_after: int = 2
    healthy_after: int = 2

    pool_size: int = 32
    idle_timeout_s: float = 30.0
    connect_timeout_s: float = 2.0
    request_timeout_s: float = 30.0

    #: Retries on a *different* backend after a failure. Only ever applied to
    #: requests that are safe to repeat; see `proxy.is_retryable`.
    retries: int = 1

    failure_threshold: int = 5
    cooldown_s: float = 5.0
    success_threshold: int = 2

    admin_port: int | None = 8081
    access_log: bool = False

    def validate(self) -> None:
        if not self.backends:
            raise ValueError("at least one backend is required")
        if self.strategy not in STRATEGIES:
            raise ValueError(
                f"unknown strategy {self.strategy!r}; choose one of {', '.join(STRATEGIES)}")
        if self.hash_key not in ("client_ip", "header"):
            raise ValueError("hash_key must be client_ip or header")
        names = [b.name for b in self.backends]
        if len(names) != len(set(names)):
            raise ValueError("backend names must be unique; they key the hash ring")
        # Port 0 means "any free port", so two zeros are not a clash.
        if self.admin_port and self.admin_port == self.listen_port:
            raise ValueError("admin_port and listen_port must differ")

    @classmethod
    def from_dict(cls, raw: dict) -> "Config":
        backends = tuple(
            BackendConfig(
                name=entry.get("name") or f"{entry['host']}:{entry['port']}",
                host=entry["host"],
                port=int(entry["port"]),
                weight=int(entry.get("weight", 1)),
            )
            for entry in raw.get("backends", [])
        )
        known = {f.name for f in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        unknown = set(raw) - known - {"backends"}
        if unknown:
            raise ValueError(f"unknown configuration keys: {', '.join(sorted(unknown))}")
        settings = {k: v for k, v in raw.items() if k != "backends"}
        config = cls(backends=backends, **settings)
        config.validate()
        return config

    @classmethod
    def from_file(cls, path: str | pathlib.Path) -> "Config":
        return cls.from_dict(json.loads(pathlib.Path(path).read_text()))

    @classmethod
    def from_addresses(cls, addresses: list[str], **kwargs) -> "Config":
        """`host:port` strings, for the command line."""
        backends = []
        for address in addresses:
            host, _, port = address.rpartition(":")
            if not host or not port.isdigit():
                raise ValueError(f"expected host:port, got {address!r}")
            backends.append(BackendConfig(name=address, host=host, port=int(port)))
        config = cls(backends=tuple(backends), **kwargs)
        config.validate()
        return config
