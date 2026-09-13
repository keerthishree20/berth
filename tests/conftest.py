from __future__ import annotations

import asyncio
from typing import Callable

import pytest
import pytest_asyncio

from berth import Config, Proxy
from berth.config import BackendConfig

from .support import ChunkedBackend, FakeBackend


@pytest_asyncio.fixture
async def backends() -> Callable:
    """Start as many controllable backends as a test asks for."""
    started: list[FakeBackend] = []

    async def start(count: int = 2, *, cls=FakeBackend, **kwargs) -> list[FakeBackend]:
        made = [await cls(f"b{i}", **kwargs).start() for i in range(count)]
        started.extend(made)
        return made

    yield start
    for backend in started:
        await backend.stop()


@pytest_asyncio.fixture
async def make_proxy() -> Callable:
    """A proxy on an ephemeral port, torn down after the test."""
    running: list[Proxy] = []

    async def start(pool: list[FakeBackend], **settings) -> Proxy:
        settings.setdefault("health_interval_s", 0.05)
        settings.setdefault("health_timeout_s", 0.5)
        settings.setdefault("connect_timeout_s", 0.5)
        settings.setdefault("request_timeout_s", 2.0)
        config = Config(
            backends=tuple(BackendConfig(b.name, "127.0.0.1", b.port) for b in pool),
            listen_host="127.0.0.1", listen_port=0, admin_port=0, **settings)
        proxy = Proxy(config)
        await proxy.start()
        running.append(proxy)
        return proxy

    yield start
    for proxy in running:
        await proxy.stop()


async def wait_until(predicate, *, timeout_s: float = 5.0, interval_s: float = 0.02,
                     what: str = "condition"):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        result = predicate()
        if result:
            return result
        await asyncio.sleep(interval_s)
    pytest.fail(f"timed out after {timeout_s:g}s waiting for {what}")
