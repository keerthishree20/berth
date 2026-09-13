"""A controllable backend and a minimal client, for the end-to-end tests.

Real sockets throughout. Mocking the transport would test the proxy's opinion of
how a connection behaves, when the whole subject is what happens when one does
something unexpected.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from berth import http1


@dataclass
class Received:
    method: str
    target: str
    headers: http1.Headers
    body: bytes


class FakeBackend:
    """An HTTP server that can be told to misbehave."""

    def __init__(self, name: str = "backend", *, status: int = 200, body: bytes = b"ok",
                 delay_s: float = 0.0, healthy: bool = True):
        self.name = name
        self.status = status
        self.body = body
        self.delay_s = delay_s
        self.healthy = healthy

        #: Make every request fail in a particular way.
        self.refuse = False          # accept then close immediately
        self.hang = False            # accept, never answer
        self.truncate = False        # send a head promising a body, then close
        self.close_after_response = False

        self.received: list[Received] = []
        self.health_checks = 0
        self.connections = 0
        self.requests_per_connection: list[int] = []

        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()

    @property
    def port(self) -> int:
        return self._server.sockets[0].getsockname()[1] if self._server else 0

    @property
    def address(self) -> tuple[str, int]:
        return ("127.0.0.1", self.port)

    async def start(self) -> "FakeBackend":
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        return self

    async def stop(self) -> None:
        # Close live connections first. A handler parked in `hang` would
        # otherwise keep wait_closed() waiting for an hour.
        for writer in list(self._writers):
            try:
                writer.close()
            except Exception:  # noqa: BLE001
                pass
        self._writers.clear()
        if self._server is not None:
            self._server.close()
            try:
                await asyncio.wait_for(self._server.wait_closed(), 5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            self._server = None

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connections += 1
        self._writers.add(writer)
        served = 0
        try:
            while True:
                head = await http1.read_request(reader)
                if head is None:
                    break
                framing, length = http1.framing_of(head.headers)
                body = b""
                if http1.request_has_body(head):
                    body = b"".join([b async for b in http1.stream_body(reader, framing, length)])

                if head.target.startswith("/health"):
                    self.health_checks += 1
                    status = 200 if self.healthy else 503
                    writer.write(http1.simple_response(status, b"health\n"))
                    await writer.drain()
                    break

                self.received.append(Received(head.method, head.target, head.headers, body))
                served += 1

                if self.refuse:
                    break
                if self.hang:
                    await asyncio.sleep(3600)
                if self.delay_s:
                    await asyncio.sleep(self.delay_s)
                if self.truncate:
                    writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\nshort")
                    await writer.drain()
                    break

                payload = self.body if isinstance(self.body, bytes) else str(self.body).encode()
                headers = http1.Headers([
                    ("Content-Type", "text/plain"),
                    ("Content-Length", str(len(payload))),
                    ("X-Served-By", self.name),
                    ("Connection", "close" if self.close_after_response else "keep-alive"),
                ])
                writer.write(http1.ResponseHead("HTTP/1.1", self.status,
                                                http1.status_line(self.status),
                                                headers).encode() + payload)
                await writer.drain()
                if self.close_after_response:
                    break
        except (http1.ProtocolError, ConnectionResetError, BrokenPipeError,
                asyncio.IncompleteReadError, asyncio.CancelledError):
            pass
        finally:
            self._writers.discard(writer)
            self.requests_per_connection.append(served)
            try:
                writer.close()
            except Exception:  # noqa: BLE001
                pass


class ChunkedBackend(FakeBackend):
    """Answers with a chunked body, to exercise the other framing."""

    async def _serve(self, reader, writer) -> None:
        self.connections += 1
        self._writers.add(writer)
        try:
            while True:
                head = await http1.read_request(reader)
                if head is None:
                    break
                if head.target.startswith("/health"):
                    self.health_checks += 1
                    writer.write(http1.simple_response(200, b"health\n"))
                    await writer.drain()
                    break
                self.received.append(Received(head.method, head.target, head.headers, b""))
                headers = http1.Headers([
                    ("Content-Type", "text/plain"),
                    ("Transfer-Encoding", "chunked"),
                    ("X-Served-By", self.name),
                ])
                writer.write(http1.ResponseHead("HTTP/1.1", 200, "OK", headers).encode())
                for piece in (b"one", b"two", b"three"):
                    writer.write(f"{len(piece):x}\r\n".encode() + piece + b"\r\n")
                    await writer.drain()
                writer.write(b"0\r\n\r\n")
                await writer.drain()
        except Exception:  # noqa: BLE001
            pass
        finally:
            self._writers.discard(writer)
            try:
                writer.close()
            except Exception:  # noqa: BLE001
                pass


@dataclass
class Answer:
    status: int
    headers: http1.Headers
    body: bytes

    def header(self, name: str) -> str | None:
        return self.headers.get(name)


class Client:
    """One keep-alive connection, so pooling and reuse can be observed."""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def __aenter__(self) -> "Client":
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        return self

    async def __aexit__(self, *_exc) -> None:
        await self.close()

    async def close(self) -> None:
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:  # noqa: BLE001
                pass
            self._writer = None

    async def request(self, method: str = "GET", target: str = "/", *,
                      headers: list[tuple[str, str]] | None = None,
                      body: bytes | None = None, close: bool = False,
                      timeout_s: float = 10.0) -> Answer:
        assert self._writer is not None and self._reader is not None
        head_headers = http1.Headers([("Host", f"{self.host}:{self.port}")])
        for name, value in headers or []:
            head_headers.add(name, value)
        if body is not None:
            head_headers.set("Content-Length", str(len(body)))
        head_headers.set("Connection", "close" if close else "keep-alive")

        self._writer.write(http1.RequestHead(method, target, "HTTP/1.1", head_headers).encode())
        if body:
            self._writer.write(body)
        await self._writer.drain()

        response = await asyncio.wait_for(http1.read_response(self._reader), timeout_s)
        payload = b""
        if http1.response_has_body(method, response.status):
            framing, length = http1.framing_of(response.headers)
            payload = b"".join([b async for b in
                                http1.stream_body(self._reader, framing, length)])
        return Answer(response.status, response.headers, payload)


async def one_shot(host: str, port: int, target: str = "/", **kwargs) -> Answer:
    async with Client(host, port) as client:
        return await client.request(target=target, close=True, **kwargs)
