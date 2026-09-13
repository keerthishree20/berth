"""The proxy itself.

One asyncio task per client connection. Each one loops: read a request, pick a
backend, forward, stream the response back, repeat until the client goes away.

Two decisions shape everything else here.

**Bodies are streamed, not buffered.** A proxy that reads whole bodies into
memory turns one large upload into a memory problem for every other connection
it is serving. The exception is small request bodies, which are held precisely
so that a failed request can be retried somewhere else; past a threshold the
body streams and retries are switched off, because you cannot replay what you
did not keep.

**Only safe requests are retried.** A GET that never reached a backend can go to
another one. A POST cannot, unless the proxy can prove the first backend never
saw it, and it usually cannot. Retrying anyway is how a proxy silently
duplicates a payment.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import AsyncIterator

from . import http1
from .backend import Backend, PooledConnection
from .balancer import Balancer, NoBackendAvailable, key_from
from .circuit import CircuitBreaker
from .config import Config
from .health import HealthChecker
from .metrics import Metrics, prometheus

log = logging.getLogger("berth.proxy")

#: Methods a proxy may repeat on its own initiative. POST and PATCH are absent
#: on purpose.
IDEMPOTENT = frozenset({"GET", "HEAD", "OPTIONS", "TRACE", "PUT", "DELETE"})

#: Request bodies up to this size are held so the request can be retried.
#: Anything larger streams straight through and forfeits retries.
RETRY_BUFFER_BYTES = 1024 * 1024


class Proxy:
    def __init__(self, config: Config):
        config.validate()
        self.config = config
        self.backends = [
            Backend(
                entry.name, entry.host, entry.port, weight=entry.weight,
                pool_size=config.pool_size, idle_timeout_s=config.idle_timeout_s,
                connect_timeout_s=config.connect_timeout_s,
                breaker=CircuitBreaker(
                    failure_threshold=config.failure_threshold,
                    cooldown_s=config.cooldown_s,
                    success_threshold=config.success_threshold),
            )
            for entry in config.backends
        ]
        self.balancer = Balancer(self.backends, strategy=config.strategy,
                                 hash_replicas=config.hash_replicas)
        self.health = HealthChecker(
            self.backends, path=config.health_path, interval_s=config.health_interval_s,
            timeout_s=config.health_timeout_s, unhealthy_after=config.unhealthy_after,
            healthy_after=config.healthy_after)
        self.metrics = Metrics()

        self._server: asyncio.Server | None = None
        self._admin: asyncio.Server | None = None

    # ------------------------------------------------------------------ server

    @property
    def port(self) -> int:
        return self._server.sockets[0].getsockname()[1] if self._server else 0

    @property
    def admin_port(self) -> int:
        return self._admin.sockets[0].getsockname()[1] if self._admin else 0

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, self.config.listen_host, self.config.listen_port)
        if self.config.admin_port is not None:
            self._admin = await asyncio.start_server(
                self._handle_admin, self.config.listen_host, self.config.admin_port)
        self.health.start()
        log.info("listening on %s:%s with %d backends",
                 self.config.listen_host, self.port, len(self.backends))

    async def stop(self) -> None:
        await self.health.stop()
        for server in (self._server, self._admin):
            if server is not None:
                server.close()
                await server.wait_closed()
        for backend in self.backends:
            backend.close_pool()
        self._server = self._admin = None

    async def serve_forever(self) -> None:
        if self._server is None:
            await self.start()
        async with self._server:
            await self._server.serve_forever()

    # --------------------------------------------------------------- the loop

    async def _handle_client(self, reader: asyncio.StreamReader,
                             writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        client_ip = peer[0] if peer else "unknown"
        try:
            while True:
                try:
                    head = await http1.read_request(reader)
                except http1.ProtocolError as exc:
                    self.metrics.client_errors += 1
                    writer.write(http1.simple_response(400, f"{exc}\n".encode()))
                    await writer.drain()
                    return
                if head is None:
                    return

                self.metrics.requests += 1
                keep_alive = await self._serve_one(head, reader, writer, client_ip)
                if not keep_alive:
                    return
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            pass
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - one bad connection must not stop the proxy
            log.exception("client connection failed")
        finally:
            try:
                writer.close()
            except Exception:  # noqa: BLE001
                pass

    async def _serve_one(self, head: http1.RequestHead, reader: asyncio.StreamReader,
                         writer: asyncio.StreamWriter, client_ip: str) -> bool:
        started = time.monotonic()
        client_wants_keep_alive = _wants_keep_alive(head.headers, head.version)

        try:
            framing, length = http1.framing_of(head.headers)
        except http1.ProtocolError as exc:
            self.metrics.client_errors += 1
            writer.write(http1.simple_response(400, f"{exc}\n".encode()))
            await writer.drain()
            return False

        has_body = http1.request_has_body(head)
        body: bytes | None = None
        streaming: AsyncIterator[bytes] | None = None
        if has_body:
            if framing == "length" and length <= RETRY_BUFFER_BYTES:
                body = b"".join([block async for block in
                                 http1.stream_body(reader, framing, length)])
                self.metrics.bytes_in += len(body)
            else:
                streaming = http1.stream_body(reader, framing, length)

        key = key_from(head.headers.get(self.config.hash_header), client_ip,
                       self.config.hash_key)
        retryable = streaming is None and head.method.upper() in IDEMPOTENT

        try:
            backend = self.balancer.choose(key)
        except NoBackendAvailable as exc:
            self.metrics.rejected_no_backend += 1
            writer.write(http1.simple_response(503, f"{exc}\n".encode()))
            await writer.drain()
            return False

        attempts = [backend]
        if retryable and self.config.retries:
            attempts += list(self.balancer.others(backend, key))[:self.config.retries]

        last_error: Exception | None = None
        for attempt_no, target in enumerate(attempts):
            if not target.breaker.allows():
                continue
            if attempt_no:
                self.metrics.retries += 1
            try:
                status = await self._forward(target, head, body, streaming, client_ip,
                                             writer, client_wants_keep_alive)
            except (OSError, asyncio.TimeoutError, http1.ProtocolError) as exc:
                target.record_failure()
                self.metrics.upstream_errors += 1
                last_error = exc
                log.warning("backend %s failed: %s", target.name, exc)
                continue
            except _ClientGone:
                # The backend did its job; the client left. Nothing is recorded
                # against the backend, and there is nobody to retry for.
                self.metrics.client_disconnects += 1
                return False
            except _ResponseAlreadyStarted as exc:
                # Bytes are already on the way to the client, so there is no
                # honest way to retry: the client has a partial response and the
                # only correct move is to end the connection.
                target.record_failure()
                self.metrics.upstream_errors += 1
                log.warning("backend %s failed mid-response: %s", target.name, exc.__cause__)
                return False

            elapsed = time.monotonic() - started
            target.record_success(elapsed)
            self.metrics.observe(status, elapsed)
            if self.config.access_log:
                log.info('%s "%s %s" %d %s %.1fms', client_ip, head.method, head.target,
                         status, target.name, elapsed * 1000)
            return client_wants_keep_alive

        writer.write(http1.simple_response(502, f"all backends failed: {last_error}\n".encode()))
        await writer.drain()
        self.metrics.observe(502, time.monotonic() - started)
        return False

    # ------------------------------------------------------------- forwarding

    async def _forward(self, backend: Backend, head: http1.RequestHead,
                       body: bytes | None, streaming: AsyncIterator[bytes] | None,
                       client_ip: str, client: asyncio.StreamWriter,
                       client_keep_alive: bool) -> int:
        # Counted before the first await. If the increment happened after the
        # connection was acquired, every request in a concurrent burst would see
        # in_flight at zero and least-connections would send them all to the same
        # backend.
        backend.in_flight += 1
        reusable = False
        connection = None
        try:
            connection = await backend.acquire()
            upstream = _upstream_request(head, client_ip, body, streaming is not None)
            connection.writer.write(upstream.encode())
            if body is not None:
                connection.writer.write(body)
            elif streaming is not None:
                async for block in streaming:
                    self.metrics.bytes_in += len(block)
                    connection.writer.write(block)
            # One timeout scope for the whole upstream exchange. asyncio.wait_for
            # wraps its argument in a new task each call, and at two per request
            # that was the largest single cost in the profile after the headers.
            async with asyncio.timeout(self.config.request_timeout_s):
                await connection.writer.drain()
                response = await http1.read_response(connection.reader)

            has_body = http1.response_has_body(head.method, response.status)
            framing, length = http1.framing_of(response.headers) if has_body else ("length", 0)

            downstream, use_chunked = _downstream_response(
                response, framing, length, has_body, client_keep_alive)
            await _to_client(client, downstream.encode())

            # From here on, a failure has two possible owners and they must not
            # be confused. Reading the body is the backend's side; writing it is
            # the client's. A client that hangs up mid-response is not evidence
            # against the backend, and counting it as one lets ordinary client
            # disconnects trip the breaker on a perfectly healthy backend.
            if has_body:
                blocks = http1.stream_body(connection.reader, framing, length).__aiter__()
                while True:
                    try:
                        block = await blocks.__anext__()
                    except StopAsyncIteration:
                        break
                    except (OSError, asyncio.TimeoutError, http1.ProtocolError) as exc:
                        raise _ResponseAlreadyStarted() from exc
                    self.metrics.bytes_out += len(block)
                    await _to_client(client, _chunk(block) if use_chunked else block)
                if use_chunked:
                    await _to_client(client, b"0\r\n\r\n")

            reusable = (framing != "eof"
                        and _wants_keep_alive(response.headers, response.version))
            return response.status
        finally:
            backend.in_flight -= 1
            if connection is not None:
                backend.release(connection, reusable=reusable)

    # ---------------------------------------------------------------- admin

    async def _handle_admin(self, reader: asyncio.StreamReader,
                            writer: asyncio.StreamWriter) -> None:
        try:
            head = await http1.read_request(reader)
            if head is None:
                return
            route = head.target.split("?", 1)[0].rstrip("/") or "/"
            snapshot = self.metrics.snapshot()
            backends = [b.snapshot() for b in self.backends]

            if route == "/metrics":
                body = prometheus(snapshot, backends).encode()
                content_type = "text/plain; version=0.0.4; charset=utf-8"
            elif route in ("/", "/stats"):
                body = json.dumps({"proxy": snapshot, "backends": backends,
                                   "strategy": self.config.strategy}, indent=2).encode()
                content_type = "application/json"
            elif route == "/health":
                body = b'{"status":"ok"}'
                content_type = "application/json"
            else:
                writer.write(http1.simple_response(404, b"try /stats, /metrics or /health\n"))
                await writer.drain()
                return

            headers = http1.Headers([
                ("Content-Type", content_type),
                ("Content-Length", str(len(body))),
                ("Connection", "close"),
            ])
            writer.write(http1.ResponseHead("HTTP/1.1", 200, "OK", headers).encode() + body)
            await writer.drain()
        except Exception:  # noqa: BLE001 - the admin port must not take the proxy down
            log.exception("admin request failed")
        finally:
            try:
                writer.close()
            except Exception:  # noqa: BLE001
                pass


class _ResponseAlreadyStarted(Exception):
    """A backend died after the client had begun receiving its response."""


class _ClientGone(Exception):
    """The client disconnected while its response was being written."""


async def _to_client(client: asyncio.StreamWriter, data: bytes) -> None:
    try:
        client.write(data)
        await client.drain()
    except (OSError, RuntimeError) as exc:
        # RuntimeError covers writing to a transport asyncio has already closed.
        raise _ClientGone() from exc


def _wants_keep_alive(headers: http1.Headers, version: str) -> bool:
    connection = (headers.get("connection") or "").lower()
    if "close" in connection:
        return False
    if version == "HTTP/1.0":
        return "keep-alive" in connection
    return True


def _upstream_request(head: http1.RequestHead, client_ip: str, body: bytes | None,
                      is_streaming: bool) -> http1.RequestHead:
    headers = head.headers.without_hop_by_hop()

    forwarded = headers.get_all("x-forwarded-for")
    headers.remove("x-forwarded-for")
    chain = ", ".join([*forwarded, client_ip]) if forwarded else client_ip
    headers.set("X-Forwarded-For", chain)
    if not headers.get("x-real-ip"):
        headers.set("X-Real-IP", client_ip)
    headers.set("X-Forwarded-Proto", "http")
    if head.headers.get("host") and not headers.get("x-forwarded-host"):
        headers.set("X-Forwarded-Host", head.headers.get("host"))

    # Framing has to be restated: without_hop_by_hop stripped Transfer-Encoding
    # along with the rest, and the body may now be a different shape.
    headers.remove("content-length")
    if body is not None:
        headers.set("Content-Length", str(len(body)))
    elif is_streaming:
        original = head.headers.get("transfer-encoding")
        if original:
            headers.set("Transfer-Encoding", original)
        elif head.headers.get("content-length"):
            headers.set("Content-Length", head.headers.get("content-length"))

    headers.set("Connection", "keep-alive")
    return http1.RequestHead(head.method, head.target, "HTTP/1.1", headers)


def _downstream_response(response: http1.ResponseHead, framing: str, length: int,
                         has_body: bool, client_keep_alive: bool
                         ) -> tuple[http1.ResponseHead, bool]:
    headers = response.headers.without_hop_by_hop()
    headers.remove("content-length")
    use_chunked = False

    if has_body:
        if framing == "length":
            headers.set("Content-Length", str(length))
        else:
            # Either the backend chunked it, or it is delimited by closing the
            # connection. Chunking to the client covers both and keeps the
            # client connection reusable in the second case, which it would not
            # otherwise be.
            headers.set("Transfer-Encoding", "chunked")
            use_chunked = True
    else:
        if response.status not in (204, 304) and not (100 <= response.status < 200):
            headers.set("Content-Length", "0")

    headers.set("Connection", "keep-alive" if client_keep_alive else "close")
    return http1.ResponseHead("HTTP/1.1", response.status, response.reason, headers), use_chunked


def _chunk(block: bytes) -> bytes:
    return f"{len(block):x}\r\n".encode() + block + b"\r\n"
