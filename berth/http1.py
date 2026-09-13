"""Just enough HTTP/1.1 to proxy correctly.

Not a general HTTP library. It parses what a proxy has to understand and leaves
everything else as opaque bytes, which is the right instinct for a proxy: the
less it interprets, the fewer ways it can change the meaning of a message it is
supposed to be passing along.

The two things it does have to get exactly right are message framing and
hop-by-hop headers. Framing, because a proxy that disagrees with its backend
about where one response ends and the next begins is a request smuggling
vulnerability, not a bug. Hop-by-hop, because those headers describe the single
connection they arrived on and forwarding them describes the wrong connection.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import AsyncIterator, Iterable

MAX_HEAD_BYTES = 64 * 1024
MAX_HEADER_COUNT = 200

#: Named by RFC 9110 as connection-specific. They apply to the hop they arrived
#: on and must not be passed to the next one.
HOP_BY_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade",
})


class ProtocolError(Exception):
    """The bytes on the wire are not a message this proxy will forward."""


class Headers:
    """An ordered, case-insensitive multi-map.

    Order is preserved and duplicates are kept, because a proxy has no business
    tidying up someone else's headers. Lookup is case-insensitive because the
    protocol says names are.
    """

    __slots__ = ("_items", "_lower")

    def __init__(self, items: Iterable[tuple[str, str]] = ()):
        self._items: list[tuple[str, str]] = list(items)
        # Lowercased names, computed once per header rather than once per
        # lookup. Profiling put str.lower at over a hundred calls per proxied
        # request before this, because every get and remove re-lowered every
        # name in the list.
        self._lower: list[str] = [k.lower() for k, _ in self._items]

    def __iter__(self):
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __contains__(self, name: str) -> bool:
        return name.lower() in self._lower

    def get(self, name: str, default: str | None = None) -> str | None:
        lowered = name.lower()
        for position, key in enumerate(self._lower):
            if key == lowered:
                return self._items[position][1]
        return default

    def get_all(self, name: str) -> list[str]:
        lowered = name.lower()
        return [self._items[i][1] for i, key in enumerate(self._lower) if key == lowered]

    def add(self, name: str, value: str) -> None:
        self._items.append((name, value))
        self._lower.append(name.lower())

    def set(self, name: str, value: str) -> None:
        self.remove(name)
        self.add(name, value)

    def remove(self, name: str) -> None:
        lowered = name.lower()
        if lowered not in self._lower:
            return  # the common case, and it costs no list rebuild
        keep = [i for i, key in enumerate(self._lower) if key != lowered]
        self._items = [self._items[i] for i in keep]
        self._lower = [self._lower[i] for i in keep]

    def without_hop_by_hop(self) -> "Headers":
        """Drop the connection-specific headers, including anything the
        `Connection` header itself nominates."""
        nominated = {
            token.strip().lower()
            for value in self.get_all("connection")
            for token in value.split(",")
            if token.strip()
        }
        drop = HOP_BY_HOP | nominated if nominated else HOP_BY_HOP
        kept = Headers()
        for item, key in zip(self._items, self._lower):
            if key not in drop:
                kept._items.append(item)
                kept._lower.append(key)
        return kept

    def encode(self) -> bytes:
        return "".join(f"{k}: {v}\r\n" for k, v in self._items).encode("latin-1")

    def __repr__(self) -> str:
        return f"Headers({self._items!r})"


@dataclass
class RequestHead:
    method: str
    target: str
    version: str
    headers: Headers = field(default_factory=Headers)

    def encode(self) -> bytes:
        return (f"{self.method} {self.target} {self.version}\r\n".encode("latin-1")
                + self.headers.encode() + b"\r\n")


@dataclass
class ResponseHead:
    version: str
    status: int
    reason: str
    headers: Headers = field(default_factory=Headers)

    def encode(self) -> bytes:
        return (f"{self.version} {self.status} {self.reason}\r\n".encode("latin-1")
                + self.headers.encode() + b"\r\n")


async def _read_head_bytes(reader: asyncio.StreamReader) -> bytes | None:
    try:
        block = await reader.readuntil(b"\r\n\r\n")
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            return None  # the peer closed cleanly between messages
        raise ProtocolError("connection closed part way through a message head") from exc
    except asyncio.LimitOverrunError as exc:
        raise ProtocolError("message head exceeded the read buffer") from exc
    if len(block) > MAX_HEAD_BYTES:
        raise ProtocolError(f"message head of {len(block)} bytes exceeds the limit")
    return block


def _parse_headers(lines: list[str]) -> Headers:
    if len(lines) > MAX_HEADER_COUNT:
        raise ProtocolError(f"{len(lines)} headers exceeds the limit of {MAX_HEADER_COUNT}")
    headers = Headers()
    for line in lines:
        name, separator, value = line.partition(":")
        if not separator or not name or name != name.strip():
            # A name with trailing space is the classic smuggling probe: some
            # parsers strip it and some do not, and then two hops disagree.
            raise ProtocolError(f"malformed header line: {line!r}")
        headers.add(name, value.strip())
    return headers


async def read_request(reader: asyncio.StreamReader) -> RequestHead | None:
    block = await _read_head_bytes(reader)
    if block is None:
        return None
    lines = block.decode("latin-1").split("\r\n")[:-2]
    if not lines:
        raise ProtocolError("empty request")
    try:
        method, target, version = lines[0].split(" ", 2)
    except ValueError:
        raise ProtocolError(f"malformed request line: {lines[0]!r}") from None
    if not version.startswith("HTTP/1."):
        raise ProtocolError(f"unsupported version {version!r}")
    return RequestHead(method, target, version, _parse_headers(lines[1:]))


async def read_response(reader: asyncio.StreamReader) -> ResponseHead:
    block = await _read_head_bytes(reader)
    if block is None:
        raise ProtocolError("backend closed before sending a response")
    lines = block.decode("latin-1").split("\r\n")[:-2]
    parts = lines[0].split(" ", 2)
    if len(parts) < 2:
        raise ProtocolError(f"malformed status line: {lines[0]!r}")
    version, status = parts[0], parts[1]
    reason = parts[2] if len(parts) > 2 else ""
    if not status.isdigit():
        raise ProtocolError(f"non-numeric status {status!r}")
    return ResponseHead(version, int(status), reason, _parse_headers(lines[1:]))


def framing_of(headers: Headers) -> tuple[str, int]:
    """How the body is delimited: ("chunked", 0), ("length", n) or ("eof", 0).

    A message carrying both a length and a chunked encoding is refused rather
    than resolved. Picking one is exactly the disagreement request smuggling
    relies on, so the only safe answer is to forward neither.
    """
    encoding = headers.get("transfer-encoding", "")
    chunked = "chunked" in encoding.lower()
    length = headers.get("content-length")

    if chunked and length is not None:
        raise ProtocolError("message carries both Transfer-Encoding and Content-Length")
    if chunked:
        return "chunked", 0
    if length is not None:
        if len(headers.get_all("content-length")) > 1:
            raise ProtocolError("more than one Content-Length")
        try:
            return "length", int(length)
        except ValueError:
            raise ProtocolError(f"non-numeric Content-Length {length!r}") from None
    return "eof", 0


def request_has_body(head: RequestHead) -> bool:
    return head.headers.get("transfer-encoding") is not None or \
        head.headers.get("content-length") is not None


def response_has_body(request_method: str, status: int) -> bool:
    if request_method.upper() == "HEAD":
        return False
    return not (100 <= status < 200 or status in (204, 304))


async def stream_body(reader: asyncio.StreamReader, framing: str, length: int,
                      *, chunk_size: int = 64 * 1024) -> AsyncIterator[bytes]:
    """Yield the body without ever holding all of it.

    A proxy that buffers whole bodies turns one large upload into a memory
    problem for every other connection it is serving.
    """
    if framing == "length":
        remaining = length
        while remaining > 0:
            block = await reader.read(min(chunk_size, remaining))
            if not block:
                raise ProtocolError(f"body ended {remaining} bytes early")
            remaining -= len(block)
            yield block
        return

    if framing == "chunked":
        while True:
            line = await reader.readline()
            if not line:
                raise ProtocolError("chunked body ended without a final chunk")
            size_text = line.split(b";", 1)[0].strip()
            try:
                size = int(size_text, 16)
            except ValueError:
                raise ProtocolError(f"bad chunk size {size_text!r}") from None
            if size == 0:
                # Consume the trailer section, ending at the blank line.
                while True:
                    trailer = await reader.readline()
                    if trailer in (b"\r\n", b"\n", b""):
                        break
                return
            remaining = size
            while remaining > 0:
                block = await reader.read(min(chunk_size, remaining))
                if not block:
                    raise ProtocolError("chunked body ended mid-chunk")
                remaining -= len(block)
                yield block
            await reader.readexactly(2)  # the CRLF after the chunk data
        return

    while True:  # framing == "eof"
        block = await reader.read(chunk_size)
        if not block:
            return
        yield block


async def drain_body(reader: asyncio.StreamReader, framing: str, length: int) -> None:
    """Read and discard a body, so the connection can be reused."""
    async for _block in stream_body(reader, framing, length):
        pass


def status_line(status: int) -> str:
    return _REASONS.get(status, "Unknown")


_REASONS = {
    200: "OK", 400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed",
    408: "Request Timeout", 413: "Payload Too Large", 500: "Internal Server Error",
    502: "Bad Gateway", 503: "Service Unavailable", 504: "Gateway Timeout",
}


def simple_response(status: int, body: bytes = b"", *,
                    content_type: str = "text/plain; charset=utf-8") -> bytes:
    if not body:
        body = f"{status} {status_line(status)}\n".encode()
    headers = Headers([
        ("Content-Type", content_type),
        ("Content-Length", str(len(body))),
        ("Connection", "close"),
    ])
    head = ResponseHead("HTTP/1.1", status, status_line(status), headers)
    return head.encode() + body
