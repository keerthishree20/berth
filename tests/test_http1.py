"""Parsing and framing.

The framing cases are not pedantry. Two hops that disagree about where a message
ends is the whole mechanism behind request smuggling, so the parser refuses
ambiguity rather than resolving it.
"""

from __future__ import annotations

import asyncio

import pytest

from berth import http1
from berth.http1 import Headers, ProtocolError


async def reader_of(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


# ------------------------------------------------------------------- headers


def test_lookup_ignores_case_and_keeps_order():
    headers = Headers([("Host", "a"), ("X-Trace", "1"), ("x-trace", "2")])
    assert headers.get("HOST") == "a"
    assert headers.get_all("X-Trace") == ["1", "2"]
    assert [k for k, _ in headers] == ["Host", "X-Trace", "x-trace"]


def test_set_replaces_every_copy():
    headers = Headers([("A", "1"), ("a", "2"), ("B", "3")])
    headers.set("A", "9")
    assert headers.get_all("a") == ["9"]
    assert headers.get("B") == "3"


def test_hop_by_hop_headers_are_dropped():
    headers = Headers([
        ("Host", "example.com"), ("Connection", "keep-alive"),
        ("Keep-Alive", "timeout=5"), ("Transfer-Encoding", "chunked"),
        ("Upgrade", "websocket"), ("X-Keep", "yes"),
    ])
    cleaned = dict(headers.without_hop_by_hop())
    assert cleaned == {"Host": "example.com", "X-Keep": "yes"}


def test_connection_can_nominate_further_headers_to_drop():
    """`Connection: X` means X describes this hop only. Forwarding it describes
    the wrong connection."""
    headers = Headers([("Connection", "close, X-Private"), ("X-Private", "secret"),
                       ("X-Public", "fine")])
    cleaned = dict(headers.without_hop_by_hop())
    assert cleaned == {"X-Public": "fine"}


# ------------------------------------------------------------------ requests


async def test_a_request_parses():
    head = await http1.read_request(await reader_of(
        b"GET /path?q=1 HTTP/1.1\r\nHost: x\r\nAccept: */*\r\n\r\n"))
    assert (head.method, head.target, head.version) == ("GET", "/path?q=1", "HTTP/1.1")
    assert head.headers.get("host") == "x"


async def test_a_clean_close_between_messages_is_not_an_error():
    assert await http1.read_request(await reader_of(b"")) is None


async def test_a_half_sent_head_is_an_error():
    with pytest.raises(ProtocolError, match="closed part way"):
        await http1.read_request(await reader_of(b"GET / HTTP/1.1\r\nHost: x"))


@pytest.mark.parametrize("raw", [
    b"NOTAREQUEST\r\n\r\n",
    b"GET /\r\n\r\n",
    b"GET / HTTP/9.9\r\n\r\n",
    b"GET / HTTP/1.1\r\nBad Header\r\n\r\n",
    b"GET / HTTP/1.1\r\nName : value\r\n\r\n",
])
async def test_malformed_requests_are_refused(raw):
    with pytest.raises(ProtocolError):
        await http1.read_request(await reader_of(raw))


async def test_too_many_headers_are_refused():
    raw = b"GET / HTTP/1.1\r\n" + b"".join(
        f"X-{i}: v\r\n".encode() for i in range(http1.MAX_HEADER_COUNT + 5)) + b"\r\n"
    with pytest.raises(ProtocolError, match="exceeds the limit"):
        await http1.read_request(await reader_of(raw))


# ------------------------------------------------------------------- framing


def test_content_length_framing():
    assert http1.framing_of(Headers([("Content-Length", "42")])) == ("length", 42)


def test_chunked_framing():
    assert http1.framing_of(Headers([("Transfer-Encoding", "chunked")])) == ("chunked", 0)


def test_no_framing_means_read_until_close():
    assert http1.framing_of(Headers()) == ("eof", 0)


def test_both_framings_at_once_is_refused():
    """The ambiguity request smuggling is built on. Choosing one is how two
    hops end up disagreeing, so neither is chosen."""
    with pytest.raises(ProtocolError, match="both"):
        http1.framing_of(Headers([("Content-Length", "5"),
                                  ("Transfer-Encoding", "chunked")]))


def test_two_content_lengths_are_refused():
    with pytest.raises(ProtocolError, match="more than one"):
        http1.framing_of(Headers([("Content-Length", "5"), ("Content-Length", "6")]))


def test_a_non_numeric_content_length_is_refused():
    with pytest.raises(ProtocolError, match="non-numeric"):
        http1.framing_of(Headers([("Content-Length", "5a")]))


# --------------------------------------------------------------------- bodies


async def test_a_measured_body_reads_exactly():
    reader = await reader_of(b"hello worldEXTRA")
    got = b"".join([b async for b in http1.stream_body(reader, "length", 11)])
    assert got == b"hello world"


async def test_a_short_body_is_an_error():
    reader = await reader_of(b"hi")
    with pytest.raises(ProtocolError, match="ended 8 bytes early"):
        [b async for b in http1.stream_body(reader, "length", 10)]


async def test_a_chunked_body_reassembles():
    reader = await reader_of(b"3\r\none\r\n3\r\ntwo\r\n5\r\nthree\r\n0\r\n\r\n")
    got = b"".join([b async for b in http1.stream_body(reader, "chunked", 0)])
    assert got == b"onetwothree"


async def test_chunk_extensions_are_ignored():
    reader = await reader_of(b"3;name=value\r\nabc\r\n0\r\n\r\n")
    assert b"".join([b async for b in http1.stream_body(reader, "chunked", 0)]) == b"abc"


async def test_trailers_are_consumed():
    reader = await reader_of(b"3\r\nabc\r\n0\r\nX-Checksum: 9\r\n\r\n")
    assert b"".join([b async for b in http1.stream_body(reader, "chunked", 0)]) == b"abc"


async def test_a_bad_chunk_size_is_refused():
    reader = await reader_of(b"zz\r\nabc\r\n")
    with pytest.raises(ProtocolError, match="bad chunk size"):
        [b async for b in http1.stream_body(reader, "chunked", 0)]


async def test_an_unterminated_chunked_body_is_refused():
    reader = await reader_of(b"3\r\nabc\r\n")
    with pytest.raises(ProtocolError, match="without a final chunk"):
        [b async for b in http1.stream_body(reader, "chunked", 0)]


async def test_an_eof_body_reads_to_the_end():
    reader = await reader_of(b"whatever is left")
    assert b"".join([b async for b in http1.stream_body(reader, "eof", 0)]) == b"whatever is left"


# ------------------------------------------------------------------ responses


async def test_a_response_parses():
    response = await http1.read_response(await reader_of(
        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n"))
    assert (response.status, response.reason) == (404, "Not Found")


async def test_a_response_without_a_reason_phrase_parses():
    response = await http1.read_response(await reader_of(b"HTTP/1.1 200\r\n\r\n"))
    assert response.status == 200 and response.reason == ""


@pytest.mark.parametrize("method,status,expected", [
    ("GET", 200, True), ("GET", 204, False), ("GET", 304, False),
    ("GET", 100, False), ("HEAD", 200, False), ("POST", 201, True),
])
def test_which_responses_carry_a_body(method, status, expected):
    assert http1.response_has_body(method, status) is expected


def test_round_tripping_a_head():
    head = http1.RequestHead("GET", "/x", "HTTP/1.1", Headers([("Host", "a"), ("X", "1")]))
    assert head.encode() == b"GET /x HTTP/1.1\r\nHost: a\r\nX: 1\r\n\r\n"


def test_a_simple_response_is_well_formed():
    raw = http1.simple_response(502)
    assert raw.startswith(b"HTTP/1.1 502 Bad Gateway\r\n")
    assert b"Content-Length: " in raw
    assert raw.endswith(b"502 Bad Gateway\n")
