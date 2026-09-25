import pytest
from protocol.basic import encode_msg, Decoder, HEADER


def test_encode_decode_roundtrip():
    d = Decoder()
    d.feed(encode_msg(request_id=42, payload=b"hello world"))
    assert d.pop_ready() == [(42, b"hello world")]


def test_empty_payload():
    d = Decoder()
    d.feed(encode_msg(request_id=1, payload=b""))
    assert d.pop_ready() == [(1, b"")]


def test_multiple_messages_in_one_feed():
    d = Decoder()
    blob = encode_msg(1, b"first") + encode_msg(2, b"second")
    d.feed(blob)
    assert d.pop_ready() == [(1, b"first"), (2, b"second")]


def test_message_split_across_many_feeds():
    d = Decoder()
    blob = encode_msg(99, b"fragmented payload")
    for i in range(len(blob) - 1):
        d.feed(blob[i : i + 1])
        assert d.pop_ready() == []
    d.feed(blob[-1:])
    assert d.pop_ready() == [(99, b"fragmented payload")]


def test_split_exactly_at_header_boundary():
    d = Decoder()
    blob = encode_msg(5, b"payload-data")
    d.feed(blob[:HEADER.size])
    assert d.pop_ready() == []
    d.feed(blob[HEADER.size:])
    assert d.pop_ready() == [(5, b"payload-data")]


def test_partial_message_left_buffered_for_next_feed():
    d = Decoder()
    blob1 = encode_msg(1, b"complete")
    blob2 = encode_msg(2, b"also complete")
    d.feed(blob1 + blob2[:5])
    assert d.pop_ready() == [(1, b"complete")]
    d.feed(blob2[5:])
    assert d.pop_ready() == [(2, b"also complete")]


def test_large_payload_roundtrip():
    d = Decoder()
    payload = b"x" * 500_000
    d.feed(encode_msg(7, payload))
    request_id, got = d.pop_ready()[0]
    assert request_id == 7
    assert got == payload