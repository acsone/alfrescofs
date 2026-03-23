"""Unit tests for internal path-utility helpers: _norm(), node(), parse_range_header()."""

import pytest

from alfrescofs.core import _norm, node, parse_range_header


@pytest.mark.parametrize(
    "path, expected",
    [
        ("", "/"),
        ("test", "/test"),
        ("/test/", "/test"),
        ("//test//file", "/test/file"),
        ("/", "/"),
        ("a/b/c", "/a/b/c"),
    ],
)
def test_norm(path, expected):
    assert _norm(path) == expected


@pytest.mark.parametrize(
    "node_id, parts, expected",
    [
        ("abc123", [], "nodes/abc123"),
        ("abc123", ["children", "content"], "nodes/abc123/children/content"),
        ("/abc123/", [], "nodes/abc123"),
        ("abc123", ["", "content"], "nodes/abc123/content"),
    ],
)
def test_node(node_id, parts, expected):
    assert node(node_id, *parts) == expected


@pytest.mark.parametrize(
    "header, expected",
    [
        ("bytes=0-499", (0, 499)),
        ("bytes=100-", (100, None)),
        ("bytes=-500", (None, 500)),
    ],
)
def test_parse_range_header(header, expected):
    assert parse_range_header(header) == expected


def test_parse_range_header_invalid():
    with pytest.raises(ValueError):
        parse_range_header("invalid")
