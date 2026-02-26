"""Tests for write tools: DOI, arXiv, and URL import."""
import urllib.request
from io import BytesIO

import pytest
import requests as requests_lib

import zotero_mcp.server as server

# ── Shared fixtures / helpers ─────────────────────────────────────────────────

CROSSREF_RESPONSE = {
    "message": {
        "title": ["Test Paper Title"],
        "author": [{"given": "Alice", "family": "Smith"}],
        "published": {"date-parts": [[2023, 1, 30]]},
        "container-title": ["Nature"],
        "volume": "1",
        "issue": "2",
        "page": "100-110",
        "abstract": "Test abstract.",
        "URL": "https://doi.org/10.1038/test",
    }
}

ARXIV_XML = b"""\
<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <title>Test Paper Title</title>
    <summary>Abstract text here.</summary>
    <published>2023-01-30T00:00:00Z</published>
    <author><name>Alice Smith</name></author>
    <category term="cs.LG"/>
    <arxiv:doi>10.48550/arXiv.2301.12345</arxiv:doi>
  </entry>
</feed>"""

ARXIV_EMPTY_XML = b"""\
<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:arxiv="http://arxiv.org/schemas/atom">
</feed>"""


class FakeRequestsResponse:
    def __init__(self, json_data, status_code=200):
        self._json = json_data
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests_lib.HTTPError(response=self)

    def json(self):
        return self._json


class FakeURLResponse:
    """Context-manager fake for urllib.request.urlopen."""

    def __init__(self, data: bytes):
        self._data = data

    def read(self, n=-1):
        return self._data if n < 0 else self._data[:n]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


# ── zotero_add_items_by_doi ───────────────────────────────────────────────────

def test_doi_single_success(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(
        server.requests, "get",
        lambda url, headers=None, timeout=None: FakeRequestsResponse(CROSSREF_RESPONSE),
    )
    result = server.add_items_by_doi(dois=["10.1038/test"], ctx=ctx)
    assert "✓" in result
    assert "NEWKEY1" in result
    assert len(patch_web_client.created_items) == 1


def test_doi_multiple_success(monkeypatch, patch_web_client, ctx):
    call_count = {"n": 0}

    def fake_get(url, headers=None, timeout=None):
        call_count["n"] += 1
        return FakeRequestsResponse(CROSSREF_RESPONSE)

    monkeypatch.setattr(server.requests, "get", fake_get)
    result = server.add_items_by_doi(dois=["10.1/a", "10.1/b"], ctx=ctx)
    assert result.count("✓") == 2
    assert call_count["n"] == 2


def test_doi_with_collection_key(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(
        server.requests, "get",
        lambda url, headers=None, timeout=None: FakeRequestsResponse(CROSSREF_RESPONSE),
    )
    server.add_items_by_doi(dois=["10.1/x"], collection_key="COL1", ctx=ctx)
    assert patch_web_client.created_items[0]["collections"] == ["COL1"]


def test_doi_http_error(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(
        server.requests, "get",
        lambda url, headers=None, timeout=None: FakeRequestsResponse({}, status_code=404),
    )
    result = server.add_items_by_doi(dois=["10.1/bad"], ctx=ctx)
    assert "✗" in result
    assert len(patch_web_client.created_items) == 0


def test_doi_no_credentials(patch_no_credentials, ctx):
    result = server.add_items_by_doi(dois=["10.1/x"], ctx=ctx)
    assert "Error" in result
    assert "credentials" in result.lower()


def test_doi_create_items_failed(monkeypatch, fake_zot, ctx):
    fake_zot.create_items = lambda items: {"successful": {}, "failed": {"0": "err"}}
    monkeypatch.setattr(server, "get_web_zotero_client", lambda: fake_zot)
    monkeypatch.setattr(
        server.requests, "get",
        lambda url, headers=None, timeout=None: FakeRequestsResponse(CROSSREF_RESPONSE),
    )
    result = server.add_items_by_doi(dois=["10.1/x"], ctx=ctx)
    assert "✗" in result


# ── zotero_add_items_by_arxiv ─────────────────────────────────────────────────

def _make_arxiv_urlopen(data: bytes):
    def fake_urlopen(url, timeout=None):
        return FakeURLResponse(data)
    return fake_urlopen


def test_arxiv_bare_id(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(urllib.request, "urlopen", _make_arxiv_urlopen(ARXIV_XML))
    result = server.add_items_by_arxiv(arxiv_ids=["2301.12345"], ctx=ctx)
    assert "✓" in result
    assert len(patch_web_client.created_items) == 1


def test_arxiv_prefix_stripped(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(urllib.request, "urlopen", _make_arxiv_urlopen(ARXIV_XML))
    result = server.add_items_by_arxiv(arxiv_ids=["arXiv:2301.12345"], ctx=ctx)
    assert "✓" in result


def test_arxiv_full_url_stripped(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(urllib.request, "urlopen", _make_arxiv_urlopen(ARXIV_XML))
    result = server.add_items_by_arxiv(
        arxiv_ids=["https://arxiv.org/abs/2301.12345"], ctx=ctx
    )
    assert "✓" in result


def test_arxiv_doi_prefix_stripped(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(urllib.request, "urlopen", _make_arxiv_urlopen(ARXIV_XML))
    result = server.add_items_by_arxiv(
        arxiv_ids=["10.48550/arXiv.2301.12345"], ctx=ctx
    )
    assert "✓" in result


def test_arxiv_no_entry_found(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(urllib.request, "urlopen", _make_arxiv_urlopen(ARXIV_EMPTY_XML))
    result = server.add_items_by_arxiv(arxiv_ids=["9999.99999"], ctx=ctx)
    assert "✗" in result
    assert "not found" in result


def test_arxiv_network_error(monkeypatch, patch_web_client, ctx):
    def fail_urlopen(url, timeout=None):
        raise OSError("Network unreachable")

    monkeypatch.setattr(urllib.request, "urlopen", fail_urlopen)
    result = server.add_items_by_arxiv(arxiv_ids=["2301.12345"], ctx=ctx)
    assert "✗" in result


def test_arxiv_no_credentials(patch_no_credentials, ctx):
    result = server.add_items_by_arxiv(arxiv_ids=["2301.12345"], ctx=ctx)
    assert "Error" in result
    assert "credentials" in result.lower()


# ── zotero_add_item_by_url ────────────────────────────────────────────────────

OG_TITLE_HTML = b"""\
<html>
<head>
  <meta property="og:title" content="OG Page Title" />
  <meta property="og:description" content="OG description" />
  <title>HTML Title</title>
</head>
<body></body>
</html>"""

PLAIN_TITLE_HTML = b"""\
<html>
<head><title>Plain Title</title></head>
<body></body>
</html>"""


def test_url_og_title_used(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(
        urllib.request, "urlopen",
        lambda req, timeout=None: FakeURLResponse(OG_TITLE_HTML),
    )
    result = server.add_item_by_url(url="https://example.com", ctx=ctx)
    assert "✓" in result
    assert "OG Page Title" in result
    assert patch_web_client.created_items[0]["title"] == "OG Page Title"


def test_url_plain_title_fallback(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(
        urllib.request, "urlopen",
        lambda req, timeout=None: FakeURLResponse(PLAIN_TITLE_HTML),
    )
    result = server.add_item_by_url(url="https://example.com", ctx=ctx)
    assert "Plain Title" in result
    assert patch_web_client.created_items[0]["title"] == "Plain Title"


def test_url_explicit_title_overrides_page(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(
        urllib.request, "urlopen",
        lambda req, timeout=None: FakeURLResponse(OG_TITLE_HTML),
    )
    result = server.add_item_by_url(
        url="https://example.com", title="My Custom Title", ctx=ctx
    )
    assert "My Custom Title" in result
    assert patch_web_client.created_items[0]["title"] == "My Custom Title"


def test_url_network_error_uses_url_as_title(monkeypatch, patch_web_client, ctx):
    def fail_urlopen(req, timeout=None):
        raise OSError("Network error")

    monkeypatch.setattr(urllib.request, "urlopen", fail_urlopen)
    result = server.add_item_by_url(url="https://example.com/page", ctx=ctx)
    assert "✓" in result
    assert patch_web_client.created_items[0]["title"] == "https://example.com/page"


def test_url_no_credentials(patch_no_credentials, ctx):
    result = server.add_item_by_url(url="https://example.com", ctx=ctx)
    assert "Error" in result
    assert "credentials" in result.lower()
