"""Tests for write tools: DOI, arXiv, URL, and smart identifier import."""
import asyncio
from pathlib import Path
import sys
import types
import urllib.request

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

CROSSREF_PROCEEDINGS_RESPONSE = {
    "message": {
        "title": ["Conference Test Paper"],
        "author": [{"given": "Alice", "family": "Smith"}],
        "published": {"date-parts": [[2024]]},
        "container-title": ["CVPR"],
        "volume": "12",
        "issue": "1",
        "page": "10-20",
        "abstract": "Conference abstract.",
        "URL": "https://doi.org/10.1109/test",
        "type": "proceedings-article",
    }
}

CROSSREF_MDPI_WORK = {
    "message": {
        "title": ["Vision Transformers for Remote Sensing Image Classification"],
        "author": [{"given": "M.", "family": "Said"}],
        "published": {"date-parts": [[2021, 2, 1]]},
        "container-title": ["Remote Sensing"],
        "volume": "13",
        "issue": "3",
        "page": "516",
        "abstract": "<jats:p>Remote sensing abstract.</jats:p>",
        "URL": "https://doi.org/10.3390/rs13030516",
        "DOI": "10.3390/rs13030516",
        "ISSN": ["2072-4292"],
        "license": [{"URL": "https://creativecommons.org/licenses/by/4.0/"}],
    }
}

CROSSREF_MDPI_SEARCH_RESPONSE = {
    "message": {
        "items": [CROSSREF_MDPI_WORK["message"]]
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
    def __init__(self, json_data=None, status_code=200, headers=None, content=None, url=None):
        self._json = json_data
        self.status_code = status_code
        self.headers = headers or {}
        self.content = content or b""
        self.url = url

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests_lib.HTTPError(response=self)

    def json(self):
        return self._json

    def iter_content(self, chunk_size=8192):
        yield self.content


EUROPEPMC_SEARCH_RESPONSE = {
    "resultList": {
        "result": [
            {
                "pmcid": "PMC7231423",
                "doi": "10.1155/2020/7902072",
                "title": "Cross-Subject Seizure Detection in EEGs Using Deep Transfer Learning.",
                "journalTitle": "Comput Math Methods Med",
                "pubYear": "2020",
            }
        ]
    }
}

EUROPEPMC_FULLTEXT_XML = b"""<?xml version='1.0' encoding='UTF-8'?>
<article>
  <front>
    <article-meta>
      <article-id pub-id-type='pmcid'>PMC7231423</article-id>
      <article-id pub-id-type='doi'>10.1155/2020/7902072</article-id>
      <title-group>
        <article-title>Cross-Subject Seizure Detection in EEGs Using Deep Transfer Learning</article-title>
      </title-group>
      <abstract>
        <p>Abstract text here.</p>
      </abstract>
    </article-meta>
  </front>
  <body>
    <sec>
      <title>Introduction</title>
      <p>Body paragraph one.</p>
      <p>Body paragraph two.</p>
    </sec>
  </body>
</article>
"""


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

    def geturl(self):
        return "https://example.com/final"


# ── zotero_add_items_by_doi ───────────────────────────────────────────────────

def test_doi_single_success(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(
        server.requests, "get",
        lambda url, headers=None, timeout=None: FakeRequestsResponse(CROSSREF_RESPONSE),
    )
    result = server.add_items_by_doi(dois=["10.1038/test"], ctx=ctx)
    assert "✓" in result
    assert "NEWKEY1" in result
    assert "route=doi" in result
    assert len(patch_web_client.created_items) == 1


def test_doi_single_success_defaults_to_user_friendly_output(monkeypatch, patch_web_client, ctx):
    monkeypatch.delenv("ZOTERO_MCP_DEBUG_IMPORT", raising=False)
    monkeypatch.setattr(
        server.requests, "get",
        lambda url, headers=None, timeout=None: FakeRequestsResponse(CROSSREF_RESPONSE),
    )
    result = server.add_items_by_doi(dois=["10.1038/test"], ctx=ctx)
    assert "Imported as paper" in result
    assert "route=doi" not in result
    assert "pdf_source=" not in result


def test_doi_multiple_success(monkeypatch, patch_web_client, ctx):
    call_count = {"n": 0}

    def fake_get(url, headers=None, timeout=None):
        call_count["n"] += 1
        return FakeRequestsResponse(CROSSREF_RESPONSE)

    monkeypatch.setattr(server.requests, "get", fake_get)
    result = server.add_items_by_doi(dois=["10.1/a", "10.1/b"], ctx=ctx)
    assert result.count("✓") == 2
    assert call_count["n"] >= 2


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


def test_doi_attach_pdf_defaults_to_true(monkeypatch, patch_web_client, ctx):
    monkeypatch.setenv("UNPAYWALL_EMAIL", "tester@example.com")

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if "crossref" in url:
            return FakeRequestsResponse(CROSSREF_RESPONSE)
        if "unpaywall" in url:
            return FakeRequestsResponse(
                {"best_oa_location": {"url_for_pdf": "https://example.com/paper.pdf"}}
            )
        return FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        )

    monkeypatch.setattr(server.requests, "get", fake_get)
    result = server.add_items_by_doi(dois=["10.1038/test"], ctx=ctx)
    assert "pdf_source=unpaywall" in result
    assert patch_web_client.attached_files
    attached_path = Path(patch_web_client.attached_files[0][0][0]).name
    assert attached_path == "Smith - 2023 - Test Paper Title.pdf"


def test_doi_attach_pdf_uses_crossref_pdf_link_before_fallbacks(
    monkeypatch,
    patch_web_client,
    ctx,
):
    crossref_with_pdf_link = {
        "message": {
            **CROSSREF_RESPONSE["message"],
            "DOI": "10.1038/test",
            "link": [
                {
                    "URL": "https://example.com/test-paper.pdf",
                    "content-type": "application/pdf",
                }
            ],
        }
    }

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if "crossref" in url:
            return FakeRequestsResponse(crossref_with_pdf_link)
        if url == "https://example.com/test-paper.pdf":
            return FakeRequestsResponse(
                headers={"Content-Type": "application/pdf"},
                content=PDF_BYTES,
            )
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(server.requests, "get", fake_get)
    result = server.add_items_by_doi(dois=["10.1038/test"], ctx=ctx)
    assert "pdf_source=crossref:link" in result
    assert patch_web_client.attached_files


def test_doi_attach_pdf_can_infer_pdf_from_resolved_landing_url(
    monkeypatch,
    patch_web_client,
    ctx,
):
    crossref_with_doi_url = {
        "message": {
            **CROSSREF_RESPONSE["message"],
            "DOI": "10.21437/test",
            "URL": "https://doi.org/10.21437/test",
        }
    }

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if "crossref" in url:
            return FakeRequestsResponse(crossref_with_doi_url)
        if url == "https://doi.org/10.21437/test":
            return FakeRequestsResponse(
                status_code=200,
                headers={"Content-Type": "text/html"},
                url="https://www.isca-archive.org/interspeech_2025/inoue25b_interspeech.html",
            )
        if url == "https://www.isca-archive.org/interspeech_2025/inoue25b_interspeech.pdf":
            return FakeRequestsResponse(
                headers={"Content-Type": "application/pdf"},
                content=PDF_BYTES,
            )
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(server.requests, "get", fake_get)
    result = server.add_items_by_doi(dois=["10.21437/test"], ctx=ctx)
    assert "pdf_source=url_pattern:same_stem_pdf" in result
    assert patch_web_client.attached_files


def test_should_prefer_local_pdf_after_download_defaults_to_prefer_when_local_available(
    monkeypatch,
    fake_zot,
):
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: object())
    assert server._should_prefer_local_pdf_after_download(
        fake_zot,
        item_payload={"data": {"title": "Test"}},
        pdf_size_bytes=1024,
    )


def test_should_prefer_local_pdf_after_download_supports_threshold_mode(
    monkeypatch,
    fake_zot,
):
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: object())
    monkeypatch.setenv("ZOTERO_MCP_LOCAL_PDF_MODE", "threshold")
    monkeypatch.setenv("ZOTERO_MCP_LOCAL_PDF_THRESHOLD_MB", "10")
    assert not server._should_prefer_local_pdf_after_download(
        fake_zot,
        item_payload={"data": {"title": "Test"}},
        pdf_size_bytes=1024,
    )
    assert server._should_prefer_local_pdf_after_download(
        fake_zot,
        item_payload={"data": {"title": "Test"}},
        pdf_size_bytes=11 * 1024 * 1024,
    )


def test_doi_attach_pdf_falls_back_to_openalex_when_unpaywall_fails(
    monkeypatch,
    patch_web_client,
    ctx,
):
    monkeypatch.setenv("UNPAYWALL_EMAIL", "tester@example.com")

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if "crossref" in url:
            return FakeRequestsResponse(CROSSREF_RESPONSE)
        if "unpaywall" in url:
            raise requests_lib.ConnectionError("Connection reset by peer")
        if "api.openalex.org/works" in url:
            return FakeRequestsResponse(
                {
                    "results": [
                        {
                            "best_oa_location": {
                                "pdf_url": "https://arxiv.org/pdf/1901.00596.pdf",
                            }
                        }
                    ]
                }
            )
        if "arxiv.org/pdf/1901.00596.pdf" in url:
            return FakeRequestsResponse(
                headers={"Content-Type": "application/pdf"},
                content=PDF_BYTES,
            )
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(server.requests, "get", fake_get)
    result = server.add_items_by_doi(dois=["10.1038/test"], ctx=ctx)
    assert "pdf_source=openalex:best_oa_location" in result
    assert patch_web_client.attached_files


def test_doi_attach_pdf_falls_back_to_openalex_without_unpaywall_email(
    monkeypatch,
    patch_web_client,
    ctx,
):
    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if "crossref" in url:
            return FakeRequestsResponse(CROSSREF_RESPONSE)
        if "api.openalex.org/works" in url:
            return FakeRequestsResponse(
                {
                    "results": [
                        {
                            "best_oa_location": {
                                "pdf_url": "https://hal.science/hal-03926082/document",
                            }
                        }
                    ]
                }
            )
        if "hal.science/hal-03926082/document" in url:
            return FakeRequestsResponse(
                headers={"Content-Type": "application/pdf"},
                content=PDF_BYTES,
            )
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(server.requests, "get", fake_get)
    result = server.add_items_by_doi(dois=["10.1038/test"], ctx=ctx)
    assert "pdf_source=openalex:best_oa_location" in result
    assert patch_web_client.attached_files


def test_find_and_attach_pdfs_defaults_to_user_friendly_output(
    monkeypatch,
    patch_web_client,
    ctx,
):
    monkeypatch.delenv("ZOTERO_MCP_DEBUG_IMPORT", raising=False)
    patch_web_client._items["ITEM1"] = {
        "data": {
            "key": "ITEM1",
            "itemType": "journalArticle",
            "title": "Test Paper Title",
            "DOI": "10.1038/test",
            "url": "https://doi.org/10.1038/test",
            "collections": [],
        }
    }
    monkeypatch.setenv("UNPAYWALL_EMAIL", "tester@example.com")

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if "unpaywall" in url:
            return FakeRequestsResponse(
                {"best_oa_location": {"url_for_pdf": "https://example.com/paper.pdf"}}
            )
        return FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        )

    monkeypatch.setattr(server.requests, "get", fake_get)
    monkeypatch.setattr(server, "_fetch_page_signals", lambda url, ctx: {"pdf_candidates": []})

    result = server.find_and_attach_pdfs(item_keys=["ITEM1"], ctx=ctx)

    assert "PDF attached" in result
    assert "pdf_source=" not in result


def test_find_and_attach_pdfs_reuses_doi_pdf_discovery_from_crossref_and_resolved_landing(
    monkeypatch,
    patch_web_client,
    ctx,
):
    patch_web_client._items["ITEM1"] = {
        "data": {
            "key": "ITEM1",
            "itemType": "conferencePaper",
            "title": "Conference Test Paper",
            "DOI": "10.21437/test",
            "url": "https://doi.org/10.21437/test",
            "collections": [],
        }
    }
    crossref_with_doi_url = {
        "message": {
            **CROSSREF_PROCEEDINGS_RESPONSE["message"],
            "DOI": "10.21437/test",
            "URL": "https://doi.org/10.21437/test",
        }
    }

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if "crossref" in url:
            return FakeRequestsResponse(crossref_with_doi_url)
        if url == "https://doi.org/10.21437/test":
            return FakeRequestsResponse(
                status_code=200,
                headers={"Content-Type": "text/html"},
                url="https://www.isca-archive.org/interspeech_2025/inoue25b_interspeech.html",
            )
        if url == "https://www.isca-archive.org/interspeech_2025/inoue25b_interspeech.pdf":
            return FakeRequestsResponse(
                headers={"Content-Type": "application/pdf"},
                content=PDF_BYTES,
            )
        if "api.openalex.org/works" in url:
            return FakeRequestsResponse({"results": []})
        if "europepmc" in url:
            return FakeRequestsResponse({"resultList": {"result": []}})
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(server.requests, "get", fake_get)
    monkeypatch.setattr(server, "_fetch_page_signals", lambda url, ctx: {"pdf_candidates": []})

    result = server.find_and_attach_pdfs(item_keys=["ITEM1"], ctx=ctx)

    assert "PDF attached" in result
    assert patch_web_client.attached_files


def test_format_pdf_attach_result_surfaces_promoted_key_in_default_mode(monkeypatch):
    monkeypatch.delenv("ZOTERO_MCP_DEBUG_IMPORT", raising=False)
    result = server._format_pdf_attach_result(
        item_key="OLDKEY",
        success=True,
        pdf_source="local_zotero_file_attach_repair",
        message="ok",
        promoted_item_key="NEWKEY",
    )
    assert "effective item `NEWKEY`" in result


def test_doi_conference_paper_uses_proceedings_title_field(monkeypatch, patch_web_client, ctx):
    original_item_template = patch_web_client.item_template

    def fake_item_template(item_type):
        if item_type == "conferencePaper":
            return {
                "itemType": item_type,
                "title": "",
                "creators": [],
                "tags": [],
                "collections": [],
                "proceedingsTitle": "",
                "conferenceName": "",
                "volume": "",
                "issue": "",
                "pages": "",
                "abstractNote": "",
                "DOI": "",
                "url": "",
                "date": "",
            }
        return original_item_template(item_type)

    monkeypatch.setattr(patch_web_client, "item_template", fake_item_template)
    monkeypatch.setattr(
        server.requests,
        "get",
        lambda url, headers=None, timeout=None: FakeRequestsResponse(CROSSREF_PROCEEDINGS_RESPONSE),
    )
    result = server.add_items_by_doi(dois=["10.1109/test"], ctx=ctx)
    assert "✓" in result
    created = patch_web_client.created_items[0]
    assert created["itemType"] == "conferencePaper"
    assert created["proceedingsTitle"] == "CVPR"
    assert "publicationTitle" not in created


def test_doi_import_reuses_existing_local_copy_before_creating_new_parent(
    monkeypatch,
    patch_web_client,
    ctx,
):
    monkeypatch.setattr(
        server.requests,
        "get",
        lambda url, headers=None, timeout=None: FakeRequestsResponse(CROSSREF_RESPONSE),
    )

    local_zot = type(patch_web_client)()
    local_zot._items["LOCALDOI1"] = {
        "key": "LOCALDOI1",
        "version": 2,
        "data": {
            "key": "LOCALDOI1",
            "version": 2,
            "itemType": "conferencePaper",
            "title": "Test Paper Title",
            "DOI": "10.1038/test",
            "collections": ["OLDCOL"],
        },
    }
    local_zot._children["LOCALDOI1"] = [
        {
            "key": "LPDF1",
            "data": {
                "key": "LPDF1",
                "itemType": "attachment",
                "parentItem": "LOCALDOI1",
                "contentType": "application/pdf",
                "filename": "local.pdf",
                "title": "PDF",
            },
        }
    ]
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    monkeypatch.setattr(
        server,
        "_find_existing_local_copy_for_import",
        lambda doi, title, collection_key=None, **kwargs: local_zot._items["LOCALDOI1"]["data"],
    )

    result = server.add_items_by_doi(dois=["10.1038/test"], collection_key="NEWCOL", ctx=ctx)

    assert "pdf_source=local_zotero_existing_copy" in result
    assert "LOCALDOI1" in result
    assert not patch_web_client.created_items
    assert any(col_key == "NEWCOL" for col_key, _ in local_zot.added_to) or any(
        col_key == "NEWCOL" for col_key, _ in patch_web_client.added_to
    )


# ── zotero_add_items_by_arxiv ─────────────────────────────────────────────────

def _make_arxiv_urlopen(data: bytes):
    def fake_urlopen(url, timeout=None):
        return FakeURLResponse(data)
    return fake_urlopen


def test_arxiv_bare_id(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(urllib.request, "urlopen", _make_arxiv_urlopen(ARXIV_XML))
    monkeypatch.setattr(
        server.requests,
        "get",
        lambda url, **kwargs: FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        ),
    )
    result = server.add_items_by_arxiv(arxiv_ids=["2301.12345"], ctx=ctx)
    assert "✓" in result
    assert "pdf_source=arxiv_pdf" in result
    assert patch_web_client.attached_files
    attached_path = Path(patch_web_client.attached_files[0][0][0]).name
    assert attached_path == "Smith - 2023 - Test Paper Title.pdf"
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


def test_arxiv_pdf_url_variant_resolves(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(urllib.request, "urlopen", _make_arxiv_urlopen(ARXIV_XML))
    monkeypatch.setattr(
        server.requests,
        "get",
        lambda url, **kwargs: FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        ),
    )
    result = server.add_items_by_arxiv(
        arxiv_ids=["https://arxiv.org/pdf/2301.12345.pdf"],
        ctx=ctx,
    )
    assert "✓" in result
    assert "route=arxiv" in result


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

LANDING_WITH_DOI_AND_PDF = b"""\
<html>
<head>
  <meta name="citation_title" content="Landing Page Paper" />
  <meta name="citation_doi" content="10.1038/test" />
  <meta name="citation_pdf_url" content="/paper.pdf" />
</head>
<body></body>
</html>"""

LANDING_WITH_PDF_ONLY = b"""\
<html>
<head>
  <meta property="og:title" content="PDF Only Landing" />
  <meta name="citation_pdf_url" content="/fallback.pdf" />
</head>
<body></body>
</html>"""

LANDING_WITH_RICH_METADATA = b"""\
<html>
<head>
  <meta name="citation_title" content="Retrieval-Augmented Open-Vocabulary Object Detection" />
  <meta name="citation_author" content="Kim, Jooyeon" />
  <meta name="citation_author" content="Cho, Eulrang" />
  <meta name="citation_publication_date" content="2024" />
  <meta name="citation_abstract" content="A strong CVPR paper abstract." />
  <meta name="citation_pdf_url" content="/paper.pdf" />
</head>
<body></body>
</html>"""

LANDING_WITH_HTTP_PDF_META = b"""\
<html>
<head>
  <meta name="citation_title" content="Learning Transferable Visual Models From Natural Language Supervision" />
  <meta name="citation_pdf_url" content="http://proceedings.mlr.press/v139/radford21a/radford21a.pdf" />
</head>
<body></body>
</html>"""

LANDING_WITH_BODY_ABSTRACT = b"""\
<html>
<head>
  <meta name="citation_title" content="Body Abstract Paper" />
  <meta name="citation_author" content="Doe, Jane" />
  <meta name="citation_publication_date" content="2025" />
  <meta name="citation_pdf_url" content="/paper.pdf" />
</head>
<body>
  <div id="abstract">Body abstract extracted from landing page.</div>
</body>
</html>"""

PDF_BYTES = b"%PDF-1.4\nfake pdf content"
HTML_BYTES = b"<html>not a pdf</html>"


def test_url_og_title_used(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(
        urllib.request, "urlopen",
        lambda req, timeout=None: FakeURLResponse(OG_TITLE_HTML),
    )
    result = server.add_item_by_url(url="https://example.com", ctx=ctx)
    assert "✓" in result
    assert "OG Page Title" in result
    assert "route=webpage" in result
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


def test_identifier_prefers_doi_from_landing_page(monkeypatch, patch_web_client, ctx):
    monkeypatch.setenv("UNPAYWALL_EMAIL", "tester@example.com")

    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(LANDING_WITH_DOI_AND_PDF),
    )

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if "crossref" in url:
            return FakeRequestsResponse(CROSSREF_RESPONSE)
        if "paper.pdf" in url:
            return FakeRequestsResponse(
                headers={"Content-Type": "application/pdf"},
                content=PDF_BYTES,
            )
        if "unpaywall" in url:
            return FakeRequestsResponse({"best_oa_location": {}})
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(server.requests, "get", fake_get)
    result = server.add_items_by_identifier(
        identifiers=["https://publisher.example.com/paper"],
        ctx=ctx,
    )
    assert "route=doi" in result
    assert "pdf_source=html:citation_pdf_url" in result
    assert patch_web_client.created_items[0]["itemType"] != "webpage"


def test_identifier_retries_trimmed_doi_from_publisher_landing_url(
    monkeypatch,
    patch_web_client,
    ctx,
):
    crossref_urls = []

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if "crossref" in url:
            crossref_urls.append(url)
            if url.endswith("/10.1093/nsr/nwaf086/8052010"):
                return FakeRequestsResponse({}, status_code=404)
            if url.endswith("/10.1093/nsr/nwaf086"):
                return FakeRequestsResponse(CROSSREF_RESPONSE)
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(server.requests, "get", fake_get)
    result = server.add_items_by_identifier(
        identifiers=["https://academic.oup.com/nsr/article/doi/10.1093/nsr/nwaf086/8052010"],
        attach_pdf=False,
        ctx=ctx,
    )

    assert "route=doi" in result
    assert patch_web_client.created_items
    assert crossref_urls == [
        "https://api.crossref.org/works/10.1093/nsr/nwaf086/8052010",
        "https://api.crossref.org/works/10.1093/nsr/nwaf086",
    ]


def test_identifier_infers_mdpi_doi_from_structured_url_when_page_fetch_is_blocked(
    monkeypatch,
    patch_web_client,
    ctx,
):
    def fail_urlopen(_req, timeout=None):
        raise OSError("403 Forbidden")

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if "api.crossref.org/works?" in url or (url.endswith("/works") and params):
            return FakeRequestsResponse(CROSSREF_MDPI_SEARCH_RESPONSE)
        if url.endswith("/10.3390/rs13030516"):
            return FakeRequestsResponse(CROSSREF_MDPI_WORK)
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(urllib.request, "urlopen", fail_urlopen)
    monkeypatch.setattr(server.requests, "get", fake_get)

    result = server.add_items_by_identifier(
        identifiers=["https://www.mdpi.com/2072-4292/13/3/516"],
        attach_pdf=False,
        ctx=ctx,
    )

    assert "route=doi" in result
    assert "10.3390/rs13030516" == patch_web_client.created_items[0]["DOI"]
    assert patch_web_client.created_items[0]["itemType"] != "webpage"


def test_identifier_falls_back_to_webpage_with_pdf(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(LANDING_WITH_PDF_ONLY),
    )
    monkeypatch.setattr(
        server.requests,
        "get",
        lambda url, **kwargs: FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        ),
    )
    result = server.add_items_by_identifier(
        identifiers=["https://example.com/landing"],
        ctx=ctx,
    )
    assert "route=webpage" in result
    assert "fallback_reason=missing_identifier" in result
    assert "pdf_source=html:citation_pdf_url" in result
    assert patch_web_client.created_items[0]["itemType"] == "webpage"
    assert patch_web_client.created_items[0]["tags"] == [{"tag": "needs-metadata"}]


def test_identifier_falls_back_to_webpage_with_pdf_defaults_to_user_friendly_output(
    monkeypatch,
    patch_web_client,
    ctx,
):
    monkeypatch.delenv("ZOTERO_MCP_DEBUG_IMPORT", raising=False)
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(LANDING_WITH_PDF_ONLY),
    )
    monkeypatch.setattr(
        server.requests,
        "get",
        lambda url, **kwargs: FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        ),
    )

    result = server.add_items_by_identifier(
        identifiers=["https://example.com/landing"],
        ctx=ctx,
    )

    assert "Saved as webpage + PDF attached" in result
    assert "route=webpage" not in result
    assert "pdf_source=" not in result


def test_fetch_page_signals_appends_url_inference_pdf_candidates_after_html_candidates(monkeypatch, ctx):
    class StableURLResponse(FakeURLResponse):
        def geturl(self):
            return "https://proceedings.mlr.press/v139/radford21a.html"

    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: StableURLResponse(LANDING_WITH_HTTP_PDF_META),
    )
    signals = server._fetch_page_signals(
        "https://proceedings.mlr.press/v139/radford21a.html",
        ctx=ctx,
    )
    assert signals["pdf_candidates"][:3] == [
        {
            "source": "html:citation_pdf_url:https_upgrade",
            "url": "https://proceedings.mlr.press/v139/radford21a/radford21a.pdf",
        },
        {
            "source": "html:citation_pdf_url",
            "url": "http://proceedings.mlr.press/v139/radford21a/radford21a.pdf",
        },
        {
            "source": "url_pattern:same_stem_pdf",
            "url": "https://proceedings.mlr.press/v139/radford21a.pdf",
        },
    ]


def test_identifier_falls_back_from_http_meta_pdf_to_inferred_https_pdf(
    monkeypatch,
    patch_web_client,
    ctx,
):
    class StableURLResponse(FakeURLResponse):
        def geturl(self):
            return "https://proceedings.mlr.press/v139/radford21a.html"

    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: StableURLResponse(LANDING_WITH_HTTP_PDF_META),
    )

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if url == "https://proceedings.mlr.press/v139/radford21a/radford21a.pdf":
            return FakeRequestsResponse(
                headers={"Content-Type": "application/pdf"},
                content=PDF_BYTES,
            )
        if url == "http://proceedings.mlr.press/v139/radford21a/radford21a.pdf":
            raise requests_lib.ReadTimeout("timed out")
        if url == "https://proceedings.mlr.press/v139/radford21a.pdf":
            return FakeRequestsResponse(
                status_code=404,
                headers={"Content-Type": "text/html"},
                content=b"not found",
            )
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(server.requests, "get", fake_get)
    result = server.add_items_by_identifier(
        identifiers=["https://proceedings.mlr.press/v139/radford21a.html"],
        ctx=ctx,
    )
    assert "route=webpage" in result
    assert "pdf_source=html:citation_pdf_url:https_upgrade" in result
    assert patch_web_client.attached_files


def test_identifier_webpage_fallback_salvages_metadata(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(LANDING_WITH_RICH_METADATA),
    )
    monkeypatch.setattr(
        server.requests,
        "get",
        lambda url, **kwargs: FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        ),
    )
    result = server.add_items_by_identifier(
        identifiers=["https://example.com/rich-landing"],
        ctx=ctx,
    )
    created = patch_web_client.created_items[0]
    assert "route=webpage" in result
    assert created["title"] == "Retrieval-Augmented Open-Vocabulary Object Detection"
    assert created["date"] == "2024"
    assert created["abstractNote"] == "A strong CVPR paper abstract."
    assert created["creators"] == [
        {"creatorType": "author", "firstName": "Jooyeon", "lastName": "Kim"},
        {"creatorType": "author", "firstName": "Eulrang", "lastName": "Cho"},
    ]


def test_identifier_uses_crossref_title_lookup_before_webpage_fallback(
    monkeypatch,
    patch_web_client,
    ctx,
):
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(LANDING_WITH_RICH_METADATA),
    )

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if url == "https://api.crossref.org/works":
            return FakeRequestsResponse(
                {
                    "message": {
                        "items": [
                            {
                                "DOI": "10.1038/test",
                                "title": ["Retrieval-Augmented Open-Vocabulary Object Detection"],
                                "author": [{"given": "Jooyeon", "family": "Kim"}],
                                "published": {"date-parts": [[2024]]},
                            }
                        ]
                    }
                }
            )
        if "crossref" in url:
            return FakeRequestsResponse(CROSSREF_RESPONSE)
        if "paper.pdf" in url:
            return FakeRequestsResponse(
                headers={"Content-Type": "application/pdf"},
                content=PDF_BYTES,
            )
        if "unpaywall" in url:
            return FakeRequestsResponse({"best_oa_location": {}})
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(server.requests, "get", fake_get)
    result = server.add_items_by_identifier(
        identifiers=["https://example.com/rich-landing"],
        ctx=ctx,
    )
    assert "route=doi" in result
    assert patch_web_client.created_items[0]["itemType"] != "webpage"


def test_identifier_webpage_fallback_extracts_body_abstract(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(LANDING_WITH_BODY_ABSTRACT),
    )
    monkeypatch.setattr(
        server.requests,
        "get",
        lambda url, **kwargs: FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        ),
    )
    result = server.add_items_by_identifier(
        identifiers=["https://example.com/body-abstract"],
        ctx=ctx,
    )
    created = patch_web_client.created_items[0]
    assert "route=webpage" in result
    assert created["abstractNote"] == "Body abstract extracted from landing page."


def test_fetch_page_signals_cvf_timeout_uses_url_derived_fallback(ctx):
    signals = server._fallback_signals_from_known_landing_page(
        "https://openaccess.thecvf.com/content/CVPR2024/html/"
        "Kim_Retrieval-Augmented_Open-Vocabulary_Object_Detection_CVPR_2024_paper.html"
    )
    assert signals is not None
    assert signals["title"] == "Retrieval-Augmented Open-Vocabulary Object Detection"
    assert signals["venue"] == "CVPR"
    assert signals["date"] == "2024"
    assert signals["pdf_candidates"] == [
        {
            "source": "url_pattern:cvf_pdf",
            "url": "https://openaccess.thecvf.com/content/CVPR2024/papers/"
                   "Kim_Retrieval-Augmented_Open-Vocabulary_Object_Detection_CVPR_2024_paper.pdf",
        }
    ]


def test_fetch_page_signals_cvf_timeout_returns_fallback(monkeypatch, ctx):
    def fail_urlopen(req, timeout=None):
        raise urllib.error.URLError("timed out")

    monkeypatch.setattr(urllib.request, "urlopen", fail_urlopen)
    signals = server._fetch_page_signals(
        "https://openaccess.thecvf.com/content/CVPR2024/html/"
        "Kim_Retrieval-Augmented_Open-Vocabulary_Object_Detection_CVPR_2024_paper.html",
        ctx=ctx,
    )
    assert signals["title"] == "Retrieval-Augmented Open-Vocabulary Object Detection"
    assert signals["date"] == "2024"
    assert signals["pdf_candidates"][0]["source"] == "url_pattern:cvf_pdf"


def test_fetch_page_signals_cvf_legacy_path_timeout_returns_fallback(monkeypatch, ctx):
    def fail_urlopen(req, timeout=None):
        raise urllib.error.URLError("timed out")

    monkeypatch.setattr(urllib.request, "urlopen", fail_urlopen)
    signals = server._fetch_page_signals(
        "https://openaccess.thecvf.com/content_ICCV_2017/html/"
        "Wang_Learning_to_Detect_Instances_ICCV_2017_paper.html",
        ctx=ctx,
    )
    assert signals["title"] == "Learning to Detect Instances"
    assert signals["venue"] == "ICCV"
    assert signals["date"] == "2017"
    assert signals["pdf_candidates"] == [
        {
            "source": "url_pattern:cvf_pdf",
            "url": "https://openaccess.thecvf.com/content_ICCV_2017/papers/"
                   "Wang_Learning_to_Detect_Instances_ICCV_2017_paper.pdf",
        }
    ]


def test_fetch_page_signals_timeout_uses_general_url_inference_for_pmlr(monkeypatch, ctx):
    def fail_urlopen(req, timeout=None):
        raise urllib.error.URLError("timed out")

    monkeypatch.setattr(urllib.request, "urlopen", fail_urlopen)
    signals = server._fetch_page_signals(
        "https://proceedings.mlr.press/v139/radford21a.html",
        ctx=ctx,
    )
    assert signals["title"] is None
    assert signals["venue"] == "Proceedings of Machine Learning Research"
    assert signals["date"] == ""
    assert signals["pdf_candidates"] == [
        {
            "source": "url_pattern:same_stem_pdf",
            "url": "https://proceedings.mlr.press/v139/radford21a.pdf",
        },
        {
            "source": "url_pattern:stem_subdir_pdf",
            "url": "https://proceedings.mlr.press/v139/radford21a/radford21a.pdf",
        },
    ]


def test_fetch_page_signals_timeout_uses_general_url_inference_for_verbose_url(monkeypatch, ctx):
    def fail_urlopen(req, timeout=None):
        raise urllib.error.URLError("timed out")

    monkeypatch.setattr(urllib.request, "urlopen", fail_urlopen)
    signals = server._fetch_page_signals(
        "https://papers.example.org/Smith_Deep_Learning_for_Science_CVPR_2025.html",
        ctx=ctx,
    )
    assert signals["title"] == "Smith Deep Learning for Science"
    assert signals["venue"] == "CVPR"
    assert signals["date"] == "2025"
    assert signals["pdf_candidates"][0] == {
        "source": "url_pattern:same_stem_pdf",
        "url": "https://papers.example.org/Smith_Deep_Learning_for_Science_CVPR_2025.pdf",
    }


def test_identifier_cvf_timeout_falls_back_to_url_derived_title_lookup(
    monkeypatch,
    patch_web_client,
    ctx,
):
    import urllib.request

    cvf_url = (
        "https://openaccess.thecvf.com/content/CVPR2024/html/"
        "Kim_Retrieval-Augmented_Open-Vocabulary_Object_Detection_CVPR_2024_paper.html"
    )

    def fail_urlopen(req, timeout=None):
        raise urllib.error.URLError("timed out")

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if url == "https://api.crossref.org/works":
            return FakeRequestsResponse(
                {
                    "message": {
                        "items": [
                            {
                                "DOI": "10.1109/cvpr52733.2024.01650",
                                "title": ["Retrieval-Augmented Open-Vocabulary Object Detection"],
                                "author": [{"given": "Jooyeon", "family": "Kim"}],
                                "published": {"date-parts": [[2024]]},
                            }
                        ]
                    }
                }
            )
        if "crossref" in url:
            return FakeRequestsResponse(
                {
                    "message": {
                        "title": ["Retrieval-Augmented Open-Vocabulary Object Detection"],
                        "author": [{"given": "Jooyeon", "family": "Kim"}],
                        "published": {"date-parts": [[2024, 6, 16]]},
                        "container-title": ["CVPR"],
                        "page": "1650-1660",
                        "abstract": "A strong CVPR paper abstract.",
                        "URL": "https://doi.org/10.1109/cvpr52733.2024.01650",
                        "type": "proceedings-article",
                    }
                }
            )
        if "openaccess.thecvf.com/content/CVPR2024/papers/" in url:
            return FakeRequestsResponse(
                headers={"Content-Type": "application/pdf"},
                content=PDF_BYTES,
            )
        if "unpaywall" in url or "api.openalex.org/works" in url:
            return FakeRequestsResponse({"results": [], "best_oa_location": {}})
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(urllib.request, "urlopen", fail_urlopen)
    monkeypatch.setattr(server.requests, "get", fake_get)

    result = server.add_items_by_identifier(
        identifiers=[cvf_url],
        ctx=ctx,
    )

    assert "route=doi" in result
    assert "pdf_source=url_pattern:cvf_pdf" in result
    assert patch_web_client.created_items[0]["itemType"] == "conferencePaper"


def test_identifier_pmlr_timeout_falls_back_to_webpage_with_inferred_pdf(
    monkeypatch,
    patch_web_client,
    ctx,
):
    def fail_urlopen(req, timeout=None):
        raise urllib.error.URLError("timed out")

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if url == "https://proceedings.mlr.press/v139/radford21a.pdf":
            return FakeRequestsResponse(
                status_code=404,
                headers={"Content-Type": "text/html"},
                content=b"not found",
            )
        if url == "https://proceedings.mlr.press/v139/radford21a/radford21a.pdf":
            return FakeRequestsResponse(
                headers={"Content-Type": "application/pdf"},
                content=PDF_BYTES,
            )
        if "api.crossref.org/works" in url or "api.openalex.org/works" in url:
            return FakeRequestsResponse({"message": {"items": []}, "results": []})
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(urllib.request, "urlopen", fail_urlopen)
    monkeypatch.setattr(server.requests, "get", fake_get)

    result = server.add_items_by_identifier(
        identifiers=["https://proceedings.mlr.press/v139/radford21a.html"],
        ctx=ctx,
    )

    assert "route=webpage" in result
    assert "pdf_source=url_pattern:stem_subdir_pdf" in result
    assert patch_web_client.attached_files
    assert patch_web_client.created_items[0]["itemType"] == "webpage"
    assert (
        patch_web_client.created_items[0]["title"]
        == "https://proceedings.mlr.press/v139/radford21a.html"
    )


def test_lookup_crossref_doi_uses_partial_title_venue_year_author_hints(monkeypatch, ctx):
    signals = {
        "title": "LLMDet Learning Strong Open-Vocabulary Object Detectors under the Supervision of",
        "venue": "CVPR",
        "date": "2025",
        "creators": [{"creatorType": "author", "lastName": "Fu"}],
        "source_url": "https://example.com/landing",
        "final_url": "https://example.com/landing",
        "pdf_candidates": [
            {
                "source": "landing_pdf",
                "url": "https://cdn.example.com/Fu_LLMDet_Learning_Strong_Open-Vocabulary_Object_Detectors_under_the_Supervision_of_Large_Language_Models_CVPR_2025_paper.pdf",
            }
        ],
    }

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        assert url == "https://api.crossref.org/works"
        return FakeRequestsResponse(
            {
                "message": {
                    "items": [
                        {
                            "DOI": "10.5555/wrong",
                            "title": ["LLMDet Learning Strong Open-Vocabulary Object Detectors"],
                            "author": [{"given": "Tom", "family": "Lee"}],
                            "published": {"date-parts": [[2024]]},
                            "container-title": ["ECCV"],
                        },
                        {
                            "DOI": "10.5555/llmdet",
                            "title": [
                                "LLMDet Learning Strong Open-Vocabulary Object Detectors under the Supervision of Large Language Models"
                            ],
                            "author": [{"given": "Yuchong", "family": "Fu"}],
                            "published": {"date-parts": [[2025]]},
                            "container-title": ["CVPR"],
                        },
                    ]
                }
            }
        )

    monkeypatch.setattr(server.requests, "get", fake_get)
    doi = server._lookup_crossref_doi_for_signals(signals, ctx=ctx)
    assert doi == "10.5555/llmdet"


def test_identifier_uses_url_pdf_venue_hints_when_title_missing(
    monkeypatch,
    patch_web_client,
    ctx,
):
    def fake_fetch_page_signals(url, *, ctx):
        return {
            "source_url": url,
            "final_url": url,
            "title": None,
            "venue": "Nature",
            "description": "",
            "abstract_note": "",
            "creators": [{"creatorType": "author", "lastName": "Smith", "firstName": "Alice"}],
            "date": "2023",
            "doi": None,
            "arxiv_id": None,
            "pdf_candidates": [
                {
                    "source": "landing_pdf",
                    "url": "https://cdn.example.com/Smith_Imaging_Biomarkers_for_Neural_Decoding_Nature_2023.pdf",
                }
            ],
            "content_type": "",
        }

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if url == "https://api.crossref.org/works":
            return FakeRequestsResponse(
                {
                    "message": {
                        "items": [
                            {
                                "DOI": "10.1038/biomarkers",
                                "title": ["Imaging Biomarkers for Neural Decoding"],
                                "author": [{"given": "Alice", "family": "Smith"}],
                                "published": {"date-parts": [[2023]]},
                                "container-title": ["Nature"],
                            }
                        ]
                    }
                }
            )
        if url == "https://api.crossref.org/works/10.1038/biomarkers":
            return FakeRequestsResponse(
                {
                    "message": {
                        "title": ["Imaging Biomarkers for Neural Decoding"],
                        "author": [{"given": "Alice", "family": "Smith"}],
                        "published": {"date-parts": [[2023, 5, 1]]},
                        "container-title": ["Nature"],
                        "abstract": "A Nature paper.",
                        "URL": "https://doi.org/10.1038/biomarkers",
                        "type": "journal-article",
                    }
                }
            )
        if "Smith_Imaging_Biomarkers_for_Neural_Decoding_Nature_2023.pdf" in url:
            return FakeRequestsResponse(
                headers={"Content-Type": "application/pdf"},
                content=PDF_BYTES,
            )
        if "unpaywall" in url or "api.openalex.org/works" in url:
            return FakeRequestsResponse({"results": [], "best_oa_location": {}})
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(server, "_fetch_page_signals", fake_fetch_page_signals)
    monkeypatch.setattr(server.requests, "get", fake_get)
    result = server.add_items_by_identifier(
        identifiers=["https://publisher.example.com/landing-page"],
        ctx=ctx,
    )
    assert "route=doi" in result
    assert "pdf_source=landing_pdf" in result
    assert patch_web_client.created_items[0]["itemType"] == "journalArticle"


def test_identifier_direct_pdf_html_error_does_not_attach(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(
        server.requests,
        "get",
        lambda url, **kwargs: FakeRequestsResponse(
            headers={"Content-Type": "text/html"},
            content=HTML_BYTES,
        ),
    )
    result = server.add_items_by_identifier(
        identifiers=["https://example.com/bad.pdf"],
        ctx=ctx,
    )
    assert "route=webpage" in result
    assert "pdf_source=none" in result
    assert not patch_web_client.attached_files


def test_identifier_direct_pdf_probe_prefers_paper_import_over_webpage(
    monkeypatch,
    patch_web_client,
    ctx,
):
    class FakePage:
        def get_text(self, *_args, **_kwargs):
            return "Test Paper Title\nAlice Smith\nDOI: 10.1038/test\nAbstract\nUseful text."

    class FakeDoc:
        metadata = {"title": "Test Paper Title", "author": "Alice Smith"}

        def __len__(self):
            return 1

        def load_page(self, _page_index):
            return FakePage()

        def close(self):
            pass

    monkeypatch.setattr(
        server,
        "_download_pdf_bytes",
        lambda pdf_url, *, ctx=None: (PDF_BYTES, "application/pdf"),
    )
    monkeypatch.setitem(
        sys.modules,
        "fitz",
        types.SimpleNamespace(open=lambda stream, filetype: FakeDoc()),
    )
    monkeypatch.setattr(
        server.requests,
        "get",
        lambda url, headers=None, timeout=None, **kwargs: FakeRequestsResponse(CROSSREF_RESPONSE),
    )

    result = server.add_items_by_identifier(
        identifiers=["https://example.com/test-paper.pdf"],
        attach_pdf=False,
        ctx=ctx,
    )

    assert "route=doi" in result
    assert patch_web_client.created_items[0]["itemType"] == "journalArticle"
    assert patch_web_client.created_items[0]["DOI"] == "10.1038/test"


def test_identifier_direct_pdf_probe_can_use_playwright_download_fallback(
    monkeypatch,
    patch_web_client,
    ctx,
):
    class FakePage:
        def get_text(self, *_args, **_kwargs):
            return "Test Paper Title\nAlice Smith\nDOI: 10.1038/test\nUseful text."

    class FakeDoc:
        metadata = {"title": "Test Paper Title", "author": "Alice Smith"}

        def __len__(self):
            return 1

        def load_page(self, _page_index):
            return FakePage()

        def close(self):
            pass

    def fake_get(url, headers=None, timeout=None, **kwargs):
        if "api.crossref.org/works/10.1038/test" in url:
            return FakeRequestsResponse(CROSSREF_RESPONSE)
        raise requests_lib.HTTPError("403 forbidden")

    monkeypatch.setattr(server.requests, "get", fake_get)
    monkeypatch.setattr(server.time, "sleep", lambda *_: None)
    monkeypatch.setattr(
        server,
        "_download_pdf_bytes_via_playwright",
        lambda pdf_url, *, ctx=None: (PDF_BYTES, "application/pdf"),
    )
    monkeypatch.setitem(
        sys.modules,
        "fitz",
        types.SimpleNamespace(open=lambda stream, filetype: FakeDoc()),
    )

    result = server.add_items_by_identifier(
        identifiers=["https://example.com/protected.pdf"],
        attach_pdf=False,
        ctx=ctx,
    )

    assert "route=doi" in result
    assert patch_web_client.created_items[0]["itemType"] == "journalArticle"


def test_identifier_direct_pdf_probe_can_rescue_doi_via_crossref_title_match(
    monkeypatch,
    patch_web_client,
    ctx,
):
    class FakePage:
        def get_text(self, *_args, **_kwargs):
            return "Test Paper Title\nAlice Smith\nAbstract\nUseful text without explicit DOI."

    class FakeDoc:
        metadata = {"title": "Test Paper Title", "author": "Alice Smith"}

        def __len__(self):
            return 1

        def load_page(self, _page_index):
            return FakePage()

        def close(self):
            pass

    def fake_get(url, headers=None, timeout=None, params=None, **kwargs):
        if "api.crossref.org/works?" in url or (url.endswith("/works") and params):
            return FakeRequestsResponse({"message": {"items": [CROSSREF_RESPONSE["message"] | {"DOI": "10.1038/test"}]}})
        if url.endswith("/10.1038/test"):
            return FakeRequestsResponse(CROSSREF_RESPONSE)
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(
        server,
        "_download_pdf_bytes",
        lambda pdf_url, *, ctx=None: (PDF_BYTES, "application/pdf"),
    )
    monkeypatch.setitem(
        sys.modules,
        "fitz",
        types.SimpleNamespace(open=lambda stream, filetype: FakeDoc()),
    )
    monkeypatch.setattr(server.requests, "get", fake_get)

    result = server.add_items_by_identifier(
        identifiers=["https://example.com/title-only.pdf"],
        attach_pdf=False,
        ctx=ctx,
    )

    assert "route=doi" in result
    assert patch_web_client.created_items[0]["itemType"] == "journalArticle"
    assert patch_web_client.created_items[0]["DOI"] == "10.1038/test"


def test_probe_identifier_from_direct_pdf_url_falls_back_to_local_connector_browser_session(
    monkeypatch,
    patch_web_client,
    ctx,
    tmp_path,
):
    local_zot = type(patch_web_client)()
    local_zot.client = object()
    state = {"save_items": 0, "save_attachment": 0}

    class FakePage:
        def get_text(self, *_args, **_kwargs):
            return "Test Paper Title\nAlice Smith\nDOI: 10.1038/test\nUseful text."

    class FakeDoc:
        metadata = {"title": "Test Paper Title", "author": "Alice Smith"}

        def __len__(self):
            return 1

        def load_page(self, _page_index):
            return FakePage()

        def close(self):
            pass

    class PostResponse:
        def __init__(self, status_code=201):
            self.status_code = status_code
            self.text = ""

        def raise_for_status(self):
            return None

    def fake_post(url, json=None, params=None, data=None, headers=None, timeout=None):
        if url.endswith("/connector/saveItems"):
            state["save_items"] += 1
            probe_title = json["items"][0]["title"]
            local_zot._items["LOCALPROBE"] = {
                "data": {
                    "key": "LOCALPROBE",
                    "itemType": "webpage",
                    "title": probe_title,
                    "url": "https://example.com/protected.pdf",
                    "collections": [],
                }
            }
            return PostResponse(201)
        if url.endswith("/connector/saveAttachment"):
            state["save_attachment"] += 1
            assert data == b""
            local_zot._children["LOCALPROBE"] = [
                {
                    "key": "LPDF1",
                    "data": {
                        "key": "LPDF1",
                        "itemType": "attachment",
                        "parentItem": "LOCALPROBE",
                        "contentType": "application/pdf",
                        "filename": "protected.pdf",
                        "title": "PDF",
                    },
                }
            ]
            return PostResponse(201)
        raise AssertionError(f"unexpected POST url: {url}")

    def fake_dump(zot, attachment_key, dest, *, ctx):
        dest.write_bytes(PDF_BYTES)

    monkeypatch.setattr(
        server,
        "_download_pdf_bytes",
        lambda pdf_url, *, ctx=None: (_ for _ in ()).throw(RuntimeError("403 forbidden")),
    )
    monkeypatch.setattr(server.requests, "post", fake_post)
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    monkeypatch.setattr(
        server,
        "_confirm_local_pdf_attachment_materialized",
        lambda item_key, *, ctx, wait_seconds=12.0, poll_interval=0.5: {
            "success": True,
            "attachment_key": "LPDF1",
        },
    )
    monkeypatch.setattr(server, "dump_attachment_to_file", fake_dump)
    monkeypatch.setitem(
        sys.modules,
        "fitz",
        types.SimpleNamespace(open=lambda stream, filetype: FakeDoc()),
    )

    signals = server._probe_identifier_from_direct_pdf_url(
        "https://example.com/protected.pdf",
        ctx=ctx,
    )

    assert signals is not None
    assert signals["doi"] == "10.1038/test"
    assert signals["title"] == "Test Paper Title"
    assert state["save_items"] == 1
    assert state["save_attachment"] == 1
    assert local_zot.deleted_items


def test_identifier_pdf_content_type_url_uses_pdf_probe_before_webpage(
    monkeypatch,
    patch_web_client,
    ctx,
):
    def fake_fetch_page_signals(url, *, ctx):
        return {
            "source_url": url,
            "final_url": "https://example.com/download/123",
            "title": None,
            "venue": "",
            "description": "",
            "abstract_note": "",
            "creators": [],
            "date": "",
            "doi": None,
            "arxiv_id": None,
            "pdf_candidates": [{"source": "direct_pdf", "url": "https://example.com/download/123"}],
            "content_type": "application/pdf",
        }

    monkeypatch.setattr(server, "_fetch_page_signals", fake_fetch_page_signals)
    monkeypatch.setattr(
        server,
        "_probe_identifier_from_direct_pdf_url",
        lambda pdf_url, ctx: {
            "source_url": pdf_url,
            "final_url": pdf_url,
            "title": "Test Paper Title",
            "venue": "",
            "description": "",
            "abstract_note": "",
            "creators": [{"creatorType": "author", "firstName": "Alice", "lastName": "Smith"}],
            "date": "2023",
            "doi": "10.1038/test",
            "arxiv_id": None,
            "pdf_candidates": [{"source": "direct_pdf", "url": pdf_url}],
            "content_type": "application/pdf",
        },
    )
    monkeypatch.setattr(
        server.requests,
        "get",
        lambda url, headers=None, timeout=None, **kwargs: FakeRequestsResponse(CROSSREF_RESPONSE),
    )

    result = server.add_items_by_identifier(
        identifiers=["https://example.com/pdf"],
        attach_pdf=False,
        ctx=ctx,
    )

    assert "route=doi" in result
    assert patch_web_client.created_items[0]["itemType"] == "journalArticle"


def test_identifier_pdf_content_type_url_retries_probe_with_source_url_when_final_url_fails(
    monkeypatch,
    patch_web_client,
    ctx,
):
    probe_calls = []

    def fake_fetch_page_signals(url, *, ctx):
        return {
            "source_url": url,
            "final_url": "https://example.com/final-403.pdf",
            "title": None,
            "venue": "",
            "description": "",
            "abstract_note": "",
            "creators": [],
            "date": "",
            "doi": None,
            "arxiv_id": None,
            "pdf_candidates": [{"source": "direct_pdf", "url": "https://example.com/final-403.pdf"}],
            "content_type": "application/pdf",
        }

    def fake_probe(pdf_url, *, ctx):
        probe_calls.append(pdf_url)
        if pdf_url == "https://example.com/final-403.pdf":
            return None
        return {
            "source_url": pdf_url,
            "final_url": pdf_url,
            "title": "Test Paper Title",
            "venue": "",
            "description": "",
            "abstract_note": "",
            "creators": [{"creatorType": "author", "firstName": "Alice", "lastName": "Smith"}],
            "date": "2023",
            "doi": "10.1038/test",
            "arxiv_id": None,
            "pdf_candidates": [{"source": "direct_pdf", "url": pdf_url}],
            "content_type": "application/pdf",
        }

    monkeypatch.setattr(server, "_fetch_page_signals", fake_fetch_page_signals)
    monkeypatch.setattr(server, "_probe_identifier_from_direct_pdf_url", fake_probe)
    monkeypatch.setattr(
        server.requests,
        "get",
        lambda url, headers=None, timeout=None, **kwargs: FakeRequestsResponse(CROSSREF_RESPONSE),
    )

    result = server.add_items_by_identifier(
        identifiers=["https://example.com/pdf"],
        attach_pdf=False,
        ctx=ctx,
    )

    assert "route=doi" in result
    assert probe_calls[:2] == ["https://example.com/final-403.pdf", "https://example.com/pdf"]
    assert patch_web_client.created_items[0]["itemType"] == "journalArticle"


def test_dump_attachment_to_file_prefers_resolved_local_path(monkeypatch, tmp_path, ctx):
    source = tmp_path / "source.pdf"
    source.write_bytes(PDF_BYTES)
    dest = tmp_path / "dest.pdf"

    class FakeLocalZot:
        def dump(self, *args, **kwargs):
            raise AssertionError("zot.dump should not be called when resolved local path exists")

    monkeypatch.setattr(server, "_resolve_local_attachment_path", lambda key: source)
    server.dump_attachment_to_file(FakeLocalZot(), "ATTACH1", dest, ctx=ctx)
    assert dest.read_bytes() == PDF_BYTES


def test_confirm_local_pdf_attachment_materialized_skips_noisy_dump_for_unmaterialized_placeholder(
    monkeypatch,
    tmp_path,
    ctx,
):
    class FakeLocalZot:
        pass

    missing = tmp_path / "missing.pdf"
    calls = {"dump": 0}

    monkeypatch.setattr(server, "get_local_zotero_client", lambda: FakeLocalZot())
    monkeypatch.setattr(
        server,
        "_iter_pdf_attachments",
        lambda zot, item_key: [
            {
                "key": "ATTACH1",
                "data": {"key": "ATTACH1", "itemType": "attachment", "filename": "probe.pdf"},
            }
        ],
    )
    monkeypatch.setattr(server, "_attachment_file_exists_locally", lambda key: False)
    monkeypatch.setattr(server, "_resolve_local_attachment_path", lambda key: missing)

    def fake_dump(*args, **kwargs):
        calls["dump"] += 1
        raise AssertionError("dump_attachment_to_file should not be called for missing materialized path")

    monkeypatch.setattr(server, "dump_attachment_to_file", fake_dump)
    result = server._confirm_local_pdf_attachment_materialized(
        "ITEM1",
        ctx=ctx,
        wait_seconds=0,
        poll_interval=0,
    )
    assert result["success"] is False
    assert "materialized" in result["message"]
    assert calls["dump"] == 0


def test_identifier_skips_duplicate_pdf(monkeypatch, patch_web_client, ctx):
    patch_web_client._children["NEWKEY1"] = [
        {
            "data": {
                "itemType": "attachment",
                "contentType": "application/pdf",
                "filename": "already.pdf",
            }
        }
    ]
    monkeypatch.setenv("UNPAYWALL_EMAIL", "tester@example.com")

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if "crossref" in url:
            return FakeRequestsResponse(CROSSREF_RESPONSE)
        if "unpaywall" in url:
            return FakeRequestsResponse(
                {"best_oa_location": {"url_for_pdf": "https://example.com/paper.pdf"}}
            )
        return FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        )

    monkeypatch.setattr(server.requests, "get", fake_get)
    result = server.add_items_by_doi(dois=["10.1038/test"], ctx=ctx)
    assert "pdf_source=existing_attachment" in result
    assert not patch_web_client.attached_files


def test_identifier_falls_back_to_local_zotero_on_quota(monkeypatch, patch_web_client, ctx):
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(LANDING_WITH_PDF_ONLY),
    )

    def fake_get(url, **kwargs):
        return FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        )

    monkeypatch.setattr(server.requests, "get", fake_get)

    def quota_attachment(files, parent_key):
        raise RuntimeError("Code: 413 Response: File would exceed quota")

    local_zot = type(patch_web_client)()
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    patch_web_client.attachment_simple = quota_attachment
    result = server.add_items_by_identifier(
        identifiers=["https://example.com/landing"],
        ctx=ctx,
    )
    assert (
        "pdf_source=local_zotero" in result
        or "pdf_source=existing_attachment" in result
    )
    assert local_zot.attached_files


def test_download_pdf_bytes_retries_after_timeout(monkeypatch):
    calls = {"n": 0}

    def fake_get(url, timeout=None, stream=None, headers=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise requests_lib.ReadTimeout("timed out")
        return FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        )

    monkeypatch.setattr(server.requests, "get", fake_get)
    monkeypatch.setattr(server.time, "sleep", lambda *_: None)
    pdf_bytes, content_type = server._download_pdf_bytes("https://example.com/paper.pdf")
    assert pdf_bytes == PDF_BYTES
    assert content_type == "application/pdf"
    assert calls["n"] == 2


def test_download_pdf_bytes_falls_back_to_playwright_after_request_failures(monkeypatch, ctx):
    calls = {"n": 0}

    def fake_get(*args, **kwargs):
        calls["n"] += 1
        raise requests_lib.HTTPError("403 forbidden")

    monkeypatch.setattr(server.requests, "get", fake_get)
    monkeypatch.setattr(server.time, "sleep", lambda *_: None)
    monkeypatch.setattr(
        server,
        "_download_pdf_bytes_via_playwright",
        lambda pdf_url, *, ctx=None: (PDF_BYTES, "application/pdf"),
    )

    pdf_bytes, content_type = server._download_pdf_bytes(
        "https://example.com/protected.pdf",
        ctx=ctx,
    )

    assert pdf_bytes == PDF_BYTES
    assert content_type == "application/pdf"
    assert calls["n"] == 2


def test_identifier_prefers_direct_web_attach_before_local_connector(
    monkeypatch,
    patch_web_client,
    ctx,
):
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(LANDING_WITH_PDF_ONLY),
    )

    def fake_get(url, **kwargs):
        return FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        )

    def fail_if_connector_post(*args, **kwargs):
        raise AssertionError("connector path should not be used for small PDFs when direct web attach works")

    local_zot = type(patch_web_client)()
    monkeypatch.setenv("ZOTERO_MCP_LOCAL_PDF_MODE", "threshold")
    monkeypatch.setattr(server.requests, "get", fake_get)
    monkeypatch.setattr(server.requests, "post", fail_if_connector_post)
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)

    result = server.add_items_by_identifier(
        identifiers=["https://example.com/landing"],
        ctx=ctx,
    )

    assert "pdf_source=html:citation_pdf_url" in result
    assert patch_web_client.attached_files
    assert not local_zot.attached_files


def test_identifier_prefers_connector_url_attach_before_pdf_download(
    monkeypatch,
    patch_web_client,
    ctx,
):
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(LANDING_WITH_PDF_ONLY),
    )

    local_zot = type(patch_web_client)()
    local_zot.client = object()
    state = {"attached": False}

    def fake_get(url, **kwargs):
        raise AssertionError("PDF bytes should not be downloaded when connector URL fallback succeeds")

    class PostResponse:
        def __init__(self, status_code=201):
            self.status_code = status_code
            self.text = ""

        def raise_for_status(self):
            return None

    def fake_post(url, json=None, params=None, data=None, headers=None, timeout=None):
        if url.endswith("/connector/saveItems"):
            return PostResponse(201)
        if url.endswith("/connector/saveAttachment"):
            assert data == b""
            state["attached"] = True
            local_zot._items["LOCAL1"] = {
                "data": {
                    "key": "LOCAL1",
                    "itemType": "webpage",
                    "title": "PDF Only Landing",
                    "url": "https://example.com/final",
                    "collections": [],
                }
            }
            local_zot._children["LOCAL1"] = [
                {
                    "key": "LPDF1",
                    "data": {
                        "key": "LPDF1",
                        "itemType": "attachment",
                        "parentItem": "LOCAL1",
                        "contentType": "application/pdf",
                        "filename": "local.pdf",
                        "title": "PDF",
                    },
                }
            ]
            return PostResponse(201)
        raise AssertionError(f"unexpected POST url: {url}")

    monkeypatch.setattr(server.requests, "get", fake_get)
    monkeypatch.setattr(server.requests, "post", fake_post)
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    monkeypatch.setattr(server, "_connector_target_snapshot", lambda: {})
    monkeypatch.setattr(
        server,
        "_confirm_local_pdf_attachment_materialized",
        lambda item_key, *, ctx, wait_seconds=20.0, poll_interval=1.0: {
            "success": True,
            "attachment_key": "LPDF1",
        },
    )

    result = server.add_items_by_identifier(
        identifiers=["https://example.com/landing"],
        ctx=ctx,
    )

    assert state["attached"] is True
    assert "pdf_source=local_zotero_url_copy" in result
    assert "[local_item_key=LOCAL1]" in result


def test_attach_pdf_from_url_skips_slow_connector_url_host_after_timeout(
    monkeypatch,
    patch_web_client,
    ctx,
):
    monkeypatch.setattr(server, "CONNECTOR_URL_FASTPATH_DISABLED_HOSTS", set())

    patch_web_client._items["WEB1"] = {
        "data": {
            "key": "WEB1",
            "itemType": "journalArticle",
            "title": "Timeout DOI Paper",
            "url": "https://example.com/paper",
            "collections": [],
        }
    }

    local_zot = type(patch_web_client)()
    local_zot.client = object()
    calls = {"url_fastpath": 0, "download": 0, "copy": 0}

    class PostResponse:
        def __init__(self, status_code=201):
            self.status_code = status_code
            self.text = ""

        def raise_for_status(self):
            return None

    def fake_post(url, json=None, params=None, data=None, headers=None, timeout=None):
        if url.endswith("/connector/saveItems"):
            calls["url_fastpath"] += 1
            return PostResponse(201)
        if url.endswith("/connector/saveAttachment"):
            raise requests_lib.ReadTimeout("connector url import timed out")
        raise AssertionError(f"unexpected POST url: {url}")

    def fake_download(url, *, ctx=None):
        calls["download"] += 1
        return PDF_BYTES, "application/pdf"

    def fake_copy(*args, **kwargs):
        calls["copy"] += 1
        return {
            "success": True,
            "pdf_source": "local_zotero_copy",
            "message": "PDF saved via local Zotero connector copy",
        }

    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    monkeypatch.setattr(server.requests, "post", fake_post)
    monkeypatch.setattr(server, "_connector_target_snapshot", lambda: {})
    monkeypatch.setattr(server, "_download_pdf_bytes", fake_download)
    monkeypatch.setattr(server, "_save_pdf_via_local_connector_copy", fake_copy)
    monkeypatch.setattr(server, "_item_has_usable_pdf_attachment", lambda *args, **kwargs: False)

    first = server._attach_pdf_from_url(
        patch_web_client,
        "WEB1",
        "https://pmc.ncbi.nlm.nih.gov/articles/PMC7231423/pdf/main.pdf",
        ctx=ctx,
        source="unpaywall",
    )
    assert first["success"] is True
    assert calls["url_fastpath"] == 1
    assert calls["download"] == 1
    assert calls["copy"] == 1
    assert "pmc.ncbi.nlm.nih.gov" in server.CONNECTOR_URL_FASTPATH_DISABLED_HOSTS

    second = server._attach_pdf_from_url(
        patch_web_client,
        "WEB1",
        "https://pmc.ncbi.nlm.nih.gov/articles/PMC7231423/pdf/main.pdf",
        ctx=ctx,
        source="unpaywall",
    )
    assert second["success"] is True
    assert calls["url_fastpath"] == 1
    assert calls["download"] == 2
    assert calls["copy"] == 2


def test_attach_europepmc_fulltext_pdf_generates_surrogate_pdf(
    monkeypatch,
    patch_web_client,
    ctx,
):
    patch_web_client._items["WEB1"] = {
        "data": {
            "key": "WEB1",
            "itemType": "journalArticle",
            "title": "Cross-Subject Seizure Detection in EEGs Using Deep Transfer Learning",
            "url": "https://doi.org/10.1155/2020/7902072",
            "DOI": "10.1155/2020/7902072",
            "creators": [{"creatorType": "author", "firstName": "Baocan", "lastName": "Zhang"}],
            "date": "2020-05-08",
            "collections": [],
        }
    }

    captured = {}

    def fake_attachment_simple(files, parent_key):
        captured["parent_key"] = parent_key
        captured["pdf_bytes"] = Path(files[0]).read_bytes()

    class Response:
        def __init__(self, *, json_data=None, content=None):
            self._json = json_data
            self.content = content or b""
            self.status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return self._json

    def fake_get(url, **kwargs):
        if "europepmc/webservices/rest/search" in url:
            return Response(json_data=EUROPEPMC_SEARCH_RESPONSE)
        if url.endswith("/PMC7231423/fullTextXML"):
            return Response(content=EUROPEPMC_FULLTEXT_XML)
        raise AssertionError(f"unexpected GET url: {url}")

    monkeypatch.setattr(server.requests, "get", fake_get)
    monkeypatch.setattr(patch_web_client, "attachment_simple", fake_attachment_simple)
    monkeypatch.setattr(server, "_item_has_usable_pdf_attachment", lambda *args, **kwargs: False)
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: None)

    result = server._attach_europepmc_fulltext_pdf(
        patch_web_client,
        "10.1155/2020/7902072",
        "WEB1",
        ctx,
    )

    assert result["success"] is True
    assert result["pdf_source"] == "europepmc_fulltext_surrogate"
    assert captured["parent_key"] == "WEB1"
    assert captured["pdf_bytes"].startswith(b"%PDF-1.4")


def test_doi_attach_pdf_falls_back_to_crossref_metadata_surrogate_for_open_mdpi(
    monkeypatch,
    patch_web_client,
    ctx,
):
    monkeypatch.setenv("UNPAYWALL_EMAIL", "tester@example.com")

    def fake_get(url, headers=None, timeout=None, params=None, stream=None):
        if "crossref" in url:
            return FakeRequestsResponse(CROSSREF_MDPI_WORK)
        if "unpaywall" in url:
            return FakeRequestsResponse(
                {"best_oa_location": {"url_for_pdf": "https://www.mdpi.com/2504-446X/7/5/287/pdf?version=1682418571"}}
            )
        if "api.openalex.org/works" in url:
            return FakeRequestsResponse(
                {
                    "results": [
                        {
                            "best_oa_location": {
                                "pdf_url": "https://www.mdpi.com/2504-446X/7/5/287/pdf?version=1682418571",
                            }
                        }
                    ]
                }
            )
        if "europepmc/webservices/rest/search" in url:
            return FakeRequestsResponse({"resultList": {"result": []}})
        if "mdpi.com" in url:
            return FakeRequestsResponse({}, status_code=403, headers={"Content-Type": "text/html"}, content=HTML_BYTES)
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(server.requests, "get", fake_get)

    result = server.add_items_by_doi(dois=["10.3390/rs13030516"], ctx=ctx)

    assert "pdf_source=crossref_metadata_surrogate" in result
    assert patch_web_client.attached_files


def test_identifier_repairs_pending_local_parent_after_connector_url_attach_stalls(
    monkeypatch,
    patch_web_client,
    ctx,
):
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(LANDING_WITH_PDF_ONLY),
    )

    local_zot = type(patch_web_client)()
    local_zot.client = object()
    state = {"save_items": 0, "save_attachment": 0}

    def fake_get(url, **kwargs):
        return FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        )

    class PostResponse:
        def __init__(self, status_code=201):
            self.status_code = status_code
            self.text = ""

        def raise_for_status(self):
            return None

    def fake_post(url, json=None, params=None, data=None, headers=None, timeout=None):
        if url.endswith("/connector/saveItems"):
            state["save_items"] += 1
            local_zot._items["LOCALPENDING"] = {
                "data": {
                    "key": "LOCALPENDING",
                    "itemType": "webpage",
                    "title": "PDF Only Landing",
                    "url": "https://example.com/final",
                    "collections": [],
                }
            }
            return PostResponse(201)
        if url.endswith("/connector/saveAttachment"):
            state["save_attachment"] += 1
            assert data == b""
            return PostResponse(201)
        raise AssertionError(f"unexpected POST url: {url}")

    def fake_confirm(item_key, *, ctx, wait_seconds=20.0, poll_interval=1.0):
        if local_zot._children.get(item_key):
            return {"success": True, "attachment_key": "LPDF1"}
        return {"success": False, "message": "not materialized yet"}

    monkeypatch.setattr(server.requests, "get", fake_get)
    monkeypatch.setattr(server.requests, "post", fake_post)
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    monkeypatch.setattr(server, "_connector_target_snapshot", lambda: {})
    monkeypatch.setattr(server, "_confirm_local_pdf_attachment_materialized", fake_confirm)

    result = server.add_items_by_identifier(
        identifiers=["https://example.com/landing"],
        ctx=ctx,
    )

    assert state["save_items"] == 1
    assert state["save_attachment"] == 1
    assert "pdf_source=local_zotero_file_attach_repair" in result
    assert "[local_item_key=LOCALPENDING]" in result
    assert local_zot._children["LOCALPENDING"]


def test_confirm_local_pdf_attachment_materialized_waits_for_late_attachment(
    monkeypatch,
    patch_web_client,
    ctx,
):
    local_zot = type(patch_web_client)()
    state = {"calls": 0}

    def fake_children(item_key):
        state["calls"] += 1
        if state["calls"] < 2:
            return []
        return [
            {
                "key": "ATT1",
                "data": {
                    "key": "ATT1",
                    "itemType": "attachment",
                    "contentType": "application/pdf",
                    "filename": "late.pdf",
                },
            }
        ]

    def fake_dump(zot, attachment_key, dest, *, ctx):
        dest.write_bytes(PDF_BYTES)

    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    monkeypatch.setattr(local_zot, "children", fake_children)
    monkeypatch.setattr(server, "dump_attachment_to_file", fake_dump)
    monkeypatch.setattr(server.time, "sleep", lambda *_: None)

    result = server._confirm_local_pdf_attachment_materialized(
        "ITEM1",
        ctx=ctx,
        wait_seconds=2.0,
        poll_interval=0.1,
    )

    assert result["success"] is True
    assert result["attachment_key"] == "ATT1"


def test_save_pdf_via_local_connector_copy_repairs_pending_local_parent_after_connector_exception(
    monkeypatch,
    patch_web_client,
    ctx,
    tmp_path,
):
    pdf_path = tmp_path / "repair.pdf"
    pdf_path.write_bytes(PDF_BYTES)

    patch_web_client._items["WEB1"] = {
        "data": {
            "key": "WEB1",
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLINT"],
        }
    }

    local_zot = type(patch_web_client)()
    local_zot.client = object()

    class PostResponse:
        def __init__(self, status_code=201):
            self.status_code = status_code
            self.text = ""

        def raise_for_status(self):
            return None

    def fake_post(url, json=None, params=None, data=None, headers=None, timeout=None):
        if url.endswith("/connector/saveItems"):
            local_zot._items["LOCALPENDING"] = {
                "data": {
                    "key": "LOCALPENDING",
                    "itemType": "webpage",
                    "title": "PDF Only Landing",
                    "url": "https://example.com/final",
                    "collections": ["COLSEL"],
                }
            }
            return PostResponse(201)
        if url.endswith("/connector/saveAttachment"):
            raise requests_lib.ReadTimeout("connector stalled")
        raise AssertionError(f"unexpected POST url: {url}")

    def fake_confirm(item_key, *, ctx, wait_seconds=20.0, poll_interval=1.0):
        if local_zot._children.get(item_key):
            return {"success": True, "attachment_key": "LPDF1"}
        return {"success": False, "message": "not materialized yet"}

    monkeypatch.setattr(server.requests, "post", fake_post)
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    monkeypatch.setattr(
        server,
        "_connector_target_snapshot",
        lambda: {"current_collection_id": "COLSEL", "current_name": "Selected"},
    )
    monkeypatch.setattr(server, "_confirm_local_pdf_attachment_materialized", fake_confirm)

    result = server._save_pdf_via_local_connector_copy(
        patch_web_client,
        "WEB1",
        pdf_path,
        pdf_url="https://example.com/final.pdf",
        ctx=ctx,
    )

    assert result["success"] is True
    assert result["pdf_source"] == "local_zotero_copy"
    assert result["local_item_key"] == "LOCALPENDING"
    assert local_zot._children["LOCALPENDING"]


def test_identifier_skips_when_local_attachment_already_exists(
    monkeypatch,
    patch_web_client,
    ctx,
):
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(LANDING_WITH_PDF_ONLY),
    )

    def fake_get(url, **kwargs):
        return FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        )

    monkeypatch.setattr(server.requests, "get", fake_get)

    local_zot = type(patch_web_client)()
    local_zot._children["NEWKEY1"] = [
        {
            "key": "ATTACH1",
            "data": {
                "key": "ATTACH1",
                "itemType": "attachment",
                "parentItem": "NEWKEY1",
                "contentType": "application/pdf",
                "filename": "local.pdf",
                "title": "PDF",
            },
        }
    ]

    def fake_dump_attachment(zot, attachment_key, dest, *, ctx):
        dest.write_bytes(PDF_BYTES)

    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    monkeypatch.setattr(server, "dump_attachment_to_file", fake_dump_attachment)

    result = server.add_items_by_identifier(
        identifiers=["https://example.com/landing"],
        ctx=ctx,
    )

    assert "pdf_source=existing_attachment" in result
    assert "PDF already attached" in result


def test_identifier_reuses_existing_local_copy_before_creating_another_one(
    monkeypatch,
    patch_web_client,
    ctx,
):
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(LANDING_WITH_PDF_ONLY),
    )

    def fake_get(url, **kwargs):
        return FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        )

    def quota_attachment(files, parent_key):
        raise RuntimeError("Code: 413 Response: File would exceed quota")

    def fail_if_connector_post(*args, **kwargs):
        raise AssertionError("should reuse existing local copy without connector POST")

    local_zot = type(patch_web_client)()
    local_zot._items["LOCAL1"] = {
        "data": {
            "key": "LOCAL1",
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLKEYX"],
        }
    }
    local_zot._children["LOCAL1"] = [
        {
            "key": "LPDF1",
            "data": {
                "key": "LPDF1",
                "itemType": "attachment",
                "parentItem": "LOCAL1",
                "contentType": "application/pdf",
                "filename": "local.pdf",
                "title": "PDF",
            },
        }
    ]
    patch_web_client._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 2,
        "data": {
            "key": "LOCAL1",
            "version": 2,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    patch_web_client._collections["COLINT"] = {"data": {"key": "COLINT", "name": "Intended", "parentCollection": False}}
    patch_web_client._collections["COLSEL"] = {"data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}}
    patch_web_client._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 2,
        "data": {
            "key": "LOCAL1",
            "version": 2,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    patch_web_client._collections["COLINT"] = {"data": {"key": "COLINT", "name": "Intended", "parentCollection": False}}
    patch_web_client._collections["COLSEL"] = {"data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}}

    monkeypatch.setattr(server.requests, "get", fake_get)
    monkeypatch.setattr(server.requests, "post", fail_if_connector_post)
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    patch_web_client.attachment_simple = quota_attachment

    result = server.add_items_by_identifier(
        identifiers=["https://example.com/landing"],
        collection_key="COLKEYX",
        ctx=ctx,
    )

    assert "pdf_source=local_zotero_existing_copy" in result
    assert "[local_item_key=LOCAL1]" in result


def test_identifier_reuses_existing_local_copy_and_reconciles_into_intended_collection(
    monkeypatch,
    patch_web_client,
    ctx,
):
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(LANDING_WITH_PDF_ONLY),
    )

    def fake_get(url, **kwargs):
        return FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        )

    def quota_attachment(files, parent_key):
        raise RuntimeError("Code: 413 Response: File would exceed quota")

    local_zot = type(patch_web_client)()
    local_zot._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 1,
        "data": {
            "key": "LOCAL1",
            "version": 1,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["OTHERCOL"],
        },
    }
    local_zot._children["LOCAL1"] = [
        {
            "key": "LPDF1",
            "data": {
                "key": "LPDF1",
                "itemType": "attachment",
                "parentItem": "LOCAL1",
                "contentType": "application/pdf",
                "filename": "local.pdf",
                "title": "PDF",
            },
        }
    ]
    patch_web_client._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 2,
        "data": {
            "key": "LOCAL1",
            "version": 2,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    patch_web_client._collections["COLINT"] = {"data": {"key": "COLINT", "name": "Intended", "parentCollection": False}}
    patch_web_client._collections["COLSEL"] = {"data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}}
    patch_web_client._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 2,
        "data": {
            "key": "LOCAL1",
            "version": 2,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    patch_web_client._collections["COLINT"] = {"data": {"key": "COLINT", "name": "Intended", "parentCollection": False}}
    patch_web_client._collections["COLSEL"] = {"data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}}

    monkeypatch.setattr(server.requests, "get", fake_get)
    monkeypatch.setattr(server.requests, "post", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should reuse existing local copy without connector POST")))
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    patch_web_client.attachment_simple = quota_attachment

    result = server.add_items_by_identifier(
        identifiers=["https://example.com/landing"],
        collection_key="COLKEYX",
        ctx=ctx,
    )

    assert "pdf_source=local_zotero_existing_copy" in result
    assert "[local_item_key=LOCAL1]" in result
    assert any(col_key == "COLKEYX" for col_key, _ in local_zot.added_to) or any(
        col_key == "COLKEYX" for col_key, _ in patch_web_client.added_to
    )


def test_identifier_reuses_existing_local_copy_and_removes_selected_target_membership(
    monkeypatch,
    patch_web_client,
    ctx,
):
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(LANDING_WITH_PDF_ONLY),
    )

    def fake_get(url, **kwargs):
        raise AssertionError("should reuse existing local copy before any PDF download")

    local_zot = type(patch_web_client)()
    local_zot.client = object()
    local_zot._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 1,
        "data": {
            "key": "LOCAL1",
            "version": 1,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    local_zot._children["LOCAL1"] = [
        {
            "key": "LPDF1",
            "data": {
                "key": "LPDF1",
                "itemType": "attachment",
                "parentItem": "LOCAL1",
                "contentType": "application/pdf",
                "filename": "local.pdf",
                "title": "PDF",
            },
        }
    ]
    local_zot._collections["COLKEYX"] = {"data": {"key": "COLKEYX", "name": "Intended", "parentCollection": False}}
    local_zot._collections["COLSEL"] = {"data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}}
    patch_web_client._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 2,
        "data": {
            "key": "LOCAL1",
            "version": 2,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    patch_web_client._collections["COLKEYX"] = {"data": {"key": "COLKEYX", "name": "Intended", "parentCollection": False}}
    patch_web_client._collections["COLSEL"] = {"data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}}

    monkeypatch.setattr(server.requests, "get", fake_get)
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    monkeypatch.setattr(
        server,
        "_connector_target_snapshot",
        lambda: {"current_collection_id": "COLSEL", "current_name": "Selected"},
    )

    result = server.add_items_by_identifier(
        identifiers=["https://example.com/landing"],
        collection_key="COLKEYX",
        ctx=ctx,
    )

    assert "pdf_source=local_zotero_existing_copy" in result
    assert "[local_item_key=LOCAL1]" in result
    assert any(col_key == "COLSEL" for col_key, _ in local_zot.removed_from) or any(
        col_key == "COLSEL" for col_key, _ in patch_web_client.removed_from
    )


def test_arxiv_import_reuses_existing_web_copy_when_local_client_is_unavailable(
    monkeypatch,
    patch_web_client,
    ctx,
):
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(ARXIV_XML),
    )
    patch_web_client._collections["COLINT"] = {
        "data": {"key": "COLINT", "name": "Intended", "parentCollection": False}
    }
    patch_web_client._items["WEB1"] = {
        "key": "WEB1",
        "version": 3,
        "data": {
            "key": "WEB1",
            "version": 3,
            "itemType": "preprint",
            "title": "Completely Different Cached Title",
            "url": "https://arxiv.org/abs/2301.12345",
            "archiveID": "arXiv:2301.12345",
            "collections": ["OLDCOL"],
        },
    }
    patch_web_client._children["WEB1"] = [
        {
            "key": "WPDF1",
            "data": {
                "key": "WPDF1",
                "itemType": "attachment",
                "parentItem": "WEB1",
                "contentType": "application/pdf",
                "filename": "cached.pdf",
                "title": "PDF",
            },
        }
    ]

    result = server.add_items_by_identifier(
        identifiers=["2301.12345"],
        collection_key="COLINT",
        ctx=ctx,
    )

    assert "pdf_source=local_zotero_existing_copy" in result
    assert "[local_item_key=WEB1]" in result
    assert patch_web_client.created_items == []
    assert any(col_key == "COLINT" for col_key, _ in patch_web_client.added_to)


def test_find_existing_local_copy_for_import_does_not_reuse_partial_title_overlap_only(
    monkeypatch,
    patch_web_client,
):
    patch_web_client._items["WEB1"] = {
        "key": "WEB1",
        "version": 3,
        "data": {
            "key": "WEB1",
            "version": 3,
            "itemType": "journalArticle",
            "title": "RS-LLaVA: A Large Vision-Language Model for Joint Captioning and Question Answering in Remote Sensing Imagery",
            "url": "https://doi.org/10.3390/rs16091477",
            "DOI": "10.3390/rs16091477",
            "collections": ["COLINT"],
        },
    }
    patch_web_client._children["WEB1"] = [
        {
            "key": "WPDF1",
            "data": {
                "key": "WPDF1",
                "itemType": "attachment",
                "parentItem": "WEB1",
                "contentType": "application/pdf",
                "filename": "cached.pdf",
                "title": "PDF",
            },
        }
    ]

    monkeypatch.setattr(server, "get_local_zotero_client", lambda: None)

    result = server._find_existing_local_copy_for_import(
        doi=None,
        title="RemoteCLIP: A Vision Language Foundation Model for Remote Sensing",
        url="https://arxiv.org/abs/2306.11029",
        arxiv_id="2306.11029",
        collection_key=None,
    )

    assert result is None


def test_find_existing_local_copy_for_import_uses_exact_query_search_when_recent_scan_misses(
    monkeypatch,
    patch_web_client,
):
    patch_web_client._items["WEB1"] = {
        "key": "WEB1",
        "version": 3,
        "data": {
            "key": "WEB1",
            "version": 3,
            "itemType": "journalArticle",
            "title": "Test Paper Title",
            "url": "https://doi.org/10.1038/test",
            "DOI": "10.1038/test",
            "collections": ["COLINT"],
        },
    }
    patch_web_client._children["WEB1"] = [
        {
            "key": "WPDF1",
            "data": {
                "key": "WPDF1",
                "itemType": "attachment",
                "parentItem": "WEB1",
                "contentType": "application/pdf",
                "filename": "cached.pdf",
                "title": "PDF",
            },
        }
    ]

    def fake_items(limit=None, sort=None, direction=None, **kwargs):
        query = kwargs.get("q")
        if query in {"10.1038/test", "Test Paper Title", "https://doi.org/10.1038/test"}:
            return [patch_web_client._items["WEB1"]]
        return []

    patch_web_client.items = fake_items
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: None)

    result = server._find_existing_local_copy_for_import(
        doi="10.1038/test",
        title="Test Paper Title",
        url="https://doi.org/10.1038/test",
        arxiv_id=None,
        collection_key=None,
    )

    assert result is not None
    assert result["key"] == "WEB1"


def test_reconcile_local_copies_uses_ledger_hint_and_removes_selected_collection(
    monkeypatch,
    patch_web_client,
    ctx,
    tmp_path,
):
    ledger_path = tmp_path / "import-ledger.jsonl"
    monkeypatch.setenv("ZOTERO_MCP_IMPORT_LEDGER_PATH", str(ledger_path))

    patch_web_client._items["WEB1"] = {
        "data": {
            "key": "WEB1",
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLINT"],
        }
    }

    local_zot = type(patch_web_client)()
    local_zot._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 1,
        "data": {
            "key": "LOCAL1",
            "version": 1,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    local_zot._collections["COLINT"] = {"data": {"key": "COLINT", "name": "Intended", "parentCollection": False}}
    local_zot._collections["COLSEL"] = {"data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}}
    local_zot._children["LOCAL1"] = [
        {
            "key": "LPDF1",
            "data": {
                "key": "LPDF1",
                "itemType": "attachment",
                "parentItem": "LOCAL1",
                "contentType": "application/pdf",
                "filename": "local.pdf",
                "title": "PDF",
            },
        }
    ]
    patch_web_client._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 2,
        "data": {
            "key": "LOCAL1",
            "version": 2,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    patch_web_client._collections["COLINT"] = {
        "data": {"key": "COLINT", "name": "Intended", "parentCollection": False}
    }
    patch_web_client._collections["COLSEL"] = {
        "data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}
    }

    ledger_path.write_text(
        '{"timestamp":"2026-03-19T12:00:00","action":"import","status":"success","item_key":"WEB1","local_item_key":"LOCAL1","actual_selected_collection_id":"COLSEL","actual_selected_target":"Selected"}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)

    result = server.reconcile_local_copies(
        item_keys=["WEB1"],
        remove_from_selected_target=True,
        ctx=ctx,
    )

    assert "✓ WEB1" in result
    assert any(col_key == "COLINT" for col_key, _ in local_zot.added_to) or any(
        col_key == "COLINT" for col_key, _ in patch_web_client.added_to
    )
    assert any(col_key == "COLSEL" for col_key, _ in local_zot.removed_from) or any(
        col_key == "COLSEL" for col_key, _ in patch_web_client.removed_from
    )
    final_collections = (
        patch_web_client._items.get("LOCAL1", {}).get("data", {}).get("collections")
        or local_zot._items["LOCAL1"]["data"]["collections"]
    )
    assert final_collections == ["COLINT"]


def test_reconcile_local_copies_normalizes_connector_collection_ids(
    monkeypatch,
    patch_web_client,
    ctx,
    tmp_path,
):
    ledger_path = tmp_path / "import-ledger.jsonl"
    monkeypatch.setenv("ZOTERO_MCP_IMPORT_LEDGER_PATH", str(ledger_path))

    patch_web_client._items["WEB1"] = {
        "data": {
            "key": "WEB1",
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLINT"],
        }
    }

    local_zot = type(patch_web_client)()
    local_zot._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 1,
        "data": {
            "key": "LOCAL1",
            "version": 1,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    local_zot._children["LOCAL1"] = [
        {
            "key": "LPDF1",
            "data": {
                "key": "LPDF1",
                "itemType": "attachment",
                "parentItem": "LOCAL1",
                "contentType": "application/pdf",
                "filename": "local.pdf",
                "title": "PDF",
            },
        }
    ]
    patch_web_client._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 2,
        "data": {
            "key": "LOCAL1",
            "version": 2,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    patch_web_client._collections["COLINT"] = {
        "data": {"key": "COLINT", "name": "Intended", "parentCollection": False}
    }
    patch_web_client._collections["COLSEL"] = {
        "data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}
    }

    ledger_path.write_text(
        '{"timestamp":"2026-03-20T00:00:00","action":"import","status":"success","item_key":"WEB1","local_item_key":"LOCAL1","actual_selected_collection_id":"56","actual_selected_target":"Research-EEGDecoding-2026-03 / Core Papers"}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    monkeypatch.setattr(
        server,
        "_resolve_connector_collection_key",
        lambda value: "COLSEL" if str(value) in {"56", "C56", "COLSEL"} else value,
    )

    result = server.reconcile_local_copies(
        item_keys=["WEB1"],
        remove_from_selected_target=True,
        ctx=ctx,
    )

    assert "✓ WEB1" in result
    assert any(col_key == "COLSEL" for col_key, _ in local_zot.removed_from) or any(
        col_key == "COLSEL" for col_key, _ in patch_web_client.removed_from
    )


def test_reconcile_local_copies_falls_back_to_live_selected_collection_when_ledger_hint_is_missing(
    monkeypatch,
    patch_web_client,
    ctx,
    tmp_path,
):
    ledger_path = tmp_path / "import-ledger.jsonl"
    monkeypatch.setenv("ZOTERO_MCP_IMPORT_LEDGER_PATH", str(ledger_path))

    patch_web_client._items["WEB1"] = {
        "data": {
            "key": "WEB1",
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLINT"],
        }
    }

    local_zot = type(patch_web_client)()
    local_zot._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 1,
        "data": {
            "key": "LOCAL1",
            "version": 1,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    local_zot._collections["COLINT"] = {"data": {"key": "COLINT", "name": "Intended", "parentCollection": False}}
    local_zot._collections["COLSEL"] = {"data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}}
    local_zot._children["LOCAL1"] = [
        {
            "key": "LPDF1",
            "data": {
                "key": "LPDF1",
                "itemType": "attachment",
                "parentItem": "LOCAL1",
                "contentType": "application/pdf",
                "filename": "local.pdf",
                "title": "PDF",
            },
        }
    ]
    patch_web_client._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 2,
        "data": {
            "key": "LOCAL1",
            "version": 2,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    patch_web_client._collections["COLINT"] = {
        "data": {"key": "COLINT", "name": "Intended", "parentCollection": False}
    }
    patch_web_client._collections["COLSEL"] = {
        "data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}
    }

    ledger_path.write_text(
        '{"timestamp":"2026-03-23T00:00:00","action":"import","status":"success","item_key":"WEB1","local_item_key":"LOCAL1"}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    monkeypatch.setattr(
        server,
        "_connector_target_snapshot",
        lambda: {"current_collection_id": "COLSEL", "current_name": "Selected"},
    )

    result = server.reconcile_local_copies(
        item_keys=["WEB1"],
        remove_from_selected_target=True,
        ctx=ctx,
    )

    assert "✓ WEB1" in result
    assert any(col_key == "COLSEL" for col_key, _ in local_zot.removed_from) or any(
        col_key == "COLSEL" for col_key, _ in patch_web_client.removed_from
    )


def test_reconcile_local_item_to_collection_falls_back_to_web_when_local_patch_is_unsupported(
    monkeypatch,
    patch_web_client,
    ctx,
):
    local_zot = type(patch_web_client)()
    local_zot._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 1,
        "data": {
            "key": "LOCAL1",
            "version": 1,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    local_zot._collections["COLINT"] = {"data": {"key": "COLINT", "name": "Intended", "parentCollection": False}}
    local_zot._collections["COLSEL"] = {"data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}}

    def fail_local_patch(*args, **kwargs):
        raise RuntimeError("Code: 501 Response: Method not implemented")

    local_zot.addto_collection = fail_local_patch
    local_zot.deletefrom_collection = fail_local_patch

    patch_web_client._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 2,
        "data": {
            "key": "LOCAL1",
            "version": 2,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    patch_web_client._collections["COLINT"] = {"data": {"key": "COLINT", "name": "Intended", "parentCollection": False}}
    patch_web_client._collections["COLSEL"] = {"data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}}

    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)

    result = server._reconcile_local_item_to_collection(
        local_zot,
        "LOCAL1",
        intended_collection_key="COLINT",
        selected_collection_key="COLSEL",
        remove_from_selected_target=True,
        ctx=ctx,
    )

    assert result["success"] is True
    assert result["status"] == "reconciled"


def test_reconcile_local_item_to_collection_falls_back_to_web_when_local_patch_does_not_materialize(
    monkeypatch,
    patch_web_client,
    ctx,
):
    local_zot = type(patch_web_client)()
    local_zot._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 1,
        "data": {
            "key": "LOCAL1",
            "version": 1,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    local_zot._collections["COLINT"] = {"data": {"key": "COLINT", "name": "Intended", "parentCollection": False}}
    local_zot._collections["COLSEL"] = {"data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}}

    def noop_local_patch(*args, **kwargs):
        return FakeRequestsResponse(status_code=204)

    local_zot.addto_collection = noop_local_patch
    local_zot.deletefrom_collection = noop_local_patch

    patch_web_client._items["LOCAL1"] = {
        "key": "LOCAL1",
        "version": 2,
        "data": {
            "key": "LOCAL1",
            "version": 2,
            "itemType": "webpage",
            "title": "PDF Only Landing",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    patch_web_client._collections["COLINT"] = {"data": {"key": "COLINT", "name": "Intended", "parentCollection": False}}
    patch_web_client._collections["COLSEL"] = {"data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}}

    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)

    original_get_item_payload = server._get_item_payload
    state = {"web_calls": 0}

    def fake_get_item_payload(zot, key):
        if zot is patch_web_client and key == "LOCAL1":
            state["web_calls"] += 1
            if state["web_calls"] == 1:
                return None
        return original_get_item_payload(zot, key)

    monkeypatch.setattr(server, "_get_item_payload", fake_get_item_payload)

    result = server._reconcile_local_item_to_collection(
        local_zot,
        "LOCAL1",
        intended_collection_key="COLINT",
        selected_collection_key="COLSEL",
        remove_from_selected_target=True,
        ctx=ctx,
    )

    assert result["success"] is True
    assert result["status"] == "reconciled"
    assert patch_web_client._items["LOCAL1"]["data"]["collections"] == ["COLINT"]


def test_attach_pdf_from_url_recovers_materialized_local_copy_with_repair_provenance(
    monkeypatch,
    patch_web_client,
    ctx,
):
    patch_web_client._items["WEB1"] = {
        "data": {
            "key": "WEB1",
            "itemType": "webpage",
            "title": "Recovered Paper",
            "url": "https://example.com/final",
            "collections": ["COLINT"],
        }
    }

    local_zot = type(patch_web_client)()
    local_zot._items["LOCALREC"] = {
        "key": "LOCALREC",
        "version": 1,
        "data": {
            "key": "LOCALREC",
            "version": 1,
            "itemType": "webpage",
            "title": "Recovered Paper",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    local_zot._children["LOCALREC"] = [
        {
            "key": "LPDF1",
            "data": {
                "key": "LPDF1",
                "itemType": "attachment",
                "parentItem": "LOCALREC",
                "contentType": "application/pdf",
                "filename": "recovered.pdf",
                "title": "PDF",
            },
        }
    ]
    local_zot._collections["COLINT"] = {"data": {"key": "COLINT", "name": "Intended", "parentCollection": False}}
    local_zot._collections["COLSEL"] = {"data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}}
    patch_web_client._items["LOCALREC"] = {
        "key": "LOCALREC",
        "version": 2,
        "data": {
            "key": "LOCALREC",
            "version": 2,
            "itemType": "webpage",
            "title": "Recovered Paper",
            "url": "https://example.com/final",
            "collections": ["COLSEL"],
        },
    }
    patch_web_client._collections["COLINT"] = {"data": {"key": "COLINT", "name": "Intended", "parentCollection": False}}
    patch_web_client._collections["COLSEL"] = {"data": {"key": "COLSEL", "name": "Selected", "parentCollection": False}}

    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    monkeypatch.setattr(
        server,
        "_connector_target_snapshot",
        lambda: {"current_collection_id": "COLSEL", "current_name": "Selected"},
    )
    monkeypatch.setattr(server, "_save_pdf_via_local_connector_url", lambda *a, **k: {"success": False, "message": "url attach timed out"})
    monkeypatch.setattr(server, "_download_pdf_bytes", lambda url, *, ctx=None: (PDF_BYTES, "application/pdf"))
    monkeypatch.setattr(server, "_save_pdf_via_local_connector_copy", lambda *a, **k: {"success": False, "message": "copy path failed"})
    monkeypatch.setattr(server, "_attach_pdf_via_local_zotero", lambda *a, **k: {"success": False, "message": "local attach pending"})

    def fake_wait_for_local_item_by_metadata(**kwargs):
        if kwargs.get("require_pdf"):
            return local_zot._items["LOCALREC"]["data"]
        return None

    monkeypatch.setattr(server, "_wait_for_local_item_by_metadata", fake_wait_for_local_item_by_metadata)

    result = server._attach_pdf_from_url(
        patch_web_client,
        "WEB1",
        "https://example.com/fallback.pdf",
        ctx=ctx,
        source="html:citation_pdf_url",
    )

    assert result["success"] is True
    assert result["pdf_source"] == "local_zotero_file_attach_repair"
    assert result["local_item_key"] == "LOCALREC"
    assert any(col_key == "COLSEL" for col_key, _ in local_zot.removed_from) or any(
        col_key == "COLSEL" for col_key, _ in patch_web_client.removed_from
    )
    assert any(col_key == "COLINT" for col_key, _ in local_zot.added_to) or any(
        col_key == "COLINT" for col_key, _ in patch_web_client.added_to
    )


def test_attach_pdf_from_url_uses_context_warning_api_when_warn_is_missing(
    monkeypatch,
    patch_web_client,
):
    class WarningOnlyContext:
        def __init__(self):
            self.messages = []

        def info(self, *_args, **_kwargs):
            pass

        def error(self, *_args, **_kwargs):
            pass

        def warning(self, message, *_args, **_kwargs):
            self.messages.append(message)

    ctx = WarningOnlyContext()
    patch_web_client._items["WEB1"] = {
        "data": {
            "key": "WEB1",
            "itemType": "preprint",
            "title": "Recovered Paper",
            "url": "https://arxiv.org/abs/2511.15967",
            "collections": [],
        }
    }

    monkeypatch.setattr(server, "_item_has_usable_pdf_attachment", lambda *a, **k: False)
    monkeypatch.setattr(server, "_get_item_payload", lambda *a, **k: None)
    monkeypatch.setattr(server, "_pdf_filename_for_item", lambda *a, **k: "Recovered Paper.pdf")
    monkeypatch.setattr(server, "_should_prefer_local_pdf_after_download", lambda *a, **k: False)
    monkeypatch.setattr(server, "_download_pdf_bytes", lambda url, *, ctx=None: (PDF_BYTES, "application/pdf"))

    result = server._attach_pdf_from_url(
        patch_web_client,
        "WEB1",
        "https://arxiv.org/pdf/2511.15967.pdf",
        ctx=ctx,
        source="arxiv_pdf",
    )

    assert result["success"] is True
    assert result["pdf_source"] == "arxiv_pdf"
    assert patch_web_client.attached_files
    assert any("not readable via current client" in message for message in ctx.messages)


def test_public_import_api_is_minimal():
    tool_names = {
        tool.name
        for tool in asyncio.run(server.mcp.list_tools())
    }

    assert "zotero_add_items_by_identifier" in tool_names
    assert "zotero_add_items_by_doi" in tool_names
    assert "zotero_add_items_by_arxiv" in tool_names
    assert "zotero_add_item_by_url" in tool_names
    assert "zotero_find_and_attach_pdfs" in tool_names
    assert "zotero_reconcile_collection_duplicates" in tool_names

    assert "zotero_get_import_ledger" not in tool_names
    assert "zotero_reconcile_local_copies" not in tool_names


def test_reconcile_collection_duplicates_merges_membership_and_trashes_duplicates(
    monkeypatch,
    patch_web_client,
    ctx,
):
    patch_web_client._collections["ROOTCOL"] = {
        "data": {"key": "ROOTCOL", "name": "Root", "parentCollection": False}
    }
    patch_web_client._collections["SUBCOL"] = {
        "data": {"key": "SUBCOL", "name": "Sub", "parentCollection": "ROOTCOL"}
    }
    patch_web_client._items["CANON1"] = {
        "key": "CANON1",
        "version": 3,
        "data": {
            "key": "CANON1",
            "version": 3,
            "itemType": "preprint",
            "title": "EEG Foundation Models",
            "url": "https://arxiv.org/abs/2507.11783",
            "archiveID": "arXiv:2507.11783",
            "collections": ["SUBCOL"],
        },
    }
    patch_web_client._children["CANON1"] = [
        {
            "key": "PDF1",
            "data": {
                "key": "PDF1",
                "itemType": "attachment",
                "parentItem": "CANON1",
                "contentType": "application/pdf",
                "filename": "paper.pdf",
                "title": "PDF",
            },
        }
    ]
    patch_web_client._items["DUP1"] = {
        "key": "DUP1",
        "version": 2,
        "data": {
            "key": "DUP1",
            "version": 2,
            "itemType": "preprint",
            "title": "EEG Foundation Models",
            "url": "https://arxiv.org/abs/2507.11783",
            "archiveID": "arXiv:2507.11783",
            "collections": ["ROOTCOL"],
        },
    }

    result = server.reconcile_collection_duplicates(
        collection_key="ROOTCOL",
        include_subcollections=True,
        dry_run=False,
        ctx=ctx,
    )

    assert "duplicate groups: 1" in result
    assert "trash 1 duplicate(s)" in result
    assert "DUP1" not in patch_web_client._items
    assert "ROOTCOL" in patch_web_client._items["CANON1"]["data"]["collections"]
    assert any(
        item.get("key") == "DUP1" or item.get("data", {}).get("key") == "DUP1"
        for item in patch_web_client.deleted_items
    )


def test_reconcile_collection_duplicates_dry_run_does_not_delete(
    monkeypatch,
    patch_web_client,
    ctx,
):
    patch_web_client._collections["ROOTCOL"] = {
        "data": {"key": "ROOTCOL", "name": "Root", "parentCollection": False}
    }
    patch_web_client._items["A1"] = {
        "key": "A1",
        "version": 1,
        "data": {
            "key": "A1",
            "version": 1,
            "itemType": "conferencePaper",
            "title": "Same DOI",
            "DOI": "10.1000/test",
            "collections": ["ROOTCOL"],
        },
    }
    patch_web_client._items["A2"] = {
        "key": "A2",
        "version": 1,
        "data": {
            "key": "A2",
            "version": 1,
            "itemType": "conferencePaper",
            "title": "Same DOI",
            "DOI": "10.1000/test",
            "collections": ["ROOTCOL"],
        },
    }

    result = server.reconcile_collection_duplicates(
        collection_key="ROOTCOL",
        dry_run=True,
        ctx=ctx,
    )

    assert "dry_run: yes" in result
    assert "duplicate groups: 1" in result
    assert "A1" in patch_web_client._items
    assert "A2" in patch_web_client._items


def test_reconcile_collection_duplicates_reports_local_only_duplicates_in_dry_run(
    monkeypatch,
    patch_web_client,
    ctx,
):
    patch_web_client._collections["ROOTCOL"] = {
        "data": {"key": "ROOTCOL", "name": "Root", "parentCollection": False}
    }
    patch_web_client._items["CANON1"] = {
        "key": "CANON1",
        "version": 1,
        "data": {
            "key": "CANON1",
            "version": 1,
            "itemType": "conferencePaper",
            "title": "Local residual paper",
            "DOI": "10.2000/local",
            "collections": ["ROOTCOL"],
        },
    }
    local_zot = type(patch_web_client)()
    local_zot._collections["ROOTCOL"] = {
        "data": {"key": "ROOTCOL", "name": "Root", "parentCollection": False}
    }
    local_zot._items["CANON1"] = patch_web_client._items["CANON1"]
    local_zot._items["DUPLOCAL"] = {
        "key": "DUPLOCAL",
        "version": 1,
        "data": {
            "key": "DUPLOCAL",
            "version": 1,
            "itemType": "conferencePaper",
            "title": "Local residual paper",
            "DOI": "10.2000/local",
            "collections": ["ROOTCOL"],
        },
    }
    local_zot._children["CANON1"] = [
        {
            "key": "PDF1",
            "data": {
                "key": "PDF1",
                "itemType": "attachment",
                "parentItem": "CANON1",
                "contentType": "application/pdf",
                "filename": "paper.pdf",
                "title": "PDF",
            },
        }
    ]
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)

    result = server.reconcile_collection_duplicates(
        collection_key="ROOTCOL",
        dry_run=True,
        ctx=ctx,
    )

    assert "Local dedupe summary" in result
    assert "DUPLOCAL" in result


def test_reconcile_collection_duplicates_runs_missing_pdf_postpass_after_dedupe(
    monkeypatch,
    patch_web_client,
    ctx,
):
    patch_web_client._collections["ROOTCOL"] = {
        "data": {"key": "ROOTCOL", "name": "Root", "parentCollection": False}
    }
    patch_web_client._items["ITEM1"] = {
        "key": "ITEM1",
        "version": 1,
        "data": {
            "key": "ITEM1",
            "version": 1,
            "itemType": "conferencePaper",
            "title": "Needs PDF",
            "DOI": "10.21437/test",
            "url": "https://doi.org/10.21437/test",
            "collections": ["ROOTCOL"],
        },
    }

    attach_calls = []

    monkeypatch.setattr(
        server,
        "_fetch_crossref_work",
        lambda doi: {"DOI": doi, "URL": "https://doi.org/10.21437/test"},
    )
    monkeypatch.setattr(
        server,
        "_discover_pdf_candidates_from_crossref_work",
        lambda work, *, doi, ctx: [
            {"source": "url_pattern:same_stem_pdf", "url": "https://example.com/paper.pdf"}
        ],
    )
    monkeypatch.setattr(server, "_fetch_page_signals", lambda url, ctx: {"pdf_candidates": []})

    def fake_attach_pdf_with_cascade(
        zot,
        item_key,
        *,
        pdf_candidates,
        doi,
        crossref_work=None,
        collection_key=None,
        ctx,
    ):
        attach_calls.append((item_key, doi, pdf_candidates))
        return {
            "success": True,
            "pdf_source": "url_pattern:same_stem_pdf",
            "message": "PDF attached",
        }

    monkeypatch.setattr(server, "_attach_pdf_with_cascade", fake_attach_pdf_with_cascade)

    result = server.reconcile_collection_duplicates(
        collection_key="ROOTCOL",
        dry_run=False,
        reconcile_local_only=False,
        ctx=ctx,
    )

    assert "Missing PDF postpass" in result
    assert "scanned_without_pdf: 1" in result
    assert "repaired: 1" in result
    assert attach_calls
    assert attach_calls[0][0] == "ITEM1"
    assert attach_calls[0][1] == "10.21437/test"


def test_reconcile_collection_duplicates_uses_local_db_fallback_for_local_only_duplicates(
    monkeypatch,
    patch_web_client,
    ctx,
):
    patch_web_client._collections["ROOTCOL"] = {
        "data": {"key": "ROOTCOL", "name": "Root", "parentCollection": False}
    }
    patch_web_client._items["CANON1"] = {
        "key": "CANON1",
        "version": 1,
        "data": {
            "key": "CANON1",
            "version": 1,
            "itemType": "conferencePaper",
            "title": "Local residual paper",
            "DOI": "10.2000/local",
            "collections": ["ROOTCOL"],
        },
    }
    local_zot = type(patch_web_client)()
    local_zot._collections["ROOTCOL"] = {
        "data": {"key": "ROOTCOL", "name": "Root", "parentCollection": False}
    }
    local_zot._items["CANON1"] = patch_web_client._items["CANON1"]
    local_zot._items["DUPLOCAL"] = {
        "key": "DUPLOCAL",
        "version": 1,
        "data": {
            "key": "DUPLOCAL",
            "version": 1,
            "itemType": "conferencePaper",
            "title": "Local residual paper",
            "DOI": "10.2000/local",
            "collections": ["ROOTCOL"],
        },
    }
    local_zot._children["CANON1"] = [
        {
            "key": "PDF1",
            "data": {
                "key": "PDF1",
                "itemType": "attachment",
                "parentItem": "CANON1",
                "contentType": "application/pdf",
                "filename": "paper.pdf",
                "title": "PDF",
            },
        }
    ]

    def fail_delete(item):
        raise RuntimeError("501 Not Implemented")

    local_zot.delete_item = fail_delete
    local_zot.trash = lambda key: (_ for _ in ()).throw(RuntimeError("trash unsupported"))
    fallback_calls = []

    def fake_mark_deleted(item_keys, *, restart_zotero, ctx):
        fallback_calls.append((tuple(item_keys), restart_zotero))
        if "DUPLOCAL" in local_zot._items:
            del local_zot._items["DUPLOCAL"]
        return {"success": True, "deleted": list(item_keys), "backup_dir": "/tmp/fake"}

    monkeypatch.setattr(server, "get_local_zotero_client", lambda: local_zot)
    monkeypatch.setattr(server, "_mark_local_items_deleted_via_db", fake_mark_deleted)

    result = server.reconcile_collection_duplicates(
        collection_key="ROOTCOL",
        dry_run=False,
        local_db_fallback=True,
        ctx=ctx,
    )

    assert "Local dedupe summary" in result
    assert fallback_calls == [(("DUPLOCAL",), True)]
    assert "DUPLOCAL" not in local_zot._items


def test_get_import_ledger_reads_recent_entries(
    monkeypatch,
    patch_web_client,
    ctx,
    tmp_path,
):
    ledger_path = tmp_path / "import-ledger.jsonl"
    monkeypatch.setenv("ZOTERO_MCP_IMPORT_LEDGER_PATH", str(ledger_path))
    patch_web_client._collections["COL1"] = {
        "data": {"key": "COL1", "name": "COL1", "parentCollection": False}
    }
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=None: FakeURLResponse(LANDING_WITH_PDF_ONLY),
    )
    monkeypatch.setattr(
        server.requests,
        "get",
        lambda url, **kwargs: FakeRequestsResponse(
            headers={"Content-Type": "application/pdf"},
            content=PDF_BYTES,
        ),
    )

    server.add_items_by_identifier(
        identifiers=["https://example.com/landing"],
        collection_key="COL1",
        ctx=ctx,
    )

    result = server.get_import_ledger(limit=5, ctx=ctx)
    assert "Import ledger summary" in result
    assert "| Time | Action | Status | Route | Item Key | Local Copy | PDF | Collection | Input |" in result
    assert "| webpage |" in result
    assert "| COL1 |" in result
