import asyncio
import subprocess

import pytest
import requests

import zotero_mcp.server as server
import zotero_mcp.tools.research_import as research_import


class FakeCtx:
    def info(self, *_args, **_kwargs):
        return None

    def error(self, *_args, **_kwargs):
        return None

    def warn(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None


class FakeWriteZotero:
    def __init__(self):
        self.created_items = []
        self._items = {}
        self._children = {}
        self._collections = {}
        self.added_to = []
        self.removed_from = []
        self.deleted_items = []
        self.updated_items = []

    def item_template(self, item_type):
        return {
            "itemType": item_type,
            "title": "",
            "creators": [],
            "tags": [],
            "collections": [],
            "relations": {},
            "date": "",
            "abstractNote": "",
            "url": "",
            "DOI": "",
            "extra": "",
            "publicationTitle": "",
            "volume": "",
            "issue": "",
            "pages": "",
            "repository": "",
            "archiveID": "",
            "accessDate": "",
        }

    def create_items(self, items, **_kwargs):
        successful = {}
        for idx, item in enumerate(items, start=1):
            key = f"NEWKEY{idx}"
            payload = {"key": key, "data": {"key": key, **item}}
            self._items[key] = payload
            self.created_items.append(item)
            successful[str(idx - 1)] = {"key": key}
        return {"successful": successful, "failed": {}}

    def item(self, key):
        return self._items.get(key, {"key": key, "data": {"key": key, "itemType": "journalArticle"}})

    def children(self, key, **_kwargs):
        return self._children.get(key, [])

    def items(self, **_kwargs):
        return list(self._items.values())

    def collection_items(self, key, **_kwargs):
        return [
            item for item in self._items.values()
            if key in item.get("data", {}).get("collections", [])
        ]

    def collections(self, **_kwargs):
        return list(self._collections.values())

    def addto_collection(self, collection_key, item, **_kwargs):
        self.added_to.append((collection_key, item))
        return True

    def deletefrom_collection(self, collection_key, item, **_kwargs):
        self.removed_from.append((collection_key, item))
        return True

    def delete_item(self, item, **_kwargs):
        self.deleted_items.append(item)
        return True

    def update_item(self, item, **_kwargs):
        self.updated_items.append(item)
        return True



def _ctx():
    return FakeCtx()


def _fake_created(*, label="Test Paper", route="doi", key="NEWKEY1", pdf_source="downloaded_pdf"):
    return {
        "success": True,
        "label": label,
        "key": key,
        "route": route,
        "pdf_source": pdf_source,
        "fallback_reason": "none",
        "pdf_message": "PDF attached",
        "local_item_key": None,
        "actual_selected_collection_id": None,
        "actual_selected_target": None,
        "intended_target": None,
        "reconcile_status": None,
        "reconcile_message": None,
    }


def test_dual_api_tools_are_registered():
    async def _run():
        tools = await server.mcp.list_tools()
        names = {tool.name for tool in tools}
        assert "zotero_add_by_doi" in names
        assert "zotero_add_by_url" in names
        assert "zotero_add_from_file" in names
        assert "zotero_find_duplicates" in names
        assert "zotero_merge_duplicates" in names
        assert "zotero_add_items_by_identifier" in names
        assert "zotero_add_items_by_doi" in names
        assert "zotero_add_items_by_arxiv" in names
        assert "zotero_find_and_attach_pdfs" in names
        assert "zotero_reconcile_collection_duplicates" in names
        assert "zotero_add_item_by_url" in names

    asyncio.run(_run())


def test_identifier_import_routes_raw_doi_first(monkeypatch):
    fake_zot = FakeWriteZotero()
    monkeypatch.setattr(research_import, "get_web_zotero_client", lambda: fake_zot)
    monkeypatch.setattr(
        research_import,
        "_create_item_from_doi",
        lambda zot, doi, collection_key, attach_pdf, ctx, pdf_candidates=None: _fake_created(label=doi, route="doi"),
    )
    called = {"arxiv": False, "webpage": False}
    monkeypatch.setattr(
        research_import,
        "_create_item_from_arxiv",
        lambda *args, **kwargs: called.__setitem__("arxiv", True),
    )
    monkeypatch.setattr(
        research_import,
        "_create_webpage_item",
        lambda *args, **kwargs: called.__setitem__("webpage", True),
    )

    result = research_import.add_items_by_identifier(["10.1038/test-doi"], ctx=_ctx())
    assert "Imported as paper" in result
    assert "route=doi" not in result
    assert called == {"arxiv": False, "webpage": False}



def test_identifier_import_routes_arxiv_url(monkeypatch):
    fake_zot = FakeWriteZotero()
    monkeypatch.setattr(research_import, "get_web_zotero_client", lambda: fake_zot)
    monkeypatch.setattr(
        research_import,
        "_create_item_from_arxiv",
        lambda zot, arxiv_id, collection_key, attach_pdf, ctx: _fake_created(label=arxiv_id, route="arxiv"),
    )
    called = {"doi": False, "webpage": False}
    monkeypatch.setattr(
        research_import,
        "_create_item_from_doi",
        lambda *args, **kwargs: called.__setitem__("doi", True),
    )
    monkeypatch.setattr(
        research_import,
        "_create_webpage_item",
        lambda *args, **kwargs: called.__setitem__("webpage", True),
    )

    result = research_import.add_items_by_identifier(["https://arxiv.org/abs/2301.12345"], ctx=_ctx())
    assert "Imported as paper" in result
    assert called == {"doi": False, "webpage": False}



def test_identifier_import_landing_page_prefers_doi_metadata(monkeypatch):
    fake_zot = FakeWriteZotero()
    monkeypatch.setattr(research_import, "get_web_zotero_client", lambda: fake_zot)
    monkeypatch.setattr(
        research_import,
        "_fetch_page_signals",
        lambda url, ctx: {
            "final_url": url,
            "doi": "10.1109/TPAMI.2024.1234567",
            "title": "Publisher Paper",
            "pdf_candidates": [{"source": "citation_pdf_url", "url": "https://example.com/paper.pdf"}],
        },
    )
    monkeypatch.setattr(
        research_import,
        "_create_item_from_doi",
        lambda zot, doi, collection_key, attach_pdf, ctx, pdf_candidates=None: _fake_created(label="Publisher Paper", route="doi"),
    )

    result = research_import.add_items_by_identifier(["https://publisher.example.com/paper"], ctx=_ctx())
    assert "Imported as paper" in result
    assert "Publisher Paper" in result



def test_direct_pdf_without_identifier_respects_skip_fallback(monkeypatch):
    fake_zot = FakeWriteZotero()
    monkeypatch.setattr(research_import, "get_web_zotero_client", lambda: fake_zot)
    monkeypatch.setattr(research_import, "_probe_identifier_from_direct_pdf_url", lambda url, ctx: None)

    result = research_import.add_items_by_identifier(
        ["https://example.com/file.pdf"],
        fallback_mode="skip",
        ctx=_ctx(),
    )
    assert "no doi/arxiv identifier" in result.lower()
    assert "✗" in result



def test_create_item_from_doi_reuses_existing_local_copy_before_new_parent(monkeypatch):
    fake_zot = FakeWriteZotero()
    monkeypatch.setattr(
        research_import,
        "_fetch_crossref_work",
        lambda doi: {"title": ["Reusable Paper"], "URL": f"https://doi.org/{doi}"},
    )
    monkeypatch.setattr(
        research_import,
        "_reuse_existing_local_copy_for_import",
        lambda **kwargs: {
            **_fake_created(label="Reusable Paper", route="doi", key="LOCALKEEP", pdf_source="local_zotero_existing_copy"),
            "local_item_key": "LOCALKEEP",
        },
    )

    created = research_import._create_item_from_doi(
        fake_zot,
        "10.1038/reuse",
        collection_key="COL1",
        attach_pdf=True,
        ctx=_ctx(),
    )
    assert created["key"] == "LOCALKEEP"
    assert fake_zot.created_items == []


def test_create_item_from_doi_reuses_existing_item_without_pdf_before_new_parent(monkeypatch):
    fake_web_zot = FakeWriteZotero()
    fake_local_zot = FakeWriteZotero()
    fake_local_zot._items["LOCALNOPDF"] = {
        "key": "LOCALNOPDF",
        "data": {
            "key": "LOCALNOPDF",
            "itemType": "journalArticle",
            "title": "Reusable Without PDF",
            "DOI": "10.1038/no-pdf-reuse",
            "collections": [],
            "creators": [],
        },
    }

    monkeypatch.setattr(research_import, "get_web_zotero_client", lambda: fake_web_zot)
    monkeypatch.setattr(research_import, "get_local_zotero_client", lambda: fake_local_zot)
    monkeypatch.setattr(
        research_import,
        "_fetch_crossref_work",
        lambda doi: {"title": ["Reusable Without PDF"], "URL": f"https://doi.org/{doi}"},
    )
    monkeypatch.setattr(
        research_import,
        "_find_existing_local_copy_for_import",
        lambda **kwargs: fake_local_zot._items["LOCALNOPDF"]["data"],
    )
    monkeypatch.setattr(research_import, "_item_has_usable_pdf_attachment", lambda *args, **kwargs: False)
    monkeypatch.setattr(research_import, "_connector_target_snapshot", lambda **kwargs: {"current_collection_id": None})
    monkeypatch.setattr(
        research_import,
        "_reconcile_local_item_to_collection",
        lambda *args, **kwargs: {"success": True, "status": "not_needed", "message": "already available"},
    )

    created = research_import._create_item_from_doi(
        fake_web_zot,
        "10.1038/no-pdf-reuse",
        collection_key="COL1",
        attach_pdf=True,
        ctx=_ctx(),
    )

    assert created["key"] == "LOCALNOPDF"
    assert created["pdf_source"] == "none"
    assert fake_web_zot.created_items == []



def test_add_item_by_url_keeps_pure_webpage_semantics(monkeypatch):
    fake_zot = FakeWriteZotero()
    monkeypatch.setattr(research_import, "get_web_zotero_client", lambda: fake_zot)
    monkeypatch.setattr(
        research_import,
        "_fetch_page_signals",
        lambda url, ctx: {
            "final_url": url,
            "title": "Plain Web Page",
            "description": "Page description",
            "abstract_note": "Page abstract",
            "creators": [],
            "date": "2026-03-23",
            "doi": "10.1000/should-not-trigger-smart-import",
        },
    )

    result = research_import.add_item_by_url("https://example.com/page", ctx=_ctx())
    assert "Saved as webpage" in result
    assert fake_zot.created_items
    assert fake_zot.created_items[0]["itemType"] == "webpage"
    assert fake_zot.created_items[0]["DOI"] == "10.1000/should-not-trigger-smart-import"


def test_create_webpage_item_enriches_missing_metadata_from_crossref(monkeypatch):
    fake_zot = FakeWriteZotero()
    monkeypatch.setattr(research_import, "get_web_zotero_client", lambda: fake_zot)
    monkeypatch.setattr(research_import, "get_local_zotero_client", lambda: None)
    monkeypatch.setattr(
        research_import,
        "_fetch_crossref_work",
        lambda doi: {
            "DOI": doi,
            "title": ["Crossref Enriched Title"],
            "author": [{"given": "Ada", "family": "Lovelace"}],
            "issued": {"date-parts": [[2024, 6, 1]]},
            "abstract": "<jats:p>Crossref abstract.</jats:p>",
        },
    )

    created = research_import._create_webpage_item(
        fake_zot,
        "https://example.com/paper",
        collection_key="COL1",
        title="",
        description="",
        abstract_note="",
        creators=[],
        date_text="",
        doi="10.1000/enrich-me",
        attach_pdf=False,
        pdf_candidates=[],
        ctx=_ctx(),
        fallback_reason="missing_identifier",
    )

    item = fake_zot.created_items[0]
    assert created["route"] == "webpage"
    assert item["title"] == "Crossref Enriched Title"
    assert item["DOI"] == "10.1000/enrich-me"
    assert item["date"] == "2024"
    assert item["creators"][0]["lastName"] == "Lovelace"
    assert item["abstractNote"] == "Crossref abstract."
    assert item.get("tags") in ([], None)


def test_normalize_arxiv_id_does_not_extract_from_doi_url():
    assert research_import._normalize_arxiv_id(
        "https://doi.org/10.1016/j.neunet.2025.107816"
    ) is None


def test_download_pdf_bytes_repair_mode_fast_fails_cookie_gated_publisher(monkeypatch):
    called = {"playwright": False, "requests": 0}

    def fake_get(*args, **kwargs):
        called["requests"] += 1
        raise requests.Timeout("timed out")

    def fake_playwright(*args, **kwargs):
        called["playwright"] = True
        return None

    monkeypatch.setattr(research_import.requests, "get", fake_get)
    monkeypatch.setattr(research_import, "_download_pdf_bytes_via_playwright", fake_playwright)
    monkeypatch.setattr(research_import, "_playwright_browser_session_available", lambda: False)

    with pytest.raises(RuntimeError):
        research_import._download_pdf_bytes(
            "https://ieeexplore.ieee.org/document/11254670/.pdf",
            repair_mode=True,
        )

    assert called["requests"] == 1
    assert called["playwright"] is False


def test_attach_pdf_with_cascade_stops_after_direct_candidates_when_budget_is_exhausted(monkeypatch):
    call_flags = {
        "unpaywall": False,
        "openalex": False,
        "europepmc": False,
        "crossref_surrogate": False,
    }
    deadline_checks = iter([False, False, True])

    monkeypatch.setattr(research_import, "_item_has_usable_pdf_attachment", lambda *args, **kwargs: False)
    monkeypatch.setattr(research_import, "_deadline_exceeded", lambda deadline: next(deadline_checks, True))
    monkeypatch.setattr(
        research_import,
        "_attach_pdf_from_url",
        lambda *args, **kwargs: {"success": False, "pdf_source": "direct", "message": "direct failed"},
    )
    monkeypatch.setattr(
        research_import,
        "_attach_unpaywall_pdf",
        lambda *args, **kwargs: call_flags.__setitem__("unpaywall", True),
    )
    monkeypatch.setattr(
        research_import,
        "_attach_openalex_pdf",
        lambda *args, **kwargs: call_flags.__setitem__("openalex", True),
    )
    monkeypatch.setattr(
        research_import,
        "_attach_europepmc_fulltext_pdf",
        lambda *args, **kwargs: call_flags.__setitem__("europepmc", True),
    )
    monkeypatch.setattr(
        research_import,
        "_attach_crossref_metadata_surrogate_pdf",
        lambda *args, **kwargs: call_flags.__setitem__("crossref_surrogate", True),
    )

    result = research_import._attach_pdf_with_cascade(
        FakeWriteZotero(),
        "ITEM1",
        pdf_candidates=[{"source": "publisher", "url": "https://publisher.example.com/paper.pdf"}],
        doi="10.1000/test",
        crossref_work=None,
        collection_key=None,
        ctx=_ctx(),
        repair_mode=True,
        deadline=1.0,
    )

    assert result["success"] is False
    assert "budget exhausted" in result["message"]
    assert call_flags == {
        "unpaywall": False,
        "openalex": False,
        "europepmc": False,
        "crossref_surrogate": False,
    }


def test_find_and_attach_pdfs_threads_single_repair_deadline_through_pipeline(monkeypatch):
    fake_zot = FakeWriteZotero()
    fake_zot._items["ITEM1"] = {
        "key": "ITEM1",
        "data": {
            "key": "ITEM1",
            "itemType": "journalArticle",
            "DOI": "10.1000/test",
            "url": "https://publisher.example.com/paper",
            "collections": ["COL1"],
        },
    }
    recorded = {}

    monkeypatch.setattr(research_import, "get_web_zotero_client", lambda: fake_zot)
    monkeypatch.setattr(research_import, "_repair_pdf_budget_seconds", lambda: 17.0)

    def fake_crossref_work(doi, *, deadline=None):
        recorded["crossref_deadline"] = deadline
        return {"title": ["Paper"]}

    def fake_page_signals(url, *, ctx, repair_mode=False, deadline=None):
        recorded["page_deadline"] = deadline
        recorded["page_repair_mode"] = repair_mode
        return {"pdf_candidates": []}

    def fake_attach_pdf_with_cascade(*args, **kwargs):
        recorded["cascade_deadline"] = kwargs.get("deadline")
        return {"success": False, "pdf_source": "none", "message": "no pdf"}

    monkeypatch.setattr(research_import, "_fetch_crossref_work", fake_crossref_work)
    monkeypatch.setattr(research_import, "_fetch_page_signals", fake_page_signals)
    monkeypatch.setattr(research_import, "_attach_pdf_with_cascade", fake_attach_pdf_with_cascade)

    result = research_import.find_and_attach_pdfs(["ITEM1"], ctx=_ctx())

    assert "ITEM1" in result
    assert recorded["page_repair_mode"] is True
    assert recorded["crossref_deadline"] is not None
    assert recorded["page_deadline"] is not None
    assert recorded["cascade_deadline"] is not None
    assert recorded["crossref_deadline"] == recorded["page_deadline"] == recorded["cascade_deadline"]


def test_attach_pdf_from_url_repair_mode_prefers_connector_url_for_fast_fail_publisher(monkeypatch):
    monkeypatch.setattr(research_import, "_deadline_exceeded", lambda deadline: False)
    monkeypatch.setattr(research_import, "_item_has_usable_pdf_attachment", lambda *args, **kwargs: False)
    monkeypatch.setattr(
        research_import,
        "_get_item_payload",
        lambda *args, **kwargs: {"data": {"title": "Paper", "collections": []}},
    )
    monkeypatch.setattr(research_import, "_pdf_filename_for_item", lambda *args, **kwargs: "paper.pdf")

    calls = {"connector": 0, "download": 0}

    def fake_connector(*args, **kwargs):
        calls["connector"] += 1
        return {"success": True, "pdf_source": "local_zotero_url_copy", "message": "ok"}

    def fake_download(*args, **kwargs):
        calls["download"] += 1
        return (b"%PDF-1.4\n", "application/pdf")

    monkeypatch.setattr(research_import, "_save_pdf_via_local_connector_url", fake_connector)
    monkeypatch.setattr(research_import, "_download_pdf_bytes", fake_download)

    result = research_import._attach_pdf_from_url(
        FakeWriteZotero(),
        "ITEM1",
        "https://ieeexplore.ieee.org/document/11254670/.pdf",
        ctx=_ctx(),
        source="publisher",
        repair_mode=True,
        deadline=100.0,
    )

    assert result["success"] is True
    assert calls["connector"] == 1
    assert calls["download"] == 0


def test_attach_pdf_with_cascade_uses_parallel_oa_candidates_in_priority_order(monkeypatch):
    monkeypatch.setattr(research_import, "_item_has_usable_pdf_attachment", lambda *args, **kwargs: False)
    monkeypatch.setattr(research_import, "_deadline_exceeded", lambda deadline: False)
    monkeypatch.setattr(research_import, "_repair_budget_allows_fallback", lambda *args, **kwargs: True)
    monkeypatch.setattr(research_import.os, "environ", {"UNPAYWALL_EMAIL": "test@example.com"})

    monkeypatch.setattr(
        research_import,
        "_discover_oa_pdf_candidates_parallel",
        lambda *args, **kwargs: [
            {"source": "unpaywall", "url": "https://oa.example.com/from-unpaywall.pdf"},
            {"source": "openalex:best_oa_location", "url": "https://oa.example.com/from-openalex.pdf"},
        ],
    )
    monkeypatch.setattr(
        research_import,
        "_attach_europepmc_fulltext_pdf",
        lambda *args, **kwargs: {"success": False, "pdf_source": "europepmc", "message": "no europepmc"},
    )
    monkeypatch.setattr(
        research_import,
        "_attach_crossref_metadata_surrogate_pdf",
        lambda *args, **kwargs: {"success": False, "pdf_source": "crossref", "message": "no crossref"},
    )

    attach_order = []

    def fake_attach(zot, item_key, pdf_url, *, ctx, source, repair_mode=False, deadline=None):
        attach_order.append((source, pdf_url))
        if source == "unpaywall":
            return {"success": False, "pdf_source": source, "message": "unpaywall failed"}
        return {"success": True, "pdf_source": source, "message": "openalex attached"}

    monkeypatch.setattr(research_import, "_attach_pdf_from_url", fake_attach)

    result = research_import._attach_pdf_with_cascade(
        FakeWriteZotero(),
        "ITEM1",
        pdf_candidates=[],
        doi="10.1000/test",
        crossref_work=None,
        collection_key=None,
        ctx=_ctx(),
        repair_mode=True,
        deadline=100.0,
    )

    assert result["success"] is True
    assert attach_order == [
        ("unpaywall", "https://oa.example.com/from-unpaywall.pdf"),
        ("openalex:best_oa_location", "https://oa.example.com/from-openalex.pdf"),
    ]


def test_attach_pdf_from_url_repair_mode_uses_local_browser_fetch_before_http(monkeypatch):
    monkeypatch.setattr(research_import, "_deadline_exceeded", lambda deadline: False)
    monkeypatch.setattr(research_import, "_item_has_usable_pdf_attachment", lambda *args, **kwargs: False)
    monkeypatch.setattr(
        research_import,
        "_get_item_payload",
        lambda *args, **kwargs: {"data": {"title": "Paper", "collections": []}},
    )
    monkeypatch.setattr(research_import, "_pdf_filename_for_item", lambda *args, **kwargs: "paper.pdf")

    calls = {"connector_url": 0, "browser_fetch": 0, "http_download": 0}

    monkeypatch.setattr(
        research_import,
        "_save_pdf_via_local_connector_url",
        lambda *args, **kwargs: calls.__setitem__("connector_url", calls["connector_url"] + 1) or {
            "success": False,
            "pdf_source": "local_zotero_url",
            "message": "connector failed",
        },
    )
    monkeypatch.setattr(
        research_import,
        "_download_pdf_bytes_via_local_connector_browser_session",
        lambda *args, **kwargs: (
            calls.__setitem__("browser_fetch", calls["browser_fetch"] + 1) or (b"%PDF-1.4\n", "application/pdf")
        ),
    )
    monkeypatch.setattr(
        research_import,
        "_download_pdf_bytes",
        lambda *args, **kwargs: calls.__setitem__("http_download", calls["http_download"] + 1) or (b"%PDF-1.4\n", "application/pdf"),
    )

    fake_zot = FakeWriteZotero()
    monkeypatch.setattr(fake_zot, "attachment_simple", lambda *args, **kwargs: True, raising=False)

    result = research_import._attach_pdf_from_url(
        fake_zot,
        "ITEM1",
        "https://linkinghub.elsevier.com/retrieve/pii/S0893608025006963.pdf",
        ctx=_ctx(),
        source="publisher",
        repair_mode=True,
        deadline=100.0,
    )

    assert result["success"] is True
    assert calls == {"connector_url": 1, "browser_fetch": 1, "http_download": 0}

def test_connector_target_snapshot_self_heals_library_context(monkeypatch):
    probe_calls = {"count": 0}
    opened = {"uri": None}

    def fake_probe(*, timeout=10.0):
        probe_calls["count"] += 1
        if probe_calls["count"] <= 2:
            return None
        return {
            "libraryName": "我的文库",
            "id": None,
            "name": "我的文库",
            "targets": [{"id": "L1", "name": "我的文库", "level": 0, "filesEditable": True}],
        }

    monkeypatch.setattr(research_import, "_connector_get_selected_collection_payload", fake_probe)
    monkeypatch.setattr(research_import, "_connector_context_candidate_item_key", lambda preferred_item_key=None: "E5PDANKJ")
    monkeypatch.setattr(research_import.time, "sleep", lambda *_args, **_kwargs: None)

    def fake_run(args, check=False, stdout=None, stderr=None):
        opened["uri"] = args[-1]
        return subprocess.CompletedProcess(args=args, returncode=0)

    monkeypatch.setattr(research_import.subprocess, "run", fake_run)

    snapshot = research_import._connector_target_snapshot(preferred_item_key="E5PDANKJ", ctx=_ctx())

    assert snapshot["library_name"] == "我的文库"
    assert opened["uri"] == "zotero://select/library/items/E5PDANKJ"
