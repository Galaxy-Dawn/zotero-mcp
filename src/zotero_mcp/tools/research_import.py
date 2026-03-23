"""Research-grade import compatibility layer on top of upstream zotero-mcp.

This module ports the fork's identifier-first import flow, source-aware PDF
cascade, local-copy reconcile helpers, and collection-level dedupe / repair
entrypoints, while keeping the upstream modular tool layout intact.
"""

import json
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import textwrap
import time
import uuid
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import suppress
from datetime import datetime, timezone
from difflib import SequenceMatcher
from html import unescape
from pathlib import Path
from typing import Any, Literal
from urllib.parse import unquote, urljoin, urlparse

import httpx
import requests
from fastmcp import Context

from zotero_mcp._app import mcp
from zotero_mcp.client import (
    get_local_zotero_client,
    get_web_zotero_client,
)
from zotero_mcp.utils import clean_html


def _ctx_warning(ctx: Context, message: str) -> None:
    warning_fn = getattr(ctx, "warning", None)
    if callable(warning_fn):
        warning_fn(message)
        return

    legacy_warn_fn = getattr(ctx, "warn", None)
    if callable(legacy_warn_fn):
        legacy_warn_fn(message)
        return

    info_fn = getattr(ctx, "info", None)
    if callable(info_fn):
        info_fn(message)


def _file_url_to_local_path(file_url: str) -> Path | None:
    try:
        parsed = urlparse(file_url)
    except Exception:
        return None
    if parsed.scheme != "file":
        return None
    path = unquote(parsed.path or "")
    # Windows file URLs may be "/C:/..." - strip the leading slash.
    if re.match(r"^/[A-Za-z]:/", path):
        path = path[1:]
    if not path:
        return None
    return Path(path)


def _dump_attachment_via_local_redirect(zot, attachment_key: str, dest: Path) -> bool:
    """Fetch attachment bytes by resolving the local API redirect to a file:// path."""
    from pyzotero._utils import build_url

    file_url = build_url(
        zot.endpoint,
        f"/{zot.library_type}/{zot.library_id}/items/{attachment_key}/file",
    )

    # Do NOT follow redirects here; Zotero local API returns 302 to file://...
    resp = httpx.Client(headers=zot.default_headers(), follow_redirects=False).get(
        file_url,
    )
    if resp.status_code == 200 and resp.content:
        dest.write_bytes(resp.content)
        return True

    location = resp.headers.get("Location") or resp.headers.get("location")
    if not location:
        return False
    local_path = _file_url_to_local_path(location)
    if not local_path:
        return False
    dest.write_bytes(local_path.read_bytes())
    return True


def _dump_attachment_via_resolved_local_path(attachment_key: str, dest: Path) -> bool:
    local_path = _resolve_local_attachment_path(attachment_key)
    if local_path is None or not local_path.exists():
        return False
    dest.write_bytes(local_path.read_bytes())
    return True


def dump_attachment_to_file(zot, attachment_key: str, dest: Path, *, ctx: Context) -> None:
    """
    Dump an attachment to disk.

    Local Zotero API may redirect `/file` to a `file://...` URL. httpx won't follow
    scheme-changing redirects, so we resolve it ourselves when needed.
    """
    if _dump_attachment_via_resolved_local_path(attachment_key, dest):
        return

    try:
        zot.dump(attachment_key, filename=dest.name, path=str(dest.parent))
        return
    except Exception as dump_error:
        if "unsupported protocol 'file://'" not in str(dump_error):
            raise

    ctx.info("Attachment download hit file:// redirect; resolving via local file path")
    ok = _dump_attachment_via_local_redirect(zot, attachment_key, dest)
    if not ok and _dump_attachment_via_resolved_local_path(attachment_key, dest):
        return
    if not ok:
        raise RuntimeError("Failed to resolve file:// redirect for attachment download")




def _require_unsafe(level: str) -> str | None:
    """Return an error string if UNSAFE_OPERATIONS env var doesn't permit `level`.

    level="items"  → requires UNSAFE_OPERATIONS in {"items", "all"}
    level="all"    → requires UNSAFE_OPERATIONS == "all"
    Returns None if the operation is permitted.
    """
    allowed = os.environ.get("UNSAFE_OPERATIONS", "").lower()
    if level == "items" and allowed not in ("items", "all"):
        return (
            "Error: This operation is disabled by default. "
            "Set UNSAFE_OPERATIONS=items (or UNSAFE_OPERATIONS=all) to enable it."
        )
    if level == "all" and allowed != "all":
        return (
            "Error: This operation is disabled by default. "
            "Set UNSAFE_OPERATIONS=all to enable it."
        )
    return None


DOI_PATTERN = re.compile(r"(10\.\d{4,9}/[^\s\"'<>]+)", re.I)
ARXIV_ID_PATTERN = re.compile(r"(\d{4}\.\d{4,5}(?:v\d+)?)", re.I)
PDF_EXTENSIONS = (".pdf",)
FILENAME_SANITIZE_PATTERN = re.compile(r'[\\/:*?"<>|\n\r\t]+')
CONNECTOR_URL_FASTPATH_DISABLED_HOSTS: set[str] = set()


def _state_dir() -> Path:
    override = os.environ.get("ZOTERO_MCP_STATE_DIR")
    base = Path(override).expanduser() if override else Path.home() / ".config" / "zotero-mcp" / "state"
    base.mkdir(parents=True, exist_ok=True)
    return base


def _import_ledger_path() -> Path:
    override = os.environ.get("ZOTERO_MCP_IMPORT_LEDGER_PATH")
    if override:
        path = Path(override).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
    return _state_dir() / "import-ledger.jsonl"


def _append_import_ledger(entry: dict[str, Any], *, ctx: Context | None = None) -> None:
    path = _import_ledger_path()
    try:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as exc:
        if ctx is not None:
            _ctx_warning(ctx, f"Failed to append import ledger at {path}: {exc}")


def _read_import_ledger(limit: int | None = None) -> list[dict[str, Any]]:
    path = _import_ledger_path()
    if not path.exists():
        return []

    entries: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    if limit is not None and limit >= 0:
        return entries[-limit:]
    return entries


def _latest_import_ledger_entry(*, item_key: str | None = None, local_item_key: str | None = None) -> dict[str, Any] | None:
    if not item_key and not local_item_key:
        return None

    for entry in reversed(_read_import_ledger()):
        if item_key and entry.get("item_key") == item_key:
            return entry
        if local_item_key and entry.get("local_item_key") == local_item_key:
            return entry
    return None


def _normalize_doi(raw: str | None) -> str | None:
    if not raw:
        return None
    text = unquote(str(raw)).strip()
    text = re.sub(r"^(?:doi:\s*|https?://(?:dx\.)?doi\.org/)", "", text, flags=re.I)
    match = DOI_PATTERN.search(text)
    if not match:
        return None
    return match.group(1).rstrip(").,;]}>")


def _doi_candidates_from_raw(raw: str | None) -> list[str]:
    normalized = _normalize_doi(raw)
    if not normalized:
        return []

    candidates: list[str] = [normalized]
    seen = {normalized.lower()}
    text = unquote(str(raw or "")).strip()

    try:
        parsed = urlparse(text)
    except Exception:
        parsed = None

    if parsed and parsed.scheme and parsed.netloc:
        segments = [segment for segment in parsed.path.split("/") if segment]
        doi_start = next(
            (
                idx for idx, segment in enumerate(segments)
                if re.match(r"^10\.\d{4,9}$", segment, flags=re.I)
            ),
            None,
        )
        if doi_start is not None:
            doi_segments = segments[doi_start:]
            for end in range(len(doi_segments), 1, -1):
                candidate = "/".join(doi_segments[:end]).rstrip(").,;]}>")
                lowered = candidate.lower()
                if lowered not in seen:
                    seen.add(lowered)
                    candidates.append(candidate)
    return candidates


def _normalize_arxiv_id(raw: str | None) -> str | None:
    if not raw:
        return None
    text = unquote(str(raw)).strip()
    lowered = text.lower()
    explicit_arxiv_context = any(
        marker in lowered for marker in ("arxiv.org/", "10.48550/arxiv.", "arxiv:")
    )
    text = re.sub(r"^https?://arxiv\.org/(?:abs|pdf)/", "", text, flags=re.I)
    text = re.sub(r"^10\.48550/arxiv\.", "", text, flags=re.I)
    text = re.sub(r"^arxiv:", "", text, flags=re.I)
    text = re.sub(r"\.pdf$", "", text, flags=re.I)
    full_match = re.fullmatch(r"\d{4}\.\d{4,5}(?:v\d+)?", text, flags=re.I)
    if full_match:
        return full_match.group(0)
    legacy_match = re.fullmatch(r"[a-z\-]+/\d{7}(?:v\d+)?", text, flags=re.I)
    if legacy_match:
        return legacy_match.group(0)
    if not explicit_arxiv_context:
        return None
    match = ARXIV_ID_PATTERN.search(text)
    return match.group(1) if match else None


RETRYABLE_HTTP_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
PUBLISHER_DIRECT_PDF_FAST_FAIL_HOSTS = {
    "api.elsevier.com",
    "linkinghub.elsevier.com",
    "sciencedirect.com",
    "ieeexplore.ieee.org",
    "xplorestaging.ieee.org",
}


def _retryable_http_exception(exc: Exception) -> bool:
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(exc, requests.HTTPError):
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        return status_code in RETRYABLE_HTTP_STATUS_CODES
    return False


def _requests_get_with_retry(
    url: str,
    *,
    ctx: Context | None = None,
    timeout: float | tuple[float, float] = 15,
    stream: bool = False,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    attempts: int = 2,
    backoff_seconds: float = 0.75,
    deadline: float | None = None,
) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, max(attempts, 1) + 1):
        if _deadline_exceeded(deadline):
            raise requests.Timeout(f"deadline exceeded before GET {url}")
        try:
            effective_timeout = _clamp_timeout_to_deadline(timeout, deadline=deadline)
            response = requests.get(
                url,
                timeout=effective_timeout,
                stream=stream,
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            return response
        except Exception as exc:
            last_exc = exc
            if attempt >= max(attempts, 1) or not _retryable_http_exception(exc):
                raise
            if ctx is not None:
                _ctx_warning(
                    ctx,
                    f"GET retry {attempt}/{attempts} for {url} after transient error: {exc}",
                )
            sleep_seconds = backoff_seconds * attempt
            if deadline is not None:
                sleep_seconds = min(sleep_seconds, max(0.0, deadline - time.monotonic()))
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
    assert last_exc is not None
    raise last_exc


def _repair_pdf_budget_seconds() -> float:
    try:
        value = float(os.environ.get("ZOTERO_MCP_REPAIR_PDF_BUDGET_SEC", "45"))
    except ValueError:
        value = 45.0
    return max(10.0, min(value, 180.0))


def _deadline_exceeded(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() >= deadline


def _remaining_budget_raw_seconds(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())


def _remaining_budget_seconds(deadline: float | None, fallback: float) -> float:
    if deadline is None:
        return fallback
    remaining = deadline - time.monotonic()
    return max(0.5, min(fallback, remaining))


def _clamp_timeout_to_deadline(
    timeout: float | tuple[float, float],
    *,
    deadline: float | None,
) -> float | tuple[float, float]:
    if deadline is None:
        return timeout
    if isinstance(timeout, tuple):
        connect_timeout, read_timeout = timeout
        remaining = _remaining_budget_seconds(deadline, max(connect_timeout, read_timeout))
        return (
            max(0.5, min(connect_timeout, remaining)),
            max(0.5, min(read_timeout, remaining)),
        )
    return _remaining_budget_seconds(deadline, timeout)


def _publisher_pdf_fast_fail_host(pdf_url: str) -> str | None:
    try:
        host = (urlparse(pdf_url).netloc or "").lower()
    except Exception:
        return None
    if not host:
        return None
    for candidate in PUBLISHER_DIRECT_PDF_FAST_FAIL_HOSTS:
        if host == candidate or host.endswith(f".{candidate}"):
            return candidate
    return None


def _playwright_browser_session_available() -> bool:
    user_data_dir = os.environ.get("ZOTERO_MCP_PLAYWRIGHT_USER_DATA_DIR", "").strip()
    return bool(user_data_dir)


def _repair_budget_allows_fallback(
    deadline: float | None,
    *,
    min_remaining_seconds: float = 2.0,
) -> bool:
    remaining = _remaining_budget_raw_seconds(deadline)
    if remaining is None:
        return True
    return remaining >= min_remaining_seconds


def _looks_like_direct_pdf_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        path = urlparse(url).path.lower()
    except Exception:
        return False
    return any(path.endswith(ext) for ext in PDF_EXTENSIONS)


def _sanitize_filename_component(text: str | None, *, max_len: int) -> str:
    value = FILENAME_SANITIZE_PATTERN.sub(" ", str(text or ""))
    value = re.sub(r"\s+", " ", value).strip(" .-_")
    if not value:
        return ""
    return value[:max_len].rstrip(" .-_")


def _creator_label(item_data: dict[str, Any]) -> str:
    creators = item_data.get("creators") or []
    if not isinstance(creators, list):
        return ""

    preferred_types = {"author", "inventor", "programmer", "presenter", "artist", "editor"}
    preferred = [
        creator for creator in creators
        if isinstance(creator, dict) and (creator.get("creatorType") in preferred_types)
    ] or [creator for creator in creators if isinstance(creator, dict)]

    if not preferred:
        return ""

    first = preferred[0]
    base = (
        first.get("lastName")
        or first.get("name")
        or first.get("firstName")
        or ""
    )
    base = _sanitize_filename_component(base, max_len=40)
    if not base:
        return ""
    return f"{base} 等" if len(preferred) > 1 else base


def _item_year(item_data: dict[str, Any]) -> str:
    date_text = str(item_data.get("date") or "").strip()
    match = re.search(r"(19|20)\d{2}", date_text)
    return match.group(0) if match else ""


def _pdf_filename_for_item(item_data: dict[str, Any], *, pdf_url: str | None = None) -> str:
    title = _sanitize_filename_component(item_data.get("title"), max_len=120)
    if not title and pdf_url:
        parsed = urlparse(pdf_url)
        title = _sanitize_filename_component(Path(parsed.path).stem, max_len=120)
    if not title:
        title = "document"

    parts = []
    creator = _creator_label(item_data)
    year = _item_year(item_data)
    if creator:
        parts.append(creator)
    if year:
        parts.append(year)
    parts.append(title)

    stem = " - ".join(part for part in parts if part)
    stem = _sanitize_filename_component(stem, max_len=180) or "document"
    if stem.lower().endswith(".pdf"):
        return stem
    return f"{stem}.pdf"


def _extract_meta_content(html: str, names: list[str]) -> str | None:
    for name in names:
        escaped = re.escape(name)
        patterns = [
            rf'<meta[^>]+(?:name|property)=["\']{escaped}["\'][^>]+content=["\'](.*?)["\']',
            rf'<meta[^>]+content=["\'](.*?)["\'][^>]+(?:name|property)=["\']{escaped}["\']',
        ]
        for pattern in patterns:
            match = re.search(pattern, html, re.I | re.S)
            if match and match.group(1).strip():
                return match.group(1).strip()
    return None


def _extract_meta_contents(html: str, names: list[str]) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for name in names:
        escaped = re.escape(name)
        patterns = [
            rf'<meta[^>]+(?:name|property)=["\']{escaped}["\'][^>]+content=["\'](.*?)["\']',
            rf'<meta[^>]+content=["\'](.*?)["\'][^>]+(?:name|property)=["\']{escaped}["\']',
        ]
        for pattern in patterns:
            for match in re.finditer(pattern, html, re.I | re.S):
                value = unescape(match.group(1).strip())
                if value and value not in seen:
                    values.append(value)
                    seen.add(value)
    return values


def _normalize_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", unescape(str(value or "")).strip())


def _normalize_title_for_match(value: str | None) -> str:
    text = _normalize_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "", text)
    return text


def _normalize_url_for_match(value: str | None) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    parsed = urlparse(text)
    path = parsed.path.rstrip("/")
    return parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        path=path,
        params="",
        query="",
        fragment="",
    ).geturl()


def _tokenize_match_text(value: str | None) -> list[str]:
    return re.findall(r"[a-z0-9]+", _normalize_text(value).lower())


def _normalize_venue_for_match(value: str | None) -> str:
    text = _normalize_text(value).lower()
    return re.sub(r"[^a-z0-9]+", "", text)


def _extract_year_from_text(value: str | None) -> str:
    match = re.search(r"(19|20)\d{2}", str(value or ""))
    return match.group(0) if match else ""


def _item_arxiv_id_from_data(data: dict[str, Any] | None) -> str | None:
    if not isinstance(data, dict):
        return None
    return (
        _normalize_arxiv_id(data.get("archiveID"))
        or _normalize_arxiv_id(data.get("url"))
        or _normalize_arxiv_id(data.get("DOI"))
    )


def _title_overlap_score(left: str | None, right: str | None) -> int:
    left_tokens = set(_tokenize_match_text(left))
    right_tokens = set(_tokenize_match_text(right))
    if not left_tokens or not right_tokens:
        return 0
    overlap = len(left_tokens & right_tokens)
    shorter = min(len(left_tokens), len(right_tokens))
    if shorter == 0:
        return 0
    ratio = overlap / shorter
    if ratio >= 0.9:
        return 18
    if ratio >= 0.75:
        return 12
    if ratio >= 0.6:
        return 6
    return 0


def _parse_creator_name(name: str) -> dict[str, str]:
    normalized = _normalize_text(name)
    if not normalized:
        return {"creatorType": "author", "name": ""}
    if "," in normalized:
        last, first = [part.strip() for part in normalized.split(",", 1)]
        if first:
            return {"creatorType": "author", "firstName": first, "lastName": last}
        return {"creatorType": "author", "lastName": last}
    parts = normalized.rsplit(" ", 1)
    if len(parts) == 2:
        return {"creatorType": "author", "firstName": parts[0], "lastName": parts[1]}
    return {"creatorType": "author", "lastName": normalized}


def _extract_jsonld_blocks(html: str) -> list[str]:
    blocks = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.I | re.S,
    )
    return [block.strip() for block in blocks if block.strip()]


def _extract_doi_from_jsonld(blocks: list[str]) -> str | None:
    for block in blocks:
        try:
            payload = json.loads(block)
        except Exception:
            doi = _normalize_doi(block)
            if doi:
                return doi
            continue

        def walk(node: Any) -> str | None:
            if isinstance(node, dict):
                for key, value in node.items():
                    key_norm = str(key).lower()
                    if key_norm == "doi" and isinstance(value, str):
                        doi = _normalize_doi(value)
                        if doi:
                            return doi
                    if key_norm == "identifier":
                        if isinstance(value, str):
                            doi = _normalize_doi(value)
                            if doi:
                                return doi
                        elif isinstance(value, dict):
                            doi = _normalize_doi(value.get("value") or value.get("@value"))
                            if doi:
                                return doi
                    found = walk(value)
                    if found:
                        return found
            elif isinstance(node, list):
                for item in node:
                    found = walk(item)
                    if found:
                        return found
            elif isinstance(node, str):
                doi = _normalize_doi(node)
                if doi:
                    return doi
            return None

        found = walk(payload)
        if found:
            return found
    return None


def _extract_creators_from_jsonld(blocks: list[str]) -> list[dict[str, str]]:
    creators: list[dict[str, str]] = []
    seen: set[str] = set()
    for block in blocks:
        try:
            payload = json.loads(block)
        except Exception:
            continue

        def visit(node: Any) -> None:
            if isinstance(node, dict):
                for key, value in node.items():
                    if str(key).lower() == "author":
                        authors = value if isinstance(value, list) else [value]
                        for author in authors:
                            name = ""
                            if isinstance(author, dict):
                                name = (
                                    author.get("name")
                                    or " ".join(
                                        part for part in [
                                            author.get("givenName") or author.get("given"),
                                            author.get("familyName") or author.get("family"),
                                        ] if part
                                    )
                                )
                            elif isinstance(author, str):
                                name = author
                            normalized = _normalize_text(name)
                            if normalized and normalized not in seen:
                                creators.append(_parse_creator_name(normalized))
                                seen.add(normalized)
                    visit(value)
            elif isinstance(node, list):
                for item in node:
                    visit(item)

        visit(payload)
    return creators


def _extract_date_from_jsonld(blocks: list[str]) -> str | None:
    keys = {"datepublished", "datecreated", "dateissued"}
    for block in blocks:
        try:
            payload = json.loads(block)
        except Exception:
            continue

        def walk(node: Any) -> str | None:
            if isinstance(node, dict):
                for key, value in node.items():
                    if str(key).lower() in keys and isinstance(value, str) and value.strip():
                        return _normalize_text(value)
                    found = walk(value)
                    if found:
                        return found
            elif isinstance(node, list):
                for item in node:
                    found = walk(item)
                    if found:
                        return found
            return None

        found = walk(payload)
        if found:
            return found
    return None


def _extract_description_from_jsonld(blocks: list[str]) -> str | None:
    for block in blocks:
        try:
            payload = json.loads(block)
        except Exception:
            continue

        def walk(node: Any) -> str | None:
            if isinstance(node, dict):
                for key, value in node.items():
                    if str(key).lower() == "description" and isinstance(value, str) and value.strip():
                        return clean_html(_normalize_text(value))
                    found = walk(value)
                    if found:
                        return found
            elif isinstance(node, list):
                for item in node:
                    found = walk(item)
                    if found:
                        return found
            return None

        found = walk(payload)
        if found:
            return found
    return None


def _extract_abstract_from_html_body(html: str) -> str | None:
    patterns = [
        r'<div[^>]+id=["\']abstract["\'][^>]*>(.*?)</div>',
        r'<section[^>]+id=["\']abstract["\'][^>]*>(.*?)</section>',
        r'<h\d[^>]*>\s*Abstract\s*</h\d>\s*(?:<[^>]+>\s*)*<p[^>]*>(.*?)</p>',
        r'<b>\s*Abstract\s*</b>\s*(?:</[^>]+>\s*)*(?:<br\s*/?>\s*)+(.*?)(?:<div|<p|<hr|</body>)',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.I | re.S)
        if not match:
            continue
        text = clean_html(match.group(1))
        text = _normalize_text(text)
        if text:
            return text
    return None


def _extract_venue_from_jsonld(blocks: list[str]) -> str | None:
    seen: set[str] = set()
    for block in blocks:
        try:
            payload = json.loads(block)
        except Exception:
            continue

        def _push(value: Any) -> str | None:
            if isinstance(value, str):
                normalized = _normalize_text(value)
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    return normalized
            elif isinstance(value, dict):
                for key in ("name", "alternateName"):
                    candidate = _push(value.get(key))
                    if candidate:
                        return candidate
            elif isinstance(value, list):
                for item in value:
                    candidate = _push(item)
                    if candidate:
                        return candidate
            return None

        def walk(node: Any) -> str | None:
            if isinstance(node, dict):
                for key, value in node.items():
                    key_norm = str(key).lower()
                    if key_norm in {
                        "ispartof",
                        "periodical",
                        "publication",
                        "journal",
                        "publisher",
                        "includedinpublication",
                    }:
                        candidate = _push(value)
                        if candidate:
                            return candidate
                    found = walk(value)
                    if found:
                        return found
            elif isinstance(node, list):
                for item in node:
                    found = walk(item)
                    if found:
                        return found
            return None

        found = walk(payload)
        if found:
            return found
    return None


def _title_candidate_from_urlish(url: str | None) -> list[str]:
    if not url:
        return []
    try:
        parsed = urlparse(url)
    except Exception:
        return []

    parts = [unquote(parsed.path or "")]
    if parsed.query:
        parts.append(unquote(parsed.query))

    candidates: list[str] = []
    seen: set[str] = set()
    suffix_stopwords = {
        "html",
        "htm",
        "php",
        "asp",
        "aspx",
        "jsp",
        "pdf",
        "paper",
        "papers",
        "article",
        "articles",
        "fulltext",
        "full",
        "document",
        "download",
        "view",
        "viewer",
        "content",
        "accepted",
        "camera",
        "cameraready",
        "ready",
        "supplement",
        "supplementary",
        "manuscript",
        "preprint",
        "abs",
        "abstract",
    }
    venue_stopwords = {
        "cvpr",
        "iccv",
        "eccv",
        "wacv",
        "accv",
        "neurips",
        "nips",
        "icml",
        "iclr",
        "aaai",
        "acl",
        "emnlp",
        "naacl",
        "nature",
        "science",
    }

    def add(text: str) -> None:
        normalized = _normalize_text(text)
        if len(_tokenize_match_text(normalized)) < 3:
            return
        if normalized not in seen:
            seen.add(normalized)
            candidates.append(normalized)

    for raw_part in parts:
        segments = [segment for segment in re.split(r"[/?&=#]", raw_part) if segment]
        for segment in segments:
            stem = Path(segment).stem
            if not stem:
                continue
            words = [word for word in re.split(r"[^A-Za-z0-9]+", stem) if word]
            if not words:
                continue

            while words and words[-1].lower() in suffix_stopwords:
                words.pop()
            while True:
                if (
                    len(words) >= 2
                    and words[-1].lower() in venue_stopwords
                    and re.fullmatch(r"(19|20)\d{2}", words[-2])
                ):
                    words = words[:-2]
                    continue
                if (
                    len(words) >= 2
                    and re.fullmatch(r"(19|20)\d{2}", words[-1])
                    and words[-2].lower() in venue_stopwords
                ):
                    words = words[:-2]
                    continue
                break
            while words and re.fullmatch(r"(19|20)\d{2}", words[-1]):
                words.pop()

            if words:
                add(" ".join(words))
            if len(words) >= 5:
                add(" ".join(words[1:]))
            if len(words) >= 7:
                add(" ".join(words[2:]))

    return candidates


def _query_items_for_existing_copy(
    zot,
    *,
    query: str | None,
    qmode: Literal["titleCreatorYear", "everything"] = "everything",
    limit: int = 25,
) -> list[dict[str, Any]]:
    normalized_query = _normalize_text(query)
    if not normalized_query or not hasattr(zot, "items"):
        return []
    try:
        return zot.items(
            q=normalized_query,
            qmode=qmode,
            itemType="-attachment",
            limit=limit,
        ) or []
    except TypeError:
        if not hasattr(zot, "add_parameters"):
            return []
        try:
            zot.add_parameters(
                q=normalized_query,
                qmode=qmode,
                itemType="-attachment",
                limit=limit,
            )
            return zot.items() or []
        except Exception:
            return []
    except Exception:
        return []


def _looks_like_informative_pdf_title(candidate: str | None) -> bool:
    normalized = _normalize_text(candidate)
    if not normalized:
        return False
    lowered = normalized.lower()
    if lowered in {
        "untitled",
        "article",
        "paper",
        "pdf",
        "full text",
        "fulltext",
        "manuscript",
        "microsoft word - article",
    }:
        return False
    tokens = _tokenize_match_text(normalized)
    if len(tokens) < 3 or len(tokens) > 28:
        return False
    if _normalize_doi(normalized):
        return False
    if lowered.startswith("abstract") or lowered.startswith("arxiv"):
        return False
    return True


def _infer_title_from_pdf_text(text: str, *, pdf_url: str | None = None) -> str | None:
    for raw_line in re.split(r"[\r\n]+", text or ""):
        line = _normalize_text(raw_line)
        if not _looks_like_informative_pdf_title(line):
            continue
        return line

    for candidate in _title_candidate_from_urlish(pdf_url):
        if _looks_like_informative_pdf_title(candidate):
            return candidate
    return None


def _extract_pdf_probe_signals(
    pdf_bytes: bytes,
    *,
    pdf_url: str,
    ctx: Context,
) -> dict[str, Any]:
    metadata_title = ""
    metadata_author = ""
    extracted_text = ""

    try:
        import fitz  # type: ignore

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        try:
            metadata = doc.metadata or {}
            metadata_title = _normalize_text(metadata.get("title"))
            metadata_author = _normalize_text(metadata.get("author"))
            text_chunks: list[str] = []
            for page_index in range(min(len(doc), 2)):
                page = doc.load_page(page_index)
                text_chunks.append(page.get_text("text"))
            extracted_text = "\n".join(text_chunks)
        finally:
            doc.close()
    except Exception as exc:
        _ctx_warning(ctx, f"PDF metadata probe could not extract text from {pdf_url}: {exc}")

    combined_text = "\n".join(part for part in [metadata_title, metadata_author, extracted_text] if part)
    combined_text = combined_text[:20000]

    creators: list[dict[str, str]] = []
    if metadata_author:
        author_parts = [part.strip() for part in re.split(r"\s*(?:;| and )\s*", metadata_author) if part.strip()]
        creators = [_parse_creator_name(part) for part in author_parts[:6]]

    title = metadata_title if _looks_like_informative_pdf_title(metadata_title) else None
    if not title:
        title = _infer_title_from_pdf_text(extracted_text, pdf_url=pdf_url)

    signals: dict[str, Any] = {
        "source_url": pdf_url,
        "final_url": pdf_url,
        "title": title,
        "venue": "",
        "description": "",
        "abstract_note": "",
        "creators": creators,
        "date": _extract_year_from_text(combined_text),
        "doi": _normalize_doi(combined_text),
        "arxiv_id": _normalize_arxiv_id(combined_text) or _normalize_arxiv_id(pdf_url),
        "pdf_candidates": [{"source": "direct_pdf", "url": pdf_url}],
        "content_type": "application/pdf",
    }
    return signals


def _probe_identifier_from_direct_pdf_url(
    pdf_url: str,
    *,
    ctx: Context,
) -> dict[str, Any] | None:
    def _probe_via_local_connector() -> dict[str, Any] | None:
        local_zot = get_local_zotero_client()
        if local_zot is None or not hasattr(local_zot, "items") or not hasattr(local_zot, "client"):
            return None

        session_id = f"zotero-mcp-probe-{uuid.uuid4().hex[:8]}"
        probe_title = f"zotero-mcp-pdf-probe-{uuid.uuid4().hex}"
        connector_item = {
            "itemType": "webpage",
            "title": probe_title,
            "url": pdf_url,
            "accessDate": datetime.now(timezone.utc).date().isoformat(),
            "attachments": [],
            "notes": [],
            "complete": True,
            "id": "item-1",
        }
        metadata = {
            "sessionID": session_id,
            "parentItemID": connector_item["id"],
            "title": _pdf_filename_for_item({}, pdf_url=pdf_url) or "probe.pdf",
            "url": pdf_url,
        }

        local_item_key: str | None = None
        try:
            _ensure_connector_library_context(ctx=ctx)
            create_resp = requests.post(
                "http://127.0.0.1:23119/connector/saveItems",
                json=_connector_save_items_payload(
                    session_id=session_id,
                    items=[connector_item],
                    base_uri=pdf_url,
                ),
                timeout=20,
            )
            create_resp.raise_for_status()

            attach_resp = requests.post(
                "http://127.0.0.1:23119/connector/saveAttachment",
                params={"sessionID": session_id},
                data=b"",
                headers={
                    "Content-Type": "application/pdf",
                    "Content-Length": "0",
                    "X-Metadata": json.dumps(metadata),
                },
                timeout=_connector_url_attach_timeout_seconds(),
            )
            attach_resp.raise_for_status()

            local_item = _wait_for_local_item_by_metadata(
                title=probe_title,
                item_type="webpage",
                doi=None,
                url=None,
                require_pdf=False,
                wait_seconds=8.0,
                poll_interval=0.5,
            )
            if not local_item:
                return None
            local_item_key = local_item.get("key")
            if not local_item_key:
                return None

            materialized = _confirm_local_pdf_attachment_materialized(
                local_item_key,
                ctx=ctx,
                wait_seconds=12.0,
                poll_interval=0.5,
            )
            if not materialized.get("success"):
                return None

            attachment_key = materialized.get("attachment_key")
            if not attachment_key:
                return None

            with tempfile.TemporaryDirectory(prefix="zotero-mcp-probe-") as tmpdir:
                probe_path = Path(tmpdir) / (_pdf_filename_for_item({}, pdf_url=pdf_url) or "probe.pdf")
                dump_attachment_to_file(local_zot, attachment_key, probe_path, ctx=ctx)
                return _extract_pdf_probe_signals(
                    probe_path.read_bytes(),
                    pdf_url=pdf_url,
                    ctx=ctx,
                )
        except Exception as exc:
            _ctx_warning(ctx, f"Local connector/browser-session PDF probe failed for {pdf_url}: {exc}")
            return None
        finally:
            if local_item_key:
                with suppress(Exception):
                    local_zot.delete_item(_get_item_payload(local_zot, local_item_key) or {"key": local_item_key})
                web_zot = get_web_zotero_client()
                if web_zot is not None:
                    with suppress(Exception):
                        web_payload = _get_item_payload(web_zot, local_item_key)
                        if web_payload:
                            web_zot.delete_item(web_payload)

    try:
        pdf_bytes, _ = _download_pdf_bytes(pdf_url, ctx=ctx)
    except Exception as exc:
        _ctx_warning(ctx, f"Direct PDF probe download failed for {pdf_url}: {exc}")
        connector_signals = _probe_via_local_connector()
        if connector_signals and (
            connector_signals.get("doi")
            or connector_signals.get("arxiv_id")
            or connector_signals.get("title")
        ):
            return connector_signals
        return None

    signals = _extract_pdf_probe_signals(pdf_bytes, pdf_url=pdf_url, ctx=ctx)
    if signals.get("doi") or signals.get("arxiv_id") or signals.get("title"):
        return signals
    connector_signals = _probe_via_local_connector()
    if connector_signals and (
        connector_signals.get("doi")
        or connector_signals.get("arxiv_id")
        or connector_signals.get("title")
    ):
        return connector_signals
    return None


def _download_pdf_bytes_via_local_connector_browser_session(
    pdf_url: str,
    *,
    filename: str,
    ctx: Context,
    deadline: float | None = None,
) -> tuple[bytes, str] | None:
    if not _connector_zero_byte_url_attach_enabled():
        return None
    local_zot = get_local_zotero_client()
    if local_zot is None or not hasattr(local_zot, "items") or not hasattr(local_zot, "client"):
        return None

    session_id = f"zotero-mcp-browser-fetch-{uuid.uuid4().hex[:8]}"
    probe_title = f"zotero-mcp-browser-fetch-{uuid.uuid4().hex}"
    connector_item = {
        "itemType": "webpage",
        "title": probe_title,
        "url": pdf_url,
        "accessDate": datetime.now(timezone.utc).date().isoformat(),
        "attachments": [],
        "notes": [],
        "complete": True,
        "id": "item-1",
    }
    metadata = {
        "sessionID": session_id,
        "parentItemID": connector_item["id"],
        "title": filename or "probe.pdf",
        "url": pdf_url,
    }

    local_item_key: str | None = None
    try:
        if _deadline_exceeded(deadline):
            return None
        _ensure_connector_library_context(ctx=ctx)
        connector_attach_timeout = min(
            _connector_url_attach_timeout_seconds(),
            _remaining_budget_seconds(deadline, _connector_url_attach_timeout_seconds()),
        )
        create_resp = requests.post(
            "http://127.0.0.1:23119/connector/saveItems",
            json=_connector_save_items_payload(
                session_id=session_id,
                items=[connector_item],
                base_uri=pdf_url,
            ),
            timeout=_remaining_budget_seconds(deadline, 10.0),
        )
        create_resp.raise_for_status()

        if _deadline_exceeded(deadline):
            return None
        attach_resp = requests.post(
            "http://127.0.0.1:23119/connector/saveAttachment",
            params={"sessionID": session_id},
            data=b"",
            headers={
                "Content-Type": "application/pdf",
                "Content-Length": "0",
                "X-Metadata": json.dumps(metadata),
            },
            timeout=connector_attach_timeout,
        )
        attach_resp.raise_for_status()

        local_item = _wait_for_local_item_by_metadata(
            title=probe_title,
            item_type="webpage",
            doi=None,
            url=None,
            require_pdf=False,
            wait_seconds=min(15.0, _remaining_budget_seconds(deadline, 15.0)),
            poll_interval=0.5,
        )
        if not local_item:
            return None
        local_item_key = local_item.get("key")
        if not local_item_key:
            return None

        materialized = _confirm_local_pdf_attachment_materialized(
            local_item_key,
            ctx=ctx,
            wait_seconds=min(
                max(connector_attach_timeout, 10.0),
                _remaining_budget_seconds(deadline, max(connector_attach_timeout, 10.0)),
            ),
            poll_interval=0.5,
        )
        if not materialized.get("success"):
            return None

        attachment_key = materialized.get("attachment_key")
        if not attachment_key:
            return None

        with tempfile.TemporaryDirectory(prefix="zotero-mcp-browser-fetch-") as tmpdir:
            probe_path = Path(tmpdir) / (filename or "probe.pdf")
            dump_attachment_to_file(local_zot, attachment_key, probe_path, ctx=ctx)
            pdf_bytes = probe_path.read_bytes()
            if not pdf_bytes.startswith(b"%PDF"):
                return None
            return pdf_bytes, "application/pdf"
    except Exception as exc:
        _ctx_warning(ctx, f"Local connector/browser-session PDF fetch failed for {pdf_url}: {exc}")
        return None
    finally:
        if local_item_key:
            with suppress(Exception):
                local_zot.delete_item(_get_item_payload(local_zot, local_item_key) or {"key": local_item_key})
            web_zot = get_web_zotero_client()
            if web_zot is not None:
                with suppress(Exception):
                    web_payload = _get_item_payload(web_zot, local_item_key)
                    if web_payload:
                        web_zot.delete_item(web_payload)


def _venue_candidates_from_urlish(url: str | None) -> list[str]:
    if not url:
        return []
    text = unquote(url)
    candidates: list[str] = []
    seen: set[str] = set()
    for segment in re.split(r"[/?&=#]", text):
        if not segment:
            continue
        for match in re.finditer(r"([A-Za-z]{2,12})[-_ ]((?:19|20)\d{2})", segment):
            venue = match.group(1).upper()
            if venue not in seen:
                seen.add(venue)
                candidates.append(venue)
    return candidates


def _collect_identifier_search_hints(signals: dict[str, Any]) -> dict[str, Any]:
    title_candidates: list[dict[str, str]] = []
    seen_titles: set[str] = set()

    def add_title(candidate: str | None, source: str) -> None:
        normalized = _normalize_text(candidate)
        if not normalized:
            return
        if len(_tokenize_match_text(normalized)) < 3:
            return
        if normalized in seen_titles:
            return
        seen_titles.add(normalized)
        title_candidates.append({"text": normalized, "source": source})

    add_title(signals.get("title"), "signals:title")
    for url_key in ("source_url", "final_url"):
        for candidate in _title_candidate_from_urlish(signals.get(url_key)):
            add_title(candidate, f"{url_key}:url")
    for pdf_candidate in signals.get("pdf_candidates") or []:
        for candidate in _title_candidate_from_urlish(pdf_candidate.get("url")):
            add_title(candidate, f"{pdf_candidate.get('source', 'pdf')}:url")

    venue_candidates: list[str] = []
    seen_venues: set[str] = set()

    def add_venue(candidate: str | None) -> None:
        normalized = _normalize_text(candidate)
        if not normalized:
            return
        key = _normalize_venue_for_match(normalized)
        if not key or key in seen_venues:
            return
        seen_venues.add(key)
        venue_candidates.append(normalized)

    add_venue(signals.get("venue"))
    for url_key in ("source_url", "final_url"):
        for venue in _venue_candidates_from_urlish(signals.get(url_key)):
            add_venue(venue)
    for pdf_candidate in signals.get("pdf_candidates") or []:
        for venue in _venue_candidates_from_urlish(pdf_candidate.get("url")):
            add_venue(venue)

    creator_last = ""
    for creator in signals.get("creators") or []:
        creator_last = _normalize_text(creator.get("lastName") or creator.get("name"))
        if creator_last:
            break

    return {
        "title_candidates": title_candidates,
        "venue_candidates": venue_candidates,
        "year": _extract_year_from_text(signals.get("date")),
        "creator_last": creator_last,
    }


def _title_similarity_score(candidate: str | None, work_title: str | None) -> float:
    candidate_norm = _normalize_title_for_match(candidate)
    work_norm = _normalize_title_for_match(work_title)
    if not candidate_norm or not work_norm:
        return 0.0
    if candidate_norm == work_norm:
        return 1.0

    shorter, longer = (
        (candidate_norm, work_norm)
        if len(candidate_norm) <= len(work_norm)
        else (work_norm, candidate_norm)
    )
    if len(shorter) >= 18 and longer.startswith(shorter):
        return 0.97
    if len(shorter) >= 18 and shorter in longer:
        return 0.92

    candidate_tokens = _tokenize_match_text(candidate)
    work_tokens = _tokenize_match_text(work_title)
    if not candidate_tokens or not work_tokens:
        return 0.0

    prefix_matches = 0
    for cand_token, work_token in zip(candidate_tokens, work_tokens):
        if cand_token != work_token:
            break
        prefix_matches += 1
    prefix_ratio = prefix_matches / max(1, min(len(candidate_tokens), len(work_tokens)))

    ordered_matches = 0
    work_index = 0
    for cand_token in candidate_tokens:
        while work_index < len(work_tokens) and work_tokens[work_index] != cand_token:
            work_index += 1
        if work_index >= len(work_tokens):
            break
        ordered_matches += 1
        work_index += 1
    ordered_ratio = ordered_matches / max(1, len(candidate_tokens))

    overlap_ratio = (
        len(set(candidate_tokens) & set(work_tokens)) / max(1, len(set(candidate_tokens)))
    )
    sequence_ratio = SequenceMatcher(None, candidate_norm, work_norm).ratio()
    return max(
        prefix_ratio * 0.95,
        ordered_ratio * 0.9,
        overlap_ratio * 0.75,
        sequence_ratio * 0.7,
    )


def _work_year(work: dict[str, Any]) -> str:
    for field_name in ("published", "published-print", "published-online", "issued"):
        date_parts = (work.get(field_name) or {}).get("date-parts", [[]])
        if date_parts and date_parts[0]:
            return str(date_parts[0][0])
    return ""


def _work_venue_candidates(work: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("container-title", "short-container-title"):
        raw_values = work.get(key) or []
        if isinstance(raw_values, str):
            raw_values = [raw_values]
        for value in raw_values:
            normalized = _normalize_text(value)
            if normalized and normalized not in values:
                values.append(normalized)
    return values


def _venue_similarity_score(venue_candidates: list[str], work: dict[str, Any]) -> float:
    if not venue_candidates:
        return 0.0
    work_venues = _work_venue_candidates(work)
    if not work_venues:
        return 0.0

    best = 0.0
    for candidate in venue_candidates:
        candidate_norm = _normalize_venue_for_match(candidate)
        if not candidate_norm:
            continue
        for work_venue in work_venues:
            work_norm = _normalize_venue_for_match(work_venue)
            if not work_norm:
                continue
            if candidate_norm == work_norm:
                best = max(best, 1.0)
            elif candidate_norm in work_norm or work_norm in candidate_norm:
                best = max(best, 0.8)
    return best


def _score_crossref_work(
    *,
    hints: dict[str, Any],
    work: dict[str, Any],
) -> tuple[float, dict[str, Any]]:
    work_titles = work.get("title", []) or []
    work_title = _normalize_text(work_titles[0] if work_titles else "")
    if not work_title:
        return -1.0, {}

    best_title_score = 0.0
    best_title_source = ""
    for candidate in hints.get("title_candidates", []):
        title_score = _title_similarity_score(candidate.get("text"), work_title)
        source = candidate.get("source", "")
        if source == "signals:title":
            title_score *= 1.03
        if title_score > best_title_score:
            best_title_score = title_score
            best_title_source = source

    if best_title_score < 0.5:
        return -1.0, {}

    score = best_title_score * 100.0
    diagnostics: dict[str, Any] = {
        "title_score": round(best_title_score, 4),
        "title_source": best_title_source,
    }

    signal_year = hints.get("year") or ""
    work_year = _work_year(work)
    if signal_year and work_year:
        if signal_year == work_year:
            score += 15.0
            diagnostics["year_match"] = True
        else:
            score -= 20.0
            diagnostics["year_match"] = False

    creator_last = _normalize_text(hints.get("creator_last"))
    if creator_last:
        work_authors = work.get("author", []) or []
        if work_authors:
            work_last = _normalize_text(work_authors[0].get("family"))
            if work_last and work_last.lower() == creator_last.lower():
                score += 12.0
                diagnostics["author_match"] = True
            elif work_last:
                score -= 8.0
                diagnostics["author_match"] = False

    venue_score = _venue_similarity_score(hints.get("venue_candidates") or [], work)
    if venue_score:
        score += venue_score * 18.0
        diagnostics["venue_score"] = round(venue_score, 4)

    return score, diagnostics


def _crossref_candidate_matches_work(
    *,
    title: str | None,
    creators: list[dict[str, str]] | None,
    date_text: str | None,
    work: dict[str, Any],
) -> bool:
    signal_title = _normalize_title_for_match(title)
    work_titles = work.get("title", []) or []
    work_title = _normalize_title_for_match(work_titles[0] if work_titles else "")
    if not signal_title or not work_title or signal_title != work_title:
        return False

    signal_year_match = re.search(r"(19|20)\d{2}", date_text or "")
    signal_year = signal_year_match.group(0) if signal_year_match else ""
    if signal_year:
        for field_name in ("published", "published-print", "published-online", "issued"):
            date_parts = (work.get(field_name) or {}).get("date-parts", [[]])
            if date_parts and date_parts[0]:
                if str(date_parts[0][0]) != signal_year:
                    return False
                break

    signal_last = ""
    for creator in creators or []:
        signal_last = _normalize_text(creator.get("lastName") or creator.get("name"))
        if signal_last:
            break
    if signal_last:
        work_authors = work.get("author", []) or []
        if work_authors:
            work_last = _normalize_text(work_authors[0].get("family"))
            if work_last and work_last.lower() != signal_last.lower():
                return False

    return True


def _lookup_crossref_doi_for_signals(signals: dict[str, Any], *, ctx: Context) -> str | None:
    hints = _collect_identifier_search_hints(signals)
    title_candidates = hints.get("title_candidates") or []
    if not title_candidates:
        return None

    query_specs: list[tuple[dict[str, Any], str]] = []
    for candidate in title_candidates[:4]:
        text = candidate["text"]
        query_specs.append(({"query.title": text, "rows": 8}, f"title:{text}"))
        venue_candidates = hints.get("venue_candidates") or []
        bibliographic = text
        if venue_candidates:
            bibliographic = f"{text} {venue_candidates[0]}"
        year = hints.get("year") or ""
        if year:
            bibliographic = f"{bibliographic} {year}"
        query_specs.append(
            ({"query.bibliographic": bibliographic, "rows": 8}, f"bibliographic:{bibliographic}")
        )

    best: tuple[float, str | None, dict[str, Any]] = (-1.0, None, {})
    second_best = -1.0
    seen_dois: set[str] = set()

    for params, label in query_specs:
        try:
            resp = requests.get(
                "https://api.crossref.org/works",
                headers={"User-Agent": "zotero-mcp/1.0 (mailto:user@example.com)"},
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            payload = resp.json() or {}
        except Exception as exc:
            _ctx_warning(ctx, f"Crossref title lookup failed for '{label}': {exc}")
            continue

        items = (payload.get("message") or {}).get("items") or []
        for work in items:
            if not isinstance(work, dict):
                continue
            doi = _normalize_doi(work.get("DOI"))
            if not doi:
                continue
            if doi in seen_dois:
                continue
            seen_dois.add(doi)

            score, diagnostics = _score_crossref_work(hints=hints, work=work)
            if score < 0:
                continue

            if score > best[0]:
                second_best = best[0]
                best = (score, doi, diagnostics | {"query": label})
            elif score > second_best:
                second_best = score

    best_score, best_doi, best_meta = best
    if not best_doi:
        return None

    if best_score >= 92 or (best_score >= 80 and best_score - second_best >= 8):
        ctx.info(
            "Crossref identifier rescue matched "
            f"{best_doi} with score={best_score:.1f} meta={best_meta}"
        )
        return best_doi
    return None


def _crossref_work_matches_structured_url(
    work: dict[str, Any],
    *,
    volume: str,
    issue: str,
    page: str,
    issn: str | None = None,
) -> bool:
    if str(work.get("volume") or "").strip() != volume:
        return False
    if str(work.get("issue") or "").strip() != issue:
        return False
    if str(work.get("page") or "").strip() != page:
        return False
    if issn:
        work_issn = {str(value).strip().lower() for value in (work.get("ISSN") or []) if value}
        if work_issn and issn.lower() not in work_issn:
            return False
    return True


def _lookup_crossref_work_from_structured_url(url: str, *, ctx: Context) -> dict[str, Any] | None:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path_parts = [part for part in (parsed.path or "").split("/") if part]

    host_prefix_map = {
        "mdpi.com": "10.3390",
        "www.mdpi.com": "10.3390",
    }
    prefix = host_prefix_map.get(host)
    if prefix is None:
        return None
    if len(path_parts) < 4:
        return None

    issn, volume, issue, article = path_parts[:4]
    if not re.fullmatch(r"\d{4}-\d{4}", issn):
        return None
    if not all(re.fullmatch(r"\d+", value) for value in (volume, issue, article)):
        return None

    bibliographic = f"{issn} {volume} {issue} {article}"
    try:
        resp = requests.get(
            "https://api.crossref.org/works",
            headers={"User-Agent": "zotero-mcp/1.0 (mailto:user@example.com)"},
            params={
                "filter": f"prefix:{prefix}",
                "query.bibliographic": bibliographic,
                "rows": 5,
            },
            timeout=15,
        )
        resp.raise_for_status()
    except Exception as exc:
        _ctx_warning(ctx, f"Structured URL DOI lookup failed for {url}: {exc}")
        return None

    for work in (resp.json().get("message") or {}).get("items") or []:
        if _crossref_work_matches_structured_url(
            work,
            volume=volume,
            issue=issue,
            page=article,
            issn=issn,
        ):
            return work
    return None


def _extract_pdf_link_from_html(html: str, base_url: str) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []

    def add_candidate(raw_url: str | None, source: str) -> None:
        if not raw_url:
            return
        joined = urljoin(base_url, raw_url)
        https_upgrade = _upgrade_to_https_candidate_url(joined, reference_url=base_url)
        if https_upgrade and https_upgrade != joined:
            candidates.append({"source": f"{source}:https_upgrade", "url": https_upgrade})
        candidates.append({"source": source, "url": joined})

    meta_pdf = _extract_meta_content(html, ["citation_pdf_url", "pdf_url"])
    if meta_pdf:
        add_candidate(meta_pdf, "html:citation_pdf_url")

    patterns = [
        r'<link[^>]+type=["\']application/pdf["\'][^>]+href=["\'](.*?)["\']',
        r'<link[^>]+href=["\'](.*?)["\'][^>]+type=["\']application/pdf["\']',
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, html, re.I | re.S):
            href = match.group(1).strip()
            if href:
                add_candidate(href, "html:alternate_pdf")

    return _dedupe_pdf_candidates(candidates)


def _dedupe_pdf_candidates(candidates: list[dict[str, str]] | None) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for candidate in candidates or []:
        url = str(candidate.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        deduped.append(
            {
                "source": str(candidate.get("source") or "unknown"),
                "url": url,
            }
        )
    return deduped


def _resolve_pdf_discovery_url(
    url: str | None,
    *,
    ctx: Context,
    repair_mode: bool = False,
    deadline: float | None = None,
) -> str | None:
    if not url:
        return None
    try:
        if _deadline_exceeded(deadline):
            return None
        response = _requests_get_with_retry(
            url,
            headers={"User-Agent": "Mozilla/5.0 zotero-mcp/1.0"},
            timeout=4 if repair_mode else 15,
            attempts=1 if repair_mode else 2,
            deadline=deadline,
        )
    except Exception as exc:
        _ctx_warning(ctx, f"Could not resolve landing URL for PDF discovery: {url} ({exc})")
        return None

    resolved_url = str(getattr(response, "url", "") or url).strip()
    return resolved_url or url


def _is_doi_resolver_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False
    return host in {"doi.org", "dx.doi.org"}


def _upgrade_to_https_candidate_url(candidate_url: str | None, *, reference_url: str | None) -> str | None:
    if not candidate_url or not reference_url:
        return None
    try:
        candidate = urlparse(candidate_url)
        reference = urlparse(reference_url)
    except Exception:
        return None

    if reference.scheme.lower() != "https" or candidate.scheme.lower() != "http":
        return None
    if not candidate.netloc or candidate.netloc.lower() != reference.netloc.lower():
        return None

    return candidate._replace(scheme="https").geturl()


def _source_label_from_url(url: str | None) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except Exception:
        return ""

    host = (parsed.netloc or "").lower()
    host_aliases = (
        ("proceedings.mlr.press", "Proceedings of Machine Learning Research"),
        ("jmlr.org", "Journal of Machine Learning Research"),
        ("aclanthology.org", "ACL Anthology"),
        ("openreview.net", "OpenReview"),
        ("nature.com", "Nature"),
        ("ieeexplore.ieee.org", "IEEE"),
        ("dl.acm.org", "ACM"),
        ("cvf.com", "CVF"),
        ("arxiv.org", "arXiv"),
    )
    for needle, label in host_aliases:
        if needle in host:
            return label

    host_parts = [
        part
        for part in re.split(r"[^a-z0-9]+", host)
        if part and part not in {"www", "com", "org", "net", "edu", "gov", "io", "co"}
    ]
    if not host_parts:
        return ""
    if len(host_parts[0]) <= 4:
        return host_parts[0].upper()
    return host_parts[0].title()


def _infer_pdf_candidates_from_url(url: str | None) -> list[dict[str, str]]:
    if not url:
        return []

    try:
        parsed = urlparse(url)
    except Exception:
        return []

    candidates: list[dict[str, str]] = []
    seen: set[str] = set()

    def add(candidate_url: str | None, source: str) -> None:
        if not candidate_url:
            return
        normalized = candidate_url.strip()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        candidates.append({"source": source, "url": normalized})

    arxiv_id = _normalize_arxiv_id(url)
    if arxiv_id:
        add(f"https://arxiv.org/pdf/{arxiv_id}.pdf", "url_pattern:arxiv_pdf")

    if _looks_like_direct_pdf_url(url):
        add(parsed.geturl(), "url_pattern:direct_pdf")

    known_fallback = _fallback_signals_from_known_landing_page(url)
    if known_fallback:
        for candidate in known_fallback.get("pdf_candidates") or []:
            add(candidate.get("url"), candidate.get("source") or "url_pattern:known_pdf")

    path = parsed.path or ""
    suffix = Path(path).suffix.lower()
    stem = Path(path).stem

    if suffix in {".html", ".htm", ".php", ".asp", ".aspx", ".jsp"}:
        add(parsed._replace(path=re.sub(r"\.[^.\/]+$", ".pdf", path), query="", fragment="").geturl(), "url_pattern:same_stem_pdf")
        if stem:
            subdir_pdf_path = f"{Path(path).parent.as_posix().rstrip('/')}/{stem}/{stem}.pdf"
            add(parsed._replace(path=subdir_pdf_path, query="", fragment="").geturl(), "url_pattern:stem_subdir_pdf")

    if not suffix and stem:
        add(parsed._replace(path=f"{path}.pdf", query="", fragment="").geturl(), "url_pattern:path_pdf")

    return _dedupe_pdf_candidates(candidates)


def _fallback_signals_from_url_inference(url: str) -> dict[str, Any] | None:
    pdf_candidates = _infer_pdf_candidates_from_url(url)
    title_candidates = _title_candidate_from_urlish(url)
    venue_candidates = _venue_candidates_from_urlish(url)
    source_label = _source_label_from_url(url)
    year = _extract_year_from_text(url)
    doi = _normalize_doi(url)
    arxiv_id = _normalize_arxiv_id(url)

    title = title_candidates[0] if title_candidates else None
    venue = venue_candidates[0] if venue_candidates else source_label

    if not any([title, venue, year, doi, arxiv_id, pdf_candidates]):
        return None

    return {
        "source_url": url,
        "final_url": url,
        "title": title,
        "venue": venue or "",
        "description": "",
        "abstract_note": "",
        "creators": [],
        "date": year,
        "doi": doi,
        "arxiv_id": arxiv_id,
        "pdf_candidates": pdf_candidates,
        "content_type": "timeout/url_inference",
    }


def _enrich_fallback_signals_from_structured_url(
    signals: dict[str, Any],
    *,
    ctx: Context,
) -> dict[str, Any]:
    if signals.get("doi"):
        return signals
    work = _lookup_crossref_work_from_structured_url(str(signals.get("final_url") or signals.get("source_url") or ""), ctx=ctx)
    if not work:
        return signals

    work_title = ((work.get("title") or [""]) or [""])[0] or ""
    work_venue = ((work.get("container-title") or [""]) or [""])[0] or ""
    work_year = _work_year(work)
    work_doi = _normalize_doi(work.get("DOI"))
    if work_title and not signals.get("title"):
        signals["title"] = work_title
    if work_venue and not signals.get("venue"):
        signals["venue"] = work_venue
    if work_year and not signals.get("date"):
        signals["date"] = work_year
    if work_doi:
        signals["doi"] = work_doi

    for link in work.get("link") or []:
        link_url = link.get("URL")
        if not link_url:
            continue
        signals.setdefault("pdf_candidates", []).append(
            {"source": "crossref:link", "url": str(link_url)}
        )
    signals["pdf_candidates"] = _dedupe_pdf_candidates(signals.get("pdf_candidates") or [])
    return signals


def _discover_pdf_candidates_from_crossref_work(
    work: dict[str, Any],
    *,
    doi: str,
    ctx: Context,
    repair_mode: bool = False,
    deadline: float | None = None,
) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    landing_urls: list[str] = []

    def add_candidate(candidate_url: str | None, source: str) -> None:
        if not candidate_url:
            return
        candidates.append({"source": source, "url": str(candidate_url).strip()})

    def add_landing_url(candidate_url: str | None) -> None:
        if not candidate_url:
            return
        normalized = str(candidate_url).strip()
        if normalized and normalized not in landing_urls:
            landing_urls.append(normalized)

    for link in work.get("link") or []:
        link_url = str((link or {}).get("URL") or "").strip()
        if not link_url:
            continue
        content_type = str((link or {}).get("content-type") or "").lower()
        if "application/pdf" in content_type or _looks_like_direct_pdf_url(link_url):
            add_candidate(link_url, "crossref:link")
            continue
        add_landing_url(link_url)

    add_landing_url(str(work.get("URL") or "").strip() or f"https://doi.org/{doi}")

    resolved_landing_urls: list[str] = []
    for landing_url in landing_urls:
        if _deadline_exceeded(deadline):
            break
        if not _is_doi_resolver_url(landing_url):
            candidates.extend(_infer_pdf_candidates_from_url(landing_url))
        resolved_url = _resolve_pdf_discovery_url(
            landing_url,
            ctx=ctx,
            repair_mode=repair_mode,
            deadline=deadline,
        )
        if resolved_url:
            resolved_landing_urls.append(resolved_url)
            if resolved_url != landing_url:
                candidates.extend(_infer_pdf_candidates_from_url(resolved_url))

    if candidates:
        return _dedupe_pdf_candidates(candidates)

    page_discovery_urls = [
        landing_url
        for landing_url in (resolved_landing_urls or landing_urls)
        if not _is_doi_resolver_url(landing_url)
    ]
    for landing_url in page_discovery_urls:
        if _deadline_exceeded(deadline):
            break
        try:
            signals = _fetch_page_signals(
                landing_url,
                ctx=ctx,
                repair_mode=repair_mode,
                deadline=deadline,
            )
        except Exception as exc:
            _ctx_warning(ctx, f"Could not inspect resolved landing page for DOI PDF discovery: {landing_url} ({exc})")
            continue
        candidates.extend(signals.get("pdf_candidates") or [])

    return _dedupe_pdf_candidates(candidates)


def _fallback_signals_from_known_landing_page(url: str) -> dict[str, Any] | None:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    if "openaccess.thecvf.com" not in host:
        return None

    match: re.Match[str] | None = None
    for pattern in (
        r"/content/([^/]+)/html/([^/]+?)(?:_paper)?\.html$",
        r"/content_([^/]+)/html/([^/]+?)(?:_paper)?\.html$",
    ):
        match = re.search(pattern, path, flags=re.I)
        if match:
            break
    if not match:
        return None

    venue_slug = match.group(1)
    title_slug = match.group(2)
    title_slug = re.sub(r"^[^_]+_", "", title_slug)
    title_slug = re.sub(
        r"_(CVPR|ICCV|ECCV|WACV|ACCV)(?:_|)?[0-9]{4}$",
        "",
        title_slug,
        flags=re.I,
    )
    title = title_slug.replace("_", " ").strip()
    year_match = re.search(r"(19|20)\d{2}", venue_slug)
    inferred_pdf_url = url.replace("/html/", "/papers/").replace(".html", ".pdf")

    if not title:
        return None

    return {
        "source_url": url,
        "final_url": url,
        "title": title,
        "venue": re.sub(r"(19|20)\d{2}", "", venue_slug).replace("_", " ").strip(),
        "description": "",
        "abstract_note": "",
        "creators": [],
        "date": year_match.group(0) if year_match else "",
        "doi": None,
        "arxiv_id": None,
        "pdf_candidates": [{"source": "url_pattern:cvf_pdf", "url": inferred_pdf_url}],
        "content_type": "",
    }


def _fetch_page_signals(
    url: str,
    *,
    ctx: Context,
    repair_mode: bool = False,
    deadline: float | None = None,
) -> dict[str, Any]:
    import urllib.request

    signals: dict[str, Any] = {
        "source_url": url,
        "final_url": url,
        "title": None,
        "venue": "",
        "description": "",
        "abstract_note": "",
        "creators": [],
        "date": "",
        "doi": None,
        "arxiv_id": None,
        "pdf_candidates": [],
        "content_type": "",
    }

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 zotero-mcp/1.0"})
    try:
        if _deadline_exceeded(deadline):
            raise TimeoutError(f"repair deadline exceeded before fetching {url}")
        timeout_seconds = _clamp_timeout_to_deadline(
            6.0 if repair_mode else 15.0,
            deadline=deadline,
        )
        with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
            final_url = getattr(response, "geturl", lambda: url)()
            headers = getattr(response, "headers", {})
            content_type = headers.get("Content-Type", "") if hasattr(headers, "get") else ""
            body = response.read(262144)
    except Exception as exc:
        fallback = _fallback_signals_from_known_landing_page(url)
        if fallback is None:
            fallback = _fallback_signals_from_url_inference(url)
        if fallback is None:
            raise
        fallback = _enrich_fallback_signals_from_structured_url(fallback, ctx=ctx)
        _ctx_warning(ctx, f"Page fetch failed for {url}; using timeout-safe URL inference fallback: {exc}")
        return fallback

    signals["final_url"] = final_url or url
    signals["content_type"] = content_type

    if "application/pdf" in content_type.lower() or _looks_like_direct_pdf_url(signals["final_url"]):
        signals["pdf_candidates"].append({"source": "direct_pdf", "url": signals["final_url"]})
        return signals

    html = body.decode("utf-8", errors="replace")
    jsonld_blocks = _extract_jsonld_blocks(html)

    signals["title"] = (
        _extract_meta_content(html, ["og:title", "citation_title"])
        or (
            re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S).group(1).strip()
            if re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
            else None
        )
    )
    signals["description"] = _extract_meta_content(
        html,
        ["og:description", "description", "dc.description"],
    ) or ""
    signals["venue"] = (
        _extract_meta_content(
            html,
            [
                "citation_journal_title",
                "citation_conference_title",
                "citation_book_title",
                "citation_inbook_title",
                "prism.publicationName",
            ],
        )
        or _extract_venue_from_jsonld(jsonld_blocks)
        or ""
    )
    signals["abstract_note"] = (
        _extract_meta_content(html, ["citation_abstract", "dc.description", "description", "og:description"])
        or _extract_description_from_jsonld(jsonld_blocks)
        or _extract_abstract_from_html_body(html)
        or signals["description"]
    )
    signals["doi"] = (
        _normalize_doi(_extract_meta_content(html, ["citation_doi", "dc.identifier", "dc.identifier.doi", "prism.doi"]))
        or _extract_doi_from_jsonld(jsonld_blocks)
    )
    meta_authors = _extract_meta_contents(html, ["citation_author", "dc.creator", "author"])
    if meta_authors:
        signals["creators"] = [_parse_creator_name(author) for author in meta_authors]
    else:
        signals["creators"] = _extract_creators_from_jsonld(jsonld_blocks)
    signals["date"] = (
        _extract_meta_content(html, ["citation_publication_date", "citation_date", "dc.date", "prism.publicationDate"])
        or _extract_date_from_jsonld(jsonld_blocks)
        or ""
    )
    signals["arxiv_id"] = (
        _normalize_arxiv_id(_extract_meta_content(html, ["citation_arxiv_id"]))
        or _normalize_arxiv_id(signals["final_url"])
        or _normalize_arxiv_id(url)
    )
    signals["pdf_candidates"] = _extract_pdf_link_from_html(html, signals["final_url"])

    if signals["arxiv_id"]:
        signals["pdf_candidates"].append(
            {
                "source": "arxiv_pdf",
                "url": f"https://arxiv.org/pdf/{signals['arxiv_id']}.pdf",
            }
        )

    signals["pdf_candidates"].extend(_infer_pdf_candidates_from_url(signals["final_url"]))
    signals["pdf_candidates"].extend(_infer_pdf_candidates_from_url(url))
    signals["pdf_candidates"] = _dedupe_pdf_candidates(signals["pdf_candidates"])
    return signals


def _item_has_pdf_attachment(zot, item_key: str) -> bool:
    try:
        children = zot.children(item_key)
    except Exception:
        return False
    for child in children or []:
        data = child.get("data", {})
        if data.get("itemType") == "attachment" and data.get("contentType") == "application/pdf":
            return True
    return False


def _iter_pdf_attachments(zot, item_key: str) -> list[dict[str, Any]]:
    try:
        children = zot.children(item_key)
    except Exception:
        return []

    pdf_children: list[dict[str, Any]] = []
    for child in children or []:
        data = child.get("data", {})
        if data.get("itemType") != "attachment":
            continue
        if data.get("contentType") != "application/pdf":
            continue
        pdf_children.append(child)
    return pdf_children


def _resolve_local_attachment_path(attachment_key: str) -> Path | None:
    try:
        from zotero_mcp.local_db import LocalZoteroReader

        reader = LocalZoteroReader()
        conn = sqlite3.connect(f"file:{reader.db_path}?immutable=1", uri=True)
        try:
            row = conn.execute(
                """
                SELECT ia.path
                FROM itemAttachments ia
                JOIN items i ON i.itemID = ia.itemID
                WHERE i.key = ?
                """,
                (attachment_key,),
            ).fetchone()
        finally:
            conn.close()
    except Exception:
        return None

    if not row:
        return None

    raw_path = row[0]
    if not raw_path:
        return None

    if raw_path.startswith("storage:"):
        rel_path = raw_path.split(":", 1)[1].lstrip("/")
        return Path(reader.db_path).parent / "storage" / attachment_key / rel_path

    path = Path(raw_path)
    if path.is_absolute():
        return path
    return None


def _attachment_file_exists_locally(attachment_key: str) -> bool:
    path = _resolve_local_attachment_path(attachment_key)
    if path is None or not path.exists():
        return False
    try:
        return path.stat().st_size > 0
    except OSError:
        return False


def _item_has_materialized_local_pdf_attachment(item_key: str) -> bool:
    local_zot = get_local_zotero_client()
    if local_zot is None:
        return False

    for child in _iter_pdf_attachments(local_zot, item_key):
        data = child.get("data", {})
        attachment_key = child.get("key") or data.get("key")
        if attachment_key and _attachment_file_exists_locally(attachment_key):
            return True
    return False


def _item_has_usable_pdf_attachment(item_key: str, *, zot=None) -> bool:
    local_zot = get_local_zotero_client()
    if local_zot is not None:
        local_children = _iter_pdf_attachments(local_zot, item_key)
        if local_children:
            for child in local_children:
                data = child.get("data", {})
                attachment_key = child.get("key") or data.get("key")
                if attachment_key and _attachment_file_exists_locally(attachment_key):
                    return True
            # If we cannot resolve any local storage paths at all, fall back to
            # metadata presence. This keeps fake/local-test clients working.
            if all(
                _resolve_local_attachment_path(child.get("key") or child.get("data", {}).get("key") or "") is None
                for child in local_children
            ):
                return True
            return False
    if zot is not None:
        return _item_has_pdf_attachment(zot, item_key)
    return False


def _should_prefer_local_connector_pdf_copy(zot) -> bool:
    local_zot = get_local_zotero_client()
    if local_zot is None:
        return False
    mode = os.environ.get("ZOTERO_MCP_LOCAL_PDF_MODE", "prefer").strip().lower()
    if mode in {"off", "disabled", "never", "web"}:
        return False
    if mode in {"prefer", "always", "local"}:
        return True
    if bool(getattr(zot, "local", False)):
        return True
    return False


def _should_prefer_local_pdf_after_download(
    zot,
    *,
    item_payload: dict[str, Any] | None,
    pdf_size_bytes: int,
) -> bool:
    local_zot = get_local_zotero_client()
    if local_zot is None:
        return False
    if _should_prefer_local_connector_pdf_copy(zot):
        return True
    if item_payload is None:
        return True
    if bool(getattr(zot, "local", False)):
        return True
    threshold_mb = float(os.environ.get("ZOTERO_MCP_LOCAL_PDF_THRESHOLD_MB", "10"))
    return pdf_size_bytes >= int(threshold_mb * 1024 * 1024)


def _should_try_local_pdf_fallback(exc: Exception) -> bool:
    text = str(exc).lower()
    return "413" in text or "quota" in text or "file would exceed quota" in text


def _connector_url_fastpath_host(pdf_url: str) -> str | None:
    try:
        host = urlparse(pdf_url).netloc.strip().lower()
    except Exception:
        return None
    return host or None


def _is_timeout_like_exception(exc: Exception) -> bool:
    if isinstance(exc, (requests.Timeout, requests.ReadTimeout)):
        return True
    text = str(exc).lower()
    return "timed out" in text or "read timed out" in text or "timeout" in text


def _should_try_connector_url_fastpath(pdf_url: str) -> bool:
    host = _connector_url_fastpath_host(pdf_url)
    if host is None:
        return True
    return host not in CONNECTOR_URL_FASTPATH_DISABLED_HOSTS


def _remember_connector_url_fastpath_timeout(pdf_url: str, exc: Exception) -> None:
    if not _is_timeout_like_exception(exc):
        return
    host = _connector_url_fastpath_host(pdf_url)
    if host:
        CONNECTOR_URL_FASTPATH_DISABLED_HOSTS.add(host)


def _connector_url_attach_timeout_seconds() -> float:
    raw_value = os.environ.get("ZOTERO_MCP_CONNECTOR_URL_ATTACH_TIMEOUT_SEC", "").strip()
    if raw_value:
        try:
            timeout_seconds = float(raw_value)
        except ValueError:
            timeout_seconds = 30.0
    else:
        try:
            general_timeout = float(
                os.environ.get("ZOTERO_MCP_CONNECTOR_ATTACH_TIMEOUT_SEC", "45")
            )
        except ValueError:
            general_timeout = 45.0
        timeout_seconds = min(general_timeout, 30.0)
    return max(3.0, min(timeout_seconds, 60.0))


def _connector_zero_byte_url_attach_enabled() -> bool:
    value = os.environ.get("ZOTERO_MCP_ENABLE_CONNECTOR_URL_ATTACH", "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _confirm_local_pdf_attachment_materialized(
    item_key: str,
    *,
    ctx: Context,
    wait_seconds: float = 20.0,
    poll_interval: float = 1.0,
) -> dict[str, Any]:
    import tempfile

    local_zot = get_local_zotero_client()
    if local_zot is None:
        return {
            "success": False,
            "pdf_source": "local_zotero",
            "message": "local Zotero is not running or local API is unavailable",
        }

    deadline = time.time() + max(wait_seconds, 0.0)
    last_error = "unknown error"
    last_reported_error = ""
    attempt = 0
    while True:
        attempt += 1
        pdf_children = _iter_pdf_attachments(local_zot, item_key)
        if not pdf_children:
            last_error = "no local PDF attachment placeholder found after web upload failure"
        for child in pdf_children:
            data = child.get("data", {})
            attachment_key = child.get("key") or data.get("key")
            if not attachment_key:
                continue

            if _attachment_file_exists_locally(attachment_key):
                return {
                    "success": True,
                    "pdf_source": "local_zotero",
                    "message": (
                        "PDF is available in local Zotero storage; "
                        "cloud upload was skipped (likely quota-limited)"
                    ),
                    "attachment_key": attachment_key,
                }

            filename = data.get("filename") or f"{attachment_key}.pdf"
            resolved_path = _resolve_local_attachment_path(attachment_key)
            if resolved_path is not None and not resolved_path.exists():
                last_error = (
                    "local PDF attachment placeholder exists but file has not materialized yet"
                )
                continue
            with tempfile.TemporaryDirectory() as tmpdir:
                probe_path = Path(tmpdir) / filename
                try:
                    dump_attachment_to_file(local_zot, attachment_key, probe_path, ctx=ctx)
                    if probe_path.exists() and probe_path.stat().st_size > 0:
                        return {
                            "success": True,
                            "pdf_source": "local_zotero",
                            "message": (
                                "PDF is available in local Zotero storage; "
                                "cloud upload was skipped (likely quota-limited)"
                            ),
                            "attachment_key": attachment_key,
                        }
                    last_error = "local attachment probe produced an empty file"
                except Exception as exc:
                    last_error = str(exc)

        if time.time() >= deadline:
            break
        should_log = attempt == 1 or last_error != last_reported_error or attempt % 5 == 0
        if should_log and pdf_children:
            ctx.info(
                f"Waiting for local PDF attachment to materialize for `{item_key}` "
                f"(attempt {attempt}, last_error={last_error})"
            )
            last_reported_error = last_error
        time.sleep(max(poll_interval, 0.1))

    return {
        "success": False,
        "pdf_source": "local_zotero",
        "message": f"local attachment probe failed: {last_error}",
    }


def _sanitize_item_for_local_connector(item_data: dict[str, Any]) -> dict[str, Any]:
    blocked = {
        "key",
        "version",
        "collections",
        "relations",
        "dateAdded",
        "dateModified",
        "parentItem",
    }
    payload = {k: v for k, v in item_data.items() if k not in blocked}
    payload["attachments"] = []
    payload["notes"] = []
    payload["complete"] = True
    payload["id"] = "item-1"
    return payload


def _connector_save_items_payload(
    *,
    session_id: str,
    items: list[dict[str, Any]],
    base_uri: str | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "sessionID": session_id,
        "items": items,
        "uri": str(base_uri or "").strip() or "about:blank",
    }
    return payload


def _connector_get_selected_collection_payload(*, timeout: float = 10.0) -> dict[str, Any] | None:
    try:
        resp = requests.post(
            "http://127.0.0.1:23119/connector/getSelectedCollection",
            json={},
            timeout=timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _connector_context_candidate_item_key(preferred_item_key: str | None = None) -> str | None:
    if preferred_item_key:
        return preferred_item_key
    try:
        conn = sqlite3.connect(f"file:{_local_zotero_db_path()}?immutable=1", uri=True)
        try:
            row = conn.execute(
                """
                SELECT items.key
                FROM items
                JOIN itemTypes USING(itemTypeID)
                LEFT JOIN deletedItems USING(itemID)
                WHERE items.libraryID = 1
                  AND deletedItems.itemID IS NULL
                  AND itemTypes.typeName NOT IN ('attachment', 'annotation', 'note')
                ORDER BY items.dateModified DESC
                LIMIT 1
                """
            ).fetchone()
        finally:
            conn.close()
    except Exception:
        return None
    return str(row[0]).strip() if row and row[0] else None


def _ensure_connector_library_context(
    *,
    preferred_item_key: str | None = None,
    ctx: Context | None = None,
) -> bool:
    if _connector_get_selected_collection_payload(timeout=5.0) is not None:
        return True

    candidate_item_key = _connector_context_candidate_item_key(preferred_item_key)
    if not candidate_item_key:
        return False

    uri = f"zotero://select/library/items/{candidate_item_key}"
    if ctx is not None:
        _ctx_warning(
            ctx,
            "Connector library context was unavailable; reopening Zotero on a regular library item",
        )
    try:
        subprocess.run(
            ["open", uri],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception as exc:
        if ctx is not None:
            _ctx_warning(ctx, f"Failed to reopen Zotero library context via URI {uri}: {exc}")
        return False

    time.sleep(2.0)
    return _connector_get_selected_collection_payload(timeout=8.0) is not None


def _item_matches_metadata(
    item_data: dict[str, Any],
    *,
    title: str,
    item_type: str,
    doi: str | None,
    url: str | None,
) -> bool:
    if item_data.get("itemType") != item_type:
        return False
    if item_data.get("title") != title:
        return False
    if doi and (item_data.get("DOI") or "").strip() != doi.strip():
        return False
    if url and (item_data.get("url") or "").strip() != url.strip():
        return False
    return True


def _collection_items_safe(zot, collection_key: str) -> list[dict[str, Any]]:
    if not hasattr(zot, "collection_items"):
        return []
    try:
        return zot.collection_items(collection_key) or []
    except Exception:
        return []


def _collection_data_safe(zot, collection_key: str) -> dict[str, Any] | None:
    try:
        collection = zot.collection(collection_key)
    except Exception:
        return None
    data = collection.get("data", collection if isinstance(collection, dict) else {})
    if data.get("deleted"):
        return None
    return data


def _collection_path(zot, collection_key: str | None) -> list[str]:
    if not collection_key:
        return []

    path: list[str] = []
    current_key = collection_key
    seen: set[str] = set()
    while current_key and current_key not in seen:
        seen.add(current_key)
        data = _collection_data_safe(zot, current_key)
        if not data:
            return []
        path.append(data.get("name") or current_key)
        parent_key = data.get("parentCollection")
        if not parent_key:
            break
        current_key = parent_key
    return list(reversed(path))


def _collection_label(zot, collection_key: str | None) -> str | None:
    path = _collection_path(zot, collection_key)
    if path:
        return " / ".join(path)
    return collection_key


def _all_collections_safe(zot) -> list[dict[str, Any]]:
    if not hasattr(zot, "collections"):
        return []
    try:
        return zot.collections() or []
    except Exception:
        return []


def _collection_descendant_keys(
    zot,
    collection_key: str | None,
    *,
    include_subcollections: bool = True,
) -> list[str]:
    if not collection_key:
        return []

    resolved_root = _resolve_connector_collection_key(collection_key) or collection_key
    if not include_subcollections:
        return [resolved_root]

    collections = _all_collections_safe(zot)
    if not collections:
        return [resolved_root]

    children_by_parent: dict[str | None, list[str]] = {}
    for coll in collections:
        data = coll.get("data", coll if isinstance(coll, dict) else {})
        key = data.get("key")
        if not key:
            continue
        parent_key = data.get("parentCollection") or None
        children_by_parent.setdefault(parent_key, []).append(key)

    result: list[str] = []
    queue: list[str] = [resolved_root]
    seen: set[str] = set()
    while queue:
        key = queue.pop(0)
        if key in seen:
            continue
        seen.add(key)
        result.append(key)
        queue.extend(children_by_parent.get(key, []))
    return result


def _coerce_item_data(item: dict[str, Any]) -> dict[str, Any]:
    return item.get("data", item if isinstance(item, dict) else {}) if isinstance(item, dict) else {}


def _collection_duplicate_group_key(item_data: dict[str, Any]) -> str | None:
    doi = _normalize_doi(item_data.get("DOI"))
    if doi:
        return f"doi:{doi}"
    arxiv_id = _item_arxiv_id_from_data(item_data)
    if arxiv_id:
        return f"arxiv:{arxiv_id}"
    title = _normalize_title_for_match(item_data.get("title"))
    if title:
        return f"title:{title}"
    return None


def _metadata_richness_score(item_data: dict[str, Any]) -> int:
    score = 0
    fields = [
        "title",
        "DOI",
        "url",
        "abstractNote",
        "date",
        "publicationTitle",
        "proceedingsTitle",
        "conferenceName",
        "archiveID",
        "repository",
    ]
    for field in fields:
        if _normalize_text(item_data.get(field)):
            score += 4
    creators = item_data.get("creators") or []
    if creators:
        score += min(len(creators), 5) * 2
    tags = item_data.get("tags") or []
    if tags:
        score += min(len(tags), 3)
    score += {
        "journalArticle": 8,
        "conferencePaper": 8,
        "preprint": 7,
        "bookSection": 6,
        "webpage": 2,
    }.get(str(item_data.get("itemType") or ""), 0)
    return score


def _choose_collection_duplicate_canonical(
    zot,
    items: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    ranked: list[tuple[int, int, str, dict[str, Any]]] = []
    for item in items:
        data = _coerce_item_data(item)
        key = str(data.get("key") or item.get("key") or "")
        pdf_score = 100 if _item_has_usable_pdf_attachment(key, zot=zot) else 0
        metadata_score = _metadata_richness_score(data)
        ranked.append((pdf_score, metadata_score, key, item))
    ranked.sort(reverse=True, key=lambda entry: (entry[0], entry[1], entry[2]))
    canonical = ranked[0][3]
    duplicates = [entry[3] for entry in ranked[1:]]
    return canonical, duplicates


def _record_collection_dedupe_event(
    *,
    status: str,
    input_value: str,
    label: str | None,
    canonical_key: str | None,
    duplicate_key: str | None,
    pdf_source: str,
    collection_key: str | None,
    collection_label: str | None,
    message: str | None,
    error: str | None,
    ctx: Context,
) -> None:
    _record_import_event(
        action="reconcile",
        status=status,
        input_value=input_value,
        route="collection_dedupe",
        label=label,
        item_key=canonical_key,
        local_item_key=duplicate_key,
        pdf_source=pdf_source,
        fallback_reason="none",
        collection_key=collection_key,
        collection_path=collection_label,
        intended_target=collection_label,
        reconcile_status=status,
        reconcile_message=message,
        message=message,
        error=error,
        ctx=ctx,
    )


def _local_zotero_db_path() -> Path:
    from zotero_mcp.local_db import LocalZoteroReader

    return Path(LocalZoteroReader().db_path)


def _is_zotero_process_running() -> bool:
    result = subprocess.run(
        ["pgrep", "-x", "zotero"],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def _wait_for_zotero_process(*, running: bool, timeout_seconds: float = 20.0) -> bool:
    deadline = time.time() + max(timeout_seconds, 0.0)
    while time.time() < deadline:
        if _is_zotero_process_running() == running:
            return True
        time.sleep(0.5)
    return _is_zotero_process_running() == running


def _quit_zotero_app() -> None:
    subprocess.run(
        ["osascript", "-e", 'tell application "Zotero" to quit'],
        check=False,
        capture_output=True,
        text=True,
    )


def _open_zotero_app() -> None:
    subprocess.run(
        ["open", "-a", "/Applications/Zotero.app"],
        check=False,
        capture_output=True,
        text=True,
    )


def _backup_local_zotero_db_files(*, ctx: Context) -> Path:
    db_path = _local_zotero_db_path()
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_dir = db_path.parent / "codex-backups" / f"zotero-local-trash-fix-{stamp}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        src = db_path.parent / f"{db_path.name}{suffix}"
        if src.exists():
            shutil.copy2(src, backup_dir / src.name)
    ctx.info(f"Backed up Zotero DB to `{backup_dir}`")
    return backup_dir


def _mark_local_items_deleted_via_db(
    item_keys: list[str],
    *,
    restart_zotero: bool,
    ctx: Context,
) -> dict[str, Any]:
    if not item_keys:
        return {"success": True, "deleted": [], "backup_dir": None}

    db_path = _local_zotero_db_path()
    was_running = _is_zotero_process_running()
    restarted = False
    if was_running and restart_zotero:
        ctx.info("Temporarily quitting Zotero to apply local deletedItems fallback")
        _quit_zotero_app()
        if not _wait_for_zotero_process(running=False, timeout_seconds=20.0):
            raise RuntimeError("Failed to stop Zotero before local DB fallback")
        restarted = True

    backup_dir = _backup_local_zotero_db_files(ctx=ctx)
    deleted_keys: list[str] = []
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        for item_key in item_keys:
            row = cur.execute(
                "SELECT itemID FROM items WHERE key = ?",
                (item_key,),
            ).fetchone()
            if not row:
                continue
            item_id = row[0]
            cur.execute(
                "INSERT OR IGNORE INTO deletedItems (itemID) VALUES (?)",
                (item_id,),
            )
            deleted_keys.append(item_key)
        conn.commit()
    finally:
        conn.close()

    if restarted:
        _open_zotero_app()
        _wait_for_zotero_process(running=True, timeout_seconds=20.0)

    return {
        "success": True,
        "deleted": deleted_keys,
        "backup_dir": str(backup_dir),
    }


def _collection_items_payload_map(
    zot,
    *,
    collection_key: str,
    include_subcollections: bool,
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    scope_keys = _collection_descendant_keys(
        zot,
        collection_key,
        include_subcollections=include_subcollections,
    )
    items_by_key: dict[str, dict[str, Any]] = {}
    for scope_key in scope_keys:
        for item in _collection_items_safe(zot, scope_key):
            data = _coerce_item_data(item)
            item_type = str(data.get("itemType") or "")
            if item_type in {"attachment", "note", "annotation"}:
                continue
            item_key = data.get("key") or item.get("key")
            if not item_key:
                continue
            payload = _get_item_payload(zot, item_key) or {"key": item_key, "data": data}
            items_by_key[str(item_key)] = payload
    return scope_keys, items_by_key


def _duplicate_groups_from_items(
    items_by_key: dict[str, dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for payload in items_by_key.values():
        group_key = _collection_duplicate_group_key(_coerce_item_data(payload))
        if not group_key:
            continue
        grouped.setdefault(group_key, []).append(payload)
    return {
        group_key: items for group_key, items in grouped.items() if len(items) > 1
    }


def _delete_local_duplicate_item(
    local_zot,
    duplicate: dict[str, Any],
    *,
    local_db_fallback: bool,
    ctx: Context,
) -> dict[str, Any]:
    duplicate_data = _coerce_item_data(duplicate)
    duplicate_key = str(duplicate_data.get("key") or duplicate.get("key") or "")
    if not duplicate_key:
        return {"success": False, "deleted": [], "message": "missing duplicate key"}

    try:
        local_zot.delete_item(_get_item_payload(local_zot, duplicate_key) or duplicate)
        return {"success": True, "deleted": [duplicate_key], "message": "deleted via local API"}
    except Exception as delete_exc:
        try:
            local_zot.trash(duplicate_key)
            return {"success": True, "deleted": [duplicate_key], "message": "trashed via local API"}
        except Exception as trash_exc:
            if not local_db_fallback:
                return {
                    "success": False,
                    "deleted": [],
                    "message": f"local delete failed: {delete_exc}; trash failed: {trash_exc}",
                }
            db_result = _mark_local_items_deleted_via_db(
                [duplicate_key],
                restart_zotero=True,
                ctx=ctx,
            )
            return {
                "success": bool(db_result.get("deleted")),
                "deleted": db_result.get("deleted", []),
                "message": (
                    f"marked deleted via local DB fallback; backup={db_result.get('backup_dir')}"
                    if db_result.get("deleted")
                    else "local DB fallback did not mark item deleted"
                ),
            }


def _reconcile_local_collection_duplicates_impl(
    *,
    local_zot,
    web_zot,
    collection_key: str,
    include_subcollections: bool,
    dry_run: bool,
    local_db_fallback: bool,
    ctx: Context,
) -> tuple[str, int]:
    scope_keys, items_by_key = _collection_items_payload_map(
        local_zot,
        collection_key=collection_key,
        include_subcollections=include_subcollections,
    )
    if not scope_keys:
        return "", 0

    duplicate_groups = _duplicate_groups_from_items(items_by_key)
    if not duplicate_groups:
        return "", 0

    lines = [
        "",
        "Local dedupe summary",
        f"- collection: {_collection_label(local_zot, collection_key) or collection_key}",
        f"- local_db_fallback: {'yes' if local_db_fallback else 'no'}",
        f"- duplicate groups: {len(duplicate_groups)}",
        "",
        "| Match | Canonical | PDF | Local duplicates | Action |",
        "|---|---|---:|---|---|",
    ]

    local_trash_count = 0
    for group_key, items in sorted(duplicate_groups.items()):
        canonical, duplicates = _choose_collection_duplicate_canonical(local_zot, items)
        canonical_data = _coerce_item_data(canonical)
        canonical_key = str(canonical_data.get("key") or canonical.get("key") or "")
        canonical_label = canonical_data.get("title") or canonical_key or group_key
        canonical_pdf = "yes" if _item_has_usable_pdf_attachment(canonical_key, zot=local_zot) else "no"
        action_bits: list[str] = []
        duplicate_keys: list[str] = []

        if not dry_run:
            canonical_payload = _get_item_payload(local_zot, canonical_key) or canonical
            canonical_collections = set(canonical_data.get("collections") or [])
            for duplicate in duplicates:
                duplicate_data = _coerce_item_data(duplicate)
                duplicate_key = str(duplicate_data.get("key") or duplicate.get("key") or "")
                duplicate_keys.append(duplicate_key)
                duplicate_collections = duplicate_data.get("collections") or []
                missing_collections = [
                    coll for coll in duplicate_collections if coll not in canonical_collections
                ]
                for target_collection in missing_collections:
                    try:
                        (web_zot or local_zot).addto_collection(target_collection, canonical_payload)
                        canonical_collections.add(target_collection)
                    except Exception:
                        pass
                delete_result = _delete_local_duplicate_item(
                    local_zot,
                    duplicate,
                    local_db_fallback=local_db_fallback,
                    ctx=ctx,
                )
                if delete_result.get("success"):
                    local_trash_count += len(delete_result.get("deleted", []))
                    action_bits.append(delete_result.get("message", "deleted"))
                    _record_collection_dedupe_event(
                        status="success",
                        input_value=group_key,
                        label=duplicate_data.get("title") or duplicate_key,
                        canonical_key=canonical_key or None,
                        duplicate_key=duplicate_key or None,
                        pdf_source="existing_attachment" if canonical_pdf == "yes" else "none",
                        collection_key=collection_key,
                        collection_label=_collection_label(local_zot, collection_key),
                        message=delete_result.get("message"),
                        error=None,
                        ctx=ctx,
                    )
                else:
                    action_bits.append(delete_result.get("message", "delete_failed"))
                    _record_collection_dedupe_event(
                        status="error",
                        input_value=group_key,
                        label=duplicate_data.get("title") or duplicate_key,
                        canonical_key=canonical_key or None,
                        duplicate_key=duplicate_key or None,
                        pdf_source="existing_attachment" if canonical_pdf == "yes" else "none",
                        collection_key=collection_key,
                        collection_label=_collection_label(local_zot, collection_key),
                        message=None,
                        error=delete_result.get("message"),
                        ctx=ctx,
                    )
        else:
            duplicate_keys = [
                str(_coerce_item_data(duplicate).get("key") or duplicate.get("key") or "?")
                for duplicate in duplicates
            ]
            action_bits.append(f"trash {len(duplicates)} local duplicate(s)")
            local_trash_count += len(duplicates)

        lines.append(
            "| "
            + " | ".join(
                [
                    group_key.replace("|", "/"),
                    f"{canonical_label} ({canonical_key})".replace("|", "/"),
                    canonical_pdf,
                    ", ".join(duplicate_keys) if duplicate_keys else "-",
                    "; ".join(action_bits) if action_bits else "keep",
                ]
            )
            + " |"
        )

    return "\n".join(lines), local_trash_count


def _reconcile_collection_duplicates_impl(
    *,
    zot,
    collection_key: str,
    include_subcollections: bool,
    dry_run: bool,
    ctx: Context,
) -> str:
    scope_keys, items_by_key = _collection_items_payload_map(
        zot,
        collection_key=collection_key,
        include_subcollections=include_subcollections,
    )
    if not scope_keys:
        return f"Error: collection `{collection_key}` not found."
    duplicate_groups = _duplicate_groups_from_items(items_by_key)
    if not duplicate_groups:
        return (
            "Collection dedupe summary\n"
            f"- collection: {_collection_label(zot, collection_key) or collection_key}\n"
            "- duplicate groups: 0\n"
            "- canonical items kept: 0\n"
            "- duplicates trashed: 0"
        )

    lines = [
        "Collection dedupe summary",
        f"- collection: {_collection_label(zot, collection_key) or collection_key}",
        f"- include_subcollections: {'yes' if include_subcollections else 'no'}",
        f"- dry_run: {'yes' if dry_run else 'no'}",
        f"- duplicate groups: {len(duplicate_groups)}",
        "",
        "| Match | Canonical | PDF | Duplicates | Collections merged | Action |",
        "|---|---|---:|---|---|---|",
    ]

    kept_count = 0
    trashed_count = 0
    merged_collection_count = 0
    for group_key, items in sorted(duplicate_groups.items()):
        canonical, duplicates = _choose_collection_duplicate_canonical(zot, items)
        canonical_data = _coerce_item_data(canonical)
        canonical_key = canonical_data.get("key") or canonical.get("key")
        canonical_label = canonical_data.get("title") or canonical_key or group_key
        canonical_pdf = "yes" if _item_has_usable_pdf_attachment(str(canonical_key), zot=zot) else "no"

        target_collections = set(canonical_data.get("collections") or [])
        for duplicate in duplicates:
            duplicate_data = _coerce_item_data(duplicate)
            target_collections.update(duplicate_data.get("collections") or [])

        missing_collections = sorted(
            collection for collection in target_collections
            if collection not in set(canonical_data.get("collections") or [])
        )

        action_bits: list[str] = []
        if missing_collections:
            action_bits.append(f"merge {len(missing_collections)} collection(s)")
        if duplicates:
            action_bits.append(f"trash {len(duplicates)} duplicate(s)")

        if not dry_run:
            if missing_collections:
                canonical_payload = _get_item_payload(zot, str(canonical_key)) or canonical
                for target_collection in missing_collections:
                    zot.addto_collection(target_collection, canonical_payload)
                merged_collection_count += len(missing_collections)
            for duplicate in duplicates:
                duplicate_data = _coerce_item_data(duplicate)
                duplicate_key = duplicate_data.get("key") or duplicate.get("key")
                duplicate_label = duplicate_data.get("title") or duplicate_key or group_key
                try:
                    zot.delete_item(_get_item_payload(zot, str(duplicate_key)) or duplicate)
                    trashed_count += 1
                    _record_collection_dedupe_event(
                        status="success",
                        input_value=group_key,
                        label=duplicate_label,
                        canonical_key=str(canonical_key) if canonical_key else None,
                        duplicate_key=str(duplicate_key) if duplicate_key else None,
                        pdf_source="existing_attachment" if canonical_pdf == "yes" else "none",
                        collection_key=collection_key,
                        collection_label=_collection_label(zot, collection_key),
                        message=(
                            f"canonical={canonical_key}; duplicate={duplicate_key}; "
                            f"merged_collections={','.join(missing_collections) or '-'}"
                        ),
                        error=None,
                        ctx=ctx,
                    )
                except Exception as exc:
                    action_bits.append(f"delete_failed:{duplicate_key}")
                    _record_collection_dedupe_event(
                        status="error",
                        input_value=group_key,
                        label=duplicate_label,
                        canonical_key=str(canonical_key) if canonical_key else None,
                        duplicate_key=str(duplicate_key) if duplicate_key else None,
                        pdf_source="existing_attachment" if canonical_pdf == "yes" else "none",
                        collection_key=collection_key,
                        collection_label=_collection_label(zot, collection_key),
                        message=None,
                        error=str(exc),
                        ctx=ctx,
                    )
            kept_count += 1
        else:
            merged_collection_count += len(missing_collections)
            trashed_count += len(duplicates)
            kept_count += 1

        duplicate_keys = [
            str(_coerce_item_data(duplicate).get("key") or duplicate.get("key") or "?")
            for duplicate in duplicates
        ]
        lines.append(
            "| "
            + " | ".join(
                [
                    group_key.replace("|", "/"),
                    f"{canonical_label} ({canonical_key})".replace("|", "/"),
                    canonical_pdf,
                    ", ".join(duplicate_keys) if duplicate_keys else "-",
                    ", ".join(missing_collections) if missing_collections else "-",
                    "; ".join(action_bits) if action_bits else "keep",
                ]
            )
            + " |"
        )

    lines.insert(5, f"- canonical items kept: {kept_count}")
    lines.insert(6, f"- duplicates trashed: {trashed_count}")
    lines.insert(7, f"- collection memberships merged: {merged_collection_count}")
    return "\n".join(lines)


def _repair_missing_pdfs_in_collection_impl(
    *,
    zot,
    collection_key: str,
    include_subcollections: bool,
    ctx: Context,
) -> str:
    _, items_by_key = _collection_items_payload_map(
        zot,
        collection_key=collection_key,
        include_subcollections=include_subcollections,
    )
    if not items_by_key:
        return (
            "Missing PDF postpass\n"
            f"- collection: {_collection_label(zot, collection_key) or collection_key}\n"
            "- scanned_without_pdf: 0\n"
            "- repaired: 0\n"
            "- failed: 0"
        )

    scanned_without_pdf = 0
    repaired = 0
    failed = 0
    lines = [
        "Missing PDF postpass",
        f"- collection: {_collection_label(zot, collection_key) or collection_key}",
    ]

    for item_key, payload in items_by_key.items():
        data = _coerce_item_data(payload)
        if _item_has_usable_pdf_attachment(item_key, zot=zot):
            continue

        doi = str(data.get("DOI") or "").strip()
        url = str(data.get("url") or "").strip()
        if not doi and not url:
            continue

        scanned_without_pdf += 1
        pdf_candidates: list[dict[str, str]] = []
        crossref_work: dict[str, Any] | None = None

        if doi:
            try:
                crossref_work = _fetch_crossref_work(doi)
                pdf_candidates.extend(
                    _discover_pdf_candidates_from_crossref_work(
                        crossref_work,
                        doi=doi,
                        ctx=ctx,
                    )
                )
            except Exception as exc:
                _ctx_warning(ctx, f"Collection PDF postpass Crossref lookup failed for {item_key}: {exc}")

        if url:
            try:
                page_signals = _fetch_page_signals(url, ctx=ctx)
                pdf_candidates.extend(page_signals.get("pdf_candidates") or [])
            except Exception as exc:
                _ctx_warning(ctx, f"Collection PDF postpass page inspection failed for {item_key}: {exc}")

        result = _attach_pdf_with_cascade(
            zot,
            item_key,
            pdf_candidates=_dedupe_pdf_candidates(pdf_candidates),
            doi=doi or None,
            crossref_work=crossref_work,
            collection_key=(data.get("collections") or [None])[0],
            ctx=ctx,
        )
        if result.get("success"):
            repaired += 1
        else:
            failed += 1

    lines.append(f"- scanned_without_pdf: {scanned_without_pdf}")
    lines.append(f"- repaired: {repaired}")
    lines.append(f"- failed: {failed}")
    return "\n".join(lines)


def _resolve_connector_collection_key(collection_id: Any) -> str | None:
    if collection_id is None:
        return None

    if isinstance(collection_id, str):
        raw_value = collection_id.strip()
        if not raw_value:
            return None
        if re.fullmatch(r"[A-Z0-9]{8}", raw_value):
            return raw_value
        if raw_value.startswith("C") and raw_value[1:].isdigit():
            collection_id = int(raw_value[1:])
        elif raw_value.isdigit():
            collection_id = int(raw_value)
        else:
            return raw_value

    if not isinstance(collection_id, int):
        return None

    try:
        from zotero_mcp.local_db import LocalZoteroReader

        reader = LocalZoteroReader()
        conn = sqlite3.connect(f"file:{reader.db_path}?immutable=1", uri=True)
        try:
            row = conn.execute(
                "SELECT key FROM collections WHERE collectionID = ?",
                (collection_id,),
            ).fetchone()
        finally:
            conn.close()
    except Exception:
        return None

    if not row:
        return None
    return row[0]


def _connector_target_snapshot(
    *,
    preferred_item_key: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    payload = _connector_get_selected_collection_payload(timeout=10.0)
    if payload is None and _ensure_connector_library_context(
        preferred_item_key=preferred_item_key,
        ctx=ctx,
    ):
        payload = _connector_get_selected_collection_payload(timeout=10.0)
    if payload is None:
        return {}

    targets = payload.get("targets") or []
    stack: list[str] = []
    target_paths: dict[str, list[str]] = {}
    for target in targets:
        level = int(target.get("level", 0))
        stack = stack[:level]
        stack.append(target.get("name") or "")
        target_id = target.get("id")
        if target_id:
            target_paths[target_id] = stack.copy()

    raw_collection_id = payload.get("id")
    connector_target_id = (
        f"C{raw_collection_id}" if isinstance(raw_collection_id, int) else raw_collection_id
    )
    current_path = target_paths.get(connector_target_id) or []
    current_collection_key = _resolve_connector_collection_key(raw_collection_id)
    current_label = " / ".join(current_path) if current_path else (
        payload.get("name") or payload.get("libraryName")
    )

    return {
        "library_name": payload.get("libraryName"),
        "current_name": current_label,
        "current_collection_id": current_collection_key or raw_collection_id,
        "current_connector_target_id": connector_target_id,
        "current_collection_key": current_collection_key,
        "current_path": current_path,
        "target_paths": target_paths,
    }


def _get_item_payload(zot, item_key: str) -> dict[str, Any] | None:
    try:
        payload = zot.item(item_key)
    except Exception:
        return None

    if not isinstance(payload, dict):
        return None

    if "data" in payload and isinstance(payload["data"], dict):
        payload.setdefault("key", payload["data"].get("key", item_key))
        payload.setdefault("version", payload["data"].get("version", payload.get("version", 0)))
        return payload

    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if not isinstance(data, dict):
        return None
    return {
        "key": data.get("key", item_key),
        "version": payload.get("version", data.get("version", 0)),
        "data": data,
    }


def _find_local_item_by_metadata(
    *,
    title: str,
    item_type: str,
    doi: str | None,
    url: str | None,
    collection_key: str | None = None,
    require_pdf: bool = False,
) -> dict[str, Any] | None:
    local_zot = get_local_zotero_client()
    if local_zot is None:
        return None
    if not hasattr(local_zot, "items"):
        return None

    if collection_key:
        for item in _collection_items_safe(local_zot, collection_key):
            data = item.get("data", item if isinstance(item, dict) else {})
            if not _item_matches_metadata(
                data,
                title=title,
                item_type=item_type,
                doi=doi,
                url=url,
            ):
                continue
            if require_pdf and not _item_has_usable_pdf_attachment(data.get("key", ""), zot=local_zot):
                continue
            return data

    for item in local_zot.items(limit=25, sort="dateAdded", direction="desc") or []:
        data = item.get("data", {})
        if not _item_matches_metadata(
            data,
            title=title,
            item_type=item_type,
            doi=doi,
            url=url,
        ):
            continue
        if require_pdf and not _item_has_usable_pdf_attachment(data.get("key", ""), zot=local_zot):
            continue
        return data
    return None


def _find_local_item_by_metadata_scoped(
    *,
    title: str,
    item_type: str,
    doi: str | None,
    url: str | None,
    collection_keys: list[str | None] | None = None,
    require_pdf: bool = False,
) -> dict[str, Any] | None:
    seen: set[str] = set()
    for collection_key in collection_keys or []:
        if collection_key in seen:
            continue
        seen.add(collection_key or "")
        item = _find_local_item_by_metadata(
            title=title,
            item_type=item_type,
            doi=doi,
            url=url,
            collection_key=collection_key,
            require_pdf=require_pdf,
        )
        if item:
            return item

    return _find_local_item_by_metadata(
        title=title,
        item_type=item_type,
        doi=doi,
        url=url,
        require_pdf=require_pdf,
    )


def _wait_for_local_item_by_metadata(
    *,
    title: str,
    item_type: str,
    doi: str | None,
    url: str | None,
    collection_keys: list[str | None] | None = None,
    require_pdf: bool = False,
    wait_seconds: float = 20.0,
    poll_interval: float = 1.0,
) -> dict[str, Any] | None:
    deadline = time.time() + max(wait_seconds, 0.0)
    while True:
        item = _find_local_item_by_metadata_scoped(
            title=title,
            item_type=item_type,
            doi=doi,
            url=url,
            collection_keys=collection_keys,
            require_pdf=require_pdf,
        )
        if item:
            return item
        if time.time() >= deadline:
            return None
        time.sleep(max(poll_interval, 0.1))


def _local_item_lookup_kwargs(parent_item: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": parent_item.get("title", ""),
        "item_type": parent_item.get("itemType", ""),
        "doi": parent_item.get("DOI"),
        "url": parent_item.get("url"),
    }


def _local_item_collection_scope(
    *,
    intended_collection_key: str | None,
    selected_collection_key: str | None = None,
) -> list[str | None]:
    keys: list[str | None] = []
    for key in (intended_collection_key, selected_collection_key):
        if key and key not in keys:
            keys.append(key)
    return keys


def _finalize_local_copy_result(
    zot,
    *,
    original_item_key: str,
    local_item_key: str | None,
    local_zot,
    intended_collection_key: str | None,
    selected_collection_key: str | None,
    remove_from_selected_target: bool,
    intended_path: list[str],
    target_name: str | None,
    ctx: Context,
    pdf_source: str,
    message_prefix: str,
) -> dict[str, Any]:
    if not local_item_key:
        return {
            "success": False,
            "pdf_source": pdf_source,
            "message": f"{message_prefix}, but no local item key was found",
        }

    reconcile_result = _reconcile_local_item_to_collection(
        local_zot,
        local_item_key,
        intended_collection_key=intended_collection_key,
        selected_collection_key=selected_collection_key,
        remove_from_selected_target=remove_from_selected_target,
        ctx=ctx,
    )
    promote_result = _promote_local_copy_over_original(
        zot,
        original_item_key=original_item_key,
        local_item_key=local_item_key,
        ctx=ctx,
    )

    target_hint = ""
    if target_name:
        target_hint += f"; actual_selected_target={target_name}"
    if intended_path:
        target_hint += f"; intended_target={' / '.join(intended_path)}"
    if reconcile_result:
        target_hint += f"; {reconcile_result['message']}"
    target_hint += f"; {promote_result['message']}"

    return {
        "success": True,
        "pdf_source": pdf_source,
        "message": (
            f"{message_prefix} as local item `{local_item_key}`"
            f"{target_hint}"
        ),
        "local_item_key": local_item_key,
        "promoted_item_key": promote_result.get("promoted_item_key") or local_item_key,
        "actual_selected_collection_id": selected_collection_key,
        "actual_selected_target": target_name,
        "intended_target": " / ".join(intended_path) if intended_path else None,
        "reconcile_status": reconcile_result.get("status") if reconcile_result else "not_needed",
        "reconcile_message": reconcile_result.get("message") if reconcile_result else "",
    }


def _reuse_existing_local_copy_result(
    zot,
    *,
    original_item_key: str,
    local_item_key: str | None,
    local_zot,
    intended_collection_key: str | None,
    selected_collection_key: str | None,
    intended_path: list[str],
    target_name: str | None,
    ctx: Context,
    existing_scope: str,
    pdf_source: str = "local_zotero_existing_copy",
) -> dict[str, Any]:
    if not local_item_key:
        return {
            "success": False,
            "pdf_source": pdf_source,
            "message": "Existing local Zotero copy was matched, but no local item key was found",
        }

    reconcile_result = _reconcile_local_item_to_collection(
        local_zot,
        local_item_key,
        intended_collection_key=intended_collection_key,
        selected_collection_key=selected_collection_key,
        remove_from_selected_target=True,
        ctx=ctx,
    )
    promote_result = _promote_local_copy_over_original(
        zot,
        original_item_key=original_item_key,
        local_item_key=local_item_key,
        ctx=ctx,
    )

    message = f"Reused existing local Zotero copy `{local_item_key}`"
    if reconcile_result:
        message += f"; {reconcile_result['message']}"
    elif intended_path and existing_scope == "global":
        message += f"; existing copy is outside intended_target={' / '.join(intended_path)}"
    elif intended_path:
        message += f"; intended_target={' / '.join(intended_path)}"
    if target_name:
        message += f"; actual_selected_target={target_name}"
    message += f"; {promote_result['message']}"

    return {
        "success": True,
        "pdf_source": pdf_source,
        "message": message,
        "local_item_key": local_item_key,
        "promoted_item_key": promote_result.get("promoted_item_key") or local_item_key,
        "actual_selected_collection_id": selected_collection_key,
        "actual_selected_target": target_name,
        "intended_target": " / ".join(intended_path) if intended_path else None,
        "reconcile_status": reconcile_result.get("status") if reconcile_result else "not_needed",
        "reconcile_message": reconcile_result.get("message") if reconcile_result else "",
    }


def _repair_local_item_with_file_attach(
    zot,
    *,
    original_item_key: str,
    parent_item: dict[str, Any],
    pdf_path: Path,
    local_zot,
    intended_collection_key: str | None,
    selected_collection_key: str | None,
    intended_path: list[str],
    target_name: str | None,
    ctx: Context,
    initial_local_item: dict[str, Any] | None = None,
    discovery_wait_seconds: float = 5.0,
    pdf_source: str = "local_zotero_file_attach_repair",
    message_prefix: str = "Attached PDF to recovered local Zotero parent",
) -> dict[str, Any]:
    lookup_kwargs = _local_item_lookup_kwargs(parent_item)
    collection_scope = _local_item_collection_scope(
        intended_collection_key=intended_collection_key,
        selected_collection_key=selected_collection_key,
    )

    local_item = initial_local_item or _wait_for_local_item_by_metadata(
        **lookup_kwargs,
        collection_keys=collection_scope,
        require_pdf=False,
        wait_seconds=discovery_wait_seconds,
        poll_interval=0.5,
    )
    if not local_item:
        return {
            "success": False,
            "pdf_source": pdf_source,
            "message": "No matching local Zotero parent was available for file-attach repair",
        }

    local_item_key = local_item.get("key")
    if not local_item_key:
        return {
            "success": False,
            "pdf_source": pdf_source,
            "message": "Recovered local Zotero parent did not expose a usable item key",
        }

    if not _item_has_usable_pdf_attachment(local_item_key, zot=local_zot):
        ctx.info(f"Repairing local Zotero parent `{local_item_key}` with downloaded PDF file")
        try:
            local_zot.attachment_simple([str(pdf_path)], local_item_key)
        except Exception as exc:
            return {
                "success": False,
                "pdf_source": pdf_source,
                "local_item_key": local_item_key,
                "message": f"local file attach repair failed for `{local_item_key}`: {exc}",
            }

    materialized_result = _confirm_local_pdf_attachment_materialized(
        local_item_key,
        ctx=ctx,
        wait_seconds=20.0,
        poll_interval=1.0,
    )
    if not materialized_result.get("success"):
        return {
            "success": False,
            "pdf_source": pdf_source,
            "local_item_key": local_item_key,
            "message": (
                f"Local parent `{local_item_key}` was found, but its PDF still did not materialize: "
                f"{materialized_result.get('message', 'unknown error')}"
            ),
        }

    return _finalize_local_copy_result(
        zot,
        original_item_key=original_item_key,
        local_item_key=local_item_key,
        local_zot=local_zot,
        intended_collection_key=intended_collection_key,
        selected_collection_key=selected_collection_key,
        remove_from_selected_target=True,
        intended_path=intended_path,
        target_name=target_name,
        ctx=ctx,
        pdf_source=pdf_source,
        message_prefix=message_prefix,
    )


def _recover_materialized_local_copy_after_failure(
    zot,
    *,
    original_item_key: str,
    parent_item: dict[str, Any],
    ctx: Context,
    pdf_source: str,
    message_prefix: str,
    wait_seconds: float = 6.0,
) -> dict[str, Any]:
    local_zot = get_local_zotero_client()
    if local_zot is None:
        return {
            "success": False,
            "pdf_source": pdf_source,
            "message": "local Zotero is not running or local API is unavailable",
        }

    collection_key = None
    collections = parent_item.get("collections") or []
    if collections:
        collection_key = collections[0]
    target_snapshot = _connector_target_snapshot(
        preferred_item_key=original_item_key,
        ctx=ctx,
    )
    target_name = target_snapshot.get("current_name") or "current local target"
    selected_collection_key = target_snapshot.get("current_collection_id")
    intended_path = _collection_path(local_zot, collection_key)

    local_item = _wait_for_local_item_by_metadata(
        **_local_item_lookup_kwargs(parent_item),
        collection_keys=_local_item_collection_scope(
            intended_collection_key=collection_key,
            selected_collection_key=selected_collection_key,
        ),
        require_pdf=True,
        wait_seconds=wait_seconds,
        poll_interval=0.5,
    )
    if not local_item:
        return {
            "success": False,
            "pdf_source": pdf_source,
            "message": "No materialized local Zotero copy appeared after attach failure",
        }

    return _finalize_local_copy_result(
        zot,
        original_item_key=original_item_key,
        local_item_key=local_item.get("key"),
        local_zot=local_zot,
        intended_collection_key=collection_key,
        selected_collection_key=selected_collection_key,
        remove_from_selected_target=True,
        intended_path=intended_path,
        target_name=target_name,
        ctx=ctx,
        pdf_source=pdf_source,
        message_prefix=message_prefix,
    )


def _local_candidate_keys_from_db(
    *,
    doi: str | None,
    title: str | None,
    url: str | None = None,
    arxiv_id: str | None = None,
    limit: int = 30,
) -> list[str]:
    normalized_title = _normalize_text(title)
    normalized_url = _normalize_url_for_match(url)
    normalized_arxiv = _normalize_arxiv_id(arxiv_id)
    if not doi and not normalized_title and not normalized_url and not normalized_arxiv:
        return []
    try:
        from zotero_mcp.local_db import LocalZoteroReader

        reader = LocalZoteroReader()
        conn = sqlite3.connect(f"file:{reader.db_path}?immutable=1", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT i.key,
                       MAX(CASE WHEN f.fieldName = 'DOI' THEN v.value END) AS doi_value,
                       MAX(CASE WHEN f.fieldName = 'title' THEN v.value END) AS title_value,
                       MAX(CASE WHEN f.fieldName = 'url' THEN v.value END) AS url_value,
                       MAX(CASE WHEN f.fieldName = 'archiveID' THEN v.value END) AS archive_id_value
                FROM items i
                LEFT JOIN itemData id ON id.itemID = i.itemID
                LEFT JOIN itemDataValues v ON v.valueID = id.valueID
                LEFT JOIN fieldsCombined f ON f.fieldID = id.fieldID
                GROUP BY i.itemID
                HAVING (? != '' AND doi_value = ?)
                    OR (? != '' AND title_value = ?)
                    OR (? != '' AND url_value = ?)
                    OR (? != '' AND archive_id_value = ?)
                ORDER BY i.itemID DESC
                LIMIT ?
                """,
                (
                    doi or "",
                    doi or "",
                    title or "",
                    title or "",
                    url or "",
                    url or "",
                    f"arXiv:{normalized_arxiv}" if normalized_arxiv else "",
                    f"arXiv:{normalized_arxiv}" if normalized_arxiv else "",
                    limit,
                ),
            ).fetchall()
        finally:
            conn.close()
    except Exception:
        return []
    keys: list[str] = []
    for row in rows:
        key = str(row["key"] or "")
        if not key:
            continue
        row_doi = _normalize_doi(row["doi_value"])
        row_title = _normalize_title_for_match(row["title_value"])
        row_url = _normalize_url_for_match(row["url_value"])
        row_arxiv = _normalize_arxiv_id(row["archive_id_value"])
        if doi and row_doi == doi:
            keys.append(key)
            continue
        if normalized_title and row_title == _normalize_title_for_match(title):
            keys.append(key)
            continue
        if normalized_url and row_url == normalized_url:
            keys.append(key)
            continue
        if normalized_arxiv and row_arxiv == normalized_arxiv:
            keys.append(key)
            continue
    return keys


def _find_existing_local_copy_for_import(
    *,
    doi: str | None,
    title: str | None,
    url: str | None = None,
    arxiv_id: str | None = None,
    collection_key: str | None = None,
) -> dict[str, Any] | None:
    local_zot = get_local_zotero_client()
    web_zot = get_web_zotero_client()
    if local_zot is None and web_zot is None:
        return None

    candidate_keys: list[str] = []
    seen: set[str] = set()
    match_title = _normalize_title_for_match(title)
    match_doi = _normalize_doi(doi)
    match_url = _normalize_url_for_match(url)
    match_arxiv = _normalize_arxiv_id(arxiv_id)

    def _push(key: str | None) -> None:
        if key and key not in seen:
            seen.add(key)
            candidate_keys.append(key)

    def _consider_item(item: dict[str, Any] | None) -> None:
        data = item.get("data", item if isinstance(item, dict) else {}) if isinstance(item, dict) else {}
        key = data.get("key")
        if not key:
            return
        item_doi = _normalize_doi(data.get("DOI"))
        item_title_norm = _normalize_title_for_match(data.get("title"))
        item_url_norm = _normalize_url_for_match(data.get("url"))
        item_arxiv = _item_arxiv_id_from_data(data)
        if match_doi and item_doi == match_doi:
            _push(key)
            return
        if match_arxiv and item_arxiv == match_arxiv:
            _push(key)
            return
        if match_url and item_url_norm == match_url:
            _push(key)
            return
        if match_title and item_title_norm == match_title:
            _push(key)
            return

    for key in _local_candidate_keys_from_db(
        doi=match_doi,
        title=title,
        url=url,
        arxiv_id=match_arxiv,
    ):
        _push(key)

    for zot in [client for client in (local_zot, web_zot) if client is not None]:
        for query, qmode in (
            (match_doi, "everything"),
            (match_arxiv, "everything"),
            (match_url, "everything"),
            (title, "titleCreatorYear"),
        ):
            for item in _query_items_for_existing_copy(
                zot,
                query=query,
                qmode=qmode,
                limit=25,
            ):
                _consider_item(item)
        for item in zot.items(limit=100, sort="dateAdded", direction="desc") or []:
            _consider_item(item)

    if collection_key:
        for zot in [client for client in (local_zot, web_zot) if client is not None]:
            for item in _collection_items_safe(zot, collection_key):
                _consider_item(item)

    best_item_with_pdf: dict[str, Any] | None = None
    best_item_without_pdf: dict[str, Any] | None = None
    best_pdf_score = -1
    best_no_pdf_score = -1
    desired_title_norm = match_title
    for key in candidate_keys:
        payload = None
        payload_zot = None
        for zot in [client for client in (local_zot, web_zot) if client is not None]:
            payload = _get_item_payload(zot, key)
            if payload:
                payload_zot = zot
                break
        if not payload:
            continue
        data = payload.get("data", {})
        score = 0
        item_doi = _normalize_doi(data.get("DOI"))
        strong_match = False
        if match_doi and item_doi == match_doi:
            score += 120
            strong_match = True
        item_arxiv = _item_arxiv_id_from_data(data)
        if match_arxiv and item_arxiv == match_arxiv:
            score += 110
            strong_match = True
        item_title_norm = _normalize_title_for_match(data.get("title"))
        if desired_title_norm and item_title_norm == desired_title_norm:
            score += 50
            strong_match = True
        item_url_norm = _normalize_url_for_match(data.get("url"))
        if match_url and item_url_norm == match_url:
            score += 40
            strong_match = True
        if not strong_match:
            continue
        if collection_key and collection_key in (data.get("collections") or []):
            score += 20
        score += sum(
            1
            for field_value in (
                data.get("abstractNote"),
                data.get("date"),
                data.get("url"),
                data.get("DOI"),
            )
            if str(field_value or "").strip()
        ) * 3
        score += min(len(data.get("creators") or []), 5) * 2
        score += {
            "conferencePaper": 15,
            "journalArticle": 15,
            "preprint": 12,
            "webpage": 5,
        }.get(str(data.get("itemType") or ""), 0)
        has_pdf = _item_has_usable_pdf_attachment(key, zot=payload_zot or local_zot or web_zot)
        if has_pdf:
            score += 40
            if score > best_pdf_score:
                best_pdf_score = score
                best_item_with_pdf = data
        elif score > best_no_pdf_score:
            best_no_pdf_score = score
            best_item_without_pdf = data

    return best_item_with_pdf or best_item_without_pdf


def _reuse_existing_local_copy_for_import(
    *,
    collection_key: str | None,
    doi: str | None,
    title: str | None,
    url: str | None = None,
    arxiv_id: str | None = None,
    route: str,
    ctx: Context,
) -> dict[str, Any] | None:
    local_zot = get_local_zotero_client()
    web_zot = get_web_zotero_client()
    if local_zot is None and web_zot is None:
        return None

    existing_local_item = _find_existing_local_copy_for_import(
        doi=doi,
        title=title,
        url=url,
        arxiv_id=arxiv_id,
        collection_key=collection_key,
    )
    if not existing_local_item:
        return None

    local_key = existing_local_item.get("key")
    if not local_key:
        return None

    target_snapshot = _connector_target_snapshot(
        preferred_item_key=local_key,
        ctx=ctx,
    )
    selected_collection_key = target_snapshot.get("current_collection_id")
    reconcile_result = _reconcile_local_item_to_collection(
        local_zot or web_zot,
        local_key,
        intended_collection_key=collection_key,
        selected_collection_key=selected_collection_key,
        remove_from_selected_target=bool(
            selected_collection_key
            and collection_key
            and selected_collection_key != collection_key
        ),
        ctx=ctx,
    )
    payload = (
        _get_item_payload(local_zot, local_key) if local_zot is not None else None
    ) or (
        _get_item_payload(web_zot, local_key) if web_zot is not None else None
    ) or {"data": existing_local_item}
    data = payload.get("data", existing_local_item)
    reused_has_pdf = _item_has_usable_pdf_attachment(
        local_key,
        zot=local_zot or web_zot,
    )
    message = f"Reused existing local Zotero copy `{local_key}`"
    if not reused_has_pdf:
        message += "; existing item does not have a PDF yet"
    if reconcile_result:
        message += f"; {reconcile_result['message']}"
    label_zot = local_zot or web_zot

    return {
        "success": True,
        "label": data.get("title") or title or doi or local_key,
        "key": local_key,
        "route": route,
        "pdf_source": "local_zotero_existing_copy" if reused_has_pdf else "none",
        "fallback_reason": "none",
        "pdf_message": message,
        "local_item_key": local_key,
        "intended_target": _collection_label(label_zot, collection_key) if label_zot is not None else None,
        "reconcile_status": reconcile_result.get("status") if reconcile_result else "not_needed",
        "reconcile_message": reconcile_result.get("message") if reconcile_result else "",
    }


def _reconcile_local_item_to_collection(
    local_zot,
    local_item_key: str,
    *,
    intended_collection_key: str | None,
    selected_collection_key: str | None = None,
    remove_from_selected_target: bool = False,
    ctx: Context | None = None,
) -> dict[str, Any]:
    intended_collection_key = _resolve_connector_collection_key(intended_collection_key) or intended_collection_key
    selected_collection_key = _resolve_connector_collection_key(selected_collection_key) or selected_collection_key

    if not intended_collection_key:
        return {
            "success": False,
            "status": "no_intended_collection",
            "message": "No intended collection key available for local reconcile",
            "collections": [],
        }

    web_zot = get_web_zotero_client()

    def _load_reconcile_target() -> tuple[Any, dict[str, Any] | None]:
        if web_zot is not None:
            web_payload = _get_item_payload(web_zot, local_item_key)
            if web_payload:
                return web_zot, web_payload
        return local_zot, _get_item_payload(local_zot, local_item_key)

    def _wait_for_web_payload(timeout_seconds: float = 8.0) -> dict[str, Any] | None:
        if web_zot is None:
            return None
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            payload = _get_item_payload(web_zot, local_item_key)
            if payload:
                return payload
            time.sleep(0.5)
        return _get_item_payload(web_zot, local_item_key)

    def _reload_current_payload(timeout_seconds: float = 0.0) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
        deadline = time.time() + max(timeout_seconds, 0.0)
        latest_payload = payload
        latest_data = payload.get("data", {}) if payload else {}
        latest_collections = list(latest_data.get("collections") or [])
        while True:
            refreshed = (
                _get_item_payload(write_zot, local_item_key)
                or _get_item_payload(local_zot, local_item_key)
                or latest_payload
            )
            if refreshed:
                latest_payload = refreshed
                latest_data = refreshed.get("data", {})
                latest_collections = list(latest_data.get("collections") or [])
            if time.time() >= deadline:
                return latest_payload, latest_data, latest_collections
            time.sleep(0.5)

    def _raise_for_http_response(response: Any) -> None:
        if response is None:
            return
        status_code = getattr(response, "status_code", None)
        if status_code is not None and int(status_code) >= 400:
            raise_fn = getattr(response, "raise_for_status", None)
            if callable(raise_fn):
                raise_fn()
            raise RuntimeError(f"collection reconcile HTTP {status_code}")

    def _apply_membership_change(
        *,
        action: Literal["add", "remove"],
        collection_key: str,
    ) -> None:
        nonlocal payload, data, collections
        expected_present = action == "add"
        last_exc: Exception | None = None

        for _attempt in range(2):
            payload, data, collections = _reload_current_payload(timeout_seconds=0.0)
            already_ok = collection_key in collections
            if already_ok == expected_present:
                return

            try:
                if action == "add":
                    response = write_zot.addto_collection(collection_key, payload)
                else:
                    response = write_zot.deletefrom_collection(collection_key, payload)
                _raise_for_http_response(response)
            except Exception as exc:
                last_exc = exc
                continue

            payload, data, collections = _reload_current_payload(timeout_seconds=3.0)
            if (collection_key in collections) == expected_present:
                return
            last_exc = RuntimeError(
                f"collection membership change did not materialize for `{local_item_key}` "
                f"(action={action}, collection={collection_key})"
            )

        raise last_exc or RuntimeError(
            f"collection membership update failed for `{local_item_key}` "
            f"(action={action}, collection={collection_key})"
        )

    write_zot, payload = _load_reconcile_target()
    if not payload:
        return {
            "success": False,
            "status": "local_item_unavailable",
            "message": f"Local item `{local_item_key}` could not be loaded for reconcile",
            "collections": [],
        }

    data = payload.get("data", {})
    collections = list(data.get("collections") or [])
    added_to_intended = False
    removed_from_selected = False

    if intended_collection_key not in collections:
        try:
            _apply_membership_change(action="add", collection_key=intended_collection_key)
            added_to_intended = True
        except Exception as exc:
            if (
                write_zot is local_zot
                and web_zot is not None
                and (
                    "Method not implemented" in str(exc)
                    or "Code: 501" in str(exc)
                    or "did not materialize" in str(exc)
                    or "HTTP 412" in str(exc)
                )
            ):
                web_payload = _wait_for_web_payload()
                if web_payload:
                    write_zot = web_zot
                    payload = web_payload
                    data = payload.get("data", {})
                    collections = list(data.get("collections") or [])
                    try:
                        _apply_membership_change(action="add", collection_key=intended_collection_key)
                        added_to_intended = True
                    except Exception as web_exc:
                        return {
                            "success": False,
                            "status": "add_to_intended_failed",
                            "message": f"Failed to add local item `{local_item_key}` to intended collection `{intended_collection_key}`: {web_exc}",
                            "collections": collections,
                        }
                else:
                    return {
                        "success": False,
                        "status": "pending_web_sync",
                        "message": (
                            f"Local item `{local_item_key}` was created via connector, but the web API "
                            "could not see it yet for collection reconcile"
                        ),
                        "collections": collections,
                    }
            else:
                return {
                    "success": False,
                    "status": "add_to_intended_failed",
                    "message": f"Failed to add local item `{local_item_key}` to intended collection `{intended_collection_key}`: {exc}",
                    "collections": collections,
                }

    if (
        remove_from_selected_target
        and selected_collection_key
        and selected_collection_key != intended_collection_key
        and selected_collection_key in collections
    ):
        try:
            _apply_membership_change(action="remove", collection_key=selected_collection_key)
            removed_from_selected = True
        except Exception as exc:
            if (
                write_zot is local_zot
                and web_zot is not None
                and (
                    "Method not implemented" in str(exc)
                    or "Code: 501" in str(exc)
                    or "did not materialize" in str(exc)
                    or "HTTP 412" in str(exc)
                )
            ):
                web_payload = _wait_for_web_payload()
                if web_payload:
                    write_zot = web_zot
                    payload = web_payload
                    data = payload.get("data", {})
                    collections = list(data.get("collections") or [])
                    try:
                        if selected_collection_key in collections:
                            _apply_membership_change(action="remove", collection_key=selected_collection_key)
                            removed_from_selected = True
                    except Exception as web_exc:
                        return {
                            "success": False,
                            "status": "remove_from_selected_failed",
                            "message": f"Added local item `{local_item_key}` to intended collection but failed to remove it from selected collection `{selected_collection_key}`: {web_exc}",
                            "collections": collections,
                        }
                else:
                    return {
                        "success": False,
                        "status": "pending_web_sync",
                        "message": (
                            f"Local item `{local_item_key}` needs selected-target cleanup for `{selected_collection_key}`, "
                            "but the web API could not see it yet for verified reconcile"
                        ),
                        "collections": collections,
                    }
            else:
                return {
                    "success": False,
                    "status": "remove_from_selected_failed",
                    "message": f"Added local item `{local_item_key}` to intended collection but failed to remove it from selected collection `{selected_collection_key}`: {exc}",
                    "collections": collections,
                }

    payload, data, collections = _reload_current_payload(timeout_seconds=1.0)
    if intended_collection_key not in collections:
        return {
            "success": False,
            "status": "intended_collection_not_materialized",
            "message": (
                f"Local item `{local_item_key}` still is not inside intended collection "
                f"`{intended_collection_key}` after reconcile"
            ),
            "collections": collections,
        }
    if (
        remove_from_selected_target
        and selected_collection_key
        and selected_collection_key != intended_collection_key
        and selected_collection_key in collections
    ):
        return {
            "success": False,
            "status": "selected_collection_still_attached",
            "message": (
                f"Local item `{local_item_key}` still remains inside selected collection "
                f"`{selected_collection_key}` after reconcile"
            ),
            "collections": collections,
        }

    if (
        remove_from_selected_target
        and selected_collection_key
        and selected_collection_key != intended_collection_key
        and removed_from_selected
        and selected_collection_key not in collections
    ):
        removed_from_selected = True
    elif (
        remove_from_selected_target
        and selected_collection_key
        and selected_collection_key != intended_collection_key
        and selected_collection_key not in collections
        and intended_collection_key in collections
    ):
        removed_from_selected = True

    actions: list[str] = []
    if added_to_intended:
        actions.append("added_to_intended_target")
    if removed_from_selected:
        actions.append("removed_from_selected_target")
    if not actions:
        actions.append("already_in_intended_target")

    label_zot = write_zot or local_zot
    intended_label = _collection_label(label_zot, intended_collection_key) or _collection_label(local_zot, intended_collection_key) or intended_collection_key
    selected_label = _collection_label(label_zot, selected_collection_key) or _collection_label(local_zot, selected_collection_key) or selected_collection_key
    message = f"Local item `{local_item_key}` {'; '.join(actions)}"
    if intended_label:
        message += f"; intended_target={intended_label}"
    if selected_label and selected_collection_key != intended_collection_key:
        message += f"; actual_selected_target={selected_label}"
    if ctx is not None:
        ctx.info(message)
    return {
        "success": True,
        "status": "reconciled",
        "message": message,
        "actions": actions,
        "collections": collections,
        "added_to_intended": added_to_intended,
        "removed_from_selected": removed_from_selected,
    }


def _promote_local_copy_over_original(
    zot,
    *,
    original_item_key: str,
    local_item_key: str | None,
    ctx: Context,
) -> dict[str, Any]:
    if not local_item_key or local_item_key == original_item_key:
        return {
            "success": False,
            "promoted_item_key": local_item_key,
            "message": "No separate local copy available to promote",
        }

    if not _item_has_usable_pdf_attachment(local_item_key):
        return {
            "success": False,
            "promoted_item_key": local_item_key,
            "message": (
                f"Local copy `{local_item_key}` exists but its PDF is not materialized yet; "
                f"kept original `{original_item_key}`"
            ),
        }

    try:
        original_item = _get_item_payload(zot, original_item_key) or {"key": original_item_key}
        zot.delete_item(original_item)
        message = (
            f"Promoted local copy `{local_item_key}` and moved original `{original_item_key}` to trash"
        )
        ctx.info(message)
        return {
            "success": True,
            "promoted_item_key": local_item_key,
            "message": message,
        }
    except Exception as exc:
        return {
            "success": False,
            "promoted_item_key": local_item_key,
            "message": (
                f"Local copy `{local_item_key}` is ready, but failed to trash original "
                f"`{original_item_key}`: {exc}"
            ),
        }


def _save_pdf_via_local_connector_copy(
    zot,
    item_key: str,
    pdf_path: Path,
    *,
    pdf_url: str,
    ctx: Context,
) -> dict[str, Any]:
    local_zot = get_local_zotero_client()
    if local_zot is None:
        return {
            "success": False,
            "pdf_source": "local_zotero",
            "message": "local Zotero is not running or local API is unavailable",
        }
    if not hasattr(local_zot, "items"):
        return {
            "success": False,
            "pdf_source": "local_zotero",
            "message": "local Zotero connector fallback requires a real local client",
        }

    parent_item = zot.item(item_key).get("data", {})
    if not parent_item:
        return {
            "success": False,
            "pdf_source": "local_zotero",
            "message": f"could not read parent item {item_key} for local connector fallback",
        }

    collection_key = None
    collections = parent_item.get("collections") or []
    if collections:
        collection_key = collections[0]
    intended_path = _collection_path(local_zot, collection_key)
    lookup_kwargs = _local_item_lookup_kwargs(parent_item)
    target_snapshot = _connector_target_snapshot(
        preferred_item_key=item_key,
        ctx=ctx,
    )
    target_name = target_snapshot.get("current_name") or "current local target"
    selected_collection_key = target_snapshot.get("current_collection_id")

    existing_local_item = _find_local_item_by_metadata(
        **lookup_kwargs,
        collection_key=collection_key,
        require_pdf=True,
    )
    existing_scope = "intended_target"
    if not existing_local_item:
        existing_local_item = _find_local_item_by_metadata(
            **lookup_kwargs,
            require_pdf=True,
        )
        existing_scope = "global"
    if existing_local_item:
        return _reuse_existing_local_copy_result(
            zot,
            original_item_key=item_key,
            local_item_key=existing_local_item.get("key"),
            local_zot=local_zot,
            intended_collection_key=collection_key,
            selected_collection_key=selected_collection_key,
            intended_path=intended_path,
            target_name=target_name,
            ctx=ctx,
            existing_scope=existing_scope,
        )

    if not hasattr(local_zot, "client"):
        return {
            "success": False,
            "pdf_source": "local_zotero",
            "message": "local Zotero connector fallback requires a real local client",
        }

    session_id = f"zotero-mcp-local-{uuid.uuid4().hex[:8]}"
    connector_item = _sanitize_item_for_local_connector(parent_item)
    metadata = {
        "sessionID": session_id,
        "parentItemID": connector_item["id"],
        "title": pdf_path.name,
        "url": pdf_url,
    }
    collection_scope = _local_item_collection_scope(
        intended_collection_key=collection_key,
        selected_collection_key=selected_collection_key,
    )

    existing_pending_local_item = _find_local_item_by_metadata_scoped(
        **lookup_kwargs,
        collection_keys=collection_scope,
        require_pdf=False,
    )
    if existing_pending_local_item and not _item_has_usable_pdf_attachment(
        existing_pending_local_item.get("key", ""),
        zot=local_zot,
    ):
        repaired_result = _repair_local_item_with_file_attach(
            zot,
            original_item_key=item_key,
            parent_item=parent_item,
            pdf_path=pdf_path,
            local_zot=local_zot,
            intended_collection_key=collection_key,
            selected_collection_key=selected_collection_key,
            intended_path=intended_path,
            target_name=target_name,
            ctx=ctx,
            initial_local_item=existing_pending_local_item,
            discovery_wait_seconds=0.0,
            message_prefix="Attached PDF to existing local Zotero parent",
        )
        if repaired_result.get("success"):
            return repaired_result

    connector_attach_timeout = _connector_url_attach_timeout_seconds()

    try:
        _ensure_connector_library_context(
            preferred_item_key=item_key,
            ctx=ctx,
        )
        create_resp = requests.post(
            "http://127.0.0.1:23119/connector/saveItems",
            json=_connector_save_items_payload(
                session_id=session_id,
                items=[connector_item],
                base_uri=parent_item.get("url") or pdf_url,
            ),
            timeout=20,
        )
        create_resp.raise_for_status()

        pdf_bytes = pdf_path.read_bytes()
        attach_resp = requests.post(
            "http://127.0.0.1:23119/connector/saveAttachment",
            params={"sessionID": session_id},
            data=pdf_bytes,
            headers={
                "Content-Type": "application/pdf",
                "Content-Length": str(len(pdf_bytes)),
                "X-Metadata": json.dumps(metadata),
            },
            timeout=connector_attach_timeout,
        )
        attach_resp.raise_for_status()

        local_item = _wait_for_local_item_by_metadata(
            **lookup_kwargs,
            collection_keys=collection_scope,
            require_pdf=False,
            wait_seconds=min(connector_attach_timeout, 8.0),
            poll_interval=0.5,
        )
        repaired_result = _repair_local_item_with_file_attach(
            zot,
            original_item_key=item_key,
            parent_item=parent_item,
            pdf_path=pdf_path,
            local_zot=local_zot,
            intended_collection_key=collection_key,
            selected_collection_key=selected_collection_key,
            intended_path=intended_path,
            target_name=target_name,
            ctx=ctx,
            initial_local_item=local_item,
            discovery_wait_seconds=0.0,
            message_prefix="PDF saved via local Zotero connector",
        )
        if repaired_result.get("success"):
            repaired_result["pdf_source"] = "local_zotero_copy"
            return repaired_result
        return repaired_result
    except Exception as exc:
        recovered_result = _repair_local_item_with_file_attach(
            zot,
            original_item_key=item_key,
            parent_item=parent_item,
            pdf_path=pdf_path,
            local_zot=local_zot,
            intended_collection_key=collection_key,
            selected_collection_key=selected_collection_key,
            intended_path=intended_path,
            target_name=target_name,
            ctx=ctx,
            discovery_wait_seconds=5.0,
            message_prefix="Recovered local Zotero parent after connector copy failure",
        )
        if recovered_result.get("success"):
            recovered_result["pdf_source"] = "local_zotero_copy"
            return recovered_result
        return {
            "success": False,
            "pdf_source": "local_zotero",
            "message": (
                f"local Zotero connector fallback failed: {exc}; "
                f"{recovered_result.get('message', 'local file attach repair not available')}"
            ),
        }


def _save_pdf_via_local_connector_url(
    zot,
    item_key: str,
    *,
    pdf_url: str,
    filename: str,
    ctx: Context,
) -> dict[str, Any]:
    if not _connector_zero_byte_url_attach_enabled():
        return {
            "success": False,
            "pdf_source": "local_zotero_url",
            "message": (
                "local connector URL import is disabled by default because Zotero connector "
                "saveAttachment expects real PDF bytes rather than an empty-body URL attach"
            ),
        }
    local_zot = get_local_zotero_client()
    if local_zot is None:
        return {
            "success": False,
            "pdf_source": "local_zotero",
            "message": "local Zotero is not running or local API is unavailable",
        }
    if not hasattr(local_zot, "items"):
        return {
            "success": False,
            "pdf_source": "local_zotero",
            "message": "local Zotero connector URL fallback requires a real local client",
        }

    parent_item = zot.item(item_key).get("data", {})
    if not parent_item:
        return {
            "success": False,
            "pdf_source": "local_zotero",
            "message": f"could not read parent item {item_key} for local connector URL fallback",
        }

    collection_key = None
    collections = parent_item.get("collections") or []
    if collections:
        collection_key = collections[0]
    intended_path = _collection_path(local_zot, collection_key)
    lookup_kwargs = _local_item_lookup_kwargs(parent_item)
    target_snapshot = _connector_target_snapshot(
        preferred_item_key=item_key,
        ctx=ctx,
    )
    target_name = target_snapshot.get("current_name") or "current local target"
    selected_collection_key = target_snapshot.get("current_collection_id")

    existing_local_item = _find_local_item_by_metadata(
        **lookup_kwargs,
        collection_key=collection_key,
        require_pdf=True,
    ) or _find_local_item_by_metadata(
        **lookup_kwargs,
        require_pdf=True,
    )
    if existing_local_item:
        existing_scope = (
            "intended_target"
            if collection_key and collection_key in (existing_local_item.get("collections") or [])
            else "global"
        )
        return _reuse_existing_local_copy_result(
            zot,
            original_item_key=item_key,
            local_item_key=existing_local_item.get("key"),
            local_zot=local_zot,
            intended_collection_key=collection_key,
            selected_collection_key=selected_collection_key,
            intended_path=intended_path,
            target_name=target_name,
            ctx=ctx,
            existing_scope=existing_scope,
        )

    if not hasattr(local_zot, "client"):
        return {
            "success": False,
            "pdf_source": "local_zotero",
            "message": "local Zotero connector URL fallback requires a real local client",
        }

    session_id = f"zotero-mcp-local-url-{uuid.uuid4().hex[:8]}"
    connector_item = _sanitize_item_for_local_connector(parent_item)
    metadata = {
        "sessionID": session_id,
        "parentItemID": connector_item["id"],
        "title": filename,
        "url": pdf_url,
    }
    collection_scope = _local_item_collection_scope(
        intended_collection_key=collection_key,
        selected_collection_key=selected_collection_key,
    )

    connector_attach_timeout = _connector_url_attach_timeout_seconds()

    try:
        _ensure_connector_library_context(
            preferred_item_key=item_key,
            ctx=ctx,
        )
        create_resp = requests.post(
            "http://127.0.0.1:23119/connector/saveItems",
            json=_connector_save_items_payload(
                session_id=session_id,
                items=[connector_item],
                base_uri=parent_item.get("url") or pdf_url,
            ),
            timeout=20,
        )
        create_resp.raise_for_status()

        attach_resp = requests.post(
            "http://127.0.0.1:23119/connector/saveAttachment",
            params={"sessionID": session_id},
            data=b"",
            headers={
                "Content-Type": "application/pdf",
                "Content-Length": "0",
                "X-Metadata": json.dumps(metadata),
            },
            timeout=connector_attach_timeout,
        )
        attach_resp.raise_for_status()

        local_item = _wait_for_local_item_by_metadata(
            **lookup_kwargs,
            collection_keys=collection_scope,
            require_pdf=False,
            wait_seconds=min(connector_attach_timeout, 15.0),
            poll_interval=0.5,
        )
        local_key = local_item.get("key") if local_item else None
        if local_key:
            materialized_result = _confirm_local_pdf_attachment_materialized(
                local_key,
                ctx=ctx,
                wait_seconds=min(max(connector_attach_timeout, 10.0), 30.0),
                poll_interval=1.0,
            )
            if materialized_result.get("success"):
                return _finalize_local_copy_result(
                    zot,
                    original_item_key=item_key,
                    local_item_key=local_key,
                    local_zot=local_zot,
                    intended_collection_key=collection_key,
                    selected_collection_key=selected_collection_key,
                    remove_from_selected_target=True,
                    intended_path=intended_path,
                    target_name=target_name,
                    ctx=ctx,
                    pdf_source="local_zotero_url_copy",
                    message_prefix="PDF saved via local Zotero connector URL import",
                )
            return {
                "success": False,
                "pdf_source": "local_zotero_url",
                "local_item_key": local_key,
                "message": (
                    f"local Zotero connector URL import created local item `{local_key}`, "
                    f"but the PDF did not materialize yet: "
                    f"{materialized_result.get('message', 'unknown error')}"
                ),
                "actual_selected_collection_id": selected_collection_key,
                "actual_selected_target": target_name,
                "intended_target": " / ".join(intended_path) if intended_path else None,
            }
        return {
            "success": False,
            "pdf_source": "local_zotero_url",
            "message": (
                "local Zotero connector URL import returned success, but no matching local "
                "parent item appeared for confirmation"
            ),
            "actual_selected_collection_id": selected_collection_key,
            "actual_selected_target": target_name,
            "intended_target": " / ".join(intended_path) if intended_path else None,
        }
    except Exception as exc:
        _remember_connector_url_fastpath_timeout(pdf_url, exc)
        return {
            "success": False,
            "pdf_source": "local_zotero_url",
            "message": f"local Zotero connector URL fallback failed: {exc}",
        }


def _attach_pdf_via_local_zotero(
    item_key: str,
    pdf_path: Path,
    *,
    ctx: Context,
) -> dict[str, Any]:
    local_zot = get_local_zotero_client()
    if local_zot is None:
        return {
            "success": False,
            "pdf_source": "local_zotero",
            "message": "local Zotero is not running or local API is unavailable",
        }

    try:
        if _item_has_usable_pdf_attachment(item_key, zot=local_zot):
            return {
                "success": True,
                "pdf_source": "existing_attachment",
                "message": "PDF already attached in local Zotero; skipped duplicate upload",
                "skipped": True,
            }
        ctx.info("Retrying PDF attachment via local Zotero")
        local_zot.attachment_simple([str(pdf_path)], item_key)
        materialized_result = _confirm_local_pdf_attachment_materialized(
            item_key,
            ctx=ctx,
            wait_seconds=20.0,
            poll_interval=1.0,
        )
        if materialized_result.get("success"):
            return {
                "success": True,
                "pdf_source": "local_zotero",
                "message": "PDF attached via local Zotero",
                "attachment_key": materialized_result.get("attachment_key"),
            }
        return {
            "success": False,
            "pdf_source": "local_zotero",
            "message": (
                "local Zotero accepted the PDF attach request, but the attachment did not "
                f"materialize: {materialized_result.get('message', 'unknown error')}"
            ),
        }
    except Exception as exc:
        return {
            "success": False,
            "pdf_source": "local_zotero",
            "message": f"local Zotero attach failed: {exc}",
        }


def _download_pdf_bytes_via_playwright(
    pdf_url: str,
    *,
    ctx: Context | None = None,
    timeout_ms_override: int | None = None,
) -> tuple[bytes, str] | None:
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception:
        return None

    timeout_ms = timeout_ms_override or int(
        max(
            5.0,
            min(
                float(os.environ.get("ZOTERO_MCP_PLAYWRIGHT_PDF_TIMEOUT_SEC", "25")),
                120.0,
            ),
        )
        * 1000
    )
    headless = os.environ.get("ZOTERO_MCP_PLAYWRIGHT_HEADLESS", "1").strip().lower() not in {
        "0",
        "false",
        "no",
    }
    channel = os.environ.get("ZOTERO_MCP_PLAYWRIGHT_CHANNEL", "").strip() or None
    user_data_dir = os.environ.get("ZOTERO_MCP_PLAYWRIGHT_USER_DATA_DIR", "").strip()

    try:
        with sync_playwright() as playwright:
            launch_kwargs: dict[str, Any] = {"headless": headless}
            if channel:
                launch_kwargs["channel"] = channel

            browser = None
            if user_data_dir:
                context = playwright.chromium.launch_persistent_context(
                    user_data_dir,
                    accept_downloads=True,
                    ignore_https_errors=True,
                    **launch_kwargs,
                )
            else:
                browser = playwright.chromium.launch(**launch_kwargs)
                context = browser.new_context(
                    accept_downloads=True,
                    ignore_https_errors=True,
                )

            try:
                page = context.new_page()
                response_candidates: list[Any] = []
                response_urls: list[str] = []
                downloads: list[Any] = []

                def remember_response(response: Any) -> None:
                    try:
                        headers = response.headers or {}
                    except Exception:
                        headers = {}
                    content_type = str(headers.get("content-type") or headers.get("Content-Type") or "")
                    response_url = str(getattr(response, "url", "") or "")
                    if "application/pdf" in content_type.lower() or _looks_like_direct_pdf_url(response_url):
                        response_candidates.append(response)
                        if response_url:
                            response_urls.append(response_url)

                def remember_download(download: Any) -> None:
                    downloads.append(download)

                page.on("response", remember_response)
                page.on("download", remember_download)
                initial_response = page.goto(pdf_url, wait_until="domcontentloaded", timeout=timeout_ms)
                if initial_response is not None:
                    remember_response(initial_response)
                with suppress(Exception):
                    page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 8000))

                for response in response_candidates:
                    with suppress(Exception):
                        body = response.body()
                        headers = response.headers or {}
                        content_type = str(headers.get("content-type") or headers.get("Content-Type") or "")
                        if body and (
                            "application/pdf" in content_type.lower() or body.startswith(b"%PDF")
                        ):
                            return body, content_type or "application/pdf"

                for download in downloads:
                    with suppress(Exception):
                        with tempfile.TemporaryDirectory(prefix="zotero-mcp-playwright-download-") as tmpdir:
                            tmp_path = Path(tmpdir) / (download.suggested_filename or "download.pdf")
                            download.save_as(str(tmp_path))
                            body = tmp_path.read_bytes()
                            if body.startswith(b"%PDF"):
                                return body, "application/pdf"

                candidate_urls: list[str] = []
                for candidate in [str(page.url or ""), pdf_url, *response_urls]:
                    candidate = candidate.strip()
                    if candidate and candidate not in candidate_urls:
                        candidate_urls.append(candidate)

                with suppress(Exception):
                    embedded_urls = page.eval_on_selector_all(
                        "iframe, embed, object",
                        "els => els.map(el => el.src || el.data || '').filter(Boolean)",
                    )
                    for embedded_url in embedded_urls or []:
                        embedded_url = str(embedded_url or "").strip()
                        if embedded_url and embedded_url not in candidate_urls:
                            candidate_urls.append(embedded_url)

                referer = str(page.url or pdf_url)
                for candidate_url in candidate_urls:
                    with suppress(Exception):
                        response = context.request.get(
                            candidate_url,
                            headers={
                                "Referer": referer,
                                "Accept": "application/pdf,*/*",
                            },
                            timeout=timeout_ms,
                        )
                        body = response.body()
                        headers = response.headers or {}
                        content_type = str(headers.get("content-type") or headers.get("Content-Type") or "")
                        if body and (
                            "application/pdf" in content_type.lower() or body.startswith(b"%PDF")
                        ):
                            return body, content_type or "application/pdf"
            finally:
                context.close()
                if browser is not None:
                    browser.close()
    except Exception as exc:
        if ctx is not None:
            _ctx_warning(ctx, f"Playwright-assisted PDF rescue failed for {pdf_url}: {exc}")
        return None

    return None


def _download_pdf_bytes(
    pdf_url: str,
    *,
    ctx: Context | None = None,
    repair_mode: bool = False,
    deadline: float | None = None,
) -> tuple[bytes, str]:
    headers = {"User-Agent": "Mozilla/5.0 zotero-mcp/1.0"}
    errors: list[str] = []
    fast_fail_host = _publisher_pdf_fast_fail_host(pdf_url) if repair_mode else None
    if fast_fail_host and _playwright_browser_session_available():
        if ctx is not None:
            _ctx_warning(
                ctx,
                "Trying Playwright browser-session PDF rescue before fast-fail HTTP fetch",
            )
        playwright_result = _download_pdf_bytes_via_playwright(
            pdf_url,
            ctx=ctx,
            timeout_ms_override=int(_remaining_budget_seconds(deadline, 8.0) * 1000),
        )
        if playwright_result is not None:
            return playwright_result
        errors.append("playwright browser-session rescue failed")
    request_plans = [
        {
            "timeout": (6, 12) if repair_mode else (12, 35),
            "stream": False,
            "label": "buffered",
            "attempts": 2,
        },
        {
            "timeout": (8, 15) if repair_mode else (12, 35),
            "stream": True,
            "label": "stream",
            "attempts": 2,
        },
    ]
    if fast_fail_host:
        request_plans = [
            {
                "timeout": (3, 5),
                "stream": False,
                "label": "fast_fail",
                "attempts": 1,
            }
        ]

    for plan in request_plans:
        if _deadline_exceeded(deadline):
            break
        for attempt in range(1, plan.get("attempts", 1) + 1):
            if _deadline_exceeded(deadline):
                break
            try:
                effective_timeout = _clamp_timeout_to_deadline(
                    plan["timeout"],
                    deadline=deadline,
                )
                response = requests.get(
                    pdf_url,
                    timeout=effective_timeout,
                    stream=plan["stream"],
                    headers=headers,
                )
                response.raise_for_status()
                content_type = response.headers.get("Content-Type", "")
                if plan["stream"]:
                    first_chunk = b""
                    content = bytearray()
                    for chunk in response.iter_content(chunk_size=8192):
                        if not chunk:
                            continue
                        if not first_chunk:
                            first_chunk = chunk
                        content.extend(chunk)
                    pdf_bytes = bytes(content)
                else:
                    pdf_bytes = response.content or b""
                    first_chunk = pdf_bytes[:16]

                if "application/pdf" not in content_type.lower() and not first_chunk.startswith(b"%PDF"):
                    raise ValueError("response is not a PDF")
                return pdf_bytes, content_type
            except Exception as exc:
                errors.append(f"{plan['label']} attempt {attempt}: {exc}")
                if attempt < plan.get("attempts", 1) and _retryable_http_exception(exc):
                    time.sleep(0.5 * attempt)

    if fast_fail_host:
        raise RuntimeError(
            "; ".join(errors) if errors else f"publisher direct PDF fast-failed for host {fast_fail_host}"
        )

    if ctx is not None:
        _ctx_warning(
            ctx,
            "Direct PDF HTTP fetch did not complete within the bounded request budget; "
            "trying Playwright-assisted rescue",
        )
    playwright_result = _download_pdf_bytes_via_playwright(
        pdf_url,
        ctx=ctx,
        timeout_ms_override=(
            int(_remaining_budget_seconds(deadline, 12.0) * 1000)
            if repair_mode
            else None
        ),
    )
    if playwright_result is not None:
        return playwright_result

    raise RuntimeError("; ".join(errors) if errors else "failed to download PDF")


def _attach_pdf_from_url(
    zot,
    item_key: str,
    pdf_url: str,
    *,
    ctx: Context,
    source: str,
    repair_mode: bool = False,
    deadline: float | None = None,
) -> dict[str, Any]:
    if _deadline_exceeded(deadline):
        return {
            "success": False,
            "pdf_source": source,
            "message": "PDF attach skipped because the per-item repair budget was exhausted",
        }
    if _item_has_usable_pdf_attachment(item_key, zot=zot):
        return {
            "success": True,
            "pdf_source": "existing_attachment",
            "message": "PDF already attached; skipped duplicate upload",
            "skipped": True,
        }

    try:
        parent_payload = _get_item_payload(zot, item_key)
        if parent_payload is None:
            _ctx_warning(
                ctx,
                f"Item `{item_key}` is not readable via current client during PDF attach; "
                "preferring local Zotero path",
            )
        payload_for_filename = parent_payload or {}
        parent_item = parent_payload.get("data", {}) if isinstance(parent_payload, dict) else {}
        filename = _pdf_filename_for_item(
            (payload_for_filename.get("data", {}) if isinstance(payload_for_filename, dict) else {}) or {},
            pdf_url=pdf_url,
        )
        prefer_connector_url_first = _should_try_connector_url_fastpath(pdf_url) and (
            not repair_mode
            or _publisher_pdf_fast_fail_host(pdf_url) is not None
        )

        if prefer_connector_url_first:
            ctx.info("Trying local Zotero connector URL import before downloading bytes")
            connector_url_result = _save_pdf_via_local_connector_url(
                zot,
                item_key,
                pdf_url=pdf_url,
                filename=filename,
                ctx=ctx,
            )
            if connector_url_result.get("success"):
                return connector_url_result
            _ctx_warning(ctx, connector_url_result.get("message", "local connector URL fallback failed"))
            if repair_mode and _publisher_pdf_fast_fail_host(pdf_url) is not None:
                ctx.info("Trying local connector/browser-session PDF fetch after URL import failure")
                local_browser_fetch = _download_pdf_bytes_via_local_connector_browser_session(
                    pdf_url,
                    filename=filename,
                    ctx=ctx,
                    deadline=deadline,
                )
                if local_browser_fetch is not None:
                    pdf_bytes, _ = local_browser_fetch
                else:
                    pdf_bytes = None
            else:
                pdf_bytes = None
        elif _connector_url_fastpath_host(pdf_url) is not None:
            ctx.info(
                "Skipping local Zotero connector URL import for this host after a prior timeout; "
                "downloading PDF bytes directly"
            )
            pdf_bytes = None
        else:
            pdf_bytes = None

        if pdf_bytes is None:
            ctx.info(f"Downloading PDF from {pdf_url}")
            pdf_bytes, _ = _download_pdf_bytes(
                pdf_url,
                ctx=ctx,
                repair_mode=repair_mode,
                deadline=deadline,
            )
        with tempfile.TemporaryDirectory(prefix="zotero-mcp-") as tmpdir:
            tmp_path = Path(tmpdir) / filename
            tmp_path.write_bytes(pdf_bytes)
            try:
                if _should_prefer_local_pdf_after_download(
                    zot,
                    item_payload=parent_payload,
                    pdf_size_bytes=len(pdf_bytes),
                ):
                    ctx.info("Preferring local Zotero PDF handling after download")
                    connector_result = _save_pdf_via_local_connector_copy(
                        zot,
                        item_key,
                        tmp_path,
                        pdf_url=pdf_url,
                        ctx=ctx,
                    )
                    if connector_result.get("success"):
                        return connector_result

                    local_result = _attach_pdf_via_local_zotero(item_key, tmp_path, ctx=ctx)
                    if local_result.get("success"):
                        return local_result

                    recovered_result = _recover_materialized_local_copy_after_failure(
                        zot,
                        original_item_key=item_key,
                        parent_item=parent_item,
                        ctx=ctx,
                        pdf_source="local_zotero_file_attach_repair",
                        message_prefix="Recovered local Zotero copy after download-based attach repair",
                    )
                    if recovered_result.get("success"):
                        return recovered_result

                    return {
                        "success": False,
                        "pdf_source": "local_zotero",
                        "message": (
                            f"{connector_result.get('message', 'local connector fallback failed')}; "
                            f"{local_result.get('message', 'local Zotero attach failed')}; "
                            f"{recovered_result.get('message', 'no recovered local copy')}"
                        ),
                    }

                ctx.info("Trying direct web PDF attachment first")
                zot.attachment_simple([str(tmp_path)], item_key)
            except Exception as exc:
                if _should_try_local_pdf_fallback(exc) or parent_payload is None:
                    _ctx_warning(ctx, f"Falling back to local PDF handling after direct attach failure: {exc}")
                    materialized_result = _confirm_local_pdf_attachment_materialized(
                        item_key,
                        ctx=ctx,
                    )
                    if materialized_result.get("success"):
                        return materialized_result
                    connector_result = _save_pdf_via_local_connector_copy(
                        zot,
                        item_key,
                        tmp_path,
                        pdf_url=pdf_url,
                        ctx=ctx,
                    )
                    if connector_result.get("success"):
                        return connector_result
                    local_result = _attach_pdf_via_local_zotero(item_key, tmp_path, ctx=ctx)
                    if local_result.get("success"):
                        return local_result
                    recovered_result = _recover_materialized_local_copy_after_failure(
                        zot,
                        original_item_key=item_key,
                        parent_item=parent_item,
                        ctx=ctx,
                        pdf_source="local_zotero_file_attach_repair",
                        message_prefix="Recovered local Zotero copy after direct attach failure",
                    )
                    if recovered_result.get("success"):
                        return recovered_result
                    return local_result
                raise
        return {
            "success": True,
            "pdf_source": source,
            "message": f"PDF attached from {source}",
        }
    except Exception as exc:
        return {
            "success": False,
            "pdf_source": source,
            "message": f"PDF attach failed from {source}: {exc}",
        }


def _attach_unpaywall_pdf(
    zot,
    doi: str,
    item_key: str,
    email: str,
    ctx: Context,
    *,
    repair_mode: bool = False,
    deadline: float | None = None,
) -> dict[str, Any]:
    try:
        resp = _requests_get_with_retry(
            f"https://api.unpaywall.org/v2/{doi}",
            params={"email": email},
            timeout=15,
            ctx=ctx,
            deadline=deadline,
        )
        data = resp.json()
        best = data.get("best_oa_location") or {}
        pdf_url = best.get("url_for_pdf") or best.get("url")
        if not pdf_url:
            return {
                "success": False,
                "pdf_source": "unpaywall",
                "message": f"no OA PDF found for {doi}",
            }
        return _attach_pdf_from_url(
            zot,
            item_key,
            pdf_url,
            ctx=ctx,
            source="unpaywall",
            repair_mode=repair_mode,
            deadline=deadline,
        )
    except Exception as exc:
        return {
            "success": False,
            "pdf_source": "unpaywall",
            "message": f"PDF attach failed via Unpaywall: {exc}",
        }


def _discover_unpaywall_pdf_candidate(
    doi: str,
    email: str,
    *,
    ctx: Context | None = None,
    deadline: float | None = None,
) -> dict[str, str] | None:
    if not email:
        return None
    resp = _requests_get_with_retry(
        f"https://api.unpaywall.org/v2/{doi}",
        params={"email": email},
        timeout=15,
        ctx=ctx,
        deadline=deadline,
    )
    data = resp.json()
    best = data.get("best_oa_location") or {}
    pdf_url = best.get("url_for_pdf") or best.get("url")
    if not pdf_url:
        return None
    return {"source": "unpaywall", "url": str(pdf_url).strip()}


def _discover_openalex_pdf_candidate(
    doi: str,
    *,
    deadline: float | None = None,
) -> dict[str, str] | None:
    resp = _requests_get_with_retry(
        "https://api.openalex.org/works",
        params={
            "filter": f"doi:{doi}",
            "per-page": 1,
            "select": "best_oa_location,primary_location,open_access",
        },
        timeout=15,
        deadline=deadline,
    )
    results = (resp.json() or {}).get("results") or []
    if not results:
        return None

    work = results[0] or {}
    location_candidates = [
        ("openalex:best_oa_location", work.get("best_oa_location") or {}),
        ("openalex:primary_location", work.get("primary_location") or {}),
        ("openalex:open_access", work.get("open_access") or {}),
    ]
    for source, location in location_candidates:
        pdf_url = location.get("pdf_url") or location.get("url_for_pdf")
        if pdf_url:
            return {"source": source, "url": pdf_url}
        oa_url = location.get("oa_url")
        if oa_url and (
            _looks_like_direct_pdf_url(oa_url)
            or "arxiv.org/pdf/" in oa_url
        ):
            return {"source": source, "url": oa_url}
    return None


def _discover_oa_pdf_candidates_parallel(
    doi: str,
    *,
    email: str,
    ctx: Context,
    deadline: float | None = None,
) -> list[dict[str, str]]:
    if _deadline_exceeded(deadline):
        return []

    tasks: list[tuple[str, Any]] = []
    if email:
        tasks.append(
            (
                "unpaywall",
                lambda: _discover_unpaywall_pdf_candidate(
                    doi,
                    email,
                    ctx=ctx,
                    deadline=deadline,
                ),
            )
        )
    tasks.append(
        (
            "openalex",
            lambda: _discover_openalex_pdf_candidate(
                doi,
                deadline=deadline,
            ),
        )
    )

    if not tasks:
        return []

    found: dict[str, dict[str, str]] = {}
    with ThreadPoolExecutor(max_workers=min(2, len(tasks))) as executor:
        future_to_name = {
            executor.submit(task_fn): task_name
            for task_name, task_fn in tasks
        }
        for future in as_completed(future_to_name):
            task_name = future_to_name[future]
            try:
                result = future.result()
            except Exception as exc:
                _ctx_warning(ctx, f"{task_name} OA probe failed for {doi}: {exc}")
                continue
            if result and result.get("url"):
                found[task_name] = result

    ordered: list[dict[str, str]] = []
    for task_name in ["unpaywall", "openalex"]:
        result = found.get(task_name)
        if result:
            ordered.append(result)
    return _dedupe_pdf_candidates(ordered)


def _discover_europepmc_fulltext_candidate(
    doi: str,
    *,
    deadline: float | None = None,
) -> dict[str, str] | None:
    resp = _requests_get_with_retry(
        "https://www.ebi.ac.uk/europepmc/webservices/rest/search",
        params={
            "query": f"DOI:{doi}",
            "format": "json",
            "pageSize": 1,
        },
        timeout=15,
        headers={"User-Agent": "Mozilla/5.0 zotero-mcp/1.0"},
        deadline=deadline,
    )
    results = ((resp.json() or {}).get("resultList") or {}).get("result") or []
    if not results:
        return None

    result = results[0] or {}
    pmcid = str(result.get("pmcid") or "").strip()
    fulltext_ids = ((result.get("fullTextIdList") or {}).get("fullTextId")) or []
    if not pmcid and fulltext_ids:
        pmcid = str(fulltext_ids[0] or "").strip()
    if not pmcid:
        return None
    if not pmcid.upper().startswith("PMC"):
        pmcid = f"PMC{pmcid}"

    return {
        "source": "europepmc_fulltext_surrogate",
        "pmcid": pmcid,
        "title": str(result.get("title") or "").strip(),
        "journal": str(result.get("journalTitle") or "").strip(),
        "year": str(result.get("pubYear") or "").strip(),
    }


def _clean_plaintext_for_pdf(text: str) -> str:
    text = unescape(text or "")
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""
    return text.encode("latin-1", "replace").decode("latin-1")


def _extract_europepmc_fulltext_lines(
    pmcid: str,
    *,
    deadline: float | None = None,
) -> list[str]:
    resp = _requests_get_with_retry(
        f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML",
        timeout=20,
        headers={"User-Agent": "Mozilla/5.0 zotero-mcp/1.0"},
        deadline=deadline,
    )

    root = ET.fromstring(resp.content)
    lines: list[str] = []

    def push_line(raw: str) -> None:
        cleaned = _clean_plaintext_for_pdf(raw)
        if cleaned:
            lines.append(cleaned)

    title_elem = root.find(".//article-title")
    if title_elem is not None:
        push_line("Title: " + "".join(title_elem.itertext()))

    abstract_paragraphs = root.findall(".//abstract//p")
    if abstract_paragraphs:
        push_line("")
        push_line("Abstract")
        for paragraph in abstract_paragraphs:
            push_line("".join(paragraph.itertext()))

    body_sections = root.findall(".//body//sec")
    seen_section_titles: set[str] = set()
    for section in body_sections:
        title_node = section.find("./title")
        title_text = _clean_plaintext_for_pdf("".join(title_node.itertext())) if title_node is not None else ""
        if title_text and title_text not in seen_section_titles:
            push_line("")
            push_line(title_text)
            seen_section_titles.add(title_text)
        for paragraph in section.findall("./p"):
            push_line("".join(paragraph.itertext()))

    if not lines:
        for paragraph in root.findall(".//body//p"):
            push_line("".join(paragraph.itertext()))

    return [line for line in lines if line][:400]


def _pdf_literal_bytes(text: str) -> bytes:
    text = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    return text.encode("latin-1", "replace")


def _build_simple_text_pdf_bytes(*, title: str, lines: list[str]) -> bytes:
    wrapped_lines: list[str] = []
    normalized_title = _clean_plaintext_for_pdf(title) or "Document"
    wrapped_lines.append(normalized_title)
    wrapped_lines.append("")
    for line in lines:
        current = _clean_plaintext_for_pdf(line)
        if not current:
            wrapped_lines.append("")
            continue
        wrapped_lines.extend(textwrap.wrap(current, width=95) or [""])

    if not wrapped_lines:
        wrapped_lines = [normalized_title, "", "No extractable full text was available."]

    page_size = 48
    pages = [
        wrapped_lines[idx: idx + page_size]
        for idx in range(0, len(wrapped_lines), page_size)
    ] or [["No content"]]

    objects: list[bytes] = []
    page_object_numbers: list[int] = []
    content_object_numbers: list[int] = []
    font_object_number = 3

    next_object_number = 4
    for _ in pages:
        page_object_numbers.append(next_object_number)
        content_object_numbers.append(next_object_number + 1)
        next_object_number += 2

    pages_kids = " ".join(f"{num} 0 R" for num in page_object_numbers)
    objects.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    objects.append(f"<< /Type /Pages /Count {len(page_object_numbers)} /Kids [{pages_kids}] >>".encode("ascii"))
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    for page_object_number, content_object_number, page_lines in zip(
        page_object_numbers,
        content_object_numbers,
        pages,
    ):
        stream_lines = [
            b"BT",
            b"/F1 10 Tf",
            b"50 780 Td",
            b"14 TL",
        ]
        for line in page_lines:
            stream_lines.append(b"(" + _pdf_literal_bytes(line) + b") Tj")
            stream_lines.append(b"T*")
        stream_lines.append(b"ET")
        stream = b"\n".join(stream_lines) + b"\n"
        objects.append(
            (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                f"/Resources << /Font << /F1 {font_object_number} 0 R >> >> "
                f"/Contents {content_object_number} 0 R >>"
            ).encode("ascii")
        )
        objects.append(
            b"<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n"
            + stream
            + b"endstream"
        )

    pdf = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for idx, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{idx} 0 obj\n".encode("ascii"))
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")

    xref_offset = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    pdf.extend(
        (
            f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
            f"startxref\n{xref_offset}\n%%EOF\n"
        ).encode("ascii")
    )
    return bytes(pdf)


def _attach_pdf_bytes(
    zot,
    item_key: str,
    pdf_bytes: bytes,
    *,
    filename: str,
    ctx: Context,
    source: str,
) -> dict[str, Any]:
    if _item_has_usable_pdf_attachment(item_key, zot=zot):
        return {
            "success": True,
            "pdf_source": "existing_attachment",
            "message": "PDF already attached; skipped duplicate upload",
            "skipped": True,
        }

    try:
        parent_payload = _get_item_payload(zot, item_key)
        parent_item = parent_payload.get("data", {}) if isinstance(parent_payload, dict) else {}

        with tempfile.TemporaryDirectory(prefix="zotero-mcp-generated-") as tmpdir:
            tmp_path = Path(tmpdir) / filename
            tmp_path.write_bytes(pdf_bytes)
            try:
                if _should_prefer_local_pdf_after_download(
                    zot,
                    item_payload=parent_payload,
                    pdf_size_bytes=len(pdf_bytes),
                ):
                    connector_result = _save_pdf_via_local_connector_copy(
                        zot,
                        item_key,
                        tmp_path,
                        pdf_url=parent_item.get("url") or "",
                        ctx=ctx,
                    )
                    if connector_result.get("success"):
                        return connector_result

                    local_result = _attach_pdf_via_local_zotero(item_key, tmp_path, ctx=ctx)
                    if local_result.get("success"):
                        return local_result

                    recovered_result = _recover_materialized_local_copy_after_failure(
                        zot,
                        original_item_key=item_key,
                        parent_item=parent_item,
                        ctx=ctx,
                        pdf_source="local_zotero_file_attach_repair",
                        message_prefix="Recovered local Zotero copy after generated-PDF attach repair",
                    )
                    if recovered_result.get("success"):
                        return recovered_result

                    return {
                        "success": False,
                        "pdf_source": source,
                        "message": (
                            f"{connector_result.get('message', 'local connector fallback failed')}; "
                            f"{local_result.get('message', 'local Zotero attach failed')}; "
                            f"{recovered_result.get('message', 'no recovered local copy')}"
                        ),
                    }

                zot.attachment_simple([str(tmp_path)], item_key)
            except Exception as exc:
                if _should_try_local_pdf_fallback(exc) or parent_payload is None:
                    materialized_result = _confirm_local_pdf_attachment_materialized(
                        item_key,
                        ctx=ctx,
                    )
                    if materialized_result.get("success"):
                        return materialized_result
                    connector_result = _save_pdf_via_local_connector_copy(
                        zot,
                        item_key,
                        tmp_path,
                        pdf_url=parent_item.get("url") or "",
                        ctx=ctx,
                    )
                    if connector_result.get("success"):
                        return connector_result
                    local_result = _attach_pdf_via_local_zotero(item_key, tmp_path, ctx=ctx)
                    if local_result.get("success"):
                        return local_result
                    recovered_result = _recover_materialized_local_copy_after_failure(
                        zot,
                        original_item_key=item_key,
                        parent_item=parent_item,
                        ctx=ctx,
                        pdf_source="local_zotero_file_attach_repair",
                        message_prefix="Recovered local Zotero copy after generated-PDF direct attach failure",
                    )
                    if recovered_result.get("success"):
                        return recovered_result
                    return local_result
                raise

        return {
            "success": True,
            "pdf_source": source,
            "message": f"PDF attached from {source}",
        }
    except Exception as exc:
        return {
            "success": False,
            "pdf_source": source,
            "message": f"PDF attach failed from {source}: {exc}",
        }


def _attach_europepmc_fulltext_pdf(
    zot,
    doi: str,
    item_key: str,
    ctx: Context,
    *,
    deadline: float | None = None,
) -> dict[str, Any]:
    try:
        if _deadline_exceeded(deadline):
            return {
                "success": False,
                "pdf_source": "europepmc_fulltext_surrogate",
                "message": f"repair budget exhausted before EuropePMC fallback for {doi}",
            }
        candidate = _discover_europepmc_fulltext_candidate(doi, deadline=deadline)
        if not candidate:
            return {
                "success": False,
                "pdf_source": "europepmc_fulltext_surrogate",
                "message": f"no EuropePMC full-text candidate found for {doi}",
            }

        lines = _extract_europepmc_fulltext_lines(candidate["pmcid"], deadline=deadline)
        if not lines:
            return {
                "success": False,
                "pdf_source": "europepmc_fulltext_surrogate",
                "message": f"EuropePMC full-text XML did not contain extractable text for {doi}",
            }

        parent_payload = _get_item_payload(zot, item_key) or {}
        parent_item = parent_payload.get("data", {}) if isinstance(parent_payload, dict) else {}
        filename = _pdf_filename_for_item(
            parent_item,
            pdf_url=f"https://www.ebi.ac.uk/europepmc/webservices/rest/{candidate['pmcid']}/fullTextXML",
        )
        if filename.lower().endswith(".pdf"):
            filename = filename[:-4] + " (EuropePMC full text).pdf"

        title = candidate.get("title") or parent_item.get("title") or doi
        header_lines = [
            f"DOI: {doi}",
            f"PMCID: {candidate['pmcid']}",
        ]
        if candidate.get("journal"):
            header_lines.append(f"Journal: {candidate['journal']}")
        if candidate.get("year"):
            header_lines.append(f"Year: {candidate['year']}")
        header_lines.append("")
        header_lines.append(
            "This PDF was generated from EuropePMC full-text XML because the publisher PDF "
            "could not be fetched automatically."
        )
        header_lines.append("")

        pdf_bytes = _build_simple_text_pdf_bytes(
            title=title,
            lines=header_lines + lines,
        )
        return _attach_pdf_bytes(
            zot,
            item_key,
            pdf_bytes,
            filename=filename,
            ctx=ctx,
            source="europepmc_fulltext_surrogate",
        )
    except Exception as exc:
        return {
            "success": False,
            "pdf_source": "europepmc_fulltext_surrogate",
            "message": f"PDF attach failed via EuropePMC full-text surrogate: {exc}",
        }


def _crossref_license_looks_open(work: dict[str, Any]) -> bool:
    for license_entry in work.get("license") or []:
        url = str((license_entry or {}).get("URL") or "").lower()
        if "creativecommons.org" in url or "openaccess" in url:
            return True
    return False


def _attach_crossref_metadata_surrogate_pdf(
    zot,
    doi: str,
    item_key: str,
    ctx: Context,
    *,
    work: dict[str, Any] | None = None,
    deadline: float | None = None,
) -> dict[str, Any]:
    try:
        if _deadline_exceeded(deadline):
            return {
                "success": False,
                "pdf_source": "crossref_metadata_surrogate",
                "message": f"repair budget exhausted before Crossref surrogate for {doi}",
            }
        if work is None:
            resp = _requests_get_with_retry(
                f"https://api.crossref.org/works/{doi}",
                headers={"User-Agent": "zotero-mcp/1.0 (mailto:user@example.com)"},
                timeout=15,
                deadline=deadline,
            )
            work = resp.json().get("message", {})

        if not work:
            return {
                "success": False,
                "pdf_source": "crossref_metadata_surrogate",
                "message": f"no Crossref work available for {doi}",
            }

        if not _crossref_license_looks_open(work):
            return {
                "success": False,
                "pdf_source": "crossref_metadata_surrogate",
                "message": f"Crossref does not indicate an open license for {doi}",
            }

        title = ((work.get("title") or [""]) or [""])[0] or doi
        abstract = clean_html(work.get("abstract", "") or "")
        if not abstract.strip():
            return {
                "success": False,
                "pdf_source": "crossref_metadata_surrogate",
                "message": f"Crossref abstract unavailable for {doi}",
            }

        parent_payload = _get_item_payload(zot, item_key) or {}
        parent_item = parent_payload.get("data", {}) if isinstance(parent_payload, dict) else {}
        filename = _pdf_filename_for_item(parent_item, pdf_url=f"https://doi.org/{doi}")
        if filename.lower().endswith(".pdf"):
            filename = filename[:-4] + " (metadata summary).pdf"

        author_names = []
        for author in work.get("author") or []:
            family = str(author.get("family") or "").strip()
            given = str(author.get("given") or "").strip()
            full = " ".join(part for part in [given, family] if part).strip()
            if full:
                author_names.append(full)

        lines = [
            f"DOI: {doi}",
            f"Journal: {((work.get('container-title') or ['']) or [''])[0] or ''}",
            f"Year: {_work_year(work)}",
        ]
        if author_names:
            lines.append("Authors: " + ", ".join(author_names[:12]))
        lines.extend(
            [
                "",
                "This PDF was generated from Crossref metadata because the publisher PDF could not be fetched automatically.",
                "",
                "Abstract",
                abstract,
            ]
        )
        pdf_bytes = _build_simple_text_pdf_bytes(title=title, lines=lines)
        return _attach_pdf_bytes(
            zot,
            item_key,
            pdf_bytes,
            filename=filename,
            ctx=ctx,
            source="crossref_metadata_surrogate",
        )
    except Exception as exc:
        return {
            "success": False,
            "pdf_source": "crossref_metadata_surrogate",
            "message": f"PDF attach failed via Crossref metadata surrogate: {exc}",
        }


def _attach_openalex_pdf(
    zot,
    doi: str,
    item_key: str,
    ctx: Context,
    *,
    repair_mode: bool = False,
    deadline: float | None = None,
) -> dict[str, Any]:
    try:
        if _deadline_exceeded(deadline):
            return {
                "success": False,
                "pdf_source": "openalex",
                "message": f"repair budget exhausted before OpenAlex fallback for {doi}",
            }
        candidate = _discover_openalex_pdf_candidate(doi, deadline=deadline)
        if not candidate:
            return {
                "success": False,
                "pdf_source": "openalex",
                "message": f"no OA PDF found via OpenAlex for {doi}",
            }
        return _attach_pdf_from_url(
            zot,
            item_key,
            candidate["url"],
            ctx=ctx,
            source=candidate["source"],
            repair_mode=repair_mode,
            deadline=deadline,
        )
    except Exception as exc:
        return {
            "success": False,
            "pdf_source": "openalex",
            "message": f"PDF attach failed via OpenAlex: {exc}",
        }


def _attach_pdf_with_cascade(
    zot,
    item_key: str,
    *,
    pdf_candidates: list[dict[str, str]] | None,
    doi: str | None,
    crossref_work: dict[str, Any] | None = None,
    collection_key: str | None,
    ctx: Context,
    repair_mode: bool = False,
    deadline: float | None = None,
) -> dict[str, Any]:
    pdf_candidates = pdf_candidates or []
    deadline = deadline or (time.monotonic() + _repair_pdf_budget_seconds() if repair_mode else None)
    if _item_has_usable_pdf_attachment(item_key, zot=zot):
        return {
            "success": True,
            "pdf_source": "existing_attachment",
            "message": "PDF already attached; skipped duplicate upload",
            "skipped": True,
        }
    if _deadline_exceeded(deadline):
        return {
            "success": False,
            "pdf_source": "none",
            "message": "per-item repair budget exhausted before starting PDF cascade",
        }

    failures: list[str] = []
    for candidate in pdf_candidates:
        if _deadline_exceeded(deadline):
            failures.append("per-item repair budget exhausted before trying remaining direct PDF candidates")
            break
        result = _attach_pdf_from_url(
            zot,
            item_key,
            candidate["url"],
            ctx=ctx,
            source=candidate["source"],
            repair_mode=repair_mode,
            deadline=deadline,
        )
        if result.get("success"):
            return result
        failures.append(result["message"])

    if repair_mode and not _repair_budget_allows_fallback(deadline):
        failures.append("per-item repair budget exhausted after direct PDF candidates")
        return {
            "success": False,
            "pdf_source": "none",
            "message": "; ".join(failures),
        }

    email = os.environ.get("UNPAYWALL_EMAIL", "")
    if repair_mode and not _repair_budget_allows_fallback(deadline):
        failures.append("per-item repair budget exhausted before OA fallback")
        return {
            "success": False,
            "pdf_source": "none",
            "message": "; ".join(failures),
        }

    if doi:
        oa_candidates = _discover_oa_pdf_candidates_parallel(
            doi,
            email=email,
            ctx=ctx,
            deadline=deadline,
        )
        if not oa_candidates:
            if email:
                failures.append(f"no OA PDF found via Unpaywall/OpenAlex for {doi}")
            else:
                failures.append("UNPAYWALL_EMAIL not set; OpenAlex found no OA PDF")
        for candidate in oa_candidates:
            if _deadline_exceeded(deadline):
                failures.append("per-item repair budget exhausted during OA candidate attach")
                break
            result = _attach_pdf_from_url(
                zot,
                item_key,
                candidate["url"],
                ctx=ctx,
                source=candidate["source"],
                repair_mode=repair_mode,
                deadline=deadline,
            )
            if result.get("success"):
                return result
            failures.append(result["message"])

    if repair_mode and not _repair_budget_allows_fallback(deadline):
        failures.append("per-item repair budget exhausted before EuropePMC fallback")
        return {
            "success": False,
            "pdf_source": "none",
            "message": "; ".join(failures),
        }

    if doi:
        result = _attach_europepmc_fulltext_pdf(
            zot,
            doi,
            item_key,
            ctx,
            deadline=deadline,
        )
        if result.get("success"):
            return result
        failures.append(result["message"])

    if repair_mode and not _repair_budget_allows_fallback(deadline, min_remaining_seconds=1.0):
        failures.append("per-item repair budget exhausted before Crossref surrogate")
        return {
            "success": False,
            "pdf_source": "none",
            "message": "; ".join(failures),
        }

    if doi:
        result = _attach_crossref_metadata_surrogate_pdf(
            zot,
            doi,
            item_key,
            ctx,
            work=crossref_work,
            deadline=deadline,
        )
        if result.get("success"):
            return result
        failures.append(result["message"])

    return {
        "success": False,
        "pdf_source": "none",
        "message": "; ".join(failures) if failures else "no PDF candidate found",
    }


def _crossref_item_type(work_type: str | None) -> str:
    mapping = {
        "proceedings-article": "conferencePaper",
        "journal-article": "journalArticle",
        "posted-content": "preprint",
        "book-chapter": "bookSection",
        "book": "book",
    }
    return mapping.get((work_type or "").lower(), "journalArticle")


def _import_output_debug_enabled() -> bool:
    value = os.environ.get("ZOTERO_MCP_DEBUG_IMPORT", "").strip().lower()
    return value in {"1", "true", "yes", "on", "debug", "verbose"}


def _summarize_import_status(
    *,
    route: str,
    pdf_source: str,
    fallback_reason: str,
) -> str:
    if route == "webpage":
        item_text = "Saved as webpage"
    else:
        item_text = "Imported as paper"

    if pdf_source != "none":
        return f"{item_text} + PDF attached"
    return item_text


def _format_import_note(
    *,
    route: str,
    pdf_source: str,
    fallback_reason: str,
    pdf_message: str | None = None,
) -> str | None:
    if _import_output_debug_enabled():
        return pdf_message or None

    notes: list[str] = []
    if route == "webpage" and fallback_reason != "none":
        notes.append("No DOI/arXiv identifier was found, so this was saved as a webpage.")
    if pdf_source == "none":
        notes.append("PDF was not attached automatically.")
    if not notes:
        return None
    return " ".join(notes)


def _append_import_note(
    results: list[str],
    *,
    route: str,
    pdf_source: str,
    fallback_reason: str,
    pdf_message: str | None = None,
) -> None:
    note = _format_import_note(
        route=route,
        pdf_source=pdf_source,
        fallback_reason=fallback_reason,
        pdf_message=pdf_message,
    )
    if note:
        results.append(f"  {note}")


def _format_pdf_attach_result(
    *,
    item_key: str,
    success: bool,
    pdf_source: str,
    message: str,
    promoted_item_key: str | None = None,
    local_item_key: str | None = None,
) -> str:
    if _import_output_debug_enabled():
        key_hint = ""
        effective_key = promoted_item_key or local_item_key
        if effective_key and effective_key != item_key:
            key_hint = f" [effective_key={effective_key}]"
        status = "✓" if success else "✗"
        return f"{status} {item_key}: {message} [pdf_source={pdf_source}]{key_hint}"

    if success:
        if pdf_source == "existing_attachment":
            return f"✓ {item_key}: PDF already attached"
        if promoted_item_key and promoted_item_key != item_key:
            return f"✓ {item_key}: PDF attached (effective item `{promoted_item_key}`)"
        return f"✓ {item_key}: PDF attached"
    return f"✗ {item_key}: PDF not attached"


def _format_import_result(
    *,
    success: bool,
    label: str,
    key: str | None = None,
    route: str,
    pdf_source: str = "none",
    fallback_reason: str = "none",
    local_item_key: str | None = None,
    error: str | None = None,
) -> str:
    if _import_output_debug_enabled():
        if success:
            return (
                f"✓ {label} → key `{key or '?'}` "
                f"[route={route}]"
                f"[pdf_source={pdf_source}]"
                f"[fallback_reason={fallback_reason}]"
                + (f"[local_item_key={local_item_key}]" if local_item_key else "")
            )
        return (
            f"✗ {label}: {error or 'unknown error'} "
            f"[route={route}]"
            f"[pdf_source={pdf_source}]"
            f"[fallback_reason={fallback_reason}]"
        )

    if success:
        return f"✓ {label} → key `{key or '?'}` — {_summarize_import_status(route=route, pdf_source=pdf_source, fallback_reason=fallback_reason)}"
    return f"✗ {label}: {error or 'unknown error'}"


def _record_import_event(
    *,
    action: Literal["import", "reconcile"],
    status: str,
    input_value: str | None,
    route: str,
    label: str | None = None,
    item_key: str | None = None,
    local_item_key: str | None = None,
    pdf_source: str = "none",
    fallback_reason: str = "none",
    collection_key: str | None = None,
    collection_path: str | None = None,
    resolved_identifier: str | None = None,
    actual_selected_collection_id: str | None = None,
    actual_selected_target: str | None = None,
    intended_target: str | None = None,
    reconcile_status: str | None = None,
    reconcile_message: str | None = None,
    message: str | None = None,
    error: str | None = None,
    ctx: Context | None = None,
) -> None:
    _append_import_ledger(
        {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "action": action,
            "status": status,
            "input": input_value,
            "resolved_identifier": resolved_identifier,
            "route": route,
            "label": label,
            "item_key": item_key,
            "local_item_key": local_item_key,
            "pdf_source": pdf_source,
            "fallback_reason": fallback_reason,
            "collection_key": collection_key,
            "collection_path": collection_path,
            "actual_selected_collection_id": actual_selected_collection_id,
            "actual_selected_target": actual_selected_target,
            "intended_target": intended_target,
            "reconcile_status": reconcile_status,
            "reconcile_message": reconcile_message,
            "message": message,
            "error": error,
        },
        ctx=ctx,
    )


def _fetch_crossref_work(
    doi: str,
    *,
    deadline: float | None = None,
) -> dict[str, Any]:
    resp = _requests_get_with_retry(
        f"https://api.crossref.org/works/{doi}",
        headers={"User-Agent": "zotero-mcp/1.0 (mailto:user@example.com)"},
        timeout=15,
        deadline=deadline,
    )
    return resp.json().get("message", {})


def _create_item_from_doi(
    zot,
    doi: str,
    *,
    collection_key: str | None,
    attach_pdf: bool,
    ctx: Context,
    pdf_candidates: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    work = _fetch_crossref_work(doi)

    desired_title = work.get("title", [""])[0] or doi
    reused = _reuse_existing_local_copy_for_import(
        collection_key=collection_key,
        doi=doi,
        title=desired_title,
        url=work.get("URL", f"https://doi.org/{doi}"),
        route="doi",
        ctx=ctx,
    )
    if reused:
        return reused

    item_type = _crossref_item_type(work.get("type"))
    template = zot.item_template(item_type)
    template["title"] = desired_title
    template["DOI"] = doi
    template["url"] = work.get("URL", f"https://doi.org/{doi}")
    template["creators"] = [
        {
            "creatorType": "author",
            "firstName": author.get("given", ""),
            "lastName": author.get("family", ""),
        }
        for author in work.get("author", [])
    ]

    date_parts = work.get("published", {}).get("date-parts", [[]])
    if date_parts and date_parts[0]:
        template["date"] = "-".join(str(part) for part in date_parts[0])

    container_title = work.get("container-title", [""])[0]
    if container_title:
        if "publicationTitle" in template:
            template["publicationTitle"] = container_title
        elif "proceedingsTitle" in template:
            template["proceedingsTitle"] = container_title
        elif "conferenceName" in template:
            template["conferenceName"] = container_title

    if "volume" in template and work.get("volume") is not None:
        template["volume"] = str(work.get("volume", ""))
    if "issue" in template and work.get("issue") is not None:
        template["issue"] = str(work.get("issue", ""))
    if "pages" in template:
        template["pages"] = work.get("page", "")
    if "abstractNote" in template:
        template["abstractNote"] = clean_html(work.get("abstract", ""))

    if collection_key:
        template["collections"] = [collection_key]

    discovered_pdf_candidates = _discover_pdf_candidates_from_crossref_work(
        work,
        doi=doi,
        ctx=ctx,
    )
    effective_pdf_candidates = _dedupe_pdf_candidates(
        (pdf_candidates or []) + discovered_pdf_candidates
    )

    create_resp = zot.create_items([template])
    created = create_resp.get("successful", {})
    if not created:
        raise RuntimeError(str(create_resp.get("failed", {})))

    key = list(created.values())[0].get("key", "?")
    pdf_result = {"success": False, "pdf_source": "none", "message": "PDF attachment not requested"}
    if attach_pdf:
        pdf_result = _attach_pdf_with_cascade(
            zot,
            key,
            pdf_candidates=effective_pdf_candidates,
            doi=doi,
            crossref_work=work,
            collection_key=collection_key,
            ctx=ctx,
        )
    effective_key = pdf_result.get("promoted_item_key") or key

    return {
        "success": True,
        "label": template["title"],
        "key": effective_key,
        "route": "doi",
        "pdf_source": pdf_result.get("pdf_source", "none") if pdf_result.get("success") else "none",
        "fallback_reason": "none",
        "pdf_message": pdf_result.get("message", ""),
        "local_item_key": pdf_result.get("local_item_key"),
        "actual_selected_collection_id": pdf_result.get("actual_selected_collection_id"),
        "actual_selected_target": pdf_result.get("actual_selected_target"),
        "intended_target": pdf_result.get("intended_target"),
        "reconcile_status": pdf_result.get("reconcile_status"),
        "reconcile_message": pdf_result.get("reconcile_message"),
    }


def _fetch_arxiv_entry(arxiv_id: str) -> tuple[Any, dict[str, str]]:
    import urllib.request
    import xml.etree.ElementTree as ET

    url = f"http://export.arxiv.org/api/query?id_list={arxiv_id}"
    with urllib.request.urlopen(url, timeout=15) as response:
        xml_data = response.read()

    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "arxiv": "http://arxiv.org/schemas/atom",
    }
    root = ET.fromstring(xml_data)
    entry = root.find("atom:entry", ns)
    return entry, ns


def _create_item_from_arxiv(
    zot,
    arxiv_id: str,
    *,
    collection_key: str | None,
    attach_pdf: bool,
    ctx: Context,
) -> dict[str, Any]:
    entry, ns = _fetch_arxiv_entry(arxiv_id)
    if entry is None:
        raise RuntimeError(f"{arxiv_id}: not found on arXiv")

    title = (entry.findtext("atom:title", "", ns) or "").strip().replace("\n", " ")
    abstract = (entry.findtext("atom:summary", "", ns) or "").strip()
    published = (entry.findtext("atom:published", "", ns) or "")[:10]
    doi_elem = entry.find("arxiv:doi", ns)
    doi = doi_elem.text.strip() if doi_elem is not None and doi_elem.text else ""
    category_elem = entry.find("atom:category", ns)
    category = category_elem.get("term", "") if category_elem is not None else ""

    authors = []
    for author_elem in entry.findall("atom:author", ns):
        name = author_elem.findtext("atom:name", "", ns).strip()
        parts = name.rsplit(" ", 1)
        if len(parts) == 2:
            authors.append({"creatorType": "author", "firstName": parts[0], "lastName": parts[1]})
        else:
            authors.append({"creatorType": "author", "firstName": "", "lastName": name})

    reused = _reuse_existing_local_copy_for_import(
        collection_key=collection_key,
        doi=doi or None,
        title=title or arxiv_id,
        url=f"https://arxiv.org/abs/{arxiv_id}",
        arxiv_id=arxiv_id,
        route="arxiv",
        ctx=ctx,
    )
    if reused:
        return reused

    template = zot.item_template("preprint")
    template["title"] = title or arxiv_id
    template["abstractNote"] = abstract
    template["date"] = published
    template["creators"] = authors
    template["url"] = f"https://arxiv.org/abs/{arxiv_id}"
    template["repository"] = "arXiv"
    template["archiveID"] = f"arXiv:{arxiv_id}"
    if doi:
        template["DOI"] = doi
    if category:
        template["tags"] = [{"tag": category}]
    if collection_key:
        template["collections"] = [collection_key]

    create_resp = zot.create_items([template])
    created = create_resp.get("successful", {})
    if not created:
        raise RuntimeError(str(create_resp.get("failed", {})))

    key = list(created.values())[0].get("key", "?")
    pdf_result = {"success": False, "pdf_source": "none", "message": "PDF attachment not requested"}
    if attach_pdf:
        pdf_result = _attach_pdf_with_cascade(
            zot,
            key,
            pdf_candidates=[{"source": "arxiv_pdf", "url": f"https://arxiv.org/pdf/{arxiv_id}.pdf"}],
            doi=doi or None,
            collection_key=collection_key,
            ctx=ctx,
        )
    effective_key = pdf_result.get("promoted_item_key") or key

    return {
        "success": True,
        "label": template["title"],
        "key": effective_key,
        "route": "arxiv",
        "pdf_source": pdf_result.get("pdf_source", "none") if pdf_result.get("success") else "none",
        "fallback_reason": "none",
        "pdf_message": pdf_result.get("message", ""),
        "local_item_key": pdf_result.get("local_item_key"),
        "actual_selected_collection_id": pdf_result.get("actual_selected_collection_id"),
        "actual_selected_target": pdf_result.get("actual_selected_target"),
        "intended_target": pdf_result.get("intended_target"),
        "reconcile_status": pdf_result.get("reconcile_status"),
        "reconcile_message": pdf_result.get("reconcile_message"),
    }


def _enrich_webpage_metadata_from_crossref(
    *,
    title: str | None,
    description: str,
    abstract_note: str | None,
    creators: list[dict[str, str]] | None,
    date_text: str | None,
    doi: str | None,
    ctx: Context,
) -> dict[str, Any]:
    enriched = {
        "title": title,
        "description": description,
        "abstract_note": abstract_note,
        "creators": creators or [],
        "date_text": date_text,
        "doi": doi,
    }
    normalized_doi = _normalize_doi(doi)
    if not normalized_doi:
        return enriched

    needs_enrichment = not all(
        [
            _normalize_text(title),
            _normalize_text(abstract_note or description),
            creators,
            _normalize_text(date_text),
        ]
    )
    if not needs_enrichment:
        return enriched

    try:
        work = _fetch_crossref_work(normalized_doi)
    except Exception as exc:
        _ctx_warning(ctx, f"Could not enrich webpage metadata from Crossref for {normalized_doi}: {exc}")
        return enriched

    work_title = ((work.get("title") or [""]) or [""])[0] or ""
    current_title = str(enriched["title"] or "").strip()
    title_is_placeholder = (
        not _normalize_text(current_title)
        or current_title.startswith("http://")
        or current_title.startswith("https://")
        or current_title.lower() in {"request rejected", "science direct", "article locator error - article not recognized"}
    )
    if work_title and title_is_placeholder:
        enriched["title"] = work_title

    if not _normalize_text(enriched["abstract_note"] or enriched["description"]):
        crossref_abstract = clean_html(work.get("abstract", "") or "")
        if crossref_abstract:
            enriched["abstract_note"] = crossref_abstract

    if not enriched["creators"]:
        enriched["creators"] = [
            {
                "creatorType": "author",
                "firstName": str(author.get("given") or "").strip(),
                "lastName": str(author.get("family") or "").strip(),
            }
            for author in work.get("author", [])
            if str(author.get("given") or author.get("family") or "").strip()
        ]

    if not _normalize_text(enriched["date_text"]):
        work_year = _work_year(work)
        if work_year:
            enriched["date_text"] = work_year

    enriched["doi"] = normalized_doi
    return enriched


def _create_webpage_item(
    zot,
    url: str,
    *,
    collection_key: str | None,
    title: str | None,
    description: str,
    abstract_note: str | None,
    creators: list[dict[str, str]] | None,
    date_text: str | None,
    doi: str | None,
    attach_pdf: bool,
    pdf_candidates: list[dict[str, str]] | None,
    ctx: Context,
    fallback_reason: str,
) -> dict[str, Any]:
    from datetime import date

    reused = _reuse_existing_local_copy_for_import(
        collection_key=collection_key,
        doi=doi,
        title=title or url,
        url=url,
        route="webpage",
        ctx=ctx,
    )
    if reused:
        reused["fallback_reason"] = fallback_reason
        return reused

    enriched = _enrich_webpage_metadata_from_crossref(
        title=title or url,
        description=description,
        abstract_note=abstract_note,
        creators=creators,
        date_text=date_text,
        doi=doi,
        ctx=ctx,
    )

    template = zot.item_template("webpage")
    template["title"] = enriched["title"] or url
    template["url"] = url
    template["abstractNote"] = enriched["abstract_note"] or enriched["description"]
    template["accessDate"] = date.today().isoformat()
    if enriched["creators"]:
        template["creators"] = enriched["creators"]
    if enriched["date_text"]:
        template["date"] = enriched["date_text"]
    if enriched["doi"]:
        template["DOI"] = enriched["doi"]
    metadata_complete = all(
        [
            _normalize_text(template.get("title")),
            _normalize_text(template.get("abstractNote")),
            template.get("creators"),
            _normalize_text(template.get("date")),
            _normalize_text(template.get("DOI")),
        ]
    )
    if not metadata_complete:
        template["tags"] = [{"tag": "needs-metadata"}]
    if collection_key:
        template["collections"] = [collection_key]

    create_resp = zot.create_items([template])
    created = create_resp.get("successful", {})
    if not created:
        raise RuntimeError(str(create_resp.get("failed", {})))

    key = list(created.values())[0].get("key", "?")
    pdf_result = {"success": False, "pdf_source": "none", "message": "PDF attachment not requested"}
    if attach_pdf:
        pdf_result = _attach_pdf_with_cascade(
            zot,
            key,
            pdf_candidates=pdf_candidates,
            doi=None,
            collection_key=collection_key,
            ctx=ctx,
        )
    effective_key = pdf_result.get("promoted_item_key") or key

    return {
        "success": True,
        "label": template["title"],
        "key": effective_key,
        "route": "webpage",
        "pdf_source": pdf_result.get("pdf_source", "none") if pdf_result.get("success") else "none",
        "fallback_reason": fallback_reason,
        "pdf_message": pdf_result.get("message", ""),
        "local_item_key": pdf_result.get("local_item_key"),
        "actual_selected_collection_id": pdf_result.get("actual_selected_collection_id"),
        "actual_selected_target": pdf_result.get("actual_selected_target"),
        "intended_target": pdf_result.get("intended_target"),
        "reconcile_status": pdf_result.get("reconcile_status"),
        "reconcile_message": pdf_result.get("reconcile_message"),
    }




@mcp.tool(
    name="zotero_add_items_by_doi",
    description=(
        "Add one or more items to Zotero by DOI. Creates proper paper items first, "
        "then runs the PDF attachment cascade when enabled."
    ),
)
def add_items_by_doi(
    dois: list[str],
    collection_key: str | None = None,
    attach_pdf: bool = True,
    *,
    ctx: Context,
) -> str:
    """
    Add items to Zotero by DOI.

    Args:
        dois: List of DOI strings (e.g. ["10.1038/nature12345"]).
        collection_key: Optional collection key to add items to.
        attach_pdf: If True, run the source-aware PDF cascade after item creation.
                    This prefers direct and landing-page PDF hints first, then
                    falls back to DOI-based services such as Unpaywall when needed.
        ctx: MCP context.

    Returns:
        Markdown summary of added items.
    """
    try:
        zot = get_web_zotero_client()
        if zot is None:
            return "Error: Web API credentials not configured. Set ZOTERO_API_KEY and ZOTERO_LIBRARY_ID."

        results = []
        for doi in dois:
            doi = _normalize_doi(doi) or doi.strip()
            ctx.info(f"Fetching metadata for DOI: {doi}")
            try:
                created = _create_item_from_doi(
                    zot,
                    doi,
                    collection_key=collection_key,
                    attach_pdf=attach_pdf,
                    ctx=ctx,
                )
                results.append(
                    _format_import_result(
                        success=True,
                        label=created["label"],
                        key=created["key"],
                        route=created["route"],
                        pdf_source=created["pdf_source"],
                        fallback_reason=created["fallback_reason"],
                        local_item_key=created.get("local_item_key"),
                    )
                )
                if attach_pdf:
                    _append_import_note(
                        results,
                        route=created["route"],
                        pdf_source=created["pdf_source"],
                        fallback_reason=created["fallback_reason"],
                        pdf_message=created.get("pdf_message"),
                    )
            except Exception as e:
                results.append(
                    _format_import_result(
                        success=False,
                        label=doi,
                        route="doi",
                        pdf_source="none",
                        fallback_reason="doi_import_failed",
                        error=str(e),
                    )
                )

        return "\n".join(results) if results else "No DOIs processed."
    except Exception as e:
        ctx.error(f"Error in add_items_by_doi: {e}")
        return f"Error: {e}"


@mcp.tool(
    name="zotero_find_and_attach_pdfs",
    description=(
        "Repair missing PDFs for existing Zotero items using the same source-aware "
        "PDF cascade as import: landing-page hints first, DOI fallbacks after."
    ),
)
def find_and_attach_pdfs(
    item_keys: list[str],
    *,
    ctx: Context,
) -> str:
    """
    For each item key, try to repair a missing PDF attachment.

    The repair flow prefers explicit PDF hints from the item's landing page or
    other known source URLs, then falls back to DOI-based OA lookups such as
    Unpaywall when available.

    Args:
        item_keys: List of Zotero item keys to process.
        ctx: MCP context.

    Returns:
        Per-item result summary.
    """
    try:
        zot = get_web_zotero_client()
        if zot is None:
            return "Error: Web API credentials not configured. Set ZOTERO_API_KEY and ZOTERO_LIBRARY_ID."

        results = []
        for key in item_keys:
            try:
                item = zot.item(key)
                doi = item.get("data", {}).get("DOI", "").strip()
                url = item.get("data", {}).get("url", "").strip()
                repair_deadline = time.monotonic() + _repair_pdf_budget_seconds()
                signals = {"pdf_candidates": []}
                crossref_work: dict[str, Any] | None = None
                if doi and not _deadline_exceeded(repair_deadline):
                    try:
                        crossref_work = _fetch_crossref_work(doi, deadline=repair_deadline)
                        signals["pdf_candidates"] = _dedupe_pdf_candidates(
                            (signals.get("pdf_candidates") or [])
                            + _discover_pdf_candidates_from_crossref_work(
                                crossref_work,
                                doi=doi,
                                ctx=ctx,
                                repair_mode=True,
                                deadline=repair_deadline,
                            )
                        )
                    except Exception as crossref_error:
                        _ctx_warning(ctx, f"Could not inspect Crossref work for DOI PDF hints: {crossref_error}")
                if url and not _deadline_exceeded(repair_deadline):
                    try:
                        page_signals = _fetch_page_signals(
                            url,
                            ctx=ctx,
                            repair_mode=True,
                            deadline=repair_deadline,
                        )
                        signals["pdf_candidates"] = _dedupe_pdf_candidates(
                            (signals.get("pdf_candidates") or [])
                            + (page_signals.get("pdf_candidates") or [])
                        )
                    except Exception as page_error:
                        _ctx_warning(ctx, f"Could not inspect item URL for PDF hints: {page_error}")

                result = _attach_pdf_with_cascade(
                    zot,
                    key,
                    pdf_candidates=signals.get("pdf_candidates", []),
                    doi=doi or None,
                    crossref_work=crossref_work,
                    collection_key=(item.get("data", {}).get("collections") or [None])[0],
                    ctx=ctx,
                    repair_mode=True,
                    deadline=repair_deadline,
                )
                results.append(
                    _format_pdf_attach_result(
                        item_key=key,
                        success=bool(result.get("success")),
                        pdf_source=result["pdf_source"],
                        message=result["message"],
                        promoted_item_key=result.get("promoted_item_key"),
                        local_item_key=result.get("local_item_key"),
                    )
                )
            except Exception as e:
                results.append(f"✗ {key}: {e}")

        return "\n".join(results) if results else "No items processed."
    except Exception as e:
        ctx.error(f"Error in find_and_attach_pdfs: {e}")
        return f"Error: {e}"


@mcp.tool(
    name="zotero_add_linked_url_attachment",
    description="Add a linked URL attachment to an existing Zotero item."
)
def add_linked_url_attachment(
    item_key: str,
    url: str,
    title: str | None = None,
    *,
    ctx: Context,
) -> str:
    """
    Attach a linked URL to an existing Zotero item.

    Args:
        item_key: Key of the parent item.
        url: URL to attach.
        title: Optional display title for the attachment.
        ctx: MCP context.

    Returns:
        Confirmation string with the new attachment key.
    """
    try:
        zot = get_web_zotero_client()
        if zot is None:
            return "Error: Web API credentials not configured. Set ZOTERO_API_KEY and ZOTERO_LIBRARY_ID."

        template = {
            "itemType": "attachment",
            "linkMode": "linked_url",
            "title": title or url,
            "url": url,
            "parentItem": item_key,
            "tags": [],
            "relations": {},
        }
        resp = zot.create_items([template])
        created = resp.get("successful", {})
        if created:
            key = list(created.values())[0].get("key", "?")
            return f"✓ Linked URL attached to {item_key} → attachment key `{key}`"
        failed = resp.get("failed", {})
        return f"✗ Failed: {failed}"
    except Exception as e:
        ctx.error(f"Error in add_linked_url_attachment: {e}")
        return f"Error: {e}"


@mcp.tool(
    name="zotero_add_items_by_arxiv",
    description=(
        "Add one or more preprints to Zotero by arXiv ID. Uses arXiv metadata "
        "and can attach the canonical arXiv PDF automatically."
    ),
)
def add_items_by_arxiv(
    arxiv_ids: list[str],
    collection_key: str | None = None,
    attach_pdf: bool = True,
    *,
    ctx: Context,
) -> str:
    """
    Add preprints to Zotero by arXiv ID.

    Args:
        arxiv_ids: List of arXiv IDs in any common format
                   (e.g. "2301.12345", "arXiv:2301.12345", "https://arxiv.org/abs/2301.12345").
        collection_key: Optional collection key to add items to.
        attach_pdf: If True, attempt to attach the canonical arXiv PDF.
        ctx: MCP context.

    Returns:
        Markdown summary of added items.
    """
    try:
        zot = get_web_zotero_client()
        if zot is None:
            return "Error: Web API credentials not configured. Set ZOTERO_API_KEY and ZOTERO_LIBRARY_ID."

        results = []
        for raw_id in arxiv_ids:
            arxiv_id = _normalize_arxiv_id(raw_id) or raw_id.strip()
            ctx.info(f"Fetching arXiv metadata for: {arxiv_id}")
            try:
                created = _create_item_from_arxiv(
                    zot,
                    arxiv_id,
                    collection_key=collection_key,
                    attach_pdf=attach_pdf,
                    ctx=ctx,
                )
                results.append(
                    _format_import_result(
                        success=True,
                        label=created["label"],
                        key=created["key"],
                        route=created["route"],
                        pdf_source=created["pdf_source"],
                        fallback_reason=created["fallback_reason"],
                        local_item_key=created.get("local_item_key"),
                    )
                )
                if attach_pdf:
                    _append_import_note(
                        results,
                        route=created["route"],
                        pdf_source=created["pdf_source"],
                        fallback_reason=created["fallback_reason"],
                        pdf_message=created.get("pdf_message"),
                    )
            except Exception as e:
                results.append(
                    _format_import_result(
                        success=False,
                        label=arxiv_id,
                        route="arxiv",
                        pdf_source="none",
                        fallback_reason="arxiv_import_failed",
                        error=str(e),
                    )
                )

        return "\n".join(results) if results else "No arXiv IDs processed."
    except Exception as e:
        ctx.error(f"Error in add_items_by_arxiv: {e}")
        return f"Error: {e}"


@mcp.tool(
    name="zotero_add_items_by_identifier",
    description="Smart import for papers by DOI, arXiv ID, direct PDF URL, or landing-page URL. Prefers proper paper/preprint items before falling back to webpage."
)
def add_items_by_identifier(
    identifiers: list[str],
    collection_key: str | None = None,
    attach_pdf: bool = True,
    fallback_mode: Literal["webpage", "skip"] = "webpage",
    *,
    ctx: Context,
) -> str:
    """
    Smart import entrypoint for mixed identifiers and URLs.

    Route order:
      1) DOI / doi.org URL
      2) arXiv ID / arXiv URL / 10.48550/arXiv.*
      3) Direct PDF URL
      4) Generic landing page URL
    """
    try:
        zot = get_web_zotero_client()
        if zot is None:
            return "Error: Web API credentials not configured. Set ZOTERO_API_KEY and ZOTERO_LIBRARY_ID."

        results = []
        collection_path = _collection_label(zot, collection_key)
        for raw_identifier in identifiers:
            raw_identifier = raw_identifier.strip()
            if not raw_identifier:
                continue

            try:
                arxiv_hint = _normalize_arxiv_id(raw_identifier)
                doi_hint = None if arxiv_hint and (
                    "arxiv.org" in raw_identifier.lower()
                    or raw_identifier.lower().startswith("arxiv:")
                    or raw_identifier.lower().startswith("10.48550/arxiv.")
                ) else _normalize_doi(raw_identifier)

                if doi_hint:
                    created = None
                    resolved_doi = doi_hint
                    last_exc: Exception | None = None
                    doi_candidates = _doi_candidates_from_raw(raw_identifier)
                    for idx, doi_candidate in enumerate(doi_candidates):
                        try:
                            created = _create_item_from_doi(
                                zot,
                                doi_candidate,
                                collection_key=collection_key,
                                attach_pdf=attach_pdf,
                                ctx=ctx,
                            )
                            resolved_doi = doi_candidate
                            break
                        except requests.HTTPError as exc:
                            status_code = getattr(getattr(exc, "response", None), "status_code", None)
                            if status_code == 404 and idx < len(doi_candidates) - 1:
                                last_exc = exc
                                continue
                            raise
                    if created is None:
                        if last_exc is not None:
                            raise last_exc
                        raise RuntimeError(f"failed to resolve DOI from {raw_identifier}")
                    results.append(
                        _format_import_result(
                            success=True,
                            label=created["label"],
                            key=created["key"],
                            route=created["route"],
                            pdf_source=created["pdf_source"],
                            fallback_reason=created["fallback_reason"],
                            local_item_key=created.get("local_item_key"),
                        )
                    )
                    if attach_pdf:
                        _append_import_note(
                            results,
                            route=created["route"],
                            pdf_source=created["pdf_source"],
                            fallback_reason=created["fallback_reason"],
                            pdf_message=created.get("pdf_message"),
                        )
                    _record_import_event(
                        action="import",
                        status="success",
                        input_value=raw_identifier,
                        resolved_identifier=resolved_doi,
                        route=created["route"],
                        label=created["label"],
                        item_key=created["key"],
                        local_item_key=created.get("local_item_key"),
                        pdf_source=created["pdf_source"],
                        fallback_reason=created["fallback_reason"],
                        collection_key=collection_key,
                        collection_path=collection_path,
                        actual_selected_collection_id=created.get("actual_selected_collection_id"),
                        actual_selected_target=created.get("actual_selected_target"),
                        intended_target=created.get("intended_target"),
                        reconcile_status=created.get("reconcile_status"),
                        reconcile_message=created.get("reconcile_message"),
                        message=created.get("pdf_message"),
                        ctx=ctx,
                    )
                    continue

                if arxiv_hint:
                    created = _create_item_from_arxiv(
                        zot,
                        arxiv_hint,
                        collection_key=collection_key,
                        attach_pdf=attach_pdf,
                        ctx=ctx,
                    )
                    results.append(
                        _format_import_result(
                            success=True,
                            label=created["label"],
                            key=created["key"],
                            route=created["route"],
                            pdf_source=created["pdf_source"],
                            fallback_reason=created["fallback_reason"],
                            local_item_key=created.get("local_item_key"),
                        )
                    )
                    if attach_pdf:
                        _append_import_note(
                            results,
                            route=created["route"],
                            pdf_source=created["pdf_source"],
                            fallback_reason=created["fallback_reason"],
                            pdf_message=created.get("pdf_message"),
                        )
                    _record_import_event(
                        action="import",
                        status="success",
                        input_value=raw_identifier,
                        resolved_identifier=arxiv_hint,
                        route=created["route"],
                        label=created["label"],
                        item_key=created["key"],
                        local_item_key=created.get("local_item_key"),
                        pdf_source=created["pdf_source"],
                        fallback_reason=created["fallback_reason"],
                        collection_key=collection_key,
                        collection_path=collection_path,
                        actual_selected_collection_id=created.get("actual_selected_collection_id"),
                        actual_selected_target=created.get("actual_selected_target"),
                        intended_target=created.get("intended_target"),
                        reconcile_status=created.get("reconcile_status"),
                        reconcile_message=created.get("reconcile_message"),
                        message=created.get("pdf_message"),
                        ctx=ctx,
                    )
                    continue

                if _looks_like_direct_pdf_url(raw_identifier):
                    pdf_signals = _probe_identifier_from_direct_pdf_url(raw_identifier, ctx=ctx)
                    if pdf_signals:
                        if pdf_signals.get("doi"):
                            created = _create_item_from_doi(
                                zot,
                                pdf_signals["doi"],
                                collection_key=collection_key,
                                attach_pdf=attach_pdf,
                                ctx=ctx,
                                pdf_candidates=pdf_signals.get("pdf_candidates"),
                            )
                            results.append(
                                _format_import_result(
                                    success=True,
                                    label=created["label"],
                                    key=created["key"],
                                    route=created["route"],
                                    pdf_source=created["pdf_source"],
                                    fallback_reason=created["fallback_reason"],
                                    local_item_key=created.get("local_item_key"),
                                )
                            )
                            if attach_pdf:
                                _append_import_note(
                                    results,
                                    route=created["route"],
                                    pdf_source=created["pdf_source"],
                                    fallback_reason=created["fallback_reason"],
                                    pdf_message=created.get("pdf_message"),
                                )
                            _record_import_event(
                                action="import",
                                status="success",
                                input_value=raw_identifier,
                                resolved_identifier=pdf_signals["doi"],
                                route=created["route"],
                                label=created["label"],
                                item_key=created["key"],
                                local_item_key=created.get("local_item_key"),
                                pdf_source=created["pdf_source"],
                                fallback_reason=created["fallback_reason"],
                                collection_key=collection_key,
                                collection_path=collection_path,
                                actual_selected_collection_id=created.get("actual_selected_collection_id"),
                                actual_selected_target=created.get("actual_selected_target"),
                                intended_target=created.get("intended_target"),
                                reconcile_status=created.get("reconcile_status"),
                                reconcile_message=created.get("reconcile_message"),
                                message=created.get("pdf_message"),
                                ctx=ctx,
                            )
                            continue

                        inferred_pdf_doi = _lookup_crossref_doi_for_signals(pdf_signals, ctx=ctx)
                        if inferred_pdf_doi:
                            created = _create_item_from_doi(
                                zot,
                                inferred_pdf_doi,
                                collection_key=collection_key,
                                attach_pdf=attach_pdf,
                                ctx=ctx,
                                pdf_candidates=pdf_signals.get("pdf_candidates"),
                            )
                            results.append(
                                _format_import_result(
                                    success=True,
                                    label=created["label"],
                                    key=created["key"],
                                    route=created["route"],
                                    pdf_source=created["pdf_source"],
                                    fallback_reason=created["fallback_reason"],
                                    local_item_key=created.get("local_item_key"),
                                )
                            )
                            if attach_pdf:
                                _append_import_note(
                                    results,
                                    route=created["route"],
                                    pdf_source=created["pdf_source"],
                                    fallback_reason=created["fallback_reason"],
                                    pdf_message=created.get("pdf_message"),
                                )
                            _record_import_event(
                                action="import",
                                status="success",
                                input_value=raw_identifier,
                                resolved_identifier=inferred_pdf_doi,
                                route=created["route"],
                                label=created["label"],
                                item_key=created["key"],
                                local_item_key=created.get("local_item_key"),
                                pdf_source=created["pdf_source"],
                                fallback_reason=created["fallback_reason"],
                                collection_key=collection_key,
                                collection_path=collection_path,
                                actual_selected_collection_id=created.get("actual_selected_collection_id"),
                                actual_selected_target=created.get("actual_selected_target"),
                                intended_target=created.get("intended_target"),
                                reconcile_status=created.get("reconcile_status"),
                                reconcile_message=created.get("reconcile_message"),
                                message=created.get("pdf_message"),
                                ctx=ctx,
                            )
                            continue

                        if pdf_signals.get("arxiv_id"):
                            created = _create_item_from_arxiv(
                                zot,
                                pdf_signals["arxiv_id"],
                                collection_key=collection_key,
                                attach_pdf=attach_pdf,
                                ctx=ctx,
                            )
                            results.append(
                                _format_import_result(
                                    success=True,
                                    label=created["label"],
                                    key=created["key"],
                                    route=created["route"],
                                    pdf_source=created["pdf_source"],
                                    fallback_reason=created["fallback_reason"],
                                    local_item_key=created.get("local_item_key"),
                                )
                            )
                            if attach_pdf:
                                _append_import_note(
                                    results,
                                    route=created["route"],
                                    pdf_source=created["pdf_source"],
                                    fallback_reason=created["fallback_reason"],
                                    pdf_message=created.get("pdf_message"),
                                )
                            _record_import_event(
                                action="import",
                                status="success",
                                input_value=raw_identifier,
                                resolved_identifier=pdf_signals["arxiv_id"],
                                route=created["route"],
                                label=created["label"],
                                item_key=created["key"],
                                local_item_key=created.get("local_item_key"),
                                pdf_source=created["pdf_source"],
                                fallback_reason=created["fallback_reason"],
                                collection_key=collection_key,
                                collection_path=collection_path,
                                actual_selected_collection_id=created.get("actual_selected_collection_id"),
                                actual_selected_target=created.get("actual_selected_target"),
                                intended_target=created.get("intended_target"),
                                reconcile_status=created.get("reconcile_status"),
                                reconcile_message=created.get("reconcile_message"),
                                message=created.get("pdf_message"),
                                ctx=ctx,
                            )
                            continue

                    if fallback_mode == "skip":
                        results.append(
                            _format_import_result(
                                success=False,
                                label=raw_identifier,
                                route="webpage",
                                pdf_source="none",
                                fallback_reason="missing_identifier",
                                error="direct PDF has no DOI/arXiv identifier and fallback_mode=skip",
                            )
                        )
                        _record_import_event(
                            action="import",
                            status="skipped",
                            input_value=raw_identifier,
                            route="webpage",
                            pdf_source="none",
                            fallback_reason="missing_identifier",
                            collection_key=collection_key,
                            collection_path=collection_path,
                            error="direct PDF has no DOI/arXiv identifier and fallback_mode=skip",
                            ctx=ctx,
                        )
                        continue
                    created = _create_webpage_item(
                        zot,
                        raw_identifier,
                        collection_key=collection_key,
                        title=Path(urlparse(raw_identifier).path).name or raw_identifier,
                        description="Imported from direct PDF URL; bibliographic metadata still needs review.",
                        abstract_note="Imported from direct PDF URL; bibliographic metadata still needs review.",
                        creators=[],
                        date_text="",
                        doi=None,
                        attach_pdf=attach_pdf,
                        pdf_candidates=[{"source": "direct_pdf", "url": raw_identifier}],
                        ctx=ctx,
                        fallback_reason="missing_identifier",
                    )
                    results.append(
                        _format_import_result(
                            success=True,
                            label=created["label"],
                            key=created["key"],
                            route=created["route"],
                            pdf_source=created["pdf_source"],
                            fallback_reason=created["fallback_reason"],
                            local_item_key=created.get("local_item_key"),
                        )
                    )
                    if attach_pdf:
                        _append_import_note(
                            results,
                            route=created["route"],
                            pdf_source=created["pdf_source"],
                            fallback_reason=created["fallback_reason"],
                            pdf_message=created.get("pdf_message"),
                        )
                    _record_import_event(
                        action="import",
                        status="success",
                        input_value=raw_identifier,
                        resolved_identifier=raw_identifier,
                        route=created["route"],
                        label=created["label"],
                        item_key=created["key"],
                        local_item_key=created.get("local_item_key"),
                        pdf_source=created["pdf_source"],
                        fallback_reason=created["fallback_reason"],
                        collection_key=collection_key,
                        collection_path=collection_path,
                        actual_selected_collection_id=created.get("actual_selected_collection_id"),
                        actual_selected_target=created.get("actual_selected_target"),
                        intended_target=created.get("intended_target"),
                        reconcile_status=created.get("reconcile_status"),
                        reconcile_message=created.get("reconcile_message"),
                        message=created.get("pdf_message"),
                        ctx=ctx,
                    )
                    continue

                signals = _fetch_page_signals(raw_identifier, ctx=ctx)
                if (
                    "application/pdf" in str(signals.get("content_type") or "").lower()
                    or any(
                        str(candidate.get("source") or "") == "direct_pdf"
                        for candidate in (signals.get("pdf_candidates") or [])
                    )
                ):
                    probe_urls: list[str] = []
                    for candidate_url in (
                        str(signals.get("final_url") or "").strip(),
                        str(signals.get("source_url") or "").strip(),
                        raw_identifier,
                    ):
                        if candidate_url and candidate_url not in probe_urls:
                            probe_urls.append(candidate_url)

                    probed_pdf_signals = None
                    for probe_url in probe_urls:
                        probed_pdf_signals = _probe_identifier_from_direct_pdf_url(
                            probe_url,
                            ctx=ctx,
                        )
                        if probed_pdf_signals:
                            break
                    if probed_pdf_signals:
                        for key in (
                            "title",
                            "venue",
                            "description",
                            "abstract_note",
                            "creators",
                            "date",
                            "doi",
                            "arxiv_id",
                        ):
                            if probed_pdf_signals.get(key):
                                signals[key] = probed_pdf_signals.get(key)
                        if probed_pdf_signals.get("pdf_candidates"):
                            signals["pdf_candidates"] = _dedupe_pdf_candidates(
                                (probed_pdf_signals.get("pdf_candidates") or [])
                                + (signals.get("pdf_candidates") or [])
                            )

                if signals.get("doi"):
                    created = _create_item_from_doi(
                        zot,
                        signals["doi"],
                        collection_key=collection_key,
                        attach_pdf=attach_pdf,
                        ctx=ctx,
                        pdf_candidates=signals.get("pdf_candidates"),
                    )
                    results.append(
                        _format_import_result(
                            success=True,
                            label=created["label"],
                            key=created["key"],
                            route=created["route"],
                            pdf_source=created["pdf_source"],
                            fallback_reason=created["fallback_reason"],
                            local_item_key=created.get("local_item_key"),
                        )
                    )
                    if attach_pdf:
                        _append_import_note(
                            results,
                            route=created["route"],
                            pdf_source=created["pdf_source"],
                            fallback_reason=created["fallback_reason"],
                            pdf_message=created.get("pdf_message"),
                        )
                    _record_import_event(
                        action="import",
                        status="success",
                        input_value=raw_identifier,
                        resolved_identifier=signals["doi"],
                        route=created["route"],
                        label=created["label"],
                        item_key=created["key"],
                        local_item_key=created.get("local_item_key"),
                        pdf_source=created["pdf_source"],
                        fallback_reason=created["fallback_reason"],
                        collection_key=collection_key,
                        collection_path=collection_path,
                        actual_selected_collection_id=created.get("actual_selected_collection_id"),
                        actual_selected_target=created.get("actual_selected_target"),
                        intended_target=created.get("intended_target"),
                        reconcile_status=created.get("reconcile_status"),
                        reconcile_message=created.get("reconcile_message"),
                        message=created.get("pdf_message"),
                        ctx=ctx,
                    )
                    continue

                inferred_doi = _lookup_crossref_doi_for_signals(signals, ctx=ctx)
                if inferred_doi:
                    created = _create_item_from_doi(
                        zot,
                        inferred_doi,
                        collection_key=collection_key,
                        attach_pdf=attach_pdf,
                        ctx=ctx,
                        pdf_candidates=signals.get("pdf_candidates"),
                    )
                    results.append(
                        _format_import_result(
                            success=True,
                            label=created["label"],
                            key=created["key"],
                            route=created["route"],
                            pdf_source=created["pdf_source"],
                            fallback_reason=created["fallback_reason"],
                            local_item_key=created.get("local_item_key"),
                        )
                    )
                    if attach_pdf:
                        _append_import_note(
                            results,
                            route=created["route"],
                            pdf_source=created["pdf_source"],
                            fallback_reason=created["fallback_reason"],
                            pdf_message=created.get("pdf_message"),
                        )
                    _record_import_event(
                        action="import",
                        status="success",
                        input_value=raw_identifier,
                        resolved_identifier=inferred_doi,
                        route=created["route"],
                        label=created["label"],
                        item_key=created["key"],
                        local_item_key=created.get("local_item_key"),
                        pdf_source=created["pdf_source"],
                        fallback_reason=created["fallback_reason"],
                        collection_key=collection_key,
                        collection_path=collection_path,
                        actual_selected_collection_id=created.get("actual_selected_collection_id"),
                        actual_selected_target=created.get("actual_selected_target"),
                        intended_target=created.get("intended_target"),
                        reconcile_status=created.get("reconcile_status"),
                        reconcile_message=created.get("reconcile_message"),
                        message=created.get("pdf_message"),
                        ctx=ctx,
                    )
                    continue

                if signals.get("arxiv_id"):
                    created = _create_item_from_arxiv(
                        zot,
                        signals["arxiv_id"],
                        collection_key=collection_key,
                        attach_pdf=attach_pdf,
                        ctx=ctx,
                    )
                    results.append(
                        _format_import_result(
                            success=True,
                            label=created["label"],
                            key=created["key"],
                            route=created["route"],
                            pdf_source=created["pdf_source"],
                            fallback_reason=created["fallback_reason"],
                        )
                    )
                    if attach_pdf:
                        _append_import_note(
                            results,
                            route=created["route"],
                            pdf_source=created["pdf_source"],
                            fallback_reason=created["fallback_reason"],
                            pdf_message=created.get("pdf_message"),
                        )
                    _record_import_event(
                        action="import",
                        status="success",
                        input_value=raw_identifier,
                        resolved_identifier=signals["arxiv_id"],
                        route=created["route"],
                        label=created["label"],
                        item_key=created["key"],
                        local_item_key=created.get("local_item_key"),
                        pdf_source=created["pdf_source"],
                        fallback_reason=created["fallback_reason"],
                        collection_key=collection_key,
                        collection_path=collection_path,
                        actual_selected_collection_id=created.get("actual_selected_collection_id"),
                        actual_selected_target=created.get("actual_selected_target"),
                        intended_target=created.get("intended_target"),
                        reconcile_status=created.get("reconcile_status"),
                        reconcile_message=created.get("reconcile_message"),
                        message=created.get("pdf_message"),
                        ctx=ctx,
                    )
                    continue

                if fallback_mode == "skip":
                    results.append(
                        _format_import_result(
                            success=False,
                            label=raw_identifier,
                            route="webpage",
                            pdf_source="none",
                            fallback_reason="missing_identifier",
                            error="no DOI/arXiv identifier detected and fallback_mode=skip",
                        )
                    )
                    _record_import_event(
                        action="import",
                        status="skipped",
                        input_value=raw_identifier,
                        route="webpage",
                        pdf_source="none",
                        fallback_reason="missing_identifier",
                        collection_key=collection_key,
                        collection_path=collection_path,
                        error="no DOI/arXiv identifier detected and fallback_mode=skip",
                        ctx=ctx,
                    )
                    continue

                created = _create_webpage_item(
                    zot,
                    signals.get("final_url") or raw_identifier,
                    collection_key=collection_key,
                    title=signals.get("title"),
                    description=signals.get("description", ""),
                    abstract_note=signals.get("abstract_note"),
                    creators=signals.get("creators"),
                    date_text=signals.get("date"),
                    doi=signals.get("doi"),
                    attach_pdf=attach_pdf,
                    pdf_candidates=signals.get("pdf_candidates"),
                    ctx=ctx,
                    fallback_reason="missing_identifier",
                )
                results.append(
                    _format_import_result(
                        success=True,
                        label=created["label"],
                        key=created["key"],
                        route=created["route"],
                        pdf_source=created["pdf_source"],
                        fallback_reason=created["fallback_reason"],
                        local_item_key=created.get("local_item_key"),
                    )
                )
                if attach_pdf:
                    _append_import_note(
                        results,
                        route=created["route"],
                        pdf_source=created["pdf_source"],
                        fallback_reason=created["fallback_reason"],
                        pdf_message=created.get("pdf_message"),
                    )
                _record_import_event(
                    action="import",
                    status="success",
                    input_value=raw_identifier,
                    resolved_identifier=signals.get("final_url") or raw_identifier,
                    route=created["route"],
                    label=created["label"],
                    item_key=created["key"],
                    local_item_key=created.get("local_item_key"),
                    pdf_source=created["pdf_source"],
                    fallback_reason=created["fallback_reason"],
                    collection_key=collection_key,
                    collection_path=collection_path,
                    actual_selected_collection_id=created.get("actual_selected_collection_id"),
                    actual_selected_target=created.get("actual_selected_target"),
                    intended_target=created.get("intended_target"),
                    reconcile_status=created.get("reconcile_status"),
                    reconcile_message=created.get("reconcile_message"),
                    message=created.get("pdf_message"),
                    ctx=ctx,
                )
            except Exception as exc:
                results.append(
                    _format_import_result(
                        success=False,
                        label=raw_identifier,
                        route="unknown",
                        pdf_source="none",
                        fallback_reason="identifier_resolution_failed",
                        error=str(exc),
                    )
                )
                _record_import_event(
                    action="import",
                    status="error",
                    input_value=raw_identifier,
                    route="unknown",
                    pdf_source="none",
                    fallback_reason="identifier_resolution_failed",
                    collection_key=collection_key,
                    collection_path=collection_path,
                    error=str(exc),
                    ctx=ctx,
                )

        return "\n".join(results) if results else "No identifiers processed."
    except Exception as exc:
        ctx.error(f"Error in add_items_by_identifier: {exc}")
        return f"Error: {exc}"

# Internal diagnostics / repair helpers intentionally kept as plain functions
# instead of public MCP tools. The stable public import surface is:
#   - zotero_add_items_by_identifier
#   - zotero_add_items_by_doi
#   - zotero_add_items_by_arxiv
#   - zotero_add_item_by_url
#   - zotero_find_and_attach_pdfs
def get_import_ledger(
    limit: int | str | None = 20,
    action: Literal["all", "import", "reconcile"] = "all",
    status: str | None = None,
    *,
    ctx: Context,
) -> str:
    try:
        parsed_limit = int(limit) if limit is not None else 20
    except (TypeError, ValueError):
        parsed_limit = 20

    entries = _read_import_ledger()
    if action != "all":
        entries = [entry for entry in entries if entry.get("action") == action]
    if status:
        entries = [entry for entry in entries if entry.get("status") == status]
    entries = entries[-max(parsed_limit, 0):]

    path = _import_ledger_path()
    if not entries:
        return (
            "No import ledger entries found. "
            "The ledger file is created automatically on first import/reconcile. "
            "Advanced users can override its location with ZOTERO_MCP_IMPORT_LEDGER_PATH."
        )

    def _table_cell(value: Any, *, max_len: int = 40) -> str:
        text = str(value or "-").replace("\n", " ").replace("|", "/").strip()
        if len(text) > max_len:
            return text[: max_len - 1] + "…"
        return text or "-"

    status_counts: dict[str, int] = {}
    route_counts: dict[str, int] = {}
    for entry in entries:
        status_key = str(entry.get("status") or "unknown")
        route_key = str(entry.get("route") or "unknown")
        status_counts[status_key] = status_counts.get(status_key, 0) + 1
        route_counts[route_key] = route_counts.get(route_key, 0) + 1

    lines = [
        "Import ledger summary",
        f"- showing {len(entries)} recent event(s)",
        "- status: " + ", ".join(f"{key}={status_counts[key]}" for key in sorted(status_counts)),
        "- route: " + ", ".join(f"{key}={route_counts[key]}" for key in sorted(route_counts)),
        "",
        "| Time | Action | Status | Route | Item Key | Local Copy | PDF | Collection | Input |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for entry in entries:
        lines.append(
            "| "
            + " | ".join(
                [
                    _table_cell(entry.get("timestamp", "?"), max_len=19),
                    _table_cell(entry.get("action", "?"), max_len=10),
                    _table_cell(entry.get("status", "?"), max_len=10),
                    _table_cell(entry.get("route", "?"), max_len=18),
                    _table_cell(entry.get("item_key", "-"), max_len=12),
                    _table_cell(entry.get("local_item_key", "-"), max_len=12),
                    _table_cell(entry.get("pdf_source", "none"), max_len=24),
                    _table_cell(entry.get("collection_path") or entry.get("collection_key") or "-", max_len=24),
                    _table_cell(entry.get("input", "-"), max_len=40),
                ]
            )
            + " |"
        )
        if entry.get("message"):
            lines.append(f"  note: {_table_cell(entry['message'], max_len=200)}")
        if entry.get("error"):
            lines.append(f"  error: {_table_cell(entry['error'], max_len=200)}")
    lines.append(
        f"(internal state file: `{path}`; advanced override: ZOTERO_MCP_IMPORT_LEDGER_PATH)"
    )
    return "\n".join(lines)


def reconcile_local_copies(
    item_keys: list[str],
    collection_key: str | None = None,
    remove_from_selected_target: bool = False,
    *,
    ctx: Context,
) -> str:
    try:
        zot = get_web_zotero_client()
        if zot is None:
            return "Error: Web API credentials not configured. Set ZOTERO_API_KEY and ZOTERO_LIBRARY_ID."

        local_zot = get_local_zotero_client()
        if local_zot is None:
            return "Error: local Zotero is not running or local API is unavailable."

        results: list[str] = []
        for item_key in item_keys:
            try:
                payload = zot.item(item_key)
                data = payload.get("data", payload if isinstance(payload, dict) else {})
                if not isinstance(data, dict):
                    raise RuntimeError(f"could not load item `{item_key}`")

                intended_collection_key = collection_key
                if not intended_collection_key:
                    collections = data.get("collections") or []
                    intended_collection_key = collections[0] if collections else None

                ledger_entry = _latest_import_ledger_entry(item_key=item_key)
                local_item_key = ledger_entry.get("local_item_key") if ledger_entry else None
                if not local_item_key:
                    local_item = _find_local_item_by_metadata(
                        title=data.get("title", ""),
                        item_type=data.get("itemType", ""),
                        doi=data.get("DOI"),
                        url=data.get("url"),
                        collection_key=intended_collection_key,
                        require_pdf=True,
                    ) or _find_local_item_by_metadata(
                        title=data.get("title", ""),
                        item_type=data.get("itemType", ""),
                        doi=data.get("DOI"),
                        url=data.get("url"),
                        require_pdf=True,
                    )
                    local_item_key = local_item.get("key") if local_item else None

                if not local_item_key:
                    raise RuntimeError("no matching local item with PDF found")

                selected_collection_key = None
                if remove_from_selected_target and ledger_entry:
                    selected_collection_key = ledger_entry.get("actual_selected_collection_id")
                if remove_from_selected_target and not selected_collection_key:
                    target_snapshot = _connector_target_snapshot()
                    live_selected_collection_key = target_snapshot.get("current_collection_id")
                    live_selected_collection_key = (
                        _resolve_connector_collection_key(live_selected_collection_key)
                        or live_selected_collection_key
                    )
                    local_payload = _get_item_payload(local_zot, local_item_key) or _get_item_payload(zot, local_item_key)
                    local_data = local_payload.get("data", {}) if local_payload else {}
                    local_collections = list(local_data.get("collections") or [])
                    if (
                        live_selected_collection_key
                        and live_selected_collection_key != intended_collection_key
                        and live_selected_collection_key in local_collections
                    ):
                        selected_collection_key = live_selected_collection_key

                reconcile_result = _reconcile_local_item_to_collection(
                    local_zot,
                    local_item_key,
                    intended_collection_key=intended_collection_key,
                    selected_collection_key=selected_collection_key,
                    remove_from_selected_target=remove_from_selected_target,
                    ctx=ctx,
                )

                status = "success" if reconcile_result.get("success") else "error"
                _record_import_event(
                    action="reconcile",
                    status=status,
                    input_value=item_key,
                    route="reconcile",
                    label=data.get("title"),
                    item_key=item_key,
                    local_item_key=local_item_key,
                    pdf_source="local_zotero",
                    fallback_reason="none",
                    collection_key=intended_collection_key,
                    collection_path=_collection_label(zot, intended_collection_key),
                    actual_selected_collection_id=selected_collection_key,
                    actual_selected_target=ledger_entry.get("actual_selected_target") if ledger_entry else None,
                    intended_target=_collection_label(local_zot, intended_collection_key),
                    reconcile_status=reconcile_result.get("status"),
                    reconcile_message=reconcile_result.get("message"),
                    message=reconcile_result.get("message"),
                    error=None if reconcile_result.get("success") else reconcile_result.get("message"),
                    ctx=ctx,
                )

                icon = "✓" if reconcile_result.get("success") else "✗"
                results.append(
                    f"{icon} {item_key} → local `{local_item_key}` [reconcile_status={reconcile_result.get('status', '?')}]"
                )
                if reconcile_result.get("message"):
                    results.append(f"  {reconcile_result['message']}")
            except Exception as exc:
                _record_import_event(
                    action="reconcile",
                    status="error",
                    input_value=item_key,
                    route="reconcile",
                    item_key=item_key,
                    pdf_source="local_zotero",
                    fallback_reason="none",
                    collection_key=collection_key,
                    collection_path=_collection_label(zot, collection_key) if zot is not None else None,
                    error=str(exc),
                    ctx=ctx,
                )
                results.append(f"✗ {item_key}: {exc}")

        return "\n".join(results) if results else "No items reconciled."
    except Exception as exc:
        ctx.error(f"Error in reconcile_local_copies: {exc}")
        return f"Error: {exc}"


@mcp.tool(
    name="zotero_reconcile_collection_duplicates",
    description=(
        "Reconcile duplicate parent items inside a Zotero collection. "
        "Merges collection memberships onto one canonical item and optionally moves duplicates to trash."
    ),
)
def reconcile_collection_duplicates(
    collection_key: str,
    include_subcollections: bool = True,
    dry_run: bool = True,
    reconcile_local_only: bool = True,
    local_db_fallback: bool = False,
    repair_missing_pdfs: bool = True,
    *,
    ctx: Context,
) -> str:
    """
    Reconcile duplicate parent items in a collection (and optionally its subcollections).

    Duplicate grouping prefers DOI, then arXiv ID, then normalized title.
    Within each duplicate group, the canonical item is chosen by:
    1. having a usable PDF attachment
    2. richer metadata
    3. stable key ordering

    Args:
        collection_key: Target collection key to scan.
        include_subcollections: Whether to include descendant collections.
        dry_run: When True, only report planned actions. When False, merges collection
                 memberships and moves duplicate items to trash.
        reconcile_local_only: Whether to run a second local-only pass after web dedupe,
                              catching local residual duplicates not visible via the web API.
        local_db_fallback: When True and local API deletion fails, temporarily restarts
                           Zotero and marks local residual duplicates in deletedItems.
        repair_missing_pdfs: When True and dry_run is False, run a collection-level postpass
                             to repair canonical items that still lack PDFs after dedupe.
        ctx: MCP context.

    Returns:
        Markdown summary table of canonical items and duplicate actions.
    """
    if not dry_run:
        if err := _require_unsafe("items"):
            return err
    try:
        zot = get_web_zotero_client()
        if zot is None:
            return "Error: Web API credentials not configured. Set ZOTERO_API_KEY and ZOTERO_LIBRARY_ID."
        web_summary = _reconcile_collection_duplicates_impl(
            zot=zot,
            collection_key=collection_key,
            include_subcollections=include_subcollections,
            dry_run=dry_run,
            ctx=ctx,
        )
        repair_summary = ""
        if repair_missing_pdfs and not dry_run:
            repair_summary = "\n\n" + _repair_missing_pdfs_in_collection_impl(
                zot=zot,
                collection_key=collection_key,
                include_subcollections=include_subcollections,
                ctx=ctx,
            )
        if not reconcile_local_only:
            return web_summary + repair_summary

        local_zot = get_local_zotero_client()
        if local_zot is None:
            return web_summary + "\n\nLocal dedupe summary\n- local Zotero unavailable" + repair_summary

        local_summary, _ = _reconcile_local_collection_duplicates_impl(
            local_zot=local_zot,
            web_zot=zot,
            collection_key=collection_key,
            include_subcollections=include_subcollections,
            dry_run=dry_run,
            local_db_fallback=local_db_fallback,
            ctx=ctx,
        )
        return web_summary + (local_summary or "") + repair_summary
    except Exception as exc:
        ctx.error(f"Error in reconcile_collection_duplicates: {exc}")
        return f"Error: {exc}"


@mcp.tool(
    name="zotero_add_item_by_url",
    description="Add a webpage item to Zotero by URL. Fetches the page title and OpenGraph metadata."
)
def add_item_by_url(
    url: str,
    collection_key: str | None = None,
    title: str | None = None,
    *,
    ctx: Context,
) -> str:
    """
    Add a webpage item to Zotero by URL.

    Args:
        url: The URL of the webpage to add.
        collection_key: Optional collection key to add the item to.
        title: Optional title override; auto-detected from page if omitted.
        ctx: MCP context.

    Returns:
        Markdown summary of the added item.
    """
    try:
        zot = get_web_zotero_client()
        if zot is None:
            return "Error: Web API credentials not configured. Set ZOTERO_API_KEY and ZOTERO_LIBRARY_ID."

        ctx.info(f"Fetching page metadata for: {url}")
        try:
            signals = _fetch_page_signals(url, ctx=ctx)
        except Exception:
            signals = {"title": None, "description": "", "final_url": url}

        created = _create_webpage_item(
            zot,
            signals.get("final_url") or url,
            collection_key=collection_key,
            title=title or signals.get("title"),
            description=signals.get("description", ""),
            abstract_note=signals.get("abstract_note"),
            creators=signals.get("creators"),
            date_text=signals.get("date"),
            doi=signals.get("doi"),
            attach_pdf=False,
            pdf_candidates=[],
            ctx=ctx,
            fallback_reason="manual_webpage",
        )
        return _format_import_result(
            success=True,
            label=created["label"],
            key=created["key"],
            route=created["route"],
            pdf_source=created["pdf_source"],
            fallback_reason=created["fallback_reason"],
            local_item_key=created.get("local_item_key"),
        )
    except Exception as e:
        ctx.error(f"Error in add_item_by_url: {e}")
        return f"Error: {e}"
