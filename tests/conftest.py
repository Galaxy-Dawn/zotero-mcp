import sys
from pathlib import Path

import pytest

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import zotero_mcp.server as server  # noqa: E402


class DummyContext:
    def info(self, *a, **kw): pass
    def error(self, *a, **kw): pass
    def warn(self, *a, **kw): pass
    def warning(self, *a, **kw): pass


@pytest.fixture
def ctx():
    return DummyContext()


@pytest.fixture(autouse=True)
def isolate_state_dir(monkeypatch, tmp_path):
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ZOTERO_MCP_STATE_DIR", str(state_dir))
    monkeypatch.setenv("ZOTERO_MCP_DEBUG_IMPORT", "1")


class FakeWebZotero:
    """Configurable fake for get_web_zotero_client()."""

    def __init__(self):
        self.created_items = []
        self.attached_files = []
        self.updated_items = []
        self.created_collections = []
        self.updated_collections = []
        self.deleted_items = []
        self.added_to = []      # (collection_key, item)
        self.removed_from = []  # (collection_key, item)
        self._items = {}        # key → item dict
        self._collections = {}  # key → collection dict
        self._children = {}     # parent key → child item dicts
        self.fail_on = set()    # item keys that raise RuntimeError

    def item_template(self, item_type):
        return {"itemType": item_type, "title": "", "creators": [],
                "tags": [], "collections": []}

    def item(self, key):
        if key in self.fail_on:
            raise RuntimeError(f"Simulated API error for {key}")
        return self._items.get(
            key, {"data": {"key": key, "itemType": "journalArticle"}}
        )

    def children(self, key, **kwargs):
        return self._children.get(key, [])

    def items(self, limit=None, sort=None, direction=None, **kwargs):
        values = list(self._items.values())
        if limit is not None:
            values = values[:limit]
        return values

    def collection_items(self, key, **kwargs):
        matches = []
        for item in self._items.values():
            collections = item.get("data", {}).get("collections", [])
            if key in collections:
                matches.append(item)
        return matches

    def collection(self, key):
        return self._collections.get(
            key,
            {"data": {"key": key, "name": "Old Name", "parentCollection": False}},
        )

    def collections(self, limit=None, **kwargs):
        values = list(self._collections.values())
        if limit is not None:
            values = values[:limit]
        return values

    def create_items(self, items):
        self.created_items.extend(items)
        for idx, item in enumerate(items):
            key = f"NEWKEY{idx + 1}"
            self._items[key] = {"data": {"key": key, **item}}
        return {"successful": {"0": {"key": "NEWKEY1"}}, "failed": {}}

    def attachment_simple(self, files, parent_key):
        self.attached_files.append((tuple(files), parent_key))
        children = self._children.setdefault(parent_key, [])
        child_key = f"ATTACH{len(children) + 1}"
        children.append(
            {
                "key": child_key,
                "data": {
                    "key": child_key,
                    "itemType": "attachment",
                    "parentItem": parent_key,
                    "contentType": "application/pdf",
                    "filename": "attached.pdf",
                    "title": "PDF",
                },
            }
        )

    def update_item(self, item):
        self.updated_items.append(item)

    def create_collections(self, cols):
        self.created_collections.extend(cols)
        return {"successful": {"0": {"key": "COLKEY1"}}, "failed": {}}

    def update_collection(self, col):
        self.updated_collections.append(col)

    def addto_collection(self, col_key, item):
        self.added_to.append((col_key, item))
        key = item.get("key") if isinstance(item, dict) else None
        if key and key in self._items:
            collections = self._items[key]["data"].setdefault("collections", [])
            if col_key not in collections:
                collections.append(col_key)

    def deletefrom_collection(self, col_key, item):
        self.removed_from.append((col_key, item))
        key = item.get("key") if isinstance(item, dict) else None
        if key and key in self._items:
            collections = self._items[key]["data"].setdefault("collections", [])
            self._items[key]["data"]["collections"] = [c for c in collections if c != col_key]

    def delete_item(self, item):
        self.deleted_items.append(item)
        key = item.get("key") if isinstance(item, dict) else None
        if key and key in self._items:
            del self._items[key]
        if key and key in self._children:
            del self._children[key]

    def delete_collection(self, col):
        self.deleted_items.append(col)


@pytest.fixture
def fake_zot():
    return FakeWebZotero()


@pytest.fixture
def patch_web_client(monkeypatch, fake_zot):
    """Patch server.get_web_zotero_client to return fake_zot."""
    monkeypatch.setattr(server, "get_web_zotero_client", lambda: fake_zot)
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: None)
    monkeypatch.setenv("UNSAFE_OPERATIONS", "all")
    monkeypatch.delenv("UNPAYWALL_EMAIL", raising=False)
    return fake_zot


@pytest.fixture
def patch_local_client(monkeypatch, fake_zot):
    monkeypatch.setattr(server, "get_local_zotero_client", lambda: fake_zot)
    return fake_zot


@pytest.fixture
def patch_no_credentials(monkeypatch):
    """Simulate missing credentials (returns None)."""
    monkeypatch.setattr(server, "get_web_zotero_client", lambda: None)
    monkeypatch.setenv("UNSAFE_OPERATIONS", "all")
