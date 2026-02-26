import pytest
import zotero_mcp.server as server


class DummyContext:
    def info(self, *a, **kw): pass
    def error(self, *a, **kw): pass
    def warn(self, *a, **kw): pass


@pytest.fixture
def ctx():
    return DummyContext()


class FakeWebZotero:
    """Configurable fake for get_web_zotero_client()."""

    def __init__(self):
        self.created_items = []
        self.updated_items = []
        self.created_collections = []
        self.updated_collections = []
        self.deleted_items = []
        self.added_to = []      # (collection_key, item)
        self.removed_from = []  # (collection_key, item)
        self._items = {}        # key → item dict
        self._collections = {}  # key → collection dict
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

    def collection(self, key):
        return self._collections.get(
            key,
            {"data": {"key": key, "name": "Old Name", "parentCollection": False}},
        )

    def create_items(self, items):
        self.created_items.extend(items)
        return {"successful": {"0": {"key": "NEWKEY1"}}, "failed": {}}

    def update_item(self, item):
        self.updated_items.append(item)

    def create_collections(self, cols):
        self.created_collections.extend(cols)
        return {"successful": {"0": {"key": "COLKEY1"}}, "failed": {}}

    def update_collection(self, col):
        self.updated_collections.append(col)

    def addto_collection(self, col_key, item):
        self.added_to.append((col_key, item))

    def deletefrom_collection(self, col_key, item):
        self.removed_from.append((col_key, item))

    def delete_item(self, item):
        self.deleted_items.append(item)

    def delete_collection(self, col):
        self.deleted_items.append(col)


@pytest.fixture
def fake_zot():
    return FakeWebZotero()


@pytest.fixture
def patch_web_client(monkeypatch, fake_zot):
    """Patch server.get_web_zotero_client to return fake_zot."""
    monkeypatch.setattr(server, "get_web_zotero_client", lambda: fake_zot)
    monkeypatch.setenv("UNSAFE_OPERATIONS", "all")
    return fake_zot


@pytest.fixture
def patch_no_credentials(monkeypatch):
    """Simulate missing credentials (returns None)."""
    monkeypatch.setattr(server, "get_web_zotero_client", lambda: None)
    monkeypatch.setenv("UNSAFE_OPERATIONS", "all")
