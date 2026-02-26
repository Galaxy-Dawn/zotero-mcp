import zotero_mcp.server as server


# ── create_collection ────────────────────────────────────────────────────────

def test_create_top_level_collection(patch_web_client, ctx):
    result = server.create_collection(name="My Papers", ctx=ctx)
    assert "✓" in result
    assert "My Papers" in result
    assert "COLKEY1" in result
    assert len(patch_web_client.created_collections) == 1
    assert patch_web_client.created_collections[0].get("parentCollection") is None


def test_create_nested_collection(patch_web_client, ctx):
    result = server.create_collection(name="Sub", parent_key="PARENT1", ctx=ctx)
    assert "✓" in result
    payload = patch_web_client.created_collections[0]
    assert payload["parentCollection"] == "PARENT1"


def test_create_collection_failed(monkeypatch, fake_zot, ctx):
    fake_zot.create_collections = lambda cols: {"successful": {}, "failed": {"0": "err"}}
    monkeypatch.setattr(server, "get_web_zotero_client", lambda: fake_zot)
    result = server.create_collection(name="Bad", ctx=ctx)
    assert "✗" in result


def test_create_collection_no_credentials(patch_no_credentials, ctx):
    result = server.create_collection(name="X", ctx=ctx)
    assert "Error" in result
    assert "credentials" in result.lower()


# ── move_items_to_collection ─────────────────────────────────────────────────

def test_move_add_single_item(patch_web_client, ctx):
    result = server.move_items_to_collection(
        item_keys=["ITEM1"], collection_key="COL1", action="add", ctx=ctx
    )
    assert "✓ ITEM1 added to COL1" in result
    assert len(patch_web_client.added_to) == 1
    assert patch_web_client.added_to[0][0] == "COL1"


def test_move_add_multiple_items(patch_web_client, ctx):
    result = server.move_items_to_collection(
        item_keys=["A", "B", "C"], collection_key="COL2", action="add", ctx=ctx
    )
    assert result.count("✓") == 3
    assert len(patch_web_client.added_to) == 3


def test_move_remove_single_item(patch_web_client, ctx):
    result = server.move_items_to_collection(
        item_keys=["ITEM1"], collection_key="COL1", action="remove", ctx=ctx
    )
    assert "✓ ITEM1 removed from COL1" in result
    assert len(patch_web_client.removed_from) == 1


def test_move_remove_multiple_items(patch_web_client, ctx):
    result = server.move_items_to_collection(
        item_keys=["X", "Y"], collection_key="COL3", action="remove", ctx=ctx
    )
    assert result.count("✓") == 2
    assert len(patch_web_client.removed_from) == 2


def test_move_one_item_raises_error(patch_web_client, ctx):
    patch_web_client.fail_on.add("BAD")
    result = server.move_items_to_collection(
        item_keys=["GOOD", "BAD"], collection_key="COL1", action="add", ctx=ctx
    )
    assert "✓ GOOD added to COL1" in result
    assert "✗ BAD:" in result


def test_move_no_credentials(patch_no_credentials, ctx):
    result = server.move_items_to_collection(
        item_keys=["ITEM1"], collection_key="COL1", action="add", ctx=ctx
    )
    assert "Error" in result
    assert "credentials" in result.lower()


# ── update_collection ────────────────────────────────────────────────────────

def test_update_collection_rename_only(patch_web_client, ctx):
    result = server.update_collection(
        collection_key="COL1", name="New Name", ctx=ctx
    )
    assert "✓" in result
    assert "COL1" in result
    saved = patch_web_client.updated_collections[0]["data"]
    assert saved["name"] == "New Name"


def test_update_collection_reparent_only(patch_web_client, ctx):
    result = server.update_collection(
        collection_key="COL1", parent_key="NEWPARENT", ctx=ctx
    )
    assert "✓" in result
    saved = patch_web_client.updated_collections[0]["data"]
    assert saved["parentCollection"] == "NEWPARENT"


def test_update_collection_both(patch_web_client, ctx):
    result = server.update_collection(
        collection_key="COL1", name="Renamed", parent_key="PAR2", ctx=ctx
    )
    assert "✓" in result
    saved = patch_web_client.updated_collections[0]["data"]
    assert saved["name"] == "Renamed"
    assert saved["parentCollection"] == "PAR2"


def test_update_collection_nothing_to_update(patch_web_client, ctx):
    result = server.update_collection(collection_key="COL1", ctx=ctx)
    assert result == "Nothing to update."
    assert len(patch_web_client.updated_collections) == 0


# ── delete_collection ─────────────────────────────────────────────────────────

def test_delete_collection_success(patch_web_client, ctx):
    result = server.delete_collection(collection_key="COL1", ctx=ctx)
    assert "✓" in result
    assert "COL1" in result
    assert len(patch_web_client.deleted_items) == 1


def test_delete_collection_api_error(monkeypatch, fake_zot, ctx):
    def bad_delete(col):
        raise RuntimeError("API error")
    fake_zot.delete_collection = bad_delete
    monkeypatch.setattr(server, "get_web_zotero_client", lambda: fake_zot)
    result = server.delete_collection(collection_key="COL1", ctx=ctx)
    assert "Error" in result


def test_delete_collection_no_credentials(patch_no_credentials, ctx):
    result = server.delete_collection(collection_key="COL1", ctx=ctx)
    assert "Error" in result
    assert "credentials" in result.lower()
