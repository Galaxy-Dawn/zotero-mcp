import zotero_mcp.server as server


# ── update_item ──────────────────────────────────────────────────────────────

def test_update_item_single_field(patch_web_client, ctx):
    patch_web_client._items["ART1"] = {
        "data": {"key": "ART1", "itemType": "journalArticle", "title": "Old Title"}
    }
    result = server.update_item(item_key="ART1", fields={"title": "New Title"}, ctx=ctx)
    assert "Updated:" in result
    assert "title" in result
    assert "New Title" in result
    assert len(patch_web_client.updated_items) == 1


def test_update_item_multiple_fields(patch_web_client, ctx):
    patch_web_client._items["ART2"] = {
        "data": {"key": "ART2", "itemType": "journalArticle",
                 "title": "Old", "abstractNote": "Old abstract"}
    }
    result = server.update_item(
        item_key="ART2",
        fields={"title": "New", "abstractNote": "New abstract"},
        ctx=ctx,
    )
    assert "title" in result
    assert "abstractNote" in result
    assert len(patch_web_client.updated_items) == 1


def test_update_item_api_error(patch_web_client, ctx):
    patch_web_client.fail_on.add("ERR1")
    result = server.update_item(item_key="ERR1", fields={"title": "X"}, ctx=ctx)
    assert "Error" in result


def test_update_item_no_credentials(patch_no_credentials, ctx):
    result = server.update_item(item_key="ART1", fields={"title": "X"}, ctx=ctx)
    assert "Error" in result
    assert "credentials" in result.lower()


def test_update_item_empty_fields(patch_web_client, ctx):
    patch_web_client._items["ART3"] = {
        "data": {"key": "ART3", "itemType": "journalArticle"}
    }
    result = server.update_item(item_key="ART3", fields={}, ctx=ctx)
    assert "Updated:" in result
    assert len(patch_web_client.updated_items) == 1


# ── update_note ──────────────────────────────────────────────────────────────

def test_update_note_valid(patch_web_client, ctx):
    patch_web_client._items["NOTE1"] = {
        "data": {"key": "NOTE1", "itemType": "note", "note": "<p>old</p>"}
    }
    result = server.update_note(item_key="NOTE1", content="<p>new content</p>", ctx=ctx)
    assert "✓ Note NOTE1 updated" in result
    assert len(patch_web_client.updated_items) == 1


def test_update_note_wrong_type(patch_web_client, ctx):
    patch_web_client._items["ART1"] = {
        "data": {"key": "ART1", "itemType": "journalArticle"}
    }
    result = server.update_note(item_key="ART1", content="<p>text</p>", ctx=ctx)
    assert "Error" in result
    assert "journalArticle" in result


def test_update_note_html_preserved(patch_web_client, ctx):
    patch_web_client._items["NOTE2"] = {
        "data": {"key": "NOTE2", "itemType": "note", "note": ""}
    }
    html = "<h1>Title</h1><p>Para <strong>bold</strong></p>"
    server.update_note(item_key="NOTE2", content=html, ctx=ctx)
    saved = patch_web_client.updated_items[0]["data"]["note"]
    assert saved == html


def test_update_note_api_error(patch_web_client, ctx):
    patch_web_client.fail_on.add("ERRN")
    result = server.update_note(item_key="ERRN", content="text", ctx=ctx)
    assert "Error" in result


def test_update_note_no_credentials(patch_no_credentials, ctx):
    result = server.update_note(item_key="NOTE1", content="text", ctx=ctx)
    assert "Error" in result
    assert "credentials" in result.lower()
