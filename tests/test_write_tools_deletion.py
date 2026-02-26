import zotero_mcp.server as server


def test_delete_single_item(patch_web_client, ctx):
    result = server.delete_items(item_keys=["ITEM1"], ctx=ctx)
    assert "✓ ITEM1 moved to trash" in result
    assert len(patch_web_client.deleted_items) == 1


def test_delete_multiple_items(patch_web_client, ctx):
    result = server.delete_items(item_keys=["A1", "B2", "C3"], ctx=ctx)
    assert "✓ A1 moved to trash" in result
    assert "✓ B2 moved to trash" in result
    assert "✓ C3 moved to trash" in result
    assert len(patch_web_client.deleted_items) == 3


def test_delete_one_item_raises_error(patch_web_client, ctx):
    patch_web_client.fail_on.add("BAD1")
    result = server.delete_items(item_keys=["GOOD1", "BAD1"], ctx=ctx)
    assert "✓ GOOD1 moved to trash" in result
    assert "✗ BAD1:" in result
    assert len(patch_web_client.deleted_items) == 1


def test_delete_all_items_raise_errors(patch_web_client, ctx):
    patch_web_client.fail_on.update(["X1", "X2"])
    result = server.delete_items(item_keys=["X1", "X2"], ctx=ctx)
    assert "✗ X1:" in result
    assert "✗ X2:" in result
    assert len(patch_web_client.deleted_items) == 0


def test_delete_empty_list(patch_web_client, ctx):
    result = server.delete_items(item_keys=[], ctx=ctx)
    assert result == ""


def test_delete_no_credentials(patch_no_credentials, ctx):
    result = server.delete_items(item_keys=["ITEM1"], ctx=ctx)
    assert "Error" in result
    assert "credentials" in result.lower()
