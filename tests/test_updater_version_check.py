from __future__ import annotations


def test_update_check_does_not_suggest_downgrade(monkeypatch) -> None:
    from zotero_mcp import updater

    monkeypatch.setattr(updater, "get_current_version", lambda: "0.1.5")
    monkeypatch.setattr(updater, "get_latest_version", lambda: "0.1.4")

    result = updater.update_zotero_mcp(check_only=True, force=False)
    assert result["success"] is True
    assert result["needs_update"] is False


def test_update_check_suggests_update_when_newer_exists(monkeypatch) -> None:
    from zotero_mcp import updater

    monkeypatch.setattr(updater, "get_current_version", lambda: "0.1.3")
    monkeypatch.setattr(updater, "get_latest_version", lambda: "0.1.4")

    result = updater.update_zotero_mcp(check_only=True, force=False)
    assert result["success"] is True
    assert result["needs_update"] is True

