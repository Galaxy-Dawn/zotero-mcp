import json
import os
from pathlib import Path

import pytest


def test_claude_code_settings_injects_keys(tmp_path, monkeypatch):
    home = tmp_path / "home"
    (home / ".claude").mkdir(parents=True)
    (home / ".claude" / "settings.json").write_text(json.dumps({
        "mcpServers": {"zotero": {"env": {
            "ZOTERO_API_KEY": "test-key",
            "ZOTERO_LIBRARY_ID": "12345",
        }}}
    }))
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.delenv("ZOTERO_API_KEY", raising=False)
    monkeypatch.delenv("ZOTERO_LIBRARY_ID", raising=False)

    from zotero_mcp.client import _load_from_ai_tool_configs
    _load_from_ai_tool_configs()

    assert os.environ["ZOTERO_API_KEY"] == "test-key"
    assert os.environ["ZOTERO_LIBRARY_ID"] == "12345"


def test_claude_code_settings_missing_zotero_key(tmp_path, monkeypatch):
    home = tmp_path / "home"
    (home / ".claude").mkdir(parents=True)
    (home / ".claude" / "settings.json").write_text(json.dumps({
        "mcpServers": {"other-server": {"env": {"SOME_KEY": "val"}}}
    }))
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.delenv("ZOTERO_API_KEY", raising=False)

    from zotero_mcp.client import _load_from_ai_tool_configs
    _load_from_ai_tool_configs()

    assert "ZOTERO_API_KEY" not in os.environ


def test_claude_code_settings_malformed_json(tmp_path, monkeypatch):
    home = tmp_path / "home"
    (home / ".claude").mkdir(parents=True)
    (home / ".claude" / "settings.json").write_text("{ not valid json }")
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.delenv("ZOTERO_API_KEY", raising=False)

    from zotero_mcp.client import _load_from_ai_tool_configs
    _load_from_ai_tool_configs()  # must not raise

    assert "ZOTERO_API_KEY" not in os.environ


def test_opencode_jsonc_with_comments(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir(parents=True)
    jsonc = (
        '// top comment\n'
        '{\n'
        '  // inner comment\n'
        '  "mcp": {\n'
        '    "servers": {\n'
        '      "zotero": {\n'
        '        "env": {\n'
        '          "ZOTERO_API_KEY": "opencode-key",\n'
        '          "ZOTERO_LIBRARY_ID": "99999"\n'
        '        }\n'
        '      }\n'
        '    }\n'
        '  }\n'
        '}\n'
    )
    (home / "opencode.jsonc").write_text(jsonc)
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.delenv("ZOTERO_API_KEY", raising=False)
    monkeypatch.delenv("ZOTERO_LIBRARY_ID", raising=False)

    from zotero_mcp.client import _load_from_ai_tool_configs
    _load_from_ai_tool_configs()

    assert os.environ["ZOTERO_API_KEY"] == "opencode-key"
    assert os.environ["ZOTERO_LIBRARY_ID"] == "99999"


def test_opencode_jsonc_malformed_after_comment_strip(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir(parents=True)
    (home / "opencode.jsonc").write_text("{ bad json }")
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.delenv("ZOTERO_API_KEY", raising=False)

    from zotero_mcp.client import _load_from_ai_tool_configs
    _load_from_ai_tool_configs()  # must not raise

    assert "ZOTERO_API_KEY" not in os.environ


def test_config_toml_valid(tmp_path, monkeypatch):
    home = tmp_path / "home"
    (home / ".codex").mkdir(parents=True)
    (home / ".codex" / "config.toml").write_text(
        '[mcp_servers.zotero.env]\n'
        'ZOTERO_API_KEY = "toml-key"\n'
        'ZOTERO_LIBRARY_ID = "77777"\n'
    )
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.delenv("ZOTERO_API_KEY", raising=False)
    monkeypatch.delenv("ZOTERO_LIBRARY_ID", raising=False)

    from zotero_mcp.client import _load_from_ai_tool_configs
    _load_from_ai_tool_configs()

    assert os.environ["ZOTERO_API_KEY"] == "toml-key"
    assert os.environ["ZOTERO_LIBRARY_ID"] == "77777"


def test_config_toml_missing_mcp_servers_key(tmp_path, monkeypatch):
    home = tmp_path / "home"
    (home / ".codex").mkdir(parents=True)
    (home / ".codex" / "config.toml").write_text('[other_section]\nfoo = "bar"\n')
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.delenv("ZOTERO_API_KEY", raising=False)

    from zotero_mcp.client import _load_from_ai_tool_configs
    _load_from_ai_tool_configs()

    assert "ZOTERO_API_KEY" not in os.environ


def test_first_seen_wins_no_duplicate(tmp_path, monkeypatch):
    """Claude Code settings take precedence over OpenCode for the same key."""
    home = tmp_path / "home"
    (home / ".claude").mkdir(parents=True)
    (home / ".claude" / "settings.json").write_text(json.dumps({
        "mcpServers": {"zotero": {"env": {"ZOTERO_API_KEY": "first-key"}}}
    }))
    (home / "opencode.jsonc").write_text(json.dumps({
        "mcp": {"servers": {"zotero": {"env": {"ZOTERO_API_KEY": "second-key"}}}}
    }))
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.delenv("ZOTERO_API_KEY", raising=False)

    from zotero_mcp.client import _load_from_ai_tool_configs
    _load_from_ai_tool_configs()

    assert os.environ["ZOTERO_API_KEY"] == "first-key"


def test_existing_env_not_overwritten(tmp_path, monkeypatch):
    home = tmp_path / "home"
    (home / ".claude").mkdir(parents=True)
    (home / ".claude" / "settings.json").write_text(json.dumps({
        "mcpServers": {"zotero": {"env": {"ZOTERO_API_KEY": "config-key"}}}
    }))
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.setenv("ZOTERO_API_KEY", "pre-existing-key")

    from zotero_mcp.client import _load_from_ai_tool_configs
    _load_from_ai_tool_configs()

    assert os.environ["ZOTERO_API_KEY"] == "pre-existing-key"


def test_no_config_files_no_crash(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.delenv("ZOTERO_API_KEY", raising=False)

    from zotero_mcp.client import _load_from_ai_tool_configs
    _load_from_ai_tool_configs()  # must not raise

    assert "ZOTERO_API_KEY" not in os.environ


def test_settings_json_empty_object(tmp_path, monkeypatch):
    home = tmp_path / "home"
    (home / ".claude").mkdir(parents=True)
    (home / ".claude" / "settings.json").write_text("{}")
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.delenv("ZOTERO_API_KEY", raising=False)

    from zotero_mcp.client import _load_from_ai_tool_configs
    _load_from_ai_tool_configs()  # must not raise

    assert "ZOTERO_API_KEY" not in os.environ


def test_all_absent_env_already_set(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.setenv("ZOTERO_API_KEY", "already-set")

    from zotero_mcp.client import _load_from_ai_tool_configs
    _load_from_ai_tool_configs()

    assert os.environ["ZOTERO_API_KEY"] == "already-set"
