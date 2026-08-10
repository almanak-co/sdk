"""Contracts for the standalone multi-model discussion helper."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType


def _load_discussion_module() -> ModuleType:
    repo_root = Path(__file__).parents[3]
    source = repo_root / "scripts" / "ai-discussion" / "discuss.py"
    spec = importlib.util.spec_from_file_location("ai_discussion", source)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_grok_reviewer_is_headless_and_read_only() -> None:
    module = _load_discussion_module()
    config = module.MODEL_CONFIGS["grok"]
    command = config["cmd"]

    assert command[-1] == "-p"
    assert command[command.index("--permission-mode") + 1] == "plan"
    assert command[command.index("--tools") + 1] == "Read,Grep,Glob"
    assert "--no-subagents" in command
    assert "--disable-web-search" in command
    assert "--no-memory" in command
    assert "--allow" not in command
    assert "--always-approve" not in command
    assert "bypassPermissions" not in command
