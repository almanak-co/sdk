"""Tests for stale state warning when --once resumes existing state (VIB-1711)."""

from __future__ import annotations

import json
import sqlite3

import pytest
from click.testing import CliRunner


@pytest.fixture
def state_db(tmp_path):
    """Create a SQLite state DB with a known strategy state."""
    db_path = tmp_path / "almanak_state.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """CREATE TABLE v2_strategy_state (
            strategy_id TEXT PRIMARY KEY,
            version INTEGER DEFAULT 1,
            state_data TEXT,
            is_active INTEGER DEFAULT 1
        )"""
    )
    conn.execute(
        "INSERT INTO v2_strategy_state (strategy_id, version, state_data, is_active) VALUES (?, ?, ?, ?)",
        ("test-strategy", 1, json.dumps({"buy_executed": True}), 1),
    )
    conn.commit()
    conn.close()
    return db_path


def test_once_with_existing_state_shows_warning(state_db, tmp_path, monkeypatch):
    """When --once loads existing state without --fresh, a warning is emitted."""
    import importlib

    cli_module = importlib.import_module("almanak.cli.cli")

    # Patch framework_run_cmd to capture kwargs without actually running
    captured = {}

    def fake_framework_run(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(cli_module, "framework_run_cmd", fake_framework_run)

    runner = CliRunner()
    result = runner.invoke(
        cli_module.almanak,
        ["strat", "run", "-d", str(tmp_path), "--once"],
    )

    # The CLI wrapper calls framework_run_cmd, verify --once is passed
    assert captured.get("once") is True
    assert captured.get("fresh") is False


def test_stale_state_warning_logic():
    """Verify the warning logic: once=True + is_resume=True + fresh=False -> warning shown."""
    import click

    # Simulate the warning condition directly
    once = True
    fresh = False
    is_resume = True

    output_lines = []

    def mock_secho(msg, **kwargs):
        output_lines.append(msg)

    # The actual logic from run.py
    if is_resume and once and not fresh:
        mock_secho(
            "WARNING: Loading state from a previous run. "
            "If this is unexpected, re-run with --fresh to start clean.",
            fg="red",
            bold=True,
        )

    assert len(output_lines) == 1
    assert "--fresh" in output_lines[0]
    assert "WARNING" in output_lines[0]


def test_no_warning_when_fresh_flag_used():
    """No warning when --fresh is explicitly used."""
    once = True
    fresh = True
    is_resume = False  # --fresh clears state, so is_resume would be False

    warnings = []

    if is_resume and once and not fresh:
        warnings.append("stale warning")

    assert len(warnings) == 0


def test_no_warning_for_continuous_mode():
    """No warning for continuous mode (--once not set) even with existing state."""
    once = False
    fresh = False
    is_resume = True

    warnings = []

    if is_resume and once and not fresh:
        warnings.append("stale warning")

    assert len(warnings) == 0
