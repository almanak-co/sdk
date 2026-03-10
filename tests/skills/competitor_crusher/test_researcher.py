"""Tests for the researcher runner module."""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add skill engine to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / ".claude" / "skills" / "competitor-crusher"))

from engine.models import ResearchReport
from engine.researcher import (
    REQUIRED_SECTIONS,
    AgentType,
    Researcher,
    _build_prompt,
    _save_raw_output,
    parse_report,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_MARKDOWN = """# Research Report

## Capabilities
Strong swap aggregation across 10+ DEXes. Supports limit orders.

## Moats
First-mover advantage on Solana. Deep liquidity partnerships.

## Implementation Gaps
No intent-based architecture. Limited backtesting support compared to Almanak.

## Technical Debt
Legacy V1 contracts still active. Monolithic codebase.

## Evidence Links
- https://docs.example.com/api
- https://github.com/example/protocol

## Confidence Score
0.82
"""


@pytest.fixture
def researcher(tmp_path: Path) -> Researcher:
    return Researcher(output_dir=tmp_path, timeout=5)


# ---------------------------------------------------------------------------
# AgentType enum
# ---------------------------------------------------------------------------


def test_agent_type_values():
    assert AgentType.CODEX.value == "CODEX"
    assert AgentType.GEMINI.value == "GEMINI"
    assert AgentType.CLAUDE.value == "CLAUDE"


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------


def test_build_prompt_contains_competitor():
    prompt = _build_prompt("jupiter", AgentType.CODEX)
    assert "jupiter" in prompt


def test_build_prompt_contains_focus():
    prompt = _build_prompt("aave", AgentType.GEMINI)
    assert "product features" in prompt
    assert "documentation quality" in prompt


def test_build_prompt_contains_all_sections():
    prompt = _build_prompt("morpho", AgentType.CLAUDE)
    for section in REQUIRED_SECTIONS:
        assert f"## {section}" in prompt


def test_build_prompt_claude_focus():
    prompt = _build_prompt("test", AgentType.CLAUDE)
    assert "competitive positioning" in prompt


# ---------------------------------------------------------------------------
# Report parsing
# ---------------------------------------------------------------------------


def test_parse_report_extracts_all_sections():
    report = parse_report(SAMPLE_MARKDOWN, "jupiter", "CODEX")
    assert isinstance(report, ResearchReport)
    assert report.competitor_id == "jupiter"
    assert report.agent_name == "CODEX"
    for section in REQUIRED_SECTIONS:
        assert section in report.sections


def test_parse_report_section_content():
    report = parse_report(SAMPLE_MARKDOWN, "jupiter", "CODEX")
    assert "swap aggregation" in report.sections["Capabilities"]
    assert "First-mover" in report.sections["Moats"]
    assert "No intent-based" in report.sections["Implementation Gaps"]


def test_parse_report_preserves_raw_markdown():
    report = parse_report(SAMPLE_MARKDOWN, "test", "GEMINI")
    assert report.raw_markdown == SAMPLE_MARKDOWN


def test_parse_report_empty_markdown():
    report = parse_report("", "test", "CLAUDE")
    assert report.sections == {}
    assert report.raw_markdown == ""


def test_parse_report_partial_sections():
    md = "## Capabilities\nSome caps\n## Moats\nSome moats\n"
    report = parse_report(md, "test", "CODEX")
    assert "Capabilities" in report.sections
    assert "Moats" in report.sections
    assert len(report.sections) == 2


def test_parse_report_has_parsed_at():
    report = parse_report(SAMPLE_MARKDOWN, "test", "CODEX")
    assert isinstance(report.parsed_at, datetime)


# ---------------------------------------------------------------------------
# Raw output saving
# ---------------------------------------------------------------------------


def test_save_raw_output(tmp_path: Path):
    path = _save_raw_output("jupiter", "codex", "# Report content", base_dir=tmp_path)
    assert path.exists()
    assert path.name == "jupiter_codex.md"
    assert path.read_text() == "# Report content"


def test_save_raw_output_creates_dirs(tmp_path: Path):
    nested = tmp_path / "a" / "b" / "c"
    path = _save_raw_output("aave", "gemini", "content", base_dir=nested)
    assert path.exists()
    assert nested.exists()


# ---------------------------------------------------------------------------
# Researcher.run - Claude (returns prompt, no subprocess)
# ---------------------------------------------------------------------------


def test_run_claude_returns_prompt(researcher: Researcher):
    report = researcher.run("jupiter", AgentType.CLAUDE)
    assert report.agent_name == "CLAUDE"
    assert report.competitor_id == "jupiter"
    assert "jupiter" in report.raw_markdown
    assert "competitive positioning" in report.raw_markdown


def test_run_claude_saves_output(researcher: Researcher):
    researcher.run("jupiter", AgentType.CLAUDE)
    output_file = researcher.output_dir / "jupiter_claude.md"
    assert output_file.exists()


# ---------------------------------------------------------------------------
# Researcher.run - Codex (subprocess mocked)
# ---------------------------------------------------------------------------


@patch("engine.researcher.subprocess.run")
def test_run_codex_success(mock_run: MagicMock, researcher: Researcher):
    mock_run.return_value = MagicMock(
        returncode=0,
        stdout=SAMPLE_MARKDOWN,
        stderr="",
    )
    report = researcher.run("aave", AgentType.CODEX)
    assert report.agent_name == "CODEX"
    assert report.competitor_id == "aave"
    assert "Capabilities" in report.sections
    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert cmd[0] == "codex"


@patch("engine.researcher.subprocess.run")
def test_run_gemini_success(mock_run: MagicMock, researcher: Researcher):
    mock_run.return_value = MagicMock(
        returncode=0,
        stdout=SAMPLE_MARKDOWN,
        stderr="",
    )
    report = researcher.run("morpho", AgentType.GEMINI)
    assert report.agent_name == "GEMINI"
    assert report.competitor_id == "morpho"
    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert cmd[0] == "gemini"


@patch("engine.researcher.subprocess.run")
def test_run_codex_nonzero_exit(mock_run: MagicMock, researcher: Researcher):
    mock_run.return_value = MagicMock(
        returncode=1,
        stdout="partial output",
        stderr="some error",
    )
    report = researcher.run("test", AgentType.CODEX)
    assert report.raw_markdown == "partial output"


@patch("engine.researcher.subprocess.run")
def test_run_codex_nonzero_exit_no_stdout(mock_run: MagicMock, researcher: Researcher):
    mock_run.return_value = MagicMock(
        returncode=1,
        stdout="",
        stderr="fatal error",
    )
    report = researcher.run("test", AgentType.CODEX)
    assert "Error" in report.raw_markdown
    assert "fatal error" in report.raw_markdown


@patch("engine.researcher.subprocess.run")
def test_run_timeout(mock_run: MagicMock, researcher: Researcher):
    mock_run.side_effect = subprocess.TimeoutExpired(cmd="codex", timeout=5)
    report = researcher.run("test", AgentType.CODEX)
    assert "timed out" in report.raw_markdown.lower()
    assert "Partial Report" in report.raw_markdown


@patch("engine.researcher.subprocess.run")
def test_run_cli_not_found(mock_run: MagicMock, researcher: Researcher):
    mock_run.side_effect = FileNotFoundError()
    report = researcher.run("test", AgentType.GEMINI)
    assert "not found" in report.raw_markdown.lower()


@patch("engine.researcher.subprocess.run")
def test_run_saves_subprocess_output(mock_run: MagicMock, researcher: Researcher):
    mock_run.return_value = MagicMock(returncode=0, stdout="# Output", stderr="")
    researcher.run("test", AgentType.CODEX)
    output_file = researcher.output_dir / "test_codex.md"
    assert output_file.exists()
    assert output_file.read_text() == "# Output"


# ---------------------------------------------------------------------------
# Timeout configuration
# ---------------------------------------------------------------------------


def test_custom_timeout():
    r = Researcher(timeout=120)
    assert r.timeout == 120


def test_default_timeout():
    r = Researcher()
    assert r.timeout == 60
