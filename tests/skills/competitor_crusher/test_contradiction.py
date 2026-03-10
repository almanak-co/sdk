"""Tests for the two-stage contradiction detector."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

# Add skill engine to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / ".claude" / "skills" / "competitor-crusher"))

from engine.contradiction import (
    ContradictionDetector,
    _are_opposing_ordinals,
    _contexts_overlap,
    _dates_conflict,
    _extract_dates,
    _extract_numerics,
    _extract_ordinals,
    _numeric_conflict,
    _parse_date_loosely,
    build_escalation_prompt,
)
from engine.models import ContradictionFlag, ResearchReport


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_report(agent_name: str, markdown: str, competitor_id: str = "dydx") -> ResearchReport:
    return ResearchReport(
        competitor_id=competitor_id,
        agent_name=agent_name,
        raw_markdown=markdown,
        sections={},
        parsed_at=datetime.now(UTC),
    )


def _make_report_with_sections(
    agent_name: str, markdown: str, sections: dict[str, str], competitor_id: str = "dydx"
) -> ResearchReport:
    return ResearchReport(
        competitor_id=competitor_id,
        agent_name=agent_name,
        raw_markdown=markdown,
        sections=sections,
        parsed_at=datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------

class TestExtractNumerics:
    def test_basic_integer(self):
        results = _extract_numerics("They have 500 validators")
        assert len(results) >= 1
        values = [v for _, v, _ in results]
        assert 500.0 in values

    def test_millions(self):
        results = _extract_numerics("TVL of $2.5B across chains")
        values = [v for _, v, _ in results]
        assert any(v == 2_500_000_000 for v in values)

    def test_percentage(self):
        results = _extract_numerics("Market share is 45%")
        values = [v for _, v, _ in results]
        assert 45.0 in values

    def test_no_numerics(self):
        results = _extract_numerics("This is a plain text sentence.")
        assert len(results) == 0


class TestExtractOrdinals:
    def test_finds_ordinal_keyword(self):
        results = _extract_ordinals("They are the largest DEX by volume")
        assert len(results) >= 1
        assert any(kw == "largest" for _, kw in results)

    def test_multiple_keywords(self):
        text = "The fastest execution and the best UX in DeFi"
        results = _extract_ordinals(text)
        keywords = {kw for _, kw in results}
        assert "fastest" in keywords
        assert "best" in keywords

    def test_no_ordinals(self):
        results = _extract_ordinals("Simple factual statement about the protocol.")
        assert len(results) == 0


class TestExtractDates:
    def test_iso_date(self):
        results = _extract_dates("Launched on 2024-06-15 with full features")
        date_strs = [d for _, d in results]
        assert "2024-06-15" in date_strs

    def test_quarter_date(self):
        results = _extract_dates("Expected in Q1 2025 for mainnet")
        date_strs = [d for _, d in results]
        assert "Q1 2025" in date_strs

    def test_month_year(self):
        results = _extract_dates("Released in January 2024")
        date_strs = [d for _, d in results]
        assert "January 2024" in date_strs


class TestNumericConflict:
    def test_same_values(self):
        assert not _numeric_conflict(100.0, 100.0)

    def test_large_difference(self):
        assert _numeric_conflict(100.0, 300.0)

    def test_small_difference(self):
        assert not _numeric_conflict(100.0, 110.0)

    def test_both_zero(self):
        assert not _numeric_conflict(0.0, 0.0)


class TestContextsOverlap:
    def test_overlapping(self):
        assert _contexts_overlap("protocol TVL across chains", "total TVL across chains")

    def test_no_overlap(self):
        assert not _contexts_overlap("speed execution", "market governance token")


class TestOpposingOrdinals:
    def test_opposing_pair(self):
        assert _are_opposing_ordinals("largest", "smallest")
        assert _are_opposing_ordinals("fastest", "slowest")
        assert _are_opposing_ordinals("best", "worst")

    def test_non_opposing(self):
        assert not _are_opposing_ordinals("largest", "fastest")
        assert not _are_opposing_ordinals("top", "leading")

    def test_case_insensitive(self):
        assert _are_opposing_ordinals("Largest", "Smallest")


class TestParseDateLoosely:
    def test_quarter(self):
        result = _parse_date_loosely("Q2 2025")
        assert result == datetime(2025, 4, 1)

    def test_month_year(self):
        result = _parse_date_loosely("March 2025")
        assert result == datetime(2025, 3, 1)

    def test_iso_date(self):
        result = _parse_date_loosely("2025-01-15")
        assert result == datetime(2025, 1, 15)

    def test_invalid(self):
        result = _parse_date_loosely("not a date")
        assert result is None


class TestDatesConflict:
    def test_close_dates(self):
        assert not _dates_conflict("2025-01-01", "2025-02-01")

    def test_distant_dates(self):
        assert _dates_conflict("2024-01-01", "2025-01-01")

    def test_unparseable(self):
        assert not _dates_conflict("not-a-date", "2025-01-01")


# ---------------------------------------------------------------------------
# ContradictionDetector tests
# ---------------------------------------------------------------------------

class TestContradictionDetector:
    def test_empty_reports(self):
        detector = ContradictionDetector()
        assert detector.detect([]) == []

    def test_single_report_no_contradictions(self):
        detector = ContradictionDetector()
        report = _make_report("CODEX", "Simple report with no numbers or claims")
        assert detector.detect([report]) == []

    def test_clean_reports_no_contradictions(self):
        detector = ContradictionDetector()
        r1 = _make_report("CODEX", "The protocol supports Ethereum and Arbitrum.")
        r2 = _make_report("GEMINI", "Users can trade on multiple chains.")
        flags = detector.detect([r1, r2])
        assert flags == []

    def test_numeric_contradiction_detected(self):
        detector = ContradictionDetector()
        r1 = _make_report("CODEX", "The protocol TVL across chains is $500M")
        r2 = _make_report("GEMINI", "The protocol TVL across chains is $2B")
        flags = detector.detect([r1, r2])
        numeric_flags = [f for f in flags if f.contradiction_type == "numeric_conflict"]
        assert len(numeric_flags) >= 1
        assert "CODEX" in numeric_flags[0].reports_involved
        assert "GEMINI" in numeric_flags[0].reports_involved

    def test_ordinal_contradiction_detected(self):
        detector = ContradictionDetector()
        r1 = _make_report("CODEX", "dYdX is the largest perpetuals DEX by trading volume")
        r2 = _make_report("GEMINI", "dYdX is the smallest perpetuals DEX by trading volume")
        flags = detector.detect([r1, r2])
        ordinal_flags = [f for f in flags if f.contradiction_type == "ordinal_conflict"]
        assert len(ordinal_flags) >= 1

    def test_date_contradiction_detected(self):
        detector = ContradictionDetector()
        r1 = _make_report("CODEX", "The v4 chain migration happened on 2023-01-15 with full deployment")
        r2 = _make_report("GEMINI", "The v4 chain migration happened on 2024-06-01 with full deployment")
        flags = detector.detect([r1, r2])
        date_flags = [f for f in flags if f.contradiction_type == "date_discrepancy"]
        assert len(date_flags) >= 1

    def test_agreeing_reports_no_flags(self):
        detector = ContradictionDetector()
        r1 = _make_report("CODEX", "The protocol has 1000 active users on mainnet")
        r2 = _make_report("GEMINI", "About 1000 active users are on the mainnet currently")
        flags = detector.detect([r1, r2])
        # Same number in similar context should not flag
        numeric_flags = [f for f in flags if f.contradiction_type == "numeric_conflict"]
        assert len(numeric_flags) == 0

    def test_three_reports(self):
        detector = ContradictionDetector()
        r1 = _make_report("CODEX", "Protocol TVL across chains is $100M")
        r2 = _make_report("GEMINI", "Protocol TVL across chains is $100M")
        r3 = _make_report("CLAUDE", "Protocol TVL across chains is $5B")
        flags = detector.detect([r1, r2, r3])
        # r3 conflicts with r1 and r2
        assert len(flags) >= 1


class TestNeedsEscalation:
    def test_no_flags_high_confidence(self):
        detector = ContradictionDetector()
        r1 = _make_report_with_sections("CODEX", "report", {"Confidence Score": "0.85"})
        r2 = _make_report_with_sections("GEMINI", "report", {"Confidence Score": "0.90"})
        assert not detector.needs_escalation([r1, r2], [])

    def test_flags_trigger_escalation(self):
        detector = ContradictionDetector()
        r1 = _make_report_with_sections("CODEX", "report", {"Confidence Score": "0.85"})
        flag = ContradictionFlag(
            finding_key="test",
            reports_involved=["CODEX", "GEMINI"],
            contradiction_type="numeric_conflict",
        )
        assert detector.needs_escalation([r1], [flag])

    def test_low_confidence_triggers_escalation(self):
        detector = ContradictionDetector()
        r1 = _make_report_with_sections("CODEX", "report", {"Confidence Score": "0.40"})
        r2 = _make_report_with_sections("GEMINI", "report", {"Confidence Score": "0.90"})
        assert detector.needs_escalation([r1, r2], [])

    def test_no_confidence_section_no_escalation(self):
        detector = ContradictionDetector()
        r1 = _make_report_with_sections("CODEX", "report", {})
        r2 = _make_report_with_sections("GEMINI", "report", {})
        assert not detector.needs_escalation([r1, r2], [])


class TestEscalationPrompt:
    def test_prompt_structure(self):
        detector = ContradictionDetector()
        r1 = _make_report("CODEX", "Report A content")
        r2 = _make_report("GEMINI", "Report B content")
        flag = ContradictionFlag(
            finding_key="test_key",
            reports_involved=["CODEX", "GEMINI"],
            contradiction_type="numeric_conflict",
        )
        prompt = detector.get_escalation_prompt([r1, r2], [flag])
        assert "resolving contradictions" in prompt.lower()
        assert "numeric_conflict" in prompt
        assert "test_key" in prompt
        assert "CODEX" in prompt
        assert "GEMINI" in prompt
        assert "FINDING_KEY:" in prompt
        assert "RESOLUTION:" in prompt

    def test_empty_flags_still_builds_prompt(self):
        detector = ContradictionDetector()
        r1 = _make_report("CODEX", "Content")
        prompt = detector.get_escalation_prompt([r1], [])
        assert "resolving contradictions" in prompt.lower()


class TestBuildEscalationPrompt:
    def test_includes_all_reports(self):
        r1 = _make_report("CODEX", "Report A")
        r2 = _make_report("GEMINI", "Report B")
        r3 = _make_report("CLAUDE", "Report C")
        prompt = build_escalation_prompt([], [r1, r2, r3])
        assert "CODEX" in prompt
        assert "GEMINI" in prompt
        assert "CLAUDE" in prompt
        assert "Report A" in prompt
        assert "Report B" in prompt
        assert "Report C" in prompt
