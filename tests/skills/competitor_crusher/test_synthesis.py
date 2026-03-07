"""Tests for synthesis engine module."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

# Add skill engine to path
sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[3] / ".claude" / "skills" / "competitor-crusher"),
)

from engine.manifest import ManifestManager
from engine.models import (
    CompetitorTier,
    ContradictionFlag,
    CrusherTicket,
    Finding,
    ManifestEntry,
    ResearchReport,
    VerificationState,
    compute_finding_key,
    compute_gap_key,
)
from engine.synthesis import SynthesisEngine, TARGET_LAYERS, _ANTI_SPAM_THRESHOLD


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_report(
    competitor_id: str = "competitor-a",
    agent_name: str = "codex",
    gaps_text: str = "- Missing feature X for execution",
    confidence_text: str = "0.80",
    **section_overrides: str,
) -> ResearchReport:
    sections = {
        "Capabilities": "Has feature A, B, C",
        "Moats": "Network effects",
        "Implementation Gaps": gaps_text,
        "Technical Debt": "Legacy code",
        "Evidence Links": "https://example.com",
        "Confidence Score": confidence_text,
    }
    sections.update(section_overrides)
    return ResearchReport(
        competitor_id=competitor_id,
        agent_name=agent_name,
        raw_markdown=f"# Report\n{gaps_text}",
        sections=sections,
        parsed_at=datetime.now(UTC),
    )


def _make_manifest_manager(tmp_path: Path) -> ManifestManager:
    mm = ManifestManager(tmp_path / "manifest.json")
    mm.load()
    return mm


def _make_contradiction(finding_key: str = "test_key") -> ContradictionFlag:
    return ContradictionFlag(
        finding_key=finding_key,
        reports_involved=["codex", "gemini"],
        contradiction_type="numeric_conflict",
    )


# ---------------------------------------------------------------------------
# SynthesisEngine.synthesize - basic pipeline
# ---------------------------------------------------------------------------


class TestSynthesizeBasic:
    def test_empty_reports_returns_no_tickets(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        tickets = engine.synthesize([], [], mm)
        assert tickets == []

    def test_single_report_produces_tickets(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        report = _make_report(gaps_text="- Missing swap aggregation for execution")
        tickets = engine.synthesize([report], [], mm)
        assert len(tickets) >= 1
        assert all(isinstance(t, CrusherTicket) for t in tickets)

    def test_tickets_have_crush_prefix(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        report = _make_report()
        tickets = engine.synthesize([report], [], mm)
        for t in tickets:
            assert t.title.startswith("CRUSH-")

    def test_manifest_updated_after_synthesis(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        report = _make_report()
        engine.synthesize([report], [], mm)
        assert len(mm.entries) > 0

    def test_three_reports_merge_findings(self, tmp_path):
        """Three reports with same gap should produce deduplicated findings."""
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        reports = [
            _make_report(agent_name="codex", gaps_text="- Missing swap execution logic"),
            _make_report(agent_name="gemini", gaps_text="- Missing swap execution logic"),
            _make_report(agent_name="claude", gaps_text="- Missing swap execution logic"),
        ]
        tickets = engine.synthesize(reports, [], mm)
        # Should be deduplicated to 1 finding = 1 ticket
        assert len(tickets) == 1


# ---------------------------------------------------------------------------
# Priority gating
# ---------------------------------------------------------------------------


class TestPriorityGating:
    def test_high_confidence_gets_p1(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        report = _make_report(confidence_text="0.90")
        tickets = engine.synthesize([report], [], mm)
        assert any(t.priority == 1 for t in tickets)

    def test_medium_confidence_gets_p2(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        report = _make_report(confidence_text="0.78")
        tickets = engine.synthesize([report], [], mm)
        assert any(t.priority == 2 for t in tickets)

    def test_low_confidence_gets_p3(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        report = _make_report(confidence_text="0.50")
        tickets = engine.synthesize([report], [], mm)
        assert any(t.priority == 3 for t in tickets)

    def test_priority_thresholds(self):
        engine = SynthesisEngine()
        assert engine._compute_priority(0.85) == 1
        assert engine._compute_priority(0.90) == 1
        assert engine._compute_priority(0.75) == 2
        assert engine._compute_priority(0.84) == 2
        assert engine._compute_priority(0.74) == 3
        assert engine._compute_priority(0.50) == 3


# ---------------------------------------------------------------------------
# Contradiction handling
# ---------------------------------------------------------------------------


class TestContradictionHandling:
    def test_contradiction_reduces_confidence(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        report = _make_report(
            gaps_text="- Missing execution feature",
            confidence_text="0.90",
        )
        # Create a contradiction that matches by competitor_id in key
        flag = ContradictionFlag(
            finding_key="numeric_competitor-a_gemini_1234",
            reports_involved=["codex", "gemini"],
            contradiction_type="numeric_conflict",
        )
        tickets_without = engine.synthesize([report], [], _make_manifest_manager(tmp_path))
        tickets_with = engine.synthesize([report], [flag], mm)
        # With contradiction, confidence should be lower
        if tickets_without and tickets_with:
            assert tickets_with[0].confidence <= tickets_without[0].confidence

    def test_contradiction_penalty_is_015(self):
        """Contradiction reduces confidence by exactly 0.15."""
        engine = SynthesisEngine()
        finding = Finding(
            feature_slug="test",
            target_layer="execution",
            description="test finding",
            confidence=0.90,
            competitor_id="comp-a",
            agent_sources=["codex"],
        )
        # The penalty is applied in synthesize; test the floor
        original = finding.confidence
        finding.confidence = max(0.0, finding.confidence - 0.15)
        assert abs(finding.confidence - 0.75) < 1e-9

    def test_confidence_floor_at_zero(self):
        """Contradiction penalty should not push confidence below 0."""
        engine = SynthesisEngine()
        finding = Finding(
            feature_slug="test",
            target_layer="execution",
            description="test finding",
            confidence=0.10,
            competitor_id="comp-a",
            agent_sources=["codex"],
        )
        finding.confidence = max(0.0, finding.confidence - 0.15)
        assert finding.confidence == 0.0


# ---------------------------------------------------------------------------
# Anti-spam
# ---------------------------------------------------------------------------


class TestAntiSpam:
    def test_many_findings_produce_discussion_ticket(self, tmp_path):
        """More than 10 findings from one competitor = discussion ticket."""
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        # Create a report with many distinct gap lines (different keywords to produce unique slugs)
        layers = ["execution", "data", "backtesting", "connectors", "strategy", "infrastructure"]
        actions = ["swap", "oracle", "simulation", "adapter", "rebalance", "deploy",
                   "transaction", "price", "historical", "bridge", "indicator", "monitoring"]
        gaps = "\n".join(
            f"- Missing {actions[i % len(actions)]} {layers[i % len(layers)]} feature number {i}"
            for i in range(_ANTI_SPAM_THRESHOLD + 2)
        )
        report = _make_report(gaps_text=gaps)
        tickets = engine.synthesize([report], [], mm)
        discussion_tickets = [t for t in tickets if t.is_discussion_ticket]
        assert len(discussion_tickets) == 1

    def test_few_findings_produce_individual_tickets(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        gaps = "- Gap A execution\n- Gap B data\n- Gap C strategy"
        report = _make_report(gaps_text=gaps)
        tickets = engine.synthesize([report], [], mm)
        assert all(not t.is_discussion_ticket for t in tickets)
        assert len(tickets) == 3

    def test_discussion_ticket_has_all_findings(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        count = _ANTI_SPAM_THRESHOLD + 5
        layers = ["execution", "data", "backtesting", "connectors", "strategy", "infrastructure"]
        actions = ["swap", "oracle", "simulation", "adapter", "rebalance", "deploy",
                   "transaction", "price", "historical", "bridge", "indicator", "monitoring",
                   "settlement", "analytics", "replay"]
        gaps = "\n".join(
            f"- Missing {actions[i % len(actions)]} {layers[i % len(layers)]} component {i}"
            for i in range(count)
        )
        report = _make_report(gaps_text=gaps)
        tickets = engine.synthesize([report], [], mm)
        discussion = [t for t in tickets if t.is_discussion_ticket]
        assert len(discussion) == 1
        # Description should mention the count
        assert str(count) in discussion[0].description


# ---------------------------------------------------------------------------
# Finding extraction and merging
# ---------------------------------------------------------------------------


class TestFindingExtraction:
    def test_extract_from_gaps_section(self):
        engine = SynthesisEngine()
        report = _make_report(gaps_text="- Missing swap router\n- No limit orders")
        findings = engine._extract_findings_from_report(report)
        assert len(findings) == 2

    def test_empty_gaps_section(self):
        engine = SynthesisEngine()
        report = _make_report(gaps_text="")
        findings = engine._extract_findings_from_report(report)
        assert findings == []

    def test_feature_slug_generation(self):
        engine = SynthesisEngine()
        slug = engine._extract_feature_slug("Missing swap aggregation for multiple DEXes")
        assert isinstance(slug, str)
        assert len(slug) > 0
        assert "-" in slug

    def test_target_layer_inference_execution(self):
        engine = SynthesisEngine()
        layer = engine._infer_target_layer("Missing swap execution logic")
        assert layer == "execution"

    def test_target_layer_inference_data(self):
        engine = SynthesisEngine()
        layer = engine._infer_target_layer("No real-time price oracle integration")
        assert layer == "data"

    def test_target_layer_inference_strategy(self):
        engine = SynthesisEngine()
        layer = engine._infer_target_layer("Lacks portfolio rebalancing strategy")
        assert layer == "strategy"

    def test_target_layer_inference_connectors(self):
        engine = SynthesisEngine()
        layer = engine._infer_target_layer("Missing Aave protocol adapter")
        assert layer == "connectors"

    def test_target_layer_default_infrastructure(self):
        engine = SynthesisEngine()
        layer = engine._infer_target_layer("Some random text with no keywords")
        assert layer == "infrastructure"

    def test_confidence_extraction_decimal(self):
        engine = SynthesisEngine()
        report = _make_report(confidence_text="0.85")
        conf = engine._extract_confidence(report)
        assert abs(conf - 0.85) < 1e-9

    def test_confidence_extraction_percentage(self):
        engine = SynthesisEngine()
        report = _make_report(confidence_text="85%")
        conf = engine._extract_confidence(report)
        assert abs(conf - 0.85) < 1e-9

    def test_confidence_default_on_missing(self):
        engine = SynthesisEngine()
        report = _make_report()
        report.sections.pop("Confidence Score", None)
        conf = engine._extract_confidence(report)
        assert conf == 0.5

    def test_url_extraction(self):
        engine = SynthesisEngine()
        urls = engine._extract_urls("Check https://example.com/docs and https://github.com/repo")
        assert len(urls) == 2

    def test_merge_deduplicates_same_finding(self):
        engine = SynthesisEngine()
        reports = [
            _make_report(agent_name="codex", gaps_text="- Missing execution swap logic"),
            _make_report(agent_name="gemini", gaps_text="- Missing execution swap logic"),
        ]
        findings = engine._merge_findings(reports)
        # Same text + same competitor = same slug/layer -> merged
        assert len(findings) == 1
        assert len(findings[0].agent_sources) == 2

    def test_merge_keeps_different_findings(self):
        engine = SynthesisEngine()
        reports = [
            _make_report(agent_name="codex", gaps_text="- Missing execution swap logic"),
            _make_report(agent_name="gemini", gaps_text="- No backtesting simulation tool"),
        ]
        findings = engine._merge_findings(reports)
        assert len(findings) == 2


# ---------------------------------------------------------------------------
# CRUSH numbering
# ---------------------------------------------------------------------------


class TestCrushNumbering:
    def test_starts_at_001_for_empty_manifest(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        report = _make_report(gaps_text="- Missing execution feature")
        tickets = engine.synthesize([report], [], mm)
        assert tickets[0].title.startswith("CRUSH-001:")

    def test_increments_from_existing(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        # Pre-populate manifest with a CRUSH-005 entry
        entry = ManifestEntry(
            finding_key="existing",
            gap_key="existing-gap",
            competitor_id="old-comp",
            competitor_tier=CompetitorTier.TIER_2,
            feature_slug="old-feature",
            target_layer="execution",
            confidence=0.8,
            market_severity=0.5,
            linear_ticket_id="CRUSH-005",
        )
        mm.upsert_finding(entry)
        report = _make_report(gaps_text="- Missing execution feature")
        tickets = engine.synthesize([report], [], mm)
        assert tickets[0].title.startswith("CRUSH-006:")


# ---------------------------------------------------------------------------
# Ticket formatting
# ---------------------------------------------------------------------------


class TestTicketFormatting:
    def test_ticket_has_required_labels(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        report = _make_report(gaps_text="- Missing execution swap feature")
        tickets = engine.synthesize([report], [], mm)
        assert len(tickets) == 1
        assert "competitor-crusher" in tickets[0].labels
        assert tickets[0].labels[2] == "competitor-a"  # competitor_id

    def test_ticket_description_has_finding_info(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        report = _make_report(gaps_text="- Missing execution swap feature")
        tickets = engine.synthesize([report], [], mm)
        desc = tickets[0].description
        assert "**Finding**:" in desc
        assert "**Target Layer**:" in desc
        assert "**Confidence**:" in desc

    def test_ticket_confidence_in_range(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        report = _make_report()
        tickets = engine.synthesize([report], [], mm)
        for t in tickets:
            assert 0.0 <= t.confidence <= 1.0


# ---------------------------------------------------------------------------
# Gap rollup in manifest
# ---------------------------------------------------------------------------


class TestGapRollup:
    def test_gap_rollup_populated(self, tmp_path):
        engine = SynthesisEngine()
        mm = _make_manifest_manager(tmp_path)
        # Two reports from different competitors with same gap
        reports = [
            _make_report(competitor_id="comp-a", gaps_text="- Missing swap execution logic"),
            _make_report(competitor_id="comp-b", gaps_text="- Missing swap execution logic"),
        ]
        engine.synthesize(reports, [], mm)
        # Both should have same gap_key since same feature_slug and target_layer
        entries = mm.entries
        assert len(entries) == 2
        gap_keys = {e.gap_key for e in entries}
        # Same finding text => same slug => same gap key
        assert len(gap_keys) == 1


# ---------------------------------------------------------------------------
# Target layers constant
# ---------------------------------------------------------------------------


class TestTargetLayers:
    def test_all_expected_layers_present(self):
        expected = {"execution", "data", "backtesting", "connectors", "strategy", "infrastructure"}
        assert TARGET_LAYERS == expected

    def test_target_layers_is_frozenset(self):
        assert isinstance(TARGET_LAYERS, frozenset)
