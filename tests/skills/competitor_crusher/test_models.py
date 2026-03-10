"""Unit tests for Competitor Crusher models, enums, and key helpers."""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

# Add the skill engine to the path so we can import it
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / ".claude" / "skills" / "competitor-crusher"))

from engine.models import (
    CompetitorTier,
    ContradictionFlag,
    CrusherTicket,
    Finding,
    ManifestEntry,
    ResearchReport,
    Signal,
    TierOverride,
    VerificationState,
    compute_finding_key,
    compute_gap_key,
)


# ---------------------------------------------------------------------------
# CompetitorTier
# ---------------------------------------------------------------------------

class TestCompetitorTier:
    def test_tier_values(self):
        assert CompetitorTier.TIER_1.value == "tier_1"
        assert CompetitorTier.TIER_2.value == "tier_2"
        assert CompetitorTier.TIER_3.value == "tier_3"

    def test_tier_scores(self):
        assert CompetitorTier.TIER_1.score == 1.0
        assert CompetitorTier.TIER_2.score == 0.6
        assert CompetitorTier.TIER_3.score == 0.3


# ---------------------------------------------------------------------------
# VerificationState
# ---------------------------------------------------------------------------

class TestVerificationState:
    def test_values(self):
        assert VerificationState.VERIFIED == "verified"
        assert VerificationState.UNVERIFIED == "unverified"
        assert VerificationState.STALE == "stale"


# ---------------------------------------------------------------------------
# Key generation
# ---------------------------------------------------------------------------

class TestKeyGeneration:
    def test_compute_finding_key_deterministic(self):
        k1 = compute_finding_key("comp_a", "feat_x", "execution")
        k2 = compute_finding_key("comp_a", "feat_x", "execution")
        assert k1 == k2
        assert len(k1) == 64  # sha256 hex

    def test_compute_finding_key_differs_by_input(self):
        k1 = compute_finding_key("comp_a", "feat_x", "execution")
        k2 = compute_finding_key("comp_b", "feat_x", "execution")
        assert k1 != k2

    def test_compute_gap_key_deterministic(self):
        k1 = compute_gap_key("feat_x", "execution")
        k2 = compute_gap_key("feat_x", "execution")
        assert k1 == k2
        assert len(k1) == 64

    def test_compute_gap_key_differs_by_input(self):
        k1 = compute_gap_key("feat_x", "execution")
        k2 = compute_gap_key("feat_y", "execution")
        assert k1 != k2

    def test_finding_key_and_gap_key_differ(self):
        fk = compute_finding_key("comp", "feat", "layer")
        gk = compute_gap_key("feat", "layer")
        assert fk != gk


# ---------------------------------------------------------------------------
# TierOverride
# ---------------------------------------------------------------------------

class TestTierOverride:
    def test_creation(self):
        now = datetime.now()
        override = TierOverride(
            override_tier=CompetitorTier.TIER_1,
            override_reason="Major threat",
            override_author="analyst",
            overridden_at=now,
        )
        assert override.override_tier == CompetitorTier.TIER_1
        assert override.override_reason == "Major threat"
        assert override.override_author == "analyst"
        assert override.overridden_at == now


# ---------------------------------------------------------------------------
# ManifestEntry
# ---------------------------------------------------------------------------

class TestManifestEntry:
    def _make_entry(self, **kwargs):
        defaults = dict(
            finding_key="abc123",
            gap_key="def456",
            competitor_id="rival_co",
            competitor_tier=CompetitorTier.TIER_2,
            feature_slug="auto_rebalance",
            target_layer="execution",
            confidence=0.8,
        )
        defaults.update(kwargs)
        return ManifestEntry(**defaults)

    def test_basic_creation(self):
        entry = self._make_entry()
        assert entry.competitor_id == "rival_co"
        assert entry.confidence == 0.8
        assert entry.verification_state == VerificationState.UNVERIFIED
        assert entry.linear_ticket_id is None

    def test_effective_tier_without_override(self):
        entry = self._make_entry(competitor_tier=CompetitorTier.TIER_3)
        assert entry.effective_tier == CompetitorTier.TIER_3

    def test_effective_tier_with_override(self):
        override = TierOverride(
            override_tier=CompetitorTier.TIER_1,
            override_reason="Acquired funding",
            override_author="pm",
            overridden_at=datetime.now(),
        )
        entry = self._make_entry(
            competitor_tier=CompetitorTier.TIER_3,
            tier_override=override,
        )
        assert entry.effective_tier == CompetitorTier.TIER_1

    def test_all_fields_present(self):
        entry = self._make_entry(
            source_urls=["https://example.com"],
            last_verified_at=datetime.now(),
            last_researched=datetime.now(),
            market_severity=0.75,
            verification_state=VerificationState.VERIFIED,
            linear_ticket_id="VIB-100",
        )
        assert entry.source_urls == ["https://example.com"]
        assert entry.market_severity == 0.75
        assert entry.verification_state == VerificationState.VERIFIED
        assert entry.linear_ticket_id == "VIB-100"

    def test_confidence_validation(self):
        with pytest.raises(Exception):
            self._make_entry(confidence=1.5)
        with pytest.raises(Exception):
            self._make_entry(confidence=-0.1)


# ---------------------------------------------------------------------------
# Signal
# ---------------------------------------------------------------------------

class TestSignal:
    def test_creation(self):
        signal = Signal(
            competitor_id="rival_co",
            signal_type="product_launch",
            threat_level="high",
            summary="Rival launched competing feature",
            source_urls=["https://example.com/blog"],
            recommend_deep_dive=True,
        )
        assert signal.competitor_id == "rival_co"
        assert signal.recommend_deep_dive is True
        assert isinstance(signal.created_at, datetime)


# ---------------------------------------------------------------------------
# ResearchReport
# ---------------------------------------------------------------------------

class TestResearchReport:
    def test_creation(self):
        report = ResearchReport(
            competitor_id="rival_co",
            agent_name="codex",
            raw_markdown="# Report\n\nContent here",
            sections={
                "Capabilities": "They can do X",
                "Moats": "Network effects",
            },
        )
        assert report.agent_name == "codex"
        assert "Capabilities" in report.sections
        assert isinstance(report.parsed_at, datetime)

    def test_required_sections_schema(self):
        """Verify the expected section keys match the PRD."""
        expected = {
            "Capabilities",
            "Moats",
            "Implementation Gaps",
            "Technical Debt",
            "Evidence Links",
            "Confidence Score",
        }
        report = ResearchReport(
            competitor_id="test",
            agent_name="test",
            raw_markdown="",
            sections={k: "" for k in expected},
        )
        assert set(report.sections.keys()) == expected


# ---------------------------------------------------------------------------
# Finding
# ---------------------------------------------------------------------------

class TestFinding:
    def test_creation(self):
        finding = Finding(
            feature_slug="auto_rebalance",
            target_layer="execution",
            description="Rival has auto-rebalance",
            confidence=0.9,
            source_urls=["https://docs.rival.com"],
            competitor_id="rival_co",
            agent_sources=["codex", "gemini"],
        )
        assert finding.feature_slug == "auto_rebalance"
        assert len(finding.agent_sources) == 2

    def test_confidence_bounds(self):
        with pytest.raises(Exception):
            Finding(
                feature_slug="x",
                target_layer="y",
                description="d",
                confidence=2.0,
                competitor_id="c",
            )


# ---------------------------------------------------------------------------
# CrusherTicket
# ---------------------------------------------------------------------------

class TestCrusherTicket:
    def test_creation(self):
        ticket = CrusherTicket(
            title="CRUSH-001: Add auto-rebalance",
            description="Rival has this, we don't",
            priority=1,
            labels=["execution", "competitive"],
            evidence_links=["https://example.com"],
            confidence=0.9,
        )
        assert ticket.title.startswith("CRUSH-")
        assert ticket.is_discussion_ticket is False

    def test_discussion_ticket(self):
        ticket = CrusherTicket(
            title="CRUSH-002: Bulk discussion",
            description="Many related findings",
            priority=3,
            confidence=0.5,
            is_discussion_ticket=True,
        )
        assert ticket.is_discussion_ticket is True

    def test_priority_bounds(self):
        with pytest.raises(Exception):
            CrusherTicket(
                title="CRUSH-999: Bad",
                description="x",
                priority=0,
                confidence=0.5,
            )
        with pytest.raises(Exception):
            CrusherTicket(
                title="CRUSH-999: Bad",
                description="x",
                priority=5,
                confidence=0.5,
            )


# ---------------------------------------------------------------------------
# ContradictionFlag
# ---------------------------------------------------------------------------

class TestContradictionFlag:
    def test_creation(self):
        flag = ContradictionFlag(
            finding_key="abc123",
            reports_involved=["codex", "gemini"],
            contradiction_type="numeric_conflict",
        )
        assert flag.finding_key == "abc123"
        assert len(flag.reports_involved) == 2
        assert flag.resolution is None
        assert flag.resolved_by is None

    def test_resolved(self):
        flag = ContradictionFlag(
            finding_key="abc123",
            reports_involved=["codex", "gemini"],
            contradiction_type="ordinal_conflict",
            resolution="Codex data more recent",
            resolved_by="llm",
        )
        assert flag.resolution == "Codex data more recent"
        assert flag.resolved_by == "llm"
