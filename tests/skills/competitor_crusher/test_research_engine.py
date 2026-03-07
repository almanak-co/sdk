"""Unit tests for ResearchEngine orchestrator."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Inject skill engine onto sys.path
sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[3] / ".claude" / "skills" / "competitor-crusher"),
)

from engine.contradiction import ContradictionDetector
from engine.linear_adapter import LinearAdapter
from engine.manifest import ManifestManager
from engine.models import (
    CompetitorTier,
    ContradictionFlag,
    CrusherTicket,
    ResearchReport,
    Signal,
)
from engine.research_engine import CrusherResult, ResearchEngine
from engine.researcher import AgentType, Researcher
from engine.synthesis import SynthesisEngine


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_report(competitor_id: str = "rival_defi", agent_name: str = "CODEX") -> ResearchReport:
    """Create a minimal valid ResearchReport."""
    return ResearchReport(
        competitor_id=competitor_id,
        agent_name=agent_name,
        raw_markdown=(
            "## Capabilities\nFast swaps\n"
            "## Moats\nLiquidity\n"
            "## Implementation Gaps\n- Missing limit orders in execution layer\n"
            "## Technical Debt\nLegacy code\n"
            "## Evidence Links\nhttps://example.com/evidence\n"
            "## Confidence Score\n0.82\n"
        ),
        sections={
            "Capabilities": "Fast swaps",
            "Moats": "Liquidity",
            "Implementation Gaps": "- Missing limit orders in execution layer",
            "Technical Debt": "Legacy code",
            "Evidence Links": "https://example.com/evidence",
            "Confidence Score": "0.82",
        },
        parsed_at=datetime.now(UTC),
    )


@pytest.fixture
def tmp_base_dir(tmp_path):
    """Create a temporary base dir mimicking skill layout."""
    research_dir = tmp_path / "research" / "competitors"
    research_dir.mkdir(parents=True)
    return tmp_path


@pytest.fixture
def engine(tmp_base_dir):
    """Create a ResearchEngine with all dependencies pointing to tmp dirs."""
    manifest_path = tmp_base_dir / "research" / "competitors" / "manifest.json"
    mm = ManifestManager(manifest_path)
    return ResearchEngine(
        manifest_manager=mm,
        base_dir=tmp_base_dir,
    )


# ---------------------------------------------------------------------------
# CrusherResult model tests
# ---------------------------------------------------------------------------

class TestCrusherResult:
    def test_defaults(self):
        r = CrusherResult()
        assert r.reports == []
        assert r.contradiction_flags == []
        assert r.findings == []
        assert r.tickets == []
        assert r.manifest_updated is False

    def test_with_data(self):
        report = _make_report()
        ticket = CrusherTicket(
            title="CRUSH-001: Test",
            description="desc",
            priority=2,
            confidence=0.8,
        )
        r = CrusherResult(
            reports=[report],
            tickets=[ticket],
            manifest_updated=True,
        )
        assert len(r.reports) == 1
        assert len(r.tickets) == 1
        assert r.manifest_updated is True


# ---------------------------------------------------------------------------
# ResearchEngine construction tests
# ---------------------------------------------------------------------------

class TestResearchEngineInit:
    def test_default_construction(self, tmp_base_dir):
        """Engine can be constructed with defaults."""
        engine = ResearchEngine(base_dir=tmp_base_dir)
        assert engine._researcher is not None
        assert engine._contradiction_detector is not None
        assert engine._synthesis_engine is not None
        assert engine._manifest_manager is not None
        assert engine._linear_adapter is not None

    def test_dependency_injection(self, tmp_base_dir):
        """Engine accepts injected dependencies."""
        researcher = MagicMock(spec=Researcher)
        detector = MagicMock(spec=ContradictionDetector)
        synthesis = MagicMock(spec=SynthesisEngine)
        mm = MagicMock(spec=ManifestManager)
        la = MagicMock(spec=LinearAdapter)

        engine = ResearchEngine(
            researcher=researcher,
            contradiction_detector=detector,
            synthesis_engine=synthesis,
            manifest_manager=mm,
            linear_adapter=la,
            base_dir=tmp_base_dir,
        )
        assert engine._researcher is researcher
        assert engine._contradiction_detector is detector
        assert engine._synthesis_engine is synthesis
        assert engine._manifest_manager is mm
        assert engine._linear_adapter is la


# ---------------------------------------------------------------------------
# run_deep tests
# ---------------------------------------------------------------------------

class TestRunDeep:
    def test_full_pipeline_dry_run(self, tmp_base_dir):
        """Full pipeline produces CrusherResult with all fields populated."""
        reports = [
            _make_report("rival_defi", "CODEX"),
            _make_report("rival_defi", "GEMINI"),
            _make_report("rival_defi", "CLAUDE"),
        ]
        tickets = [
            CrusherTicket(
                title="CRUSH-001: Missing limit orders",
                description="desc",
                priority=2,
                confidence=0.82,
            )
        ]

        researcher = MagicMock(spec=Researcher)
        researcher.run.side_effect = reports

        detector = MagicMock(spec=ContradictionDetector)
        detector.detect.return_value = []
        detector.needs_escalation.return_value = False

        synthesis = MagicMock(spec=SynthesisEngine)
        synthesis.synthesize.return_value = tickets

        mm = ManifestManager(tmp_base_dir / "research" / "competitors" / "manifest.json")

        la = MagicMock(spec=LinearAdapter)
        la.create_tickets.return_value = [{"mode": "dry_run"}]

        engine = ResearchEngine(
            researcher=researcher,
            contradiction_detector=detector,
            synthesis_engine=synthesis,
            manifest_manager=mm,
            linear_adapter=la,
            base_dir=tmp_base_dir,
        )

        result = engine.run_deep("rival_defi", dry_run=True)

        assert isinstance(result, CrusherResult)
        assert len(result.reports) == 3
        assert result.contradiction_flags == []
        assert result.manifest_updated is True
        assert len(result.tickets) == 1
        assert result.findings[0]["title"] == "CRUSH-001: Missing limit orders"

        # Verify pipeline calls
        assert researcher.run.call_count == 3
        detector.detect.assert_called_once_with(reports)
        synthesis.synthesize.assert_called_once()
        la.create_tickets.assert_called_once_with(tickets, dry_run=True)

    def test_commit_mode(self, tmp_base_dir):
        """Commit mode passes dry_run=False to linear adapter."""
        researcher = MagicMock(spec=Researcher)
        researcher.run.return_value = _make_report()

        detector = MagicMock(spec=ContradictionDetector)
        detector.detect.return_value = []
        detector.needs_escalation.return_value = False

        synthesis = MagicMock(spec=SynthesisEngine)
        synthesis.synthesize.return_value = []

        mm = ManifestManager(tmp_base_dir / "research" / "competitors" / "manifest.json")

        la = MagicMock(spec=LinearAdapter)
        la.create_tickets.return_value = []

        engine = ResearchEngine(
            researcher=researcher,
            contradiction_detector=detector,
            synthesis_engine=synthesis,
            manifest_manager=mm,
            linear_adapter=la,
            base_dir=tmp_base_dir,
        )

        engine.run_deep("rival_defi", dry_run=False)
        la.create_tickets.assert_called_once_with([], dry_run=False)

    def test_contradiction_flags_passed_to_synthesis(self, tmp_base_dir):
        """Contradiction flags flow from detector to synthesis."""
        reports = [_make_report("rival_defi", "CODEX"), _make_report("rival_defi", "GEMINI")]
        flags = [
            ContradictionFlag(
                finding_key="numeric_CODEX_GEMINI_1234",
                reports_involved=["CODEX", "GEMINI"],
                contradiction_type="numeric_conflict",
            )
        ]

        researcher = MagicMock(spec=Researcher)
        researcher.run.side_effect = reports

        detector = MagicMock(spec=ContradictionDetector)
        detector.detect.return_value = flags
        detector.needs_escalation.return_value = True

        synthesis = MagicMock(spec=SynthesisEngine)
        synthesis.synthesize.return_value = []

        mm = ManifestManager(tmp_base_dir / "research" / "competitors" / "manifest.json")
        la = MagicMock(spec=LinearAdapter)
        la.create_tickets.return_value = []

        engine = ResearchEngine(
            researcher=researcher,
            contradiction_detector=detector,
            synthesis_engine=synthesis,
            manifest_manager=mm,
            linear_adapter=la,
            base_dir=tmp_base_dir,
        )

        result = engine.run_deep("rival_defi")

        assert len(result.contradiction_flags) == 1
        # Verify flags were passed to synthesis
        call_kwargs = synthesis.synthesize.call_args
        assert call_kwargs.kwargs.get("contradiction_flags") == flags or call_kwargs[1].get("contradiction_flags") == flags

    def test_no_reports_aborts_pipeline(self, tmp_base_dir):
        """If all researchers fail, pipeline aborts gracefully."""
        researcher = MagicMock(spec=Researcher)
        researcher.run.side_effect = RuntimeError("All agents down")

        detector = MagicMock(spec=ContradictionDetector)
        synthesis = MagicMock(spec=SynthesisEngine)
        mm = ManifestManager(tmp_base_dir / "research" / "competitors" / "manifest.json")
        la = MagicMock(spec=LinearAdapter)

        engine = ResearchEngine(
            researcher=researcher,
            contradiction_detector=detector,
            synthesis_engine=synthesis,
            manifest_manager=mm,
            linear_adapter=la,
            base_dir=tmp_base_dir,
        )

        result = engine.run_deep("rival_defi")

        assert result.reports == []
        assert result.manifest_updated is False
        # Synthesis and detection should NOT have been called
        detector.detect.assert_not_called()
        synthesis.synthesize.assert_not_called()

    def test_manifest_saved_after_synthesis(self, tmp_base_dir):
        """Manifest is loaded before synthesis and saved after."""
        researcher = MagicMock(spec=Researcher)
        researcher.run.return_value = _make_report()

        detector = MagicMock(spec=ContradictionDetector)
        detector.detect.return_value = []
        detector.needs_escalation.return_value = False

        synthesis = MagicMock(spec=SynthesisEngine)
        synthesis.synthesize.return_value = []

        mm = MagicMock(spec=ManifestManager)
        la = MagicMock(spec=LinearAdapter)
        la.create_tickets.return_value = []

        engine = ResearchEngine(
            researcher=researcher,
            contradiction_detector=detector,
            synthesis_engine=synthesis,
            manifest_manager=mm,
            linear_adapter=la,
            base_dir=tmp_base_dir,
        )

        engine.run_deep("rival_defi")

        mm.load.assert_called_once()
        mm.save.assert_called_once()


# ---------------------------------------------------------------------------
# Graceful degradation tests
# ---------------------------------------------------------------------------

class TestGracefulDegradation:
    def test_one_researcher_fails(self, tmp_base_dir):
        """Pipeline continues if one researcher fails."""
        good_report = _make_report("rival_defi", "CODEX")

        researcher = MagicMock(spec=Researcher)
        researcher.run.side_effect = [
            good_report,
            RuntimeError("Gemini down"),
            _make_report("rival_defi", "CLAUDE"),
        ]

        detector = MagicMock(spec=ContradictionDetector)
        detector.detect.return_value = []
        detector.needs_escalation.return_value = False

        synthesis = MagicMock(spec=SynthesisEngine)
        synthesis.synthesize.return_value = []

        mm = ManifestManager(tmp_base_dir / "research" / "competitors" / "manifest.json")
        la = MagicMock(spec=LinearAdapter)
        la.create_tickets.return_value = []

        engine = ResearchEngine(
            researcher=researcher,
            contradiction_detector=detector,
            synthesis_engine=synthesis,
            manifest_manager=mm,
            linear_adapter=la,
            base_dir=tmp_base_dir,
        )

        result = engine.run_deep("rival_defi")

        assert len(result.reports) == 2  # 2 of 3 succeeded
        assert result.manifest_updated is True

    def test_two_researchers_fail(self, tmp_base_dir):
        """Pipeline continues with even just one report."""
        researcher = MagicMock(spec=Researcher)
        researcher.run.side_effect = [
            RuntimeError("Codex down"),
            RuntimeError("Gemini down"),
            _make_report("rival_defi", "CLAUDE"),
        ]

        detector = MagicMock(spec=ContradictionDetector)
        detector.detect.return_value = []
        detector.needs_escalation.return_value = False

        synthesis = MagicMock(spec=SynthesisEngine)
        synthesis.synthesize.return_value = []

        mm = ManifestManager(tmp_base_dir / "research" / "competitors" / "manifest.json")
        la = MagicMock(spec=LinearAdapter)
        la.create_tickets.return_value = []

        engine = ResearchEngine(
            researcher=researcher,
            contradiction_detector=detector,
            synthesis_engine=synthesis,
            manifest_manager=mm,
            linear_adapter=la,
            base_dir=tmp_base_dir,
        )

        result = engine.run_deep("rival_defi")

        assert len(result.reports) == 1
        assert result.manifest_updated is True


# ---------------------------------------------------------------------------
# run_scan tests
# ---------------------------------------------------------------------------

class TestRunScan:
    def test_scan_produces_signals(self, tmp_base_dir):
        """Scan mode produces Signal objects for each competitor."""
        researcher = MagicMock(spec=Researcher)
        researcher.run.return_value = _make_report("rival_a", "CLAUDE")

        engine = ResearchEngine(
            researcher=researcher,
            base_dir=tmp_base_dir,
        )

        signals = engine.run_scan(["rival_a", "rival_b"])

        assert len(signals) == 2
        assert all(isinstance(s, Signal) for s in signals)
        assert signals[0].competitor_id == "rival_a"
        assert signals[0].signal_type == "scan"

    def test_scan_dry_run_no_persist(self, tmp_base_dir):
        """Dry-run scan does not write signals.json."""
        researcher = MagicMock(spec=Researcher)
        researcher.run.return_value = _make_report("rival_a", "CLAUDE")

        engine = ResearchEngine(
            researcher=researcher,
            base_dir=tmp_base_dir,
        )

        engine.run_scan(["rival_a"], dry_run=True)

        signals_path = tmp_base_dir / "research" / "competitors" / "signals.json"
        assert not signals_path.exists()

    def test_scan_commit_persists_signals(self, tmp_base_dir):
        """Commit scan writes signals.json."""
        researcher = MagicMock(spec=Researcher)
        researcher.run.return_value = _make_report("rival_a", "CLAUDE")

        engine = ResearchEngine(
            researcher=researcher,
            base_dir=tmp_base_dir,
        )

        engine.run_scan(["rival_a"], dry_run=False)

        signals_path = tmp_base_dir / "research" / "competitors" / "signals.json"
        assert signals_path.exists()
        data = json.loads(signals_path.read_text())
        assert len(data) == 1
        assert data[0]["competitor_id"] == "rival_a"

    def test_scan_graceful_failure(self, tmp_base_dir):
        """Scan continues if one competitor fails."""
        researcher = MagicMock(spec=Researcher)
        researcher.run.side_effect = [
            RuntimeError("API down"),
            _make_report("rival_b", "CLAUDE"),
        ]

        engine = ResearchEngine(
            researcher=researcher,
            base_dir=tmp_base_dir,
        )

        signals = engine.run_scan(["rival_a", "rival_b"])

        assert len(signals) == 1
        assert signals[0].competitor_id == "rival_b"

    def test_scan_threat_level_high(self, tmp_base_dir):
        """High confidence maps to high threat level."""
        report = _make_report("rival_a", "CLAUDE")
        report.sections["Confidence Score"] = "0.92"

        researcher = MagicMock(spec=Researcher)
        researcher.run.return_value = report

        engine = ResearchEngine(researcher=researcher, base_dir=tmp_base_dir)
        signals = engine.run_scan(["rival_a"])

        assert signals[0].threat_level == "high"

    def test_scan_threat_level_low(self, tmp_base_dir):
        """Low confidence maps to low threat level."""
        report = _make_report("rival_a", "CLAUDE")
        report.sections["Confidence Score"] = "0.25"

        researcher = MagicMock(spec=Researcher)
        researcher.run.return_value = report

        engine = ResearchEngine(researcher=researcher, base_dir=tmp_base_dir)
        signals = engine.run_scan(["rival_a"])

        assert signals[0].threat_level == "low"

    def test_scan_recommend_deep_dive(self, tmp_base_dir):
        """Signals with implementation gaps recommend deep dive."""
        report = _make_report("rival_a", "CLAUDE")
        # Implementation Gaps has content -> should recommend deep dive

        researcher = MagicMock(spec=Researcher)
        researcher.run.return_value = report

        engine = ResearchEngine(researcher=researcher, base_dir=tmp_base_dir)
        signals = engine.run_scan(["rival_a"])

        assert signals[0].recommend_deep_dive is True

    def test_scan_no_deep_dive_when_no_gaps(self, tmp_base_dir):
        """Signals without implementation gaps do not recommend deep dive."""
        report = _make_report("rival_a", "CLAUDE")
        report.sections["Implementation Gaps"] = ""

        researcher = MagicMock(spec=Researcher)
        researcher.run.return_value = report

        engine = ResearchEngine(researcher=researcher, base_dir=tmp_base_dir)
        signals = engine.run_scan(["rival_a"])

        assert signals[0].recommend_deep_dive is False

    def test_scan_extracts_evidence_urls(self, tmp_base_dir):
        """Scan signals include URLs from Evidence Links section."""
        report = _make_report("rival_a", "CLAUDE")
        report.sections["Evidence Links"] = "https://github.com/rival https://docs.rival.io"

        researcher = MagicMock(spec=Researcher)
        researcher.run.return_value = report

        engine = ResearchEngine(researcher=researcher, base_dir=tmp_base_dir)
        signals = engine.run_scan(["rival_a"])

        assert "https://github.com/rival" in signals[0].source_urls
        assert "https://docs.rival.io" in signals[0].source_urls

    def test_scan_no_tickets_created(self, tmp_base_dir):
        """Scan mode must NOT create tickets."""
        researcher = MagicMock(spec=Researcher)
        researcher.run.return_value = _make_report("rival_a", "CLAUDE")

        la = MagicMock(spec=LinearAdapter)

        engine = ResearchEngine(
            researcher=researcher,
            linear_adapter=la,
            base_dir=tmp_base_dir,
        )

        engine.run_scan(["rival_a"])
        la.create_tickets.assert_not_called()


# ---------------------------------------------------------------------------
# _run_researchers internal tests
# ---------------------------------------------------------------------------

class TestRunResearchers:
    def test_calls_all_agent_types(self, tmp_base_dir):
        """_run_researchers invokes all 3 agent types."""
        researcher = MagicMock(spec=Researcher)
        researcher.run.return_value = _make_report()

        engine = ResearchEngine(researcher=researcher, base_dir=tmp_base_dir)
        reports = engine._run_researchers("rival_defi")

        assert len(reports) == 3
        call_args = [call.args[1] for call in researcher.run.call_args_list]
        assert AgentType.CODEX in call_args
        assert AgentType.GEMINI in call_args
        assert AgentType.CLAUDE in call_args

    def test_all_fail_returns_empty(self, tmp_base_dir):
        """If all agents fail, returns empty list."""
        researcher = MagicMock(spec=Researcher)
        researcher.run.side_effect = RuntimeError("down")

        engine = ResearchEngine(researcher=researcher, base_dir=tmp_base_dir)
        reports = engine._run_researchers("rival_defi")

        assert reports == []


# ---------------------------------------------------------------------------
# _report_to_signal internal tests
# ---------------------------------------------------------------------------

class TestReportToSignal:
    def test_basic_conversion(self, tmp_base_dir):
        """Converts a ResearchReport to a Signal."""
        report = _make_report("rival_defi", "CLAUDE")
        engine = ResearchEngine(base_dir=tmp_base_dir)

        signal = engine._report_to_signal(report)

        assert isinstance(signal, Signal)
        assert signal.competitor_id == "rival_defi"
        assert signal.signal_type == "scan"
        assert signal.threat_level == "high"  # 0.82 >= 0.8

    def test_percentage_confidence_normalized(self, tmp_base_dir):
        """Confidence > 1.0 is treated as percentage and normalized."""
        report = _make_report("rival_defi", "CLAUDE")
        report.sections["Confidence Score"] = "85"

        engine = ResearchEngine(base_dir=tmp_base_dir)
        signal = engine._report_to_signal(report)

        assert signal.threat_level == "high"  # 85% -> 0.85 >= 0.8

    def test_missing_confidence_defaults_medium(self, tmp_base_dir):
        """Missing confidence section defaults to medium threat."""
        report = _make_report("rival_defi", "CLAUDE")
        report.sections.pop("Confidence Score", None)

        engine = ResearchEngine(base_dir=tmp_base_dir)
        signal = engine._report_to_signal(report)

        assert signal.threat_level == "medium"

    def test_summary_from_capabilities(self, tmp_base_dir):
        """Summary is taken from Capabilities section."""
        report = _make_report("rival_defi", "CLAUDE")
        engine = ResearchEngine(base_dir=tmp_base_dir)

        signal = engine._report_to_signal(report)

        assert "Fast swaps" in signal.summary
