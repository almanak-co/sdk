"""Tests for the Linear adapter with ticket creation and dedup."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

# Add skill engine to path
sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parents[3]
        / ".claude"
        / "skills"
        / "competitor-crusher"
    ),
)

from engine.linear_adapter import (
    LINEAR_PARENT_TICKET,
    LINEAR_PROJECT,
    LINEAR_TEAM,
    LinearAdapter,
)
from engine.manifest import ManifestManager
from engine.models import (
    CompetitorTier,
    CrusherTicket,
    ManifestEntry,
    VerificationState,
    compute_finding_key,
    compute_gap_key,
)


def _make_ticket(
    title: str = "CRUSH-001: Test finding",
    priority: int = 2,
    confidence: float = 0.80,
    evidence_links: list[str] | None = None,
    labels: list[str] | None = None,
    is_discussion: bool = False,
) -> CrusherTicket:
    return CrusherTicket(
        title=title,
        description="Test description",
        priority=priority,
        labels=labels or ["competitor-crusher"],
        evidence_links=evidence_links or [],
        confidence=confidence,
        is_discussion_ticket=is_discussion,
    )


def _make_manifest_entry(
    competitor_id: str = "dextools",
    feature_slug: str = "advanced-charting",
    target_layer: str = "data",
    linear_ticket_id: str | None = None,
    verification_state: VerificationState = VerificationState.UNVERIFIED,
) -> ManifestEntry:
    return ManifestEntry(
        finding_key=compute_finding_key(competitor_id, feature_slug, target_layer),
        gap_key=compute_gap_key(feature_slug, target_layer),
        competitor_id=competitor_id,
        competitor_tier=CompetitorTier.TIER_1,
        feature_slug=feature_slug,
        target_layer=target_layer,
        confidence=0.80,
        source_urls=["https://example.com"],
        last_researched=datetime.now(UTC),
        market_severity=0.75,
        verification_state=verification_state,
        linear_ticket_id=linear_ticket_id,
    )


class TestLinearAdapterDryRun:
    """Test dry-run mode (default)."""

    def test_dry_run_returns_ticket_dicts(self):
        adapter = LinearAdapter()
        tickets = [_make_ticket()]
        results = adapter.create_tickets(tickets, dry_run=True)
        assert len(results) == 1
        assert results[0]["mode"] == "dry_run"
        assert results[0]["title"] == "CRUSH-001: Test finding"

    def test_dry_run_is_default(self):
        adapter = LinearAdapter()
        tickets = [_make_ticket()]
        results = adapter.create_tickets(tickets)
        assert results[0]["mode"] == "dry_run"

    def test_dry_run_does_not_call_linear(self):
        adapter = LinearAdapter()
        tickets = [_make_ticket(), _make_ticket(title="CRUSH-002: Another")]
        results = adapter.create_tickets(tickets)
        assert len(results) == 2
        for r in results:
            assert r["mode"] == "dry_run"

    def test_dry_run_dict_contains_all_fields(self):
        ticket = _make_ticket(
            evidence_links=["https://evidence.com"],
            labels=["competitor-crusher", "data"],
        )
        adapter = LinearAdapter()
        result = adapter.create_tickets([ticket])[0]
        assert "title" in result
        assert "description" in result
        assert "priority" in result
        assert "labels" in result
        assert "evidence_links" in result
        assert "confidence" in result
        assert "is_discussion_ticket" in result
        assert "status" in result

    def test_empty_tickets_returns_empty(self):
        adapter = LinearAdapter()
        results = adapter.create_tickets([])
        assert results == []


class TestLinearAdapterCommitMode:
    """Test commit mode (prepares args for Linear MCP tools)."""

    def test_commit_mode_sets_team(self):
        adapter = LinearAdapter()
        tickets = [_make_ticket()]
        results = adapter.create_tickets(tickets, dry_run=False)
        assert results[0]["teamName"] == LINEAR_TEAM

    def test_commit_mode_sets_project(self):
        adapter = LinearAdapter()
        tickets = [_make_ticket()]
        results = adapter.create_tickets(tickets, dry_run=False)
        assert results[0]["projectName"] == LINEAR_PROJECT

    def test_commit_mode_sets_parent_ticket(self):
        adapter = LinearAdapter()
        tickets = [_make_ticket()]
        results = adapter.create_tickets(tickets, dry_run=False)
        assert results[0]["parentTicket"] == LINEAR_PARENT_TICKET

    def test_commit_mode_marker(self):
        adapter = LinearAdapter()
        tickets = [_make_ticket()]
        results = adapter.create_tickets(tickets, dry_run=False)
        assert results[0]["mode"] == "commit"

    def test_commit_mode_includes_evidence_in_description(self):
        ticket = _make_ticket(evidence_links=["https://evidence.com/1"])
        adapter = LinearAdapter()
        result = adapter.create_tickets([ticket], dry_run=False)[0]
        assert "https://evidence.com/1" in result["description"]

    def test_commit_mode_references_parent_ticket(self):
        adapter = LinearAdapter()
        ticket = _make_ticket()
        result = adapter.create_tickets([ticket], dry_run=False)[0]
        assert LINEAR_PARENT_TICKET in result["description"]


class TestCheckDuplicate:
    """Test duplicate checking against manifest."""

    def test_duplicate_found(self, tmp_path):
        mm = ManifestManager(tmp_path / "manifest.json")
        entry = _make_manifest_entry(linear_ticket_id="CRUSH-001")
        mm.upsert_finding(entry)

        adapter = LinearAdapter(manifest_manager=mm)
        fk = compute_finding_key("dextools", "advanced-charting", "data")
        assert adapter.check_duplicate(fk) is True

    def test_no_duplicate(self, tmp_path):
        mm = ManifestManager(tmp_path / "manifest.json")
        entry = _make_manifest_entry(linear_ticket_id=None)
        mm.upsert_finding(entry)

        adapter = LinearAdapter(manifest_manager=mm)
        fk = compute_finding_key("dextools", "advanced-charting", "data")
        assert adapter.check_duplicate(fk) is False

    def test_no_manifest_manager(self):
        adapter = LinearAdapter()
        assert adapter.check_duplicate("some-key") is False

    def test_override_manifest_manager(self, tmp_path):
        mm = ManifestManager(tmp_path / "manifest.json")
        entry = _make_manifest_entry(linear_ticket_id="CRUSH-042")
        mm.upsert_finding(entry)

        adapter = LinearAdapter()  # No default manifest
        fk = compute_finding_key("dextools", "advanced-charting", "data")
        assert adapter.check_duplicate(fk, manifest_manager=mm) is True

    def test_nonexistent_key(self, tmp_path):
        mm = ManifestManager(tmp_path / "manifest.json")
        adapter = LinearAdapter(manifest_manager=mm)
        assert adapter.check_duplicate("nonexistent-key") is False


class TestTicketStatus:
    """Test status routing (Triage vs Backlog)."""

    def test_unverified_routes_to_triage(self, tmp_path):
        mm = ManifestManager(tmp_path / "manifest.json")
        entry = _make_manifest_entry(verification_state=VerificationState.UNVERIFIED)
        mm.upsert_finding(entry)

        adapter = LinearAdapter(manifest_manager=mm)
        fk = compute_finding_key("dextools", "advanced-charting", "data")
        assert adapter.route_status(fk) == "Triage"

    def test_verified_routes_to_backlog(self, tmp_path):
        mm = ManifestManager(tmp_path / "manifest.json")
        entry = _make_manifest_entry(verification_state=VerificationState.VERIFIED)
        mm.upsert_finding(entry)

        adapter = LinearAdapter(manifest_manager=mm)
        fk = compute_finding_key("dextools", "advanced-charting", "data")
        assert adapter.route_status(fk) == "Backlog"

    def test_stale_routes_to_triage(self, tmp_path):
        mm = ManifestManager(tmp_path / "manifest.json")
        entry = _make_manifest_entry(verification_state=VerificationState.STALE)
        mm.upsert_finding(entry)

        adapter = LinearAdapter(manifest_manager=mm)
        fk = compute_finding_key("dextools", "advanced-charting", "data")
        assert adapter.route_status(fk) == "Triage"

    def test_unknown_key_routes_to_triage(self, tmp_path):
        mm = ManifestManager(tmp_path / "manifest.json")
        adapter = LinearAdapter(manifest_manager=mm)
        assert adapter.route_status("unknown-key") == "Triage"

    def test_no_manifest_routes_to_triage(self):
        adapter = LinearAdapter()
        assert adapter.route_status("any-key") == "Triage"

    def test_discussion_ticket_status_is_triage(self):
        adapter = LinearAdapter()
        ticket = _make_ticket(is_discussion=True, confidence=0.90)
        result = adapter.create_tickets([ticket])[0]
        assert result["status"] == "Triage"

    def test_high_confidence_ticket_status_is_backlog(self):
        adapter = LinearAdapter()
        ticket = _make_ticket(confidence=0.85)
        result = adapter.create_tickets([ticket])[0]
        assert result["status"] == "Backlog"

    def test_low_confidence_ticket_status_is_triage(self):
        adapter = LinearAdapter()
        ticket = _make_ticket(confidence=0.50)
        result = adapter.create_tickets([ticket])[0]
        assert result["status"] == "Triage"


class TestFormatDryRunDiff:
    """Test human-readable dry-run diff formatting."""

    def test_basic_format(self):
        adapter = LinearAdapter()
        ticket = _make_ticket(title="CRUSH-001: Test", priority=2, confidence=0.80)
        diff = adapter.format_dry_run_diff([ticket])
        assert "+ CRUSH-001: Test [P2] (confidence: 0.80)" in diff

    def test_with_evidence(self):
        adapter = LinearAdapter()
        ticket = _make_ticket(evidence_links=["https://example.com/proof"])
        diff = adapter.format_dry_run_diff([ticket])
        assert "evidence: https://example.com/proof" in diff

    def test_discussion_ticket_marker(self):
        adapter = LinearAdapter()
        ticket = _make_ticket(is_discussion=True)
        diff = adapter.format_dry_run_diff([ticket])
        assert "[DISCUSSION TICKET]" in diff

    def test_labels_shown(self):
        adapter = LinearAdapter()
        ticket = _make_ticket(labels=["competitor-crusher", "data", "dextools"])
        diff = adapter.format_dry_run_diff([ticket])
        assert "labels: competitor-crusher, data, dextools" in diff

    def test_empty_tickets(self):
        adapter = LinearAdapter()
        diff = adapter.format_dry_run_diff([])
        assert diff == "No tickets to create."

    def test_multiple_tickets(self):
        adapter = LinearAdapter()
        tickets = [
            _make_ticket(title="CRUSH-001: First", priority=1, confidence=0.90),
            _make_ticket(title="CRUSH-002: Second", priority=3, confidence=0.60),
        ]
        diff = adapter.format_dry_run_diff(tickets)
        assert "+ CRUSH-001: First [P1] (confidence: 0.90)" in diff
        assert "+ CRUSH-002: Second [P3] (confidence: 0.60)" in diff
