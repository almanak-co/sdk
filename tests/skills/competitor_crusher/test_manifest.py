"""Tests for ManifestManager."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

# Engine is outside almanak/ package, so add to path
sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[3] / ".claude" / "skills" / "competitor-crusher"),
)

from engine.manifest import ManifestManager
from engine.models import CompetitorTier, ManifestEntry, VerificationState, compute_finding_key, compute_gap_key


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_entry(
    competitor_id: str = "competitor_a",
    feature_slug: str = "feature_x",
    target_layer: str = "execution",
    confidence: float = 0.8,
    last_researched: datetime | None = None,
    tier: CompetitorTier = CompetitorTier.TIER_1,
) -> ManifestEntry:
    fk = compute_finding_key(competitor_id, feature_slug, target_layer)
    gk = compute_gap_key(feature_slug, target_layer)
    return ManifestEntry(
        finding_key=fk,
        gap_key=gk,
        competitor_id=competitor_id,
        competitor_tier=tier,
        feature_slug=feature_slug,
        target_layer=target_layer,
        confidence=confidence,
        last_researched=last_researched,
    )


# ---------------------------------------------------------------------------
# Tests: load / save round-trip
# ---------------------------------------------------------------------------

class TestLoadSave:
    def test_load_missing_file(self, tmp_path: Path) -> None:
        mgr = ManifestManager(tmp_path / "missing.json")
        mgr.load()
        assert mgr.entries == []

    def test_save_creates_parent_dirs(self, tmp_path: Path) -> None:
        path = tmp_path / "deep" / "nested" / "manifest.json"
        mgr = ManifestManager(path)
        mgr.load()
        mgr.save()
        assert path.exists()
        assert json.loads(path.read_text()) == []

    def test_round_trip(self, tmp_path: Path) -> None:
        path = tmp_path / "manifest.json"
        entry = _make_entry(last_researched=datetime.now(UTC))

        # Save
        mgr = ManifestManager(path)
        mgr.load()
        mgr.upsert_finding(entry)
        mgr.save()

        # Reload
        mgr2 = ManifestManager(path)
        mgr2.load()
        assert len(mgr2.entries) == 1
        assert mgr2.entries[0].finding_key == entry.finding_key
        assert mgr2.entries[0].competitor_id == "competitor_a"

    def test_save_pretty_formatting(self, tmp_path: Path) -> None:
        path = tmp_path / "manifest.json"
        mgr = ManifestManager(path)
        mgr.load()
        mgr.upsert_finding(_make_entry())
        mgr.save()
        text = path.read_text()
        # Pretty-printed JSON has newlines
        assert "\n" in text


# ---------------------------------------------------------------------------
# Tests: upsert_finding dedup
# ---------------------------------------------------------------------------

class TestUpsertFinding:
    def test_insert_new_entry(self, tmp_path: Path) -> None:
        mgr = ManifestManager(tmp_path / "m.json")
        mgr.load()
        entry = _make_entry()
        mgr.upsert_finding(entry)
        assert len(mgr.entries) == 1

    def test_upsert_updates_existing(self, tmp_path: Path) -> None:
        mgr = ManifestManager(tmp_path / "m.json")
        mgr.load()
        entry1 = _make_entry(confidence=0.5)
        mgr.upsert_finding(entry1)

        entry2 = _make_entry(confidence=0.9)  # same finding_key
        mgr.upsert_finding(entry2)

        assert len(mgr.entries) == 1
        assert mgr.entries[0].confidence == 0.9

    def test_different_findings_not_deduped(self, tmp_path: Path) -> None:
        mgr = ManifestManager(tmp_path / "m.json")
        mgr.load()
        mgr.upsert_finding(_make_entry(feature_slug="feat_a"))
        mgr.upsert_finding(_make_entry(feature_slug="feat_b"))
        assert len(mgr.entries) == 2

    def test_cross_competitor_duplicate_logged(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """Same gap_key from different competitors should log an info message."""
        mgr = ManifestManager(tmp_path / "m.json")
        mgr.load()
        # Same feature/layer -> same gap_key, different competitor -> different finding_key
        mgr.upsert_finding(_make_entry(competitor_id="alpha"))
        with caplog.at_level(logging.INFO):
            mgr.upsert_finding(_make_entry(competitor_id="beta"))
        assert "cross-competitor duplicate" in caplog.text.lower()
        assert len(mgr.entries) == 2


# ---------------------------------------------------------------------------
# Tests: get_gap_rollup
# ---------------------------------------------------------------------------

class TestGapRollup:
    def test_rollup_groups_by_gap_key(self, tmp_path: Path) -> None:
        mgr = ManifestManager(tmp_path / "m.json")
        mgr.load()
        e1 = _make_entry(competitor_id="a", feature_slug="feat", target_layer="execution")
        e2 = _make_entry(competitor_id="b", feature_slug="feat", target_layer="execution")
        e3 = _make_entry(competitor_id="c", feature_slug="other", target_layer="data")
        mgr.upsert_finding(e1)
        mgr.upsert_finding(e2)
        mgr.upsert_finding(e3)

        rollup = mgr.get_gap_rollup(e1.gap_key)
        assert len(rollup) == 2
        assert {e.competitor_id for e in rollup} == {"a", "b"}

    def test_rollup_empty_for_unknown_key(self, tmp_path: Path) -> None:
        mgr = ManifestManager(tmp_path / "m.json")
        mgr.load()
        assert mgr.get_gap_rollup("nonexistent") == []


# ---------------------------------------------------------------------------
# Tests: get_stale_findings
# ---------------------------------------------------------------------------

class TestStaleFindingS:
    def test_no_last_researched_is_stale(self, tmp_path: Path) -> None:
        mgr = ManifestManager(tmp_path / "m.json")
        mgr.load()
        mgr.upsert_finding(_make_entry(last_researched=None))
        stale = mgr.get_stale_findings(threshold_days=30)
        assert len(stale) == 1

    def test_recent_finding_not_stale(self, tmp_path: Path) -> None:
        mgr = ManifestManager(tmp_path / "m.json")
        mgr.load()
        mgr.upsert_finding(_make_entry(last_researched=datetime.now(UTC)))
        stale = mgr.get_stale_findings(threshold_days=30)
        assert len(stale) == 0

    def test_old_finding_is_stale(self, tmp_path: Path) -> None:
        mgr = ManifestManager(tmp_path / "m.json")
        mgr.load()
        old_date = datetime.now(UTC) - timedelta(days=60)
        mgr.upsert_finding(_make_entry(last_researched=old_date))
        stale = mgr.get_stale_findings(threshold_days=30)
        assert len(stale) == 1

    def test_custom_threshold(self, tmp_path: Path) -> None:
        mgr = ManifestManager(tmp_path / "m.json")
        mgr.load()
        old_date = datetime.now(UTC) - timedelta(days=15)
        mgr.upsert_finding(_make_entry(last_researched=old_date))
        assert len(mgr.get_stale_findings(threshold_days=10)) == 1
        assert len(mgr.get_stale_findings(threshold_days=30)) == 0

    def test_mixed_stale_and_fresh(self, tmp_path: Path) -> None:
        mgr = ManifestManager(tmp_path / "m.json")
        mgr.load()
        mgr.upsert_finding(_make_entry(
            feature_slug="old",
            last_researched=datetime.now(UTC) - timedelta(days=60),
        ))
        mgr.upsert_finding(_make_entry(
            feature_slug="new",
            last_researched=datetime.now(UTC),
        ))
        stale = mgr.get_stale_findings(threshold_days=30)
        assert len(stale) == 1
        assert stale[0].feature_slug == "old"


# ---------------------------------------------------------------------------
# Tests: mark_verified
# ---------------------------------------------------------------------------

class TestMarkVerified:
    def test_mark_verified_updates_entry(self, tmp_path: Path) -> None:
        mgr = ManifestManager(tmp_path / "m.json")
        mgr.load()
        entry = _make_entry()
        mgr.upsert_finding(entry)

        ts = datetime.now(UTC)
        result = mgr.mark_verified(entry.finding_key, ts)
        assert result is True
        assert mgr.entries[0].last_verified_at == ts
        assert mgr.entries[0].verification_state == VerificationState.VERIFIED

    def test_mark_verified_unknown_key(self, tmp_path: Path) -> None:
        mgr = ManifestManager(tmp_path / "m.json")
        mgr.load()
        result = mgr.mark_verified("nonexistent")
        assert result is False

    def test_mark_verified_default_timestamp(self, tmp_path: Path) -> None:
        mgr = ManifestManager(tmp_path / "m.json")
        mgr.load()
        entry = _make_entry()
        mgr.upsert_finding(entry)
        mgr.mark_verified(entry.finding_key)
        assert mgr.entries[0].last_verified_at is not None
