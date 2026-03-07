"""Tests for scoring_policy module."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

# Add skill engine to path
sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[3] / ".claude" / "skills" / "competitor-crusher"),
)

from engine.models import CompetitorTier, Finding
from engine.scoring_policy import (
    DEFAULT_WEIGHTS,
    _recency_score,
    _validate_weights,
    compute_market_severity,
)


def _make_finding(**overrides) -> Finding:
    defaults = {
        "feature_slug": "test-feature",
        "target_layer": "execution",
        "description": "Test finding",
        "confidence": 0.8,
        "source_urls": ["https://example.com"],
        "competitor_id": "competitor-a",
        "agent_sources": ["codex"],
    }
    defaults.update(overrides)
    return Finding(**defaults)


class TestDefaultWeights:
    def test_default_weights_keys(self):
        assert set(DEFAULT_WEIGHTS.keys()) == {
            "recency",
            "source_confidence",
            "competitor_tier",
            "adoption_signal",
        }

    def test_default_weights_values(self):
        assert DEFAULT_WEIGHTS["recency"] == 0.30
        assert DEFAULT_WEIGHTS["source_confidence"] == 0.25
        assert DEFAULT_WEIGHTS["competitor_tier"] == 0.25
        assert DEFAULT_WEIGHTS["adoption_signal"] == 0.20

    def test_default_weights_sum_to_one(self):
        assert abs(sum(DEFAULT_WEIGHTS.values()) - 1.0) < 1e-9


class TestValidateWeights:
    def test_valid_weights_pass(self):
        _validate_weights(DEFAULT_WEIGHTS)  # should not raise

    def test_invalid_weights_raise(self):
        bad = {"a": 0.5, "b": 0.3}
        with pytest.raises(ValueError, match="must sum to 1.0"):
            _validate_weights(bad)


class TestRecencyScore:
    def test_recent(self):
        recent = datetime.now(timezone.utc) - timedelta(days=5)
        assert _recency_score(recent) == 1.0

    def test_medium(self):
        medium = datetime.now(timezone.utc) - timedelta(days=60)
        assert _recency_score(medium) == 0.5

    def test_old(self):
        old = datetime.now(timezone.utc) - timedelta(days=120)
        assert _recency_score(old) == 0.2

    def test_none(self):
        assert _recency_score(None) == 0.2

    def test_boundary_30_days(self):
        at_30 = datetime.now(timezone.utc) - timedelta(days=30)
        assert _recency_score(at_30) == 0.5

    def test_boundary_90_days(self):
        at_90 = datetime.now(timezone.utc) - timedelta(days=90)
        assert _recency_score(at_90) == 0.5

    def test_naive_datetime(self):
        recent = datetime.now() - timedelta(days=5)
        assert _recency_score(recent) == 1.0


class TestComputeMarketSeverity:
    def test_returns_float_in_range(self):
        finding = _make_finding()
        score = compute_market_severity(finding)
        assert 0.0 <= score <= 1.0

    def test_uses_default_weights(self):
        finding = _make_finding(confidence=0.8)
        score = compute_market_severity(
            finding,
            competitor_tier=CompetitorTier.TIER_1,
            adoption_signal=1.0,
            last_researched=datetime.now(timezone.utc),
        )
        # All factors high -> score should be high
        assert score > 0.8

    def test_tier_1_scores_higher_than_tier_3(self):
        finding = _make_finding()
        s1 = compute_market_severity(finding, competitor_tier=CompetitorTier.TIER_1)
        s3 = compute_market_severity(finding, competitor_tier=CompetitorTier.TIER_3)
        assert s1 > s3

    def test_custom_weights(self):
        weights = {
            "recency": 0.0,
            "source_confidence": 1.0,
            "competitor_tier": 0.0,
            "adoption_signal": 0.0,
        }
        finding = _make_finding(confidence=0.9)
        score = compute_market_severity(finding, weights)
        assert abs(score - 0.9) < 1e-9

    def test_single_weak_factor_does_not_zero_score(self):
        """Additive blend: one zero factor should NOT zero the entire score."""
        finding = _make_finding(confidence=0.8)
        score = compute_market_severity(
            finding,
            competitor_tier=CompetitorTier.TIER_1,
            adoption_signal=0.0,  # one factor is zero
            last_researched=datetime.now(timezone.utc),
        )
        # Should still be > 0 because other factors contribute
        assert score > 0.3

    def test_invalid_weights_raise(self):
        finding = _make_finding()
        bad = {"recency": 0.5, "source_confidence": 0.5, "competitor_tier": 0.5, "adoption_signal": 0.5}
        with pytest.raises(ValueError):
            compute_market_severity(finding, bad)

    def test_all_minimum_factors(self):
        finding = _make_finding(confidence=0.0)
        score = compute_market_severity(
            finding,
            competitor_tier=CompetitorTier.TIER_3,
            adoption_signal=0.0,
            last_researched=datetime.now(timezone.utc) - timedelta(days=365),
        )
        # Even all-low should produce a non-negative score
        assert score >= 0.0

    def test_weighted_blend_not_multiplication(self):
        """Verify additive formula: score = sum(w_i * f_i), not product."""
        weights = {
            "recency": 0.25,
            "source_confidence": 0.25,
            "competitor_tier": 0.25,
            "adoption_signal": 0.25,
        }
        finding = _make_finding(confidence=0.8)
        score = compute_market_severity(
            finding,
            weights,
            competitor_tier=CompetitorTier.TIER_1,
            adoption_signal=0.8,
            last_researched=datetime.now(timezone.utc),
        )
        expected = 0.25 * 1.0 + 0.25 * 0.8 + 0.25 * 1.0 + 0.25 * 0.8
        assert abs(score - expected) < 1e-9
