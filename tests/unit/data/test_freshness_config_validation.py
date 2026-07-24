"""Tests for FreshnessConfig.__post_init__ validation.

FreshnessConfig guards MarketSnapshot staleness thresholds. __post_init__
enforces two invariants per data type (price / gas / pool):

1. Every threshold must be non-negative.
2. The warn threshold must be <= the error threshold.

These tests construct FreshnessConfig directly (never MarketSnapshot, which
is surface-locked) and pin the exact ValueError messages.
"""

from __future__ import annotations

import pytest

from almanak.framework.data.market_snapshot import FreshnessConfig


class TestFreshnessConfigValidConstruction:
    def test_defaults_are_valid(self):
        config = FreshnessConfig()

        assert config.price_warn_sec == 30.0
        assert config.price_error_sec == 300.0
        assert config.gas_warn_sec == 30.0
        assert config.gas_error_sec == 300.0
        assert config.pool_warn_sec == 30.0
        assert config.pool_error_sec == 300.0
        assert config.enabled is True

    def test_custom_strict_thresholds_accepted(self):
        config = FreshnessConfig(
            price_warn_sec=10,
            price_error_sec=60,
            gas_warn_sec=5,
            gas_error_sec=30,
            pool_warn_sec=1,
            pool_error_sec=2,
        )

        assert config.price_warn_sec == 10
        assert config.pool_error_sec == 2

    def test_zero_thresholds_accepted(self):
        """Zero is non-negative and satisfies warn <= error at the boundary."""
        config = FreshnessConfig(
            price_warn_sec=0,
            price_error_sec=0,
            gas_warn_sec=0,
            gas_error_sec=0,
            pool_warn_sec=0,
            pool_error_sec=0,
        )

        assert config.price_warn_sec == 0
        assert config.gas_error_sec == 0

    @pytest.mark.parametrize(
        ("warn_field", "error_field"),
        [
            ("price_warn_sec", "price_error_sec"),
            ("gas_warn_sec", "gas_error_sec"),
            ("pool_warn_sec", "pool_error_sec"),
        ],
    )
    def test_warn_equal_to_error_accepted(self, warn_field, error_field):
        """warn == error is allowed; only warn > error is rejected."""
        config = FreshnessConfig(**{warn_field: 120.0, error_field: 120.0})

        assert getattr(config, warn_field) == 120.0
        assert getattr(config, error_field) == 120.0

    def test_disabled_config_still_validates(self):
        """enabled=False disables runtime checks, not construction validation."""
        config = FreshnessConfig(enabled=False)
        assert config.enabled is False

        with pytest.raises(ValueError, match="price_warn_sec must be non-negative"):
            FreshnessConfig(price_warn_sec=-1, enabled=False)


class TestFreshnessConfigNegativeThresholds:
    @pytest.mark.parametrize(
        "field",
        [
            "price_warn_sec",
            "price_error_sec",
            "gas_warn_sec",
            "gas_error_sec",
            "pool_warn_sec",
            "pool_error_sec",
        ],
    )
    def test_negative_value_rejected(self, field):
        with pytest.raises(ValueError, match=f"{field} must be non-negative"):
            FreshnessConfig(**{field: -0.5})

    def test_negative_warn_reported_before_ordering_violation(self):
        """A negative warn threshold fails the non-negativity check first,
        even though it would also violate warn <= error ordering."""
        with pytest.raises(ValueError, match="gas_warn_sec must be non-negative"):
            FreshnessConfig(gas_warn_sec=-10, gas_error_sec=-20)


class TestFreshnessConfigWarnAboveError:
    @pytest.mark.parametrize(
        ("warn_field", "error_field"),
        [
            ("price_warn_sec", "price_error_sec"),
            ("gas_warn_sec", "gas_error_sec"),
            ("pool_warn_sec", "pool_error_sec"),
        ],
    )
    def test_warn_greater_than_error_rejected(self, warn_field, error_field):
        with pytest.raises(ValueError, match=f"{warn_field} must be <= {error_field}"):
            FreshnessConfig(**{warn_field: 301.0, error_field: 300.0})


class TestFreshnessConfigSerialization:
    def test_round_trip_preserves_values(self):
        """from_dict(to_dict(...)) re-runs __post_init__ on valid values."""
        original = FreshnessConfig(
            price_warn_sec=15.0,
            price_error_sec=90.0,
            gas_warn_sec=5.0,
            gas_error_sec=45.0,
            pool_warn_sec=20.0,
            pool_error_sec=200.0,
            enabled=False,
        )

        restored = FreshnessConfig.from_dict(original.to_dict())

        assert restored == original

    def test_from_dict_invalid_values_still_rejected(self):
        with pytest.raises(ValueError, match="pool_warn_sec must be <= pool_error_sec"):
            FreshnessConfig.from_dict({"pool_warn_sec": 500.0, "pool_error_sec": 100.0})
