"""Unit tests for configuration hash and reproducibility functionality.

Tests cover:
- Config hash calculation is deterministic
- Same config produces same hash
- Different config produces different hash
- Hash survives serialization round-trip
- Config hash is included in BacktestResult
"""

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.models import BacktestEngine, BacktestMetrics, BacktestResult
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def base_config() -> PnLBacktestConfig:
    """Create a base configuration for testing."""
    return PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
        interval_seconds=3600,
        token_funding=_pnl_token_funding(Decimal("10000"), chain="arbitrum"),
        fee_model="realistic",
        slippage_model="realistic",
        include_gas_costs=True,
        gas_price_gwei=Decimal("30"),
        inclusion_delay_blocks=1,
        chain="arbitrum",
        tokens=["WETH", "USDC"],
        benchmark_token="WETH",
        risk_free_rate=Decimal("0.05"),
        trading_days_per_year=365,
        initial_margin_ratio=Decimal("0.1"),
        maintenance_margin_ratio=Decimal("0.05"),
        mev_simulation_enabled=False,
        auto_correct_positions=False,
        reconciliation_alert_threshold_pct=Decimal("0.05"),
        random_seed=42,
    )


@pytest.fixture
def identical_config() -> PnLBacktestConfig:
    """Create an identical configuration to base_config."""
    return PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
        interval_seconds=3600,
        token_funding=_pnl_token_funding(Decimal("10000"), chain="arbitrum"),
        fee_model="realistic",
        slippage_model="realistic",
        include_gas_costs=True,
        gas_price_gwei=Decimal("30"),
        inclusion_delay_blocks=1,
        chain="arbitrum",
        tokens=["WETH", "USDC"],
        benchmark_token="WETH",
        risk_free_rate=Decimal("0.05"),
        trading_days_per_year=365,
        initial_margin_ratio=Decimal("0.1"),
        maintenance_margin_ratio=Decimal("0.05"),
        mev_simulation_enabled=False,
        auto_correct_positions=False,
        reconciliation_alert_threshold_pct=Decimal("0.05"),
        random_seed=42,
    )


# =============================================================================
# Config Hash Determinism Tests
# =============================================================================


def test_config_hash_is_deterministic(base_config: PnLBacktestConfig) -> None:
    """Config hash should return the same value for multiple calls."""
    hash1 = base_config.calculate_config_hash()
    hash2 = base_config.calculate_config_hash()
    hash3 = base_config.calculate_config_hash()

    assert hash1 == hash2
    assert hash2 == hash3
    assert len(hash1) == 64  # SHA-256 produces 64 hex characters


def test_identical_configs_produce_same_hash(
    base_config: PnLBacktestConfig,
    identical_config: PnLBacktestConfig,
) -> None:
    """Two separate config objects with identical values should produce same hash."""
    hash1 = base_config.calculate_config_hash()
    hash2 = identical_config.calculate_config_hash()

    assert hash1 == hash2


def test_different_start_time_produces_different_hash(base_config: PnLBacktestConfig) -> None:
    """Different start_time should produce different hash."""
    modified_config = PnLBacktestConfig(
        start_time=datetime(2024, 2, 1, tzinfo=UTC),  # Changed
        end_time=base_config.end_time,
        interval_seconds=base_config.interval_seconds,
        token_funding=_pnl_token_funding(Decimal("10000"), chain=base_config.chain),
        fee_model=base_config.fee_model,
        slippage_model=base_config.slippage_model,
        chain=base_config.chain,
        tokens=base_config.tokens,
        random_seed=base_config.random_seed,
    )

    assert base_config.calculate_config_hash() != modified_config.calculate_config_hash()


def test_different_capital_produces_different_hash(base_config: PnLBacktestConfig) -> None:
    """Different startup funding should produce different hash."""
    modified_config = PnLBacktestConfig(
        start_time=base_config.start_time,
        end_time=base_config.end_time,
        interval_seconds=base_config.interval_seconds,
        token_funding=_pnl_token_funding(Decimal("50000"), chain=base_config.chain),  # Changed
        fee_model=base_config.fee_model,
        slippage_model=base_config.slippage_model,
        chain=base_config.chain,
        tokens=base_config.tokens,
        random_seed=base_config.random_seed,
    )

    assert base_config.calculate_config_hash() != modified_config.calculate_config_hash()


def test_different_fee_model_produces_different_hash(base_config: PnLBacktestConfig) -> None:
    """Different fee_model should produce different hash."""
    modified_config = PnLBacktestConfig(
        start_time=base_config.start_time,
        end_time=base_config.end_time,
        interval_seconds=base_config.interval_seconds,
        token_funding=_pnl_token_funding(Decimal("10000"), chain=base_config.chain),
        fee_model="zero",  # Changed
        slippage_model=base_config.slippage_model,
        chain=base_config.chain,
        tokens=base_config.tokens,
        random_seed=base_config.random_seed,
    )

    assert base_config.calculate_config_hash() != modified_config.calculate_config_hash()


def test_different_random_seed_produces_different_hash(base_config: PnLBacktestConfig) -> None:
    """Different random_seed should produce different hash."""
    modified_config = PnLBacktestConfig(
        start_time=base_config.start_time,
        end_time=base_config.end_time,
        interval_seconds=base_config.interval_seconds,
        token_funding=_pnl_token_funding(Decimal("10000"), chain=base_config.chain),
        fee_model=base_config.fee_model,
        slippage_model=base_config.slippage_model,
        chain=base_config.chain,
        tokens=base_config.tokens,
        random_seed=123,  # Changed
    )

    assert base_config.calculate_config_hash() != modified_config.calculate_config_hash()


def test_token_order_does_not_affect_hash(base_config: PnLBacktestConfig) -> None:
    """Token order should not affect hash (tokens are sorted)."""
    modified_config = PnLBacktestConfig(
        start_time=base_config.start_time,
        end_time=base_config.end_time,
        interval_seconds=base_config.interval_seconds,
        token_funding=_pnl_token_funding(Decimal("10000"), chain=base_config.chain),
        fee_model=base_config.fee_model,
        slippage_model=base_config.slippage_model,
        # VIB-5088: the gas default is chain-aware, not a constant -- copy
        # the fixture's explicit value so token order is the only difference.
        gas_price_gwei=base_config.gas_price_gwei,
        chain=base_config.chain,
        tokens=["USDC", "WETH"],  # Reversed order
        random_seed=base_config.random_seed,
    )

    assert base_config.calculate_config_hash() == modified_config.calculate_config_hash()


def test_config_hash_normalizes_json_unsafe_token_values() -> None:
    """Token refs and Decimal funding values are normalized before hashing."""
    base_usdc = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    config = PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 2, tzinfo=UTC),
        token_funding=[
            {
                "symbol": "USDC",
                "address": base_usdc,
                "chain": "base",
                "amount": Decimal("10000"),
                "amount_type": "token",
            }
        ],
        chain="base",
        tokens=[("base", base_usdc), "WETH"],  # type: ignore[list-item]
    )

    assert len(config.calculate_config_hash()) == 64
    assert config.to_dict()["token_funding"][0]["amount"] == "10000"
    assert ["base", base_usdc] in config.to_dict()["tokens"]
    json.dumps(config.to_dict())


def test_different_chain_produces_different_hash(base_config: PnLBacktestConfig) -> None:
    """Different chain should produce different hash."""
    modified_config = PnLBacktestConfig(
        start_time=base_config.start_time,
        end_time=base_config.end_time,
        interval_seconds=base_config.interval_seconds,
        token_funding=_pnl_token_funding(Decimal("10000"), chain="base"),
        fee_model=base_config.fee_model,
        slippage_model=base_config.slippage_model,
        chain="base",  # Changed
        tokens=base_config.tokens,
        random_seed=base_config.random_seed,
    )

    assert base_config.calculate_config_hash() != modified_config.calculate_config_hash()


# =============================================================================
# Config Validation Boundary Tests
# =============================================================================


class TestPnLBacktestConfigValidation:
    """Direct coverage for PnLBacktestConfig.__post_init__ validation."""

    @staticmethod
    def _config(**overrides: object) -> PnLBacktestConfig:
        params = {
            "start_time": datetime(2024, 1, 1, tzinfo=UTC),
            "end_time": datetime(2024, 1, 2, tzinfo=UTC),
            "interval_seconds": 3600,
            "token_funding": _pnl_token_funding(Decimal("10000")),
            "gas_price_gwei": Decimal("30"),
            "chain": "arbitrum",
            "tokens": ["WETH", "USDC"],
        }
        params.update(overrides)
        return PnLBacktestConfig(**params)

    @pytest.mark.parametrize(
        ("overrides", "message"),
        [
            ({"end_time": datetime(2024, 1, 1, tzinfo=UTC)}, "end_time must be after start_time"),
            ({"interval_seconds": 0}, "interval_seconds must be positive"),
            ({"token_funding": {"symbol": "USDC"}}, "token_funding must be a list"),
            ({"inclusion_delay_blocks": -1}, "inclusion_delay_blocks cannot be negative"),
            ({"tokens": []}, "tokens list cannot be empty"),
            ({"gas_price_gwei": Decimal("-0.1")}, "gas_price_gwei cannot be negative"),
        ],
    )
    def test_basic_invalid_values_raise(self, overrides: dict[str, object], message: str) -> None:
        with pytest.raises(ValueError, match=message):
            self._config(**overrides)

    @pytest.mark.parametrize(
        ("overrides", "message"),
        [
            ({"initial_margin_ratio": Decimal("0")}, "initial_margin_ratio"),
            ({"initial_margin_ratio": Decimal("1.01")}, "initial_margin_ratio"),
            ({"maintenance_margin_ratio": Decimal("0")}, "maintenance_margin_ratio"),
            ({"maintenance_margin_ratio": Decimal("1.01")}, "maintenance_margin_ratio"),
            (
                {"initial_margin_ratio": Decimal("0.1"), "maintenance_margin_ratio": Decimal("0.2")},
                "maintenance_margin_ratio must be <= initial_margin_ratio",
            ),
        ],
    )
    def test_margin_invalid_values_raise(self, overrides: dict[str, object], message: str) -> None:
        with pytest.raises(ValueError, match=message):
            self._config(**overrides)

    @pytest.mark.parametrize(
        "overrides",
        [
            {"initial_margin_ratio": Decimal("1"), "maintenance_margin_ratio": Decimal("1")},
            {"initial_margin_ratio": Decimal("0.5"), "maintenance_margin_ratio": Decimal("0.5")},
        ],
    )
    def test_margin_boundary_values_are_allowed(self, overrides: dict[str, object]) -> None:
        config = self._config(**overrides)

        assert config.initial_margin_ratio == overrides["initial_margin_ratio"]
        assert config.maintenance_margin_ratio == overrides["maintenance_margin_ratio"]

    @pytest.mark.parametrize(
        ("overrides", "message"),
        [
            (
                {"reconciliation_alert_threshold_pct": Decimal("-0.01")},
                "reconciliation_alert_threshold_pct cannot be negative",
            ),
            ({"staleness_threshold_seconds": -1}, "staleness_threshold_seconds cannot be negative"),
            ({"min_data_coverage": Decimal("-0.01")}, "min_data_coverage must be between 0 and 1"),
            ({"min_data_coverage": Decimal("1.01")}, "min_data_coverage must be between 0 and 1"),
        ],
    )
    def test_data_quality_invalid_values_raise(self, overrides: dict[str, object], message: str) -> None:
        with pytest.raises(ValueError, match=message):
            self._config(**overrides)

    @pytest.mark.parametrize("min_data_coverage", [Decimal("0"), Decimal("1")])
    def test_data_coverage_boundaries_are_allowed(self, min_data_coverage: Decimal) -> None:
        config = self._config(min_data_coverage=min_data_coverage)

        assert config.min_data_coverage == min_data_coverage

    def test_institutional_mode_enforces_strict_defaults_and_minimum_coverage(self) -> None:
        config = self._config(
            institutional_mode=True,
            strict_reproducibility=False,
            allow_degraded_data=True,
            allow_hardcoded_fallback=True,
            require_symbol_mapping=False,
            min_data_coverage=Decimal("0.5"),
        )

        assert config.strict_reproducibility is True
        assert config.allow_degraded_data is False
        assert config.allow_hardcoded_fallback is False
        assert config.require_symbol_mapping is True
        assert config.min_data_coverage == Decimal("0.98")

    def test_institutional_mode_preserves_higher_minimum_coverage(self) -> None:
        config = self._config(
            institutional_mode=True,
            min_data_coverage=Decimal("0.99"),
        )

        assert config.min_data_coverage == Decimal("0.99")


# =============================================================================
# Serialization Round-Trip Tests
# =============================================================================


def test_hash_survives_serialization_roundtrip(base_config: PnLBacktestConfig) -> None:
    """Config hash should be identical after serialization round-trip."""
    original_hash = base_config.calculate_config_hash()

    # Serialize and deserialize
    config_dict = base_config.to_dict()
    restored_config = PnLBacktestConfig.from_dict(config_dict)

    restored_hash = restored_config.calculate_config_hash()

    assert original_hash == restored_hash


def test_hash_survives_metadata_roundtrip(base_config: PnLBacktestConfig) -> None:
    """Config hash should be identical after to_dict_with_metadata round-trip."""
    original_hash = base_config.calculate_config_hash()

    # Serialize with metadata and deserialize (metadata is ignored by from_dict)
    config_dict = base_config.to_dict_with_metadata(
        data_provider_info={"name": "coingecko", "version": "1.0"}
    )
    restored_config = PnLBacktestConfig.from_dict(config_dict)

    restored_hash = restored_config.calculate_config_hash()

    assert original_hash == restored_hash


# =============================================================================
# BacktestResult Integration Tests
# =============================================================================


def test_backtest_result_includes_config_hash() -> None:
    """BacktestResult should include config_hash field."""
    result = BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id="test_strategy",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
        metrics=BacktestMetrics(),
        config_hash="abc123def456",
    )

    assert result.config_hash == "abc123def456"


def test_backtest_result_config_hash_serialization() -> None:
    """BacktestResult config_hash should serialize and deserialize correctly."""
    expected_hash = "a1b2c3d4e5f6" * 8 + "a1b2c3d4e5f67890"  # 64 chars
    result = BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id="test_strategy",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
        metrics=BacktestMetrics(),
        config_hash=expected_hash,
    )

    # Serialize
    result_dict = result.to_dict()
    assert result_dict["config_hash"] == expected_hash

    # Deserialize
    restored_result = BacktestResult.from_dict(result_dict)
    assert restored_result.config_hash == expected_hash


def test_backtest_result_config_hash_none_by_default() -> None:
    """BacktestResult config_hash should be None by default."""
    result = BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id="test_strategy",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
        metrics=BacktestMetrics(),
    )

    assert result.config_hash is None


def test_backtest_result_config_hash_none_serialization() -> None:
    """BacktestResult with None config_hash should serialize/deserialize correctly."""
    result = BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id="test_strategy",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
        metrics=BacktestMetrics(),
        config_hash=None,
    )

    # Serialize
    result_dict = result.to_dict()
    assert result_dict["config_hash"] is None

    # Deserialize
    restored_result = BacktestResult.from_dict(result_dict)
    assert restored_result.config_hash is None


# =============================================================================
# Hash Format Tests
# =============================================================================


def test_config_hash_is_valid_sha256(base_config: PnLBacktestConfig) -> None:
    """Config hash should be a valid SHA-256 hex string."""
    hash_value = base_config.calculate_config_hash()

    # SHA-256 produces 64 hex characters
    assert len(hash_value) == 64

    # Should only contain valid hex characters
    assert all(c in "0123456789abcdef" for c in hash_value)


def test_config_hash_is_lowercase(base_config: PnLBacktestConfig) -> None:
    """Config hash should be lowercase hex."""
    hash_value = base_config.calculate_config_hash()

    assert hash_value == hash_value.lower()


# =============================================================================
# Edge Case Tests
# =============================================================================


def test_config_hash_with_none_random_seed() -> None:
    """Config hash should handle None random_seed correctly."""
    config_with_none = PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
        token_funding=_pnl_token_funding(Decimal("10000")),
        random_seed=None,
    )

    config_with_seed = PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
        token_funding=_pnl_token_funding(Decimal("10000")),
        random_seed=42,
    )

    # Both should produce valid hashes
    hash_none = config_with_none.calculate_config_hash()
    hash_seed = config_with_seed.calculate_config_hash()

    assert len(hash_none) == 64
    assert len(hash_seed) == 64
    assert hash_none != hash_seed


def test_config_hash_with_many_tokens() -> None:
    """Config hash should handle many tokens correctly."""
    config = PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
        token_funding=_pnl_token_funding(Decimal("10000")),
        tokens=["WETH", "USDC", "WBTC", "DAI", "USDT", "LINK", "UNI", "AAVE"],
    )

    hash_value = config.calculate_config_hash()
    assert len(hash_value) == 64


def test_config_hash_with_extreme_values() -> None:
    """Config hash should handle extreme values correctly."""
    config = PnLBacktestConfig(
        start_time=datetime(2020, 1, 1, tzinfo=UTC),
        end_time=datetime(2030, 12, 31, tzinfo=UTC),
        interval_seconds=60,
        token_funding=_pnl_token_funding(Decimal("1000000000")),  # 1 billion
        gas_price_gwei=Decimal("1000"),
        inclusion_delay_blocks=100,
    )

    hash_value = config.calculate_config_hash()
    assert len(hash_value) == 64
