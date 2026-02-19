"""Unit tests for configuration hash and reproducibility functionality.

Tests cover:
- Config hash calculation is deterministic
- Same config produces same hash
- Different config produces different hash
- Hash survives serialization round-trip
- Config hash is included in BacktestResult
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.models import BacktestEngine, BacktestMetrics, BacktestResult
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig

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
        initial_capital_usd=Decimal("10000"),
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
        initial_capital_usd=Decimal("10000"),
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
        initial_capital_usd=base_config.initial_capital_usd,
        fee_model=base_config.fee_model,
        slippage_model=base_config.slippage_model,
        chain=base_config.chain,
        tokens=base_config.tokens,
        random_seed=base_config.random_seed,
    )

    assert base_config.calculate_config_hash() != modified_config.calculate_config_hash()


def test_different_capital_produces_different_hash(base_config: PnLBacktestConfig) -> None:
    """Different initial_capital_usd should produce different hash."""
    modified_config = PnLBacktestConfig(
        start_time=base_config.start_time,
        end_time=base_config.end_time,
        interval_seconds=base_config.interval_seconds,
        initial_capital_usd=Decimal("50000"),  # Changed
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
        initial_capital_usd=base_config.initial_capital_usd,
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
        initial_capital_usd=base_config.initial_capital_usd,
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
        initial_capital_usd=base_config.initial_capital_usd,
        fee_model=base_config.fee_model,
        slippage_model=base_config.slippage_model,
        chain=base_config.chain,
        tokens=["USDC", "WETH"],  # Reversed order
        random_seed=base_config.random_seed,
    )

    assert base_config.calculate_config_hash() == modified_config.calculate_config_hash()


def test_different_chain_produces_different_hash(base_config: PnLBacktestConfig) -> None:
    """Different chain should produce different hash."""
    modified_config = PnLBacktestConfig(
        start_time=base_config.start_time,
        end_time=base_config.end_time,
        interval_seconds=base_config.interval_seconds,
        initial_capital_usd=base_config.initial_capital_usd,
        fee_model=base_config.fee_model,
        slippage_model=base_config.slippage_model,
        chain="base",  # Changed
        tokens=base_config.tokens,
        random_seed=base_config.random_seed,
    )

    assert base_config.calculate_config_hash() != modified_config.calculate_config_hash()


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
        strategy_id="test_strategy",
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
        strategy_id="test_strategy",
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
        strategy_id="test_strategy",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
        metrics=BacktestMetrics(),
    )

    assert result.config_hash is None


def test_backtest_result_config_hash_none_serialization() -> None:
    """BacktestResult with None config_hash should serialize/deserialize correctly."""
    result = BacktestResult(
        engine=BacktestEngine.PNL,
        strategy_id="test_strategy",
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
        initial_capital_usd=Decimal("10000"),
        random_seed=None,
    )

    config_with_seed = PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
        initial_capital_usd=Decimal("10000"),
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
        initial_capital_usd=Decimal("10000"),
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
        initial_capital_usd=Decimal("1000000000"),  # 1 billion
        gas_price_gwei=Decimal("1000"),
        inclusion_delay_blocks=100,
    )

    hash_value = config.calculate_config_hash()
    assert len(hash_value) == 64
