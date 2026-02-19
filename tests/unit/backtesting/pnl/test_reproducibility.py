"""Tests for backtest reproducibility with strict mode.

Tests verify that with strict_reproducibility=True and identical config+seed:
1. Running the same backtest twice produces byte-identical results
2. Config hash remains consistent across runs
3. All timestamps come from simulation, not wall clock
"""

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.models import BacktestResult
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)


class DeterministicDataProvider:
    """Data provider that returns deterministic market states.

    Uses fixed prices and timestamps derived from config, not wall clock.
    This ensures reproducibility across test runs.
    """

    provider_name = "deterministic"

    def __init__(self, num_ticks: int = 5, start_time: datetime | None = None):
        self.num_ticks = num_ticks
        self.start_time = start_time

    async def iterate(self, config: Any):
        """Yield deterministic (timestamp, market_state) tuples.

        Prices are calculated from tick index, not fetched from external sources.
        """
        start = self.start_time or config.start_time
        for i in range(self.num_ticks):
            timestamp = start + timedelta(hours=i)
            # Deterministic prices based on tick index
            eth_price = Decimal("3000") + Decimal(i * 50)
            market_state = MarketState(
                timestamp=timestamp,
                prices={
                    "ETH": eth_price,
                    "WETH": eth_price,
                    "USDC": Decimal("1"),
                },
                chain="arbitrum",
                block_number=1000 + i,
            )
            yield timestamp, market_state


@dataclass
class MockSwapIntent:
    """Deterministic mock swap intent for testing."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "ETH"
    amount: Decimal = field(default_factory=lambda: Decimal("100"))
    protocol: str = "uniswap_v3"


class DeterministicStrategy:
    """Strategy that returns deterministic intents based on tick count.

    Always returns the same intent for a given tick number, ensuring
    reproducibility across runs.
    """

    def __init__(self, strategy_id: str = "test_reproducibility"):
        self._strategy_id = strategy_id
        self.decide_call_count = 0

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def decide(self, market: Any) -> MockSwapIntent | None:
        """Return a swap intent on odd ticks, None on even ticks."""
        self.decide_call_count += 1
        # Deterministic behavior: swap on odd ticks
        if self.decide_call_count % 2 == 1:
            return MockSwapIntent(
                from_token="USDC",
                to_token="ETH",
                amount=Decimal("100"),
            )
        return None

    def get_metadata(self) -> None:
        return None


def normalize_result_for_comparison(result: BacktestResult) -> dict[str, Any]:
    """Normalize a BacktestResult for deterministic comparison.

    Removes fields that may vary between runs (wall clock times, IDs)
    while preserving all simulation-derived values that should be identical.
    """
    result_dict = result.to_dict()

    # Remove wall-clock derived fields that vary between runs
    fields_to_remove = [
        "run_started_at",
        "run_ended_at",
        "run_duration_seconds",
        "backtest_id",
        "phase_timings",
        "preflight_report",  # Contains validation_time_seconds which varies
    ]
    for field_name in fields_to_remove:
        result_dict.pop(field_name, None)

    # Remove metadata that contains wall clock times
    if "_metadata" in result_dict.get("config", {}):
        del result_dict["config"]["_metadata"]

    return result_dict


async def run_backtest_with_config(config: PnLBacktestConfig, num_ticks: int = 5) -> BacktestResult:
    """Run a backtest with the given config and return the result."""
    data_provider = DeterministicDataProvider(
        num_ticks=num_ticks,
        start_time=config.start_time,
    )

    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    strategy = DeterministicStrategy()
    return await backtester.backtest(strategy, config)


# =============================================================================
# Tests: Reproducibility with Strict Mode
# =============================================================================


@pytest.mark.asyncio
async def test_identical_config_produces_identical_results():
    """Test that running the same backtest twice produces identical results.

    This is the core reproducibility test. With strict_reproducibility=True
    and identical configuration, two runs must produce byte-identical results
    (excluding wall-clock fields like run_started_at).
    """
    # Fixed start time for reproducibility
    start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=5)

    # Create identical configs with strict_reproducibility=True
    config1 = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        inclusion_delay_blocks=1,
        random_seed=42,
        strict_reproducibility=True,
    )

    config2 = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        inclusion_delay_blocks=1,
        random_seed=42,
        strict_reproducibility=True,
    )

    # Run both backtests
    result1 = await run_backtest_with_config(config1, num_ticks=5)
    result2 = await run_backtest_with_config(config2, num_ticks=5)

    # Verify both completed successfully
    assert result1.error is None, f"First backtest failed: {result1.error}"
    assert result2.error is None, f"Second backtest failed: {result2.error}"

    # Normalize results for comparison
    normalized1 = normalize_result_for_comparison(result1)
    normalized2 = normalize_result_for_comparison(result2)

    # Serialize to JSON for byte-level comparison
    json1 = json.dumps(normalized1, sort_keys=True, default=str)
    json2 = json.dumps(normalized2, sort_keys=True, default=str)

    assert json1 == json2, (
        "Backtests with identical config did not produce identical results.\n"
        f"Config hash 1: {result1.config_hash}\n"
        f"Config hash 2: {result2.config_hash}\n"
        "This indicates non-deterministic behavior in the backtest engine."
    )


@pytest.mark.asyncio
async def test_config_hash_consistency():
    """Test that config hash is consistent for identical configurations."""
    start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=5)

    config1 = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        random_seed=42,
        strict_reproducibility=True,
    )

    config2 = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        random_seed=42,
        strict_reproducibility=True,
    )

    # Hashes should be identical
    hash1 = config1.calculate_config_hash()
    hash2 = config2.calculate_config_hash()

    assert hash1 == hash2, f"Config hashes differ: {hash1} != {hash2}"


@pytest.mark.asyncio
async def test_different_seed_produces_different_hash():
    """Test that different random seeds produce different config hashes."""
    start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=5)

    config1 = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        random_seed=42,
        strict_reproducibility=True,
    )

    config2 = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        random_seed=123,  # Different seed
        strict_reproducibility=True,
    )

    hash1 = config1.calculate_config_hash()
    hash2 = config2.calculate_config_hash()

    assert hash1 != hash2, "Different random seeds should produce different config hashes"


@pytest.mark.asyncio
async def test_trade_records_deterministic():
    """Test that trade records are identical across reproducible runs."""
    start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=5)

    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        inclusion_delay_blocks=1,
        random_seed=42,
        strict_reproducibility=True,
    )

    # Run twice
    result1 = await run_backtest_with_config(config, num_ticks=5)
    result2 = await run_backtest_with_config(config, num_ticks=5)

    assert result1.error is None, f"First backtest failed: {result1.error}"
    assert result2.error is None, f"Second backtest failed: {result2.error}"

    # Same number of trades
    assert len(result1.trades) == len(result2.trades), (
        f"Trade count differs: {len(result1.trades)} vs {len(result2.trades)}"
    )

    # Compare each trade
    for i, (trade1, trade2) in enumerate(zip(result1.trades, result2.trades, strict=True)):
        trade1_dict = trade1.to_dict()
        trade2_dict = trade2.to_dict()

        # Remove trade_id which may vary
        trade1_dict.pop("trade_id", None)
        trade2_dict.pop("trade_id", None)

        assert trade1_dict == trade2_dict, (
            f"Trade {i} differs between runs:\nRun 1: {trade1_dict}\nRun 2: {trade2_dict}"
        )


@pytest.mark.asyncio
async def test_metrics_deterministic():
    """Test that calculated metrics are identical across reproducible runs."""
    start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=5)

    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        random_seed=42,
        strict_reproducibility=True,
    )

    result1 = await run_backtest_with_config(config, num_ticks=5)
    result2 = await run_backtest_with_config(config, num_ticks=5)

    assert result1.error is None, f"First backtest failed: {result1.error}"
    assert result2.error is None, f"Second backtest failed: {result2.error}"

    # Compare metrics
    metrics1 = result1.metrics.to_dict()
    metrics2 = result2.metrics.to_dict()

    assert metrics1 == metrics2, f"Metrics differ between runs:\nRun 1: {metrics1}\nRun 2: {metrics2}"


@pytest.mark.asyncio
async def test_equity_curve_deterministic():
    """Test that equity curve is identical across reproducible runs."""
    start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=5)

    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        random_seed=42,
        strict_reproducibility=True,
    )

    result1 = await run_backtest_with_config(config, num_ticks=5)
    result2 = await run_backtest_with_config(config, num_ticks=5)

    assert result1.error is None, f"First backtest failed: {result1.error}"
    assert result2.error is None, f"Second backtest failed: {result2.error}"

    # Same number of equity points
    assert len(result1.equity_curve) == len(result2.equity_curve), (
        f"Equity curve length differs: {len(result1.equity_curve)} vs {len(result2.equity_curve)}"
    )

    # Compare each point
    for i, (point1, point2) in enumerate(zip(result1.equity_curve, result2.equity_curve, strict=True)):
        point1_dict = point1.to_dict()
        point2_dict = point2.to_dict()

        assert point1_dict == point2_dict, (
            f"Equity point {i} differs between runs:\nRun 1: {point1_dict}\nRun 2: {point2_dict}"
        )


@pytest.mark.asyncio
async def test_final_capital_deterministic():
    """Test that final capital is identical across reproducible runs."""
    start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=5)

    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        random_seed=42,
        strict_reproducibility=True,
    )

    result1 = await run_backtest_with_config(config, num_ticks=5)
    result2 = await run_backtest_with_config(config, num_ticks=5)

    assert result1.error is None, f"First backtest failed: {result1.error}"
    assert result2.error is None, f"Second backtest failed: {result2.error}"

    assert result1.final_capital_usd == result2.final_capital_usd, (
        f"Final capital differs: {result1.final_capital_usd} vs {result2.final_capital_usd}"
    )
