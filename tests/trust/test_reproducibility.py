#!/usr/bin/env python3
"""
Phase 4: Reproducibility Certification

This file contains tests that verify results are deterministic and auditable.
These tests ensure that the same configuration produces identical results.

Usage:
    python -c "exec(open('tests/trust/test_reproducibility.py').read())"

Tests:
    4.1 Bit-for-Bit Reproducibility: Same config + same seed = identical results
    4.2 Config Hash Verification: Identical configs produce identical hashes
"""

# Add project root to path (works with exec and direct execution)
import os
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

try:
    # Try to get the file path (works when run directly)
    current_file = os.path.abspath(__file__)
except NameError:
    # When run via exec(), assume we're in the project root
    current_file = os.path.join(os.getcwd(), "tests/trust/test_reproducibility.py")

project_root = Path(current_file).parent.parent.parent
sys.path.insert(0, str(project_root))

from almanak import HoldIntent, IntentStrategy, MarketSnapshot
from almanak.framework.backtesting.pnl import PnLBacktestConfig, PnLBacktester
from almanak.framework.backtesting.pnl.providers import CoinGeckoDataProvider
from almanak.framework.models.hot_reload_config import HotReloadableConfig

DUMMY_WALLET = "0x" + "0" * 40


class SimpleStrategy(IntentStrategy):
    """Simple strategy for reproducibility testing."""

    @property
    def strategy_id(self) -> str:
        return "reproducibility-test-strategy"

    def decide(self, market: MarketSnapshot):  # noqa: ARG002
        return HoldIntent(reason="Reproducibility test: hold only")

    def get_open_positions(self):
        from almanak.framework.teardown.models import TeardownPositionSummary
        return TeardownPositionSummary.empty(self.strategy_id)

    def generate_teardown_intents(self, mode=None, market=None):
        return []


@pytest.mark.asyncio
async def test_bit_for_bit_reproducibility():
    """Test 4.1: Bit-for-Bit Reproducibility

    Same config + same seed = identical results.

    This catches: non-deterministic code paths, time-based randomness, cache issues
    """
    print("Testing 4.1: Bit-for-Bit Reproducibility...")

    # Run twice with same seed
    config_base = {
        "start_time": datetime(2024, 1, 1, tzinfo=UTC),
        "end_time": datetime(2024, 1, 7, tzinfo=UTC),  # Short period for quick test
        "initial_capital_usd": Decimal("10000"),
        "tokens": ["ETH", "USDC"],
        "random_seed": 42,  # Fixed seed
        "strict_reproducibility": True,
    }

    config1 = PnLBacktestConfig(**config_base)
    config2 = PnLBacktestConfig(**config_base)

    backtester = PnLBacktester(
        data_provider=CoinGeckoDataProvider(),
        fee_models={},
        slippage_models={},
    )

    strategy = SimpleStrategy(config=HotReloadableConfig(), chain="ethereum", wallet_address=DUMMY_WALLET)

    # Run first backtest
    result1 = await backtester.backtest(strategy, config1)

    # Run second backtest with identical config
    result2 = await backtester.backtest(strategy, config2)

    # Compare all key metrics
    assert result1.metrics.total_pnl_usd == result2.metrics.total_pnl_usd, (
        f"total_pnl_usd mismatch: {result1.metrics.total_pnl_usd} vs {result2.metrics.total_pnl_usd}"
    )
    assert result1.metrics.net_pnl_usd == result2.metrics.net_pnl_usd, (
        f"net_pnl_usd mismatch: {result1.metrics.net_pnl_usd} vs {result2.metrics.net_pnl_usd}"
    )
    assert result1.metrics.total_return_pct == result2.metrics.total_return_pct, (
        f"total_return_pct mismatch: {result1.metrics.total_return_pct} vs {result2.metrics.total_return_pct}"
    )
    assert result1.metrics.max_drawdown_pct == result2.metrics.max_drawdown_pct, (
        f"max_drawdown_pct mismatch: {result1.metrics.max_drawdown_pct} vs {result2.metrics.max_drawdown_pct}"
    )
    assert result1.metrics.sharpe_ratio == result2.metrics.sharpe_ratio, (
        f"sharpe_ratio mismatch: {result1.metrics.sharpe_ratio} vs {result2.metrics.sharpe_ratio}"
    )
    assert result1.metrics.total_trades == result2.metrics.total_trades, (
        f"total_trades mismatch: {result1.metrics.total_trades} vs {result2.metrics.total_trades}"
    )

    print("PASS: Results are bit-for-bit reproducible")
    print(f"   Total PnL: ${result1.metrics.total_pnl_usd}")
    print(f"   Max Drawdown: {result1.metrics.max_drawdown_pct:.2%}")
    print(f"   Total Trades: {result1.metrics.total_trades}")


def test_config_hash():
    """Test 4.2: Config Hash Verification

    Identical configs produce identical hashes.
    Different configs produce different hashes.

    This catches: hash collisions, incomplete serialization
    """
    print("Testing 4.2: Config Hash Verification...")

    # Create identical configs
    config1 = PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 31, tzinfo=UTC),
        initial_capital_usd=Decimal("10000"),
        tokens=["ETH", "USDC"],
        random_seed=42,
    )

    config2 = PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 31, tzinfo=UTC),
        initial_capital_usd=Decimal("10000"),
        tokens=["ETH", "USDC"],
        random_seed=42,
    )

    # Create different config (different seed)
    config3 = PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 31, tzinfo=UTC),
        initial_capital_usd=Decimal("10000"),
        tokens=["ETH", "USDC"],
        random_seed=43,  # Different seed
    )

    # Use calculate_config_hash() for deterministic SHA-256 verification
    hash1 = config1.calculate_config_hash()
    hash2 = config2.calculate_config_hash()
    hash3 = config3.calculate_config_hash()

    assert hash1 == hash2, f"Identical configs produced different hashes: {hash1} vs {hash2}"
    assert hash1 != hash3, f"Different configs produced same hash: {hash1}"

    print("PASS: Config hash verification successful")
    print(f"   Identical configs hash: {hash1[:16]}...")
    print(f"   Different config hash:  {hash3[:16]}...")


async def run_phase_4_tests():
    """Run all Phase 4 reproducibility tests."""
    print("=" * 60)
    print("PHASE 4: Reproducibility Certification")
    print("=" * 60)

    await test_bit_for_bit_reproducibility()
    test_config_hash()

    print("\n" + "=" * 60)
    print("PHASE 4 RESULTS: ALL REPRODUCIBILITY TESTS PASSED")


if __name__ == "__main__":
    import asyncio

    asyncio.run(run_phase_4_tests())
