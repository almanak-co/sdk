"""Tests for PnL backtesting CLI with TraderJoe V2 LP strategy on Avalanche.

Validates that the PnL backtesting CLI correctly:
1. Recognizes the demo_traderjoe_pnl_lp strategy
2. PnLBacktestConfig accepts Avalanche chain settings
3. Dry-run mode displays correct Avalanche configuration
4. Strategy metadata includes correct chain and protocol

First PnL backtest test coverage for Avalanche chain and TraderJoe V2 LP
(Kitchen Loop iteration 86, VIB-1374).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from click.testing import CliRunner

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.cli.backtest import backtest
from almanak.framework.strategies import get_strategy, list_strategies


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


# =============================================================================
# PnLBacktestConfig Validation for Avalanche
# =============================================================================


class TestPnLBacktestConfigForAvalanche:
    """Test PnLBacktestConfig creation for Avalanche chain."""

    def test_avalanche_chain_config(self) -> None:
        """Verify PnLBacktestConfig accepts Avalanche chain settings."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 6, 1, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            chain="avalanche",
            tokens=["WAVAX", "USDC"],
        )
        assert config.chain == "avalanche"
        assert config.initial_capital_usd == Decimal("10000")
        assert "WAVAX" in config.tokens
        assert "USDC" in config.tokens

    def test_avalanche_default_interval(self) -> None:
        """Verify default interval is 3600 seconds (1 hour)."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 6, 1, tzinfo=UTC),
            chain="avalanche",
        )
        assert config.interval_seconds == 3600

    def test_config_with_traderjoe_fee_model(self) -> None:
        """Verify config accepts traderjoe fee model."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 6, 1, tzinfo=UTC),
            chain="avalanche",
            fee_model="traderjoe",
        )
        assert config.fee_model == "traderjoe"


# =============================================================================
# PnL Backtest CLI Dry Run on Avalanche
# =============================================================================


class TestPnLBacktestDryRunAvalanche:
    """Test PnL backtest CLI dry-run with TraderJoe V2 strategy on Avalanche."""

    def test_dry_run_shows_config(self, cli_runner: CliRunner) -> None:
        """Dry run displays backtest configuration without running."""
        result = cli_runner.invoke(
            backtest,
            [
                "pnl",
                "-s", "demo_traderjoe_pnl_lp",
                "--chain", "avalanche",
                "--start", "2024-01-01",
                "--end", "2024-06-01",
                "--tokens", "WAVAX,USDC",
                "--initial-capital", "10000",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "demo_traderjoe_pnl_lp" in result.output
        assert "avalanche" in result.output

    def test_dry_run_with_gas_price(self, cli_runner: CliRunner) -> None:
        """Dry run accepts gas price override for Avalanche."""
        result = cli_runner.invoke(
            backtest,
            [
                "pnl",
                "-s", "demo_traderjoe_pnl_lp",
                "--chain", "avalanche",
                "--start", "2024-01-01",
                "--end", "2024-03-01",
                "--gas-price", "25",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"


# =============================================================================
# Strategy Registration
# =============================================================================


class TestTraderJoePnLRegistration:
    """Test that the TraderJoe V2 PnL LP strategy is registered."""

    def test_strategy_is_discoverable(self) -> None:
        """Verify demo_traderjoe_pnl_lp is in the strategy registry."""
        strategies = list_strategies()
        assert "demo_traderjoe_pnl_lp" in strategies, (
            f"demo_traderjoe_pnl_lp not found. Available: {sorted(strategies)}"
        )

    def test_strategy_metadata(self) -> None:
        """Verify strategy metadata for TraderJoe V2 PnL LP."""
        strategy_class = get_strategy("demo_traderjoe_pnl_lp")
        metadata = strategy_class.STRATEGY_METADATA
        assert "avalanche" in metadata.supported_chains
        assert "traderjoe_v2" in metadata.supported_protocols
        assert "LP_OPEN" in metadata.intent_types
        assert "LP_CLOSE" in metadata.intent_types
        assert metadata.default_chain == "avalanche"
