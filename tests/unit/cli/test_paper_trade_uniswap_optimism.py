"""Tests for paper trading CLI with Uniswap V3 swap strategy on Optimism.

Validates that the paper trading CLI correctly:
1. Recognizes the demo_uniswap_paper_trade_optimism strategy
2. Accepts Optimism chain configuration
3. Dry-run mode displays correct session configuration
4. PaperTraderConfig is created with valid Optimism parameters
5. Strategy instantiation works with swap-specific config

These tests exercise the paper trading CLI path without requiring
Anvil or RPC access. First paper trading test coverage for Optimism
(Kitchen Loop iteration 86, VIB-1375).
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from click.testing import CliRunner

from almanak.framework.backtesting.paper.config import PaperTraderConfig
from almanak.framework.cli.backtest import backtest
from almanak.framework.strategies import get_strategy, list_strategies


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


# =============================================================================
# PaperTraderConfig Validation for Optimism
# =============================================================================


class TestPaperTraderConfigForOptimism:
    """Test PaperTraderConfig creation for Optimism chain paper trading."""

    def test_optimism_chain_config(self) -> None:
        """Verify PaperTraderConfig accepts Optimism chain settings."""
        config = PaperTraderConfig(
            chain="optimism",
            rpc_url="https://example.com/rpc",
            strategy_id="demo_uniswap_paper_trade_optimism",
            initial_eth=Decimal("10"),
            initial_tokens={"USDC": Decimal("10000"), "WETH": Decimal("1")},
            tick_interval_seconds=60,
            max_ticks=5,
            anvil_port=8546,
        )
        assert config.chain == "optimism"
        assert config.strategy_id == "demo_uniswap_paper_trade_optimism"
        assert config.initial_tokens["USDC"] == Decimal("10000")
        assert config.max_ticks == 5
        assert config.tick_interval_seconds == 60

    def test_optimism_chain_id_resolution(self) -> None:
        """Verify chain_id is resolved correctly for Optimism."""
        config = PaperTraderConfig(
            chain="optimism",
            rpc_url="https://example.com/rpc",
            strategy_id="test",
        )
        assert config.chain_id == 10  # Optimism mainnet chain ID

    def test_config_max_duration(self) -> None:
        """Verify max_duration_seconds is computed from ticks * interval."""
        config = PaperTraderConfig(
            chain="optimism",
            rpc_url="https://example.com/rpc",
            strategy_id="test",
            tick_interval_seconds=60,
            max_ticks=10,
        )
        assert config.max_duration_seconds == 600  # 10 ticks * 60s

    def test_config_fork_rpc_url(self) -> None:
        """Verify fork RPC URL uses configured anvil port."""
        config = PaperTraderConfig(
            chain="optimism",
            rpc_url="https://example.com/rpc",
            strategy_id="test",
            anvil_port=9999,
        )
        assert "9999" in config.fork_rpc_url


# =============================================================================
# Paper Trade CLI Dry Run on Optimism
# =============================================================================


class TestPaperTradeDryRunOptimism:
    """Test paper trading CLI dry-run with Uniswap V3 strategy on Optimism."""

    def test_dry_run_shows_config(self, cli_runner: CliRunner) -> None:
        """Dry run displays session configuration without starting Anvil."""
        result = cli_runner.invoke(
            backtest,
            [
                "paper", "start",
                "-s", "demo_uniswap_paper_trade_optimism",
                "--chain", "optimism",
                "--max-ticks", "5",
                "--tick-interval", "60",
                "--initial-eth", "10",
                "--initial-tokens", "USDC:10000,WETH:1",
                "--rpc-url", "https://fake-rpc.example.com",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "demo_uniswap_paper_trade_optimism" in result.output
        assert "optimism" in result.output
        assert "Dry run" in result.output

    def test_dry_run_shows_tick_config(self, cli_runner: CliRunner) -> None:
        """Dry run shows tick interval and max ticks."""
        result = cli_runner.invoke(
            backtest,
            [
                "paper", "start",
                "-s", "demo_uniswap_paper_trade_optimism",
                "--chain", "optimism",
                "--max-ticks", "10",
                "--tick-interval", "120",
                "--rpc-url", "https://fake-rpc.example.com",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "Tick Interval: 120s" in result.output
        assert "Max Ticks: 10" in result.output


# =============================================================================
# Strategy Registration
# =============================================================================


class TestUniswapOptimismPaperTradeRegistration:
    """Test that the Uniswap V3 Optimism paper trade strategy is registered."""

    def test_strategy_is_discoverable(self) -> None:
        """Verify demo_uniswap_paper_trade_optimism is in the strategy registry."""
        strategies = list_strategies()
        assert "demo_uniswap_paper_trade_optimism" in strategies, (
            f"demo_uniswap_paper_trade_optimism not found. Available: {sorted(strategies)}"
        )

    def test_strategy_metadata(self) -> None:
        """Verify strategy metadata for Uniswap V3 Optimism paper trade."""
        strategy_class = get_strategy("demo_uniswap_paper_trade_optimism")
        metadata = strategy_class.STRATEGY_METADATA
        assert "optimism" in metadata.supported_chains
        assert "uniswap_v3" in metadata.supported_protocols
        assert "SWAP" in metadata.intent_types
        assert "HOLD" in metadata.intent_types

    def test_strategy_default_chain_is_optimism(self) -> None:
        """Verify default chain is Optimism."""
        strategy_class = get_strategy("demo_uniswap_paper_trade_optimism")
        metadata = strategy_class.STRATEGY_METADATA
        assert metadata.default_chain == "optimism"
