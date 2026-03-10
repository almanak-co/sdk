"""Tests for paper trading CLI with Aerodrome LP strategy on Base.

Validates that the paper trading CLI correctly:
1. Recognizes the demo_aerodrome_paper_trade strategy
2. Accepts Base chain configuration
3. Dry-run mode displays correct session configuration
4. PaperTraderConfig is created with valid parameters
5. Strategy instantiation works with LP-specific config

These tests exercise the paper trading CLI path without requiring
Anvil or RPC access. This is the first test coverage for paper
trading with LP strategies (Kitchen Loop iteration 52, VIB-578).
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
# PaperTraderConfig Validation
# =============================================================================


class TestPaperTraderConfigForBase:
    """Test PaperTraderConfig creation for Base chain paper trading."""

    def test_base_chain_config(self) -> None:
        """Verify PaperTraderConfig accepts Base chain settings."""
        config = PaperTraderConfig(
            chain="base",
            rpc_url="https://example.com/rpc",
            strategy_id="demo_aerodrome_paper_trade",
            initial_eth=Decimal("10"),
            initial_tokens={"USDC": Decimal("10000"), "WETH": Decimal("1")},
            tick_interval_seconds=60,
            max_ticks=5,
            anvil_port=8546,
        )
        assert config.chain == "base"
        assert config.strategy_id == "demo_aerodrome_paper_trade"
        assert config.initial_tokens["USDC"] == Decimal("10000")
        assert config.max_ticks == 5
        assert config.tick_interval_seconds == 60

    def test_config_chain_id_resolution(self) -> None:
        """Verify chain_id is resolved correctly for Base."""
        config = PaperTraderConfig(
            chain="base",
            rpc_url="https://example.com/rpc",
            strategy_id="test",
        )
        assert config.chain_id == 8453  # Base mainnet chain ID

    def test_config_max_duration(self) -> None:
        """Verify max_duration_seconds is computed from ticks * interval."""
        config = PaperTraderConfig(
            chain="base",
            rpc_url="https://example.com/rpc",
            strategy_id="test",
            tick_interval_seconds=60,
            max_ticks=10,
        )
        assert config.max_duration_seconds == 600  # 10 ticks * 60s

    def test_config_unlimited_ticks(self) -> None:
        """Verify unlimited ticks yields None max_duration."""
        config = PaperTraderConfig(
            chain="base",
            rpc_url="https://example.com/rpc",
            strategy_id="test",
            max_ticks=None,
        )
        assert config.max_duration_seconds is None


# =============================================================================
# Paper Trade CLI Dry Run
# =============================================================================


class TestPaperTradeDryRun:
    """Test paper trading CLI dry-run with Aerodrome strategy."""

    def test_dry_run_shows_config(self, cli_runner: CliRunner) -> None:
        """Dry run displays session configuration without starting Anvil."""
        result = cli_runner.invoke(
            backtest,
            [
                "paper", "start",
                "-s", "demo_aerodrome_paper_trade",
                "--chain", "base",
                "--max-ticks", "5",
                "--tick-interval", "60",
                "--initial-eth", "10",
                "--initial-tokens", "USDC:10000,WETH:1",
                "--rpc-url", "https://fake-rpc.example.com",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "demo_aerodrome_paper_trade" in result.output
        assert "base" in result.output
        assert "Dry run" in result.output

    def test_dry_run_shows_tick_config(self, cli_runner: CliRunner) -> None:
        """Dry run shows tick interval and max ticks."""
        result = cli_runner.invoke(
            backtest,
            [
                "paper", "start",
                "-s", "demo_aerodrome_paper_trade",
                "--chain", "base",
                "--max-ticks", "10",
                "--tick-interval", "120",
                "--rpc-url", "https://fake-rpc.example.com",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "Tick Interval: 120s" in result.output
        assert "Max Ticks: 10" in result.output

    def test_dry_run_foreground_mode(self, cli_runner: CliRunner) -> None:
        """Dry run shows foreground mode when flag is set."""
        result = cli_runner.invoke(
            backtest,
            [
                "paper", "start",
                "-s", "demo_aerodrome_paper_trade",
                "--chain", "base",
                "--max-ticks", "3",
                "--rpc-url", "https://fake-rpc.example.com",
                "--foreground",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "Foreground" in result.output


# =============================================================================
# Strategy Registration
# =============================================================================


class TestAerodromePaperTradeRegistration:
    """Test that the Aerodrome paper trade strategy is registered."""

    def test_strategy_is_discoverable(self) -> None:
        """Verify demo_aerodrome_paper_trade is in the strategy registry."""
        strategies = list_strategies()
        assert "demo_aerodrome_paper_trade" in strategies, (
            f"demo_aerodrome_paper_trade not found. Available: {sorted(strategies)}"
        )

    def test_strategy_metadata(self) -> None:
        """Verify strategy metadata for Aerodrome paper trade."""
        strategy_class = get_strategy("demo_aerodrome_paper_trade")
        metadata = strategy_class.STRATEGY_METADATA
        assert "base" in metadata.supported_chains
        assert "aerodrome" in metadata.supported_protocols
        assert "LP_OPEN" in metadata.intent_types
        assert "LP_CLOSE" in metadata.intent_types


# =============================================================================
# Paper Trade Subcommands
# =============================================================================


class TestPaperSubcommands:
    """Test paper trading subcommands exist and are accessible."""

    def test_paper_start_help(self, cli_runner: CliRunner) -> None:
        """paper start --help shows all expected options."""
        result = cli_runner.invoke(backtest, ["paper", "start", "--help"])
        assert result.exit_code == 0
        assert "--strategy" in result.output
        assert "--chain" in result.output
        assert "--max-ticks" in result.output
        assert "--tick-interval" in result.output
        assert "--foreground" in result.output
        assert "--dry-run" in result.output

    def test_paper_status_help(self, cli_runner: CliRunner) -> None:
        """paper status --help shows expected options."""
        result = cli_runner.invoke(backtest, ["paper", "status", "--help"])
        assert result.exit_code == 0
        assert "--strategy" in result.output
        assert "--all" in result.output

    def test_paper_stop_help(self, cli_runner: CliRunner) -> None:
        """paper stop --help shows expected options."""
        result = cli_runner.invoke(backtest, ["paper", "stop", "--help"])
        assert result.exit_code == 0
        assert "--strategy" in result.output
        assert "--force" in result.output

    def test_paper_logs_help(self, cli_runner: CliRunner) -> None:
        """paper logs --help shows expected options."""
        result = cli_runner.invoke(backtest, ["paper", "logs", "--help"])
        assert result.exit_code == 0
        assert "--strategy" in result.output
        assert "--follow" in result.output
        assert "--lines" in result.output
