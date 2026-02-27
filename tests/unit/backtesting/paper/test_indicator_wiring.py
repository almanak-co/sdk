"""Tests for paper trader indicator wiring (VIB-198).

Verifies that create_market_snapshot_from_fork passes the RSI calculator
to the MarketSnapshot so strategies can call market.rsi(), market.macd(),
market.bollinger_bands(), and market.atr().
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.paper.engine import (
    PaperTrader,
    create_market_snapshot_from_fork,
)


# ---------------------------------------------------------------------------
# create_market_snapshot_from_fork tests
# ---------------------------------------------------------------------------


class TestCreateMarketSnapshotFromForkIndicators:
    """Verify rsi_calculator is passed through to MarketSnapshot."""

    @pytest.fixture()
    def mock_fork_manager(self):
        fm = MagicMock()
        fm.is_running = True
        fm.current_block = 12345
        fm.get_rpc_url.return_value = "http://localhost:8545"
        return fm

    @pytest.mark.asyncio()
    async def test_rsi_calculator_passed_to_snapshot(self, mock_fork_manager):
        """When rsi_calculator is provided, the snapshot should have it configured."""
        mock_rsi_calc = MagicMock()

        snapshot = await create_market_snapshot_from_fork(
            fork_manager=mock_fork_manager,
            chain="arbitrum",
            wallet_address="0x1234",
            rsi_calculator=mock_rsi_calc,
        )

        assert snapshot._rsi_calculator is mock_rsi_calc

    @pytest.mark.asyncio()
    async def test_no_rsi_calculator_leaves_none(self, mock_fork_manager):
        """When rsi_calculator is not provided, it should be None."""
        snapshot = await create_market_snapshot_from_fork(
            fork_manager=mock_fork_manager,
            chain="arbitrum",
            wallet_address="0x1234",
        )

        assert snapshot._rsi_calculator is None

    @pytest.mark.asyncio()
    async def test_rsi_raises_without_calculator(self, mock_fork_manager):
        """Without calculator, calling rsi() should raise ValueError."""
        snapshot = await create_market_snapshot_from_fork(
            fork_manager=mock_fork_manager,
            chain="arbitrum",
            wallet_address="0x1234",
        )

        with pytest.raises(ValueError, match="No RSI calculator configured"):
            snapshot.rsi("WETH")


# ---------------------------------------------------------------------------
# PaperTrader._init_indicator_calculators tests
# ---------------------------------------------------------------------------


class TestPaperTraderIndicatorInit:
    """Verify PaperTrader initializes indicator calculators."""

    @pytest.fixture()
    def _mock_deps(self):
        """Patch PaperTrader dependencies to avoid real network calls."""
        with (
            patch(
                "almanak.framework.backtesting.paper.engine.PaperTrader._init_price_provider"
            ),
            patch(
                "almanak.framework.backtesting.paper.engine.PaperTrader._init_indicator_calculators"
            ) as mock_init_ind,
        ):
            yield mock_init_ind

    def test_init_calls_indicator_setup(self, _mock_deps):
        """PaperTrader.__post_init__ should call _init_indicator_calculators."""
        mock_init_ind = _mock_deps

        fm = MagicMock()
        pt = MagicMock()
        config = MagicMock()
        config.tick_interval_seconds = 60

        PaperTrader(fork_manager=fm, portfolio_tracker=pt, config=config)

        mock_init_ind.assert_called_once()

    def test_init_indicator_calculators_creates_rsi(self):
        """_init_indicator_calculators should create an RSICalculator with BinanceOHLCVProvider."""
        with (
            patch(
                "almanak.framework.backtesting.paper.engine.PaperTrader._init_price_provider"
            ),
        ):
            fm = MagicMock()
            pt = MagicMock()
            config = MagicMock()
            config.tick_interval_seconds = 60

            trader = PaperTrader(fork_manager=fm, portfolio_tracker=pt, config=config)

            # RSI calculator should be initialized
            assert trader._rsi_calculator is not None

    def test_init_indicator_calculators_graceful_failure(self):
        """If indicator init fails, _rsi_calculator should be None (not crash)."""
        with (
            patch(
                "almanak.framework.backtesting.paper.engine.PaperTrader._init_price_provider"
            ),
            patch(
                "almanak.framework.data.indicators.rsi.RSICalculator",
                side_effect=ImportError("test failure"),
            ),
        ):
            fm = MagicMock()
            pt = MagicMock()
            config = MagicMock()
            config.tick_interval_seconds = 60

            trader = PaperTrader(fork_manager=fm, portfolio_tracker=pt, config=config)

            # Should gracefully degrade, not crash
            assert trader._rsi_calculator is None


# ---------------------------------------------------------------------------
# Integration: _execute_tick passes rsi_calculator
# ---------------------------------------------------------------------------


class TestExecuteTickIndicators:
    """Verify _execute_tick passes rsi_calculator to create_market_snapshot_from_fork."""

    @pytest.mark.asyncio()
    async def test_execute_tick_passes_rsi_calculator(self):
        """The snapshot created during _execute_tick should have rsi_calculator set."""
        with (
            patch(
                "almanak.framework.backtesting.paper.engine.PaperTrader._init_price_provider"
            ),
        ):
            fm = MagicMock()
            fm.is_running = True
            pt = MagicMock()
            pt.current_balances = {}
            config = MagicMock()
            config.tick_interval_seconds = 60
            config.chain = "arbitrum"

            trader = PaperTrader(fork_manager=fm, portfolio_tracker=pt, config=config)

            # Mock orchestrator
            mock_orch = MagicMock()
            mock_orch.signer.address = "0xtest"
            trader._orchestrator = mock_orch
            trader._tick_count = 0

            # Mock _get_portfolio_prices
            trader._get_portfolio_prices = AsyncMock(return_value={})

            # Capture what create_market_snapshot_from_fork receives
            with patch(
                "almanak.framework.backtesting.paper.engine.create_market_snapshot_from_fork",
                new_callable=AsyncMock,
            ) as mock_create:
                mock_snapshot = MagicMock()
                mock_create.return_value = mock_snapshot

                # Mock strategy
                mock_strategy = MagicMock()
                mock_strategy.decide.return_value = None  # HOLD

                await trader._execute_tick(mock_strategy)

                # Verify rsi_calculator was passed
                mock_create.assert_called_once()
                call_kwargs = mock_create.call_args[1]
                assert "rsi_calculator" in call_kwargs
                assert call_kwargs["rsi_calculator"] is trader._rsi_calculator
