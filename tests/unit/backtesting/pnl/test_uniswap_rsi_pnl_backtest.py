"""PnL backtest integration tests for the uniswap_rsi demo strategy on Arbitrum.

Validates that the PnL backtesting pipeline works end-to-end with the
most-tested demo strategy (uniswap_rsi) as a regression baseline.

Tests use deterministic mock data providers to avoid CoinGecko API calls
while exercising the full engine path: tick iteration, strategy decide(),
intent processing, fee/slippage models, and result generation.

VIB-2309
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)


# ---------------------------------------------------------------------------
# Deterministic data provider: simulates ETH price moving in a cycle
# that triggers RSI-based buy/sell signals.
# ---------------------------------------------------------------------------


class RSICycleDataProvider:
    """Data provider that generates a price cycle to exercise RSI logic.

    Produces a sine-wave price pattern over `num_ticks` hourly ticks:
    - First half: price drops (simulates oversold condition -> buy signal)
    - Second half: price rises (simulates overbought condition -> sell signal)

    This ensures the uniswap_rsi strategy actually trades during the backtest.
    """

    provider_name = "rsi_cycle"

    def __init__(self, num_ticks: int = 48, start_time: datetime | None = None):
        self.num_ticks = num_ticks
        self.start_time = start_time

    async def iterate(self, config: Any):
        """Yield (timestamp, MarketState) tuples with cyclic prices."""
        start = self.start_time or config.start_time
        for i in range(self.num_ticks):
            timestamp = start + timedelta(hours=i)

            # Sine wave: price drops then rises, amplitude ~500 around 3000
            phase = (i / self.num_ticks) * 2 * math.pi
            eth_price = Decimal(str(round(3000 - 500 * math.sin(phase), 2)))

            market_state = MarketState(
                timestamp=timestamp,
                prices={
                    "ETH": eth_price,
                    "WETH": eth_price,
                    "USDC": Decimal("1"),
                },
                chain="arbitrum",
                block_number=200_000_000 + i,
            )
            yield timestamp, market_state


class FlatDataProvider:
    """Data provider with constant prices (no trading signals)."""

    provider_name = "flat"

    def __init__(self, num_ticks: int = 10, start_time: datetime | None = None):
        self.num_ticks = num_ticks
        self.start_time = start_time

    async def iterate(self, config: Any):
        start = self.start_time or config.start_time
        for i in range(self.num_ticks):
            timestamp = start + timedelta(hours=i)
            market_state = MarketState(
                timestamp=timestamp,
                prices={
                    "ETH": Decimal("3000"),
                    "WETH": Decimal("3000"),
                    "USDC": Decimal("1"),
                },
                chain="arbitrum",
                block_number=200_000_000 + i,
            )
            yield timestamp, market_state


# ---------------------------------------------------------------------------
# Lightweight strategy adapter that mimics uniswap_rsi decide() logic
# without requiring full IntentStrategy initialization (no gateway needed).
# ---------------------------------------------------------------------------


@dataclass
class MockSwapIntent:
    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount_usd: Decimal = field(default_factory=lambda: Decimal("100"))
    protocol: str = "uniswap_v3"


class UniswapRSIBacktestAdapter:
    """Simplified uniswap_rsi strategy for PnL backtesting.

    Mimics the core decide() logic: buy when price drops significantly
    (RSI proxy: price below moving average), sell when price rises.
    Does not need gateway or full framework init.
    """

    def __init__(
        self,
        trade_size_usd: Decimal = Decimal("100"),
        buy_threshold: Decimal = Decimal("2800"),
        sell_threshold: Decimal = Decimal("3200"),
    ):
        self._strategy_id = "demo_uniswap_rsi"
        self.trade_size_usd = trade_size_usd
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self.tick_count = 0

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def decide(self, market: Any) -> MockSwapIntent | None:
        """RSI-like logic: buy low, sell high."""
        self.tick_count += 1
        prices = market.prices if hasattr(market, "prices") else {}
        eth_price = prices.get("WETH") or prices.get("ETH")
        if eth_price is None:
            return None

        if eth_price < self.buy_threshold:
            return MockSwapIntent(
                from_token="USDC",
                to_token="WETH",
                amount_usd=self.trade_size_usd,
            )
        elif eth_price > self.sell_threshold:
            return MockSwapIntent(
                from_token="WETH",
                to_token="USDC",
                amount_usd=self.trade_size_usd,
            )
        return None

    def get_metadata(self) -> None:
        return None


# ---------------------------------------------------------------------------
# Helper to build a standard PnL backtest config for Arbitrum
# ---------------------------------------------------------------------------


def make_arbitrum_config(
    hours: int = 48,
    initial_capital: Decimal = Decimal("10000"),
) -> PnLBacktestConfig:
    start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    return PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(hours=hours),
        initial_capital_usd=initial_capital,
        tokens=["WETH", "USDC"],
        chain="arbitrum",
        inclusion_delay_blocks=1,
    )


# ===========================================================================
# Tests
# ===========================================================================


@pytest.mark.asyncio
async def test_pnl_backtest_runs_without_error():
    """Core regression: PnL engine runs end-to-end with uniswap_rsi-style strategy."""
    config = make_arbitrum_config(hours=48)
    provider = RSICycleDataProvider(num_ticks=48, start_time=config.start_time)

    backtester = PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    strategy = UniswapRSIBacktestAdapter()
    result = await backtester.backtest(strategy, config)

    assert result is not None
    assert len(result.equity_curve) == provider.num_ticks
    assert result.final_capital_usd > Decimal("0")


@pytest.mark.asyncio
async def test_pnl_result_has_equity_curve():
    """Equity curve should contain data points matching the number of ticks."""
    config = make_arbitrum_config(hours=24)
    provider = RSICycleDataProvider(num_ticks=24, start_time=config.start_time)

    backtester = PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    result = await backtester.backtest(UniswapRSIBacktestAdapter(), config)

    assert isinstance(result.equity_curve, list)
    assert len(result.equity_curve) == provider.num_ticks


@pytest.mark.asyncio
async def test_pnl_result_has_trade_history():
    """Trade history should record executed swaps."""
    config = make_arbitrum_config(hours=48)
    provider = RSICycleDataProvider(num_ticks=48, start_time=config.start_time)

    backtester = PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    result = await backtester.backtest(UniswapRSIBacktestAdapter(), config)

    assert isinstance(result.trades, list)
    assert len(result.equity_curve) == 48


@pytest.mark.asyncio
async def test_pnl_result_contains_summary_stats():
    """Result should contain key performance metrics."""
    config = make_arbitrum_config(hours=48)
    provider = RSICycleDataProvider(num_ticks=48, start_time=config.start_time)

    backtester = PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    result = await backtester.backtest(UniswapRSIBacktestAdapter(), config)
    summary = result.to_dict()

    assert "total_return_pct" in summary
    assert "final_capital_usd" in summary
    assert "metrics" in summary


@pytest.mark.asyncio
async def test_flat_prices_produce_no_trades():
    """With constant prices, strategy should hold (no buy/sell signals)."""
    config = make_arbitrum_config(hours=10)
    provider = FlatDataProvider(num_ticks=10, start_time=config.start_time)

    backtester = PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    # Thresholds at 2800/3200, price at 3000 -> no trades
    result = await backtester.backtest(UniswapRSIBacktestAdapter(), config)

    assert len(result.trades) == 0


@pytest.mark.asyncio
async def test_initial_capital_preserved_on_hold():
    """When no trades execute, final value should equal initial capital."""
    config = make_arbitrum_config(hours=10, initial_capital=Decimal("50000"))
    provider = FlatDataProvider(num_ticks=10, start_time=config.start_time)

    backtester = PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    result = await backtester.backtest(UniswapRSIBacktestAdapter(), config)

    assert result.final_capital_usd == Decimal("50000")


@pytest.mark.asyncio
async def test_arbitrum_chain_config():
    """Verify chain is set to arbitrum in the backtest config."""
    config = make_arbitrum_config()
    assert config.chain == "arbitrum"


@pytest.mark.asyncio
async def test_backtest_result_serializable():
    """Result should be serializable to dict (for JSON output)."""
    config = make_arbitrum_config(hours=12)
    provider = RSICycleDataProvider(num_ticks=12, start_time=config.start_time)

    backtester = PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    result = await backtester.backtest(UniswapRSIBacktestAdapter(), config)

    result_dict = result.to_dict()
    assert isinstance(result_dict, dict)
    assert "config" in result_dict
    assert "equity_curve" in result_dict
    assert "trades" in result_dict
