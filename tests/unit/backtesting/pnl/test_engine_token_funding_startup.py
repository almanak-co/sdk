from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import normalize_token_key
from almanak.framework.backtesting.pnl.engine import DefaultFeeModel, DefaultSlippageModel, PnLBacktester
from tests.unit.backtesting.pnl._mocks import MockDataProvider

BASE_CBBTC = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
BASE_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


def _token_funding() -> list[dict[str, str]]:
    return [
        {
            "symbol": "cbBTC",
            "address": BASE_CBBTC,
            "chain": "base",
            "amount": "200",
            "amount_type": "usd",
        },
        {
            "symbol": "USDC",
            "address": BASE_USDC,
            "chain": "base",
            "amount": "200",
            "amount_type": "usd",
        },
    ]


def _config(start: datetime) -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(hours=1),
        interval_seconds=3600,
        token_funding=_token_funding(),
        chain="base",
        tokens=["cbBTC", "USDC"],
        include_gas_costs=False,
        inclusion_delay_blocks=0,
        preflight_validation=False,
    )


def _backtester() -> PnLBacktester:
    provider = MockDataProvider(
        base_prices={
            normalize_token_key("base", BASE_CBBTC): Decimal("100000"),
            normalize_token_key("base", BASE_USDC): Decimal("1"),
        }
    )
    return PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
    )


class RecordingHoldStrategy:
    deployment_id = "recording_hold"

    def __init__(self) -> None:
        self.first_cbbtc_balance: Decimal | None = None
        self.first_usdc_balance: Decimal | None = None

    def decide(self, market: Any) -> None:
        if self.first_cbbtc_balance is None:
            self.first_cbbtc_balance = market.balance(BASE_CBBTC).balance
            self.first_usdc_balance = market.balance(BASE_USDC).balance
        return None


@dataclass
class _SwapIntent:
    intent_type: str = "SWAP"
    from_token: str = BASE_CBBTC
    to_token: str = BASE_USDC
    amount_usd: Decimal = Decimal("100")
    protocol: str = "uniswap_v3"


class SellOnceStrategy:
    deployment_id = "sell_once"

    def __init__(self) -> None:
        self._decided = False

    def decide(self, _market: Any) -> _SwapIntent | None:
        if self._decided:
            return None
        self._decided = True
        return _SwapIntent()


@pytest.mark.asyncio
async def test_first_snapshot_exposes_exact_funded_token_addresses() -> None:
    start = datetime(2026, 6, 1)
    strategy = RecordingHoldStrategy()

    result = await _backtester().backtest(strategy, _config(start))

    assert result.initial_portfolio_value_usd == Decimal("400")
    assert strategy.first_cbbtc_balance == Decimal("0.002")
    assert strategy.first_usdc_balance == Decimal("200")
    assert result.final_capital_usd == Decimal("400.000")


@pytest.mark.asyncio
async def test_funded_address_native_token_can_be_sold_without_insufficient_balance() -> None:
    start = datetime(2026, 6, 1)

    result = await _backtester().backtest(SellOnceStrategy(), _config(start))

    assert result.success
    assert result.trades
    assert result.trades[0].success
    assert "insufficient" not in str(result.trades[0].metadata).lower()
