"""Lending/vault intents must not be charged the default swap fee.

Campaign-50 s31/s32: the generic execution path charged DefaultFeeModel's
0.3% swap fee on ALL non-HOLD intents, so a SUPPLY+WITHDRAW round trip lost
0.6% to phantom fees and a profitable Aave supply-hold read as a loss.

Pins:
- _ZERO_FEE_INTENTS mirrors _ZERO_SLIPPAGE_INTENTS (the engine's non-swap set)
- DefaultFeeModel charges 0 for lending/vault intents, still charges swaps
  (and other market-trade intents — no over-widening)
- Connector fee models (fee_models ABC convention, leading ``trade_amount``)
  are invocable through the engine's call convention without TypeError
- Engine-level: a SUPPLY fill through the generic path records fee_usd == 0
"""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.connectors.aave_v3.fee_model import AaveV3FeeModel
from almanak.connectors.compound_v3.fee_model import CompoundV3FeeModel
from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    _ZERO_FEE_INTENTS,
    _ZERO_SLIPPAGE_INTENTS,
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
    _fee_model_takes_trade_amount,
    _invoke_fee_model,
)
from almanak.framework.intents.vocabulary import Intent
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding
from tests.unit.backtesting.pnl._mocks import MockDataProvider

# Intents that are NOT market trades: no swap-style protocol fee applies.
NON_SWAP_INTENTS = frozenset(
    {
        IntentType.HOLD,
        IntentType.SUPPLY,
        IntentType.WITHDRAW,
        IntentType.REPAY,
        IntentType.BORROW,
        IntentType.VAULT_DEPOSIT,
        IntentType.VAULT_REDEEM,
    }
)

# Intents that remain fee-charged on the generic default path (no over-widening).
CHARGED_INTENTS = frozenset(
    {
        IntentType.SWAP,
        IntentType.LP_OPEN,
        IntentType.LP_CLOSE,
        IntentType.PERP_OPEN,
        IntentType.PERP_CLOSE,
        IntentType.BRIDGE,
        IntentType.UNKNOWN,
    }
)


@pytest.fixture
def market_state() -> MarketState:
    return MarketState(
        timestamp=datetime(2024, 1, 1),
        prices={"USDC": Decimal("1"), "WETH": Decimal("3000")},
    )


class TestZeroFeeIntentSet:
    """The zero-fee set must mirror the zero-slippage (non-swap) set exactly."""

    def test_mirrors_zero_slippage_intents(self) -> None:
        assert _ZERO_FEE_INTENTS == _ZERO_SLIPPAGE_INTENTS

    def test_contains_all_lending_and_vault_intents(self) -> None:
        assert NON_SWAP_INTENTS <= _ZERO_FEE_INTENTS

    def test_does_not_over_widen(self) -> None:
        assert _ZERO_FEE_INTENTS & CHARGED_INTENTS == frozenset()


class TestDefaultFeeModelLendingVault:
    """DefaultFeeModel: zero fee on lending/vault, swap fee unchanged."""

    @pytest.mark.parametrize("intent_type", sorted(NON_SWAP_INTENTS))
    def test_non_swap_intents_zero_fee(self, intent_type: IntentType, market_state: MarketState) -> None:
        model = DefaultFeeModel(fee_pct=Decimal("0.003"))
        fee = model.calculate_fee(
            intent_type=intent_type,
            amount_usd=Decimal("10000"),
            market_state=market_state,
        )
        assert fee == Decimal("0")

    @pytest.mark.parametrize("intent_type", sorted(CHARGED_INTENTS))
    def test_market_trade_intents_still_charged(self, intent_type: IntentType, market_state: MarketState) -> None:
        model = DefaultFeeModel(fee_pct=Decimal("0.003"))
        fee = model.calculate_fee(
            intent_type=intent_type,
            amount_usd=Decimal("10000"),
            market_state=market_state,
        )
        assert fee == Decimal("30")


class TestFeeModelConventionDispatch:
    """Connector fee models (ABC convention) are invocable by the engine."""

    def test_detects_abc_convention(self) -> None:
        assert _fee_model_takes_trade_amount(AaveV3FeeModel()) is True
        assert _fee_model_takes_trade_amount(CompoundV3FeeModel()) is True

    def test_detects_engine_protocol_convention(self) -> None:
        assert _fee_model_takes_trade_amount(DefaultFeeModel()) is False

    def test_connector_model_invocable_without_typeerror(self, market_state: MarketState) -> None:
        fee = _invoke_fee_model(
            AaveV3FeeModel(),
            intent_type=IntentType.SUPPLY,
            amount_usd=Decimal("10000"),
            market_state=market_state,
            protocol="aave_v3",
        )
        assert fee == Decimal("0")

    def test_connector_model_receives_amount_as_trade_amount(self, market_state: MarketState) -> None:
        # Aave charges an origination fee on BORROW only: a non-zero result
        # proves amount_usd bound to the ABC's leading trade_amount parameter.
        model = AaveV3FeeModel(borrow_origination_fee_pct=Decimal("0.0001"))
        fee = _invoke_fee_model(
            model,
            intent_type=IntentType.BORROW,
            amount_usd=Decimal("10000"),
            market_state=market_state,
            protocol="aave_v3",
        )
        assert fee == Decimal("1")

    def test_default_model_dispatch_unchanged(self, market_state: MarketState) -> None:
        fee = _invoke_fee_model(
            DefaultFeeModel(fee_pct=Decimal("0.003")),
            intent_type=IntentType.SWAP,
            amount_usd=Decimal("1000"),
            market_state=market_state,
            protocol="uniswap_v3",
        )
        assert fee == Decimal("3")


class _SupplyOnceStrategy:
    """Mirrors the campaign-50 s31 shape: supply once, then hold."""

    def __init__(self, protocol: str = "aave_v3") -> None:
        self._protocol = protocol
        self._supplied = False

    @property
    def deployment_id(self) -> str:
        return "supply_once"

    def decide(self, market: Any) -> Any:
        if self._supplied:
            return None
        self._supplied = True
        return Intent.supply(
            protocol=self._protocol,
            token="USDC",
            amount=Decimal("1000"),
            chain="arbitrum",
        )


class _SwapOnceStrategy:
    def __init__(self) -> None:
        self._swapped = False

    @property
    def deployment_id(self) -> str:
        return "swap_once"

    def decide(self, market: Any) -> Any:
        if self._swapped:
            return None
        self._swapped = True
        return Intent.swap(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1000"),
            chain="arbitrum",
            protocol="uniswap_v3",
        )


def _make_backtester(fee_models: dict[str, Any] | None = None) -> PnLBacktester:
    return PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models=fee_models or {"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )


def _make_config() -> PnLBacktestConfig:
    start = datetime(2024, 1, 1)
    return PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(hours=6),
        interval_seconds=3600,
        token_funding=_pnl_token_funding(Decimal("10000")),
        tokens=["WETH", "USDC"],
    )


class TestEngineGenericPathFees:
    """Engine-level: fees recorded on generic-path fills (s31 shape)."""

    @pytest.mark.asyncio
    async def test_supply_fill_has_zero_fee(self) -> None:
        result = await _make_backtester().backtest(_SupplyOnceStrategy(), _make_config())

        assert result.success
        supply_trades = [t for t in result.trades if t.intent_type == "SUPPLY" and t.success]
        assert supply_trades, "expected a filled SUPPLY trade on the generic path"
        for trade in supply_trades:
            assert trade.fee_usd == Decimal("0")
            assert trade.slippage_usd == Decimal("0")
        assert result.metrics.total_fees_usd == Decimal("0")

    @pytest.mark.asyncio
    async def test_supply_fill_via_connector_fee_model_no_typeerror(self) -> None:
        # The ABC-convention connector model wired for the intent's protocol:
        # the engine must invoke it without TypeError and record zero fee.
        backtester = _make_backtester(
            fee_models={"default": DefaultFeeModel(), "aave_v3": AaveV3FeeModel()},
        )
        result = await backtester.backtest(_SupplyOnceStrategy(), _make_config())

        assert result.success
        supply_trades = [t for t in result.trades if t.intent_type == "SUPPLY" and t.success]
        assert supply_trades, "expected a filled SUPPLY trade via the connector fee model"
        assert all(t.fee_usd == Decimal("0") for t in supply_trades)

    @pytest.mark.asyncio
    async def test_swap_fill_still_charged(self) -> None:
        result = await _make_backtester().backtest(_SwapOnceStrategy(), _make_config())

        assert result.success
        swap_trades = [t for t in result.trades if t.intent_type == "SWAP" and t.success]
        assert swap_trades, "expected a filled SWAP trade"
        for trade in swap_trades:
            assert trade.fee_usd == trade.amount_usd * Decimal("0.003")
            assert trade.fee_usd > Decimal("0")
