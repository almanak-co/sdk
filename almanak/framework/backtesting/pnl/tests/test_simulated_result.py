"""Unit tests for the PnL backtester `SimulatedExecutionResult` (VIB-2916).

Covers `build_simulated_result`, the `is_lp_open_intent` helper, and an
end-to-end backtest that asserts `on_intent_executed` is called with a
populated `position_id` matching the id the engine tracks internally — so
stateful LP strategies stop opening duplicate positions every tick AND can
later close those positions via `Intent.lp_close(position_id=...)`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.models import IntentType, TradeRecord
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.simulated_result import (
    SimulatedExecutionResult,
    build_simulated_result,
    is_lp_open_intent,
)
from almanak.framework.backtesting.pnl.tests.test_engine import (
    MockDataProvider,
)

# =============================================================================
# Mock intents
# =============================================================================


@dataclass
class _MockIntent:
    """Test double for an intent. ``intent_type`` may be an enum or a string."""

    intent_type: IntentType | str


def _trade_record(intent_type: IntentType, **overrides) -> TradeRecord:
    """Build a minimal TradeRecord for tests."""
    base = {
        "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
        "intent_type": intent_type,
        "executed_price": Decimal("3000"),
        "fee_usd": Decimal("3"),
        "slippage_usd": Decimal("0.5"),
        "gas_cost_usd": Decimal("1"),
        "pnl_usd": Decimal("0"),
        "success": True,
        "amount_usd": Decimal("1000"),
        "protocol": "uniswap_v3",
        "tokens": ["USDC", "WETH"],
    }
    base.update(overrides)
    return TradeRecord(**base)


def _backtester() -> PnLBacktester:
    return PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )


# =============================================================================
# is_lp_open_intent
# =============================================================================


class TestIsLPOpenIntent:
    def test_enum_intent_type(self):
        assert is_lp_open_intent(_MockIntent(intent_type=IntentType.LP_OPEN)) is True

    def test_string_intent_type(self):
        # AlmanakCode-generated strategies sometimes hand-roll intents whose
        # `intent_type` is a plain string. Accept both shapes.
        assert is_lp_open_intent(_MockIntent(intent_type="LP_OPEN")) is True

    def test_non_lp_intents(self):
        assert is_lp_open_intent(_MockIntent(intent_type=IntentType.SWAP)) is False
        assert is_lp_open_intent(_MockIntent(intent_type="LP_CLOSE")) is False

    def test_intent_without_type_attribute(self):
        assert is_lp_open_intent(object()) is False


# =============================================================================
# build_simulated_result
# =============================================================================


class TestBuildSimulatedResult:
    def test_lp_open_uses_real_position_id_from_trade_record(self):
        # The engine populates trade_record.position_id from the SimulatedPosition
        # it just added to the portfolio; build_simulated_result must surface
        # the same id so a later Intent.lp_close(position_id=...) resolves.
        intent = _MockIntent(intent_type=IntentType.LP_OPEN)
        record = _trade_record(IntentType.LP_OPEN, position_id="LP_uniswap_v3_USDC_WETH_1704067200")

        result = build_simulated_result(intent=intent, trade_record=record, success=True)

        assert isinstance(result, SimulatedExecutionResult)
        assert result.success is True
        assert result.position_id == "LP_uniswap_v3_USDC_WETH_1704067200"
        assert result.swap_amounts is None
        assert result.error is None
        assert result.trade_record is record

    def test_lp_open_without_position_id_in_trade_record_yields_none(self):
        # Defensive: if upstream forgets to thread position_id (e.g. an adapter
        # path that returns no position_delta), strategies see None instead of
        # a fabricated id that would never match a real position.
        intent = _MockIntent(intent_type=IntentType.LP_OPEN)
        record = _trade_record(IntentType.LP_OPEN)  # position_id defaults to None

        result = build_simulated_result(intent=intent, trade_record=record, success=True)

        assert result.position_id is None

    def test_lp_open_failure_does_not_emit_position_id(self):
        intent = _MockIntent(intent_type=IntentType.LP_OPEN)

        result = build_simulated_result(
            intent=intent,
            trade_record=None,
            success=False,
            error="liquidity too thin",
        )

        assert result.success is False
        assert result.position_id is None
        assert result.error == "liquidity too thin"
        assert result.trade_record is None

    def test_swap_success_populates_swap_amounts_from_trade_record(self):
        intent = _MockIntent(intent_type=IntentType.SWAP)
        record = _trade_record(
            IntentType.SWAP,
            actual_amount_in=Decimal("1000"),
            actual_amount_out=Decimal("0.33"),
            tokens=["USDC", "WETH"],
        )

        result = build_simulated_result(intent=intent, trade_record=record, success=True)

        assert result.success is True
        assert result.position_id is None
        assert result.swap_amounts is not None
        assert result.swap_amounts.amount_in_decimal == Decimal("1000")
        assert result.swap_amounts.amount_out_decimal == Decimal("0.33")
        assert result.swap_amounts.token_in == "USDC"
        assert result.swap_amounts.token_out == "WETH"
        # _human aliases delegate to _decimal fields (VIB-295)
        assert result.swap_amounts.amount_out_human == Decimal("0.33")

    def test_swap_without_actual_amounts_skips_swap_amounts(self):
        intent = _MockIntent(intent_type=IntentType.SWAP)
        record = _trade_record(IntentType.SWAP)  # actual_amount_in/out default None

        result = build_simulated_result(intent=intent, trade_record=record, success=True)

        assert result.success is True
        assert result.swap_amounts is None

    def test_string_intent_type_value_recognised(self):
        # AlmanakCode-generated strategies sometimes hand-roll intents whose
        # `intent_type` is a plain string. Accept both shapes.
        intent = _MockIntent(intent_type="LP_OPEN")
        record = _trade_record(IntentType.LP_OPEN, position_id="LP_uniswap_v3_USDC_WETH_x")

        result = build_simulated_result(intent=intent, trade_record=record, success=True)

        assert result.position_id == "LP_uniswap_v3_USDC_WETH_x"


# =============================================================================
# Engine._build_callback_result
# =============================================================================


class TestEngineBuildCallbackResult:
    def test_lp_open_surfaces_trade_record_position_id(self):
        engine = _backtester()
        intent = _MockIntent(intent_type=IntentType.LP_OPEN)
        record = _trade_record(IntentType.LP_OPEN, position_id="LP_uniswap_v3_USDC_WETH_42")

        result = engine._build_callback_result(intent, record, success=True)

        assert result.position_id == "LP_uniswap_v3_USDC_WETH_42"

    def test_failure_emits_no_position_id(self):
        engine = _backtester()
        intent = _MockIntent(intent_type=IntentType.LP_OPEN)

        result = engine._build_callback_result(intent, None, success=False, error="boom")

        assert result.success is False
        assert result.position_id is None
        assert result.error == "boom"

    def test_swap_intent_emits_no_position_id(self):
        engine = _backtester()
        swap_intent = _MockIntent(intent_type=IntentType.SWAP)
        record = _trade_record(IntentType.SWAP, position_id="should_be_ignored")

        result = engine._build_callback_result(swap_intent, record, success=True)

        # SWAP intents must not surface a position_id even if the trade_record
        # carries one (would never happen in practice but guards the contract).
        assert result.position_id is None


# =============================================================================
# End-to-end backtest: stateful LP strategy receives populated position_id
# =============================================================================


@dataclass
class _LPOpenIntent:
    """Plain LP_OPEN intent for backtest pipeline (string intent_type)."""

    intent_type: str = "LP_OPEN"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount_usd: Decimal = Decimal("500")
    protocol: str = "uniswap_v3"


@dataclass
class _StatefulLPStrategy:
    """Test double mirroring real LP strategy callback contract.

    Issues a single LP_OPEN intent and records every callback invocation so
    the test can assert that `result.position_id` is populated for the
    backtest, matching what production strategies expect.
    """

    callbacks: list[tuple[bool, Any]] = field(default_factory=list)
    decide_calls: int = 0

    @property
    def strategy_id(self) -> str:
        return "stateful_lp_test"

    def decide(self, _market: Any) -> _LPOpenIntent | None:
        self.decide_calls += 1
        if self.decide_calls == 1:
            return _LPOpenIntent()
        return None

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        # This is the field strategies actually read in production.
        self.callbacks.append((success, getattr(result, "position_id", None)))


@pytest.mark.asyncio
async def test_pnl_backtest_invokes_callback_with_real_position_id() -> None:
    """End-to-end: PnL backtester populates result.position_id for LP_OPEN.

    Without VIB-2916, strategies receive the raw `TradeRecord` (which lacked a
    `position_id` attribute), `result.position_id` resolves to None, and
    stateful strategies open duplicate positions every tick. The id surfaced
    must match `SimulatedPosition.position_id` so a later
    `Intent.lp_close(position_id=...)` resolves against the open position.
    """
    base_ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    config = PnLBacktestConfig(
        start_time=base_ts,
        end_time=base_ts + timedelta(hours=5),
        interval_seconds=3600,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
    )

    backtester = PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    strategy = _StatefulLPStrategy()
    result = await backtester.backtest(strategy, config)

    assert result.success
    successful = [pid for ok, pid in strategy.callbacks if ok and pid is not None]
    assert successful, "Stateful LP strategy never received a populated position_id — VIB-2916 regression"
    surfaced_id = successful[0]
    # Must match SimulatedPosition.__post_init__ format: "<type>_<protocol>_<tokens>_<ts>"
    # rather than the historical synthetic "backtest-pos-N" placeholder, so that
    # Intent.lp_close(position_id=surfaced_id) resolves against the engine's
    # tracked position instead of silently failing.
    assert isinstance(surfaced_id, str)
    assert surfaced_id.startswith("LP_uniswap_v3_"), surfaced_id
    assert not surfaced_id.startswith("backtest-pos-"), (
        "Synthetic placeholder id leaked back into the callback — VIB-2916 regression"
    )


@pytest.mark.asyncio
async def test_pnl_backtest_callback_result_is_simulated_execution_result() -> None:
    """Strategies should receive a SimulatedExecutionResult (not a raw TradeRecord).

    This is the contract that lets strategies use `hasattr(result, "swap_amounts")`,
    `result.error`, etc. — same shape as production ExecutionResult.
    """
    base_ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    config = PnLBacktestConfig(
        start_time=base_ts,
        end_time=base_ts + timedelta(hours=5),
        interval_seconds=3600,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
    )

    received: list[Any] = []

    @dataclass
    class _CaptureStrategy:
        decide_calls: int = 0

        @property
        def strategy_id(self) -> str:
            return "capture"

        def decide(self, _market: Any) -> _LPOpenIntent | None:
            self.decide_calls += 1
            if self.decide_calls == 1:
                return _LPOpenIntent()
            return None

        def on_intent_executed(self, _intent: Any, _success: bool, result: Any) -> None:
            received.append(result)

    backtester = PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    await backtester.backtest(_CaptureStrategy(), config)

    assert received, "on_intent_executed was never invoked"
    callback_result = received[0]
    assert isinstance(callback_result, SimulatedExecutionResult)
    # Mirror the ExecutionResult attributes strategies read in production.
    assert hasattr(callback_result, "position_id")
    assert hasattr(callback_result, "swap_amounts")
    assert hasattr(callback_result, "extracted_data")
    assert hasattr(callback_result, "error")


@dataclass
class _SwapIntent:
    """Plain SWAP intent for backtest pipeline (string intent_type)."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount_usd: Decimal = Decimal("500")
    protocol: str = "uniswap_v3"


@pytest.mark.asyncio
async def test_pnl_backtest_swap_callback_reaches_strategy() -> None:
    """End-to-end SWAP: PnL backtester delivers a SimulatedExecutionResult.

    Exercises the real path through `SimulatedFill.to_trade_record()` so a
    SWAP intent flowing through `PnLBacktester.backtest()` reaches the
    strategy callback with the new wrapper shape (instead of a raw
    `TradeRecord`).

    Note on `swap_amounts`: this test does NOT assert that
    `result.swap_amounts` is populated end-to-end because the PnL engine
    does not currently set `TradeRecord.actual_amount_in` /
    `actual_amount_out` for SWAP fills (only `executed_price` and
    `amount_usd`). That upstream bookkeeping gap is out of scope for
    VIB-2916/VIB-2918 — the callback shape contract introduced here is
    correct and `_build_swap_amounts` is unit-tested with hand-built
    records that do carry actual amounts.
    """
    base_ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    config = PnLBacktestConfig(
        start_time=base_ts,
        end_time=base_ts + timedelta(hours=5),
        interval_seconds=3600,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
    )

    received: list[Any] = []

    @dataclass
    class _SwapStrategy:
        decide_calls: int = 0

        @property
        def strategy_id(self) -> str:
            return "swap_capture"

        def decide(self, _market: Any) -> _SwapIntent | None:
            self.decide_calls += 1
            if self.decide_calls == 1:
                return _SwapIntent()
            return None

        def on_intent_executed(self, _intent: Any, success: bool, result: Any) -> None:
            if success:
                received.append(result)

    backtester = PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    await backtester.backtest(_SwapStrategy(), config)

    assert received, "Successful SWAP never reached on_intent_executed"
    callback_result = received[0]
    assert isinstance(callback_result, SimulatedExecutionResult), (
        "SWAP callback must receive a SimulatedExecutionResult, not a raw TradeRecord"
    )
    assert callback_result.success is True
    # No position_id for SWAP intents (only LP_OPEN populates it).
    assert callback_result.position_id is None
    # The trade record reached the wrapper even though the engine doesn't
    # currently fill in actual_amount_in/out — confirms the callback shape
    # change in VIB-2916 is wired into the real SWAP path.
    assert callback_result.trade_record is not None
    assert callback_result.trade_record.intent_type == IntentType.SWAP
