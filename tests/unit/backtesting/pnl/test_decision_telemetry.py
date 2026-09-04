"""Per-tick decision telemetry in the pnl backtest (iteration_summary counterpart).

Covers the DecisionLog unit contract (exactly-once per tick, hold-reason
grouping, JSON safety), the engine-side hold recording in
``_invoke_strategy_decide``, and the end-to-end plane: a 100%-hold run must
explain itself in ``result.decision_summary`` / ``result.decision_events``
instead of rendering as a silent flat line (the staging ``643d3686`` genre),
and ``serialize_result`` must ship the aggregate but never the per-tick
stream (that is the decisions.jsonl sidecar's job).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.backtesting.pnl._engine_helpers import _invoke_strategy_decide
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import normalize_token_key
from almanak.framework.backtesting.pnl.decision_log import DecisionLog
from almanak.framework.backtesting.pnl.engine import DefaultFeeModel, DefaultSlippageModel, PnLBacktester
from almanak.framework.backtesting.pnl.run_validity import build_verdict, family_all_rejected_reason, terminal_errors
from almanak.framework.intents import Intent
from almanak.services.backtest.serialization import serialize_result
from tests.unit.backtesting.pnl._mocks import MockDataProvider

BASE_WETH = "0x4200000000000000000000000000000000000006"
BASE_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"

_T0 = datetime(2026, 6, 1, tzinfo=UTC)


def _ts(hours: int = 0) -> datetime:
    return _T0 + timedelta(hours=hours)


# ---------------------------------------------------------------------------
# DecisionLog unit contract
# ---------------------------------------------------------------------------


class TestDecisionLog:
    def test_hold_intent_record_carries_reason_and_code(self):
        log = DecisionLog()
        hold = Intent.hold(reason="Allocation data unavailable: live lending position is unavailable")
        log.record(tick=1, timestamp=_ts(), intent=hold, source="strategy")

        (event,) = log.events()
        assert event["event"] == "decision"
        assert event["decision"] == "HOLD"
        assert event["source"] == "strategy"
        assert event["hold_reason"] == "Allocation data unavailable: live lending position is unavailable"
        assert "intents" not in event

    def test_first_write_wins_per_tick(self):
        # Engine-side warm-up hold records first; the loop's follow-up call
        # for the same tick must be a no-op.
        log = DecisionLog()
        log.record(tick=1, timestamp=_ts(), intent=None, source="warm_up", detail="RSI not ready")
        log.record(tick=1, timestamp=_ts(), intent=None, source="strategy")

        (event,) = log.events()
        assert event["source"] == "warm_up"
        assert event["hold_reason"] == "RSI not ready"
        assert event["hold_reason_code"] == "ENGINE_INDICATOR_WARM_UP"
        assert log.summary()["ticks"] == 1

    def test_digit_normalized_reason_grouping(self):
        log = DecisionLog()
        for tick in range(1, 6):
            hold = Intent.hold(reason=f"Circuit breaker active, cooldown {1800 - tick}s remaining")
            log.record(tick=tick, timestamp=_ts(tick), intent=hold, source="strategy")

        summary = log.summary()
        assert summary["hold_ticks"] == 5
        (group,) = summary["hold_reasons"]
        assert group["reason_template"] == "Circuit breaker active, cooldown Ns remaining"
        assert group["example"] == "Circuit breaker active, cooldown 1799s remaining"
        assert group["ticks"] == 5
        assert (group["first_tick"], group["last_tick"]) == (1, 5)

    def test_reason_group_cap_collapses_into_other(self):
        from almanak.framework.backtesting.pnl import decision_log as module

        log = DecisionLog()
        for tick in range(1, module._MAX_REASON_GROUPS + 11):
            # Letters vary (digit normalization must not merge these).
            hold = Intent.hold(reason=f"reason-{'x' * (tick % 200)}y")
            log.record(tick=tick, timestamp=_ts(), intent=hold, source="strategy")

        summary = log.summary()
        assert len(summary["hold_reasons"]) <= module._MAX_REASON_GROUPS + 1
        other = [g for g in summary["hold_reasons"] if g["reason_template"] == "(other)"]
        assert other and other[0]["ticks"] >= 1

    def test_rejections_are_grouped_and_expose_dominant_reason(self):
        log = DecisionLog()
        trades = [
            SimpleNamespace(
                success=False,
                intent_type=SimpleNamespace(value="PERP_OPEN"),
                protocol="gmx_v2",
                error="insufficient USDC balance: required 8.80, held 8.53",
                metadata={"rejection_code": "INSUFFICIENT_BALANCE", "retryable": False},
                timestamp=_ts(hour),
                position_id=None,
            )
            for hour in range(3)
        ]

        summary = log.summary(trades=trades)

        assert summary["execution_by_intent_type"] == {"PERP_OPEN": {"fills": 0, "rejected": 3}}
        (rejection,) = summary["rejections"]
        assert rejection["count"] == 3
        assert rejection["rejection_code"] == "INSUFFICIENT_BALANCE"
        assert rejection["reason_template"] == "insufficient USDC balance: required N, held N"
        assert rejection["retryable"] is False

    def test_fully_rejected_intent_family_invalidates_otherwise_busy_run(self):
        log = DecisionLog()
        trades = [
            SimpleNamespace(
                success=True,
                intent_type=SimpleNamespace(value="LP_OPEN"),
                protocol="uniswap_v3",
                error=None,
                metadata={},
                timestamp=_ts(),
                position_id="lp-1",
            ),
            SimpleNamespace(
                success=False,
                intent_type=SimpleNamespace(value="PERP_OPEN"),
                protocol="gmx_v2",
                error="insufficient USDC balance",
                metadata={"rejection_code": "INSUFFICIENT_BALANCE"},
                timestamp=_ts(1),
                position_id=None,
            ),
        ]

        reason = family_all_rejected_reason(log.summary(trades=trades))

        assert reason is not None and reason.code == "FAMILY_ALL_REJECTED"
        error = terminal_errors(build_verdict([reason], executed_fills=0))[0]
        assert error.startswith("BACKTEST_EXECUTION_REJECTED:")
        assert "PERP_OPEN" in error

    def test_string_intent_type_and_serialize_fallback(self):
        @dataclass
        class DuckSwap:
            intent_type: str = "SWAP"
            amount_usd: Decimal = Decimal("100")

        log = DecisionLog()
        log.record(tick=1, timestamp=_ts(), intent=DuckSwap(), source="strategy")

        (event,) = log.events()
        assert event["decision"] == "SWAP"
        assert event["intents"]  # str() fallback for serialize()-less intents
        summary = log.summary()
        assert summary["intent_ticks"] == 1
        assert summary["intent_types"] == {"SWAP": 1}

    def test_events_and_summary_are_json_serializable(self):
        log = DecisionLog()
        log.record(tick=1, timestamp=_ts(), intent=Intent.hold(reason="r"), source="strategy")
        swap = Intent.swap(from_token=BASE_WETH, to_token=BASE_USDC, amount=Decimal("1"), chain="base")
        log.record(tick=2, timestamp=_ts(1), intent=swap, source="strategy")

        json.dumps(log.events())
        json.dumps(log.summary())

    def test_extraction_failure_keeps_exactly_once_invariant(self):
        class Hostile:
            @property
            def intent_type(self):
                raise RuntimeError("boom")

        log = DecisionLog()
        log.record(tick=1, timestamp=_ts(), intent=Hostile(), source="strategy")
        log.record(tick=1, timestamp=_ts(), intent=None, source="strategy")  # still first-write-wins

        (event,) = log.events()
        assert event["extraction_error"] is True
        summary = log.summary()
        assert summary["ticks"] == 1
        assert summary["hold_ticks"] == 1
        assert summary["hold_reasons"][0]["reason_code"] == "TELEMETRY_EXTRACTION_ERROR"
        json.dumps(log.events())

    @pytest.mark.parametrize(
        ("intent", "expected"),
        [
            (SimpleNamespace(intents=(1, 2)), [1, 2]),
            (SimpleNamespace(intents=[]), []),
            (SimpleNamespace(intents=3), None),
            (None, None),
            (SimpleNamespace(intent_type="SEQUENCE", intents=[1]), None),
        ],
    )
    def test_sequence_member_detection_is_defensive(self, intent: Any, expected: list[Any] | None) -> None:
        assert DecisionLog._sequence_members(intent) == expected

    def test_sequence_event_serializes_every_member_and_counts_each_type(self) -> None:
        class SerializableMember:
            intent_type = SimpleNamespace(value="LP_OPEN")

            @staticmethod
            def serialize() -> dict[str, Any]:
                return {"amount": Decimal("1"), "created_at": _ts()}

        class BrokenMember:
            intent_type = "LP_CLOSE"

            @staticmethod
            def serialize() -> dict[str, Any]:
                raise RuntimeError("broken serializer")

            def __str__(self) -> str:
                return "broken-member"

        log = DecisionLog()
        event = log._build_sequence_event(
            tick=4,
            timestamp=_ts(3),
            members=[SerializableMember(), BrokenMember()],
            source="strategy",
        )

        assert event == {
            "event": "decision",
            "tick": 4,
            "timestamp": _ts(3).isoformat(),
            "source": "strategy",
            "decision": "SEQUENCE",
            "intents": [
                {"amount": "1", "created_at": _ts().isoformat()},
                "broken-member",
            ],
        }
        summary = log.summary()
        assert summary["intent_ticks"] == 1
        assert summary["intent_types"] == {"LP_CLOSE": 1, "LP_OPEN": 1}
        json.dumps(event)

    def test_executions_counted_from_trades(self):
        log = DecisionLog()
        trades = [SimpleNamespace(success=True), SimpleNamespace(success=True), SimpleNamespace(success=False)]
        summary = log.summary(trades=trades)
        assert summary["executions"] == {"fills": 2, "rejected": 1}


# ---------------------------------------------------------------------------
# Engine-side hold recording (_invoke_strategy_decide branches)
# ---------------------------------------------------------------------------


class TestEngineSideHoldRecording:
    def _invoke(self, strategy: Any, *, warming_up: bool) -> DecisionLog:
        log = DecisionLog()
        _invoke_strategy_decide(
            backtester=SimpleNamespace(_error_handler=None),
            strategy=strategy,
            snapshot=object(),
            tick_tokens={"WETH"},
            tick_count=7,
            timestamp=_ts(),
            indicator_engine=SimpleNamespace(is_warming_up=lambda *_a, **_k: warming_up),
            strategy_config={},
            bt_logger=SimpleNamespace(debug=lambda *_a, **_k: None, warning=lambda *_a, **_k: None),
            decision_log=log,
        )
        return log

    def test_warm_up_hold_recorded_with_cause(self):
        class Raises:
            def decide(self, _s):
                raise ValueError("Cannot calculate RSI")

        log = self._invoke(Raises(), warming_up=True)
        (event,) = log.events()
        assert event["source"] == "warm_up"
        assert event["tick"] == 7
        assert event["hold_reason"] == "Cannot calculate RSI"
        assert event["hold_reason_code"] == "ENGINE_INDICATOR_WARM_UP"

    def test_decide_error_hold_recorded_with_cause(self):
        class Raises:
            def decide(self, _s):
                raise KeyError("boom")

        log = self._invoke(Raises(), warming_up=False)
        (event,) = log.events()
        assert event["source"] == "decide_error"
        assert event["hold_reason_code"] == "ENGINE_DECIDE_ERROR"


# ---------------------------------------------------------------------------
# End-to-end: the silent 100%-hold genre must explain itself
# ---------------------------------------------------------------------------


def _config(hours: int = 3) -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=_T0,
        end_time=_T0 + timedelta(hours=hours),
        interval_seconds=3600,
        token_funding=[{"symbol": "USDC", "address": BASE_USDC, "chain": "base", "amount": "50", "amount_type": "usd"}],
        chain="base",
        tokens=["WETH", "USDC"],
        include_gas_costs=False,
        inclusion_delay_blocks=0,
        preflight_validation=False,
    )


def _backtester() -> PnLBacktester:
    provider = MockDataProvider(
        base_prices={
            normalize_token_key("base", BASE_WETH): Decimal("2500"),
            normalize_token_key("base", BASE_USDC): Decimal("1"),
        }
    )
    return PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
    )


class AlwaysHoldStrategy:
    deployment_id = "always_hold"

    def decide(self, market: Any) -> Any:
        del market
        return Intent.hold(reason="Allocation data unavailable: live lending position is unavailable")


@dataclass
class _DuckSwapIntent:
    intent_type: str = "SWAP"
    from_token: str = BASE_WETH
    to_token: str = BASE_USDC
    amount_usd: Decimal = Decimal("10")
    protocol: str = "uniswap_v3"


class TradeOnceStrategy:
    deployment_id = "trade_once"

    def __init__(self) -> None:
        self._done = False

    def decide(self, market: Any) -> Any:
        del market
        if self._done:
            return None
        self._done = True
        return _DuckSwapIntent(from_token=BASE_USDC, to_token=BASE_WETH)


@pytest.mark.asyncio
async def test_all_hold_run_explains_itself_in_result() -> None:
    result = await _backtester().backtest(AlwaysHoldStrategy(), _config())

    assert result.success
    summary = result.decision_summary
    assert summary is not None
    assert result.decision_events is not None
    assert summary["ticks"] == len(result.decision_events)
    assert summary["ticks"] > 0
    assert summary["hold_ticks"] == summary["ticks"]
    assert summary["intent_ticks"] == 0
    (group,) = summary["hold_reasons"]
    assert group["source"] == "strategy"
    assert group["example"] == "Allocation data unavailable: live lending position is unavailable"
    assert group["ticks"] == summary["ticks"]
    assert summary["executions"] == {"fills": 0, "rejected": 0}

    # Every tick carries the reason in the per-tick stream too.
    assert all(e["hold_reason"] == group["example"] for e in result.decision_events)

    # The artifact ships the aggregate, never the per-tick stream.
    payload = serialize_result(result)
    assert payload["decision_summary"] == summary
    assert "decision_events" not in payload
    json.dumps(payload["decision_summary"])


@pytest.mark.asyncio
async def test_result_dict_round_trip_preserves_decision_telemetry() -> None:
    from almanak.framework.backtesting.models import BacktestResult

    result = await _backtester().backtest(AlwaysHoldStrategy(), _config())
    restored = BacktestResult.from_dict(result.to_dict())

    assert restored.decision_summary == result.decision_summary
    assert restored.decision_events == result.decision_events
    assert restored.decision_events is not None
    assert len(restored.decision_events) == result.decision_summary["ticks"]


@pytest.mark.asyncio
async def test_trading_run_counts_intents_and_fills() -> None:
    result = await _backtester().backtest(TradeOnceStrategy(), _config())

    assert result.success
    summary = result.decision_summary
    assert summary is not None
    assert result.decision_events is not None
    assert summary["intent_ticks"] == 1
    assert summary["intent_types"] == {"SWAP": 1}
    assert summary["hold_ticks"] == summary["ticks"] - 1
    fills = [t for t in result.trades if t.success]
    assert fills
    assert summary["executions"]["fills"] == len(fills)

    intent_events = [e for e in result.decision_events if e["decision"] == "SWAP"]
    assert len(intent_events) == 1
    assert intent_events[0]["intents"]
