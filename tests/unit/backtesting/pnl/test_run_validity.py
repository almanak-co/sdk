"""Run-validity verdict (ALM-3045 / epic ALM-3471, blueprint 31 §4.2).

The verdict is the one place that decides whether a finished run's metrics may
be published as strategy performance. These tests pin the classifier's
predicates, the wire prefixes on ``result.error``, the round trip through the
artifact, and the engine-level shapes that used to certify as successful:
zero capital, zero ticks, and an intent lost from the ledger.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
    RunValidity,
    RunValidityReason,
    RunValidityVerdict,
)
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import DefaultFeeModel, DefaultSlippageModel, PnLBacktester
from almanak.framework.backtesting.pnl.run_validity import (
    ENGINE_ERROR,
    FAMILY_ALL_REJECTED,
    INPUT_STARVED,
    INPUT_STARVED_LANE,
    INTENTS_UNRECORDED,
    NO_TICKS,
    ZERO_INITIAL_CAPITAL,
    build_verdict,
    classify_run_validity,
    engine_error_verdict,
    terminal_errors,
)
from almanak.services.backtest.serialization import serialize_result
from tests.backtesting_funding import pnl_token_funding

ARB_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"


def _summary(
    *,
    intent_ticks: int = 0,
    fills: int = 0,
    rejected: int = 0,
    by_type: dict[str, dict[str, int]] | None = None,
    rejections: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "ticks": 10,
        "intent_ticks": intent_ticks,
        "hold_ticks": 10 - intent_ticks,
        "executions": {"fills": fills, "rejected": rejected},
        "execution_by_intent_type": by_type or {},
        "rejections": rejections or [],
    }


def _failure(pattern: str, ticks: int = 9) -> dict[str, Any]:
    return {"source": "twap", "key": "unconfigured", "ticks": ticks, "detail": "no provider", "pattern": pattern}


def _classify(**overrides: Any) -> RunValidityVerdict:
    kwargs: dict[str, Any] = {
        "tick_count": 10,
        "initial_capital_usd": Decimal("10000"),
        "decision_summary": _summary(),
        "decision_input_failures": [],
        "executed_fills": 0,
    }
    kwargs.update(overrides)
    return classify_run_validity(**kwargs)


class TestClassifier:
    def test_traded_run_is_valid(self):
        verdict = _classify(decision_summary=_summary(intent_ticks=2, fills=2), executed_fills=2)
        assert verdict.validity is RunValidity.VALID
        assert verdict.reasons == ()
        assert verdict.passive_only is False
        assert terminal_errors(verdict) == []

    def test_held_run_is_valid_but_passive_only(self):
        verdict = _classify()
        assert verdict.validity is RunValidity.VALID
        assert verdict.passive_only is True
        assert verdict.executed_fills == 0

    def test_zero_ticks_is_invalid(self):
        verdict = _classify(tick_count=0, initial_capital_usd=Decimal("0"))
        assert verdict.validity is RunValidity.INVALID
        # No capital is not a second reason when nothing ran at all.
        assert verdict.reason_codes == (NO_TICKS,)
        assert terminal_errors(verdict)[0].startswith("BACKTEST_INVALID: ")

    def test_zero_capital_is_invalid(self):
        verdict = _classify(initial_capital_usd=Decimal("0"))
        assert verdict.validity is RunValidity.INVALID
        assert verdict.reason_codes == (ZERO_INITIAL_CAPITAL,)
        assert verdict.passive_only is False
        assert "passive mark-to-market" in verdict.reasons[0].message

    def test_intents_without_terminal_record_are_invalid(self):
        verdict = _classify(decision_summary=_summary(intent_ticks=3))
        assert verdict.reason_codes == (INTENTS_UNRECORDED,)
        assert verdict.validity is RunValidity.INVALID

    def test_all_rejected_family_keeps_its_wire_prefix(self):
        summary = _summary(
            intent_ticks=2,
            fills=1,
            rejected=2,
            by_type={"SWAP": {"fills": 1, "rejected": 0}, "PERP_OPEN": {"fills": 0, "rejected": 2}},
            rejections=[{"intent_type": "PERP_OPEN", "example": "insufficient USDC balance"}],
        )
        verdict = _classify(decision_summary=summary, executed_fills=1)
        assert verdict.validity is RunValidity.INVALID
        assert verdict.reason_codes == (FAMILY_ALL_REJECTED,)
        (error,) = terminal_errors(verdict)
        assert error.startswith("BACKTEST_EXECUTION_REJECTED: ")
        assert "PERP_OPEN (2 rejected)" in error
        assert verdict.reasons[0].details["families"] == {"PERP_OPEN": {"fills": 0, "rejected": 2}}

    def test_partial_perp_rejection_remains_valid_after_same_family_filled(self):
        summary = _summary(
            intent_ticks=2,
            fills=1,
            rejected=1,
            by_type={"PERP_OPEN": {"fills": 1, "rejected": 1}},
            rejections=[{"intent_type": "PERP_OPEN", "rejection_code": "INSUFFICIENT_MARGIN"}],
        )
        verdict = _classify(decision_summary=summary, executed_fills=1)

        assert verdict.validity is RunValidity.VALID
        assert verdict.reasons == ()
        assert terminal_errors(verdict) == []

    def test_partial_non_perp_rejection_remains_valid(self):
        summary = _summary(
            intent_ticks=2,
            fills=1,
            rejected=1,
            by_type={"SWAP": {"fills": 1, "rejected": 1}},
            rejections=[{"intent_type": "SWAP", "rejection_code": "INSUFFICIENT_BALANCE"}],
        )
        verdict = _classify(decision_summary=summary, executed_fills=1)

        assert verdict.validity is RunValidity.VALID
        assert verdict.reasons == ()

    def test_persistent_starvation_without_action_is_not_evaluable(self):
        # 9 of 10 ticks is "persistent" but not 100%: the old predicate let
        # this run through as a successful hold.
        verdict = _classify(decision_input_failures=[_failure("persistent", ticks=9)])
        assert verdict.validity is RunValidity.NOT_EVALUABLE
        assert verdict.reason_codes == (INPUT_STARVED,)
        (error,) = terminal_errors(verdict)
        assert error.startswith("BACKTEST_UNSUPPORTED_DATA: ")
        assert "twap:unconfigured (9/10 ticks" in error

    @pytest.mark.parametrize("pattern", ["intermittent", "warm_up"])
    def test_non_persistent_failures_keep_a_held_run_valid(self, pattern: str):
        verdict = _classify(decision_input_failures=[_failure(pattern, ticks=2)])
        assert verdict.validity is RunValidity.VALID
        assert verdict.warnings == ()

    def test_starved_lane_on_a_traded_run_is_a_warning_only(self):
        verdict = _classify(
            decision_summary=_summary(intent_ticks=2, fills=2),
            decision_input_failures=[_failure("persistent")],
            executed_fills=2,
        )
        assert verdict.validity is RunValidity.VALID
        assert verdict.reasons == ()
        assert [warning.code for warning in verdict.warnings] == [INPUT_STARVED_LANE]
        assert terminal_errors(verdict) == []

    def test_required_exact_pool_ohlcv_starvation_invalidates_a_traded_run(self):
        failure = {
            "source": "ohlcv",
            "key": (
                "pool:base:0x1111111111111111111111111111111111111111:"
                "0x2222222222222222222222222222222222222222/"
                "0x3333333333333333333333333333333333333333@1h:pool_scoped"
            ),
            "ticks": 9,
            "detail": "exact-pool candles unavailable",
            "pattern": "persistent",
        }
        verdict = _classify(
            decision_summary=_summary(intent_ticks=2, fills=1),
            decision_input_failures=[failure],
            executed_fills=1,
        )

        assert verdict.validity is RunValidity.NOT_EVALUABLE
        assert verdict.reason_codes == (INPUT_STARVED,)
        assert verdict.warnings == ()
        assert "1 fill(s) across 2 intent tick(s)" in verdict.reasons[0].message
        assert terminal_errors(verdict)[0].startswith("BACKTEST_UNSUPPORTED_DATA: ")

    def test_exact_pool_starvation_keeps_pool_orientation_and_timeframe_lanes_distinct(self):
        pool_a = "0x1111111111111111111111111111111111111111"
        pool_b = "0x4444444444444444444444444444444444444444"
        token_a = "0x2222222222222222222222222222222222222222"
        token_b = "0x3333333333333333333333333333333333333333"
        lane_ids = (
            (pool_a, f"{token_a}/{token_b}", "15m"),
            (pool_b, f"{token_a}/{token_b}", "15m"),
            (pool_a, f"{token_b}/{token_a}", "1h"),
            (pool_a, f"{token_a}/{token_b}", "1h"),
        )
        failures = [
            {
                "source": "ohlcv",
                "key": f"pool:base:{pool}:{orientation}@{timeframe}:pool_scoped",
                "ticks": 9,
                "detail": "lane unavailable",
                "pattern": "persistent",
            }
            for pool, orientation, timeframe in lane_ids
        ]

        verdict = _classify(decision_input_failures=failures)

        assert verdict.validity is RunValidity.NOT_EVALUABLE
        inputs = verdict.reasons[0].details["inputs"]
        assert [entry["key"] for entry in inputs] == [failure["key"] for failure in failures]
        error = terminal_errors(verdict)[0]
        assert pool_b in error
        assert f"{token_b}/{token_a}" in error
        assert "@1h:pool_scoped" in error

    def test_most_severe_reason_wins_and_all_are_kept(self):
        summary = _summary(intent_ticks=0)
        verdict = _classify(
            initial_capital_usd=Decimal("0"),
            decision_summary=summary,
            decision_input_failures=[_failure("persistent")],
        )
        assert verdict.validity is RunValidity.INVALID
        assert set(verdict.reason_codes) == {ZERO_INITIAL_CAPITAL, INPUT_STARVED}
        errors = terminal_errors(verdict)
        assert errors[0].startswith("BACKTEST_INVALID: ")
        assert errors[1].startswith("BACKTEST_UNSUPPORTED_DATA: ")

    def test_engine_error_verdict(self):
        verdict = engine_error_verdict(RuntimeError("boom"))
        assert verdict.validity is RunValidity.INVALID
        assert verdict.reason_codes == (ENGINE_ERROR,)
        assert verdict.reasons[0].details == {"exception_type": "RuntimeError"}

    def test_unknown_reason_code_is_treated_as_invalid(self):
        verdict = build_verdict([RunValidityReason(code="SOMETHING_NEW", message="m")], executed_fills=0)
        assert verdict.validity is RunValidity.INVALID
        assert terminal_errors(verdict) == ["BACKTEST_INVALID: m"]


class TestRoundTrip:
    def test_verdict_dict_round_trip(self):
        verdict = build_verdict(
            [RunValidityReason(code=ZERO_INITIAL_CAPITAL, message="no capital", details={"tick_count": 3})],
            warnings=[RunValidityReason(code=INPUT_STARVED_LANE, message="lane")],
            executed_fills=0,
        )
        payload = verdict.to_dict()
        assert payload["validity"] == "INVALID"
        assert payload["reasons"][0]["code"] == ZERO_INITIAL_CAPITAL
        assert payload["schema_version"] == 1
        assert RunValidityVerdict.from_dict(payload) == verdict

    def test_unknown_stored_validity_degrades_to_invalid(self):
        # A newer writer's fifth verdict value must not make the whole artifact unreadable.
        verdict = RunValidityVerdict.from_dict(
            {"schema_version": 2, "validity": "SOMETHING_NEW", "reasons": [], "executed_fills": 0, "passive_only": True}
        )
        assert verdict.validity is RunValidity.INVALID
        assert verdict.schema_version == 2
        # passive_only is derived from the verdict, never copied from the payload.
        assert verdict.passive_only is False

    def test_passive_only_is_derived_on_load(self):
        held = RunValidityVerdict.from_dict({"validity": "VALID", "executed_fills": 0, "passive_only": False})
        traded = RunValidityVerdict.from_dict({"validity": "VALID", "executed_fills": 3, "passive_only": True})
        assert held.passive_only is True
        assert traded.passive_only is False

    def test_backtest_result_round_trip_and_legacy_artifacts(self):
        verdict = build_verdict([], executed_fills=0)
        start = datetime(2024, 1, 1, tzinfo=UTC)
        result = BacktestResult(
            engine=BacktestEngine.PNL,
            deployment_id="round_trip",
            start_time=start,
            end_time=start + timedelta(hours=1),
            metrics=BacktestMetrics(),
            run_validity=verdict,
        )
        data = result.to_dict()
        assert data["run_validity"]["validity"] == "VALID"
        assert data["run_validity"]["passive_only"] is True
        assert BacktestResult.from_dict(data).run_validity == verdict
        # Artifacts written before the contract existed carry no verdict.
        data["run_validity"] = None
        assert BacktestResult.from_dict(data).run_validity is None


class _TickingProvider:
    provider_name = "mock_ticking"

    def __init__(self, num_ticks: int) -> None:
        self.num_ticks = num_ticks

    async def iterate(self, config: Any):
        start = datetime(2024, 1, 1, tzinfo=UTC)
        for i in range(self.num_ticks):
            timestamp = start + timedelta(hours=i)
            price = Decimal("3000") + Decimal(i % 10)
            yield (
                timestamp,
                MarketState(
                    timestamp=timestamp,
                    prices={"WETH": price, "ETH": price, "USDC": Decimal("1"), ("arbitrum", ARB_USDC): Decimal("1")},
                    chain="arbitrum",
                    block_number=1000 + i,
                ),
            )


class _Holder:
    deployment_id = "holder"

    def decide(self, market: Any) -> Any:
        return None


def _config(num_hours: int, funding_amount: str) -> PnLBacktestConfig:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    return PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(hours=num_hours),
        token_funding=pnl_token_funding(Decimal(funding_amount), chain="arbitrum"),
        tokens=["WETH", "USDC"],
        preflight_validation=False,
        fail_on_preflight_error=True,
        inclusion_delay_blocks=0,
    )


def _backtester(num_ticks: int) -> PnLBacktester:
    return PnLBacktester(
        data_provider=_TickingProvider(num_ticks=num_ticks),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )


class TestEngineVerdict:
    @pytest.mark.asyncio
    async def test_held_run_is_valid_passive_only_and_serialized(self):
        result = await _backtester(4).backtest(_Holder(), _config(4, "10000"))

        assert result.success is True
        assert result.run_validity is not None
        assert result.run_validity.validity is RunValidity.VALID
        assert result.run_validity.passive_only is True
        assert result.institutional_compliance is True
        payload = serialize_result(result)
        assert payload["run_validity"]["validity"] == "VALID"
        assert payload["run_validity"]["passive_only"] is True

    @pytest.mark.asyncio
    async def test_zero_capital_run_no_longer_certifies(self):
        # A zero-amount funding entry seeds $0 without a first-tick price and
        # previously ran to completion as a successful, compliant result.
        result = await _backtester(4).backtest(_Holder(), _config(4, "0"))

        assert result.initial_portfolio_value_usd == Decimal("0")
        assert result.run_validity is not None
        assert result.run_validity.validity is RunValidity.INVALID
        assert result.run_validity.reason_codes == (ZERO_INITIAL_CAPITAL,)
        assert result.success is False
        assert result.error is not None and result.error.startswith("BACKTEST_INVALID: ")
        assert result.institutional_compliance is False
        assert result.error in result.compliance_violations

    @pytest.mark.asyncio
    async def test_zero_tick_run_is_invalid(self):
        result = await _backtester(0).backtest(_Holder(), _config(4, "10000"))

        assert result.run_validity is not None
        assert result.run_validity.validity is RunValidity.INVALID
        assert result.run_validity.reason_codes == (NO_TICKS,)
        assert result.success is False
        assert result.institutional_compliance is False

    @pytest.mark.asyncio
    async def test_raising_strategy_gets_engine_error_verdict(self):
        class _Raiser:
            deployment_id = "raiser"

            def decide(self, market: Any) -> Any:
                raise RuntimeError("decide exploded")

        result = await _backtester(4).backtest(_Raiser(), _config(4, "10000"))

        assert result.success is False
        assert result.run_validity is not None
        assert result.run_validity.validity is RunValidity.INVALID
        assert result.run_validity.reason_codes == (ENGINE_ERROR,)
