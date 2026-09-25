"""Price consensus waits preserve safety checks and recover without redeployment."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.execution.circuit_breaker import CircuitBreakerState
from almanak.framework.intents.vocabulary import HoldIntent
from almanak.framework.market.builders import MarketSnapshotBuilder
from almanak.framework.runner._run_loop_helpers import handle_iteration_failure, handle_iteration_success
from almanak.framework.runner.failure_kind import FailureKind
from almanak.framework.runner.runner_models import IterationResult, IterationStatus
from almanak.framework.runner.strategy_runner import RunIterationState
from tests.unit.runner.test_data_outage_recovery import _make_breaker, _make_runner, _make_strategy

TOKEN = "0xaa07a0e9209e16ac99708c3ec70159c6ef3128a3"
DISAGREEMENT = (
    "All data sources failed: dexscreener: Two-source divergence: "
    "coingecko=0.062437 vs dexscreener=0.06530 (4.59% apart, no consensus possible); "
    "coingecko: Two-source divergence: coingecko=0.062437 vs dexscreener=0.06530 "
    "(4.59% apart, no consensus possible); binance: Data source 'binance' unavailable: "
    "Token '0XAA07A0E9209E16AC99708C3EC70159C6EF3128A3' has no corroborated Binance listing"
)


def _market(error=DISAGREEMENT):
    return MarketSnapshotBuilder.for_strategy_runner(
        strategy=SimpleNamespace(
            chain="robinhood",
            wallet_address="0xtest",
            price_oracle=MagicMock(side_effect=DataSourceUnavailable("price_aggregator", error)),
        ),
        runtime_surface="unit_test",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("exposure", [False, True, None])
async def test_repeated_disagreement_is_neutral_and_recovers(exposure):
    breaker = _make_breaker()
    if exposure is not None:
        breaker.record_exposure(exposure)
    runner = _make_runner(circuit_breaker=breaker)
    strategy = _make_strategy()
    market = _market()
    breaker.record_failure("previous revert", kind=FailureKind.EXECUTION_REVERTED)
    runner._consecutive_errors = 1
    runner._first_error_at = datetime.now(UTC)
    first_error = runner._first_error_at

    with (
        patch.object(runner, "_maybe_trigger_emergency", new_callable=AsyncMock) as emergency,
        patch.object(runner, "_lifecycle_write_state") as lifecycle,
        patch.object(runner, "_alert_consecutive_errors", new_callable=AsyncMock) as alert,
    ):
        for _ in range(35):
            with pytest.raises(ValueError, match="Cannot determine price"):
                market.price(TOKEN)
            assert market.classify_critical_data_failures() == "price_disagreement"
            state = RunIterationState(
                strategy=strategy,
                deployment_id="test_strategy",
                start_time=datetime.now(UTC),
                market=market,
                decide_result=HoldIntent(reason="ORBIO price data unavailable"),
            )
            result = runner._step_extract_intents(state)
            assert result.status is IterationStatus.DATA_ERROR
            assert not result.success
            assert result.intent.reason_code == "PRICE_DISAGREEMENT"
            assert "retrying automatically" in result.intent.reason
            assert "stop-loss protection is unavailable" in result.error
            assert "4.59%" in result.intent.reason_details["price_failure"]
            await handle_iteration_failure(runner, strategy, "test_strategy", result)

        assert breaker.state is CircuitBreakerState.CLOSED
        assert breaker.get_status()["consecutive_action_failures"] == 1
        assert breaker.get_status()["consecutive_data_failures"] == 0
        assert runner._consecutive_errors == 1
        assert runner._first_error_at == first_error
        emergency.assert_not_awaited()
        alert.assert_not_awaited()
        lifecycle.assert_not_called()

        market._price_oracle.side_effect = None
        market._price_oracle.return_value = Decimal("0.064")
        assert market.price(TOKEN) == Decimal("0.064")
        assert not market.has_critical_data_failures()
        state.decide_result = HoldIntent(reason="Price recovered; awaiting entry signal")
        recovered = runner._step_extract_intents(state)
        assert recovered.status is IterationStatus.HOLD
        assert recovered.success
        handle_iteration_success(runner, "test_strategy", was_in_error_streak=True)
        assert runner._consecutive_errors == 0
        # A healthy HOLD does not prove the previously failing execution path.
        assert breaker.get_status()["consecutive_action_failures"] == 1


@pytest.mark.parametrize(
    ("source", "error"),
    [
        ("price", "Unknown token: USD"),
        ("price", "Price magnitude mismatch"),
        ("price", "connection timeout"),
        ("price", "upstream echoed: Two-source divergence: a=1 vs b=2 (50.00% apart, no consensus possible)"),
        ("price", "a: Two-source divergence: a=1 vs a=2 (50.00% apart, no consensus possible)"),
        ("balance", DISAGREEMENT),
    ],
)
def test_other_critical_failures_do_not_inherit_consensus_exemption(source, error):
    market = _market()
    with pytest.raises(ValueError):
        market.price(TOKEN)
    market._record_critical_data_failure(source, "other", error)
    assert market.classify_critical_data_failures() != "price_disagreement"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [IterationStatus.ACCOUNTING_FAILED, IterationStatus.EXECUTION_FAILED])
async def test_later_failure_cannot_be_neutralized_by_price_wait(status):
    breaker = _make_breaker()
    runner = _make_runner(circuit_breaker=breaker)
    strategy = _make_strategy()
    result = IterationResult(
        status=status,
        intent=HoldIntent(reason="Waiting for price"),
        error="failed",
        failure_kind=FailureKind.PRICE_DISAGREEMENT,
    )
    await handle_iteration_failure(runner, strategy, "test_strategy", result)
    assert breaker.get_status()["consecutive_action_failures"] == 1
    assert runner._consecutive_errors == 1


@pytest.mark.asyncio
async def test_real_failures_still_trip_after_consensus_wait():
    breaker = _make_breaker()
    runner = _make_runner(circuit_breaker=breaker)
    strategy = _make_strategy()
    with patch.object(runner, "_maybe_trigger_emergency", new_callable=AsyncMock):
        for _ in range(3):
            await handle_iteration_failure(
                runner, strategy, "test_strategy", IterationResult(status=IterationStatus.EXECUTION_FAILED)
            )
            await handle_iteration_failure(
                runner,
                strategy,
                "test_strategy",
                IterationResult(
                    status=IterationStatus.DATA_ERROR,
                    intent=HoldIntent(),
                    failure_kind=FailureKind.PRICE_DISAGREEMENT,
                ),
            )
    assert breaker.state is CircuitBreakerState.OPEN


@pytest.mark.asyncio
async def test_full_iteration_retries_price_and_returns_to_normal_decisions():
    breaker = _make_breaker()
    breaker.record_exposure(False)
    runner = _make_runner(circuit_breaker=breaker)
    strategy = _make_strategy()
    market = _market()
    strategy.create_market_snapshot.return_value = market

    def decide(snapshot):
        try:
            snapshot.price(TOKEN)
        except ValueError:
            return HoldIntent(reason="ORBIO price data unavailable")
        return HoldIntent(reason="Price recovered; awaiting entry signal")

    strategy.decide.side_effect = decide
    with (
        patch.object(runner, "_is_strategy_paused", new_callable=AsyncMock, return_value=(False, None)),
        patch.object(runner, "_check_teardown_requested", return_value=None),
        patch.object(runner, "_pre_warm_prices", new_callable=AsyncMock),
        patch.object(runner, "_maybe_trigger_emergency", new_callable=AsyncMock) as emergency,
    ):
        for _ in range(4):
            result = await runner.run_iteration(strategy)
            assert result.failure_kind is FailureKind.PRICE_DISAGREEMENT
            await handle_iteration_failure(runner, strategy, strategy.deployment_id, result)
        assert runner._total_iterations == 4
        assert runner._consecutive_errors == 0
        assert breaker.state is CircuitBreakerState.CLOSED
        emergency.assert_not_awaited()

        market._price_oracle.side_effect = None
        market._price_oracle.return_value = Decimal("0.064")
        result = await runner.run_iteration(strategy)
        assert result.status is IterationStatus.HOLD
        assert result.intent.reason == "Price recovered; awaiting entry signal"
        assert strategy.decide.call_count == 5
        runner.execution_orchestrator.execute.assert_not_called()
