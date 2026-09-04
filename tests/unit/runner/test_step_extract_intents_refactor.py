"""Characterization tests for StrategyRunner decide-result extraction."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.intents.vocabulary import HoldIntent, IntentSequence, SwapIntent
from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunIterationState,
    RunnerConfig,
    StrategyRunner,
)
from almanak.framework.strategies.lp_position_tracker import LPPositionTracker
from tests.unit.runner._state_manager import absent_state_manager

_TOKEN_ADDRESSES = {
    "USDC": "0x0000000000000000000000000000000000000001",
    "WETH": "0x0000000000000000000000000000000000000002",
    "ETH": "0x0000000000000000000000000000000000000003",
    "DAI": "0x0000000000000000000000000000000000000004",
}


def _make_runner() -> StrategyRunner:
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=absent_state_manager(),
        config=RunnerConfig(
            default_interval_seconds=1,
            enable_state_persistence=False,
            enable_alerting=False,
        ),
    )


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "extract-test"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.generate_teardown_intents.side_effect = NotImplementedError
    del strategy._wallet_activity_provider
    return strategy


def _make_state(strategy: MagicMock, decide_result: Any, market: Any = None) -> RunIterationState:
    return RunIterationState(
        strategy=strategy,
        deployment_id=strategy.deployment_id,
        start_time=datetime.now(UTC),
        decide_result=decide_result,
        market=market,
    )


def _swap(source: str, destination: str) -> SwapIntent:
    return SwapIntent(
        from_token=_TOKEN_ADDRESSES[source],
        to_token=_TOKEN_ADDRESSES[destination],
        amount=Decimal("1"),
    )


@pytest.mark.parametrize(
    ("case", "expected_hold"),
    [
        ("none", True),
        ("empty_list", True),
        ("none_leaves", True),
        ("single", False),
        ("sequence", False),
        ("list_with_sequence", False),
        ("tuple_leaf", False),
        ("nested_sequence_leaf", False),
        ("hold", True),
        ("hold_with_none", True),
    ],
)
def test_decide_result_forms_preserve_shallow_order_and_identity(case: str, expected_hold: bool) -> None:
    runner = _make_runner()
    strategy = _make_strategy()
    first = _swap("USDC", "WETH")
    second = _swap("WETH", "DAI")
    third = _swap("DAI", "USDC")
    hold = HoldIntent(reason="wait")

    if case == "none":
        decide_result, expected = None, []
    elif case == "empty_list":
        decide_result, expected = [], []
    elif case == "none_leaves":
        decide_result, expected = [None, None], []
    elif case == "single":
        decide_result, expected = first, [first]
    elif case == "sequence":
        decide_result, expected = IntentSequence([first, second]), [first, second]
    elif case == "list_with_sequence":
        decide_result = [first, IntentSequence([second, third]), None]
        expected = [first, second, third]
    elif case == "tuple_leaf":
        decide_result = (first, second)
        expected = [decide_result]
    elif case == "nested_sequence_leaf":
        nested = IntentSequence([first])
        decide_result = IntentSequence([nested])
        expected = [nested]
    elif case == "hold":
        decide_result, expected = hold, [hold]
    else:
        decide_result, expected = [None, hold, None], [hold]

    state = _make_state(strategy, decide_result)
    result = runner._step_extract_intents(state)

    assert len(state.intents) == len(expected)
    assert all(actual is wanted for actual, wanted in zip(state.intents, expected, strict=True))
    assert (result is not None) is expected_hold
    if result is not None:
        assert result.status == IterationStatus.HOLD


def test_metadata_injection_and_copy_hook_keep_order_identity_and_failure_fallback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner = _make_runner()
    strategy = _make_strategy()
    strategy._lp_position_tracker = LPPositionTracker()
    first = _swap("USDC", "WETH")
    second = _swap("WETH", "DAI")
    replacement = _swap("USDC", "ETH")
    decide_result = [first, second]
    events: list[tuple[str, Any, Any]] = []

    def inject(intent: Any) -> Any:
        events.append(("inject", intent, None))
        if intent is second:
            raise RuntimeError("metadata unavailable")
        return replacement

    def copy_hook(raw_result: Any, intents: list[Any]) -> None:
        events.append(("copy", raw_result, intents))

    strategy._framework_inject_intent_params = inject
    strategy.on_copy_decision_output = copy_hook
    state = _make_state(strategy, decide_result)

    with caplog.at_level("WARNING", logger="almanak.framework.runner.strategy_runner"):
        result = runner._step_extract_intents(state)

    assert result is None
    assert [event[0] for event in events] == ["inject", "inject", "copy"]
    assert events[0][1] is first
    assert events[1][1] is second
    assert events[2][1] is decide_result
    assert events[2][2] is state.intents
    assert state.intents[0] is replacement
    assert state.intents[1] is second
    assert "non-fatal, passing original intent" in caplog.text


def test_metadata_injection_requires_real_tracker_but_copy_hook_still_runs() -> None:
    runner = _make_runner()
    strategy = _make_strategy()
    strategy._lp_position_tracker = object()
    strategy._framework_inject_intent_params = MagicMock(side_effect=AssertionError("must not run"))
    copy_hook = MagicMock()
    strategy.on_copy_decision_output = copy_hook
    intent = _swap("USDC", "WETH")
    decide_result = [intent]
    state = _make_state(strategy, decide_result)

    assert runner._step_extract_intents(state) is None
    strategy._framework_inject_intent_params.assert_not_called()
    copy_hook.assert_called_once_with(decide_result, state.intents)
    assert state.intents[0] is intent


def test_copy_hook_error_is_nonfatal_and_state_receives_the_same_list(caplog: pytest.LogCaptureFixture) -> None:
    runner = _make_runner()
    strategy = _make_strategy()
    strategy.on_copy_decision_output.side_effect = RuntimeError("copy failed")
    intent = _swap("USDC", "WETH")
    state = _make_state(strategy, intent)

    with caplog.at_level("WARNING", logger="almanak.framework.runner.strategy_runner"):
        result = runner._step_extract_intents(state)

    assert result is None
    assert state.intents == [intent]
    assert "Error in strategy hook on_copy_decision_output: copy failed" in caplog.text


@pytest.mark.parametrize(
    ("market", "expected_status", "error_fragment"),
    [
        (None, IterationStatus.HOLD, None),
        (object(), IterationStatus.HOLD, None),
        (SimpleNamespace(has_critical_data_failures=False), IterationStatus.HOLD, None),
        (SimpleNamespace(has_critical_data_failures=lambda: False), IterationStatus.HOLD, None),
        (
            SimpleNamespace(
                has_critical_data_failures=lambda: True,
                is_quiet_pool_hold=lambda: True,
            ),
            IterationStatus.HOLD,
            None,
        ),
        (
            SimpleNamespace(
                has_critical_data_failures=lambda: True,
                is_quiet_pool_hold=lambda: 1,
            ),
            IterationStatus.DATA_ERROR,
            "classification=unknown",
        ),
        (
            SimpleNamespace(has_critical_data_failures=lambda: True),
            IterationStatus.DATA_ERROR,
            "classification=unknown",
        ),
        (
            SimpleNamespace(
                has_critical_data_failures=lambda: True,
                is_quiet_pool_hold=lambda: False,
                classify_critical_data_failures=lambda: "transient",
                summarize_critical_data_failures=lambda *, limit: f"top {limit}: timeout",
            ),
            IterationStatus.DATA_ERROR,
            "classification=transient): top 3: timeout",
        ),
    ],
)
def test_no_action_market_data_validation_table(
    market: Any,
    expected_status: IterationStatus,
    error_fragment: str | None,
) -> None:
    runner = _make_runner()
    strategy = _make_strategy()
    hold = HoldIntent(reason="no signal")
    state = _make_state(strategy, hold, market)

    result = runner._step_extract_intents(state)

    assert result is not None
    assert result.status == expected_status
    assert result.intent is hold
    if error_fragment is None:
        assert result.error is None
    else:
        assert error_fragment in (result.error or "")


def test_market_data_validation_preserves_probe_order_and_summary_limit() -> None:
    runner = _make_runner()
    strategy = _make_strategy()
    events: list[Any] = []
    market = SimpleNamespace(
        has_critical_data_failures=lambda: events.append("has") or True,
        is_quiet_pool_hold=lambda: events.append("quiet") or False,
        classify_critical_data_failures=lambda: events.append("classify") or "permanent",
        summarize_critical_data_failures=lambda *, limit: events.append(("summarize", limit)) or "bad feed",
    )

    result = runner._step_extract_intents(_make_state(strategy, None, market))

    assert result is not None
    assert result.status == IterationStatus.DATA_ERROR
    assert events == ["has", "quiet", "classify", ("summarize", 3)]


@pytest.mark.parametrize("failing_probe", ["has", "quiet", "classify", "summarize"])
def test_market_data_probe_errors_propagate_after_state_assignment(failing_probe: str) -> None:
    runner = _make_runner()
    strategy = _make_strategy()
    hold = HoldIntent(reason="wait")
    error = RuntimeError(f"{failing_probe} failed")

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise error

    market = SimpleNamespace(
        has_critical_data_failures=fail if failing_probe == "has" else lambda: True,
        is_quiet_pool_hold=fail if failing_probe == "quiet" else lambda: False,
        classify_critical_data_failures=fail if failing_probe == "classify" else lambda: "transient",
        summarize_critical_data_failures=fail if failing_probe == "summarize" else lambda *, limit: "timeout",
    )
    state = _make_state(strategy, hold, market)

    with pytest.raises(RuntimeError) as raised:
        runner._step_extract_intents(state)

    assert raised.value is error
    assert state.intents == [hold]
    assert runner._total_iterations == 0


def test_hold_logging_and_metrics_follow_quiet_pool_metadata(caplog: pytest.LogCaptureFixture) -> None:
    runner = _make_runner()
    strategy = _make_strategy()
    hold = HoldIntent(reason="quiet market")
    market = SimpleNamespace(
        has_critical_data_failures=lambda: True,
        is_quiet_pool_hold=lambda: True,
    )

    with (
        patch("almanak.framework.runner.strategy_runner._emojis_enabled", return_value=False),
        caplog.at_level("INFO", logger="almanak.framework.runner.strategy_runner"),
    ):
        result = runner._step_extract_intents(_make_state(strategy, hold, market))

    assert result is not None
    assert result.status == IterationStatus.HOLD
    relevant = [record.getMessage() for record in caplog.records if "extract-test HOLD" in record.getMessage()]
    assert relevant == [
        "extract-test HOLD on quiet but live pool (no recent trades; price still available) "
        "— not escalating to DATA_ERROR",
        "[HOLD] extract-test HOLD: quiet market",
    ]
    assert runner._total_iterations == 1
    assert runner._successful_iterations == 1


@pytest.mark.asyncio
async def test_public_iteration_preserves_filtered_hold_identity_and_copy_metadata() -> None:
    runner = _make_runner()
    strategy = _make_strategy()
    hold = HoldIntent(reason="public hold")
    decide_result = [None, hold, None]
    strategy.decide.return_value = decide_result
    market = MagicMock()
    market.has_critical_data_failures.return_value = False
    strategy.create_market_snapshot.return_value = market
    copy_hook = MagicMock()
    strategy.on_copy_decision_output = copy_hook

    with (
        patch.object(runner, "_is_strategy_paused", new=AsyncMock(return_value=(False, None))),
        patch.object(runner, "_check_teardown_requested", return_value=None),
    ):
        result = await runner.run_iteration(strategy)

    assert result.status == IterationStatus.HOLD
    assert result.intent is hold
    copied_raw, copied_intents = copy_hook.call_args.args
    assert copied_raw is decide_result
    assert copied_intents == [hold]
    assert copied_intents[0] is hold


@pytest.mark.asyncio
async def test_public_iteration_currently_dispatches_mixed_hold_and_action() -> None:
    runner = _make_runner()
    strategy = _make_strategy()
    hold = HoldIntent(reason="wait")
    action = _swap("USDC", "WETH")
    strategy.decide.return_value = [hold, action]
    market = MagicMock()
    market.has_critical_data_failures.return_value = False
    strategy.create_market_snapshot.return_value = market
    terminal = IterationResult(status=IterationStatus.SUCCESS, intent=action)

    async def assert_dispatched(state: RunIterationState) -> IterationResult:
        assert state.intents[0] is hold
        assert state.intents[1] is action
        return terminal

    with (
        patch.object(runner, "_is_strategy_paused", new=AsyncMock(return_value=(False, None))),
        patch.object(runner, "_check_teardown_requested", return_value=None),
        patch.object(runner, "_step_attach_lp_outstanding", new=AsyncMock()),
        patch.object(runner, "_step_snapshot_pre_balances", new=AsyncMock()),
        patch.object(runner, "_step_execute", side_effect=assert_dispatched),
    ):
        result = await runner.run_iteration(strategy)

    assert result is terminal
