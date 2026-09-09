"""Unit tests for `_run_test_lifecycle` in run_helpers.py.

Covers the contract of `almanak strat test` separately from the e2e
demo runs: predicate consistency, teardown-only success, action
fail-fast with `failure_logs`, and the JSON summary shape.
"""

import json
import logging
import re
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.cli.run_helpers import _run_test_lifecycle
from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult, TransactionResult
from almanak.framework.runner.runner_models import IterationResult, IterationStatus
from almanak.framework.teardown.models import TeardownPositionSummary


def _parse_last_json_object(stream: str) -> dict:
    """Extract the last top-level JSON object from a pretty-printed stream.

    Uses ``JSONDecoder.raw_decode`` so quoted braces inside string fields
    (e.g. ``failure_logs`` or ``error``) don't confuse the parser.
    """
    decoder = json.JSONDecoder()
    for m in reversed(list(re.finditer(r"^\{", stream, re.MULTILINE))):
        try:
            payload, _ = decoder.raw_decode(stream[m.start() :])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    raise AssertionError(f"No JSON object found in stream:\n{stream}")


def _make_runner(*results: IterationResult) -> MagicMock:
    """Mock runner.run_iteration that yields the given results in order.

    Configures the production-parity hooks the helper now invokes (matching
    _run_once): snapshot capture is disabled by default to keep tests focused
    on the lifecycle contract; tests that want to exercise it can override.
    """
    runner = MagicMock()
    runner.setup_gateway_integration = MagicMock()
    runner.teardown_gateway_integration = MagicMock()
    runner._emit_iteration_summary = MagicMock()
    runner.run_iteration = AsyncMock(side_effect=list(results))
    runner.config = MagicMock(enable_state_persistence=False)
    runner._capture_portfolio_snapshot = AsyncMock()
    return runner


def _make_strategy() -> MagicMock:
    s = MagicMock(
        spec=[
            "deployment_id",
            "STRATEGY_NAME",
            "chain",
            "force_action",
            "load_state_async",
            "_wallet_activity_provider",
            "flush_pending_saves",
            # VIB-6285: ``get_open_positions`` is a concrete method on
            # ``IntentStrategy`` — EVERY real strategy has it. Omitting it from the
            # spec made this double lower-fidelity than production: the
            # post-teardown residual read raised AttributeError, i.e. the check was
            # UNMEASURED. That used to pass the ladder silently; an unmeasured
            # post-teardown read no longer certifies, so the double now models what
            # a real strategy actually exposes. The genuinely-unreadable case keeps
            # its own dedicated coverage in
            # tests/unit/cli/test_strat_test_teardown_residual.py.
            "get_open_positions",
        ]
    )
    s.deployment_id = "TestStrategy:abc"
    s.STRATEGY_NAME = "TestStrategy"
    s.chain = "ethereum"
    s.force_action = ""
    s.load_state_async = AsyncMock(return_value=False)
    s._wallet_activity_provider = None  # not a copy-trading strategy
    s.flush_pending_saves = AsyncMock()
    s.get_open_positions = MagicMock(
        return_value=TeardownPositionSummary(
            deployment_id="TestStrategy:abc",
            timestamp=datetime.now(UTC),
            positions=[],
        )
    )
    return s


def _noop_cleanup() -> AsyncMock:
    return AsyncMock()


def _result(status: IterationStatus, error: str | None = None) -> IterationResult:
    return IterationResult(status=status, deployment_id="TestStrategy:abc", error=error)


def _executed_result(*, tx_hashes: list[str] | None = None) -> IterationResult:
    intent = MagicMock()
    intent.serialize.return_value = {"type": "LP_OPEN", "pool": "USDF/USDT"}
    execution_result = MagicMock()
    execution_result.to_dict.return_value = {
        "success": True,
        "tx_hashes": ["0xabc"] if tx_hashes is None else tx_hashes,
    }
    return IterationResult(
        status=IterationStatus.SUCCESS,
        intent=intent,
        execution_result=execution_result,
        deployment_id="TestStrategy:abc",
    )


def _orchestrator_executed_result(*, tx_hash: str) -> IterationResult:
    intent = MagicMock()
    intent.serialize.return_value = {"type": "LP_OPEN", "pool": "USDF/USDT"}
    execution_result = ExecutionResult(
        success=True,
        phase=ExecutionPhase.COMPLETE,
        transaction_results=[TransactionResult(tx_hash=tx_hash, success=True)],
    )
    return IterationResult(
        status=IterationStatus.SUCCESS,
        intent=intent,
        execution_result=execution_result,
        deployment_id="TestStrategy:abc",
    )


def test_unmeasured_teardown_only_fails(capsys, monkeypatch):
    """Completion without measured unwind evidence cannot certify teardown."""
    # No-op teardown state manager; the lifecycle creates a teardown request and
    # the runner's iteration returns a TEARDOWN status.
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager",
        lambda *a, **k: MagicMock(create_request=MagicMock()),
    )
    runner = _make_runner(_result(IterationStatus.TEARDOWN))
    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=[],
        teardown=True,
        json_output=True,
    )
    captured = capsys.readouterr()
    payload = _parse_last_json_object(captured.out)
    assert exit_code == 1
    assert payload["summary"]["all_passed"] is False
    assert payload["summary"]["actions_passed"] is True  # all([]) == True
    assert payload["summary"]["teardown_passed"] is False
    assert len(payload["steps"]) == 1
    assert payload["steps"][0]["action"] == "teardown"


def test_action_failure_attaches_failure_logs_and_breaks(capsys):
    """When an action fails, `failure_logs` must be attached and remaining actions skipped."""
    runner = _make_runner(_result(IterationStatus.EXECUTION_FAILED, error="bad swap"))
    # Inject a WARN+ERROR log record after _BufferingHandler is attached so
    # logs_before is captured cleanly per-step.
    strategy = _make_strategy()

    def _failing_iteration(_strategy):
        logging.getLogger("test").error("synthetic on-chain error")
        return _result(IterationStatus.EXECUTION_FAILED, error="bad swap")

    runner.run_iteration = AsyncMock(side_effect=lambda s: _failing_iteration(s))

    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=strategy,
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["open", "close"],  # 'close' must NOT run after 'open' fails
        teardown=False,
        json_output=True,
    )
    captured = capsys.readouterr()
    payload = _parse_last_json_object(captured.out)
    assert exit_code == 1
    assert payload["summary"]["all_passed"] is False
    # Fail-fast: only the failing step ran.
    assert len(payload["steps"]) == 1
    assert payload["steps"][0]["action"] == "open"
    assert payload["steps"][0]["status"] == "EXECUTION_FAILED"
    assert "failure_logs" in payload["steps"][0]
    assert any("synthetic on-chain error" in r for r in payload["steps"][0]["failure_logs"])
    assert payload["summary"]["coverage"]["actions"] == [
        {"action": "open", "outcome": "failed"},
        {"action": "close", "outcome": "not_run"},
    ]
    assert payload["summary"]["coverage"]["requested_paths_exercised"] is False


def test_empty_transaction_hash_does_not_prove_action_coverage(capsys):
    """A serialized placeholder hash is not trade-effective evidence."""
    runner = _make_runner(_executed_result(tx_hashes=[""]))

    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["open"],
        teardown=False,
        json_output=True,
    )
    payload = _parse_last_json_object(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["summary"]["coverage"] == {
        "requested_paths_exercised": False,
        "actions": [{"action": "open", "outcome": "unmeasured"}],
        "teardown": "not_requested",
    }


def test_real_orchestrator_result_requires_usable_transaction_hash(capsys):
    """The production ExecutionResult serializer rejects whitespace-only hashes."""
    runner = _make_runner(_orchestrator_executed_result(tx_hash="   "))

    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["open"],
        teardown=False,
        json_output=True,
    )
    payload = _parse_last_json_object(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["summary"]["coverage"]["actions"] == [{"action": "open", "outcome": "unmeasured"}]
    assert payload["summary"]["coverage"]["requested_paths_exercised"] is False


def test_accepted_clob_order_is_trade_effective_evidence(capsys):
    """An accepted order id covers the asynchronous prediction-market lane."""
    result = _executed_result(tx_hashes=[])
    result.execution_result.to_dict.return_value["extracted_data"] = {"order_id": "order-123"}
    runner = _make_runner(result)

    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["buy"],
        teardown=False,
        json_output=True,
    )
    payload = _parse_last_json_object(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["summary"]["coverage"]["actions"] == [{"action": "buy", "outcome": "executed"}]
    assert payload["summary"]["coverage"]["requested_paths_exercised"] is True


@pytest.mark.parametrize("order_id", ["", "   ", 123])
def test_invalid_clob_order_id_does_not_prove_action_coverage(capsys, order_id):
    result = _executed_result(tx_hashes=[])
    result.execution_result.to_dict.return_value["extracted_data"] = {"order_id": order_id}
    runner = _make_runner(result)

    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["buy"],
        teardown=False,
        json_output=True,
    )
    payload = _parse_last_json_object(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["summary"]["coverage"]["actions"] == [{"action": "buy", "outcome": "unmeasured"}]
    assert payload["summary"]["coverage"]["requested_paths_exercised"] is False


def test_inject_sentinel_is_not_reported_as_requested_force_action(capsys):
    """An empty force_action runs natural decide() and is not a requested path."""
    runner = _make_runner(_result(IterationStatus.HOLD))

    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=[""],
        teardown=False,
        json_output=True,
    )
    payload = _parse_last_json_object(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["summary"]["coverage"] == {
        "requested_paths_exercised": None,
        "actions": [],
        "teardown": "not_requested",
    }


def test_inject_sentinel_keeps_natural_decide_human_verdict(capsys):
    runner = _make_runner(_result(IterationStatus.HOLD))

    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=[""],
        teardown=False,
        json_output=False,
    )

    assert exit_code == 0
    assert "Test lifecycle passed." in capsys.readouterr().out


def test_action_hold_does_not_certify_requested_paths(capsys, monkeypatch):
    """Safe holds continue the sequence but cannot prove requested execution."""
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager",
        lambda *a, **k: MagicMock(create_request=MagicMock()),
    )
    runner = _make_runner(
        _result(IterationStatus.HOLD),
        _result(IterationStatus.SUCCESS),
        _result(IterationStatus.TEARDOWN),
    )
    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["maybe_buy", "buy"],
        teardown=True,
        json_output=True,
    )
    captured = capsys.readouterr()
    payload = _parse_last_json_object(captured.out)
    assert exit_code == 1
    assert payload["summary"]["all_passed"] is False
    assert len(payload["steps"]) == 3  # 2 actions + 1 teardown, no fail-fast
    assert "failure_logs" in payload["steps"][0]
    assert "failure_logs" in payload["steps"][1]
    assert "observed held" in payload["steps"][0]["assertion_error"]
    assert "observed unmeasured" in payload["steps"][1]["assertion_error"]
    assert payload["summary"]["coverage"] == {
        "requested_paths_exercised": False,
        "actions": [
            {"action": "maybe_buy", "outcome": "held"},
            {"action": "buy", "outcome": "unmeasured"},
        ],
        "teardown": "unmeasured",
    }
    assert payload["steps"][0]["coverage"] == "held"
    assert payload["steps"][2]["coverage"] == "unmeasured"


def test_trustworthy_zero_position_breakdown_reports_nothing_to_unwind(capsys, monkeypatch):
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager",
        lambda *a, **k: MagicMock(create_request=MagicMock()),
    )
    runner = _make_runner(_result(IterationStatus.TEARDOWN))
    runner._teardown_closure_verification = {
        "all_closed": True,
        "positions_total": 0,
        "closure_unknown": False,
        "positions_closed": 0,
        "has_position_breakdown": True,
    }

    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=[],
        teardown=True,
        json_output=True,
    )
    payload = _parse_last_json_object(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["summary"]["all_passed"] is True
    assert payload["summary"]["deployment_ready"] is False
    assert payload["summary"]["coverage"]["teardown"] == "nothing_to_unwind"
    assert payload["summary"]["coverage"]["requested_paths_exercised"] is False


def test_positive_execution_and_chain_closure_prove_requested_paths(capsys, monkeypatch):
    """Only execution evidence plus measured closure certifies lifecycle coverage."""
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager",
        lambda *a, **k: MagicMock(create_request=MagicMock()),
    )
    runner = _make_runner(_orchestrator_executed_result(tx_hash="0xabc"), _result(IterationStatus.TEARDOWN))
    runner._teardown_closure_verification = {
        "all_closed": True,
        "closure_unknown": False,
        "has_position_breakdown": True,
        "protocols_to_prove": ["curve"],
        "measured_closed_protocols": ["curve"],
        "positions_total": 1,
        "positions_closed": 1,
    }

    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["open"],
        teardown=True,
        json_output=True,
    )
    payload = _parse_last_json_object(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["summary"]["coverage"] == {
        "requested_paths_exercised": True,
        "actions": [{"action": "open", "outcome": "executed"}],
        "teardown": "proved",
    }
    assert payload["steps"][0]["coverage"] == "executed"
    assert payload["steps"][1]["coverage"] == "proved"
    assert payload["summary"]["deployment_ready"] is True


def test_action_requires_terminal_settlement_but_teardown_uses_recovery_lane(capsys, monkeypatch):
    """The lifecycle barrier applies to actions and is restored after teardown."""
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager",
        lambda *a, **k: MagicMock(create_request=MagicMock()),
    )
    runner = _make_runner()
    runner._require_terminal_async_settlement = False
    requirements_seen: list[bool] = []

    async def fake_run_iteration(_strategy):
        requirements_seen.append(runner._require_terminal_async_settlement)
        if len(requirements_seen) == 1:
            return _result(IterationStatus.SUCCESS)
        return _result(IterationStatus.TEARDOWN)

    runner.run_iteration = AsyncMock(side_effect=fake_run_iteration)
    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["open"],
        teardown=True,
        json_output=True,
    )
    payload = _parse_last_json_object(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["summary"]["all_passed"] is False
    assert requirements_seen == [True, False]
    assert runner._require_terminal_async_settlement is False


def test_teardown_failure_marks_all_passed_false(capsys, monkeypatch):
    """If teardown returns a non-TEARDOWN status, the run must fail."""
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager",
        lambda *a, **k: MagicMock(create_request=MagicMock()),
    )
    runner = _make_runner(
        _result(IterationStatus.SUCCESS),
        _result(IterationStatus.STRATEGY_ERROR, error="positions still open"),
    )
    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["supply"],
        teardown=True,
        json_output=True,
    )
    captured = capsys.readouterr()
    payload = _parse_last_json_object(captured.out)
    assert exit_code == 1
    assert payload["summary"]["all_passed"] is False
    assert payload["summary"]["actions_passed"] is False
    assert payload["summary"]["teardown_passed"] is False


def test_action_failure_still_runs_teardown(capsys, monkeypatch):
    """Load-bearing contract: a failed action must NOT skip teardown when --teardown is set.

    The helper's design says "Always run teardown when requested — even if an earlier
    action failed". This test pins that behavior so a regression flips a clear signal.
    """
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager",
        lambda *a, **k: MagicMock(create_request=MagicMock()),
    )
    runner = _make_runner(
        _result(IterationStatus.EXECUTION_FAILED, error="bad swap"),
        _result(IterationStatus.TEARDOWN),
    )
    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["open", "close"],  # 'close' must NOT run after 'open' fails
        teardown=True,
        json_output=True,
    )
    payload = _parse_last_json_object(capsys.readouterr().out)
    assert exit_code == 1  # overall failure because `open` failed
    # Steps must be: failed `open` (fail-fast skipped `close`), then teardown.
    assert [s["action"] for s in payload["steps"]] == ["open", "teardown"]
    assert payload["summary"]["actions_passed"] is False
    assert payload["summary"]["teardown_passed"] is False
    assert payload["summary"]["all_passed"] is False


def test_action_raise_does_not_skip_teardown(capsys, monkeypatch):
    """If run_iteration raises mid-action, teardown still runs.

    Without this, an exception in iteration N would leave positions opened by
    iterations 1..N-1 unclosed. The contract is "always run teardown when requested."
    """
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager",
        lambda *a, **k: MagicMock(create_request=MagicMock()),
    )
    runner = MagicMock()
    runner.setup_gateway_integration = MagicMock()
    runner.teardown_gateway_integration = MagicMock()
    runner._emit_iteration_summary = MagicMock()
    runner.config = MagicMock(enable_state_persistence=False)
    runner._capture_portfolio_snapshot = AsyncMock()
    # First action: SUCCESS. Second action: raises. Teardown: TEARDOWN.
    iteration_calls = [
        _result(IterationStatus.SUCCESS),
        RuntimeError("transient connector blowup"),
        _result(IterationStatus.TEARDOWN),
    ]

    async def fake_run_iteration(_strategy):
        nxt = iteration_calls.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt

    runner.run_iteration = AsyncMock(side_effect=fake_run_iteration)

    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["open", "close"],
        teardown=True,
        json_output=True,
    )
    payload = _parse_last_json_object(capsys.readouterr().out)
    actions_seen = [s["action"] for s in payload["steps"]]
    # open succeeded, close raised → recorded as STRATEGY_ERROR step, then teardown ran.
    assert actions_seen == ["open", "close", "teardown"]
    assert payload["steps"][1]["status"] == "STRATEGY_ERROR"
    assert "transient connector blowup" in payload["steps"][1]["error"]
    assert payload["summary"]["teardown_passed"] is False
    assert payload["summary"]["actions_passed"] is False
    assert payload["summary"]["all_passed"] is False
    assert exit_code == 1


def test_teardown_raise_materializes_failed_step(capsys, monkeypatch):
    """A raise during the teardown iteration becomes a failed teardown step in JSON."""
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager",
        lambda *a, **k: MagicMock(create_request=MagicMock()),
    )
    runner = MagicMock()
    runner.setup_gateway_integration = MagicMock()
    runner.teardown_gateway_integration = MagicMock()
    runner._emit_iteration_summary = MagicMock()
    runner.config = MagicMock(enable_state_persistence=False)
    runner._capture_portfolio_snapshot = AsyncMock()
    iteration_calls = [
        _result(IterationStatus.SUCCESS),
        RuntimeError("teardown blew up"),
    ]

    async def fake_run_iteration(_strategy):
        nxt = iteration_calls.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt

    runner.run_iteration = AsyncMock(side_effect=fake_run_iteration)
    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["open"],
        teardown=True,
        json_output=True,
    )
    payload = _parse_last_json_object(capsys.readouterr().out)
    assert [s["action"] for s in payload["steps"]] == ["open", "teardown"]
    teardown_step = payload["steps"][-1]
    assert teardown_step["status"] == "STRATEGY_ERROR"
    assert "teardown blew up" in teardown_step["error"]
    assert payload["summary"]["teardown_passed"] is False
    assert payload["summary"]["all_passed"] is False
    assert exit_code == 1


def test_lifecycle_exception_emits_json_error(capsys, monkeypatch):
    """If the lifecycle coroutine raises, --json must still emit a structured payload."""
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager",
        lambda *a, **k: MagicMock(create_request=MagicMock()),
    )
    runner = MagicMock()
    runner.setup_gateway_integration = MagicMock(side_effect=RuntimeError("gateway boot failed"))
    runner.teardown_gateway_integration = MagicMock()
    runner._emit_iteration_summary = MagicMock()
    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["supply"],
        teardown=False,
        json_output=True,
    )
    captured = capsys.readouterr()
    payload = _parse_last_json_object(captured.out)
    assert exit_code == 1
    assert payload["summary"]["all_passed"] is False
    assert "gateway boot failed" in payload["summary"]["error"]


@pytest.mark.parametrize("expected_hold", [False, True])
def test_all_held_lifecycle_requires_explicit_opt_in(capsys, monkeypatch, expected_hold):
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager",
        lambda *a, **k: MagicMock(create_request=MagicMock()),
    )
    runner = _make_runner(*[_result(IterationStatus.HOLD) for _ in range(3)], _result(IterationStatus.TEARDOWN))
    runner._teardown_closure_verification = {
        "all_closed": True,
        "positions_total": 0,
        "closure_unknown": False,
        "positions_closed": 0,
        "has_position_breakdown": True,
    }
    cleanup = _noop_cleanup()
    strategy = _make_strategy()
    strategy.test_action_expectations = {
        action: {"expected_to_hold": expected_hold, "reason": "Exercise guard behavior."}
        for action in ["open", "close"]
    }
    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=strategy,
        state_manager=MagicMock(),
        cleanup_fn=cleanup,
        actions=["open", "close", "open"],
        teardown=True,
        json_output=True,
    )
    summary = _parse_last_json_object(capsys.readouterr().out)["summary"]
    assert exit_code == (0 if expected_hold else 1)
    assert summary["all_passed"] is expected_hold
    assert summary["actions_passed"] is expected_hold
    assert summary["teardown_passed"] is True
    assert summary["test_action_expectations"] == strategy.test_action_expectations
    assert summary["deployment_ready"] is False
    assert summary["coverage"]["requested_paths_exercised"] is False
    assert runner.run_iteration.await_count == 4
    cleanup.assert_awaited_once()


@pytest.mark.parametrize("status", [IterationStatus.SUCCESS, IterationStatus.EXECUTION_FAILED])
def test_expected_hold_never_accepts_failed_or_unmeasured_execution(capsys, status):
    strategy = _make_strategy()
    strategy.test_action_expectations = {"open": {"expected_to_hold": True, "reason": "Guard must hold."}}
    exit_code = _run_test_lifecycle(
        runner=_make_runner(_result(status)),
        strategy_instance=strategy,
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["open"],
        teardown=False,
        json_output=True,
    )
    assert exit_code == 1
    assert _parse_last_json_object(capsys.readouterr().out)["summary"]["actions_passed"] is False


@pytest.mark.parametrize("residual", [False, True])
def test_expected_hold_cannot_bypass_unmeasured_or_residual_teardown(capsys, monkeypatch, residual):
    from almanak.framework.cli import _run_modes

    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager",
        lambda *a, **k: MagicMock(create_request=MagicMock()),
    )
    monkeypatch.setattr(
        _run_modes,
        "_measure_open_positions_after_teardown",
        lambda _: ([{"position_id": "open"}], None) if residual else ([], "unavailable"),
    )
    exit_code = _run_test_lifecycle(
        runner=_make_runner(_result(IterationStatus.TEARDOWN)),
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=[],
        teardown=True,
        json_output=True,
    )
    assert exit_code == 1
    assert _parse_last_json_object(capsys.readouterr().out)["summary"]["teardown_passed"] is False


@pytest.mark.parametrize(
    "override",
    [
        {"positions_total": None},
        {"positions_total": -1},
        {"positions_total": 0.5},
        {"positions_total": False},
        {"positions_total": "0"},
        {"positions_closed": None},
        {"positions_closed": 1},
        {"positions_closed": False},
        {"closure_unknown": True},
        {"closure_unknown": None},
        {"all_closed": False},
        {"all_closed": None},
        {"all_closed": 1},
    ],
)
def test_expected_hold_rejects_malformed_empty_teardown_evidence(capsys, monkeypatch, override):
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager",
        lambda *a, **k: MagicMock(create_request=MagicMock()),
    )
    runner = _make_runner(_result(IterationStatus.TEARDOWN))
    runner._teardown_closure_verification = {
        "all_closed": True,
        "positions_total": 0,
        "positions_closed": 0,
        "closure_unknown": False,
        "has_position_breakdown": True,
        **override,
    }
    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=[],
        teardown=True,
        json_output=True,
    )
    summary = _parse_last_json_object(capsys.readouterr().out)["summary"]
    assert exit_code == 1
    assert summary["teardown_passed"] is False
    assert summary["coverage"]["teardown"] == "unmeasured"


@pytest.mark.parametrize(
    "declared",
    [
        None,
        [],
        {"": {}},
        {"teardown": {}},
        {"open": {}},
        {"open": {"expected_to_hold": "true", "reason": "guard"}},
        {"open": {"expected_to_hold": 1, "reason": "guard"}},
        {"open": {"expected_to_hold": True, "reason": "  "}},
        {"open": {"expected_to_hold": False, "reason": None}},
        {"open": {"expected_to_hold": True, "reason": "guard", "typo": True}},
    ],
)
def test_invalid_expectations_fail_before_setup_and_still_cleanup(capsys, declared):
    runner = _make_runner(_executed_result())
    strategy = _make_strategy()
    strategy.test_action_expectations = declared
    cleanup = _noop_cleanup()
    code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=strategy,
        state_manager=MagicMock(),
        cleanup_fn=cleanup,
        actions=["open"],
        teardown=False,
        json_output=True,
    )
    summary = _parse_last_json_object(capsys.readouterr().out)["summary"]
    assert code == 1
    assert summary["all_passed"] is False
    assert summary["deployment_ready"] is False
    assert "test_action_expectations" in summary["error"]
    assert summary["test_action_expectations"] is None
    runner.setup_gateway_integration.assert_not_called()
    runner.run_iteration.assert_not_awaited()
    strategy.load_state_async.assert_not_awaited()
    cleanup.assert_awaited_once()


@pytest.mark.parametrize("initial_hold", [False, True])
def test_expectations_cannot_change_after_observing_results(capsys, initial_hold):
    strategy = _make_strategy()
    strategy.test_action_expectations = {"open": {"expected_to_hold": initial_hold, "reason": "Before run."}}
    runner = _make_runner()

    async def mutate_and_hold(_):
        strategy.test_action_expectations["open"]["expected_to_hold"] = not initial_hold
        strategy.test_action_expectations["open"]["reason"] = "After result."
        return _result(IterationStatus.HOLD)

    runner.run_iteration.side_effect = mutate_and_hold
    code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=strategy,
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["open"],
        teardown=False,
        json_output=True,
    )
    payload = _parse_last_json_object(capsys.readouterr().out)
    assert code == (0 if initial_hold else 1)
    assert payload["summary"]["test_action_expectations"]["open"] == {
        "expected_to_hold": initial_hold,
        "reason": "Before run.",
    }
    assert payload["steps"][0]["expectation"] == payload["summary"]["test_action_expectations"]["open"]
    assert payload["summary"]["deployment_ready"] is False


def test_hold_assertion_fails_on_unexpected_execution(capsys):
    strategy = _make_strategy()
    strategy.test_action_expectations = {"open": {"expected_to_hold": True, "reason": "Guard must hold."}}
    code = _run_test_lifecycle(
        runner=_make_runner(_executed_result()),
        strategy_instance=strategy,
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["open"],
        teardown=False,
        json_output=True,
    )
    summary = _parse_last_json_object(capsys.readouterr().out)["summary"]
    assert code == 1
    assert summary["all_passed"] is False
    assert summary["coverage"]["requested_paths_exercised"] is True
    assert summary["deployment_ready"] is False


def test_one_hold_declaration_does_not_excuse_other_action(capsys):
    strategy = _make_strategy()
    strategy.test_action_expectations = {"open": {"expected_to_hold": True, "reason": "Entry guard."}}
    runner = _make_runner(_result(IterationStatus.HOLD), _result(IterationStatus.HOLD))
    code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=strategy,
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["open", "close"],
        teardown=False,
        json_output=True,
    )
    summary = _parse_last_json_object(capsys.readouterr().out)["summary"]
    assert code == 1
    assert summary["test_action_expectations"]["close"]["expected_to_hold"] is False
    assert runner.run_iteration.await_count == 2


def test_hold_declaration_cannot_excuse_error_payload(capsys):
    strategy = _make_strategy()
    strategy.test_action_expectations = {"open": {"expected_to_hold": True, "reason": "Entry guard."}}
    code = _run_test_lifecycle(
        runner=_make_runner(_result(IterationStatus.HOLD, error="data unavailable")),
        strategy_instance=strategy,
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["open"],
        teardown=False,
        json_output=True,
    )
    assert code == 1
    assert _parse_last_json_object(capsys.readouterr().out)["summary"]["all_passed"] is False


@pytest.mark.parametrize("json_output", [False, True])
def test_assertion_mismatch_reports_expected_and_actual_without_stopping_recovery(capsys, json_output):
    runner = _make_runner(_result(IterationStatus.HOLD), _result(IterationStatus.HOLD))
    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=_make_strategy(),
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=["open", "close"],
        teardown=False,
        json_output=json_output,
    )
    captured = capsys.readouterr()
    assert exit_code == 1
    assert runner.run_iteration.await_count == 2
    if json_output:
        steps = _parse_last_json_object(captured.out)["steps"]
        assert all("failure_logs" in step for step in steps)
        assert "Expected executed action 'open', observed held" in steps[0]["assertion_error"]
    else:
        assert "assertion failed: Expected executed action 'open', observed held" in captured.err
        assert "assertion failed: Expected executed action 'close', observed held" in captured.err


def test_valid_empty_expectations_are_captured_as_mapping(capsys):
    runner = _make_runner(_executed_result())
    strategy = _make_strategy()
    strategy.test_action_expectations = {}
    code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=strategy,
        state_manager=MagicMock(),
        cleanup_fn=_noop_cleanup(),
        actions=[""],
        teardown=False,
        json_output=True,
    )
    summary = _parse_last_json_object(capsys.readouterr().out)["summary"]
    assert code == 0
    assert summary["test_action_expectations"] == {}
    runner.setup_gateway_integration.assert_called_once()
