"""Unit tests for `_run_test_lifecycle` in run_helpers.py.

Covers the contract of `almanak strat test` separately from the e2e
demo runs: predicate consistency, teardown-only success, action
fail-fast with `failure_logs`, and the JSON summary shape.
"""

import json
import logging
import re
from unittest.mock import AsyncMock, MagicMock

from almanak.framework.cli.run_helpers import _run_test_lifecycle
from almanak.framework.runner.runner_models import IterationResult, IterationStatus


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
        ]
    )
    s.deployment_id = "TestStrategy:abc"
    s.STRATEGY_NAME = "TestStrategy"
    s.chain = "ethereum"
    s.force_action = ""
    s.load_state_async = AsyncMock(return_value=False)
    s._wallet_activity_provider = None  # not a copy-trading strategy
    s.flush_pending_saves = AsyncMock()
    return s


def _noop_cleanup() -> AsyncMock:
    return AsyncMock()


def _result(status: IterationStatus, error: str | None = None) -> IterationResult:
    return IterationResult(status=status, deployment_id="TestStrategy:abc", error=error)


def test_teardown_only_succeeds(capsys, monkeypatch):
    """`almanak strat test --teardown` (no actions) must return 0 when teardown completes."""
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
    assert exit_code == 0
    assert payload["summary"]["all_passed"] is True
    assert payload["summary"]["actions_passed"] is True  # all([]) == True
    assert payload["summary"]["teardown_passed"] is True
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


def test_action_hold_counts_as_pass(capsys, monkeypatch):
    """A HOLD status for an action should NOT trigger fail-fast or attach logs."""
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
    assert exit_code == 0
    assert payload["summary"]["all_passed"] is True
    assert len(payload["steps"]) == 3  # 2 actions + 1 teardown, no fail-fast
    assert "failure_logs" not in payload["steps"][0]
    assert "failure_logs" not in payload["steps"][1]


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

    assert exit_code == 0
    assert payload["summary"]["all_passed"] is True
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
    assert payload["summary"]["actions_passed"] is True
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
    assert payload["summary"]["teardown_passed"] is True
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
    assert payload["summary"]["teardown_passed"] is True
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
