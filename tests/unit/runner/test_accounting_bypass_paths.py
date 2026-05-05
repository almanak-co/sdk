"""Tests for VIB-3762 — close accounting-write bypass paths.

April 29 found that 8 of 10 strategies wrote zero accounting rows with no
operator-visible signal. The root cause was code paths that called
``_capture_portfolio_snapshot`` directly instead of going through the
mode-aware wrapper ``capture_snapshot_with_accounting``, plus
non-live failure logs at WARNING level (operator dashboards filter to
ERROR+).

These tests pin:
  * The ``--once`` CLI path now routes snapshot persistence through the
    mode-aware wrapper (no direct ``_capture_portfolio_snapshot`` call).
  * Outbox / metrics / unavailable-snapshot non-live failures log at
    ERROR (not WARNING).
  * Anti-gaming guard: no production code outside the wrapper itself
    calls ``runner._capture_portfolio_snapshot(...)`` directly.

Test IDs T-3762-1..T-3762-5.
"""

from __future__ import annotations

import logging
import re
import subprocess
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner


# ---------------------------------------------------------------------------
# Minimal runner harness — mirrors test_accounting_persistence.py
# ---------------------------------------------------------------------------
class _Strategy:
    def __init__(self, sid: str = "s1") -> None:
        self.strategy_id = sid


class _Runner(StrategyRunner):
    def __init__(
        self,
        *,
        config: RunnerConfig | None = None,
        state_manager: object | None = None,
        alert_manager: object | None = None,
    ) -> None:
        self.config = config or RunnerConfig()
        self.state_manager = state_manager
        self.alert_manager = alert_manager
        self._iteration_had_trade = False
        self._total_iterations = 1


# ---------------------------------------------------------------------------
# T-3762-1 / T-3762-2: outbox non-live failures log at ERROR
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_t_3762_1_outbox_returned_none_logs_error_in_non_live(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T-3762-1: outbox write returning None in non-live logs ERROR (not WARNING).

    Operator dashboards filter to ERROR+. Logging at WARNING when an
    outbox write fails was the operator-visibility half of April 29's
    silent-failure class.
    """
    runner = _Runner(config=RunnerConfig(dry_run=True))
    runner._is_live_mode = lambda: False  # type: ignore[method-assign]

    # Stub out write_outbox_entry to return None (the failure shape that
    # used to log at WARNING). _accounting_processor must exist for the
    # branch to even be reached.
    runner._accounting_processor = MagicMock()
    runner._pending_drain_tasks = set()

    intent = MagicMock()
    intent.intent_type = "SWAP"
    strategy = _Strategy()

    # Patch the import that _write_outbox_and_fire_processor performs.
    # The function does `from ..accounting.processor import write_outbox_entry`,
    # so patch the source module.
    import almanak.framework.accounting.processor as processor_mod

    original_write = processor_mod.write_outbox_entry
    processor_mod.write_outbox_entry = AsyncMock(return_value=None)
    try:
        with caplog.at_level(logging.ERROR, logger="almanak.framework.runner.strategy_runner"):
            await runner._write_outbox_and_fire_processor(
                strategy=strategy,
                intent=intent,
                ledger_entry_id="led-001",
            )
    finally:
        processor_mod.write_outbox_entry = original_write

    # The exact ERROR text we now emit:
    matching = [r for r in caplog.records if r.levelno == logging.ERROR and "outbox write returned None" in r.message]
    assert matching, "expected an ERROR-level 'outbox write returned None' log, got: " + repr(
        [(r.levelname, r.message) for r in caplog.records]
    )


@pytest.mark.asyncio
async def test_t_3762_2_outbox_unexpected_exception_logs_error_in_non_live(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T-3762-2: outbox unexpected-exception path logs ERROR in non-live."""
    runner = _Runner(config=RunnerConfig(dry_run=True))
    runner._is_live_mode = lambda: False  # type: ignore[method-assign]
    runner._accounting_processor = MagicMock()
    runner._pending_drain_tasks = set()

    import almanak.framework.accounting.processor as processor_mod

    original_write = processor_mod.write_outbox_entry
    processor_mod.write_outbox_entry = AsyncMock(side_effect=RuntimeError("boom"))
    try:
        with caplog.at_level(logging.ERROR, logger="almanak.framework.runner.strategy_runner"):
            await runner._write_outbox_and_fire_processor(
                strategy=_Strategy(),
                intent=MagicMock(intent_type="SWAP"),
                ledger_entry_id="led-002",
            )
    finally:
        processor_mod.write_outbox_entry = original_write

    matching = [r for r in caplog.records if r.levelno == logging.ERROR and "non-live" in r.message]
    assert matching, "expected ERROR log on non-live outbox failure, got: " + repr(
        [(r.levelname, r.message) for r in caplog.records]
    )


# ---------------------------------------------------------------------------
# T-3762-3: update_portfolio_metrics non-live failure logs ERROR
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_t_3762_3_update_portfolio_metrics_logs_error_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T-3762-3: update_portfolio_metrics non-live failure logs ERROR.

    ``runner_state`` uses structlog (not stdlib ``logging``) so we patch the
    module-level ``logger`` and assert on the call. Asserting via stdlib
    caplog only works for stdlib loggers; the structlog/caplog bridge
    isn't installed in this test environment.
    """
    from almanak.framework.runner import runner_state

    fake_metrics = MagicMock()
    monkey_runner = MagicMock()
    monkey_runner.state_manager = MagicMock()
    monkey_runner.state_manager.save_portfolio_metrics = AsyncMock(side_effect=RuntimeError("backend unreachable"))

    real_build = runner_state._build_metrics_for_snapshot
    runner_state._build_metrics_for_snapshot = AsyncMock(return_value=fake_metrics)

    captured_logger = MagicMock()
    monkeypatch.setattr(runner_state, "logger", captured_logger)
    try:
        await runner_state.update_portfolio_metrics(
            runner=monkey_runner,
            strategy_id="s1",
            snapshot=MagicMock(),
        )
    finally:
        runner_state._build_metrics_for_snapshot = real_build

    # Must call .error(), not .warning() — VIB-3762 §C2.
    assert captured_logger.error.called, (
        "update_portfolio_metrics non-live failure must log at ERROR. "
        f"warning calls: {captured_logger.warning.call_args_list}; "
        f"error calls: {captured_logger.error.call_args_list}"
    )
    # And it must be exactly the metrics-save failure message we expect.
    error_messages = [str(call.args[0]) if call.args else "" for call in captured_logger.error.call_args_list]
    assert any("Failed to save portfolio metrics" in m for m in error_messages), (
        f"expected 'Failed to save portfolio metrics' in error log, got: {error_messages}"
    )
    # And: the WARNING-level call site we replaced must NOT be reachable any more.
    warning_messages = [str(call.args[0]) if call.args else "" for call in captured_logger.warning.call_args_list]
    assert not any("Failed to save portfolio metrics" in m for m in warning_messages), (
        f"Regression: legacy WARNING-level metrics-save log re-introduced. warnings: {warning_messages}"
    )


# ---------------------------------------------------------------------------
# T-3762-3b: _persist_unavailable_on_failure non-live also logs ERROR
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_t_3762_3b_unavailable_snapshot_logs_error_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The UNAVAILABLE-snapshot fallback failure must also log ERROR.

    Same VIB-3762 §C2 rule. An UNAVAILABLE snapshot fallback that itself
    fails is double accounting drift; can't be hidden in WARNING.
    """
    from almanak.framework.runner import runner_state

    monkey_runner = MagicMock()
    monkey_runner.state_manager = MagicMock()
    monkey_runner.state_manager.save_portfolio_snapshot = AsyncMock(side_effect=RuntimeError("disk full"))

    captured_logger = MagicMock()
    monkeypatch.setattr(runner_state, "logger", captured_logger)
    monkeypatch.setattr(
        runner_state,
        "_make_unavailable_snapshot",
        MagicMock(return_value=MagicMock()),
    )

    from datetime import UTC, datetime

    await runner_state._persist_unavailable_on_failure(
        runner=monkey_runner,
        strategy=_Strategy(),
        iteration_number=1,
        now=datetime.now(UTC),
        error=RuntimeError("orig failure"),
    )

    assert captured_logger.error.called, (
        "_persist_unavailable_on_failure non-live failure must log ERROR. "
        f"warning calls: {captured_logger.warning.call_args_list}"
    )


# ---------------------------------------------------------------------------
# T-3762-4: --once CLI path uses the mode-aware wrapper
# ---------------------------------------------------------------------------
def test_t_3762_4_once_path_uses_mode_aware_wrapper() -> None:
    """T-3762-4: ``--once`` source must route snapshot through the wrapper.

    Source-level guard: the bypass at run_helpers.py:2687 is the
    canonical example of the silent-failure class. Pin the fix in
    source so a future refactor cannot silently revert it.
    """
    import inspect

    from almanak.framework.cli import run_helpers

    src = inspect.getsource(run_helpers)
    # Strip out the comment block we wrote that mentions
    # ``_capture_portfolio_snapshot`` as historical bypass; we want to
    # forbid actual *call sites*, not commentary.
    code_lines = []
    for line in src.split("\n"):
        stripped = line.lstrip()
        if stripped.startswith("#") or stripped.startswith('"') or stripped.startswith("'"):
            continue
        code_lines.append(line)
    code = "\n".join(code_lines)

    # No direct calls to runner._capture_portfolio_snapshot(... in code.
    direct_call = re.compile(r"runner\._capture_portfolio_snapshot\s*\(")
    assert not direct_call.search(code), (
        "run_helpers.py contains a direct runner._capture_portfolio_snapshot(...) call. "
        "Route snapshot persistence through capture_snapshot_with_accounting() so "
        "live-mode failures escalate to ACCOUNTING_FAILED and non-live failures "
        "log at ERROR. See VIB-3762 §C1."
    )
    # Positive assertion: the wrapper is invoked.
    assert "capture_snapshot_with_accounting(" in code, (
        "--once path no longer calls capture_snapshot_with_accounting; the bypass is back. See VIB-3762 §C1."
    )


# ---------------------------------------------------------------------------
# T-3762-5: anti-gaming guard — no other production bypass paths
# ---------------------------------------------------------------------------
def _repo_root() -> Path:
    """Resolve the repo root from this test file.

    Layout after #2066: this file lives at
    ``tests/unit/runner/test_accounting_bypass_paths.py`` so the repo root
    is three ancestors up. Pre-#2066 the file lived at
    ``almanak/framework/runner/tests/test_accounting_bypass_paths.py``
    (four ancestors up); the index used to be ``parents[4]``.
    """
    return Path(__file__).resolve().parents[3]


def test_t_3762_5_no_direct_capture_portfolio_snapshot_outside_wrapper() -> None:
    """T-3762-5: zero direct ``runner._capture_portfolio_snapshot(`` calls in
    production code outside the mode-aware wrapper.

    Anti-gaming guard. The whole point of §C is that EVERY snapshot write
    flows through ``capture_snapshot_with_accounting`` so the live/paper
    failure semantics are uniform. Without this test, future code can
    silently re-introduce the April 29 silent-failure class.

    Exempt files: the wrapper itself, the runner method that defines the
    function, tests, and conftest.
    """
    root = _repo_root()

    # Use ``git ls-files`` so only tracked, production-relevant files are
    # scanned. Untracked test fixtures or junk files don't matter.
    completed = subprocess.run(
        ["git", "ls-files", "--", "*.py"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    candidate_paths = [Path(line) for line in completed.stdout.splitlines() if line.strip()]

    PRODUCTION_PREFIXES = ("almanak/", "platform-plugins/", "strategies/")
    # Test files (under any /tests/ subtree) are exempt — they drive the
    # production handler directly to verify its behaviour. The guard is
    # about production code paths.
    TEST_DIR_TOKENS = ("/tests/", "tests/")
    EXEMPT = {
        # The wrapper IS allowed to call _capture_portfolio_snapshot — it's
        # the very purpose of the wrapper.
        "almanak/framework/runner/_run_loop_helpers.py",
        # The method's own definition lives here.
        "almanak/framework/runner/strategy_runner.py",
    }

    pattern = re.compile(r"runner\._capture_portfolio_snapshot\s*\(")
    offenders: list[str] = []
    for rel in candidate_paths:
        rel_str = str(rel)
        if not rel_str.startswith(PRODUCTION_PREFIXES):
            continue
        if rel_str in EXEMPT:
            continue
        if any(token in rel_str for token in TEST_DIR_TOKENS):
            continue
        try:
            content = (root / rel).read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        # Skip pure-comment occurrences by stripping before pattern check.
        # A direct-call pattern like `runner._capture_portfolio_snapshot(`
        # in a comment is not a bypass; only code calls matter.
        code_only = re.sub(r"#[^\n]*", "", content)
        code_only = re.sub(r"\"\"\"[\s\S]*?\"\"\"", "", code_only)
        code_only = re.sub(r"'''[\s\S]*?'''", "", code_only)
        if pattern.search(code_only):
            offenders.append(rel_str)

    assert not offenders, (
        "Found direct runner._capture_portfolio_snapshot(...) calls in "
        "production code outside the wrapper.\n"
        "Use capture_snapshot_with_accounting() so accounting failures "
        "follow the live/paper contract uniformly (VIB-3762 §C1).\n"
        "Offenders:\n  - " + "\n  - ".join(offenders)
    )
