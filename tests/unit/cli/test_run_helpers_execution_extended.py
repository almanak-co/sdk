"""Extended unit tests for Phase 4d helpers in `run_helpers.py`.

Focuses on gaps left by `test_run_helpers_execution.py`:
    - _run_once:
        * load_state_async restore (True -> "restored", False -> "fresh")
        * copy trading state load/save exceptions (warnings only)
        * flush_pending_saves exception is caught
    - _run_continuous:
        * TTY branch prints banner
        * load_state_async restore (True + False)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from click.testing import CliRunner

from almanak.framework.cli import run_helpers
from tests.unit.cli.test_run_helpers_execution import (
    _make_fake_runner,
    _make_fake_state_manager,
    _make_fake_strategy,
    _make_success_result,
)

# ---------------------------------------------------------------------------
# _run_once — additional branches
# ---------------------------------------------------------------------------


class TestRunOnceExtras:
    def test_load_state_async_restored_prints_message(self) -> None:
        """load_state_async returning True prints 'Strategy state restored' (2451)."""
        runner = _make_fake_runner(iteration_result=_make_success_result())
        strategy = _make_fake_strategy(with_load_state=True)
        # Make load_state_async return True
        strategy.load_state_async = AsyncMock(return_value=True)
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation() as (out, _err):
            run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=_make_fake_state_manager(),
                cleanup_fn=cleanup,
                teardown_after=False,
            )
        assert "Strategy state restored from persistence" in out.getvalue().decode()

    def test_load_state_async_fresh_prints_message(self) -> None:
        """load_state_async returning False prints 'No previous state found' (2453)."""
        runner = _make_fake_runner(iteration_result=_make_success_result())
        strategy = _make_fake_strategy(with_load_state=True)
        strategy.load_state_async = AsyncMock(return_value=False)
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation() as (out, _err):
            run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=_make_fake_state_manager(),
                cleanup_fn=cleanup,
                teardown_after=False,
            )
        assert "No previous state found" in out.getvalue().decode()

    def test_copy_trading_load_exception_is_logged_not_raised(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """When load_state raises during restore, helper logs warning (2462-2463)."""
        runner = _make_fake_runner(iteration_result=_make_success_result())
        strategy = _make_fake_strategy()
        activity = MagicMock()
        activity.get_state = MagicMock(return_value={})
        activity.set_state = MagicMock()
        strategy._wallet_activity_provider = activity

        state_manager = MagicMock()
        # First call (restore): raises; second call (persist): returns None.
        state_manager.load_state = AsyncMock(side_effect=[RuntimeError("grpc down"), None])
        state_manager.save_state = AsyncMock()
        cleanup = AsyncMock()

        import logging

        cli = CliRunner()
        with cli.isolation(), caplog.at_level(logging.WARNING):
            code = run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=state_manager,
                cleanup_fn=cleanup,
                teardown_after=False,
            )
        assert code == 0
        assert any("restore copy trading" in rec.message.lower() for rec in caplog.records)

    def test_copy_trading_persist_exception_is_logged(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """save_state raising during persist logs warning, does not abort (2522-2523)."""
        runner = _make_fake_runner(iteration_result=_make_success_result())
        strategy = _make_fake_strategy()
        activity = MagicMock()
        activity.get_state = MagicMock(return_value={"cursor": 1})
        activity.set_state = MagicMock()
        strategy._wallet_activity_provider = activity

        state_manager = MagicMock()
        state_manager.load_state = AsyncMock(return_value=None)  # restore returns None, persist creates StateData
        state_manager.save_state = AsyncMock(side_effect=RuntimeError("save failed"))
        cleanup = AsyncMock()

        import logging

        cli = CliRunner()
        with cli.isolation(), caplog.at_level(logging.WARNING):
            code = run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=state_manager,
                cleanup_fn=cleanup,
                teardown_after=False,
            )
        assert code == 0
        assert any("persist copy trading" in rec.message.lower() for rec in caplog.records)

    def test_flush_pending_saves_exception_is_logged(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """flush_pending_saves raising logs warning, does not abort (2530-2531)."""
        runner = _make_fake_runner(iteration_result=_make_success_result())
        strategy = _make_fake_strategy(with_flush=True)
        strategy.flush_pending_saves = AsyncMock(side_effect=RuntimeError("flush blew up"))
        cleanup = AsyncMock()

        import logging

        cli = CliRunner()
        with cli.isolation(), caplog.at_level(logging.WARNING):
            code = run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=_make_fake_state_manager(),
                cleanup_fn=cleanup,
                teardown_after=False,
            )
        assert code == 0
        assert any("flushing pending saves" in rec.message.lower() for rec in caplog.records)

    def test_copy_trading_fresh_state_creates_new_statedata(self) -> None:
        """When load_state returns None on persist, helper constructs a new StateData (2513-2519)."""
        runner = _make_fake_runner(iteration_result=_make_success_result())
        strategy = _make_fake_strategy()
        activity = MagicMock()
        activity.get_state = MagicMock(return_value={"cursor": 99})
        activity.set_state = MagicMock()
        strategy._wallet_activity_provider = activity

        state_manager = MagicMock()
        # Both restore + persist: load_state returns None.
        state_manager.load_state = AsyncMock(return_value=None)
        state_manager.save_state = AsyncMock()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=state_manager,
                cleanup_fn=cleanup,
                teardown_after=False,
            )
        # save_state was called with a newly-constructed StateData.
        saved = state_manager.save_state.await_args.args[0]
        assert saved.state["copy_trading_state"] == {"cursor": 99}
        assert saved.version == 0


# ---------------------------------------------------------------------------
# _run_continuous — additional branches
# ---------------------------------------------------------------------------


class TestRunContinuousExtras:
    def test_isatty_prints_startup_banner(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """sys.stdout.isatty() -> True prints the banner lines (2616-2620).

        We skip CliRunner.isolation() because it replaces sys.stdout with a
        BytesIO stream whose .isatty() is fixed at False. Patching isatty on
        the real sys.stdout is honored by the helper and capsys captures
        the resulting click.echo output.
        """
        runner = _make_fake_runner()
        runner.run_loop = AsyncMock()
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        import sys as _sys

        monkeypatch.setattr(_sys.stdout, "isatty", lambda: True)

        run_helpers._run_continuous(
            runner=runner,
            strategy_instance=strategy,
            cleanup_fn=cleanup,
            interval=30,
            max_iterations=None,
            reset_fork=False,
            managed_gateway=None,
        )
        captured = capsys.readouterr()
        text = captured.out
        assert "Starting continuous execution" in text
        assert "Press Ctrl+C to stop gracefully" in text

    def test_load_state_async_restored_in_continuous(self) -> None:
        """load_state_async=True in run_loop wrapper prints restored message (2650-2651)."""
        runner = _make_fake_runner()
        runner.run_loop = AsyncMock()
        strategy = _make_fake_strategy(with_load_state=True)
        strategy.load_state_async = AsyncMock(return_value=True)
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation() as (out, _err):
            run_helpers._run_continuous(
                runner=runner,
                strategy_instance=strategy,
                cleanup_fn=cleanup,
                interval=30,
                max_iterations=None,
                reset_fork=False,
                managed_gateway=None,
            )
        assert "Strategy state restored from persistence" in out.getvalue().decode()

    def test_load_state_async_fresh_in_continuous(self) -> None:
        """load_state_async=False prints 'No previous state found' (2652-2653)."""
        runner = _make_fake_runner()
        runner.run_loop = AsyncMock()
        strategy = _make_fake_strategy(with_load_state=True)
        strategy.load_state_async = AsyncMock(return_value=False)
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation() as (out, _err):
            run_helpers._run_continuous(
                runner=runner,
                strategy_instance=strategy,
                cleanup_fn=cleanup,
                interval=30,
                max_iterations=None,
                reset_fork=False,
                managed_gateway=None,
            )
        assert "No previous state found" in out.getvalue().decode()

    def test_pre_iteration_callback_echoes_reset_messages(self) -> None:
        """The reset_fork callback prints 'Resetting Anvil fork' + 'Fork reset complete' on success."""
        runner = _make_fake_runner()
        runner.run_loop = AsyncMock()
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        managed = MagicMock()
        managed.reset_anvil_forks = MagicMock(return_value=True)

        cli = CliRunner()
        with cli.isolation() as (out, _err):
            run_helpers._run_continuous(
                runner=runner,
                strategy_instance=strategy,
                cleanup_fn=cleanup,
                interval=30,
                max_iterations=None,
                reset_fork=True,
                managed_gateway=managed,
            )
            # Invoke the captured pre_iteration_cb
            cb = runner.run_loop.await_args.kwargs["pre_iteration_callback"]
            cb()
        text = out.getvalue().decode()
        assert "Resetting Anvil fork" in text
        assert "Fork reset complete" in text
