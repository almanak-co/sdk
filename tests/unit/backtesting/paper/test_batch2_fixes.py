"""Tests for Paper Trading Batch 2 fixes.

VIB-1951: on_intent_executed callback parity
VIB-1952: Receipt parsing diagnostics
VIB-1953: paper resume CLI command
VIB-1954: force_action config guard warning
"""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


class TestCallbackParity:
    """VIB-1951: on_intent_executed must be called after intent execution."""

    def test_notify_strategy_callback_success(self):
        """Callback fires with success=True when trade succeeds."""
        from almanak.framework.backtesting.paper.engine import PaperTrader

        trader = PaperTrader.__new__(PaperTrader)
        trader._backtest_id = "test"
        trader._last_execution_result = SimpleNamespace(success=True, error=None)

        strategy = MagicMock()
        strategy.on_intent_executed = MagicMock()

        trade = MagicMock()  # Non-None = success

        trader._notify_strategy_callback(strategy, MagicMock(), trade)

        strategy.on_intent_executed.assert_called_once()
        call_args = strategy.on_intent_executed.call_args
        assert call_args[1]["success"] is True
        assert call_args[1]["result"] is trader._last_execution_result

    def test_notify_strategy_callback_failure(self):
        """Callback fires with success=False when trade fails."""
        from almanak.framework.backtesting.paper.engine import PaperTrader

        trader = PaperTrader.__new__(PaperTrader)
        trader._backtest_id = "test"
        trader._last_execution_result = SimpleNamespace(success=False, error="Simulation failed")

        strategy = MagicMock()
        strategy.on_intent_executed = MagicMock()

        trader._notify_strategy_callback(strategy, MagicMock(), None)  # None = failure

        strategy.on_intent_executed.assert_called_once()
        call_args = strategy.on_intent_executed.call_args
        assert call_args[1]["success"] is False

    def test_notify_strategy_callback_no_execution_result(self):
        """Callback uses SimpleNamespace fallback when no ExecutionResult available."""
        from almanak.framework.backtesting.paper.engine import PaperTrader

        trader = PaperTrader.__new__(PaperTrader)
        trader._backtest_id = "test"
        trader._last_execution_result = None  # Compilation failure

        strategy = MagicMock()
        strategy.on_intent_executed = MagicMock()

        trader._notify_strategy_callback(strategy, MagicMock(), None)

        strategy.on_intent_executed.assert_called_once()
        call_args = strategy.on_intent_executed.call_args
        result = call_args[1]["result"]
        assert hasattr(result, "error")
        assert "compilation failed" in result.error.lower()

    def test_notify_strategy_callback_no_method(self):
        """No crash when strategy doesn't have on_intent_executed."""
        from almanak.framework.backtesting.paper.engine import PaperTrader

        trader = PaperTrader.__new__(PaperTrader)
        trader._backtest_id = "test"
        trader._last_execution_result = None

        strategy = MagicMock(spec=[])  # No on_intent_executed

        # Should not raise
        trader._notify_strategy_callback(strategy, MagicMock(), None)

    def test_notify_strategy_callback_exception_caught(self):
        """Callback exceptions are caught and logged, not propagated."""
        from almanak.framework.backtesting.paper.engine import PaperTrader

        trader = PaperTrader.__new__(PaperTrader)
        trader._backtest_id = "test"
        trader._last_execution_result = SimpleNamespace(success=True)

        strategy = MagicMock()
        strategy.on_intent_executed = MagicMock(side_effect=RuntimeError("Strategy bug"))

        # Should not raise
        trader._notify_strategy_callback(strategy, MagicMock(), MagicMock())


class TestForceActionGuard:
    """VIB-1954: force_action config guard warning."""

    def test_force_action_warning_emitted(self):
        """paper start warns when force_action is in config."""
        from click.testing import CliRunner

        from almanak.framework.cli.backtest import paper_start

        runner = CliRunner(mix_stderr=False)

        with (
            patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test_strat"]),
            patch("almanak.framework.cli.backtest.load_strategy_config", return_value={"force_action": "supply"}),
            patch("almanak.framework.cli.backtest._create_backtest_strategy"),
            patch("almanak.framework.cli.backtest.get_strategy"),
            patch.dict("os.environ", {"ALMANAK_ARBITRUM_RPC_URL": "http://test"}),
        ):
            result = runner.invoke(paper_start, ["-s", "test_strat", "--dry-run"])

        combined = result.output + (result.stderr or "")
        assert "force_action" in combined

    def test_no_warning_without_force_action(self):
        """No warning when force_action is not in config."""
        from click.testing import CliRunner

        from almanak.framework.cli.backtest import paper_start

        runner = CliRunner(mix_stderr=False)

        with (
            patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test_strat"]),
            patch("almanak.framework.cli.backtest.load_strategy_config", return_value={}),
            patch("almanak.framework.cli.backtest._create_backtest_strategy"),
            patch("almanak.framework.cli.backtest.get_strategy"),
            patch.dict("os.environ", {"ALMANAK_ARBITRUM_RPC_URL": "http://test"}),
        ):
            result = runner.invoke(paper_start, ["-s", "test_strat", "--dry-run"])

        combined = result.output + (result.stderr or "")
        assert "force_action" not in combined


def _make_state(strategy_id="test_strat", status="stopped", tick_count=10, pid=None):
    """Helper to create a PaperTraderState for testing."""
    from almanak.framework.backtesting.paper.background import PaperTraderState

    return PaperTraderState(
        strategy_id=strategy_id,
        session_start=datetime(2026, 3, 26, tzinfo=UTC),
        last_save=datetime(2026, 3, 26, 1, 0, tzinfo=UTC),
        tick_count=tick_count,
        trades=[],
        errors=[],
        current_balances={"ETH": Decimal("10")},
        initial_balances={"ETH": Decimal("10")},
        equity_curve=[],
        config={
            "chain": "arbitrum",
            "rpc_url": "***masked***",
            "tick_interval_seconds": 60,
            "max_ticks": 100,
            "initial_eth": "10",
            "initial_tokens": {},
            "anvil_port": 8546,
            "reset_fork_every_tick": True,
        },
        pid=pid,
        status=status,
    )


class TestPaperResume:
    """VIB-1953: paper resume CLI command."""

    def test_resume_happy_path(self, tmp_path):
        """Resume successfully starts background process and saves state."""
        from click.testing import CliRunner

        from almanak.framework.cli.backtest import paper_resume

        state = _make_state(status="stopped", tick_count=10)
        state_file = tmp_path / "test_strat.state.json"
        state.save(state_file)

        runner = CliRunner(mix_stderr=False)

        with (
            patch("almanak.framework.cli.backtest.BackgroundPaperTrader") as mock_bg,
            patch("almanak.framework.cli.backtest.PaperTraderState.load", return_value=state),
            patch("almanak.framework.cli.backtest.get_strategy", return_value=type("S", (), {})),
            patch("almanak.framework.cli.backtest.load_strategy_config", return_value={}),
            patch("almanak.framework.cli.backtest.update_paper_session_status"),
            patch("almanak.framework.cli.backtest.save_paper_session_state") as mock_save,
            patch.dict("os.environ", {"ALMANAK_ARBITRUM_RPC_URL": "http://test-rpc"}),
        ):
            mock_instance = MagicMock()
            mock_instance.state_file = state_file
            mock_instance.pid_file_path = tmp_path / "test_strat.pid"
            mock_instance.resume.return_value = 12345
            mock_bg.return_value = mock_instance

            result = runner.invoke(paper_resume, ["-s", "test_strat"])

        assert result.exit_code == 0, result.output
        assert "12345" in result.output
        mock_save.assert_called_once()
        call_kwargs = mock_save.call_args[1]
        assert call_kwargs["pid"] == 12345
        assert call_kwargs["strategy_id"] == "test_strat"

    def test_resume_dead_process_resets_status(self, tmp_path):
        """Dead process detected -> status reset to stopped and PID cleaned up."""
        from click.testing import CliRunner

        from almanak.framework.cli.backtest import paper_resume

        state = _make_state(status="running", pid=99999)
        state_file = tmp_path / "test_strat.state.json"
        state.save(state_file)

        runner = CliRunner(mix_stderr=False)

        with (
            patch("almanak.framework.cli.backtest.BackgroundPaperTrader") as mock_bg,
            patch("almanak.framework.cli.backtest.PaperTraderState.load", return_value=state),
            patch("almanak.framework.cli.backtest.is_process_running", return_value=False),
            patch("almanak.framework.cli.backtest.PIDFile") as mock_pidfile,
            patch("almanak.framework.cli.backtest.get_strategy", return_value=type("S", (), {})),
            patch("almanak.framework.cli.backtest.load_strategy_config", return_value={}),
            patch("almanak.framework.cli.backtest.update_paper_session_status"),
            patch("almanak.framework.cli.backtest.save_paper_session_state"),
            patch.dict("os.environ", {"ALMANAK_ARBITRUM_RPC_URL": "http://test-rpc"}),
        ):
            mock_instance = MagicMock()
            mock_instance.state_file = state_file
            mock_instance.pid_file_path = tmp_path / "test_strat.pid"
            mock_instance.resume.return_value = 55555
            mock_bg.return_value = mock_instance

            result = runner.invoke(paper_resume, ["-s", "test_strat"])

        assert result.exit_code == 0, result.output
        assert "no longer running" in result.output
        mock_pidfile.return_value.release.assert_called_once()

    def test_resume_masked_rpc_resolved_from_env(self, tmp_path):
        """Masked RPC URL is resolved from environment variable."""
        from click.testing import CliRunner

        from almanak.framework.cli.backtest import paper_resume

        state = _make_state(status="stopped")
        state_file = tmp_path / "test_strat.state.json"
        state.save(state_file)

        runner = CliRunner(mix_stderr=False)

        with (
            patch("almanak.framework.cli.backtest.BackgroundPaperTrader") as mock_bg,
            patch("almanak.framework.cli.backtest.PaperTraderState.load", return_value=state),
            patch("almanak.framework.cli.backtest.get_strategy", return_value=type("S", (), {})),
            patch("almanak.framework.cli.backtest.load_strategy_config", return_value={}),
            patch("almanak.framework.cli.backtest.update_paper_session_status"),
            patch("almanak.framework.cli.backtest.save_paper_session_state"),
            patch.dict("os.environ", {"ALMANAK_ARBITRUM_RPC_URL": "http://my-rpc"}, clear=False),
        ):
            mock_instance = MagicMock()
            mock_instance.state_file = state_file
            mock_instance.pid_file_path = tmp_path / "test_strat.pid"
            mock_instance.resume.return_value = 111
            mock_bg.return_value = mock_instance

            result = runner.invoke(paper_resume, ["-s", "test_strat"])

        assert result.exit_code == 0, result.output
        # Config should use the env var RPC URL
        config_arg = mock_bg.call_args[1]["config"]
        assert config_arg.rpc_url == "http://my-rpc"

    def test_resume_no_rpc_env_var_aborts(self, tmp_path):
        """Missing RPC URL env var aborts with clear error."""
        from click.testing import CliRunner

        from almanak.framework.cli.backtest import paper_resume

        state = _make_state(status="stopped")
        state_file = tmp_path / "test_strat.state.json"
        state.save(state_file)

        runner = CliRunner(mix_stderr=False)

        with (
            patch("almanak.framework.cli.backtest.BackgroundPaperTrader") as mock_bg,
            patch("almanak.framework.cli.backtest.PaperTraderState.load", return_value=state),
            patch.dict("os.environ", {}, clear=True),
        ):
            mock_instance = MagicMock()
            mock_instance.state_file = state_file
            mock_bg.return_value = mock_instance

            result = runner.invoke(paper_resume, ["-s", "test_strat"])

        assert result.exit_code != 0

    def test_resume_duration_extends_max_ticks(self, tmp_path):
        """--duration flag extends max_ticks from current count."""
        from click.testing import CliRunner

        from almanak.framework.cli.backtest import paper_resume

        state = _make_state(status="stopped", tick_count=50)
        state_file = tmp_path / "test_strat.state.json"
        state.save(state_file)

        runner = CliRunner(mix_stderr=False)

        with (
            patch("almanak.framework.cli.backtest.BackgroundPaperTrader") as mock_bg,
            patch("almanak.framework.cli.backtest.PaperTraderState.load", return_value=state),
            patch("almanak.framework.cli.backtest.get_strategy", return_value=type("S", (), {})),
            patch("almanak.framework.cli.backtest.load_strategy_config", return_value={}),
            patch("almanak.framework.cli.backtest.update_paper_session_status"),
            patch("almanak.framework.cli.backtest.save_paper_session_state"),
            patch.dict("os.environ", {"ALMANAK_ARBITRUM_RPC_URL": "http://test-rpc"}),
        ):
            mock_instance = MagicMock()
            mock_instance.state_file = state_file
            mock_instance.pid_file_path = tmp_path / "test_strat.pid"
            mock_instance.resume.return_value = 222
            mock_bg.return_value = mock_instance

            result = runner.invoke(paper_resume, ["-s", "test_strat", "--duration", "1h"])

        assert result.exit_code == 0, result.output
        # 1h = 3600s / 60s interval = 60 ticks + 1 = 61, total = 50 + 61 = 111
        config_arg = mock_bg.call_args[1]["config"]
        assert config_arg.max_ticks == 111

    def test_resume_mutually_exclusive_flags(self):
        """--duration and --max-ticks together is an error."""
        from click.testing import CliRunner

        from almanak.framework.cli.backtest import paper_resume

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(paper_resume, ["-s", "test", "--duration", "1h", "--max-ticks", "100"])

        assert result.exit_code != 0
