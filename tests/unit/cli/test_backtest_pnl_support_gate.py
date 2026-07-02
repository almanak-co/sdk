"""CLI regression tests for the Phase 5d support-matrix gate.

This exact code path produced one divergence bug already: the CLI ran the
support matrix unconditionally, so a ``--from-result`` artifact recording
``preflight_validation=false`` (the documented bypass, honored by the engine)
still exited 2 on support hard failures. These tests pin both directions:

- ``preflight_validation=False`` skips the phase entirely (no evaluation,
  no abort);
- a hard-unsupported chain aborts with exit code 2 BEFORE strategy
  instantiation / provider construction, end to end through the real
  ``almanak strat backtest pnl`` command.
"""

from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from almanak.framework.backtesting.pnl.support_matrix import BacktestSupportReport
from almanak.framework.cli.backtest import pnl as pnl_module


def _ctx(preflight_validation: bool) -> SimpleNamespace:
    """Minimal stand-in for PnLBacktestContext: the gate reads one flag."""
    return SimpleNamespace(pnl_config=SimpleNamespace(preflight_validation=preflight_validation))


class TestSupportMatrixPhaseGate:
    """Direct tests for _run_support_matrix_phase's bypass/abort contract."""

    def test_preflight_validation_false_skips_the_phase_entirely(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def _must_not_run(*args: object, **kwargs: object) -> BacktestSupportReport:
            raise AssertionError("support matrix must not be evaluated when preflight_validation=False")

        monkeypatch.setattr(pnl_module, "_evaluate_support_matrix", _must_not_run)

        pnl_module._run_support_matrix_phase(_ctx(preflight_validation=False), {}, None)

    def test_hard_failure_exits_2(self, monkeypatch: pytest.MonkeyPatch) -> None:
        report = BacktestSupportReport(
            chain="solana",
            hard_failures=["chain 'solana' declares no coingecko platform id"],
            recommendations=["Run the backtest on a coingecko-indexed chain."],
        )
        monkeypatch.setattr(pnl_module, "_evaluate_support_matrix", lambda *a, **k: report)

        with pytest.raises(SystemExit) as excinfo:
            pnl_module._run_support_matrix_phase(_ctx(preflight_validation=True), {}, None)

        assert excinfo.value.code == 2

    def test_all_green_report_continues(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            pnl_module,
            "_evaluate_support_matrix",
            lambda *a, **k: BacktestSupportReport(chain="arbitrum"),
        )

        pnl_module._run_support_matrix_phase(_ctx(preflight_validation=True), {}, None)


class TestSupportGateEndToEnd:
    """The real CLI command aborts at the gate for an unpriceable chain."""

    def test_solana_aborts_exit_2_before_any_provider_work(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_STRATEGIES_DIR", "almanak/demo_strategies")

        runner = CliRunner()
        result = runner.invoke(
            pnl_module.pnl_backtest,
            [
                "-s",
                "demo_uniswap_rsi",
                "-c",
                "solana",
                "--tokens",
                "WETH,USDC",
                "--start",
                "2026-05-01",
                "--end",
                "2026-05-03",
            ],
        )

        assert result.exit_code == 2
        assert "BACKTEST ABORTED: UNSUPPORTED CHAIN / PROTOCOL COMBINATION" in result.output
        assert "coingecko platform id" in result.output
