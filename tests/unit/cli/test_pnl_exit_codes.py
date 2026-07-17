"""Exit-code contract tests for `almanak strat backtest pnl`.

The PnL engine's `BacktestErrorHandler` captures fatal simulation errors
(e.g. the LP adapter's missing-volume `NoAcceptableDataSourceError`) and
returns a partial `BacktestResult` with `error` set instead of raising
(`build_error_result`, pnl/_engine_helpers.py). Before this contract the
CLI printed the (empty) results block and exited 0, so scripts/CI could
not detect the failure without parsing the JSON output.

Contract under test:

- exit 0: backtest completed (`result.error is None`); recoverable errors
  in `result.errors` alone do not flip the exit code.
- exit 1: result carries a fatal error -- the results block and the JSON
  output file are still produced before exiting.
- exit 2: click usage errors (unchanged, asserted as a guardrail).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner

from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
    EquityPoint,
)
from almanak.framework.cli.backtest import backtest
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_result(*, error: str | None = None, errors: list[dict[str, Any]] | None = None) -> BacktestResult:
    return BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id="demo",
        start_time=datetime(2025, 11, 1, tzinfo=UTC),
        end_time=datetime(2025, 11, 15, tzinfo=UTC),
        metrics=BacktestMetrics(),
        equity_curve=[
            EquityPoint(
                timestamp=datetime(2025, 11, 1, i, tzinfo=UTC),
                value_usd=Decimal("10000"),
            )
            for i in range(3)
        ],
        error=error,
        errors=errors or [],
    )


class _DummyStrategy:
    """Minimal non-IntentStrategy class for the signature ladder."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

    def decide(self, market: Any) -> None:
        return None


def _strategy_config(deployment_id: str = "dummy_strategy") -> dict[str, Any]:
    return {
        "deployment_id": deployment_id,
        "token_funding": _pnl_token_funding(Decimal("10000"), chain="arbitrum"),
    }


def _invoke_pnl(cli_runner: CliRunner, result: BacktestResult, extra_args: list[str] | None = None) -> Any:
    """Invoke `backtest pnl` with heavy phases mocked; backtester returns `result`."""
    base_args = [
        "pnl",
        "-s",
        "dummy_strategy",
        "--start",
        "2025-11-01",
        "--end",
        "2025-11-15",
        "--chain",
        "arbitrum",
    ]
    with (
        patch("almanak.framework.cli.backtest.pnl.validate_strategy_is_registered"),
        patch(
            "almanak.framework.cli.backtest.pnl.get_strategy",
            return_value=_DummyStrategy,
        ),
        patch(
            "almanak.framework.cli.backtest.pnl.load_strategy_config",
            return_value=_strategy_config(),
        ),
        patch("almanak.framework.cli.backtest.pnl.CoinGeckoDataProvider"),
        patch("almanak.framework.cli.backtest.pnl._print_benchmark_comparison"),
        patch("almanak.framework.cli.backtest.pnl.PnLBacktester") as mock_backtester,
    ):
        mock_backtester.return_value.backtest = AsyncMock(return_value=result)
        return cli_runner.invoke(backtest, base_args + (extra_args or []))


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


# ===========================================================================
# Exit codes
# ===========================================================================


class TestPnLExitCodes:
    def test_successful_result_exits_zero(self, cli_runner: CliRunner) -> None:
        result = _invoke_pnl(cli_runner, _make_result())

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "BACKTEST RESULTS" in result.output
        # The post-backtest tip only renders on the success path.
        assert "Tip: Try 'almanak backtest sweep'" in result.output

    def test_fatal_result_exits_one(self, cli_runner: CliRunner) -> None:
        """Engine-captured fatal error (result.error set) -> exit 1."""
        failed = _make_result(error="No acceptable volume data source for 'position:LP_TEST'")

        result = _invoke_pnl(cli_runner, failed)

        assert result.exit_code == 1
        # Results block is still printed before exiting (diagnostics contract).
        assert "BACKTEST RESULTS" in result.output
        assert "Error: backtest failed:" in result.output
        # No misleading next-steps tip after a failure.
        assert "Tip: Try 'almanak backtest sweep'" not in result.output

    def test_recoverable_errors_alone_keep_exit_zero(self, cli_runner: CliRunner) -> None:
        """`result.errors` records handled errors; only fatal `result.error` fails the run."""
        recovered = _make_result(
            errors=[
                {
                    "error_type": "DataUnavailableError",
                    "error_message": "gap interpolated",
                    "classification": {"is_recoverable": True},
                }
            ]
        )

        result = _invoke_pnl(cli_runner, recovered)

        assert result.exit_code == 0, f"CLI failed: {result.output}"

    def test_fatal_result_still_writes_json_output(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """`-o` artifact is written before the non-zero exit -- scripts keep the detail."""
        failed = _make_result(error="simulation stopped: fatal data error")
        output_path = tmp_path / "result.json"

        result = _invoke_pnl(cli_runner, failed, extra_args=["-o", str(output_path)])

        assert result.exit_code == 1
        assert output_path.exists()
        payload = json.loads(output_path.read_text())
        assert payload["error"] == "simulation stopped: fatal data error"

    def test_engine_raise_still_exits_one(self, cli_runner: CliRunner) -> None:
        """The pre-existing raise path keeps its exit code (same verdict, same code)."""
        base_args = [
            "pnl",
            "-s",
            "dummy_strategy",
            "--start",
            "2025-11-01",
            "--end",
            "2025-11-15",
        ]
        with (
            patch("almanak.framework.cli.backtest.pnl.validate_strategy_is_registered"),
            patch(
                "almanak.framework.cli.backtest.pnl.get_strategy",
                return_value=_DummyStrategy,
            ),
            patch(
                "almanak.framework.cli.backtest.pnl.load_strategy_config",
                return_value=_strategy_config(),
            ),
            patch("almanak.framework.cli.backtest.pnl.CoinGeckoDataProvider"),
            patch("almanak.framework.cli.backtest.pnl.PnLBacktester") as mock_backtester,
        ):
            mock_backtester.return_value.backtest = AsyncMock(side_effect=RuntimeError("boom"))
            result = cli_runner.invoke(backtest, base_args)

        assert result.exit_code == 1
        assert "Error running backtest: boom" in result.output

    def test_usage_error_exits_two(self, cli_runner: CliRunner) -> None:
        """Click usage errors keep their distinct code (guardrail for the contract doc)."""
        result = cli_runner.invoke(backtest, ["pnl", "--no-such-flag"])
        assert result.exit_code == 2
