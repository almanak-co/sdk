"""Tests for the LP volume-source flags on `almanak backtest pnl`.

The PnL engine's LP adapter refuses to fabricate pool volume by default
(VIB-4849): with no acceptable volume source it raises
`DataSourceUnavailableError` instead of inventing a number. Before these
flags existed the CLI exposed no way to provide a source, so no LP PnL
backtest could complete through the CLI. Covered contracts:

- `_build_volume_data_config`: returns None when no flag is passed (the
  backtester keeps its historical no-data_config behaviour), maps each flag
  onto `BacktestDataConfig`, and emits the LOW-confidence warning when
  `--allow-volume-fallback` is used.
- `_run_backtest`: a missing-volume `DataSourceUnavailableError` keeps the
  grep-asserted `"Error running backtest: ..."` line and adds a hint naming
  the new flags.
- `pnl_backtest` wiring: the built config reaches `PnLBacktester(data_config=...)`;
  refuse-to-fabricate stays the default (data_config=None without flags).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from almanak.framework.backtesting import PnLBacktestConfig
from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.exceptions import DataSourceUnavailableError
from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
    EquityPoint,
)
from almanak.framework.cli.backtest import backtest
from almanak.framework.cli.backtest.pnl import (
    _build_volume_data_config,
    _emit_missing_volume_hint_for_result,
    _run_backtest,
)
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pnl_config() -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=datetime(2025, 11, 1, tzinfo=UTC),
        end_time=datetime(2025, 11, 15, tzinfo=UTC),
        interval_seconds=3600,
        token_funding=_pnl_token_funding(Decimal("10000"), chain="arbitrum"),
        chain="arbitrum",
        tokens=["WETH", "USDC"],
    )


def _make_result() -> BacktestResult:
    return BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id="demo",
        start_time=datetime(2025, 11, 1, tzinfo=UTC),
        end_time=datetime(2025, 11, 15, tzinfo=UTC),
        metrics=BacktestMetrics(
            total_trades=2,
            win_rate=Decimal("0.5"),
            total_return_pct=Decimal("1.0"),
            max_drawdown_pct=Decimal("1"),
            sharpe_ratio=Decimal("1"),
            sortino_ratio=Decimal("1"),
            calmar_ratio=Decimal("1"),
            profit_factor=Decimal("1"),
            annualized_return_pct=Decimal("10"),
            net_pnl_usd=Decimal("100"),
        ),
        equity_curve=[
            EquityPoint(
                timestamp=datetime(2025, 11, 1, i, tzinfo=UTC),
                value_usd=Decimal("10000") + Decimal(i),
            )
            for i in range(3)
        ],
    )


class _DummyStrategy:
    """Minimal non-IntentStrategy class for the signature ladder."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

    def decide(self, market: Any) -> None:
        return None


def _strategy_config(deployment_id: str = "dummy_lp_strategy") -> dict[str, Any]:
    return {
        "deployment_id": deployment_id,
        "token_funding": _pnl_token_funding(Decimal("10000"), chain="arbitrum"),
    }


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


# ===========================================================================
# _build_volume_data_config
# ===========================================================================


class TestBuildVolumeDataConfig:
    def test_no_flags_returns_none(self) -> None:
        """No flag passed -> no data_config -> historical behaviour preserved."""
        assert (
            _build_volume_data_config(
                historical_volume=None,
                pool_volume_usd_daily=None,
                pool_liquidity_usd=None,
                allow_volume_fallback=False,
            )
            is None
        )

    def test_explicit_volume_maps_to_decimal_field(self) -> None:
        config = _build_volume_data_config(
            historical_volume=None,
            pool_volume_usd_daily=5_000_000.0,
            pool_liquidity_usd=None,
            allow_volume_fallback=False,
        )
        assert isinstance(config, BacktestDataConfig)
        assert config.explicit_pool_volume_usd_daily == Decimal("5000000.0")
        assert config.explicit_pool_liquidity_usd is None
        assert config.allow_volume_fallback is False

    def test_zero_explicit_volume_is_a_measured_zero(self) -> None:
        """Empty != Zero: 0 builds a config with a measured-zero volume."""
        config = _build_volume_data_config(
            historical_volume=None,
            pool_volume_usd_daily=0.0,
            pool_liquidity_usd=None,
            allow_volume_fallback=False,
        )
        assert config is not None
        assert config.explicit_pool_volume_usd_daily == Decimal("0.0")

    def test_explicit_liquidity_maps_to_decimal_field(self) -> None:
        config = _build_volume_data_config(
            historical_volume=None,
            pool_volume_usd_daily=None,
            pool_liquidity_usd=2_000_000.0,
            allow_volume_fallback=False,
        )
        assert config is not None
        assert config.explicit_pool_liquidity_usd == Decimal("2000000.0")

    def test_historical_volume_tristate(self) -> None:
        """None leaves the engine default; True/False override explicitly."""
        enabled = _build_volume_data_config(
            historical_volume=True,
            pool_volume_usd_daily=None,
            pool_liquidity_usd=None,
            allow_volume_fallback=False,
        )
        disabled = _build_volume_data_config(
            historical_volume=False,
            pool_volume_usd_daily=None,
            pool_liquidity_usd=None,
            allow_volume_fallback=False,
        )
        assert enabled is not None and enabled.use_historical_volume is True
        assert disabled is not None and disabled.use_historical_volume is False

    def test_allow_volume_fallback_sets_flag_and_warns(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config = _build_volume_data_config(
            historical_volume=None,
            pool_volume_usd_daily=None,
            pool_liquidity_usd=None,
            allow_volume_fallback=True,
        )
        assert config is not None
        assert config.allow_volume_fallback is True

        captured = capsys.readouterr()
        assert "LOW-confidence" in captured.err
        assert "order of magnitude" in captured.err

    def test_no_warning_without_fallback_optin(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _build_volume_data_config(
            historical_volume=None,
            pool_volume_usd_daily=1000.0,
            pool_liquidity_usd=None,
            allow_volume_fallback=False,
        )
        assert capsys.readouterr().err == ""

    def test_echoes_chosen_volume_sources(self, capsys: pytest.CaptureFixture[str]) -> None:
        """The run record must show where LP fee numbers came from."""
        _build_volume_data_config(
            historical_volume=False,
            pool_volume_usd_daily=5_000_000.0,
            pool_liquidity_usd=2_000_000.0,
            allow_volume_fallback=False,
        )
        out = capsys.readouterr().out
        assert "LP Historical Volume: disabled" in out
        assert "LP Pool Volume (explicit): $5,000,000.00/day" in out
        assert "LP Pool Liquidity (explicit): $2,000,000.00" in out


# ===========================================================================
# _run_backtest missing-volume hint
# ===========================================================================


class TestRunBacktestVolumeHint:
    def _volume_error(self) -> DataSourceUnavailableError:
        return DataSourceUnavailableError(
            data_type="volume",
            identifier="position:LP_TEST",
            remediation="set use_historical_volume=True ...",
        )

    def test_missing_volume_error_adds_flag_hint(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        backtester = MagicMock()
        backtester.backtest = AsyncMock(side_effect=self._volume_error())

        with pytest.raises(SystemExit) as exc_info:
            _run_backtest(backtester, MagicMock(), _make_pnl_config())

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        # Grep-asserted line preserved.
        assert "Error running backtest:" in captured.err
        assert "No acceptable volume data source" in captured.err
        # And the hint names the CLI flags that resolve it.
        assert "--pool-volume-usd-daily" in captured.err
        assert "--pool-liquidity-usd" in captured.err
        assert "--allow-volume-fallback" in captured.err
        assert "--historical-volume" in captured.err

    def test_non_volume_data_source_error_gets_no_volume_hint(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        backtester = MagicMock()
        backtester.backtest = AsyncMock(
            side_effect=DataSourceUnavailableError(
                data_type="funding",
                identifier="perp:TEST",
                remediation="provide funding data",
            )
        )

        with pytest.raises(SystemExit):
            _run_backtest(backtester, MagicMock(), _make_pnl_config())

        captured = capsys.readouterr()
        assert "Error running backtest:" in captured.err
        assert "--pool-volume-usd-daily" not in captured.err


# ===========================================================================
# _emit_missing_volume_hint_for_result
# ===========================================================================


class TestEmitMissingVolumeHintForResult:
    """The engine's error handler captures the fail-loud into the result
    instead of raising; the CLI must still point at the flags."""

    def _volume_error_message(self) -> str:
        return (
            "No acceptable volume data source for 'position:LP_TEST' and "
            "refusing to fabricate a value. To proceed: ..."
        )

    def test_hint_emitted_for_result_carried_volume_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        result = _make_result()
        result.error = self._volume_error_message()
        result.errors = [
            {
                "error_type": "DataSourceUnavailableError",
                "error_message": self._volume_error_message(),
            }
        ]

        _emit_missing_volume_hint_for_result(result)

        captured = capsys.readouterr()
        assert "stopped early" in captured.err
        assert "--pool-volume-usd-daily" in captured.err
        assert "--allow-volume-fallback" in captured.err

    def test_silent_for_clean_result(self, capsys: pytest.CaptureFixture[str]) -> None:
        _emit_missing_volume_hint_for_result(_make_result())
        assert capsys.readouterr().err == ""

    def test_silent_for_non_volume_error(self, capsys: pytest.CaptureFixture[str]) -> None:
        result = _make_result()
        result.error = "No acceptable funding data source for 'perp:TEST'"
        result.errors = [
            {
                "error_type": "DataSourceUnavailableError",
                "error_message": "No acceptable funding data source for 'perp:TEST'",
            }
        ]

        _emit_missing_volume_hint_for_result(result)
        assert capsys.readouterr().err == ""

    def test_cli_run_with_result_carried_volume_error_shows_hint(
        self, cli_runner: CliRunner
    ) -> None:
        """End-to-end through `pnl_backtest`: partial result -> hint in output."""
        failed = _make_result()
        failed.error = self._volume_error_message()
        failed.errors = [
            {
                "error_type": "DataSourceUnavailableError",
                "error_message": self._volume_error_message(),
            }
        ]

        base_args = [
            "pnl",
            "-s",
            "dummy_lp_strategy",
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
            mock_backtester.return_value.backtest = AsyncMock(return_value=failed)
            result = cli_runner.invoke(backtest, base_args)

        assert "--pool-volume-usd-daily" in result.output
        assert "--allow-volume-fallback" in result.output


# ===========================================================================
# pnl_backtest wiring
# ===========================================================================


def _invoke_pnl(cli_runner: CliRunner, extra_args: list[str]) -> tuple[Any, MagicMock]:
    """Invoke `backtest pnl` with heavy phases mocked; return (result, PnLBacktester mock)."""
    base_args = [
        "pnl",
        "-s",
        "dummy_lp_strategy",
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
        mock_backtester.return_value.backtest = AsyncMock(return_value=_make_result())
        result = cli_runner.invoke(backtest, base_args + extra_args)
    return result, mock_backtester


class TestPnLBacktestVolumeWiring:
    def test_explicit_volume_flags_reach_backtester_data_config(
        self, cli_runner: CliRunner
    ) -> None:
        result, mock_backtester = _invoke_pnl(
            cli_runner,
            ["--pool-volume-usd-daily", "5000000", "--pool-liquidity-usd", "2000000"],
        )

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        data_config = mock_backtester.call_args.kwargs["data_config"]
        assert isinstance(data_config, BacktestDataConfig)
        assert data_config.explicit_pool_volume_usd_daily == Decimal("5000000.0")
        assert data_config.explicit_pool_liquidity_usd == Decimal("2000000.0")
        assert data_config.allow_volume_fallback is False

    def test_allow_volume_fallback_flag_reaches_backtester(
        self, cli_runner: CliRunner
    ) -> None:
        result, mock_backtester = _invoke_pnl(cli_runner, ["--allow-volume-fallback"])

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        data_config = mock_backtester.call_args.kwargs["data_config"]
        assert data_config.allow_volume_fallback is True

    def test_no_volume_flags_keeps_data_config_none(self, cli_runner: CliRunner) -> None:
        """Refuse-to-fabricate stays the default: no flags -> data_config=None."""
        result, mock_backtester = _invoke_pnl(cli_runner, [])

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert mock_backtester.call_args.kwargs["data_config"] is None

    def test_negative_pool_volume_rejected_by_click(self, cli_runner: CliRunner) -> None:
        result, _ = _invoke_pnl(cli_runner, ["--pool-volume-usd-daily", "-1"])
        assert result.exit_code != 0

    def test_zero_pool_liquidity_rejected_by_click(self, cli_runner: CliRunner) -> None:
        """TVL must be strictly positive (it is a share denominator)."""
        result, _ = _invoke_pnl(cli_runner, ["--pool-liquidity-usd", "0"])
        assert result.exit_code != 0

    def test_dry_run_shows_volume_source_lines(self, cli_runner: CliRunner) -> None:
        """Flag echo + warning fire on the dry-run path too."""
        result, _ = _invoke_pnl(
            cli_runner,
            ["--pool-volume-usd-daily", "5000000", "--allow-volume-fallback", "--dry-run"],
        )

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "LP Pool Volume (explicit): $5,000,000.00/day" in result.output
        assert "LP Volume Fallback: enabled (LOW confidence)" in result.output
