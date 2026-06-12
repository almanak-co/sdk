"""Feature-flag gate for the backtesting CLI surface.

Backtesting is disabled by default behind ``ALMANAK_ENABLE_BACKTESTING``
while VIB-5079 (backtesting v1 completion) is in flight: the PnL engine has
a known value-conservation bug (VIB-5082) that makes results untrustworthy.

Two entry points are gated:

* ``almanak strat backtest <cmd>`` -- the group callback in
  ``almanak/framework/cli/backtest/group.py``.
* ``almanak backtest-service`` -- the command in ``almanak/cli/cli.py``.

The ``_enable_backtesting_feature_flag`` autouse fixture in this package's
conftest sets the flag for the rest of the CLI suite; tests here remove or
override it to exercise the gate itself.
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from almanak.config.backtest import (
    BACKTESTING_FLAG_ENV_VAR,
    backtesting_disabled_message,
    backtesting_enabled,
)
from almanak.framework.cli.backtest import backtest


@pytest.fixture
def _flag_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(BACKTESTING_FLAG_ENV_VAR, raising=False)


class TestBacktestingEnabledHelper:
    @pytest.mark.usefixtures("_flag_unset")
    def test_unset_means_disabled(self) -> None:
        assert backtesting_enabled() is False

    @pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on", " 1 "])
    def test_truthy_values_enable(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv(BACKTESTING_FLAG_ENV_VAR, value)
        assert backtesting_enabled() is True

    @pytest.mark.parametrize("value", ["", "0", "false", "no", "off", "enable"])
    def test_non_truthy_values_disable(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv(BACKTESTING_FLAG_ENV_VAR, value)
        assert backtesting_enabled() is False


@pytest.mark.usefixtures("_flag_unset")
class TestBacktestGroupGate:
    def test_subcommand_blocked_when_flag_unset(self) -> None:
        runner = CliRunner()
        result = runner.invoke(backtest, ["pnl", "--list-strategies"])
        assert result.exit_code != 0
        assert backtesting_disabled_message() in result.output
        assert BACKTESTING_FLAG_ENV_VAR in result.output

    def test_group_help_still_visible_when_flag_unset(self) -> None:
        # Discoverability: bare --help on the group must keep working so the
        # operator can see the commands and the EXPERIMENTAL notice.
        runner = CliRunner()
        result = runner.invoke(backtest, ["--help"])
        assert result.exit_code == 0
        assert "EXPERIMENTAL" in result.output
        assert BACKTESTING_FLAG_ENV_VAR in result.output

    def test_subcommand_passes_gate_when_flag_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(BACKTESTING_FLAG_ENV_VAR, "1")
        runner = CliRunner()
        result = runner.invoke(backtest, ["pnl", "--help"])
        assert result.exit_code == 0
        assert backtesting_disabled_message() not in result.output


@pytest.mark.usefixtures("_flag_unset")
class TestBacktestServiceGate:
    def test_service_blocked_when_flag_unset(self) -> None:
        from almanak.cli.cli import almanak as almanak_cli

        runner = CliRunner()
        result = runner.invoke(almanak_cli, ["backtest-service"])
        assert result.exit_code != 0
        assert backtesting_disabled_message() in result.output

    def test_service_starts_when_flag_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(BACKTESTING_FLAG_ENV_VAR, "1")

        # The real server module needs uvicorn (service-only extra); stub the
        # module so the test covers the gate + passthrough, not uvicorn boot.
        import sys
        import types

        from almanak.cli.cli import almanak as almanak_cli

        calls: list[dict] = []
        fake_server = types.ModuleType("almanak.services.backtest.server")
        fake_server.run_server = lambda **kwargs: calls.append(kwargs)  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "almanak.services.backtest.server", fake_server)

        runner = CliRunner()
        result = runner.invoke(almanak_cli, ["backtest-service", "--port", "9123"])
        assert result.exit_code == 0
        assert len(calls) == 1
        assert calls[0]["port"] == 9123
