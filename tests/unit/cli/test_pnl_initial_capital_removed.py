import pytest
from click.testing import CliRunner

from almanak.framework.cli.backtest.advanced import (
    monte_carlo_backtest,
    scenario_backtest,
    walk_forward_backtest,
)
from almanak.framework.cli.backtest.pnl import pnl_backtest
from almanak.framework.cli.backtest.sweep import optimize_backtest, sweep_backtest


@pytest.mark.parametrize(
    "command",
    [
        pnl_backtest,
        sweep_backtest,
        optimize_backtest,
        walk_forward_backtest,
        monte_carlo_backtest,
        scenario_backtest,
    ],
)
def test_pnl_cli_rejects_initial_capital_option(command) -> None:
    result = CliRunner().invoke(command, ["--initial-capital", "10000"])

    assert result.exit_code == 2
    assert "No such option: --initial-capital" in result.output
