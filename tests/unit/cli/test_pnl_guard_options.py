import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from click.testing import CliRunner

from almanak.framework.backtesting.pnl.config_loader import load_config_from_result
from almanak.framework.backtesting.pnl.dependencies import check_declared_dependencies
from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError
from almanak.framework.cli.backtest.pnl import _run_backtest
from tests.unit.backtesting.pnl.test_historical_depth_contract import _DepthStrategy
from tests.unit.cli.test_pnl_helpers_execution import _make_pnl_config
from tests.unit.cli.test_pnl_volume_options import _invoke_pnl, _strategy_config


def test_config_file_guard_variant_reaches_backtest_and_is_labeled(tmp_path):
    guards = {"entry_depth": "Evaluate the signal without the live tick-depth guard"}
    path = tmp_path / "variant.json"
    path.write_text(json.dumps({**_strategy_config(), "altered_backtest_guards": guards}))
    result, backend = _invoke_pnl(CliRunner(), ["--config-file", str(path)])
    assert result.exit_code == 0, result.output
    config = backend.return_value.backtest.await_args.args[1]
    assert config.altered_backtest_guards == guards
    assert config.to_dict()["strategy_comparison"] == "altered_guards_not_live_equivalent"
    assert "altered_guards_not_live_equivalent" in result.output
    assert guards["entry_depth"] in result.output


@pytest.mark.parametrize("guards", [{"entry_depth": ""}, {"entry_depth": "  "}, {"entry_depth": 1}, []])
def test_cli_rejects_invalid_guard_audit_before_building_provider(tmp_path, guards):
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps({**_strategy_config(), "altered_backtest_guards": guards}))
    result, backend = _invoke_pnl(CliRunner(), ["--config-file", str(path)])
    assert result.exit_code == 2
    assert "nonempty reasons" in result.output
    backend.assert_not_called()


@pytest.mark.parametrize("guards", [{}, {"entry_depth": "Explicit research variant"}])
def test_strict_result_replay_accepts_derived_comparison_and_preserves_guards(tmp_path, guards):
    config = _make_pnl_config()
    config.altered_backtest_guards = guards
    path = tmp_path / "result.json"
    path.write_text(json.dumps({"config": config.to_dict()}))
    loaded = load_config_from_result(path, strict=True)
    assert loaded.warnings == []
    assert loaded.config.altered_backtest_guards == guards
    assert loaded.config.to_dict()["strategy_comparison"] == config.to_dict()["strategy_comparison"]
    assert loaded.config.calculate_config_hash() == config.calculate_config_hash()


def test_cli_replay_preserves_recorded_variant_and_rejects_explicit_conflict(tmp_path):
    config = _make_pnl_config()
    config.altered_backtest_guards = {"entry_depth": "Recorded experiment"}
    result_path = tmp_path / "result.json"
    result_path.write_text(json.dumps({"config": config.to_dict()}))
    result, backend = _invoke_pnl(CliRunner(), ["--from-result", str(result_path)])
    assert result.exit_code == 0, result.output
    assert backend.return_value.backtest.await_args.args[1].altered_backtest_guards == config.altered_backtest_guards

    variant_path = tmp_path / "conflict.json"
    variant_path.write_text(json.dumps({**_strategy_config(), "altered_backtest_guards": {}}))
    refused, backend = _invoke_pnl(CliRunner(), ["--from-result", str(result_path), "--config-file", str(variant_path)])
    assert refused.exit_code == 2
    assert "conflicts with the recorded" in refused.output
    backend.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["", "  ", 1])
async def test_mutated_guard_reason_is_rejected_at_consumption_and_persistence(reason):
    config = _make_pnl_config()
    config.altered_backtest_guards["entry_depth"] = reason
    with pytest.raises(ValueError, match="nonempty reasons"):
        await check_declared_dependencies(_DepthStrategy(), config)
    with pytest.raises(ValueError, match="nonempty reasons"):
        config.to_dict()
    with pytest.raises(ValueError, match="nonempty reasons"):
        config.calculate_config_hash()


@pytest.mark.parametrize("check", ["historical_pool_analytics", "historical_pool_analytics_grid"])
def test_analytics_refusal_does_not_advertise_missing_price_bypass(capsys, check):
    backend = MagicMock()
    backend.backtest = AsyncMock(
        side_effect=PreflightValidationError("Missing historical analytics", failed_checks=[check])
    )
    with pytest.raises(SystemExit) as caught:
        _run_backtest(backend, object(), _make_pnl_config())
    assert caught.value.code == 2
    error = capsys.readouterr().err
    assert "cannot bypass this guard" in error
    assert "To run anyway" not in error
