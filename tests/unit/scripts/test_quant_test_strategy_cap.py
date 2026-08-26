"""VIB-5415 regression tests for the quant strategy-cap pre-funding gate."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

import almanak

_SCRIPT = Path(__file__).parents[3] / "scripts" / "quant-test" / "validate_strategy_cap.py"
_SPEC = importlib.util.spec_from_file_location("validate_strategy_cap_test", _SCRIPT)
assert _SPEC and _SPEC.loader
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)

CapCompatibilityError = _MODULE.CapCompatibilityError
validate_strategy_cap = _MODULE.validate_strategy_cap

_DEMOS = Path(almanak.__file__).parent / "demo_strategies"
_UNISWAP_LP = _DEMOS / "uniswap_lp"
_UNISWAP_RSI = _DEMOS / "uniswap_rsi"


def _write_config(tmp_path: Path, config: dict) -> Path:
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config))
    return path


def test_hidden_typed_floor_fails_below_cap_card(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, {"total_value_usd": 4})

    with pytest.raises(CapCompatibilityError, match=r"\$100.*typed strategy default.*\$4"):
        validate_strategy_cap(_UNISWAP_LP, trading_cap_usd="4", config_path=config_path)


def test_explicit_demo_floor_passes_below_cap(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, {"total_value_usd": 4, "min_position_usd": "3"})

    result = validate_strategy_cap(_UNISWAP_LP, trading_cap_usd="4", config_path=config_path)

    assert str(result.min_position_usd) == "3"
    assert str(result.available_trading_usd) == "4"
    assert result.applicable is True


def test_floor_equal_to_cap_fails_closed(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, {"min_position_usd": "4"})

    with pytest.raises(CapCompatibilityError, match="is not below"):
        validate_strategy_cap(_UNISWAP_LP, trading_cap_usd="4", config_path=config_path)


def test_declared_allocation_cannot_hide_behind_larger_cap(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, {"total_value_usd": "4", "min_position_usd": "5"})

    with pytest.raises(CapCompatibilityError, match=r"allocation=\$4.*TRADING_CAP_USD=\$10"):
        validate_strategy_cap(_UNISWAP_LP, trading_cap_usd="10", config_path=config_path)


def test_strategy_without_floor_is_not_applicable() -> None:
    result = validate_strategy_cap(_UNISWAP_RSI, trading_cap_usd="4")

    assert result.min_position_usd is None
    assert result.applicable is False
