from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.connectors._strategy_base.accounting_treatment_registry import AccountingTreatmentRegistry
from almanak.framework.primitives import Primitive
from almanak.framework.runner import strategy_runner
from almanak.framework.runner.strategy_runner import StrategyRunner

_GOLDEN_PATH = Path(__file__).parents[2] / "fixtures" / "runner" / "legacy_position_key_truth_table.json"
_GOLDEN_CASES = json.loads(_GOLDEN_PATH.read_text())


def _compute(case: dict) -> tuple[str, str]:
    return StrategyRunner._compute_outbox_position_key(
        SimpleNamespace(),  # type: ignore[arg-type]
        SimpleNamespace(**case["intent"]),
        case["intent_type"],
        case["chain"],
        case["wallet"],
        case.get("resolved_pool"),
    )


@pytest.mark.parametrize("case", _GOLDEN_CASES, ids=[case["name"] for case in _GOLDEN_CASES])
def test_legacy_position_key_truth_table(case: dict) -> None:
    assert _compute(case) == tuple(case["expected"])


def test_truth_table_covers_every_primitive() -> None:
    assert {case["primitive"] for case in _GOLDEN_CASES} == {primitive.value for primitive in Primitive}


def test_generic_lp_missing_pool_does_not_use_error_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    debug = Mock()
    monkeypatch.setattr(strategy_runner.logger, "debug", debug)

    result = StrategyRunner._compute_outbox_position_key(
        SimpleNamespace(),  # type: ignore[arg-type]
        SimpleNamespace(protocol="uniswap_v3"),
        "LP_OPEN",
        "arbitrum",
        "0xWallet",
    )

    assert result == ("", "")
    debug.assert_not_called()


def test_registry_failure_uses_error_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    failure = RuntimeError("registry unavailable")
    monkeypatch.setattr(AccountingTreatmentRegistry, "position_key_for", Mock(side_effect=failure))
    debug = Mock()
    monkeypatch.setattr(strategy_runner.logger, "debug", debug)

    result = StrategyRunner._compute_outbox_position_key(
        SimpleNamespace(),  # type: ignore[arg-type]
        SimpleNamespace(protocol="uniswap_v3"),
        "SWAP",
        "arbitrum",
        "0xWallet",
    )

    assert result == ("", "")
    debug.assert_called_once_with("_compute_outbox_position_key failed", exc_info=True)
