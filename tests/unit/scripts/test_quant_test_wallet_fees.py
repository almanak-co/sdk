"""Regression tests for quant-test wallet helper fee-model preservation."""

from __future__ import annotations

import importlib.util
import inspect
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO = Path(__file__).resolve().parents[3]


def _load(name: str):
    path = REPO / "qa_lab" / name
    spec = importlib.util.spec_from_file_location(name.removesuffix(".py"), path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("name", ["fund_pool_wallet.py", "sweep_pool_wallet.py"])
def test_preserves_builder_selected_eip1559_fees(name: str) -> None:
    module = _load(name)
    tx = {"type": 2, "maxFeePerGas": 10, "maxPriorityFeePerGas": 1}

    module._apply_missing_fees(SimpleNamespace(eth=SimpleNamespace()), tx)

    assert tx == {"type": 2, "maxFeePerGas": 10, "maxPriorityFeePerGas": 1}
    assert "gasPrice" not in tx


@pytest.mark.parametrize("name", ["fund_pool_wallet.py", "sweep_pool_wallet.py"])
def test_adds_fees_to_manually_built_transaction(name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load(name)
    monkeypatch.setattr(module, "_fees", lambda _w3: {"gasPrice": 7})
    tx = {"to": "0xabc"}

    module._apply_missing_fees(object(), tx)

    assert tx == {"to": "0xabc", "gasPrice": 7}


def test_funder_token_path_uses_fee_model_preserving_helper() -> None:
    module = _load("fund_pool_wallet.py")

    source = inspect.getsource(module.send_token)

    assert "_apply_missing_fees(w3, tx)" in source
    assert "tx.update(_fees(w3))" not in source
