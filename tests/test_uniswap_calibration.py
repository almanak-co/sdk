"""Detailed runs require a complete, explicitly selected measured cost table."""

import hashlib
import json
from pathlib import Path
from typing import get_args

import pytest

from almanak.caip import ChainId
from almanak.connectors.uniswap_v4.backtest import UniswapModel
from almanak.connectors.uniswap_v4.backtest_config import OperationKind
from almanak.connectors.uniswap_v4.calibration import load_calibration, validate_costs
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest
from tests.legacy_references import harness_directory

ROOT = Path(__file__).parents[1]


def settings(reference):
    directory = harness_directory(reference)
    author = read_author(directory / "config.yaml")
    return read_backtest(directory / "backtest.yaml", author)


@pytest.mark.parametrize("reference", ["uniswap_v4_lp", "uniswap_v4_avalanche_lp"])
def test_selected_tables_cover_every_operation(reference):
    selected = settings(reference)
    validate_costs(selected)
    assert set(selected.costs.per_operation) == set(get_args(OperationKind))


@pytest.mark.parametrize("operation", get_args(OperationKind))
@pytest.mark.parametrize("reference", ["uniswap_v4_lp", "uniswap_v4_avalanche_lp"])
def test_missing_operation_refuses_before_model_execution(monkeypatch, reference, operation):
    from almanak.connectors.uniswap_v4 import backtest

    selected = settings(reference)
    costs = dict(selected.costs.per_operation)
    del costs[operation]
    selected = selected.model_copy(
        update={"costs": selected.costs.model_copy(update={"per_operation": costs})}
    )
    monkeypatch.setattr(backtest, "strategy_capabilities", forbidden)
    with pytest.raises(ValueError, match="fixed costs missing operations: " + operation):
        UniswapModel(None, selected, None, {})


def forbidden(*args, **kwargs):
    raise AssertionError("missing financial costs must refuse before author evaluation")


def test_complete_avalanche_calibration_preserves_original_evidence():
    folder = ROOT / "src/almanak/connectors/uniswap_v4/artifacts"
    original = (folder / "avalanche-safe-gas-calibration.json").read_bytes()
    assert hashlib.sha256(original).hexdigest() == (
        "789b7273dd00bcd450c24a258a4141dbd339809d79dc9b1fc8a7695326cd0d1b"
    )
    complete = json.loads((folder / "avalanche-safe-gas-calibration-complete.json").read_text())
    assert complete["measurements"][:-2] == json.loads(original)["measurements"]
    _, costs = load_calibration(ChainId("eip155:43114"))
    assert costs["revoke_erc20"] == 101882536320870


@pytest.mark.parametrize(
    "chain,name",
    [("eip155:56", "bnb"), ("eip155:137", "polygon"), ("eip155:4663", "robinhood")],
)
def test_unmeasured_candidate_chain_names_the_missing_artifact(chain, name):
    folder = ROOT / "src/almanak/connectors/uniswap_v4/artifacts"
    filename = f"{name}-safe-gas-calibration.json"
    assert not (folder / filename).exists(), "measured receipts must be validated, not skipped"
    with pytest.raises(
        ValueError, match=f"no retained Safe gas calibration for {chain}.*{filename}"
    ):
        load_calibration(ChainId(chain))


def test_unknown_chain_remains_refused_without_naming_an_artifact():
    with pytest.raises(ValueError, match="for the selected chain$"):
        load_calibration(ChainId("eip155:100"))
