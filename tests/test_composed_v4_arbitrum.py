"""Actual Arbitrum mixed observations retain scope and one shared fund cash book."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.caip import ChainId
from almanak.connectors.composed.reporting import report
from almanak.connectors.composed.state import Checkpoint
from almanak.connectors.uniswap_v4.calibration import Measurement, load_calibration
from almanak.connectors.uniswap_v4.deployments import ARTIFACTS, verify_identities
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity
from tests import test_composed_uniswap_v4 as shared
from tests.lending_captures import read_capture
from tests.uniswap_support import scope_only_json

ROOT = Path(__file__).parents[1]


def actual_state():
    path = ROOT / "tests/fixtures/composed-v4-arbitrum-closed-fork.json.gz"
    return Checkpoint.model_validate_json(scope_only_json(read_capture(path)))


@pytest.fixture
def actual_decision(monkeypatch):
    path = ROOT / "tests/fixtures/composed-v4-arbitrum-mixed-decision.json.gz"
    monkeypatch.setattr(
        shared,
        "captured_decision",
        lambda: json.loads(gzip.decompress(path.read_bytes()))["record"],
    )


def test_actual_arbitrum_mixed_decision_roundtrip(actual_decision):
    shared.test_actual_v4_decision_roundtrips_mixed_fork_observations()


@pytest.mark.parametrize("mutation", ["allocation", "chain", "environment", "missing", "duplicate"])
def test_actual_arbitrum_decision_scope_refusals(actual_decision, mutation):
    shared.test_v4_decision_rejects_mismatched_observation_scope(mutation)


def test_returned_arbitrum_collateral_is_shared_cash():
    state = actual_state()
    assert state.config.target("trade").fund.chain.chain_id == ChainId("eip155:42161")
    assert state.mode == "closed"
    measured = report(state)
    assert measured["transaction_accounting_complete"] and not measured["accounting_issues"]
    assert len(measured["inventory"]) == len(state.inventory) == 2
    collateral = state.config.target("collateral").asset
    cash = next(b for b in state.inventory if b.key.asset == collateral)
    assert cash.amount >= state.config.allocation("collateral").capital
    exchange = measured["positions"][-1]
    assert exchange["owned"]["inventory0"]["amount"] == cash.amount
    assert "pnl_excluding_gas" not in exchange and "fund_value_excluding_gas" not in exchange


@pytest.mark.parametrize("index", range(6))
def test_each_arbitrum_runtime_change_is_refused(index):
    target = actual_state().config.target("trade")
    identities = json.loads((ARTIFACTS / "arbitrum-protocol.json").read_text())
    observed = tuple(
        ContractIdentity.model_validate_json(json.dumps(i)) for i in identities.values()
    )
    verify_identities(target, observed)
    changed = list(observed)
    changed[index] = changed[index].model_copy(update={"code_hash": "0x" + "12" * 32})
    with pytest.raises(DataError, match="verified implementation"):
        verify_identities(target, changed)


def test_arbitrum_calibration_reconciles_all_retained_receipts():
    _, costs = load_calibration(ChainId("eip155:42161"))
    raw = json.loads((ARTIFACTS / "arbitrum-safe-gas-calibration.json").read_text())
    assert len(raw["measurements"]) == 19 and len(costs) == 8
    for row in raw["measurements"]:
        item = Measurement.model_validate_json(json.dumps(row))
        assert item.op_fee_evidence is None
        assert item.origin.chain.chain_id == ChainId("eip155:42161")
        assert item.cost() == int(item.receipt["gasUsed"], 16) * int(
            item.receipt["effectiveGasPrice"], 16
        )
