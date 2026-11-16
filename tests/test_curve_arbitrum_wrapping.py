"""Captured aeWETH mint/burn receipts must reconcile exact pool and fund quantities."""

import copy
import gzip
import json
from pathlib import Path

import pytest

from almanak.chains import evm_network
from almanak.connectors.curve import receipts
from almanak.connectors.curve.state import Outcome
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS

DIRECTORY = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-arbitrum-tricrypto-eoa"
)
CASE = json.loads((DIRECTORY / "manifest.json").read_text())["cases"][0]["case"]
RAW = json.loads(gzip.decompress((DIRECTORY / "native" / CASE / "checkpoint.json.gz").read_bytes()))
OUTCOMES = tuple(Outcome.model_validate_json(json.dumps(row)) for row in RAW["outcomes"])


def wrapping_log(outcome):
    network = evm_network(outcome.operation.target.pool.chain.chain_id)
    address = network.wrapped_native.reference
    zero = "0x" + "0" * 64
    return next(
        (
            i
            for i, log in enumerate(outcome.receipt["logs"])
            if log["address"] == address
            and log["topics"][0] == receipts.TRANSFER
            and zero in log["topics"][1:]
        ),
        None,
    )


@pytest.mark.parametrize("outcome", OUTCOMES)
def test_actual_receipt_preserves_exact_fund_effects(outcome):
    result = receipts.movement(outcome.receipt, outcome.operation, outcome.before)
    assert result.asset_changes == outcome.asset_changes
    assert result.share_change == outcome.share_change


@pytest.mark.parametrize("outcome", tuple(o for o in OUTCOMES if wrapping_log(o) is not None))
@pytest.mark.parametrize("mutation", ("amount", "recipient", "missing", "duplicate", "asset"))
def test_wrapping_events_cannot_hide_changed_settlement(outcome, mutation):
    index = wrapping_log(outcome)
    assert index is not None
    receipt = copy.deepcopy(outcome.receipt)
    log = receipt["logs"][index]
    if mutation == "amount":
        log["data"] = "0x" + (int(log["data"], 16) + 1).to_bytes(32, "big").hex()
    elif mutation == "recipient":
        log["topics"][2] = "0x" + "0" * 24 + outcome.operation.target.fund.address[2:]
    elif mutation == "missing":
        receipt["logs"].pop(index)
    elif mutation == "duplicate":
        receipt["logs"].append(copy.deepcopy(log))
    else:
        log["address"] = ZERO_ADDRESS
    with pytest.raises(DataError, match="transfers differ"):
        receipts.movement(receipt, outcome.operation, outcome.before)


def test_reference_exercises_wrapping_on_every_economic_operation():
    assert sum(wrapping_log(o) is not None for o in OUTCOMES) == 12
