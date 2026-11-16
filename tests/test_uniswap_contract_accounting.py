"""Integer LP calculations checked against fresh contract transfer receipts."""

import gzip
import json
from functools import cache

import pytest
from eth_utils import keccak

from almanak.connectors.uniswap_v4.math import accrued, amounts
from almanak.connectors.uniswap_v4.reader import sqrt_bounds
from almanak.connectors.uniswap_v4.state import Checkpoint
from tests.uniswap_support import ROOT, scope_only


@cache
def lp_cases():
    path = (
        ROOT
        / "docs/internal/reports/evidence/2026-09-06-backtest-integration"
        / "final-fork-lifecycles.json.gz"
    )
    raw = json.loads(gzip.decompress(path.read_bytes()))
    return tuple(
        row
        for row in raw["runs"]
        if row["result"]["strategy"] in ("uniswap_v4_lp", "uniswap_v4_avalanche_lp")
    )


def receipt_delta(outcome, asset):
    signature = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
    fund = outcome.operation.target.fund.address
    delta = 0
    for log in outcome.receipt["logs"]:
        if log["address"].lower() != asset.reference or log["topics"][0] != signature:
            continue
        assert len(log["topics"]) == 3
        sender, recipient = ("0x" + topic[-40:] for topic in log["topics"][1:])
        quantity = int(log["data"], 16)
        delta += quantity * ((recipient == fund) - (sender == fund))
    return delta


@pytest.mark.parametrize("index", range(5))
def test_lp_integer_principal_and_fees_match_deployed_pool_transfers(index):
    row = lp_cases()[index]
    state = Checkpoint.model_validate_json(json.dumps(scope_only(row["checkpoint"])))
    checked = []
    for outcome in state.outcomes:
        operation = outcome.operation
        if operation.kind not in ("mint", "collect", "burn"):
            continue
        assert outcome.success and outcome.receipt["status"] == "0x1"
        target = operation.target
        actual = tuple(receipt_delta(outcome, asset) for asset in target.assets)
        if operation.kind == "mint":
            expected = tuple(
                -v
                for v in amounts(
                    operation.liquidity,
                    outcome.after.pool.sqrt_price_x96,
                    *sqrt_bounds(target),
                    up=True,
                )
            )
            assert outcome.after.owned.position.liquidity == operation.liquidity
        elif operation.kind == "collect":
            position = outcome.before.owned.position
            expected = tuple(
                accrued(position.liquidity, before, after)
                for before, after in zip(
                    position.fee_growth, outcome.after.position_growth, strict=True
                )
            )
        else:
            # These fixtures swap before collection and perform no later swap.
            # The following full burn therefore isolates rounded principal.
            expected = amounts(
                outcome.before.owned.position.liquidity,
                outcome.after.pool.sqrt_price_x96,
                *sqrt_bounds(target),
            )
        assert actual == expected == (outcome.delta0, outcome.delta1)
        checked.append(operation.kind)
    assert checked == ["mint", "collect", "burn"]
