import copy

import pytest
from eth_abi import encode

from almanak.connectors.aerodrome.abi import call
from almanak.connectors.aerodrome.math import addition, removal
from almanak.connectors.aerodrome.operations import Addition, Removal
from almanak.connectors.aerodrome.receipts import movement
from almanak.connectors.aerodrome.simulation import validate_simulation
from almanak.gateway.ethereum import DataError
from tests.aerodrome_support import CAPTURE, operation


@pytest.mark.parametrize("row", CAPTURE, ids=["volatile", "stable"])
def test_sdk_calls_returns_and_receipts_match_independent_native_operations(row):
    assert len(row["steps"]) == 20
    for step in row["steps"]:
        op = operation(row, step)
        assert call(op).model_dump(mode="json") == step["call"]
        validate_simulation(op, step["simulation"])
        effects = movement(step["receipt"], op, row["market"]["pool_fees"])
        if op.target.fund.address == row["fund"]["address"]:
            before, after = step["before"], step["after"]
            assert (effects.delta0, effects.delta1) == tuple(
                a - b for a, b in zip(after["wallets"], before["wallets"], strict=True)
            )
            assert effects.shares_delta == after["shares"] - before["shares"]


@pytest.mark.parametrize("row", CAPTURE, ids=["volatile", "stable"])
def test_liquidity_integer_model_matches_actual_router_returns(row):
    for step in row["steps"]:
        op = operation(row, step)
        before = step["before"]
        if isinstance(op, Addition):
            result = addition(
                *before["reserves"][:2],
                before["total_supply"],
                op.maximum0.amount,
                op.maximum1.amount,
            )
            assert "0x" + encode(("uint256",) * 3, result).hex() == step["simulation"]
        elif isinstance(op, Removal):
            result = removal(*before["reserves"][:2], before["total_supply"], op.shares.amount)
            assert "0x" + encode(("uint256",) * 2, result).hex() == step["simulation"]


@pytest.mark.parametrize(
    "kind", ["approve", "swap", "mint", "increase", "decrease", "collect", "close"]
)
def test_receipt_cannot_redirect_fund_cash_or_shares(kind):
    row = CAPTURE[0]
    step = next(s for s in row["steps"] if s["kind"] == kind)
    op = operation(row, step)
    receipt = copy.deepcopy(step["receipt"])
    needle = op.target.fund.address[2:]
    changed = False
    for log in receipt["logs"]:
        for index, topic in enumerate(log["topics"]):
            if topic.endswith(needle):
                log["topics"][index] = "0x" + "00" * 31 + "01"
                changed = True
    assert changed
    with pytest.raises(DataError):
        movement(receipt, op, row["market"]["pool_fees"])


@pytest.mark.parametrize(
    "kind", ["approve", "swap", "mint", "increase", "decrease", "collect", "close"]
)
def test_simulation_rejects_noncanonical_or_missing_returns(kind):
    row = CAPTURE[0]
    step = next(s for s in row["steps"] if s["kind"] == kind)
    op = operation(row, step)
    with pytest.raises(DataError, match="malformed"):
        validate_simulation(op, step["simulation"] + "00")
