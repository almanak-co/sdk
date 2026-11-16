from types import SimpleNamespace

import pytest

from almanak.connectors.aerodrome.operations import Collect
from almanak.connectors.aerodrome.positions import Ownership, Position
from almanak.connectors.aerodrome.receipts import movement
from almanak.connectors.aerodrome.reconciliation import owned_after, retained_position
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from tests.aerodrome_support import CAPTURE, operation


def stored(target, raw):
    return Position(
        shares=AssetAmount(asset=target.pool.shares_asset, amount=raw["shares"]),
        fees0=AssetAmount(asset=target.assets[0], amount=raw["claimable"][0]),
        fees1=AssetAmount(asset=target.assets[1], amount=raw["claimable"][1]),
        index0=raw["supply_indices"][0],
        index1=raw["supply_indices"][1],
    )


def inputs(row, step):
    op = operation(row, step)
    old = stored(op.target, step["before"])
    owned = Ownership(
        target=op.target,
        inventory0=AssetAmount(asset=op.target.assets[0], amount=step["before"]["wallets"][0]),
        inventory1=AssetAmount(asset=op.target.assets[1], amount=step["before"]["wallets"][1]),
        position=None if old.empty else old,
    )
    before = SimpleNamespace(
        owned=owned,
        pool=SimpleNamespace(
            index0=step["before"]["indices"][0], index1=step["before"]["indices"][1]
        ),
    )
    pool = SimpleNamespace(index0=step["after"]["indices"][0], index1=step["after"]["indices"][1])
    current = stored(op.target, step["after"])
    effects = movement(step["receipt"], op, row["market"]["pool_fees"])
    return op, before, effects, current, pool


@pytest.mark.parametrize("row", CAPTURE, ids=["volatile", "stable"])
def test_native_receipts_reconcile_stored_claims_shares_and_principal(row):
    count = 0
    for step in row["steps"]:
        if step["call"]["fund"]["address"] != row["fund"]["address"]:
            continue
        op, before, effects, current, pool = inputs(row, step)
        position = retained_position(before, op, effects, current, pool, True)
        result = owned_after(before, op, effects, position, True)
        assert (result.inventory0.amount, result.inventory1.amount) == tuple(
            step["after"]["wallets"]
        )
        assert result.position == (None if current.empty else current)
        count += 1
    assert count == 18


@pytest.mark.parametrize("mutation", ["shares", "claimable", "index", "paid"])
def test_collection_cannot_adopt_unexplained_share_or_fee_changes(mutation):
    row = CAPTURE[0]
    step = next(s for s in row["steps"] if s["kind"] == "collect")
    op, before, effects, current, pool = inputs(row, step)
    assert isinstance(op, Collect) and effects.fees0 > 0
    if mutation == "shares":
        current = current.model_copy(
            update={
                "shares": current.shares.model_copy(update={"amount": current.shares.amount + 1})
            }
        )
    elif mutation == "claimable":
        current = current.model_copy(
            update={"fees0": current.fees0.model_copy(update={"amount": 1})}
        )
    elif mutation == "index":
        current = current.model_copy(update={"index0": pool.index0 + 1})
    else:
        effects = effects.model_copy(update={"fees0": effects.fees0 + 1})
    with pytest.raises(DataError):
        retained_position(before, op, effects, current, pool, True)
