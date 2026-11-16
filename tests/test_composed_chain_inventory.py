"""Chain recovery must preserve balances owned on every unaffected chain."""

from types import SimpleNamespace

import pytest

from almanak.caip import AssetType, ChainId
from almanak.effects import Balance, WalletAsset
from almanak.models import Chain
from almanak.work import merge_inventory, project_inventory


def balances():
    return tuple(
        Balance(
            key=WalletAsset(
                account=name,
                chain=Chain(chain_id=ChainId(chain), environment="mainnet"),
                asset=AssetType(chain + "/erc20:0x" + "12" * 20),
            ),
            amount=amount,
        )
        for name, chain, amount in (("ethereum", "eip155:1", 20), ("base", "eip155:8453", 30))
    )


def test_chain_merge_cannot_erase_another_chains_cash():
    initial = balances()
    state = SimpleNamespace(inventory=initial)
    chain = initial[1].key.chain
    projected = project_inventory(state, chain)
    assert projected["inventory"] == (initial[1],)
    changed = SimpleNamespace(inventory=(initial[1].model_copy(update={"amount": 5}),))
    merged = merge_inventory(state, changed, chain)["inventory"]
    assert merged[0] is initial[0]
    assert [b.amount for b in merged] == [20, 5]


@pytest.mark.parametrize("mutation", ["foreign", "missing", "duplicate"])
def test_chain_merge_refuses_inventory_scope_changes(mutation):
    initial = balances()
    changes = {"foreign": initial, "missing": (), "duplicate": (initial[1], initial[1])}
    with pytest.raises(ValueError, match="chain's wallet assets"):
        merge_inventory(
            SimpleNamespace(inventory=initial),
            SimpleNamespace(inventory=changes[mutation]),
            initial[1].key.chain,
        )
