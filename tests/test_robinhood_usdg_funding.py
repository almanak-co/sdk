"""Identified-fixture USDG capital cannot overwrite payout state or changed code."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.caip import AssetType, ChainId
from almanak.gateway.roles_storage import mapping_slot
from almanak.gateway.safe_funding import fund_robinhood_usdg_storage, reviewed_funder

TOKEN = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
IMPLEMENTATION = "0x68184c449e1a8f34fa18d289737129fd27b66f8f"
POINTER = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
FUND = "0x1111111111111111111111111111111111111111"
BALANCE = hex(mapping_slot("address", FUND, 1))
FROZEN = hex(mapping_slot("address", FUND, 6))


class Node:
    def __init__(self):
        self.origin = SimpleNamespace(chain=SimpleNamespace(chain_id=ChainId("eip155:4663")))
        self.code = json.loads(
            (Path(__file__).parent / "fixtures/robinhood-usdg-funding.json").read_text()
        )["codes"]
        self.storage = {POINTER: int(IMPLEMENTATION, 16), BALANCE: 0, FROZEN: 0, "0x4": 0}
        self.validated = False
        self.calls = []
        self.writes = []

    def _validate_origin(self):
        self.validated = True

    def rpc(self, method, params):
        assert self.validated
        self.calls.append((method, params))
        if method == "eth_getCode":
            return self.code[params[0]]
        if method == "eth_getStorageAt":
            return hex(self.storage[params[1]])
        assert method == "anvil_setStorageAt"
        self.writes.append(params)
        return True


def test_usdg_funder_routes_and_writes_only_fresh_base_balance():
    node = Node()
    amount = 123456789
    assert reviewed_funder(AssetType("eip155:4663/erc20:" + TOKEN)) is fund_robinhood_usdg_storage
    fund_robinhood_usdg_storage(node, FUND, TOKEN, amount)
    assert node.writes == [
        [
            TOKEN,
            "0x" + int(BALANCE, 16).to_bytes(32, "big").hex(),
            "0x" + amount.to_bytes(32, "big").hex(),
        ]
    ]


@pytest.mark.parametrize(
    "kind,key,value",
    (
        ("storage", POINTER, 0),
        ("code", TOKEN, "0x00"),
        ("code", IMPLEMENTATION, "0x00"),
        ("storage", BALANCE, 1),
        ("storage", BALANCE, 1 << 128),
        ("storage", FROZEN, 1),
        ("storage", "0x4", 1 << 160),
    ),
)
def test_usdg_funding_refuses_changed_code_or_existing_account_state(kind, key, value):
    node = Node()
    getattr(node, kind)[key] = value
    with pytest.raises(ValueError, match="USDG fixture"):
        fund_robinhood_usdg_storage(node, FUND, TOKEN, 1000)
    assert not node.writes


@pytest.mark.parametrize("amount", (-1, 2**64, True, 0.5))
def test_usdg_amount_refusal_precedes_rpc(amount):
    node = Node()
    with pytest.raises(ValueError, match="uint64 balance"):
        fund_robinhood_usdg_storage(node, FUND, TOKEN, amount)
    assert not node.calls


@pytest.mark.parametrize("chain,token", (("eip155:1", TOKEN), ("eip155:4663", FUND)))
def test_usdg_scope_refusal_precedes_rpc(chain, token):
    node = Node()
    node.origin.chain.chain_id = ChainId(chain)
    with pytest.raises(ValueError, match="verified Robinhood"):
        fund_robinhood_usdg_storage(node, FUND, token, 1000)
    assert not node.calls
