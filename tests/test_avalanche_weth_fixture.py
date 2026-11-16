"""WETH.e fixture funding is confined to its compiled Avalanche balance layout."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.caip import AssetType, ChainId
from almanak.gateway.roles_storage import mapping_slot
from almanak.gateway.safe_funding import fund_avalanche_weth_storage, reviewed_funder

TOKEN = "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab"
FUND = "0x" + "12" * 20
SOURCE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-09-finish-lending/avalanche-weth-source"
    / "token-sourcify.json"
)


class Node:
    def __init__(self):
        self.origin = SimpleNamespace(chain=SimpleNamespace(chain_id=ChainId("eip155:43114")))
        self.verified = False
        self.unidentified = False
        self.code = json.loads(SOURCE.read_text())["runtimeBytecode"]["onchainBytecode"]
        self.writes = []

    def _validate_origin(self):
        if self.unidentified:
            raise ValueError("not an identified fork")
        self.verified = True

    def rpc(self, method, params):
        assert self.verified
        if method == "eth_getCode":
            assert params == [TOKEN, "latest"]
            return self.code
        assert method == "anvil_setStorageAt"
        self.writes.append(params)


def test_verified_bridge_token_funds_exact_slot_zero_balance():
    node = Node()
    assert reviewed_funder(AssetType("eip155:43114/erc20:" + TOKEN)) is fund_avalanche_weth_storage
    fund_avalanche_weth_storage(node, FUND, TOKEN, 10**16)
    slot = mapping_slot("address", FUND, 0)
    assert node.writes == [
        [TOKEN, "0x" + slot.to_bytes(32, "big").hex(), "0x" + (10**16).to_bytes(32, "big").hex()]
    ]


@pytest.mark.parametrize(
    "failure", ["origin", "chain", "asset", "code", "negative", "overflow", "boolean"]
)
def test_wrong_scope_or_layout_cannot_write_fixture_storage(failure):
    node, token, amount = Node(), TOKEN, 10**16
    if failure == "origin":
        node.unidentified = True
    elif failure == "chain":
        node.origin.chain.chain_id = ChainId("eip155:1")
    elif failure == "asset":
        token = FUND
    elif failure == "code":
        node.code = "0x00" + node.code[4:]
    elif failure == "negative":
        amount = -1
    elif failure == "overflow":
        amount = 2**255
    else:
        amount = True
    with pytest.raises(ValueError):
        fund_avalanche_weth_storage(node, FUND, token, amount)
    assert not node.writes
