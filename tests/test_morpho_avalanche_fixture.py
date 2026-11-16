"""stAVAX funding cannot cross proxy implementation or fork boundaries."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.caip import AssetType, ChainId
from almanak.gateway.morpho_avalanche_fixture import (
    IMPLEMENTATION,
    IMPLEMENTATION_SLOT,
    TOKEN,
    fund_stavax_storage,
)
from almanak.gateway.roles_storage import mapping_slot
from almanak.gateway.safe_funding import reviewed_funder

SOURCE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-09-morpho-avalanche-vault-source"
)
FUND = "0x" + "12" * 20


class Node:
    def __init__(self):
        self.origin = SimpleNamespace(chain=SimpleNamespace(chain_id=ChainId("eip155:43114")))
        self.valid = True
        self.checked = False
        self.pointer = IMPLEMENTATION
        self.codes = {
            address: json.loads((SOURCE / (name + "-sourcify.json")).read_text())[
                "runtimeBytecode"
            ]["onchainBytecode"]
            for name, address in (("proxy", TOKEN), ("implementation", IMPLEMENTATION))
        }
        self.writes = []

    def _validate_origin(self):
        if not self.valid:
            raise ValueError("unidentified fork")
        self.checked = True

    def rpc(self, method, params):
        assert self.checked
        if method == "eth_getStorageAt":
            assert params == [TOKEN, IMPLEMENTATION_SLOT, "latest"]
            return self.pointer
        if method == "eth_getCode":
            return self.codes[params[0]]
        assert method == "anvil_setStorageAt"
        self.writes.append(params)


def test_compiled_proxy_and_implementation_fund_only_balance_mapping():
    node = Node()
    assert reviewed_funder(AssetType("eip155:43114/erc20:" + TOKEN)) is fund_stavax_storage
    fund_stavax_storage(node, FUND, TOKEN, 10**18)
    slot = mapping_slot("address", FUND, 5)
    assert node.writes == [
        [TOKEN, "0x" + slot.to_bytes(32, "big").hex(), "0x" + (10**18).to_bytes(32, "big").hex()]
    ]


@pytest.mark.parametrize(
    "failure",
    [
        "origin",
        "chain",
        "asset",
        "pointer",
        "proxy",
        "implementation",
        "amount",
        "negative",
        "overflow",
    ],
)
def test_invalid_fixture_identity_or_layout_never_changes_storage(failure):
    node, token, amount = Node(), TOKEN, 10**18
    if failure == "origin":
        node.valid = False
    elif failure == "chain":
        node.origin.chain.chain_id = ChainId("eip155:1")
    elif failure == "asset":
        token = FUND
    elif failure == "pointer":
        node.pointer = FUND
    elif failure in ("proxy", "implementation"):
        contract = TOKEN if failure == "proxy" else IMPLEMENTATION
        node.codes[contract] = "0x00" + node.codes[contract][4:]
    elif failure == "negative":
        amount = -1
    elif failure == "overflow":
        amount = 2**255
    else:
        amount = True
    with pytest.raises(ValueError):
        fund_stavax_storage(node, FUND, token, amount)
    assert not node.writes
