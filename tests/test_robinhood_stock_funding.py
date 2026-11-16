"""Only a verified identified fork can alter Stock raw balances and total supply."""

import json
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.gateway import ethereum
from almanak.gateway.robinhood_stock import BALANCES_NAMESPACE, BEACON, BEACON_SLOT
from almanak.gateway.roles_storage import mapping_slot
from almanak.gateway.safe_funding import fund_stock_storage
from almanak.models import Block, Chain
from tests.test_robinhood_stock_identity import captured

FUND = "0x" + "12" * 20
TOKEN = "0xd0601ce157db5bdc3162bbac2a2c8af5320d9eec"


@pytest.fixture
def node(monkeypatch):
    data = captured("tokens")
    beacon = captured("beacon")["tokens"][0]
    block = Block.model_validate_json(json.dumps(data["block"]))
    token = next(row for row in data["tokens"] if row["label"] == "nvda")
    codes = {
        TOKEN: token["code"],
        BEACON: beacon["stock-beacon_code"],
        beacon["implementation"]: beacon["stock-implementation_code"],
    }
    slots = {
        BEACON_SLOT: int(BEACON, 16),
        mapping_slot("address", FUND, BALANCES_NAMESPACE): 7,
        BALANCES_NAMESPACE + 2: 1000,
    }
    calls = []

    def rpc(method, params):
        calls.append((method, params))
        if method == "anvil_setStorageAt":
            slots[int(params[1], 16)] = int(params[2], 16)
            return True
        if method == "eth_getCode":
            return codes[params[0]]
        if method == "eth_getStorageAt":
            return hex(slots[int(params[1], 16)])
        if method == "eth_call":
            slot = (
                BALANCES_NAMESPACE + 2
                if params[0]["data"].startswith("0x18160ddd")
                else mapping_slot("address", FUND, BALANCES_NAMESPACE)
            )
            return "0x" + slots[slot].to_bytes(32, "big").hex()
        raise AssertionError(method)

    rpc.endpoint = "unused"
    rpc.signing_chain = ChainId("eip155:31337")

    def validate():
        calls.append(("origin_verified", []))

    class Reader:
        def __init__(self, chain, *args):
            self.chain = chain

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def pin_block(self, *args):
            self._pinned = block.model_copy(update={"chain": self.chain})
            return self._pinned

        def _rpc(self, method, params):
            return rpc(method, params)

        def call(self, address, signature, block, **kwargs):
            if signature == "implementation()":
                return (beacon["implementation"],)
            slot = (
                BALANCES_NAMESPACE + 2
                if signature == "totalSupply()"
                else mapping_slot("address", FUND, BALANCES_NAMESPACE)
            )
            return (slots[slot],)

    monkeypatch.setattr(ethereum, "EthereumReader", Reader)
    return SimpleNamespace(
        origin=SimpleNamespace(chain=block.chain),
        metadata={"instanceId": "fixture"},
        rpc=rpc,
        _validate_origin=validate,
        calls=calls,
        codes=codes,
        slots=slots,
        implementation=beacon["implementation"],
    )


def test_raw_stock_funding_preserves_existing_supply_and_verifies_both_getters(node):
    fund_stock_storage(node, FUND, TOKEN, 19)
    assert node.calls[0] == ("origin_verified", [])
    writes = [params for method, params in node.calls if method == "anvil_setStorageAt"]
    assert len(writes) == 2 and all(params[0] == TOKEN for params in writes)
    assert node.slots[mapping_slot("address", FUND, BALANCES_NAMESPACE)] == 19
    assert node.slots[BALANCES_NAMESPACE + 2] == 1012


@pytest.mark.parametrize("amount", [-1, True, 2**255])
def test_invalid_stock_amount_cannot_edit_storage(node, amount):
    with pytest.raises(ValueError, match="verified Robinhood"):
        fund_stock_storage(node, FUND, TOKEN, amount)
    assert node.calls == [("origin_verified", [])]


@pytest.mark.parametrize(
    "change",
    [
        "origin",
        "chain",
        "asset",
        "proxy",
        "registry",
        "implementation",
        "beacon",
        "overflow",
        "underflow",
    ],
)
def test_unverified_stock_identity_or_supply_cannot_edit_storage(node, change):
    from almanak.gateway.ethereum import DataError

    token = TOKEN
    if change == "origin":

        def refuse():
            raise ValueError("unidentified fork")

        node._validate_origin = refuse
    elif change == "chain":
        node.origin.chain = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    elif change == "asset":
        token = FUND
    elif change == "proxy":
        node.codes[TOKEN] = "0x6000"
        from almanak.gateway.roles_storage import IMPLEMENTATION_SLOT

        node.slots[IMPLEMENTATION_SLOT] = 0
    elif change == "registry":
        node.codes[BEACON] = "0x6000"
    elif change == "implementation":
        node.codes[node.implementation] = "0x6000"
    elif change == "beacon":
        node.slots[BEACON_SLOT] = 0
    elif change == "overflow":
        node.slots[BALANCES_NAMESPACE + 2] = 2**256 - 1
    else:
        node.slots[BALANCES_NAMESPACE + 2] = 1
    amount = 19 if change != "underflow" else 0
    with pytest.raises((ValueError, DataError)):
        fund_stock_storage(node, FUND, token, amount)
    assert not any(method == "anvil_setStorageAt" for method, _ in node.calls)


def test_failed_fixture_readback_is_visible(node):
    original = node.rpc

    def broken(method, params):
        if method == "anvil_setStorageAt":
            return True
        return original(method, params)

    broken.endpoint = original.endpoint
    broken.signing_chain = original.signing_chain
    node.rpc = broken
    with pytest.raises(ValueError, match="capital or total supply"):
        fund_stock_storage(node, FUND, TOKEN, 19)
