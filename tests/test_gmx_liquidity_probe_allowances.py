"""The GM permission baseline grants request-sized allowances only through a call override."""

import pytest

from tests.gmx_liquidity_permissions import allowance_slot, request_allowances, word

TOKEN = "0x" + "aa" * 20
OTHER = "0x" + "bb" * 20
IMPLEMENTATION = "0x" + "11" * 32
ALLOWANCE_KEY = "0x" + "22" * 32


class Node:
    """A proxy token that reads its implementation slot before the allowance slot."""

    def __init__(self, keys):
        self.keys = keys
        self.calls = []

    def rpc(self, method, params):
        self.calls.append((method, params))
        if method == "eth_createAccessList":
            return {"accessList": [{"address": TOKEN.upper(), "storageKeys": self.keys}]}
        assert method == "eth_call"
        changed = params[2].get(params[0]["to"], {}).get("stateDiff", {})
        return changed.get(ALLOWANCE_KEY, word(0))


def test_allowance_slot_is_the_key_that_echoes_the_probe():
    node = Node([IMPLEMENTATION, ALLOWANCE_KEY])
    assert allowance_slot(node, TOKEN, OTHER, OTHER) == ALLOWANCE_KEY
    assert all(method != "eth_sendTransaction" for method, _ in node.calls)


def test_missing_allowance_key_refuses():
    with pytest.raises(AssertionError, match="no allowance storage key"):
        allowance_slot(Node([IMPLEMENTATION]), TOKEN, OTHER, OTHER)


class Asset:
    reference = TOKEN


class Value:
    def __init__(self, amount):
        self.asset, self.amount = Asset(), amount


class Operation:
    class target:
        class fund:
            address = OTHER

        class market:
            class contracts:
                token_router = OTHER

    inputs = (Value(0), Value(7))


def test_request_allowances_override_only_nonzero_inputs_with_their_exact_amount():
    overrides = request_allowances(Node([ALLOWANCE_KEY]), Operation())
    assert overrides == {TOKEN: {"stateDiff": {ALLOWANCE_KEY: word(7)}}}
