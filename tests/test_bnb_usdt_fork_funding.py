"""Fork-only balance edits require the captured token revision and compiler layout."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.gateway.roles_storage import mapping_slot
from almanak.gateway.safe_funding import fund_bnb_usdt_storage
from almanak.models import Chain

DIRECTORY = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-bnb-discovery"
)

CAPTURE = json.loads(gzip.decompress((DIRECTORY / "curve-bnb-agent-tokens.json.gz").read_bytes()))
TOKEN = CAPTURE["tokens"][1]["address"]
FUND = "0x" + "12" * 20


@pytest.fixture
def node():
    calls = []

    def rpc(method, params):
        calls.append((method, params))
        return CAPTURE["tokens"][1]["code"]

    def validate():
        calls.append(("origin_verified", []))

    return SimpleNamespace(
        origin=SimpleNamespace(chain=Chain(chain_id=ChainId("eip155:56"), environment="mainnet")),
        rpc=rpc,
        _validate_origin=validate,
        calls=calls,
    )


def test_verified_origin_and_runtime_precede_the_exact_compiler_balance_slot(node):
    fund_bnb_usdt_storage(node, FUND, TOKEN, 10**19)
    assert node.calls == [
        ("origin_verified", []),
        ("eth_getCode", [TOKEN, "latest"]),
        (
            "anvil_setStorageAt",
            [
                TOKEN,
                "0x" + mapping_slot("address", FUND, 1).to_bytes(32, "big").hex(),
                "0x" + (10**19).to_bytes(32, "big").hex(),
            ],
        ),
    ]


@pytest.mark.parametrize("amount", [-1, True, 2**255])
def test_invalid_capital_cannot_edit_storage(node, amount):
    with pytest.raises(ValueError, match="verified BNB"):
        fund_bnb_usdt_storage(node, FUND, TOKEN, amount)
    assert node.calls == [("origin_verified", [])]


@pytest.mark.parametrize("changed", ["chain", "token", "code", "origin"])
def test_unknown_scope_or_revision_cannot_edit_storage(node, changed):
    token = TOKEN
    if changed == "chain":
        node.origin.chain = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    elif changed == "token":
        token = FUND
    elif changed == "code":
        node.rpc = lambda *args: "0x6000"
    else:

        def refuse():
            raise ValueError("unidentified fork")

        node._validate_origin = refuse
    with pytest.raises(ValueError):
        fund_bnb_usdt_storage(node, FUND, token, 1)
    assert not any(method == "anvil_setStorageAt" for method, _ in node.calls)
