"""A pinned-runtime balance slot is edited only on its chain, token and exact runtime."""

from types import SimpleNamespace

import pytest
from eth_utils import keccak

from almanak.caip import ChainId
from almanak.gateway.roles_storage import mapping_slot
from almanak.gateway.safe_funding import VerifiedSlot, fund_verified_slot
from almanak.models import Chain

RUNTIME = "0x6080604052"
TOKEN = "0x" + "ab" * 20
FUND = "0x" + "12" * 20
LAYOUT = VerifiedSlot(
    chain=ChainId("eip155:8453"),
    token=TOKEN,
    runtime_hash=keccak(bytes.fromhex(RUNTIME[2:])).hex(),
    slot=3,
    deployment_refusal="needs its deployment",
    runtime_refusal="needs its runtime",
)


def node(chain="eip155:8453", runtime=RUNTIME):
    calls = []

    def rpc(method, params):
        calls.append((method, params))
        return runtime

    return SimpleNamespace(
        origin=SimpleNamespace(chain=Chain(chain_id=ChainId(chain), environment="mainnet")),
        rpc=rpc,
        _validate_origin=lambda: calls.append(("origin_verified", [])),
        calls=calls,
    )


def test_the_verified_runtime_receives_the_balance_at_its_compiled_slot():
    fork = node()
    fund_verified_slot(fork, FUND, TOKEN, 7, LAYOUT)
    assert fork.calls == [
        ("origin_verified", []),
        ("eth_getCode", [TOKEN, "latest"]),
        (
            "anvil_setStorageAt",
            [
                TOKEN,
                "0x" + mapping_slot("address", FUND, 3).to_bytes(32, "big").hex(),
                "0x" + (7).to_bytes(32, "big").hex(),
            ],
        ),
    ]


@pytest.mark.parametrize(
    "chain,token,amount",
    [
        ("eip155:1", TOKEN, 1),
        ("eip155:8453", "0x" + "cd" * 20, 1),
        ("eip155:8453", TOKEN, True),
        ("eip155:8453", TOKEN, -1),
        ("eip155:8453", TOKEN, 2**255),
    ],
)
def test_another_deployment_or_amount_is_refused_before_any_read(chain, token, amount):
    fork = node(chain)
    with pytest.raises(ValueError, match="needs its deployment"):
        fund_verified_slot(fork, FUND, token, amount, LAYOUT)
    assert fork.calls == [("origin_verified", [])]


def test_a_different_runtime_is_refused_without_a_storage_edit():
    fork = node(runtime="0x6080604053")
    with pytest.raises(ValueError, match="needs its runtime"):
        fund_verified_slot(fork, FUND, TOKEN, 1, LAYOUT)
    assert [method for method, _ in fork.calls] == ["origin_verified", "eth_getCode"]
