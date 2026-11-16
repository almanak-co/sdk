"""Installed Pancake LP Roles predicates reject malformed manager calls."""

import json

import pytest

from almanak.connectors.pancakeswap_v3 import abi
from tests.fork.test_pancakeswap_v3_foreign_nft import (
    ROOT,
    prepare,
)
from tests.fork.test_pancakeswap_v3_foreign_nft import (
    permission_node as permission_node,
)
from tests.fork.test_safe_permissions import rejected_submission

pytestmark = pytest.mark.fork


def mint_attacks(fixture):
    target = fixture.target
    values = (
        target.assets[0].reference,
        target.assets[1].reference,
        target.pool.fee,
        target.range.lower,
        target.range.upper,
        1,
        1,
        0,
        0,
        target.fund.address,
        100,
    )
    for name, index, replacement in (
        ("wrong mint token0", 0, "0x" + "ab" * 20),
        ("wrong mint token1", 1, "0x" + "ab" * 20),
        ("wrong mint fee", 2, target.pool.fee + 1),
        ("wrong mint recipient", 9, "0x" + "ab" * 20),
        ("zero mint deadline", 10, 0),
    ):
        changed = list(values)
        changed[index] = replacement
        yield name, abi.calldata(f"mint({abi.MINT_PARAMS})", (abi.MINT_PARAMS,), (changed,)), 0, 0
    valid = abi.calldata(f"mint({abi.MINT_PARAMS})", (abi.MINT_PARAMS,), (values,))
    yield "native mint value", valid, 1, 0
    yield "delegatecall mint", valid, 0, 1


def exit_attacks(fixture):
    target = fixture.target
    decrease = abi.calldata(
        f"decreaseLiquidity({abi.DECREASE_PARAMS})", (abi.DECREASE_PARAMS,), ((1, 1, 0, 0, 100),)
    )
    collect = abi.calldata(
        f"collect({abi.COLLECT_PARAMS})",
        (abi.COLLECT_PARAMS,),
        ((1, target.fund.address, abi.MAX128, abi.MAX128),),
    )
    burn = abi.calldata("burn(uint256)", ("uint256",), (1,))
    other = abi.calldata(
        f"collect({abi.COLLECT_PARAMS})",
        (abi.COLLECT_PARAMS,),
        ((1, "0x" + "ab" * 20, abi.MAX128, abi.MAX128),),
    )
    for name, calls in (
        ("extra exit call", (decrease, collect, burn, burn)),
        ("missing exit collect", (decrease, burn)),
        ("reordered exit", (collect, decrease, burn)),
        ("wrong exit recipient", (decrease, other, burn)),
        ("changed exit selector", (decrease, collect, bytes(4) + burn[4:])),
    ):
        yield name, abi.calldata("multicall(bytes[])", ("bytes[]",), (calls,)), 0, 0


def test_generated_lp_predicates_reject_direct_roles_attacks(permission_node):
    fixture = prepare(permission_node)
    results = [
        rejected_submission(
            fixture.node,
            fixture.operator,
            fixture.profile,
            (name, fixture.target.contracts.position_manager, "0x" + data.hex(), value, operation),
        )
        for name, data, value, operation in (*mint_attacks(fixture), *exit_attacks(fixture))
    ]
    path = (
        ROOT
        / "artifacts/pancakeswap-v3-authority"
        / permission_node.proof_network
        / "forbidden-lp.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(results, indent=2) + "\n")
