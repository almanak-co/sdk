"""Deployed Roles admission for generated V3 trading templates, without venue execution."""

import json
from types import SimpleNamespace

import pytest
from eth_utils import to_checksum_address

from almanak.connectors.uniswap_v3.abi import SWAP_PARAMS, calldata
from almanak.connectors.uniswap_v3.authority import declared
from almanak.connectors.uniswap_v3.permissions import derive
from almanak.gateway.accounts import ContractCall, prepare_call
from almanak.gateway.fork_config import ForkTarget
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.permissions import installation_bundle
from almanak.gateway.providers import AlchemyProvider
from almanak.gateway.roles_storage import ARTIFACTS
from almanak.gateway.safe_fixture import deploy_profile, owner_transaction
from almanak.models import AccountBinding, Block, StrategyAccounts
from tests.fork.test_safe_permissions import rejected_submission
from tests.uniswap_v3_support import parameters


@pytest.fixture
def permission_node(request):
    pin = Block.model_validate_json(
        json.dumps(
            json.loads((ARTIFACTS / "ethereum-25908472-authority.json").read_text())["block"]
        )
    )
    target = ForkTarget(
        chain=pin.chain,
        block=pin,
        provider=AlchemyProvider(
            kind="alchemy", network="eth-mainnet", api_key_env="ALCHEMY_API_KEY"
        ),
    )
    with Fork(request.config.getoption("--fork-env"), pin, target=target) as node:
        yield node


def setup_trading(node):
    config = parameters()
    owner = node.funded_signer("", 0, 10**18, 81)
    operator = node.funded_signer("", 0, 10**18, 82)
    profile, _ = deploy_profile(node.rpc, config.allocations[0].chain, owner, operator, "fund")
    accounts = StrategyAccounts(bindings=(AccountBinding(name="fund", profile=profile),))
    config = config.model_copy(update={"accounts": accounts})
    capability = declared(config).requirements[0]
    # STOP isolates Roles admission. This cannot establish router or economic correctness.
    venue = "0x" + "bd" * 20
    target = capability.target.model_copy(
        update={"contracts": capability.target.contracts.model_copy(update={"router": venue})}
    )
    capability = capability.model_copy(update={"target": target})
    grants = derive(profile, SimpleNamespace(requirements=(capability,)))
    node.rpc("anvil_setCode", [venue, "0x00"])
    partition = SimpleNamespace(profile=profile, grants=grants)
    for call in installation_bundle(partition):
        owner_transaction(node.rpc, owner, profile.fund.address, call.to, call.data)
    return operator, profile, accounts, capability


def swap_bytes(values):
    return calldata(f"exactInputSingle({SWAP_PARAMS})", (SWAP_PARAMS,), (values,))


def outer(calls, deadline=100):
    return (
        "0x"
        + calldata("multicall(uint256,bytes[])", ("uint256", "bytes[]"), (deadline, calls)).hex()
    )


def attacks(values):
    valid = swap_bytes(values)
    cases = [
        ("changed nested selector", (b"\x12\x34\x56\x78" + valid[4:],)),
        ("extra nested call", (valid, valid)),
        ("empty nested array", ()),
        ("truncated nested call", (valid[:4],)),
    ]
    for name, index, replacement in (
        ("wrong token", 0, "0x" + "ab" * 20),
        ("wrong output", 1, "0x" + "ab" * 20),
        ("wrong fee", 2, values[2] + 1),
        ("wrong recipient", 3, "0x" + "ab" * 20),
        ("zero input", 4, 0),
        ("zero output bound", 5, 0),
        ("alternate price limit", 6, 1),
    ):
        changed = list(values)
        changed[index] = replacement
        cases.append((name, (swap_bytes(tuple(changed)),)))
    return cases


@pytest.mark.fork
def test_generated_nested_selector_array_recipient_and_repeated_swaps(permission_node):
    node = permission_node
    operator, profile, accounts, capability = setup_trading(node)
    target = capability.target
    values = (
        target.pool.currency0.reference,
        target.pool.currency1.reference,
        target.pool.fee,
        profile.fund.address,
        10**18,
        1,
        0,
    )
    for name, calls in attacks(values):
        rejected_submission(
            node,
            operator,
            profile,
            (name, target.contracts.router, outer(calls), 0, 0),
            error="execution reverted" if name == "truncated nested call" else "0xd0a9bf58",
        )
    rejected_submission(
        node,
        operator,
        profile,
        ("zero deadline", target.contracts.router, outer((swap_bytes(values),), 0), 0, 0),
    )
    data = outer((swap_bytes(values),))
    call = ContractCall(fund=profile.fund, to=target.contracts.router, value=0, data=data)
    wrapped = prepare_call(accounts, "fund", call)
    # Scope-only grants (blueprint 27): the same swap is admitted again with no quota.
    for _ in range(2):
        receipt = node.rpc.send(operator, to_checksum_address(wrapped.to), wrapped.data)
        assert receipt["status"] == "0x1"
