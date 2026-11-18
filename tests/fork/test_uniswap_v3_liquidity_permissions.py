"""Deployed Roles checks for LP templates; STOP targets do not prove economics."""

from types import SimpleNamespace

import pytest
from eth_utils import to_checksum_address

from almanak.connectors.uniswap_v3 import abi
from almanak.connectors.uniswap_v3.authority import declared
from almanak.connectors.uniswap_v3.permissions import derive
from almanak.gateway.accounts import ContractCall, prepare_call
from almanak.gateway.permissions import installation_bundle
from almanak.gateway.safe_fixture import deploy_profile, owner_transaction
from almanak.models import AccountBinding, StrategyAccounts
from tests.fork.test_safe_permissions import rejected_submission
from tests.fork.test_uniswap_v3_permissions import permission_node as permission_node
from tests.uniswap_v3_support import parameters


def setup_lp(node):
    config = parameters(lp=True)
    owner = node.funded_signer("", 0, 10**18, 81)
    operator = node.funded_signer("", 0, 10**18, 82)
    profile, _ = deploy_profile(node.rpc, config.allocations[0].chain, owner, operator, "fund")
    accounts = StrategyAccounts(bindings=(AccountBinding(name="fund", profile=profile),))
    config = config.model_copy(update={"accounts": accounts})
    capability = declared(config).requirements[0]
    venue = "0x" + "bd" * 20
    target = capability.target.model_copy(
        update={
            "contracts": capability.target.contracts.model_copy(update={"position_manager": venue})
        }
    )
    capability = capability.model_copy(update={"target": target})
    grants = derive(profile, SimpleNamespace(requirements=(capability,)))
    node.rpc("anvil_setCode", [venue, "0x00"])
    for call in installation_bundle(SimpleNamespace(profile=profile, grants=grants)):
        owner_transaction(node.rpc, owner, profile.fund.address, call.to, call.data)
    return SimpleNamespace(
        node=node,
        operator=operator,
        profile=profile,
        accounts=accounts,
        capability=capability,
        target=target,
    )


def send(fixture, data):
    call = ContractCall(
        fund=fixture.profile.fund,
        to=fixture.target.contracts.position_manager,
        value=0,
        data="0x" + data.hex(),
    )
    wrapped = prepare_call(fixture.accounts, "fund", call)
    receipt = fixture.node.rpc.send(fixture.operator, to_checksum_address(wrapped.to), wrapped.data)
    assert receipt["status"] == "0x1"


def reject(fixture, name, data, *, value=0, operation=0):
    rejected_submission(
        fixture.node,
        fixture.operator,
        fixture.profile,
        (name, fixture.target.contracts.position_manager, "0x" + data.hex(), value, operation),
    )


def encode_tuple(name, type_, values):
    return abi.calldata(f"{name}({type_})", (type_,), (values,))


def multicall(*calls):
    return abi.calldata("multicall(bytes[])", ("bytes[]",), (calls,))


def exit_calls(fixture, nft=2**200, *, recipient=None, amount=1):
    decrease = encode_tuple("decreaseLiquidity", abi.DECREASE_PARAMS, (nft, 1, amount, 0, 100))
    collect = encode_tuple(
        "collect",
        abi.COLLECT_PARAMS,
        (nft, recipient or fixture.profile.fund.address, abi.MAX128, abi.MAX128),
    )
    burn = abi.calldata("burn(uint256)", ("uint256",), (nft,))
    return decrease, collect, burn


@pytest.mark.fork
def test_lp_exit_templates_reject_mutations_and_accept_broad_nft_ids(permission_node):
    fixture = setup_lp(permission_node)
    decrease, collect, burn = exit_calls(fixture)
    wrong = exit_calls(fixture, recipient="0x" + "ab" * 20)
    excessive = exit_calls(fixture, amount=2**128)
    for name, data in (
        ("wrong recipient", multicall(*wrong)),
        ("excess numeric bound", multicall(*excessive)),
        ("empty array", multicall()),
        ("extra call", multicall(decrease, collect, burn, burn)),
        ("changed selector", multicall(decrease, collect, b"\x12\x34\x56\x78" + burn[4:])),
        ("wrong order", multicall(collect, decrease, burn)),
    ):
        reject(fixture, name, data)
    reject(fixture, "value", multicall(decrease, collect), value=1)
    reject(fixture, "delegatecall", multicall(decrease, collect), operation=1)
    send(fixture, multicall(decrease, collect))
    send(fixture, multicall(decrease, collect, burn))
    send(fixture, collect)
    # Correlation and owned-NFT admission belong to the gateway under the accepted broad scope.
    send(fixture, multicall(decrease, collect, abi.calldata("burn(uint256)", ("uint256",), (99,))))


def mint_values(fixture, amounts, shift=0):
    target = fixture.target
    return (
        target.pool.currency0.reference,
        target.pool.currency1.reference,
        target.pool.fee,
        target.range.lower + shift,
        target.range.upper + shift,
        *amounts,
        0,
        0,
        target.fund.address,
        100,
    )


@pytest.mark.fork
def test_lp_mint_and_increase_bind_pool_and_recipient_with_any_ticks_and_amounts(permission_node):
    # Scope-only grants (blueprint 27): ticks and deposit amounts are gateway checks, and
    # mints and increases repeat with no shared deposit quota.
    fixture = setup_lp(permission_node)
    values = mint_values(fixture, (10**18, 10**9))
    for name, index, value in (
        ("asset", 0, "0x" + "ab" * 20),
        ("fee", 2, values[2] + 1),
        ("recipient", 9, "0x" + "ab" * 20),
    ):
        changed = list(values)
        changed[index] = value
        reject(fixture, name, encode_tuple("mint", abi.MINT_PARAMS, tuple(changed)))
    spacing = fixture.target.pool.tick_spacing
    for minted in (values, mint_values(fixture, (2**128 - 1, 0), shift=spacing), values):
        send(fixture, encode_tuple("mint", abi.MINT_PARAMS, minted))
    increase = encode_tuple(
        "increaseLiquidity", abi.INCREASE_PARAMS, (2**200, 10**18, 1, 0, 0, 100)
    )
    send(fixture, increase)
    send(fixture, increase)
