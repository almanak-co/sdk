"""Generated authority remains reproducible, with explicit broader NFT exits."""

import json
from types import SimpleNamespace

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.uniswap_v4 import deployments, permissions
from almanak.gateway.ethereum import DataError
from almanak.gateway.roles_conditions import (
    any_amount,
    bounded,
    equal,
    flatten,
    inline,
    matches,
    packed,
    positive,
)
from tests.scope_trees import assert_same_scope
from tests.uniswap_support import ROOT, live_state


def retained_budgets(name):
    """The author amounts the capture's former quotas were sized from."""
    raw = json.loads((ROOT / f"tests/fixtures/uniswap-v4-{name}-lifecycle.json").read_text())
    allocation = raw["config"]["allocations"][0]
    return {
        allocation[field]["amount"]
        for field in ("capital", "maximum0", "maximum1")
        if allocation.get(field) is not None
    }


def comparison_values(grant):
    return {
        int.from_bytes(bytes.fromhex(c.value[2:]), "big")
        for c in grant.conditions
        if c.operator in (17, 18)
    }


@pytest.mark.parametrize("name", ["trading", "lp"])
def test_derived_grants_keep_the_installed_scope_without_amount_terms(name):
    state = live_state(name)
    partition = state.permissions.partitions[0]
    grants = permissions.derive(partition.profile, state.capabilities)
    # Scope-only authority (blueprint 27): every scope word of the installed fork evidence
    # by value, with no allowance term and no author-sized bound.
    assert_same_scope(grants, partition.grants)
    assert all(c.operator != 28 for g in grants for c in g.conditions)
    budgets = retained_budgets(name)
    for grant in grants:
        assert comparison_values(grant).isdisjoint(budgets | {b + 1 for b in budgets})
        assert not any("quota" in text or "ceiling" in text for text in grant.explanation)
    assert all(len(g.conditions) <= 256 for g in grants)
    if name == "lp":
        assert any("Broad NFT exit" in text for g in grants for text in g.explanation)
    other = partition.profile.model_copy(
        update={"fund": partition.profile.fund.model_copy(update={"address": "0x" + "12" * 20})}
    )
    assert permissions.derive(other, state.capabilities) == ()


def test_approvals_pin_the_exact_spender_with_any_amount():
    state = live_state("lp")
    target = state.capabilities.requirements[0].target
    grants = permissions.derive(state.permissions.partitions[0].profile, state.capabilities)
    erc20 = [g for g in grants if g.signature == "approve(address,uint256)"]
    assert {g.target for g in erc20} == {a.reference for a in target.assets}
    for grant in erc20:
        assert grant.condition == matches(equal("address", target.contracts.permit2), any_amount())
    permit = next(g for g in grants if g.signature == "approve(address,address,uint160,uint48)")
    spenders = {target.contracts.router, target.contracts.position_manager}
    branches = permit.condition.children
    assert len(branches) == 2 * len(spenders)
    for branch in branches:
        token, spender, amount, expiry = branch.children
        assert token in {equal("address", a.reference) for a in target.assets}
        assert spender in {equal("address", s) for s in spenders}
        assert amount == any_amount()
        assert expiry == permissions.zero_or(2**48 - 1)


@pytest.mark.parametrize("name", ["trading", "lp"])
def test_swap_amount_words_take_any_nonzero_input(name):
    state = live_state(name)
    target = state.capabilities.requirements[0].target
    sanity = bounded(permissions.MAXIMUM)
    for direction in (True, False):
        grant = permissions.swap_grant(target, direction)
        words = grant.condition.children[1].children[0].children
        # amountIn (uint128) and the settlement maximum (uint256) carry the input; the
        # two minimum output words stay nonzero. None is sized from an author field.
        assert words.count(positive()) == 1
        assert words.count(sanity) == 3
        assert all(w.operator == 16 for w in words if w not in (positive(), sanity))


def test_dynamic_equality_packing_matches_deployed_roles_packer():
    dynamic = flatten(permissions.bytes_equal(b"\x10"))
    assert not inline(dynamic, 0)
    assert packed(dynamic)[2:] == keccak(encode(("bytes",), (b"\x10",))[32:])
    static = flatten(equal("uint256", 5))
    assert inline(static, 0)
    assert packed(static)[2:] == keccak(encode(("uint256",), (5,)))
    nested = flatten(matches(equal("uint256", 5), parameter=3))
    assert inline(nested, 0)


def test_ambiguous_template_marker_fails_compilation():
    marker = permissions.MARKERS[0]
    payload = encode(("uint256", "uint256"), (marker, marker))
    with pytest.raises(ValueError, match="ambiguous variable"):
        permissions.words_condition(payload, {marker: (equal("uint256", 1),)})


@pytest.mark.parametrize("mutation", ["code", "pool_manager", "chain", "role"])
def test_deployment_pins_and_periphery_relationships_are_independent_checks(monkeypatch, mutation):
    state = live_state()
    config = state.config
    pins = {c.address: c for c in deployments.pinned_identities()}
    manager = config.allocations[0].pool.pool_manager
    monkeypatch.setattr(deployments, "read_pool", lambda *_: None)
    if mutation == "chain":
        config = config.model_copy(
            update={
                "fork_origin": config.fork_origin.model_copy(
                    update={"chain": config.accounts.bindings[0].profile.fund.chain}
                )
            }
        )
    if mutation == "role":
        allocation = config.allocations[0]
        allocation = allocation.model_copy(
            update={
                "contracts": allocation.contracts.model_copy(
                    update={"router": allocation.contracts.quoter}
                )
            }
        )
        config = config.model_copy(update={"allocations": (allocation,)})

    def identity(address):
        item = pins[address]
        return (
            item.model_copy(update={"code_hash": "0x" + "12" * 32}) if mutation == "code" else item
        )

    reader = SimpleNamespace(
        reader=None,
        block=None,
        identity=identity,
        call=lambda *_: (("0x" + "12" * 20) if mutation == "pool_manager" else manager,),
    )
    with pytest.raises(DataError):
        deployments.identities(config, config.allocations[0].account, reader)
    reader.call = lambda *_: (manager,)
    reader.identity = lambda address: pins[address]
    assert deployments.identities(state.config, state.config.allocations[0].account, reader)
