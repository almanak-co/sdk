"""Derived GMX Roles grants carry scope only (blueprint 27) and refuse ambiguous templates."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_abi import decode, encode

from almanak.connectors.gmx_v2 import permissions
from almanak.connectors.gmx_v2.candidate_permissions import (
    OrderScope,
    candidate_cancellation_grants,
    candidate_order_grants,
    word_conditions,
)
from almanak.connectors.gmx_v2.domain import Target
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.gmx_v2.exchange_models import ExchangeCapability
from almanak.connectors.gmx_v2.liquidity_models import LiquidityCapability
from almanak.connectors.gmx_v2.models import Capabilities, Capability
from almanak.gateway.roles_conditions import any_amount, bounded, equal
from almanak.models import Account
from scripts.legacy_configuration.gmx_authoring import RETIRED
from tests.gmx_synthetic_frames import FUND, author
from tests.scope_trees import assert_same_scope

ALLOWANCE, GREATER, LESS = 28, 17, 18


def fund(allocation):
    return Account(chain=allocation.market.chain, address=FUND)


def position(name):
    allocation = author(name, "position").allocations[0]
    target = Target(
        allocation=allocation.name,
        account=allocation.account,
        fund=fund(allocation),
        market=allocation.market,
        collateral_asset=allocation.capital.asset,
        side=allocation.side,
    )
    return allocation, Capability(
        target=target, maximum_execution_fee=allocation.execution_fee.amount
    )


def exchange_target(allocation):
    return ExchangeTarget(
        allocation=allocation.name,
        account=allocation.account,
        fund=fund(allocation),
        market=allocation.market,
        funding_asset=allocation.funding_asset,
    )


def swap(name):
    allocation = author(name, "swap").allocations[0]
    capability = ExchangeCapability(
        target=exchange_target(allocation), maximum_execution_fee=allocation.execution_fee.amount
    )
    return allocation, capability


def liquidity(name):
    allocation = author(name, "liquidity").allocations[0]
    capability = LiquidityCapability(
        target=exchange_target(allocation),
        minimum_market_tokens=allocation.minimum_market_tokens,
        minimum_outputs=allocation.withdrawal_minimums,
        maximum_execution_fee=allocation.execution_fee.amount,
    )
    return allocation, capability


KINDS = {"position": position, "swap": swap, "liquidity": liquidity}


@pytest.fixture(params=[(n, k) for n in ("arbitrum", "avalanche") for k in KINDS], ids=str)
def derived(request):
    name, kind = request.param
    allocation, capability = KINDS[kind](name)
    profile = SimpleNamespace(fund=capability.target.fund)
    grants = permissions.derive(profile, Capabilities(requirements=(capability,)))
    return SimpleNamespace(kind=kind, allocation=allocation, capability=capability, grants=grants)


def nodes(condition):
    yield condition
    for child in condition.children:
        yield from nodes(child)


def compared(grants, operator):
    return [
        int.from_bytes(bytes.fromhex(node.value[2:]), "big")
        for grant in grants
        if grant.condition is not None
        for node in nodes(grant.condition)
        if node.operator == operator
    ]


def grant(grants, signature, target=None):
    (selected,) = (
        g for g in grants if g.signature == signature and (target is None or g.target == target)
    )
    return selected


# The decrease branch's createOrder word 3, initialCollateralDeltaAmount, in the retained
# multicall condition: OR(increase, decrease) > calls > createOrder > its argument words.
DECREASE_COLLATERAL = (1, 0, 1, 0, 3)


def test_generated_grants_reproduce_the_actual_installed_fork_package():
    """Every scope word the identified fork installed, by value (blueprint 27).

    Author amount bounds and their quotas are history. The review package also pinned a
    decrease's collateral withdrawal to zero: that was its author bound
    `maximum_collateral_withdrawal_base_units` at zero, which blueprint 27 section 9
    removes, so a decrease now withdraws any amount. Every other word must match.
    """
    path = (
        Path(__file__).parents[1]
        / "docs/internal/reports/evidence/2026-09-08-gmx-request-fixtures"
        / "arbitrum-candidate-safe-authority.json.gz"
    )
    proof = json.loads(gzip.decompress(path.read_bytes()))
    retired = {name: proof["scope"].pop(name) for name in RETIRED if name in proof["scope"]}
    assert retired.get("maximum_collateral_withdrawal_base_units", 0) == 0
    scope = OrderScope.model_validate_json(json.dumps(proof["scope"]))
    grants = (
        *candidate_order_grants(scope),
        *candidate_cancellation_grants(scope.exchange_router),
    )
    (creation,) = (g for g in proof["grants"] if g["signature"] == "multicall(bytes[])")
    node = creation["condition"]
    for index in DECREASE_COLLATERAL[:-1]:
        node = node["children"][index]
    word = node["children"][DECREASE_COLLATERAL[-1]]
    assert word == equal("uint256", 0).model_dump(mode="json")
    node["children"][DECREASE_COLLATERAL[-1]] = any_amount().model_dump(mode="json")
    assert_same_scope(grants, proof["grants"], {scope.maximum_execution_fee})


def test_derived_grants_carry_no_allowance_and_no_author_amount_bound(derived):
    """Teardown invariant: only the keeper fee bound remains, so no author amount can refuse
    an exit, a withdrawal or a revocation."""
    assert not compared(derived.grants, ALLOWANCE)
    fee = derived.allocation.execution_fee.amount
    assert set(compared(derived.grants, LESS)) <= {fee + 1}
    assert not any("quota" in line.lower() for g in derived.grants for line in g.explanation)


def test_approvals_pin_the_router_spender_and_take_any_amount(derived):
    market = derived.capability.target.market
    approvals = [g for g in derived.grants if g.signature == "approve(address,uint256)"]
    assert approvals
    for approval in approvals:
        spender, amount = approval.condition.children
        assert spender.operator == 16
        assert decode(("address",), bytes.fromhex(spender.value[2:]))[0].lower() == (
            market.contracts.token_router.lower()
        )
        # Zero (revocation) or any positive amount.
        assert {child.operator for child in amount.children} == {16, GREATER}


def test_request_scope_keeps_receivers_fee_and_floors(derived):
    target = derived.capability.target
    creation = grant(derived.grants, "multicall(bytes[])", target.market.contracts.exchange_router)
    equalities = {
        node.value
        for node in nodes(creation.condition)
        if node.operator == 16 and len(node.value) == 66
    }
    word = "0x" + encode(("address",), (target.fund.address,)).hex()
    assert word in equalities
    fee = derived.allocation.execution_fee.amount
    if derived.kind == "position":
        # The order's executionFee, and a decrease's fee-only escrow, stay within the author fee.
        assert set(compared((creation,), LESS)) == {fee + 1}
    if derived.kind == "liquidity":
        floors = set(compared((creation,), GREATER))
        for minimum in derived.allocation.withdrawal_minimums.root:
            if minimum.amount:
                assert minimum.amount - 1 in floors
    if derived.kind == "swap":
        # A wrapped-native input escrows more than its keeper fee; outputs stay positive.
        assert fee in compared((creation,), GREATER)


def test_cancellation_and_claims_keep_their_scope(derived):
    router = derived.capability.target.market.contracts.exchange_router
    cancels = {g.signature for g in derived.grants if g.signature.startswith("cancel")}
    if derived.kind == "position":
        claim = grant(derived.grants, "claimFundingFees(address[],address[],address)", router)
        assert claim.condition is not None
        assert cancels == {
            "cancelOrder(bytes32)",
            "cancelDeposit(bytes32)",
            "cancelWithdrawal(bytes32)",
        }
    elif derived.kind == "swap":
        assert cancels == {"cancelOrder(bytes32)"}
    else:
        assert cancels == {"cancelDeposit(bytes32)", "cancelWithdrawal(bytes32)"}
    for signature in cancels:
        assert compared((grant(derived.grants, signature, router),), GREATER) == [0]


def test_derivation_ignores_capabilities_of_another_fund(derived):
    other = SimpleNamespace(
        fund=Account(chain=derived.capability.target.fund.chain, address="0x" + "34" * 20)
    )
    assert permissions.derive(other, Capabilities(requirements=(derived.capability,))) == ()


@pytest.mark.parametrize("words", [(0,), (7, 7)])
def test_permission_variable_must_exist_exactly_once(words):
    with pytest.raises(ValueError, match="exactly one ABI word"):
        word_conditions(encode(("uint256",) * len(words), words), {7: bounded(10)})


def test_permission_template_refuses_partial_words():
    with pytest.raises(ValueError, match="whole ABI words"):
        word_conditions(bytes(31), {})
