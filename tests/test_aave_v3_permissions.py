"""Scope-only exported Roles predicates; installed authority requires fork evidence."""

import pytest
from eth_abi import decode

from almanak.connectors.aave_v3.authority import declared
from almanak.connectors.aave_v3.permissions import derive
from almanak.models import SafeRoles
from tests.aave_v3_helpers import recorded_v3 as recorded_v3
from tests.test_aave_v3_config import parameters as parameters


def profile(config):
    fund = config.accounts.bindings[0].profile.fund
    return SafeRoles(
        kind="safe_roles",
        fund=fund,
        operator=fund.model_copy(update={"address": "0x" + "12" * 20}),
        roles_module="0x" + "34" * 20,
        role_key="0x" + "56" * 32,
    )


def value(condition, kind="uint256"):
    return decode((kind,), bytes.fromhex(condition.value[2:]))[0]


def branches(grant):
    """The per-allocation conditions combined into one grant for its selector."""
    return grant.condition.children if grant.condition.operator == 2 else (grant.condition,)


def grant(grants, signature):
    return next(g for g in grants if g.signature.startswith(signature + "("))


def by_asset(grant):
    return {value(branch.children[0], "address"): branch.children for branch in branches(grant)}


def without_borrowing(config):
    allocations = tuple(a.model_copy(update={"borrow": False}) for a in config.allocations)
    return config.model_copy(update={"allocations": allocations})


def test_supply_grants_scope_asset_and_fund_with_any_positive_amount(parameters):
    safe = profile(parameters)
    grants = derive(safe, declared(parameters))
    supply = grant(grants, "supply")
    assert supply.target == parameters.allocations[0].reserve.pool
    # Supply is always granted, whatever an allocation's capital or borrowing.
    assert set(by_asset(supply)) == {a.reserve.asset.reference for a in parameters.allocations}
    for args in by_asset(supply).values():
        assert args[1].operator == 17 and value(args[1]) == 0
        assert value(args[2], "address") == safe.fund.address
        assert value(args[3], "uint16") == 0


def test_borrow_grant_is_variable_debt_to_the_safe_for_a_borrowing_allocation(parameters):
    safe = profile(parameters)
    borrow = grant(derive(safe, declared(parameters)), "borrow")
    loan = parameters.target("loan")
    assert borrow.target == loan.reserve.pool
    (args,) = (branch.children for branch in branches(borrow))
    assert value(args[0], "address") == loan.asset.reference
    assert args[1].operator == 17 and value(args[1]) == 0
    assert value(args[2]) == 2
    assert value(args[3], "uint16") == 0
    assert value(args[4], "address") == safe.fund.address


def test_no_grant_carries_an_amount_bound_or_quota(parameters):
    grants = derive(profile(parameters), declared(parameters))
    operators = {c.operator for g in grants for c in g.conditions}
    # 18 is a less-than bound and 28 a Roles allowance; scope-only grants use neither.
    assert not operators & {18, 28}


def test_borrow_and_repay_grants_follow_the_borrow_flag(parameters):
    safe = profile(parameters)
    lending = derive(safe, declared(without_borrowing(parameters)))
    assert not [g for g in lending if g.signature.startswith(("borrow(", "repay("))]
    assert {g.signature.split("(")[0] for g in lending} == {
        "supply",
        "withdraw",
        "setUserUseReserveAsCollateral",
        "approve",
    }
    first, loan = parameters.allocations
    both = parameters.model_copy(
        update={"allocations": (first.model_copy(update={"borrow": True}), loan)}
    )
    grants = derive(safe, declared(both))
    for signature in ("borrow", "repay"):
        assert set(by_asset(grant(grants, signature))) == {
            first.reserve.asset.reference,
            loan.reserve.asset.reference,
        }


def test_repayment_is_scoped_to_the_safe_debt_with_any_amount(parameters):
    safe = profile(parameters)
    grants = derive(safe, declared(parameters))
    loan = parameters.target("loan")
    (args,) = by_asset(grant(grants, "repay")).values()
    # Any nonzero amount: the Pool caps a repayment at the Safe's own debt.
    assert value(args[0], "address") == loan.asset.reference
    assert args[1].operator == 17
    assert value(args[1]) == 0
    assert value(args[2]) == 2
    assert value(args[3], "address") == safe.fund.address


def test_collateral_control_follows_the_collateral_flag(parameters):
    grants = derive(profile(parameters), declared(parameters))
    first, loan = (a.reserve.asset.reference for a in parameters.allocations)
    control = by_asset(grant(grants, "setUserUseReserveAsCollateral"))
    enabled = control[first][1]
    assert enabled.operator == 2
    assert {value(child, "bool") for child in enabled.children} == {True, False}
    assert value(control[loan][1], "bool") is False


def test_cleanup_permissions_keep_the_pool_asset_and_withdrawal_recipient(parameters):
    safe = profile(parameters)
    grants = derive(safe, declared(parameters))
    approvals = [g for g in grants if g.signature.startswith("approve(")]
    assert len(approvals) == 2
    assert {g.target for g in approvals} == {
        a.reserve.asset.reference for a in parameters.allocations
    }
    for approval in approvals:
        assert (
            value(approval.condition.children[0], "address")
            == parameters.allocations[0].reserve.pool
        )
        revoke, amount = approval.condition.children[1].children
        assert value(revoke) == 0
        assert amount.operator == 17 and value(amount) == 0
    withdraw = by_asset(grant(grants, "withdraw"))
    assert set(withdraw) == {a.reserve.asset.reference for a in parameters.allocations}
    for args in withdraw.values():
        assert args[1].operator == 17
        assert value(args[1]) == 0
        assert value(args[2], "address") == safe.fund.address
    assert all(g.conditions for g in grants)


def test_other_fund_cannot_widen_authority(parameters):
    safe = profile(parameters)
    other = safe.model_copy(
        update={"fund": safe.fund.model_copy(update={"address": "0x" + "78" * 20})}
    )
    assert derive(other, declared(parameters)) == ()


def test_permission_identities_pin_reserve_tokens_and_configured_oracle(recorded_v3, monkeypatch):
    from types import SimpleNamespace

    from almanak.connectors.aave_v3 import permissions

    state = recorded_v3
    reserves = {o.target.reserve: o.position.reserve for o in state.baseline}
    block = state.baseline[0].position.reserve.block
    calls = []

    def read(reader, source, at):
        assert at == block
        calls.append(source)
        return reserves[source]

    monkeypatch.setattr(permissions, "read_reserve", read)
    reader = SimpleNamespace(reader=object(), block=block, identity=lambda address: address)
    result = permissions.identities(state.config, "fund", reader)
    expected = {
        address
        for allocation in state.config.allocations
        for address in (
            allocation.reserve.pool,
            allocation.reserve.asset.reference,
            reserves[allocation.reserve].a_token.reference,
            reserves[allocation.reserve].variable_debt_token.reference,
            allocation.oracle,
            allocation.price_source,
        )
    }
    assert set(result) == expected
    assert len(calls) == 2
    assert permissions.identities(state.config, "unbound", reader) == ()
    first = state.config.allocations[0]
    reserves[first.reserve] = reserves[first.reserve].model_copy(update={"oracle": None})
    with pytest.raises(ValueError, match="oracle source differs"):
        permissions.identities(state.config, "fund", reader)
