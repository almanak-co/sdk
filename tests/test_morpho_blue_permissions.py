"""Morpho Roles trees preserve complete market and fund correlations."""

from eth_abi import decode

from almanak.connectors.morpho_blue.authority import declared
from almanak.connectors.morpho_blue.permissions import MorphoPermissions, derive
from almanak.models import SafeRoles
from tests.test_morpho_blue_config import parameters as parameters


def profile(parameters, name):
    fund = parameters.accounts.binding(name).profile.fund
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
    return grant.condition.children if grant.condition.operator == 2 else (grant.condition,)


def operators(condition):
    yield condition.operator
    for child in condition.children:
        yield from operators(child)


def signatures(grants):
    return {grant.signature.split("(")[0] for grant in grants}


def assert_market_and_safe(branch, market, fund):
    params = branch.children[0]
    assert params.parameter == 3
    kinds = ("address", "address", "address", "address", "uint256")
    assert (
        tuple(value(c, k) for c, k in zip(params.children, kinds, strict=True)) == market.parameters
    )
    assert value(branch.children[-2], "address") == fund.address


def test_scope_only_grants_preserve_exact_market_safe_and_callbacks(parameters):
    safe = profile(parameters, "borrower")
    grants = derive(safe, declared(parameters))
    market = parameters.target("loan").reserve.market
    assert signatures(grants) == {
        "approve",
        "supplyCollateral",
        "withdrawCollateral",
        "supply",
        "withdraw",
        "borrow",
        "repay",
    }
    for grant in grants:
        assert 28 not in operators(grant.condition)
        if grant.signature.startswith("approve"):
            continue
        assert grant.target == market.core
        for branch in branches(grant):
            assert_market_and_safe(branch, market, safe.fund)
            if grant.signature.startswith(("supply", "repay")):
                callback = branch.children[-1]
                assert callback.parameter == 2 and value(callback, "bytes") == b""
            else:
                assert value(branch.children[-1], "address") == safe.fund.address


def test_supply_and_borrow_take_any_positive_amount(parameters):
    grants = derive(profile(parameters, "borrower"), declared(parameters))
    by_name = {grant.signature.split("(")[0]: grant for grant in grants}
    for name in ("supplyCollateral", "borrow"):
        (branch,) = branches(by_name[name])
        assets = branch.children[1]
        assert assets.operator == 17 and value(assets) == 0
    collateral_withdrawal = branches(by_name["withdrawCollateral"])[0].children[1]
    assert collateral_withdrawal.operator == 1
    assert value(collateral_withdrawal.children[1]) == 2**128
    for grant in grants:
        text = " ".join(grant.explanation)
        assert "quota" not in text and "limit" not in text
        assert grant.signature.startswith("approve") or "positive" in text


def test_lender_has_no_borrow_or_repay_grant_and_withdraws_assets_or_shares(parameters):
    grants = derive(profile(parameters, "lender"), declared(parameters))
    assert signatures(grants) == {"approve", "supply", "withdraw"}
    withdrawal = next(grant for grant in grants if grant.signature.startswith("withdraw"))
    choices = branches(withdrawal)
    assert len(choices) == 2
    assert any(
        branch.children[1].operator == 16 and value(branch.children[1]) == 0 for branch in choices
    )
    assert any(
        branch.children[2].operator == 16 and value(branch.children[2]) == 0 for branch in choices
    )


def test_approvals_pin_the_core_with_any_amount_and_disclose_gateway_checks(parameters):
    grants = derive(profile(parameters, "borrower"), declared(parameters))
    for grant in grants:
        if not grant.signature.startswith("approve"):
            continue
        assert (
            value(grant.condition.children[0], "address")
            == parameters.target("loan").reserve.market.core
        )
        zero, amount = grant.condition.children[1].children
        assert value(zero) == 0
        assert amount.operator == 17 and value(amount) == 0
    assert "approves the held loan balance" in " ".join(MorphoPermissions.runtime_checks)
    foreign = profile(parameters, "borrower")
    foreign = foreign.model_copy(
        update={"fund": foreign.fund.model_copy(update={"address": "0x" + "78" * 20})}
    )
    assert derive(foreign, declared(parameters)) == ()
