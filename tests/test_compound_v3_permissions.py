"""Generated Comet grants pin scope only; economic distinctions stay gateway checks."""

from eth_abi import decode

from almanak.connectors.compound_v3.authority import declared
from almanak.connectors.compound_v3.permissions import CompoundPermissions, derive
from almanak.models import SafeRoles
from tests.test_compound_v3_config import parameters as parameters


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


def call_grants(grants):
    return [g for g in grants if not g.signature.startswith("approve")]


def test_supply_and_withdraw_take_any_nonzero_amount_of_each_allocated_asset(parameters):
    grants = derive(profile(parameters, "borrower"), declared(parameters))
    comet = parameters.target("loan").reserve.market.comet
    assets = {parameters.target(n).asset.reference for n in ("collateral", "loan")}
    calls = call_grants(grants)
    assert sorted(g.signature for g in calls) == [
        "supply(address,uint256)",
        "withdraw(address,uint256)",
    ]
    for grant in calls:
        assert grant.target == comet
        found = set()
        for branch in branches(grant):
            asset, amount = branch.children
            found.add(value(asset, "address"))
            # A strictly positive amount admits the full-repay and full-withdraw
            # sentinels too, so no separate sentinel grant or allowance is needed.
            assert amount.operator == 17 and value(amount) == 0
        assert found == assets
        assert 28 not in set(operators(grant.condition))


def test_lender_receives_the_same_scope_for_its_own_asset_only(parameters):
    grants = derive(profile(parameters, "lender"), declared(parameters))
    earn = parameters.target("earn")
    assert {(g.target, g.signature) for g in grants} == {
        (earn.reserve.market.comet, "supply(address,uint256)"),
        (earn.reserve.market.comet, "withdraw(address,uint256)"),
        (earn.asset.reference, "approve(address,uint256)"),
    }
    for grant in call_grants(grants):
        (branch,) = branches(grant)
        assert value(branch.children[0], "address") == earn.asset.reference
    assert all(g.conditions for g in grants)


def test_approvals_pin_the_comet_with_any_amount_and_disclose_economic_checks(parameters):
    grants = derive(profile(parameters, "borrower"), declared(parameters))
    approvals = [g for g in grants if g.signature.startswith("approve")]
    assert len(approvals) == 2
    for grant in approvals:
        asset = next(
            a
            for a in parameters.allocations
            if a.reserve.asset.reference == grant.target and a.account == "borrower"
        )
        assert value(grant.condition.children[0], "address") == asset.reserve.market.comet
        zero, amount = grant.condition.children[1].children
        assert value(zero) == 0
        assert amount.operator == 17 and value(amount) == 0
    text = " ".join(CompoundPermissions.runtime_checks)
    assert "gateway checks" in text and "shares each ABI selector" in text


def test_grants_do_not_depend_on_capital_or_borrowing(parameters):
    safe = profile(parameters, "borrower")
    changed = parameters.model_copy(
        update={
            "allocations": tuple(
                a.model_copy(update={"capital": a.capital * 7 + 1}) for a in parameters.allocations
            )
        }
    )
    assert derive(safe, declared(changed)) == derive(safe, declared(parameters))


def test_foreign_fund_receives_no_grants(parameters):
    safe = profile(parameters, "borrower")
    foreign = safe.model_copy(
        update={"fund": safe.fund.model_copy(update={"address": "0x" + "78" * 20})}
    )
    assert derive(foreign, declared(parameters)) == ()
