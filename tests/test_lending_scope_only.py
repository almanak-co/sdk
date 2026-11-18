"""Scope-only lending authority (blueprint 27) as a property over every lending connector.

No generated grant carries a Roles allowance, the only author amount left (`capital`)
never reaches a grant, and the exits a teardown needs (withdraw, full repay, approval
revocation) exist for every allocation that can hold that obligation (section 11).
"""

import pytest

from almanak.connectors.aave_v3.authority import declared as aave_v3_declared
from almanak.connectors.aave_v3.permissions import derive as aave_v3_derive
from almanak.connectors.aave_v4.authority import declared as aave_v4_declared
from almanak.connectors.aave_v4.permissions import derive_aave
from almanak.connectors.compound_v3.authority import declared as compound_declared
from almanak.connectors.compound_v3.permissions import derive as compound_derive
from almanak.connectors.morpho_blue.authority import declared as morpho_declared
from almanak.connectors.morpho_blue.permissions import derive as morpho_derive
from almanak.connectors.sparklend.authority import declared as spark_declared
from almanak.connectors.sparklend.permissions import derive as spark_derive
from almanak.gateway.roles_conditions import any_amount
from almanak.models import SafeRoles
from tests.test_aave_v3_config import parameters as aave_v3_parameters
from tests.test_compound_v3_config import parameters as compound_parameters
from tests.test_morpho_blue_config import parameters as morpho_parameters
from tests.test_sparklend_config import parameters as spark_parameters

ALLOWANCE_OPERATOR = 28
CONNECTORS = {
    "aave_v3": ("aave_v3_parameters", aave_v3_declared, aave_v3_derive),
    "sparklend": ("spark_parameters", spark_declared, spark_derive),
    "compound_v3": ("compound_parameters", compound_declared, compound_derive),
    "morpho_blue": ("morpho_parameters", morpho_declared, morpho_derive),
}
# Fixtures are imported for pytest to discover by name.
FIXTURES = (aave_v3_parameters, spark_parameters, compound_parameters, morpho_parameters)


def safe(fund):
    return SafeRoles(
        kind="safe_roles",
        fund=fund,
        operator=fund.model_copy(update={"address": "0x" + "12" * 20}),
        roles_module="0x" + "34" * 20,
        role_key="0x" + "56" * 32,
    )


def operators(condition):
    yield condition.operator
    for child in condition.children:
        yield from operators(child)


def grants_by_fund(config, declared, derive):
    """Each bound fund's grants, keyed by fund, as a Safe partition would derive them."""
    funds = {binding.profile.fund for binding in config.accounts.bindings}
    return {fund: derive(safe(fund), declared(config)) for fund in funds}


def with_capital(config, factor):
    """The same allocations with a different starting capital."""
    allocations = tuple(
        a.model_copy(update={"capital": a.capital * factor + 7}) for a in config.allocations
    )
    return config.model_copy(update={"allocations": allocations})


def names(grants):
    return {grant.signature.split("(")[0] for grant in grants}


@pytest.fixture(params=sorted(CONNECTORS))
def connector(request):
    fixture, declared, derive = CONNECTORS[request.param]
    return request.param, request.getfixturevalue(fixture), declared, derive


def test_no_grant_carries_a_roles_allowance(connector):
    _, config, declared, derive = connector
    for grants in grants_by_fund(config, declared, derive).values():
        assert grants
        for grant in grants:
            if grant.condition is not None:
                assert ALLOWANCE_OPERATOR not in set(operators(grant.condition)), grant.signature


def test_capital_never_reaches_a_grant(connector):
    _, config, declared, derive = connector
    before = grants_by_fund(config, declared, derive)
    assert grants_by_fund(with_capital(config, 3), declared, derive) == before


def test_every_allocation_keeps_its_exits_and_revocation(connector):
    name, config, declared, derive = connector
    for allocation in config.allocations:
        fund = config.target(allocation.name).fund
        grants = derive(safe(fund), declared(config))
        asset = config.target(allocation.name).asset.reference
        approvals = [g for g in grants if g.target == asset and g.signature.startswith("approve")]
        assert approvals and any(any_amount() in approval_branches(g) for g in approvals)
        assert names(grants) & {"withdraw", "withdrawCollateral"}
        if allocation.borrow:
            # Comet repays the whole debt through its supply sentinel.
            assert names(grants) & ({"supply"} if name == "compound_v3" else {"repay"})


def approval_branches(grant):
    """The approval amount condition: one exact spender, then any amount or zero."""
    spender, amount = grant.condition.children
    return (amount, *amount.children) if amount.operator == 2 else (amount,)


def test_borrow_and_repay_follow_the_borrow_flag_only(connector):
    name, config, declared, derive = connector
    borrowing = next(a for a in config.allocations if a.borrow)
    plain = config.model_copy(
        update={
            "allocations": tuple(
                a.model_copy(update={"borrow": False}) if a is borrowing else a
                for a in config.allocations
            )
        }
    )
    fund = config.target(borrowing.name).fund
    with_borrow = derive(safe(fund), declared(config))
    without = derive(safe(fund), declared(plain))
    if name == "compound_v3":
        # Comet shares its selectors, so the borrow flag is enforced by the gateway alone.
        assert with_borrow == without
        return
    assert {"borrow", "repay"} <= names(with_borrow)
    assert not {"borrow", "repay"} & names(without)


def aave_v4_grants(parameters, fund):
    requirements = tuple(
        r for r in aave_v4_declared(parameters).requirements if r.target.fund == fund
    )
    return derive_aave(safe(fund), requirements)


def test_aave_v4_grants_ignore_capital_and_carry_no_allowance(recorded):
    config = recorded.config
    for fund in {config.target(a.name).fund for a in config.positions}:
        grants = aave_v4_grants(config, fund)
        assert grants == aave_v4_grants(with_capital(config, 5), fund)
        for grant in grants:
            if grant.condition is not None:
                assert ALLOWANCE_OPERATOR not in set(operators(grant.condition))
