"""Comet author parameters preserve fund ownership and market-wide risk scope."""

import json

import pytest
from pydantic import TypeAdapter

from almanak.connectors.compound_v3.models import Allocation, Intent, Parameters
from almanak.gateway.evm import native_asset
from almanak.models import AccountBinding, Eoa, StrategyAccounts
from almanak.triggers import IntervalTrigger
from tests.lending_health_scope import check_health_floor_scope
from tests.test_compound_v3_operations import read, target


@pytest.fixture
def parameters():
    raw, run = read("mainnet"), read("mainnet-operations")
    targets = [
        target(raw, next(s for s in run["steps"] if s["kind"] == kind))
        for kind in ("collateral_supply", "borrow", "base_supply")
    ]
    allocations = tuple(
        Allocation(
            name=name,
            account=t.account,
            reserve=t.reserve,
            capital=10**18 if name == "collateral" else 1000 * 10**6 if name == "earn" else 0,
            borrow=name == "loan",
            minimum_health_factor=2 * 10**18,
            native_asset=native_asset(t.fund.chain.chain_id),
            minimum_native_balance=0,
        )
        for name, t in zip(("collateral", "loan", "earn"), targets, strict=True)
    )
    return Parameters(
        decision_trigger=IntervalTrigger(kind="interval", seconds=60),
        accounts=StrategyAccounts(
            bindings=tuple(
                AccountBinding(
                    name=t.account,
                    profile=Eoa(kind="eoa", fund=t.fund),
                )
                for t in (targets[0], targets[2])
            )
        ),
        allocations=allocations,
    )


def revalidate(model, change, drop=()):
    raw = model.model_dump(mode="json") | change
    for field in drop:
        del raw[field]
    return type(model).model_validate_json(json.dumps(raw))


def validate(parameters, allocations):
    return Parameters.model_validate_json(
        parameters.model_copy(update={"allocations": allocations}).model_dump_json()
    )


def test_all_six_intents_retain_the_named_fund_market_and_asset(parameters):
    intents = (
        parameters.supply("collateral", 10**18),
        parameters.borrow("loan", 10**6),
        parameters.withdraw("earn"),
        parameters.withdraw_amount("collateral", 1),
        parameters.repay("loan"),
        parameters.repay_amount("loan", 10**6),
    )
    for intent in intents:
        assert TypeAdapter(Intent).validate_json(intent.model_dump_json()) == intent
        assert intent.target == parameters.target(intent.target.allocation)
    assert parameters.target("earn").fund != parameters.target("loan").fund
    with pytest.raises(ValueError, match="unknown"):
        parameters.target("missing")


@pytest.mark.parametrize("fault", ["alias", "name", "account", "risk", "market", "collateral"])
def test_parameters_refuse_ambiguous_ownership_or_borrowing_scope(parameters, fault):
    collateral, loan, earn = parameters.allocations
    if fault == "alias":
        earn = earn.model_copy(update={"account": loan.account})
    elif fault == "name":
        earn = earn.model_copy(update={"name": loan.name})
    elif fault == "account":
        loan = loan.model_copy(update={"account": "unbound"})
    elif fault == "risk":
        loan = loan.model_copy(update={"minimum_health_factor": 3 * 10**18})
    elif fault == "market":
        market = collateral.reserve.market.model_copy(update={"base_price_feed": "0x" + "12" * 20})
        collateral = collateral.model_copy(
            update={"reserve": collateral.reserve.model_copy(update={"market": market})}
        )
    else:
        collateral = collateral.model_copy(update={"account": earn.account})
    with pytest.raises(ValueError):
        validate(parameters, (collateral, loan, earn))


@pytest.mark.parametrize("field", ["supply_limit", "borrow_limit", "entry_amount"])
def test_deleted_amount_fields_are_refused_as_unknown(parameters, field):
    with pytest.raises(ValueError, match="Extra inputs"):
        revalidate(parameters.allocations[0], {field: 1})


def test_collateral_cannot_carry_borrowing_authority(parameters):
    with pytest.raises(ValueError, match="collateral cannot carry borrowing"):
        revalidate(parameters.allocations[0], {"borrow": True})


def test_capital_is_finite_and_defaults_to_none(parameters):
    first = parameters.allocations[0]
    with pytest.raises(ValueError):
        revalidate(first, {"capital": 2**256 - 1})
    assert revalidate(first, {}, drop=("capital",)).capital == 0


def test_borrowing_requires_collateral_in_the_same_fund_and_comet(parameters):
    collateral, loan, earn = parameters.allocations
    with pytest.raises(ValueError, match="requires collateral allocated to the same fund/Comet"):
        validate(parameters, (collateral, loan, earn.model_copy(update={"borrow": True})))


def test_base_borrowing_needs_no_repayment_amount(parameters):
    # Repaying the fund's own debt is not an author amount; borrowing alone authorizes it.
    loan = parameters.allocations[1]
    assert loan.borrow and loan.capital == 0
    assert Allocation.model_validate_json(loan.model_dump_json()) == loan


def test_capability_codec_retains_authoritative_scope(parameters):
    from almanak.connectors.compound_v3.authority import capability_codec, declared, validate

    codec = capability_codec()
    capabilities = declared(parameters)
    validate(parameters, capabilities)
    assert [(c.target.allocation, c.borrow) for c in capabilities.requirements] == [
        ("collateral", False),
        ("loan", True),
        ("earn", False),
    ]
    for intent in (
        parameters.supply("collateral", 1),
        parameters.borrow("loan", 1),
        parameters.repay("loan"),
        parameters.repay_amount("loan", 1),
        parameters.withdraw("earn"),
        parameters.withdraw_amount("earn", 1),
    ):
        codec.admit(capabilities, intent)
    with pytest.raises(ValueError, match="authoritative"):
        validate(parameters, capabilities.model_copy(update={"requirements": ()}))


@pytest.mark.parametrize(
    "action,name,amount",
    [
        ("supply", "loan", 1),
        ("supply", "collateral", 10**30),
        ("supply", "earn", 10**30),
        ("withdraw_amount", "loan", 10**30),
        ("borrow", "loan", 10**30),
    ],
)
def test_supply_withdraw_and_borrow_take_any_amount(parameters, action, name, amount):
    # Amounts are the strategy's decision; execution checks principal, health and funding.
    from almanak.connectors.compound_v3.authority import admit, declared

    admit(declared(parameters), getattr(parameters, action)(name, amount))


@pytest.mark.parametrize("name", ["collateral", "earn"])
def test_borrowing_and_repayment_require_a_borrowing_allocation(parameters, name):
    from almanak.connectors.compound_v3.authority import admit, declared

    capabilities = declared(parameters)
    admit(capabilities, parameters.repay_amount("loan", 10**30))
    for intent in (
        parameters.borrow(name, 1),
        parameters.repay(name),
        parameters.repay_amount(name, 1),
    ):
        with pytest.raises(ValueError, match="require a borrowing allocation"):
            admit(capabilities, intent)


def test_compound_capabilities_refuse_unknown_kinds_foreign_targets_and_collateral_borrow(
    parameters,
):
    from types import SimpleNamespace

    from almanak.connectors.compound_v3.authority import admit, declared

    capabilities = declared(parameters)
    with pytest.raises(ValueError, match="outside"):
        admit(
            capabilities, SimpleNamespace(target=parameters.target("collateral"), kind="collateral")
        )
    intent = parameters.supply("collateral", 1)
    intent = intent.model_copy(
        update={"target": intent.target.model_copy(update={"allocation": "foreign"})}
    )
    with pytest.raises(ValueError, match="exactly one"):
        admit(capabilities, intent)
    first = capabilities.requirements[0]
    assert not first.target.reserve.is_base
    with pytest.raises(ValueError, match="collateral cannot authorize"):
        revalidate(first, {"borrow": True})
    with pytest.raises(ValueError, match="Extra inputs"):
        revalidate(first, {"amount_limit": 1})


def test_minimum_health_factor_is_required_only_where_debt_is_possible(parameters):
    check_health_floor_scope(parameters)
