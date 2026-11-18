"""Morpho author parameters retain market identity, fund scope and scope-only authority."""

import json

import pytest
from pydantic import TypeAdapter

from almanak.connectors.morpho_blue import authority
from almanak.connectors.morpho_blue.domain import Market, MarketId, Reserve
from almanak.connectors.morpho_blue.models import Allocation, Intent, Parameters
from almanak.gateway.evm import native_asset
from almanak.models import Account, AccountBinding, AssetMetadata, Eoa, StrategyAccounts
from almanak.triggers import IntervalTrigger
from tests.lending_health_scope import check_health_floor_scope
from tests.test_morpho_blue_reader import recorded


@pytest.fixture
def parameters():
    raw = recorded(1, True)
    market = Market.model_validate_json(json.dumps(raw["source"]))
    funds = [Account.model_validate_json(json.dumps(row["fund"])) for row in raw["accounts"]]
    allocations = tuple(
        Allocation(
            name=name,
            account=account,
            reserve=Reserve(
                market=market,
                asset=market.collateral_asset if name == "collateral" else market.loan_asset,
            ),
            capital=capital,
            borrow=borrow,
            minimum_health_factor=2 * 10**18,
            native_asset=native_asset(market.chain.chain_id),
            minimum_native_balance=0,
        )
        for name, account, capital, borrow in (
            ("collateral", "borrower", 10**18, False),
            ("loan", "borrower", 0, True),
            ("earn", "lender", 1000 * 10**6, False),
        )
    )
    return Parameters(
        decision_trigger=IntervalTrigger(kind="interval", seconds=60),
        # The reference converts its borrow constant with the loan asset's decimals.
        asset_metadata={
            market.loan_asset: AssetMetadata(decimals=6),
            market.collateral_asset: AssetMetadata(decimals=18),
        },
        accounts=StrategyAccounts(
            bindings=tuple(
                AccountBinding(name=name, profile=Eoa(kind="eoa", fund=fund))
                for name, fund in zip(("lender", "borrower"), funds, strict=True)
            )
        ),
        allocations=allocations,
    )


def revalidate(parameters, allocations):
    return Parameters.model_validate_json(
        parameters.model_copy(update={"allocations": allocations}).model_dump_json()
    )


def test_all_intents_round_trip_and_capabilities_match_authoritative_parameters(parameters):
    intents = (
        parameters.supply("collateral", 10**18),
        parameters.borrow("loan", 10**6),
        parameters.withdraw("earn"),
        parameters.withdraw_amount("collateral", 1),
        parameters.repay("loan"),
        parameters.repay_amount("loan", 10**6),
    )
    capabilities = authority.declared(parameters)
    authority.validate(parameters, capabilities)
    for intent in intents:
        assert TypeAdapter(Intent).validate_json(intent.model_dump_json()) == intent
        authority.admit(capabilities, intent)
    assert authority.capability_codec().worker_factory
    with pytest.raises(ValueError, match="unknown"):
        parameters.target("missing")


@pytest.mark.parametrize("fault", ["alias", "name", "account", "risk", "tuple", "collateral"])
def test_parameters_refuse_ambiguous_market_or_fund_ownership(parameters, fault):
    collateral, loan, earn = parameters.allocations
    if fault == "alias":
        earn = earn.model_copy(update={"account": loan.account})
    elif fault == "name":
        earn = earn.model_copy(update={"name": loan.name})
    elif fault == "account":
        loan = loan.model_copy(update={"account": "unbound"})
    elif fault == "risk":
        loan = loan.model_copy(update={"minimum_health_factor": 3 * 10**18})
    elif fault == "tuple":
        market = loan.reserve.market.model_copy(update={"lltv": 5 * 10**17})
        loan = loan.model_copy(
            update={"reserve": loan.reserve.model_copy(update={"market": market})}
        )
    else:
        collateral = collateral.model_copy(update={"account": earn.account})
    with pytest.raises(ValueError):
        revalidate(parameters, (collateral, loan, earn))


@pytest.mark.parametrize(
    "change",
    [
        {"borrow": True},
        {"capital": 2**128},
        {"minimum_health_factor": 10**18},
    ],
)
def test_collateral_authority_refuses_debt_or_unbounded_quantities(parameters, change):
    altered = parameters.allocations[0].model_copy(update=change)
    with pytest.raises(ValueError):
        Allocation.model_validate_json(altered.model_dump_json())


def test_collateral_cannot_carry_borrowing_authority(parameters):
    collateral = parameters.allocations[0].model_dump(mode="json")
    with pytest.raises(ValueError, match="cannot carry borrowing"):
        Allocation.model_validate_json(json.dumps({**collateral, "borrow": True}))


@pytest.mark.parametrize(
    "field", ["supply_limit", "borrow_limit", "entry_amount", "repayment_ceiling"]
)
def test_retired_budget_fields_are_refused_as_unknown(parameters, field):
    loan = parameters.allocations[1].model_dump(mode="json")
    with pytest.raises(ValueError, match="Extra inputs"):
        Allocation.model_validate_json(json.dumps({**loan, field: 1}))


def test_borrowing_requires_collateral_in_the_same_fund_and_market(parameters):
    collateral, loan, earn = parameters.allocations
    lender_loan = earn.model_copy(update={"borrow": True})
    with pytest.raises(ValueError, match="requires collateral allocated"):
        revalidate(parameters, (collateral, loan, lender_loan))


def test_capital_is_optional_and_not_an_authority_limit(parameters):
    collateral, loan, earn = parameters.allocations
    assert loan.capital == 0 and earn.capital == 1000 * 10**6
    unfunded = revalidate(
        parameters,
        (collateral.model_copy(update={"capital": 0}), loan, earn),
    )
    capabilities = authority.declared(unfunded)
    authority.admit(capabilities, unfunded.supply("collateral", 2**127))
    authority.admit(capabilities, unfunded.withdraw_amount("earn", 2**127))


def test_market_identity_requires_typed_tuple_hash_and_nonzero_contracts(parameters):
    market = parameters.allocations[0].reserve.market
    with pytest.raises(ValueError):
        MarketId("0x" + "AB" * 32)
    for change in (
        {"market_id": MarketId("0x" + "00" * 32)},
        {"core": "0x" + "00" * 20},
        {"loan_asset": market.collateral_asset},
    ):
        with pytest.raises(ValueError):
            Market.model_validate_json(market.model_copy(update=change).model_dump_json())


def test_admission_takes_any_amount_and_requires_borrow_for_debt(parameters):
    capabilities = authority.declared(parameters)
    assert [row.borrow for row in capabilities.requirements] == [False, True, False]
    authority.admit(capabilities, parameters.borrow("loan", 2**127))
    authority.admit(capabilities, parameters.repay_amount("loan", 2**127))
    authority.admit(capabilities, parameters.supply("earn", 2**127))
    authority.admit(capabilities, parameters.withdraw("loan"))
    for intent in (
        parameters.borrow("earn", 1),
        parameters.repay("earn"),
        parameters.repay_amount("collateral", 1),
    ):
        with pytest.raises(ValueError, match="require a borrowing allocation"):
            authority.admit(capabilities, intent)


def test_capabilities_cannot_expand_or_change_economic_authority(parameters):
    capabilities = authority.declared(parameters)
    altered = capabilities.model_copy(update={"requirements": capabilities.requirements[1:]})
    with pytest.raises(ValueError, match="allocations"):
        authority.validate(parameters, altered)
    with pytest.raises(ValueError, match="exactly one"):
        authority.admit(altered, parameters.supply("collateral", 1))
    collateral = capabilities.requirements[0].model_dump(mode="json")
    with pytest.raises(ValueError, match="cannot authorize borrowing"):
        authority.ReserveCapability.model_validate_json(json.dumps({**collateral, "borrow": True}))
    with pytest.raises(ValueError, match="Extra inputs"):
        authority.ReserveCapability.model_validate_json(
            json.dumps({**collateral, "amount_limit": 1})
        )


def test_minimum_health_factor_is_required_only_where_debt_is_possible(parameters):
    check_health_floor_scope(parameters)
