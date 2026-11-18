"""One reserve owner and scope-only economic authority across Aave V3 actions."""

import json
from types import SimpleNamespace

import pytest
from pydantic import TypeAdapter

from almanak.caip import AssetType
from almanak.connectors.aave_v3.authority import admit, capability_codec, declared, validate
from almanak.connectors.aave_v3.models import Allocation, Intent, Parameters
from almanak.gateway.evm import native_asset
from almanak.models import AccountBinding, AssetMetadata, Eoa, StrategyAccounts
from almanak.triggers import IntervalTrigger
from tests.lending_health_scope import check_health_floor_scope
from tests.test_aave_v3_risk import position


@pytest.fixture
def parameters():
    observations = (position(0), position(1))
    allocations = []
    for index, p in enumerate(observations):
        quote = p.reserve.oracle
        assert quote is not None
        allocations.append(
            Allocation(
                name="collateral" if index == 0 else "loan",
                account="fund",
                reserve=p.reserve.source,
                capital=10**18 if index == 0 else 0,
                borrow=index == 1,
                collateral=index == 0,
                minimum_health_factor=2 * 10**18,
                oracle=quote.oracle,
                price_source=quote.feed,
                native_asset=native_asset(p.fund.chain.chain_id),
                minimum_native_balance=0,
            )
        )
    return Parameters(
        decision_trigger=IntervalTrigger(kind="interval", seconds=60),
        # WETH and USDC, which the recorded reserves hold.
        asset_metadata={
            observations[0].reserve.source.asset: AssetMetadata(decimals=18),
            observations[1].reserve.source.asset: AssetMetadata(decimals=6),
        },
        accounts=StrategyAccounts(
            bindings=(
                AccountBinding(
                    name="fund",
                    profile=Eoa(kind="eoa", fund=observations[0].fund),
                ),
            )
        ),
        allocations=tuple(allocations),
    )


def validated_with(config, allocations):
    return Parameters.model_validate_json(
        config.model_copy(update={"allocations": allocations}).model_dump_json()
    )


def test_helpers_keep_every_intent_typed_scoped_and_authorized(parameters):
    intents = (
        parameters.supply("collateral", 10**18),
        parameters.borrow("loan", 1000 * 10**6),
        parameters.withdraw_amount("collateral", 10**18 + 1),
        parameters.withdraw("collateral"),
        parameters.repay_amount("loan", 500 * 10**6),
        parameters.repay("loan"),
        parameters.set_collateral("collateral", True),
        parameters.set_collateral("collateral", False),
    )
    capabilities = capability_codec().declared(parameters)
    validate(parameters, capabilities)
    for intent in intents:
        assert TypeAdapter(Intent).validate_json(intent.model_dump_json()) == intent
        admit(capabilities, intent)
        assert intent.target == parameters.target(intent.target.allocation)


@pytest.mark.parametrize("fault", ["name", "alias", "account", "oracle", "health", "fund"])
def test_parameters_refuse_aliases_and_inconsistent_pool_policy(parameters, fault):
    first, second = parameters.allocations
    if fault == "name":
        second = second.model_copy(update={"name": first.name})
    elif fault == "alias":
        second = second.model_copy(update={"reserve": first.reserve})
    elif fault == "account":
        second = second.model_copy(update={"account": "unbound"})
    elif fault == "oracle":
        second = second.model_copy(update={"oracle": "0x" + "12" * 20})
    elif fault == "health":
        second = second.model_copy(update={"minimum_health_factor": 3 * 10**18})
    else:
        second = second.model_copy(
            update={
                "reserve": second.reserve.model_copy(
                    update={
                        "chain": second.chain.model_copy(update={"environment": "different"}),
                    }
                )
            }
        )
    with pytest.raises(ValueError):
        validated_with(parameters, (first, second))


@pytest.mark.parametrize("field", ["supply_limit", "borrow_limit", "entry_amount"])
def test_retired_budget_fields_are_refused(parameters, field):
    wire = parameters.allocations[0].model_dump(mode="json")
    with pytest.raises(ValueError, match="Extra inputs are not permitted") as refused:
        Allocation.model_validate_json(json.dumps({**wire, field: 10**18}))
    assert [error["loc"] for error in refused.value.errors()] == [(field,)]


def test_capital_defaults_to_zero_and_borrowing_to_off(parameters):
    wire = parameters.allocations[0].model_dump(mode="json", exclude={"capital", "borrow"})
    allocation = Allocation.model_validate_json(json.dumps(wire))
    assert allocation.capital == 0 and allocation.borrow is False


def test_native_asset_must_belong_to_the_reserve_chain(parameters):
    first = parameters.allocations[0]
    changed = first.model_copy(update={"native_asset": AssetType("eip155:8453/slip44:60")})
    with pytest.raises(ValueError, match="native payer asset"):
        Allocation.model_validate_json(changed.model_dump_json())


@pytest.mark.parametrize(
    "action,allocation,amount",
    [
        ("supply", "loan", 1),
        ("supply", "collateral", 10**30),
        ("withdraw_amount", "loan", 10**30),
        ("borrow", "loan", 10**30),
    ],
)
def test_admission_names_no_amount(parameters, action, allocation, amount):
    # Capital is what an allocation starts with, not a bound: the strategy decides how
    # much to supply, withdraw or borrow within its own reserve.
    admit(declared(parameters), getattr(parameters, action)(allocation, amount))


def test_repayment_requires_borrowing_authority_not_an_amount(parameters):
    # Repayment is scoped to the fund's own debt; planning bounds a partial amount
    # by the observed debt, so admission only requires borrowing authority.
    admit(declared(parameters), parameters.repay_amount("loan", 10**30))
    admit(declared(parameters), parameters.repay("loan"))
    for intent in (
        parameters.borrow("collateral", 1),
        parameters.repay_amount("collateral", 1),
        parameters.repay("collateral"),
    ):
        with pytest.raises(ValueError, match="require a borrowing allocation"):
            admit(declared(parameters), intent)


def test_capability_hook_cannot_change_borrowing_targets_or_collateral_authority(parameters):
    capabilities = declared(parameters)
    first = capabilities.requirements[0]
    for update in ({"borrow": True}, {"collateral": False}):
        changed = first.model_copy(update=update)
        forged = (changed, *capabilities.requirements[1:])
        with pytest.raises(ValueError, match="authoritative"):
            validate(parameters, capabilities.model_copy(update={"requirements": forged}))
    forged = parameters.supply("collateral", 1)
    forged = forged.model_copy(
        update={"target": forged.target.model_copy(update={"allocation": "foreign"})}
    )
    with pytest.raises(ValueError, match="exactly one"):
        admit(capabilities, forged)
    admit(capabilities, parameters.withdraw("loan"))
    admit(capabilities, parameters.set_collateral("loan", False))
    with pytest.raises(ValueError, match="collateral action"):
        admit(capabilities, parameters.set_collateral("loan", True))
    with pytest.raises(ValueError, match="outside"):
        admit(capabilities, SimpleNamespace(target=first.target, kind="arbitrary"))
    with pytest.raises(ValueError, match="unknown"):
        parameters.target("missing")


def test_minimum_health_factor_is_required_only_where_debt_is_possible(parameters):
    check_health_floor_scope(parameters)
