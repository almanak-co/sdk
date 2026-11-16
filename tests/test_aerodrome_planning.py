import json
from types import SimpleNamespace

import pytest

from almanak.connectors.aerodrome import operations as ops
from almanak.connectors.aerodrome.authority import admit
from almanak.connectors.aerodrome.models import Capabilities, Capability, Parameters, Target
from almanak.connectors.aerodrome.observations import Snapshot
from almanak.connectors.aerodrome.ownership import at_indices, validate_initial, validate_owned
from almanak.connectors.aerodrome.planning import operation
from almanak.connectors.aerodrome.positions import Position
from almanak.dex_intents import (
    CloseLiquidity,
    CollectLiquidityFees,
    DecreaseLiquidity,
    ExitExchange,
    IncreaseLiquidity,
    OpenLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from tests.exchange_scope import without_exchange_budgets
from tests.test_aerodrome_observations import CAPTURE


def fixture():
    row = CAPTURE["rows"][0]
    config = Parameters.model_validate_json(json.dumps(without_exchange_budgets(row["config"])))
    observed = Snapshot.model_validate_json(json.dumps(row["snapshot"]))
    owned = observed.owned
    observed = observed.model_copy(
        update={
            "position": None,
            "wallet0": owned.inventory0,
            "wallet1": owned.inventory1,
        }
    )
    capability = Capability(target=observed.target)
    state = SimpleNamespace(
        capabilities=Capabilities(requirements=(capability,)),
        decision=SimpleNamespace(observations=(observed,)),
    )
    return config, state, observed


def deposits():
    """The desired inputs the capture's addition used, as the retired maxima recorded them."""
    allocation = CAPTURE["rows"][0]["config"]["allocations"][0]
    return tuple(
        AssetAmount.model_validate_json(json.dumps(allocation[name]))
        for name in ("maximum0", "maximum1")
    )


def position(before, shares=1000, fees=0):
    return Position(
        shares=AssetAmount(asset=before.target.pool.shares_asset, amount=shares),
        fees0=AssetAmount(asset=before.target.assets[0], amount=fees),
        fees1=AssetAmount(asset=before.target.assets[1], amount=0),
        index0=before.pool.index0,
        index1=before.pool.index1,
    )


def with_position(before, value):
    return before.model_copy(
        update={"position": value, "owned": before.owned.model_copy(update={"position": value})}
    )


def authorized(before, asset, amount):
    values = tuple(
        a.model_copy(update={"amount": amount}) if a.asset == asset else a
        for a in before.allowances
    )
    return before.model_copy(update={"allowances": values})


def test_initial_ownership_refuses_unrelated_lp_claims_and_pool_deposits():
    _, _, before = fixture()
    validate_initial(before)
    for value in (position(before), position(before, shares=0, fees=7)):
        with pytest.raises(DataError, match="initial fund"):
            validate_initial(with_position(before, value))
    contaminated = before.model_copy(
        update={"pool": before.pool.model_copy(update={"deposited_shares": 1})}
    )
    with pytest.raises(DataError, match="another transfer"):
        validate_initial(contaminated)


def test_fee_observation_does_not_recheckpoint_fractional_accrual():
    _, _, before = fixture()
    old = position(before, shares=3).model_copy(update={"index0": 0, "index1": 0})
    first = before.pool.model_copy(update={"index0": 2 * 10**17, "index1": 0})
    second = first.model_copy(update={"index0": 4 * 10**17})
    assert at_indices(old, first).fees0.amount == 0
    assert at_indices(old, second).fees0.amount == 1
    assert at_indices(at_indices(old, first), second).fees0.amount == 0
    retained = with_position(before.model_copy(update={"pool": second}), old)
    validate_owned(retained.owned, retained)
    with pytest.raises(DataError, match="fee claims"):
        validate_owned(
            retained.owned, retained.model_copy(update={"position": at_indices(old, second)})
        )


def test_swap_resets_existing_authority_then_grants_exact_input():
    config, state, before = fixture()
    quote = before.entry_quote
    intent = SwapExactInput[Target](
        target=before.target,
        amount=quote.amount_in,
        minimum_output=quote.amount_out,
        deadline=before.block.timestamp + 30,
    )
    first = operation(config, state, intent, authorized(before, intent.amount.asset, 9))
    assert isinstance(first, ops.Approval) and first.amount == 0
    grant = operation(config, state, intent, before)
    assert isinstance(grant, ops.Approval) and grant.amount == intent.amount.amount
    swap = operation(config, state, intent, authorized(before, intent.amount.asset, grant.amount))
    assert isinstance(swap, ops.Swap) and swap.minimum == quote.amount_out


@pytest.mark.parametrize("kind", [OpenLiquidity, IncreaseLiquidity])
def test_addition_requires_proportional_budgets_and_retained_ownership(kind):
    config, state, before = fixture()
    if kind is IncreaseLiquidity:
        before = with_position(before, position(before))
    desired = deposits()
    intent = kind[Target](
        target=before.target,
        maximum0=desired[0],
        maximum1=desired[1],
        deadline=before.block.timestamp + 30,
    )
    for value in desired:
        step = operation(config, state, intent, before)
        assert isinstance(step, ops.Approval) and step.asset == value.asset
        before = authorized(before, value.asset, value.amount)
    step = operation(config, state, intent, before)
    assert isinstance(step, ops.Mint if kind is OpenLiquidity else ops.Increase)
    assert step.minimum_shares.amount > 0
    with pytest.raises(ValueError, match="synchronized"):
        operation(
            config,
            state,
            intent,
            before.model_copy(
                update={
                    "pool": before.pool.model_copy(update={"balance0": before.pool.balance0 + 1})
                }
            ),
        )
    inverse = with_position(before, None if kind is IncreaseLiquidity else position(before))
    with pytest.raises(ValueError, match="preserve"):
        operation(config, state, intent, inverse)


@pytest.mark.parametrize("kind", [DecreaseLiquidity, CloseLiquidity])
def test_removal_approves_only_owned_shares_and_refuses_unrelated_pool_deposits(kind):
    config, state, before = fixture()
    before = with_position(before, position(before, shares=10**15))
    intent = kind[Target](
        target=before.target,
        minimum0=AssetAmount(asset=before.target.assets[0], amount=0),
        minimum1=AssetAmount(asset=before.target.assets[1], amount=0),
        deadline=before.block.timestamp + 30,
        **({"liquidity": 10**14} if kind is DecreaseLiquidity else {}),
    )
    expected = 10**14 if kind is DecreaseLiquidity else 10**15
    grant = operation(config, state, intent, before)
    assert isinstance(grant, ops.Approval) and grant.asset == before.target.pool.shares_asset
    assert grant.amount == expected
    step = operation(config, state, intent, authorized(before, grant.asset, grant.amount))
    assert step.shares.amount == expected
    with pytest.raises(DataError, match="another transfer"):
        operation(
            config,
            state,
            intent,
            before.model_copy(
                update={"pool": before.pool.model_copy(update={"deposited_shares": 1})}
            ),
        )


def test_exit_keeps_fees_after_last_share_and_cleans_lp_allowance():
    config, state, before = fixture()
    claim = with_position(before, position(before, shares=0, fees=99))
    exit_intent = ExitExchange[Target](target=before.target)
    assert isinstance(operation(config, state, exit_intent, claim), ops.Collect)
    collect = CollectLiquidityFees[Target](
        target=before.target, deadline=before.block.timestamp + 30
    )
    assert isinstance(operation(config, state, collect, claim), ops.Collect)
    funded = with_position(before, position(before, shares=10**15))
    assert operation(config, state, exit_intent, funded).asset == before.target.pool.shares_asset
    before = authorized(before, before.target.pool.shares_asset, 100)
    cleanup = operation(config, state, exit_intent, before)
    assert cleanup.asset == before.target.pool.shares_asset and cleanup.amount == 0
    assert operation(config, state, RevokeExchange[Target](target=before.target), before) == cleanup


def test_capability_refuses_missing_duplicate_and_excess_authority():
    config, state, before = fixture()
    exit_intent = ExitExchange[Target](target=before.target)
    for capabilities in (
        None,
        Capabilities(requirements=()),
        Capabilities(requirements=state.capabilities.requirements * 2),
    ):
        with pytest.raises(ValueError):
            admit(capabilities, exit_intent)
    # Authority is scope only (blueprint 27): no swap size is refused by admission.
    first, second = before.target.assets
    intent = SwapExactInput[Target](
        target=before.target,
        amount=AssetAmount(asset=first, amount=2**127),
        minimum_output=AssetAmount(asset=second, amount=1),
        deadline=1,
    )
    admit(state.capabilities, intent)
    with pytest.raises(ValueError, match="Extra inputs"):
        Capability.model_validate_json(
            json.dumps(
                {
                    "target": before.target.model_dump(mode="json"),
                    "maximum0": {"asset": str(first), "amount": 1},
                }
            )
        )
