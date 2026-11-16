"""Economic round trips preserve shares and debt with scope-only authority."""

import json

import pytest

from almanak.backtest.types import UnixTime
from almanak.connectors.morpho_blue.domain import MarketObservation
from almanak.connectors.morpho_blue.economic_actions import Change, apply
from almanak.connectors.morpho_blue.economic_market import latest, seed
from almanak.connectors.morpho_blue.economic_positions import Position
from tests.test_morpho_blue_config import parameters as parameters
from tests.test_morpho_blue_reader import recorded


@pytest.fixture
def model(parameters):
    market = MarketObservation.model_validate_json(json.dumps(recorded(1, True)["market"]))
    ledger = seed(market, UnixTime(market.block.timestamp))
    positions = tuple(
        Position(target=parameters.target(a.name), supply_shares=0, borrow_shares=0, collateral=0)
        for a in parameters.allocations
    )
    return market, ledger, positions


def execute(parameters, model, intent, inventory, *, time=None):
    market, ledger, positions = model
    group = tuple(p for p in positions if p.target.fund == intent.target.fund)
    change = apply(
        parameters,
        intent,
        group,
        market,
        ledger,
        inventory,
        time=ledger.effective_at if time is None else time,
    )
    assert latest(ledger, (change.market,)) == change.market.after
    assert Change.model_validate_json(change.model_dump_json()) == change
    changed = {p.target: p for p in change.positions}
    return (market, change.market.after, tuple(changed.get(p.target, p) for p in positions)), change


def test_lender_and_borrower_round_trip_through_one_shared_market(parameters, model):
    model, supplied = execute(
        parameters, model, parameters.supply("earn", 1000 * 10**6), 1000 * 10**6
    )
    assert supplied.inventory_delta == -1000 * 10**6
    model, _ = execute(parameters, model, parameters.supply("collateral", 10**18), 10**18)
    model, borrowed = execute(parameters, model, parameters.borrow("loan", 100 * 10**6), 0)
    assert borrowed.inventory_delta == 100 * 10**6
    market, ledger, positions = model
    model, partial = execute(
        parameters,
        model,
        parameters.repay_amount("loan", 10**6),
        10**6,
        time=UnixTime(ledger.effective_at + 86400),
    )
    assert partial.inventory_delta == -(10**6)
    model, repaid = execute(parameters, model, parameters.repay("loan"), 110 * 10**6)
    assert repaid.assets > 99 * 10**6
    model, _ = execute(parameters, model, parameters.withdraw_amount("collateral", 10**17), 0)
    model, collateral = execute(parameters, model, parameters.withdraw("collateral"), 0)
    assert collateral.inventory_delta == 9 * 10**17
    model, _ = execute(parameters, model, parameters.withdraw_amount("earn", 10**6), 0)
    model, earned = execute(parameters, model, parameters.withdraw("earn"), 0)
    assert earned.inventory_delta >= 999 * 10**6
    assert all(position.empty for position in model[2])
    assert model[1].generation == 9
    # No lifetime budget: the closed lender can supply again, above its starting capital.
    model, again = execute(parameters, model, parameters.supply("earn", 2000 * 10**6), 2000 * 10**6)
    assert again.inventory_delta == -2000 * 10**6


def test_borrowing_follows_the_borrow_flag_not_an_amount(parameters, model):
    model, _ = execute(parameters, model, parameters.supply("collateral", 2 * 10**18), 2 * 10**18)
    model, borrowed = execute(parameters, model, parameters.borrow("loan", 400 * 10**6), 0)
    assert borrowed.inventory_delta == 400 * 10**6
    with pytest.raises(ValueError, match="borrowing allocation"):
        execute(parameters, model, parameters.borrow("earn", 1), 0)


def test_missing_inventory_and_unhealthy_borrowing_leave_inputs_unchanged(parameters, model):
    original = tuple(p.model_dump_json() for p in model[2])
    with pytest.raises(ValueError, match="missing fund inventory"):
        execute(parameters, model, parameters.supply("collateral", 10**18), 0)
    with pytest.raises(ValueError, match="health floor"):
        execute(parameters, model, parameters.borrow("loan", 10**6), 0)
    assert original == tuple(p.model_dump_json() for p in model[2])
    with pytest.raises(ValueError, match="every configured"):
        apply(
            parameters,
            parameters.borrow("loan", 1),
            (model[2][1],),
            *model[:2],
            0,
            time=model[1].effective_at,
        )
