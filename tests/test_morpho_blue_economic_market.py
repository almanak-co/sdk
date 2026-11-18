"""Sampling and journal invariants are model checks, separate from contract comparisons."""

import json

import pytest

from almanak.backtest.types import UnixTime
from almanak.connectors.morpho_blue.domain import Accounting, MarketObservation
from almanak.connectors.morpho_blue.economic_market import (
    TOTALS,
    MarketLedger,
    committed,
    latest,
    project,
    reanchor,
    seed,
)
from tests.test_morpho_blue_reader import recorded


@pytest.fixture(params=[1, 8453])
def observation(request):
    return MarketObservation.model_validate_json(
        json.dumps(recorded(request.param, False)["market"])
    )


def test_unchanged_external_samples_do_not_introduce_additional_accrual(observation):
    start = UnixTime(observation.block.timestamp + 17)
    initial = seed(observation, start)
    for offset in (1, 19, 200):
        at = UnixTime(start + offset)
        actual = reanchor(initial, observation, observation, at)
        expected = seed(observation, at)
        assert actual.accounting == expected.accounting
        assert actual.rate_at_target == expected.rate_at_target
        initial = actual


def test_hypothetical_bad_debt_loss_survives_subsequent_external_samples(observation):
    at = UnixTime(observation.block.timestamp)
    initial = seed(observation, at)
    # A synthetic loss in the shared pool changes assets while leaving supply
    # shares intact. Native liquidation arithmetic has separate contract proofs.
    totals = initial.accounting.model_dump()
    totals["supply_assets"] -= 1000000
    totals["borrow_assets"] -= 1000000
    totals["borrow_shares"] -= 900000000000
    loss = MarketLedger(**(initial.model_dump() | {"accounting": Accounting(**totals)}))
    later = UnixTime(at + 60)
    projected_loss, projected_base = project(loss, later), project(initial, later)
    actual = reanchor(loss, observation, observation, later)
    baseline = seed(observation, later)
    for field in TOTALS:
        assert getattr(actual.accounting, field) - getattr(baseline.accounting, field) == (
            getattr(projected_loss.accounting, field) - getattr(projected_base.accounting, field)
        )
    assert actual.accounting.supply_assets < baseline.accounting.supply_assets
    assert actual.accounting.supply_shares == baseline.accounting.supply_shares


def test_same_time_market_changes_have_one_recoverable_generation_order(observation):
    at = UnixTime(observation.block.timestamp)
    initial = seed(observation, at)
    first = committed(initial, project(initial, UnixTime(at + 1)))
    second = committed(first.after, project(first.after, UnixTime(at + 1)))
    assert latest(initial, (second, first)) == second.after
    for changes in ((second,), (first, first), (second, second)):
        with pytest.raises(ValueError, match="generation"):
            latest(initial, changes)


def test_external_net_flow_conflicts_cannot_erase_hypothetical_market_state(observation):
    at = UnixTime(observation.block.timestamp)
    initial = seed(observation, at)
    near_limit = Accounting(
        **(initial.accounting.model_dump() | {"borrow_assets": initial.accounting.supply_assets})
    )
    hypothetical = MarketLedger(**(initial.model_dump() | {"accounting": near_limit}))
    reduced = observation.model_copy(
        update={
            "accounting": Accounting(
                **(
                    observation.accounting.model_dump()
                    | {"supply_assets": observation.accounting.supply_assets - 1000000}
                )
            )
        }
    )
    with pytest.raises(ValueError, match="borrowing exceeds"):
        reanchor(hypothetical, observation, reduced, UnixTime(at + 1))
    with pytest.raises(ValueError, match="out of order"):
        reanchor(initial, observation, observation, at)
    with pytest.raises(ValueError, match="future"):
        seed(observation, UnixTime(at - 1))
