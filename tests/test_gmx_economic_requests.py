"""Hypothetical request accounting preserves escrow, cumulative inputs and fee attribution."""

from datetime import UTC, datetime

import pytest

from almanak.backtest.engine import Pending
from almanak.connectors.gmx_v2.economic_accounting import (
    effects,
    submitter_cost,
    validate_inventory,
)
from almanak.connectors.gmx_v2.economic_book import MarketBook
from almanak.connectors.gmx_v2.economic_parameters import initial, wallet
from almanak.connectors.gmx_v2.economic_requests import (
    admit,
    cancellation_eligible,
    eligible_opportunity,
    fill_amount,
    release,
)
from almanak.connectors.gmx_v2.economic_state import State
from almanak.connectors.gmx_v2.exchange_domain import Swap
from almanak.triggers import IntervalEvent, IntervalSelection
from tests.gmx_retired import retired_closure as retired_closure
from tests.test_gmx_economic_book import prepared as prepared

# The retained prepared datasets predate GMX's 1 October rotation; these accounting checks
# re-decode them against the retired closure they were collected from (tests.gmx_retired).
pytestmark = pytest.mark.usefixtures("retired_closure")


@pytest.fixture
def request_case(prepared):
    directory, config, settings, _ = prepared
    book = MarketBook(directory, config, settings)
    interval = IntervalSelection(configured=config.decision_trigger)
    state = initial(settings, config, interval)
    held = state.portfolio.positions[0]
    allocation = config.allocations[0]
    intent = Swap(
        target=held.payload.target,
        amount=allocation.entry_amount,
        minimum_output=allocation.entry_minimum_output,
        deadline=int(settings.schedule.start) + allocation.deadline_seconds,
    )
    at = datetime.fromtimestamp(settings.schedule.start, UTC)
    pending = Pending(
        identity="retained-admission",
        decision_identity="first-decision",
        trigger=IntervalEvent(started_at=at, due_at=at),
        intent=intent,
        decision_time=settings.schedule.start,
        eligible_at=settings.schedule.start,
    )
    row = book.at(held.payload.target, settings.schedule.start)
    opportunity = next(
        value
        for row in book.timelines[held.payload.target].rows
        for value in row.payload.opportunities
    )
    yield config, settings, state, held, pending, row.payload.risk[-1], opportunity
    book.close()


def test_acceptance_escrows_once_and_survives_exact_state_serialization(request_case):
    config, settings, state, held, pending, risk, _ = request_case
    after, movements = admit(config, held.payload, pending, risk)
    gas = submitter_cost(settings, held.payload.target, "acceptance")
    portfolio = state.portfolio.apply(effects(settings, held, after, movements, gas))
    validate_inventory(portfolio)
    original = state.portfolio.balance(wallet(held.payload.target, pending.intent.amount.asset))
    spent = pending.intent.amount.amount + config.allocations[0].execution_fee.amount
    assert (
        portfolio.balance(wallet(held.payload.target, pending.intent.amount.asset))
        == original - spent
    )
    assert after.escrow.identity == pending.identity
    assert sum(value.amount for value in after.spent) == spent
    restored = State.model_validate_json(
        state.model_copy(update={"portfolio": portfolio}).model_dump_json()
    )
    assert restored.portfolio == portfolio
    with pytest.raises(ValueError, match="new exact-target swap"):
        admit(config, after, pending, risk)


def test_published_quote_release_returns_native_fee_change_to_the_fund(request_case):
    config, settings, _, held, pending, risk, opportunity = request_case
    after, _ = admit(config, held.payload, pending, risk)
    # This unit checks accounting only. The tiny dataset's report predates admission.
    assert not eligible_opportunity(
        after.escrow, opportunity, settings, opportunity.publication.block.timestamp
    )
    output = fill_amount(after.escrow, opportunity)
    settled, movements, keeper, refund = release(after, settings, (output,))
    assert settled.escrow is None
    assert keeper.amount + refund.amount == after.escrow.execution_fee.amount
    assert refund.asset == config.allocations[0].native_asset
    assert refund.asset != after.escrow.execution_fee.asset
    assert (
        next(value.amount for value in settled.owned.inventory if value.asset == output.asset)
        == output.amount
    )
    assert all(value.key.account == held.payload.target.account for value in movements)
    assert settled.spent == after.spent


@pytest.mark.parametrize("owner", [False, True])
def test_cancellation_refunds_original_input_without_refilling_spending(request_case, owner):
    config, settings, _, held, pending, risk, _ = request_case
    accepted, _ = admit(config, held.payload, pending, risk)
    cancelled, _, keeper, refund = release(
        accepted, settings, accepted.escrow.inputs, owner_cancellation=owner
    )
    before = next(
        value.amount
        for value in held.payload.owned.inventory
        if value.asset == pending.intent.amount.asset
    )
    after = next(
        value.amount
        for value in cancelled.owned.inventory
        if value.asset == pending.intent.amount.asset
    )
    assert before - after == accepted.escrow.execution_fee.amount
    assert keeper.amount == (0 if owner else settings.keeper.payments[0].payment.amount)
    assert refund.amount == accepted.escrow.execution_fee.amount - keeper.amount
    assert cancelled.spent == accepted.spent


def test_repeated_cancelled_requests_are_bounded_by_inventory_not_a_budget(request_case):
    """Scope-only authority: requests repeat until owned inventory, not a quota, runs out."""
    config, settings, _, held, pending, risk, _ = request_case
    payload = held.payload
    per_request = pending.intent.amount.amount + config.allocations[0].execution_fee.amount
    cycles = 0
    with pytest.raises(ValueError, match="retained owned inventory"):
        for _ in range(1000):
            accepted, _ = admit(config, payload, pending, risk)
            payload, _, _, _ = release(
                accepted, settings, accepted.escrow.inputs, owner_cancellation=True
            )
            cycles += 1
    assert cycles > 1
    assert sum(value.amount for value in payload.spent) == cycles * per_request
    assert not any(value.amount for value in payload.allowances)


def test_request_cancellation_uses_current_expiration_and_current_enablement(request_case):
    config, _, _, held, pending, risk, _ = request_case
    accepted, _ = admit(config, held.payload, pending, risk)
    time = accepted.escrow.accepted_at + 10
    shortened = risk.model_copy(update={"request_expiration_seconds": 10})
    assert cancellation_eligible(accepted.escrow, shortened, time)
    assert not cancellation_eligible(accepted.escrow, shortened, time - 1)
    assert not cancellation_eligible(
        accepted.escrow, shortened.model_copy(update={"cancellation_disabled": True}), time
    )
    assert not cancellation_eligible(
        accepted.escrow, shortened.model_copy(update={"request_expiration_seconds": 11}), time
    )


@pytest.mark.parametrize(
    "change", ["wrong_second", "disabled", "latency", "early_report", "expiration", "age"]
)
def test_keeper_eligibility_cannot_reuse_an_ineligible_report(request_case, change):
    config, settings, _, held, pending, risk, opportunity = request_case
    accepted, _ = admit(config, held.payload, pending, risk)
    escrow = accepted.escrow.model_copy(
        update={"accepted_at": opportunity.publication.minimum_timestamp}
    )
    at = opportunity.publication.block.timestamp
    assert eligible_opportunity(escrow, opportunity, settings, at)
    if change == "wrong_second":
        at += 1
    elif change == "disabled":
        opportunity = opportunity.model_copy(
            update={"risk": opportunity.risk.model_copy(update={"execution_disabled": True})}
        )
    elif change == "latency":
        settings = settings.model_copy(
            update={"keeper": settings.keeper.model_copy(update={"minimum_latency_seconds": 100})}
        )
    elif change == "early_report":
        escrow = escrow.model_copy(
            update={"accepted_at": opportunity.publication.minimum_timestamp + 1}
        )
    elif change == "expiration":
        escrow = escrow.model_copy(
            update={"accepted_at": opportunity.publication.minimum_timestamp - 1}
        )
        opportunity = opportunity.model_copy(
            update={"risk": opportunity.risk.model_copy(update={"request_expiration_seconds": 0})}
        )
    else:
        opportunity = opportunity.model_copy(
            update={"risk": opportunity.risk.model_copy(update={"maximum_oracle_age_seconds": 0})}
        )
    assert not eligible_opportunity(escrow, opportunity, settings, at)


@pytest.mark.parametrize("change", ["future", "disabled", "expired"])
def test_admission_requires_current_available_enabled_state(request_case, change):
    config, _, _, held, pending, risk, _ = request_case
    if change == "future":
        risk = risk.model_copy(
            update={"block": risk.block.model_copy(update={"timestamp": pending.eligible_at + 1})}
        )
    elif change == "disabled":
        risk = risk.model_copy(update={"creation_disabled": True})
    else:
        pending = pending.model_copy(update={"eligible_at": pending.intent.deadline})
    with pytest.raises(ValueError):
        admit(config, held.payload, pending, risk)
