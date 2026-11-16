"""Prepared position requests escrow once and settle with deployed-order accounting.

Every input is synthetic (``tests/gmx_perpetual_support.py``). These checks prove
wiring, ordering and exact integer composition of the accounting components;
they are not Reader comparisons and establish no historical coverage.
"""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2.economic_funding import accumulated_funding
from almanak.connectors.gmx_v2.economic_perpetual_requests import (
    admit,
    cleanup,
    liquidate,
    liquidation_opportunity,
    next_liquidation,
    proportional_pending,
    settle,
    validate_position,
)
from almanak.connectors.gmx_v2.economic_perpetual_state import (
    EconomicPerpetualPosition,
    PerpetualEscrow,
)
from almanak.connectors.gmx_v2.economic_pnl import funding_amount, position_pnl
from almanak.connectors.gmx_v2.economic_position_collateral import (
    DecreaseCollateralInput,
    decrease_collateral,
)
from almanak.connectors.gmx_v2.economic_position_fees import (
    FeeAccumulators,
    PositionFeeInput,
    position_fees,
)
from almanak.connectors.gmx_v2.economic_position_math import (
    decrease_price,
    increase_price,
    realized_impact,
)
from almanak.connectors.gmx_v2.economic_requests import (
    cancellation_eligible,
    eligible_opportunity,
    release,
)
from almanak.connectors.gmx_v2.economic_simulation import advance, revocation
from almanak.connectors.gmx_v2.economic_valuation import next_borrowing_factor
from almanak.connectors.gmx_v2.intents import RevokePosition
from almanak.models import AssetAmount
from tests import gmx_perpetual_support as support
from tests.gmx_perpetual_support import COLLATERAL, SIZE, START

FLOAT_PER_SIZE = 10**45


@pytest.fixture(params=["arbitrum", "avalanche"])
def chain(request):
    return request.param


@pytest.fixture(params=["long", "short"])
def side(request):
    return request.param


@pytest.fixture
def case(chain, side):
    config = support.parameters(chain, side=side)
    settings = support.settings(config)
    state = support.seed(config, settings)
    payload = state.portfolio.positions[0].payload
    return SimpleNamespace(
        config=config,
        settings=settings,
        state=state,
        payload=payload,
        target=payload.target,
        allocation=config.allocations[0],
        side=side,
        long=side == "long",
    )


def risk_at(case, time, **changes):
    return support.frame(case.target, time, **changes).risk[0]


def opportunity_at(case, time, **changes):
    return support.frame(case.target, time, **changes).opportunities[0]


def admitted(case, payload, intent, time):
    after, movements = admit(
        case.config, payload, support.pending_for(intent, time), risk_at(case, time)
    )
    return after, movements


def opened(case, time=START, settle_at=START + 6):
    accepted, _ = admitted(case, case.payload, support.increase()(case.payload), time)
    filled, outputs = settle(accepted, opportunity_at(case, settle_at))
    released, _, _, _ = release(filled, case.settings, outputs)
    return released


def expected_latest(opportunity, target):
    indexes = accumulated_funding(opportunity.funding, opportunity.funding_indexes)
    long = target.side == "long"
    payments = indexes.long_payments if long else indexes.short_payments
    claims = indexes.long_claims if long else indexes.short_claims
    return FeeAccumulators(
        borrowing_factor=next_borrowing_factor(opportunity.accounting, long),
        funding_payment_per_size=payments.long,
        funding_long_claim_per_size=claims.long,
        funding_short_claim_per_size=claims.short,
    )


def expected_fees(opportunity, target, position, delta, improved, *, liquidation=False):
    stored = FeeAccumulators(
        borrowing_factor=0 if position is None else position.borrowing_factor,
        funding_payment_per_size=0 if position is None else position.funding_fee_amount_per_size,
        funding_long_claim_per_size=(
            0 if position is None else position.long_claimable_funding_amount_per_size
        ),
        funding_short_claim_per_size=(
            0 if position is None else position.short_claimable_funding_amount_per_size
        ),
    )
    return position_fees(
        PositionFeeInput(
            collateral_price=opportunity.spot.long.price,
            size_usd_e30=0 if position is None else position.size_usd_e30,
            size_delta_usd_e30=delta,
            position=stored,
            latest=expected_latest(opportunity, target),
            policy=opportunity.fees.select(improved),
            liquidation=liquidation,
        )
    )


def inventory(payload, asset):
    return next(value.amount for value in payload.owned.inventory if value.asset == asset)


def test_admission_escrows_collateral_and_fee_once_without_inventing_a_position(case):
    fee = case.allocation.execution_fee.amount
    before = inventory(case.payload, case.target.collateral_asset)
    after, movements = admitted(case, case.payload, support.increase()(case.payload), START)
    assert isinstance(after.escrow, PerpetualEscrow) and after.escrow.request.order_type == 2
    assert after.escrow.identity == "admission" and after.escrow.accepted_at == START
    assert after.spent[0].amount == COLLATERAL + fee and after.spent_notional_usd_e30 == SIZE
    # The request-sized approval is consumed by the escrow transfer.
    assert after.allowances[0].amount == 0
    assert after.owned.position is None
    assert inventory(after, case.target.collateral_asset) == before - COLLATERAL - fee
    assert sum(value.amount for value in movements) == -(COLLATERAL + fee)
    assert EconomicPerpetualPosition.model_validate_json(after.model_dump_json()) == after
    with pytest.raises(ValueError, match="new exact-target"):
        admitted(case, after, support.increase()(case.payload), START)


def test_admission_refuses_disabled_creation_and_repeats_without_a_notional_budget(case):
    disabled = risk_at(case, START).model_copy(update={"market_disabled": True})
    with pytest.raises(ValueError, match="historically disabled"):
        admit(
            case.config,
            case.payload,
            support.pending_for(support.increase()(case.payload), START),
            disabled,
        )
    payload = case.payload
    for _ in range(4):
        accepted, _ = admitted(case, payload, support.increase(10**15, SIZE)(payload), START)
        payload, _, _, _ = release(
            accepted, case.settings, accepted.escrow.inputs, owner_cancellation=True
        )
    assert payload.spent_notional_usd_e30 == 4 * SIZE
    accepted, _ = admitted(case, payload, support.increase(10**15, SIZE)(payload), START)
    assert accepted.spent_notional_usd_e30 == 5 * SIZE


def test_cancellation_refunds_escrow_and_reports_cumulative_input_spending(case):
    fee = case.allocation.execution_fee.amount
    payload = case.payload
    per_request = 10**15 + fee
    count = 8
    for _ in range(count):
        accepted, _ = admitted(case, payload, support.increase(10**15, 1)(payload), START)
        payload, _, keeper, refund = release(
            accepted, case.settings, accepted.escrow.inputs, owner_cancellation=True
        )
        assert keeper.amount == 0 and refund.amount == fee
        assert (
            inventory(payload, case.target.collateral_asset)
            == inventory(accepted, case.target.collateral_asset) + 10**15
        )
    assert payload.spent[0].amount == count * per_request
    # Spending is reported, never limited: only owned inventory bounds another request.
    accepted, _ = admitted(case, payload, support.increase(10**15, 1)(payload), START)
    assert accepted.spent[0].amount == (count + 1) * per_request


def test_increase_settlement_takes_fees_from_collateral_and_stores_latest_accumulators(case):
    accepted, _ = admitted(case, case.payload, support.increase()(case.payload), START)
    opportunity = opportunity_at(case, START + 6)
    filled, outputs = settle(accepted, opportunity)
    position = filled.owned.position
    price = increase_price(opportunity.pricing, SIZE, case.long)
    fees = expected_fees(opportunity, case.target, None, SIZE, price.balance_improved)
    latest = expected_latest(opportunity, case.target)
    assert outputs == ()
    assert position.identity == accepted.escrow.identity
    assert position.size_usd_e30 == SIZE
    assert position.index_amount.amount == price.base_size_delta_tokens
    assert position.collateral.amount == COLLATERAL - fees.total
    assert fees.total > 0 and fees.funding_amount == 0 and fees.borrowing_amount == 0
    assert position.pending_impact_amount == price.price_impact_amount
    assert position.borrowing_factor == latest.borrowing_factor
    assert position.funding_fee_amount_per_size == latest.funding_payment_per_size
    assert position.long_claimable_funding_amount_per_size == latest.funding_long_claim_per_size
    assert position.short_claimable_funding_amount_per_size == latest.funding_short_claim_per_size
    assert (position.increased_at, position.decreased_at) == (START + 6, 0)
    assert filled.escrow == accepted.escrow and filled.spent == accepted.spent
    released, movements, keeper, refund = release(filled, case.settings, outputs)
    assert released.escrow is None
    assert (
        keeper.amount == 10**15
        and refund.amount == case.allocation.execution_fee.amount - keeper.amount
    )
    assert refund.asset == case.allocation.native_asset
    assert [value.amount for value in movements] == [refund.amount]
    assert EconomicPerpetualPosition.model_validate_json(released.model_dump_json()) == released


def test_second_increase_charges_accrued_funding_and_borrowing_before_adding_collateral(case):
    held = opened(case)
    position = held.owned.position
    accepted, _ = admitted(case, held, support.increase(5 * 10**16, SIZE // 2)(held), START + 10)
    opportunity = opportunity_at(case, START + 16)
    filled, _ = settle(accepted, opportunity)
    after = filled.owned.position
    price = increase_price(opportunity.pricing, SIZE // 2, case.long)
    fees = expected_fees(opportunity, case.target, position, SIZE // 2, price.balance_improved)
    latest = expected_latest(opportunity, case.target)
    delta_index = latest.funding_payment_per_size - position.funding_fee_amount_per_size
    assert delta_index > 0
    assert fees.funding_amount == -(-delta_index * SIZE // FLOAT_PER_SIZE)
    assert fees.funding_amount == funding_amount(
        latest.funding_payment_per_size, position.funding_fee_amount_per_size, SIZE, payment=True
    )
    assert fees.borrowing_usd_e30 > 0 and fees.borrowing_usd_e30 == (
        (latest.borrowing_factor - position.borrowing_factor) * SIZE // 10**30
    )
    assert after.collateral.amount == position.collateral.amount + 5 * 10**16 - fees.total
    assert after.size_usd_e30 == SIZE + SIZE // 2
    assert after.index_amount.amount == position.index_amount.amount + price.base_size_delta_tokens
    assert after.pending_impact_amount == position.pending_impact_amount + price.price_impact_amount
    assert after.identity == position.identity and after.increased_at == START + 16
    claims = filled.owned.claimable_funding
    long_claim = (
        (latest.funding_long_claim_per_size - position.long_claimable_funding_amount_per_size)
        * SIZE
        // FLOAT_PER_SIZE
    )
    assert [value.amount for value in claims] == [long_claim, fees.short_funding_claim]
    assert long_claim == fees.long_funding_claim > 0


@pytest.mark.parametrize("move", [None, support.PROFIT_FOR_SHORT])
def test_partial_decrease_realizes_pnl_impact_fees_and_withdrawal_in_deployed_order(case, move):
    held = opened(case)
    position = held.owned.position
    withdrawal, delta = 10**16, SIZE // 2
    accepted, _ = admitted(case, held, support.decrease(withdrawal, delta)(held), START + 10)
    opportunity = opportunity_at(case, START + 16, long_price=move)
    filled, outputs = settle(accepted, opportunity)
    after = filled.owned.position
    size, tokens = position.size_usd_e30, position.index_amount.amount
    price = decrease_price(opportunity.pricing, size, tokens, delta, case.long)
    pnl = position_pnl(opportunity.pnl, size, tokens, delta, case.long)
    impact = realized_impact(
        opportunity.impact, size, position.pending_impact_amount, delta, price.price_impact_usd_e30
    )
    fees = expected_fees(opportunity, case.target, position, delta, price.balance_improved)
    profit_price = opportunity.spot.long.price if case.long else opportunity.spot.short.price
    paid = decrease_collateral(
        DecreaseCollateralInput(
            collateral_price=opportunity.spot.long.price,
            pnl_price=profit_price,
            collateral_amount=position.collateral.amount,
            base_pnl_usd_e30=pnl.pnl_usd_e30,
            impact_usd_e30=impact.total_usd_e30,
            deferred_impact_usd_e30=impact.claimable_difference_usd_e30,
            fees=fees,
            withdrawal=withdrawal,
            insolvent_close_allowed=False,
            holding_address_available=True,
        )
    )
    assert after.size_usd_e30 == size - delta
    assert after.index_amount.amount == tokens - pnl.size_delta_tokens
    assert after.collateral.amount == paid.balances.remaining
    assert after.pending_impact_amount == position.pending_impact_amount - proportional_pending(
        position.pending_impact_amount, delta, size
    )
    assert (after.increased_at, after.decreased_at) == (START + 6, START + 16)
    assert after.borrowing_factor == expected_latest(opportunity, case.target).borrowing_factor
    received = {value.asset: value.amount for value in outputs}
    assert received.get(case.target.collateral_asset, 0) == paid.balances.output
    profit_asset = case.target.market.long_asset if case.long else case.target.market.short_asset
    if profit_asset != case.target.collateral_asset:
        assert received.get(profit_asset, 0) == paid.balances.secondary
    assert paid.balances.output >= withdrawal
    assert fees.funding_amount > 0 and (pnl.pnl_usd_e30 > 0) == (move is not None and not case.long)
    assert (paid.balances.secondary > 0) == (move is not None and not case.long)
    released, movements, _, _ = release(filled, case.settings, outputs)
    for value in outputs:
        assert inventory(released, value.asset) == inventory(filled, value.asset) + value.amount


@pytest.mark.parametrize("move", [None, support.PROFIT_FOR_SHORT])
def test_full_close_returns_all_remaining_collateral_and_profit_token(case, move):
    held = opened(case)
    position = held.owned.position
    accepted, _ = admitted(case, held, support.close()(held), START + 10)
    assert accepted.escrow.request.size_delta_usd_e30 == position.size_usd_e30
    assert accepted.escrow.request.collateral_delta.amount == 0
    opportunity = opportunity_at(case, START + 16, long_price=move)
    filled, outputs = settle(accepted, opportunity)
    assert filled.owned.position is None
    size, tokens = position.size_usd_e30, position.index_amount.amount
    price = decrease_price(opportunity.pricing, size, tokens, size, case.long)
    pnl = position_pnl(opportunity.pnl, size, tokens, size, case.long)
    impact = realized_impact(
        opportunity.impact, size, position.pending_impact_amount, size, price.price_impact_usd_e30
    )
    fees = expected_fees(opportunity, case.target, position, size, price.balance_improved)
    profit_price = opportunity.spot.long.price if case.long else opportunity.spot.short.price
    paid = decrease_collateral(
        DecreaseCollateralInput(
            collateral_price=opportunity.spot.long.price,
            pnl_price=profit_price,
            collateral_amount=position.collateral.amount,
            base_pnl_usd_e30=pnl.pnl_usd_e30,
            impact_usd_e30=impact.total_usd_e30,
            deferred_impact_usd_e30=impact.claimable_difference_usd_e30,
            fees=fees,
            withdrawal=0,
            insolvent_close_allowed=False,
            holding_address_available=True,
        )
    )
    received = {value.asset: value.amount for value in outputs}
    assert received[case.target.collateral_asset] == paid.balances.output + paid.balances.remaining
    profit_asset = case.target.market.long_asset if case.long else case.target.market.short_asset
    if profit_asset != case.target.collateral_asset:
        assert received.get(profit_asset, 0) == paid.balances.secondary
    assert len(received) == (2 if paid.balances.secondary else 1)
    released, _, _, _ = release(filled, case.settings, outputs)
    assert released.escrow is None and released.spent == accepted.spent
    assert any(value.amount for value in released.owned.claimable_funding)


def test_constraint_breach_cancels_with_keeper_payment_and_exact_refund(case):
    fee = case.allocation.execution_fee.amount
    accepted, _ = admitted(case, case.payload, support.increase(10**15, SIZE)(case.payload), START)
    pending = support.pending_for(accepted.escrow.intent, START + 6)
    step = advance(
        case.config, case.settings, accepted, pending, support.record_at(case.target, START + 6)
    )
    assert step.phase == "cancelled" and "leverage" in step.reason
    assert step.after.escrow is None and step.after.owned.position is None
    assert step.keeper.amount == 10**15 and step.refund.amount == fee - step.keeper.amount
    returned = {value.key.asset: value.amount for value in step.assets}
    assert returned[case.target.collateral_asset] == 10**15
    assert returned[case.allocation.native_asset] == step.refund.amount
    assert step.after.spent == accepted.spent and step.gas.amount == 0


def test_decrease_without_a_position_and_insufficient_decrease_cancel(case):
    accepted, _ = admitted(case, case.payload, support.increase()(case.payload), START)
    decrease = support.decrease(0, SIZE // 2)(case.payload)
    with pytest.raises(ValueError, match="retained aggregate position"):
        admitted(case, case.payload, decrease, START)
    # A retained decrease escrow whose position vanished settles as a refunded cancellation.
    escrow = accepted.escrow.model_copy(
        update={
            "intent": decrease,
            "request": accepted.escrow.request.model_copy(
                update={"order_type": 4, "collateral_delta": decrease.collateral_withdrawal}
            ),
        }
    )
    orphan = accepted.model_copy(update={"escrow": escrow})
    step = advance(
        case.config,
        case.settings,
        orphan,
        support.pending_for(decrease, START + 6),
        support.record_at(case.target, START + 6),
    )
    assert step.phase == "cancelled" and "no retained position" in step.reason
    assert step.after.spent == accepted.spent


def test_eligibility_and_cancellation_use_the_request_direction_controls(case):
    held = opened(case)
    increase, _ = admitted(case, case.payload, support.increase()(case.payload), START)
    decrease, _ = admitted(case, held, support.close()(held), START)
    opportunity = opportunity_at(case, START + 6)
    assert eligible_opportunity(increase.escrow, opportunity, case.settings, START + 6)
    assert eligible_opportunity(decrease.escrow, opportunity, case.settings, START + 6)
    disabled = opportunity.risk.decreases.model_copy(update={"execution_disabled": True})
    blocked = opportunity.model_copy(
        update={"risk": opportunity.risk.model_copy(update={"decreases": disabled})}
    )
    assert eligible_opportunity(increase.escrow, blocked, case.settings, START + 6)
    assert not eligible_opportunity(decrease.escrow, blocked, case.settings, START + 6)
    assert not eligible_opportunity(increase.escrow, opportunity, case.settings, START + 4)
    expired = opportunity.risk.model_copy(update={"request_expiration_seconds": 5})
    assert cancellation_eligible(increase.escrow, expired, START + 5)
    frozen = expired.model_copy(
        update={"increases": expired.increases.model_copy(update={"cancellation_disabled": True})}
    )
    assert not cancellation_eligible(increase.escrow, frozen, START + 5)
    assert cancellation_eligible(decrease.escrow, frozen, START + 5)


@pytest.mark.parametrize("insolvent", [False, True])
def test_liquidation_sweep_settles_only_strictly_after_the_last_change(case, insolvent):
    held = opened(case)
    move = support.liquidation_prices(case.side, insolvent=insolvent)
    same_second = support.record_at(case.target, START + 6, long_price=move)
    assert liquidation_opportunity(held, same_second, START + 6) is None
    healthy = support.record_at(case.target, START + 9)
    assert liquidation_opportunity(held, healthy, START + 9) is None
    crashed = support.record_at(case.target, START + 9, long_price=move)
    found = liquidation_opportunity(held, crashed, START + 9)
    assert found is not None
    opportunity, reason = found
    assert reason == ("min collateral" if insolvent else "min collateral for leverage")
    paused = support.record_at(
        case.target,
        START + 9,
        long_price=move,
        risk=crashed.payload.risk[0].model_copy(update={"liquidation_disabled": True}),
    )
    assert liquidation_opportunity(held, paused, START + 9) is None
    after, movements, settlement = liquidate(held, opportunity)
    assert after.owned.position is None and after.escrow is None
    assert after.spent == held.spent and after.allowances == held.allowances
    position = held.owned.position
    fees = expected_fees(
        opportunity, case.target, position, position.size_usd_e30, True, liquidation=True
    )
    assert fees.liquidation_amount > 0
    if insolvent:
        assert settlement.unpaid_usd_e30 > 0 and not settlement.fees_collected
        assert movements == () and settlement.outputs == ()
    else:
        assert settlement.unpaid_usd_e30 == 0 and settlement.fees_collected
        assert [value.amount for value in movements] == [
            value.amount for value in settlement.outputs
        ]
        assert all(value.amount > 0 for value in settlement.outputs)
        for value in settlement.outputs:
            assert inventory(after, value.asset) == inventory(held, value.asset) + value.amount
    assert any(value.amount for value in after.owned.claimable_funding)
    assert EconomicPerpetualPosition.model_validate_json(after.model_dump_json()) == after


def test_next_liquidation_scans_rows_after_the_start_and_within_the_bound(case):
    held = opened(case)
    move = support.liquidation_prices(case.side)
    frames = [
        support.frame(case.target, time, long_price=move if time >= START + 9 else None)
        for time in range(START, START + 12)
    ]
    book = support.SyntheticBook(case.settings, {case.target: frames})
    timeline = book.timelines[case.target]
    assert next_liquidation(timeline, held, START, START + 8) is None
    assert next_liquidation(timeline, held, START, START + 9) == START + 9
    assert next_liquidation(timeline, held, START + 10, START + 11) == START + 10
    assert next_liquidation(timeline, case.payload, START, START + 11) is None


def test_cleanup_revokes_authority_claims_funding_and_refuses_deferred_collateral(case):
    held = opened(case)
    assert not any(value.amount for value in held.allowances)
    # A residual allowance, as a crash between approval and request would leave.
    residual = held.model_copy(
        update={"allowances": (held.allowances[0].model_copy(update={"amount": 1}),)}
    )
    risk = risk_at(case, START + 7)
    with pytest.raises(ValueError, match="revoked first"):
        cleanup(residual, risk)
    step = revocation(case.settings, residual)
    revoked = step.after
    assert step.phase == "revoked" and step.assets == ()
    assert not any(value.amount for value in revoked.allowances)
    claims = tuple(
        value.model_copy(update={"amount": 5}) for value in revoked.owned.claimable_funding
    )
    deferred = (
        revoked.owned.deferred_collateral[0].model_copy(update={"amount": 7}),
        revoked.owned.deferred_collateral[1],
    )
    deferred_owed = revoked.model_copy(
        update={
            "owned": revoked.owned.model_copy(
                update={"claimable_funding": claims, "deferred_collateral": deferred}
            )
        }
    )
    with pytest.raises(ValueError, match="claim window and factor are not modeled"):
        cleanup(deferred_owed, risk)
    owed = revoked.model_copy(
        update={"owned": revoked.owned.model_copy(update={"claimable_funding": claims})}
    )
    with pytest.raises(ValueError, match="claims are historically disabled"):
        cleanup(owed, risk.model_copy(update={"funding_claim_disabled": True}))
    claimed, movements, phase = cleanup(owed, risk)
    assert phase == "claimed"
    assert sorted(value.amount for value in movements) == [5, 5]
    assert not any(value.amount for value in claimed.owned.claimable_funding)
    assert not any(value.amount for value in claimed.owned.deferred_collateral)
    assert inventory(claimed, claims[0].asset) == inventory(owed, claims[0].asset) + 5
    with pytest.raises(ValueError, match="already closed"):
        cleanup(claimed, risk)


def test_revocation_while_escrow_is_pending_waits_for_settlement_or_cancellation(case):
    accepted, _ = admitted(case, case.payload, support.increase()(case.payload), START)
    revoke = RevokePosition(target=case.target)
    waiting = advance(
        case.config,
        case.settings,
        accepted,
        support.pending_for(revoke, START + 3),
        support.record_at(case.target, START + 3),
    )
    assert waiting.phase == "waiting" and waiting.after == accepted
    settled = advance(
        case.config,
        case.settings,
        accepted,
        support.pending_for(revoke, START + 6),
        support.record_at(case.target, START + 6),
    )
    assert settled.phase == "settled" and settled.after.owned.position is not None
    other = support.increase(1, 1)(case.payload)
    with pytest.raises(ValueError, match="cannot replace"):
        advance(
            case.config,
            case.settings,
            accepted,
            support.pending_for(other, START + 6),
            support.record_at(case.target, START + 6),
        )


def test_resumed_state_refuses_changed_escrow_economics(case):
    accepted, _ = admitted(case, case.payload, support.increase()(case.payload), START)
    validate_position(case.allocation, accepted, START)
    with pytest.raises(ValueError, match="admission time"):
        validate_position(case.allocation, accepted, START - 1)
    changed = accepted.escrow.request.model_copy(update={"acceptable_price": 1})
    with pytest.raises(ValueError, match="price bound"):
        validate_position(
            case.allocation,
            accepted.model_copy(
                update={"escrow": accepted.escrow.model_copy(update={"request": changed})}
            ),
            START,
        )
    # Cumulative notional is reported, not limited.
    validate_position(
        case.allocation, accepted.model_copy(update={"spent_notional_usd_e30": 50 * SIZE}), START
    )
    with pytest.raises(ValueError, match="position allocation"):
        validate_position(SimpleNamespace(), accepted, START)


def test_short_position_settles_profit_in_the_short_asset_with_claims_per_asset(chain):
    config = support.parameters(chain, side="short")
    settings = support.settings(config)
    payload = support.seed(config, settings).portfolio.positions[0].payload
    case = SimpleNamespace(
        config=config,
        settings=settings,
        payload=payload,
        target=payload.target,
        allocation=config.allocations[0],
        side="short",
        long=False,
    )
    held = opened(case)
    accepted, _ = admitted(case, held, support.close()(held), START + 10)
    filled, outputs = settle(
        accepted, opportunity_at(case, START + 16, long_price=support.PROFIT_FOR_SHORT)
    )
    assets = [value.asset for value in outputs]
    assert assets == [case.target.market.long_asset, case.target.market.short_asset]
    assert all(value.amount > 0 for value in outputs)
    assert filled.owned.claimable_funding[1].asset == case.target.market.short_asset
    assert AssetAmount.model_validate(outputs[1].model_dump()) == outputs[1]
