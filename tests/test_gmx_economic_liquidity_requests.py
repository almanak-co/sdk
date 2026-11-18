"""GM prepared admission retains share escrow and exact finite authority through cancellation."""

from datetime import UTC, datetime

import pytest

from almanak.backtest.engine import Pending
from almanak.connectors.gmx_v2.economic_accounting import inventory_after
from almanak.connectors.gmx_v2.economic_parameters import initial, wallet
from almanak.connectors.gmx_v2.economic_prepare import binding
from almanak.connectors.gmx_v2.economic_requests import admit, cancellation_eligible, release
from almanak.connectors.gmx_v2.economic_state import EconomicPosition, LiquidityEscrow
from almanak.connectors.gmx_v2.exchange_domain import Close, Open
from almanak.effects import AssetDelta
from almanak.models import AssetAmount
from almanak.triggers import IntervalEvent, IntervalSelection
from tests.test_gmx_economic_book import prepared as prepared
from tests.test_gmx_economic_configuration import selected_author
from tests.test_gmx_economic_liquidity_risk import permissive_risk
from tests.test_gmx_economic_valuation import flat_deposit_state
from tests.test_gmx_economic_valuation import valuation as valuation


@pytest.fixture
def admitted_gm(prepared, valuation):
    _, swap_config, settings, _ = prepared
    author = selected_author(swap_config, "liquidity")
    allocation = author.allocations[0]
    fund = settings.funds[0]
    balances = (
        *allocation.initial_inventory.root,
        AssetAmount(asset=allocation.native_asset, amount=10**18),
        AssetAmount(asset=allocation.market.share_asset, amount=0),
    )
    settings = settings.model_copy(
        update={
            "liquidity_model": "fixed_external_gm_valuation",
            "funds": (fund.model_copy(update={"balances": balances}),),
        }
    )
    config = binding.selected_parameters(author, settings)
    state = initial(settings, config, IntervalSelection(configured=config.decision_trigger))
    payload = state.portfolio.positions[0].payload
    target = payload.target
    intent = Open(
        target=target,
        maximum0=allocation.entry_amounts.root[0],
        maximum1=allocation.entry_amounts.root[1],
        deadline=settings.schedule.start + 300,
    )
    timestamp = datetime.fromtimestamp(settings.schedule.start, UTC)
    pending = Pending(
        identity="gm-admission",
        decision_identity="gm-decision",
        trigger=IntervalEvent(started_at=timestamp, due_at=timestamp),
        intent=intent,
        decision_time=settings.schedule.start,
        eligible_at=settings.schedule.start,
    )
    risk = permissive_risk(flat_deposit_state(valuation))
    risk = risk.model_copy(
        update={
            "market": target.market,
            "block": risk.block.model_copy(
                update={"chain": target.fund.chain, "timestamp": settings.schedule.start}
            ),
        }
    )
    # The risk fixture is synthetic; its switches and expiry isolate admission accounting.
    from almanak.connectors.gmx_v2.economic_data import accounting_market

    risk = risk.model_copy(update={"market": accounting_market(target.market)})
    return config, settings, payload, pending, risk


def test_gm_acceptance_escrows_principal_and_fee_without_inventing_shares(admitted_gm):
    config, settings, payload, pending, risk = admitted_gm
    after, movements = admit(config, payload, pending, risk)
    assert isinstance(after.escrow, LiquidityEscrow)
    assert after.escrow.inputs == config.allocations[0].entry_amounts.root
    assert after.owned.market_tokens.amount == 0
    assert EconomicPosition.model_validate_json(after.model_dump_json()) == after
    restored, _, keeper, refund = release(
        after, settings, after.escrow.inputs, owner_cancellation=True
    )
    assert restored.spent == after.spent and restored.owned.market_tokens.amount == 0
    assert keeper.amount == 0 and refund.amount == config.allocations[0].execution_fee.amount
    assert sum(v.amount for v in movements) < 0


def test_share_withdrawal_escrow_survives_cancel_and_keeps_cumulative_inputs(admitted_gm):
    config, settings, payload, pending, risk = admitted_gm
    shares = config.allocations[0].partial_withdrawal.amount
    owned = inventory_after(
        payload.owned,
        (AssetDelta(key=wallet(payload.target, payload.target.market.share_asset), amount=shares),),
    )
    payload = payload.model_copy(update={"owned": owned})
    minima = config.allocations[0].withdrawal_minimums.root
    intent = Close(
        target=payload.target,
        minimum0=minima[0],
        minimum1=minima[1],
        deadline=pending.intent.deadline,
    )
    accepted, _ = admit(config, payload, pending.model_copy(update={"intent": intent}), risk)
    assert accepted.owned.market_tokens.amount == 0
    assert accepted.escrow.inputs == (
        AssetAmount(asset=payload.target.market.share_asset, amount=shares),
    )
    cancelled, _, _, _ = release(
        accepted, settings, accepted.escrow.inputs, owner_cancellation=True
    )
    assert cancelled.owned.market_tokens.amount == shares
    assert (
        next(v.amount for v in cancelled.spent if v.asset == payload.target.market.share_asset)
        == shares
    )
    assert cancelled.spent == accepted.spent


def test_gm_request_cancellation_uses_its_own_family_switch(admitted_gm):
    config, _, payload, pending, risk = admitted_gm
    accepted, _ = admit(config, payload, pending, risk)
    time = accepted.escrow.accepted_at + risk.request_expiration_seconds
    assert cancellation_eligible(accepted.escrow, risk, time)
    wrong = risk.model_copy(
        update={"withdrawals": risk.withdrawals.model_copy(update={"cancellation_disabled": True})}
    )
    assert cancellation_eligible(accepted.escrow, wrong, time)
    blocked = risk.model_copy(
        update={"deposits": risk.deposits.model_copy(update={"cancellation_disabled": True})}
    )
    assert not cancellation_eligible(accepted.escrow, blocked, time)


@pytest.mark.parametrize("owned_native,gas,remaining", [(0, 8, 0), (4, 8, 2), (10, 8, 2)])
def test_eoa_gas_consumes_unallocated_reserve_before_owned_native_cash(
    admitted_gm, owned_native, gas, remaining
):
    from almanak.connectors.gmx_v2.economic_accounting import apply_fund_gas
    from almanak.connectors.gmx_v2.economic_state import EconomicSnapshot

    config, settings, payload, _, risk = admitted_gm
    native = config.allocations[0].native_asset
    owned = inventory_after(
        payload.owned, (AssetDelta(key=wallet(payload.target, native), amount=owned_native),)
    )
    payload = payload.model_copy(update={"owned": owned})
    measured = tuple(
        v.model_copy(update={"amount": 10}) if v.asset == native else v for v in owned.inventory
    )
    before = EconomicSnapshot(
        target=payload.target,
        block=risk.block,
        owned=owned,
        wallets=measured,
        market_tokens=owned.market_tokens,
        allowances=payload.allowances,
        native_balance=AssetAmount(asset=native, amount=10),
        payer=payload.target.fund,
        simulation_time=settings.schedule.start,
        escrow=None,
    )
    after = apply_fund_gas(before, payload, AssetAmount(asset=native, amount=gas))
    assert next(v.amount for v in after.owned.inventory if v.asset == native) == remaining
    assert after.spent == payload.spent and after.allowances == payload.allowances
    separate = before.model_copy(
        update={
            "payer": before.payer.model_copy(
                update={"address": "0x1111111111111111111111111111111111111111"}
            )
        }
    )
    assert apply_fund_gas(separate, payload, AssetAmount(asset=native, amount=gas)) == payload


def test_restored_gm_escrow_requires_recorded_principal_and_fee_spending(admitted_gm):
    from almanak.connectors.gmx_v2.economic_model import validate_escrow_spending

    config, _, payload, pending, risk = admitted_gm
    after, _ = admit(config, payload, pending, risk)
    spending = {value.asset: value.amount for value in after.spent}
    validate_escrow_spending(after.escrow, spending)
    spending[after.escrow.execution_fee.asset] -= 1
    with pytest.raises(ValueError, match="escrow exceeds"):
        validate_escrow_spending(after.escrow, spending)


@pytest.mark.parametrize("field", ["fee", "deadline", "admission", "principal", "shares"])
def test_restored_gm_deposit_refuses_changed_admitted_terms(admitted_gm, field):
    from almanak.connectors.gmx_v2.economic_model import validate_exchange_escrow

    config, _, payload, pending, risk = admitted_gm
    after, _ = admit(config, payload, pending, risk)
    escrow, allocation = after.escrow, config.allocations[0]
    validate_exchange_escrow(allocation, escrow, pending.eligible_at)
    request = escrow.request
    principal = request.amounts.root[0]
    changes = {
        "fee": {"execution_fee": request.execution_fee.model_copy(update={"amount": 1})},
        "deadline": {"admission_deadline": request.admission_deadline + 1},
        "principal": {
            "amounts": request.amounts.model_copy(
                update={
                    "root": (
                        principal.model_copy(update={"amount": principal.amount + 1}),
                        request.amounts.root[1],
                    )
                }
            )
        },
        "shares": {
            "minimum_market_tokens": request.minimum_market_tokens.model_copy(
                update={"amount": request.minimum_market_tokens.amount + 1}
            )
        },
    }
    changed = (
        escrow.model_copy(update={"accepted_at": pending.eligible_at + 1})
        if field == "admission"
        else escrow.model_copy(update={"request": request.model_copy(update=changes[field])})
    )
    with pytest.raises(ValueError, match="GM modeled"):
        validate_exchange_escrow(allocation, changed, pending.eligible_at)
