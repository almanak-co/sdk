"""Captured EOA receipts account for real escrow, keeper payment and fund refunds."""

import pytest
from test_gmx_positions import target
from test_gmx_request_reader import CapturedReader, accepted_request

from almanak.connectors.gmx_v2.receipts import execution_fee, remaining_fee, token_movement
from almanak.connectors.gmx_v2.request_reader import next_event
from almanak.gateway.ethereum import DataError


def captured_target(reader, request):
    original = target()
    contracts = original.market.contracts.model_copy(
        update={
            "event_emitter": "0xc8ee91a54287db53897056e12d9819156d3822fb",
            "order_handler": "0x63492b775e30a9e6b4b4761c12605eb9d071d5e9",
            "order_vault": "0x31ef83a530fde1b38ee9a18093a333d8bbbc40d5",
        }
    )
    market = original.market.model_copy(
        update={
            "chain": reader.chain,
            "share_asset": request.market,
            "index_asset": request.execution_fee.asset,
            "long_asset": request.execution_fee.asset,
            "wrapped_native": request.execution_fee.asset,
            "contracts": contracts,
        }
    )
    return original.model_copy(
        update={
            "fund": request.fund,
            "market": market,
            "collateral_asset": request.execution_fee.asset,
        }
    )


def settled_request():
    reader = CapturedReader()
    request = accepted_request(reader)
    scope = captured_target(reader, request)
    event = next_event(reader, request, reader.blocks[max(reader.blocks)], maximum_block_span=2)
    return request, scope, event


def test_real_keeper_payment_is_distinct_from_position_collateral():
    request, scope, event = settled_request()
    accepted = request.events[0].receipt
    # The original direct EOA fixture wraps native value in the vault.
    assert token_movement(accepted, scope, request.execution_fee.asset) == 0
    fee = execution_fee(event.receipt, scope, request.execution_fee.amount)
    # The fund receives exactly the refund.
    assert (fee.paid, fee.refunded, fee.fund_received) == (
        5620218894242082,
        14379781105757918,
        14379781105757918,
    )
    assert token_movement(event.receipt, scope, request.execution_fee.asset) == 0
    assert remaining_fee(request.append(event), scope) == 0


def test_real_keeper_payment_refuses_another_escrow_or_fund():
    request, scope, event = settled_request()
    with pytest.raises(DataError, match="retained escrow"):
        execution_fee(event.receipt, scope, request.execution_fee.amount + 1)
    foreign = scope.model_copy(update={"fund": target().fund})
    with pytest.raises(DataError, match="retained escrow"):
        execution_fee(event.receipt, foreign, request.execution_fee.amount)


def test_real_self_cancellation_returns_both_keeper_payment_and_refund_to_fund():
    reader = CapturedReader()
    request = accepted_request(reader)
    scope = captured_target(reader, request)
    receipt = reader.proof["actions"][-1]["receipt"]
    fee = execution_fee(receipt, scope, request.execution_fee.amount)
    assert fee.paid == 2261624691693808
    assert fee.refunded == 17738375308306192
    assert fee.fund_received == request.execution_fee.amount
    assert token_movement(receipt, scope, request.execution_fee.asset) == 10**17


def test_zero_fee_is_only_measured_when_no_fee_events_and_no_remaining_escrow():
    reader = CapturedReader()
    request = accepted_request(reader)
    scope = captured_target(reader, request)
    receipt = {"logs": []}
    assert execution_fee(receipt, scope, 0).fund_received == 0
    with pytest.raises(DataError, match="KeeperExecutionFee"):
        execution_fee(receipt, scope, 1)
