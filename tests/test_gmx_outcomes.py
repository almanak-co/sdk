"""Keeper outcome wire facts preserve receipt, decision and economic ownership scope."""

from datetime import UTC, datetime

import pytest
from test_gmx_receipts import captured_target
from test_gmx_request_reader import CapturedReader, accepted_request

from almanak.caip import AssetType
from almanak.connectors.gmx_v2.intents import IncreasePosition
from almanak.connectors.gmx_v2.observations import Snapshot
from almanak.connectors.gmx_v2.operations import CreateOrder
from almanak.connectors.gmx_v2.positions import Ownership
from almanak.connectors.gmx_v2.request_reader import next_event
from almanak.connectors.gmx_v2.state import DecisionRecord, RequestOutcome
from almanak.effects import AssetDelta, WalletAsset
from almanak.models import AssetAmount
from almanak.strategy_api import Memory
from almanak.triggers import TeardownEvent


def outcome():
    reader = CapturedReader()
    request = accepted_request(reader)
    target = captured_target(reader, request)
    native = AssetAmount(asset=AssetType("eip155:42161/slip44:60"), amount=0)
    inventory = (AssetAmount(asset=target.collateral_asset, amount=10**18), native)
    owned = Ownership(target=target, inventory=inventory)
    before = Snapshot(
        target=target,
        block=request.events[0].identity.block,
        owned=owned,
        wallets=inventory,
        position=None,
        allowance=0,
        native_balance=native,
        payer=target.fund,
        claimable_funding=tuple(
            AssetAmount(asset=asset, amount=0)
            for asset in (target.market.long_asset, target.market.short_asset)
        ),
    )
    event = next_event(reader, request, reader.blocks[max(reader.blocks)], maximum_block_span=2)
    after = before.model_copy(update={"block": event.identity.block})
    collateral = AssetAmount(asset=target.collateral_asset, amount=10**17)
    decision = DecisionRecord(
        trigger=TeardownEvent(started_at=datetime(2026, 1, 1, tzinfo=UTC)),
        identity=request.decision_identity,
        intent=IncreasePosition(target=target, collateral=collateral, size_delta_usd_e30=10**33),
        observations=(),
        memory=Memory(),
    )
    return RequestOutcome(
        decision=decision,
        operation=CreateOrder(
            target=target,
            order_type=2,
            collateral_delta=collateral,
            size_delta_usd_e30=10**33,
            execution_fee=request.execution_fee,
            acceptable_price=10**18,
        ),
        before=before,
        after=after,
        tx_hash=event.identity.transaction_hash,
        receipt=event.receipt,
        success=True,
        request=request.append(event),
        assets=(),
    )


def test_keeper_result_has_no_submitter_facts_and_round_trips():
    value = outcome()
    assert RequestOutcome.model_validate_json(value.model_dump_json()) == value
    assert not {"nonce", "family", "gas_paid", "transaction_plan"} & value.model_dump().keys()
    assert value.strategy_result().complete


@pytest.mark.parametrize("field", ["tx_hash", "receipt", "request", "assets"])
def test_keeper_outcome_refuses_unbound_facts(field):
    value = outcome()
    changes = {
        "tx_hash": "0x" + "99" * 32,
        "receipt": {**value.receipt, "blockHash": "0x" + "99" * 32},
        "request": None,
        "assets": (
            AssetDelta(
                key=WalletAsset(
                    account="foreign",
                    chain=value.after.block.chain,
                    asset=value.before.wallets[0].asset,
                ),
                amount=1,
            ),
        ),
    }
    with pytest.raises(ValueError):
        RequestOutcome.model_validate({**value.model_dump(), field: changes[field]})
