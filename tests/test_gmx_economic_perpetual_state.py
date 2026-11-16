"""Hypothetical identities and retained escrow cannot be rewritten on resume."""

import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2.economic_component import protocol_closed
from almanak.connectors.gmx_v2.economic_perpetual_component import initial_position
from almanak.connectors.gmx_v2.economic_perpetual_state import (
    EconomicLiquidationBatch,
    EconomicLiquidationResult,
    EconomicPerpetualPosition,
    PerpetualEscrow,
    PerpetualOwnership,
    SimulatedPosition,
)
from almanak.connectors.gmx_v2.intents import IncreasePosition
from almanak.connectors.gmx_v2.operations import CreateOrder
from almanak.connectors.gmx_v2.positions import Ownership
from almanak.models import AssetAmount
from almanak.strategy_api import SimulationEvidence
from tests.test_gmx_positions import target


@pytest.fixture
def position():
    value = target()
    zero = tuple(
        AssetAmount(asset=asset, amount=0)
        for asset in (value.market.long_asset, value.market.short_asset)
    )
    collateral = AssetAmount(asset=value.collateral_asset, amount=20)
    intent = IncreasePosition(target=value, collateral=collateral, size_delta_usd_e30=100)
    request = CreateOrder(
        target=value,
        order_type=2,
        collateral_delta=collateral,
        size_delta_usd_e30=100,
        execution_fee=collateral.model_copy(update={"amount": 3}),
        acceptable_price=100,
    )
    return EconomicPerpetualPosition(
        target=value,
        owned=PerpetualOwnership(
            target=value,
            inventory=zero,
            position=None,
            claimable_funding=zero,
            deferred_collateral=zero,
        ),
        spent=(collateral.model_copy(update={"amount": 23}),),
        spent_notional_usd_e30=100,
        allowances=(zero[0],),
        escrow=PerpetualEscrow(
            identity="hypothetical-order-1", accepted_at=1, intent=intent, request=request
        ),
    )


def test_perpetual_resume_roundtrip_retains_economic_identity(position):
    restored = EconomicPerpetualPosition.model_validate_json(position.model_dump_json())
    assert restored == position
    assert restored.escrow.identity == "hypothetical-order-1"


@pytest.mark.parametrize("field,value", [("spent", 22), ("spent_notional_usd_e30", 99)])
def test_perpetual_resume_refuses_reduced_consumed_authority(position, field, value):
    raw = position.model_dump(mode="json")
    if field == "spent":
        raw[field][0]["amount"] = value
    else:
        raw[field] = value
    with pytest.raises(ValueError, match="spending"):
        EconomicPerpetualPosition.model_validate_json(json.dumps(raw))


def test_perpetual_resume_refuses_changed_request_economics(position):
    raw = position.model_dump(mode="json")
    raw["escrow"]["request"]["size_delta_usd_e30"] = 99
    with pytest.raises(ValueError, match="economic amounts"):
        EconomicPerpetualPosition.model_validate_json(json.dumps(raw))


def test_simulated_position_does_not_require_a_native_position_key(position):
    collateral = position.escrow.intent.collateral
    held = SimulatedPosition(
        identity="hypothetical-position-1",
        size_usd_e30=100,
        index_amount=collateral,
        collateral=collateral,
        pending_impact_amount=-1,
        borrowing_factor=1,
        funding_fee_amount_per_size=2,
        long_claimable_funding_amount_per_size=3,
        short_claimable_funding_amount_per_size=4,
        increased_at=1,
        decreased_at=0,
    )
    raw = position.owned.model_dump(mode="json")
    raw["position"] = held.model_dump(mode="json")
    restored = PerpetualOwnership.model_validate_json(json.dumps(raw))
    assert restored.position == held
    assert "key" not in restored.position.model_dump()


def test_perpetual_initializer_preserves_shared_cash_without_inventing_positions(position):
    native = Ownership(target=position.target, inventory=position.owned.inventory)
    config = SimpleNamespace(target=lambda name: position.target)
    initialized = initial_position(config, SimpleNamespace(name="Main"), native)
    assert initialized.owned.inventory == native.inventory
    assert initialized.owned.position is None and initialized.escrow is None
    assert initialized.spent[0].amount == initialized.spent_notional_usd_e30 == 0
    assert protocol_closed(initialized)
    deferred = initialized.owned.deferred_collateral[0].model_copy(update={"amount": 1})
    unresolved = initialized.model_copy(
        update={
            "owned": initialized.owned.model_copy(
                update={"deferred_collateral": (deferred, initialized.owned.deferred_collateral[1])}
            )
        }
    )
    assert not protocol_closed(unresolved)


def test_perpetual_initializer_refuses_unmodeled_native_funding(position):
    native = Ownership(
        target=position.target,
        inventory=position.owned.inventory,
        claimable_funding=(position.owned.claimable_funding[0].model_copy(update={"amount": 1}),),
    )
    with pytest.raises(ValueError, match="unmodeled native"):
        initial_position(
            SimpleNamespace(target=lambda name: position.target),
            SimpleNamespace(name="Main"),
            native,
        )


def test_liquidation_batch_retains_pending_work_and_refuses_duplicate_position(position):
    held = SimulatedPosition(
        identity="hypothetical-held",
        size_usd_e30=100,
        index_amount=position.escrow.intent.collateral,
        collateral=position.escrow.intent.collateral,
        pending_impact_amount=0,
        borrowing_factor=0,
        funding_fee_amount_per_size=0,
        long_claimable_funding_amount_per_size=0,
        short_claimable_funding_amount_per_size=0,
        increased_at=1,
        decreased_at=0,
    )
    before = position.model_copy(
        update={"owned": position.owned.model_copy(update={"position": held})}
    )
    result = EconomicLiquidationResult(
        allocation=position.target.allocation,
        effective_at=datetime.fromtimestamp(2, UTC),
        simulation=SimulationEvidence(execution_id="event-1", model="candidate", inputs=()),
        before=before,
        after=position,
        assets=(),
        reason="min collateral",
    )
    batch = EconomicLiquidationBatch(results=(result,))
    assert EconomicLiquidationBatch.model_validate_json(batch.model_dump_json()) == batch
    assert batch.results[0].after.escrow == before.escrow
    with pytest.raises(ValueError, match="repeat a position"):
        EconomicLiquidationBatch(results=(result, result))
