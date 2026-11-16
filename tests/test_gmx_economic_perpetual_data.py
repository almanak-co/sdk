"""Perpetual frames keep exact settlement inputs through serialization and union decoding.

These checks use the synthetic inputs in ``tests/gmx_synthetic_frames.py``.
They establish type behavior only, never historical coverage or a Reader comparison.
"""

from types import SimpleNamespace

import pytest

from almanak.backtest.types import UnixTime
from almanak.connectors.gmx_v2 import economic_data as data
from almanak.connectors.gmx_v2.domain import Target
from almanak.connectors.gmx_v2.economic_binding import Manifest
from almanak.connectors.gmx_v2.economic_data import (
    MarketFrame,
    PerpetualOpportunity,
    PositionFeePolicies,
)
from almanak.connectors.gmx_v2.economic_perpetual_risk import PerpetualRiskState
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.gmx_v2.models import Parameters
from almanak.models import Account, AccountBinding, SimulationAccount, StrategyAccounts
from tests import gmx_synthetic_frames as synthetic


@pytest.fixture(params=["arbitrum", "avalanche"])
def frame(request):
    return synthetic.perpetual_frame(request.param)


def parameters(name, kind):
    author = synthetic.author(name, kind)
    allocation = author.allocations[0]
    return Parameters(
        decision_trigger=author.decision_trigger,
        asset_metadata=author.asset_metadata,
        allocations=author.allocations,
        accounts=StrategyAccounts(
            bindings=(
                AccountBinding(
                    name=allocation.account,
                    profile=SimulationAccount(
                        fund=Account(chain=allocation.market.chain, address=synthetic.FUND)
                    ),
                ),
            )
        ),
    )


def test_perpetual_frame_round_trips_and_decodes_its_own_union_members(frame):
    restored = MarketFrame.model_validate_json(frame.model_dump_json())
    assert restored == frame
    assert isinstance(restored.target, Target)
    assert isinstance(restored.risk[0], PerpetualRiskState)
    assert isinstance(restored.opportunities[0], PerpetualOpportunity)
    opportunity = restored.opportunities[0]
    assert opportunity.block == frame.block == opportunity.pricing.block
    assert opportunity.spot is opportunity.accounting.spot
    assert opportunity.ordering == (frame.block.number, 0)
    assert opportunity.fees.select(True) is opportunity.fees.improved
    assert opportunity.fees.select(False) is opportunity.fees.worsened


def test_perpetual_record_round_trips_with_its_dataset_key(frame):
    target = frame.target
    record = data.Record(
        key=data.key(target),
        unit=data.UNIT,
        payload=frame,
        reference=frame.block,
        source_time=UnixTime(frame.block.timestamp),
        available_at=UnixTime(frame.block.timestamp),
        retrieved_at=UnixTime(frame.block.timestamp + 1),
        evidence=("0" * 64,),
    )
    restored = data.Record.model_validate_json(record.model_dump_json())
    assert restored == record
    assert isinstance(restored.payload.opportunities[0], PerpetualOpportunity)
    assert restored.key.source == target.allocation


def test_swap_frame_still_decodes_to_exchange_types():
    frame = synthetic.swap_frame()
    restored = MarketFrame.model_validate_json(frame.model_dump_json())
    assert restored == frame
    assert isinstance(restored.target, ExchangeTarget)
    assert type(restored.risk[0]).__name__ == "SwapRiskState"
    assert type(restored.opportunities[0]).__name__ == "SwapOpportunity"


def test_frame_refuses_a_perpetual_risk_family_under_an_exchange_target(frame):
    exchange = synthetic.exchange_target()
    with pytest.raises(ValueError, match="target kind"):
        MarketFrame.model_validate(frame.model_dump() | {"target": exchange})
    swap = synthetic.swap_frame()
    with pytest.raises(ValueError, match="target kind"):
        MarketFrame.model_validate(swap.model_dump() | {"target": frame.target})


def test_frame_refuses_a_swap_opportunity_inside_a_perpetual_risk_frame(frame):
    swap = synthetic.swap_frame().opportunities[0]
    with pytest.raises(ValueError, match="outside its retained risk"):
        MarketFrame.model_validate(frame.model_dump() | {"opportunities": (swap,)})


def test_opportunity_refuses_a_state_from_another_block(frame):
    opportunity = frame.opportunities[0]
    moved = opportunity.funding.model_copy(
        update={"block": opportunity.block.model_copy(update={"number": frame.block.number + 1})}
    )
    with pytest.raises(ValueError, match="exact canonical market state"):
        PerpetualOpportunity.model_validate(opportunity.model_dump() | {"funding": moved})


def test_opportunity_refuses_a_non_swap_accounting_action(frame):
    opportunity = frame.opportunities[0]
    accounting = opportunity.accounting
    changed = accounting.model_copy(
        update={"spot": accounting.spot.model_copy(update={"pricing_type": "DEPOSIT"})}
    )
    with pytest.raises(ValueError, match="exact canonical market state"):
        PerpetualOpportunity.model_validate(opportunity.model_dump() | {"accounting": changed})


def test_opportunity_refuses_prices_that_differ_from_the_signed_publication(frame):
    opportunity = frame.opportunities[0]
    price = opportunity.pnl.index_price
    changed = opportunity.pnl.model_copy(
        update={"index_price": price.model_copy(update={"maximum": price.maximum + 1})}
    )
    with pytest.raises(ValueError, match="verified publication"):
        PerpetualOpportunity.model_validate(opportunity.model_dump() | {"pnl": changed})


@pytest.mark.parametrize("field", ["pnl", "impact", "funding", "accounting"])
def test_opportunity_refuses_readers_that_disagree_on_shared_datastore_values(frame, field):
    opportunity = frame.opportunities[0]
    if field == "pnl":
        side = opportunity.pnl.long.model_copy(
            update={"open_interest_tokens": opportunity.pnl.long.open_interest_tokens + 1}
        )
        changed = opportunity.pnl.model_copy(update={"long": side})
    elif field == "impact":
        changed = opportunity.impact.model_copy(
            update={"impact_pool_amount": opportunity.impact.impact_pool_amount + 1}
        )
    elif field == "funding":
        interest = opportunity.funding.long_interest.model_copy(
            update={"short": opportunity.funding.long_interest.short + 1}
        )
        changed = opportunity.funding.model_copy(update={"long_interest": interest})
    else:
        changed = opportunity.accounting.model_copy(
            update={"use_open_interest_tokens_for_balance": True}
        )
    with pytest.raises(ValueError, match="disagree with each other"):
        PerpetualOpportunity.model_validate(opportunity.model_dump() | {field: changed})


def test_fee_policies_may_differ_only_in_the_balance_dependent_factor():
    improved = synthetic.fee_policy(1)
    PositionFeePolicies(improved=improved, worsened=synthetic.fee_policy(2))
    with pytest.raises(ValueError, match="balance-dependent factor"):
        PositionFeePolicies(
            improved=improved,
            worsened=synthetic.fee_policy(2).model_copy(update={"pro_discount_factor": 1}),
        )


@pytest.mark.parametrize(
    "kind,expected",
    [
        ("swap", (data.MODEL, data.REVISION)),
        ("liquidity", (data.LIQUIDITY_MODEL, data.LIQUIDITY_REVISION)),
        ("position", (data.PERPETUAL_MODEL, data.PERPETUAL_REVISION)),
    ],
)
def test_revisions_select_the_perpetual_model_only_for_homogeneous_positions(kind, expected):
    assert data.revisions(parameters("arbitrum", kind)) == expected
    assert expected[0] in Manifest.model_fields["model"].annotation.__args__
    assert expected[0].startswith("gmx-v2-published-opportunity-integer-")


@pytest.mark.parametrize(
    "position_model,liquidity_model,expected",
    [
        (None, None, data.REVISION),
        (None, "fixed_external_gm_valuation", data.LIQUIDITY_REVISION),
        ("prepared_position_accounting", None, data.PERPETUAL_REVISION),
    ],
)
def test_requirement_selects_the_payload_revision_from_the_model_selection(
    frame, position_model, liquidity_model, expected
):
    settings = SimpleNamespace(
        position_model=position_model,
        liquidity_model=liquidity_model,
        schedule=SimpleNamespace(start=UnixTime(1), deadline=UnixTime(2)),
        data=SimpleNamespace(max_age_seconds=60),
    )
    requirement = data.requirement(frame.target, settings)
    assert requirement.payload_revision == expected
    assert requirement.key == data.key(frame.target)
    assert len({data.REVISION, data.LIQUIDITY_REVISION, data.PERPETUAL_REVISION}) == 3
