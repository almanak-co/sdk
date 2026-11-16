"""Publication frame identity and same-second ordering survive dataset serialization."""

import json
from pathlib import Path

import pytest

from almanak.connectors.gmx_v2.economic_data import (
    LiquidityOpportunity,
    MarketFrame,
    SwapOpportunity,
    accounting_market,
)
from almanak.connectors.gmx_v2.economic_solvency import MarketSolvency, TokenSolvency
from almanak.connectors.gmx_v2.economic_spot_types import SpotState
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.gmx_v2.published_prices import PublishedPrices
from almanak.models import Account
from scripts.legacy_configuration.author import read_author
from tests.legacy_references import current_gmx_roles, harness_directory
from tests.test_gmx_economic_liquidity_risk import permissive_risk
from tests.test_gmx_economic_swap_risk import risk
from tests.test_gmx_economic_valuation import valuation as valuation

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-09-gmx-prepared"


@pytest.fixture
def liquidity_opportunity(frame, valuation):
    # Synthetic accounting isolates canonical scope from execution economics.
    swap = frame.opportunities[0]
    prices = {price.asset: price for price in swap.publication.prices}
    deposit, withdrawal = (
        valuation.model_copy(
            update={
                "spot": swap.spot.model_copy(update={"pricing_type": action}),
                "index_price": prices[swap.spot.market.index_asset],
            }
        )
        for action in ("DEPOSIT", "WITHDRAWAL")
    )
    return LiquidityOpportunity(
        publication=swap.publication,
        transaction_index=0,
        deposit=deposit,
        withdrawal=withdrawal,
        risk=permissive_risk(deposit),
        solvency=swap.solvency,
    )


def test_liquidity_opportunity_retains_one_canonical_publication(liquidity_opportunity):
    assert (
        LiquidityOpportunity.model_validate_json(liquidity_opportunity.model_dump_json())
        == liquidity_opportunity
    )


@pytest.mark.parametrize("field", ["deposit", "withdrawal", "risk", "solvency"])
def test_liquidity_opportunity_refuses_mixed_blocks(liquidity_opportunity, field):
    wire = json.loads(liquidity_opportunity.model_dump_json())
    scopes = {
        "deposit": wire["deposit"]["spot"],
        "withdrawal": wire["withdrawal"]["spot"],
        "risk": wire["risk"],
        "solvency": wire["solvency"],
    }
    scopes[field]["block"]["number"] += 1
    with pytest.raises(ValueError, match="exact canonical"):
        LiquidityOpportunity.model_validate_json(json.dumps(wire))


def test_liquidity_opportunity_refuses_substituted_prices(liquidity_opportunity):
    wire = json.loads(liquidity_opportunity.model_dump_json())
    price = wire["withdrawal"]["spot"]["long"]["price"]
    price["minimum"] += 1
    price["maximum"] += 1
    with pytest.raises(ValueError, match="verified publication"):
        LiquidityOpportunity.model_validate_json(json.dumps(wire))


@pytest.fixture(params=["arbitrum", "avalanche"])
def frame(request):
    name = request.param
    author = read_author(harness_directory(f"gmx_v2_{name}_swap") / "config.yaml")
    allocation = author.allocations[0]
    market = allocation.market
    target = ExchangeTarget(
        allocation=allocation.name,
        account=allocation.account,
        fund=Account(chain=market.chain, address="0x" + "77" * 20),
        market=market,
        funding_asset=market.long_asset,
    )
    # The spot state was read before GMX's 1 October rotation and names the retired role
    # contracts, while the frozen harness names their successors. These checks exercise
    # frame and opportunity scope, not deployment identity, so the captured market is read
    # as its successor.
    proof = current_gmx_roles(
        json.loads((EVIDENCE / f"{name}-spot-reader-comparison.json").read_text())
    )
    spot = SpotState.model_validate_json(json.dumps(proof["state"]))
    publication = PublishedPrices.model_validate_json(
        (EVIDENCE / f"{name}-published-prices.json").read_text()
    )
    limit = risk(spot)
    solvency = MarketSolvency(
        market=spot.market,
        block=spot.block,
        **{
            name: TokenSolvency(
                asset=currency.price.asset,
                balance=10**60,
                minimum_balance=currency.pool_amount + currency.impact_pool_amount,
                collateral_amount=0,
                claimable_funding_amount=0,
            )
            for name, currency in (("long", spot.long), ("short", spot.short))
        },
    )
    opportunity = SwapOpportunity(
        publication=publication, transaction_index=0, spot=spot, risk=limit, solvency=solvency
    )
    return MarketFrame(target=target, risk=(limit,), opportunities=(opportunity,))


def test_exact_accounting_market_and_publication_round_trip(frame):
    assert accounting_market(frame.target.market) == frame.risk[0].market
    assert MarketFrame.model_validate_json(frame.model_dump_json()) == frame
    assert frame.block == frame.opportunities[0].publication.block


@pytest.mark.parametrize("field", ["block", "pricing_type"])
def test_opportunity_refuses_changed_pricing_scope(frame, field):
    opportunity = frame.opportunities[0]
    change = (
        {
            "block": opportunity.spot.block.model_copy(
                update={"number": opportunity.spot.block.number + 1}
            )
        }
        if field == "block"
        else {"pricing_type": "DEPOSIT"}
    )
    with pytest.raises(ValueError, match="exact canonical"):
        SwapOpportunity.model_validate(
            opportunity.model_dump() | {"spot": opportunity.spot.model_copy(update=change)}
        )


def test_opportunity_refuses_prices_that_differ_from_signed_source(frame):
    opportunity = frame.opportunities[0]
    currency = opportunity.spot.long
    changed = currency.model_copy(
        update={
            "price": currency.price.model_copy(
                update={
                    "minimum": currency.price.minimum + 1,
                    "maximum": currency.price.maximum + 1,
                }
            )
        }
    )
    with pytest.raises(ValueError, match="verified publication"):
        SwapOpportunity.model_validate(
            opportunity.model_dump()
            | {"spot": opportunity.spot.model_copy(update={"long": changed})}
        )


@pytest.mark.parametrize("number_delta,time_delta", [(2, 0), (1, 1), (0, 0)])
def test_frame_refuses_missing_repeated_or_differently_timed_blocks(
    frame, number_delta, time_delta
):
    first = frame.risk[0]
    following = first.model_copy(
        update={
            "block": first.block.model_copy(
                update={
                    "number": first.block.number + number_delta,
                    "timestamp": first.block.timestamp + time_delta,
                }
            )
        }
    )
    with pytest.raises(ValueError, match="consecutive canonical"):
        MarketFrame.model_validate(frame.model_dump() | {"risk": (first, following)})


def test_frame_preserves_consecutive_blocks_in_one_second(frame):
    first = frame.risk[0]
    following = first.model_copy(
        update={"block": first.block.model_copy(update={"number": first.block.number + 1})}
    )
    result = MarketFrame.model_validate(frame.model_dump() | {"risk": (first, following)})
    assert result.block == following.block
    assert MarketFrame.model_validate_json(result.model_dump_json()) == result


def test_frame_refuses_repeated_and_out_of_frame_publications(frame):
    opportunity = frame.opportunities[0]
    with pytest.raises(ValueError, match="unique canonical"):
        MarketFrame.model_validate(
            frame.model_dump() | {"opportunities": (opportunity, opportunity)}
        )
    changed = frame.risk[0].model_copy(update={"creation_disabled": True})
    with pytest.raises(ValueError, match="outside its retained risk"):
        MarketFrame.model_validate(frame.model_dump() | {"risk": (changed,)})


def test_frame_refuses_other_market_accounting(frame):
    changed_market = frame.risk[0].market.model_copy(
        update={"share_asset": frame.target.market.wrapped_native}
    )
    changed_risk = frame.risk[0].model_copy(update={"market": changed_market})
    with pytest.raises(ValueError, match="configured target"):
        MarketFrame.model_validate(frame.model_dump() | {"risk": (changed_risk,)})
