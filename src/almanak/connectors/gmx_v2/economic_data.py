"""Prepared GMX opportunities retain publication ordering and every bounded risk block."""

from itertools import pairwise
from typing import Annotated

from pydantic import Field, TypeAdapter, model_validator

from almanak.backtest.dataset import DataKey, DataRequirement, InputRecord
from almanak.connectors.gmx_v2.domain import Market, Target
from almanak.connectors.gmx_v2.economic_funding import FundingIndexes, FundingState
from almanak.connectors.gmx_v2.economic_liquidity_risk import LiquidityRiskState
from almanak.connectors.gmx_v2.economic_perpetual_risk import PerpetualRiskState
from almanak.connectors.gmx_v2.economic_pnl import PnlState
from almanak.connectors.gmx_v2.economic_position_fees import FeePolicy
from almanak.connectors.gmx_v2.economic_position_types import ImpactSettlementState
from almanak.connectors.gmx_v2.economic_solvency import MarketSolvency
from almanak.connectors.gmx_v2.economic_spot_types import SpotState
from almanak.connectors.gmx_v2.economic_swap_risk import SwapRiskState
from almanak.connectors.gmx_v2.economic_valuation_types import AccountingState, ValuationState
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.gmx_v2.published_prices import PublishedPrices
from almanak.connectors.identifiers import ConnectorId
from almanak.models import Block, Model, UInt

CONNECTOR = ConnectorId("gmx_v2")
MODEL = "gmx-v2-published-opportunity-integer-swap-v1"
REVISION = "gmx-v2-bounded-risk-publications-v1"
LIQUIDITY_MODEL = "gmx-v2-published-opportunity-integer-gm-v1"
LIQUIDITY_REVISION = "gmx-v2-gm-bounded-risk-publications-v1"
PERPETUAL_MODEL = "gmx-v2-published-opportunity-integer-perpetual-v1"
PERPETUAL_REVISION = "gmx-v2-perpetual-bounded-risk-publications-v1"
UNIT = "gmx_v2_market_native_integer_units"


def accounting_market(market):
    # Exchange routing adds handlers; pool accounting retains the shared market contract.
    return Market.model_validate_json(TypeAdapter(Market).dump_json(market))


class SwapOpportunity(Model):
    publication: PublishedPrices
    transaction_index: UInt
    spot: SpotState
    risk: SwapRiskState
    solvency: MarketSolvency

    @property
    def ordering(self):
        return self.publication.block.number, self.transaction_index

    @model_validator(mode="after")
    def scope(self):
        if (
            self.spot.block != self.publication.block
            or self.risk.block != self.spot.block
            or self.risk.market != self.spot.market
            or self.spot.pricing_type != "SWAP"
            or self.solvency.market != self.spot.market
            or self.solvency.block != self.spot.block
        ):
            raise ValueError("GMX opportunity requires one exact canonical swap and risk state")
        prices = {price.asset: price for price in self.publication.prices}
        if (
            prices.get(self.spot.long.price.asset) != self.spot.long.price
            or prices.get(self.spot.short.price.asset) != self.spot.short.price
            or self.spot.market.index_asset not in prices
        ):
            raise ValueError("GMX opportunity prices differ from its verified publication")
        return self


class LiquidityOpportunity(Model):
    publication: PublishedPrices
    transaction_index: UInt
    deposit: ValuationState
    withdrawal: ValuationState
    risk: LiquidityRiskState
    solvency: MarketSolvency

    @property
    def ordering(self):
        return self.publication.block.number, self.transaction_index

    @model_validator(mode="after")
    def scope(self):
        prices = {price.asset: price for price in self.publication.prices}
        for state, action in ((self.deposit, "DEPOSIT"), (self.withdrawal, "WITHDRAWAL")):
            validate_liquidity_scope(self, state, action)
            validate_liquidity_prices(state, prices)
        return self


def validate_liquidity_scope(opportunity, state, action):
    scope = state.spot.market, state.spot.block
    if (
        scope != (opportunity.risk.market, opportunity.risk.block)
        or scope != (opportunity.solvency.market, opportunity.solvency.block)
        or (state.spot.block, state.spot.pricing_type) != (opportunity.publication.block, action)
    ):
        raise ValueError("GM opportunity requires exact canonical valuation and risk")


def validate_liquidity_prices(state, prices):
    if any(
        prices.get(price.asset) != price
        for price in (state.index_price, state.spot.long.price, state.spot.short.price)
    ):
        raise ValueError("GM opportunity prices differ from its verified publication")


class PositionFeePolicies(Model):
    """Both balance-dependent fee factors; settlement selects one after pricing impact."""

    improved: FeePolicy
    worsened: FeePolicy

    def select(self, balance_improved):
        return self.improved if balance_improved else self.worsened

    @model_validator(mode="after")
    def scope(self):
        excluded = {"position_factor"}
        if self.improved.model_dump(exclude=excluded) != self.worsened.model_dump(exclude=excluded):
            raise ValueError(
                "GMX position fee policies differ beyond their balance-dependent factor"
            )
        return self


class PerpetualOpportunity(Model):
    """Every canonical input a perpetual settlement needs at one verified publication.

    ``accounting`` retains swap-priced pool state, borrowing accumulators and rates,
    the traders' PnL caps and impact-pool distribution; ``impact`` nests the position
    pricing inputs with the impact settlement bounds; ``funding`` and
    ``funding_indexes`` retain the next-funding tuple and accumulated indexes;
    ``fees`` retains referral and pro discount precedence for both impact outcomes.
    All amounts are exact native integer units; USD values carry the e30 suffix.
    """

    publication: PublishedPrices
    transaction_index: UInt
    accounting: AccountingState
    impact: ImpactSettlementState
    funding: FundingState
    funding_indexes: FundingIndexes
    pnl: PnlState
    fees: PositionFeePolicies
    risk: PerpetualRiskState
    solvency: MarketSolvency

    @property
    def ordering(self):
        return self.publication.block.number, self.transaction_index

    @property
    def block(self):
        return self.publication.block

    @property
    def spot(self):
        return self.accounting.spot

    @property
    def pricing(self):
        return self.impact.pricing

    @model_validator(mode="after")
    def scope(self):
        validate_perpetual_scope(self)
        validate_perpetual_prices(self)
        validate_perpetual_consistency(self)
        return self


def perpetual_states(opportunity):
    return (
        opportunity.accounting.spot,
        opportunity.impact.pricing,
        opportunity.funding,
        opportunity.pnl,
        opportunity.risk,
        opportunity.solvency,
    )


def validate_perpetual_scope(opportunity):
    block, market = opportunity.publication.block, opportunity.risk.market
    if (
        any(
            (state.market, state.block) != (market, block)
            for state in perpetual_states(opportunity)
        )
        or opportunity.accounting.spot.pricing_type != "SWAP"
    ):
        raise ValueError("GMX perpetual opportunity requires one exact canonical market state")


def validate_perpetual_prices(opportunity):
    prices = {price.asset: price for price in opportunity.publication.prices}
    spot, pnl, funding = opportunity.accounting.spot, opportunity.pnl, opportunity.funding
    retained = (
        spot.long.price,
        spot.short.price,
        opportunity.accounting.index_price,
        opportunity.impact.pricing.index_price,
        funding.long_price,
        funding.short_price,
        pnl.index_price,
        pnl.long.pool_price,
        pnl.short.pool_price,
    )
    if any(prices.get(price.asset) != price for price in retained):
        raise ValueError("GMX perpetual opportunity prices differ from its verified publication")


def validate_perpetual_consistency(opportunity):
    # Independent readers derive these from the same DataStore keys; disagreement
    # means a reader changed or the retained responses were not one canonical block.
    accounting, pricing, impact = (
        opportunity.accounting,
        opportunity.impact.pricing,
        opportunity.impact,
    )
    in_tokens = accounting.use_open_interest_tokens_for_balance
    sides = ((accounting.long, accounting.spot.long), (accounting.short, accounting.spot.short))
    accounting_sides = tuple(
        (s.open_interest_usd_e30, s.open_interest_tokens, c.pool_amount, s.maximum_pnl_factor)
        for s, c in sides
    )
    pnl_sides = tuple(
        (
            s.open_interest_usd_e30,
            s.open_interest_tokens,
            s.pool_amount,
            s.maximum_trader_pnl_factor,
        )
        for s in (opportunity.pnl.long, opportunity.pnl.short)
    )
    pricing_interest = (pricing.long_open_interest, pricing.short_open_interest)
    expected_interest = tuple(
        s.open_interest_tokens if in_tokens else s.open_interest_usd_e30 for s, _ in sides
    )
    funding_interest = (
        opportunity.funding.long_interest.total,
        opportunity.funding.short_interest.total,
    )
    pool_usd = sum(c.pool_amount * c.price.minimum for _, c in sides)
    if (
        pricing.use_open_interest_in_tokens != in_tokens
        or pricing_interest != expected_interest
        or pnl_sides != accounting_sides
        or funding_interest
        != (accounting.long.open_interest_usd_e30, accounting.short.open_interest_usd_e30)
        or (impact.impact_pool_amount, impact.lent_impact_amount, impact.pool_usd_e30)
        != (accounting.impact.pool_amount, accounting.impact.lent_amount, pool_usd)
    ):
        raise ValueError("GMX perpetual opportunity accounting inputs disagree with each other")


RiskState = SwapRiskState | LiquidityRiskState | PerpetualRiskState
Opportunity = SwapOpportunity | LiquidityOpportunity | PerpetualOpportunity


def validate_frame_family(target, risk):
    if len({type(value) for value in risk}) != 1:
        raise ValueError("GMX frame cannot mix different request-family risk controls")
    if isinstance(risk[0], PerpetualRiskState) != isinstance(target, Target):
        raise ValueError("GMX frame risk family differs from its configured target kind")


class MarketFrame(Model):
    target: ExchangeTarget | Target
    # All same-second blocks remain explicit even though decisions have second precision.
    risk: Annotated[tuple[RiskState, ...], Field(min_length=1)]
    opportunities: tuple[Opportunity, ...]

    @property
    def block(self):
        return self.risk[-1].block

    @model_validator(mode="after")
    def scope(self):
        blocks = tuple(value.block for value in self.risk)
        validate_frame_family(self.target, self.risk)
        if any(value.market != accounting_market(self.target.market) for value in self.risk):
            raise ValueError("GMX frame risk differs from its configured target")
        if any(
            following.number != previous.number + 1 or following.timestamp != previous.timestamp
            for previous, following in pairwise(blocks)
        ):
            raise ValueError("GMX frame requires consecutive canonical blocks in one second")
        risks = {value.block: value for value in self.risk}
        for opportunity in self.opportunities:
            if risks.get(opportunity.publication.block) != opportunity.risk:
                raise ValueError("GMX publication is outside its retained risk frame")
        order = tuple(value.ordering for value in self.opportunities)
        transactions = tuple(value.publication.transaction for value in self.opportunities)
        if any(a >= b for a, b in pairwise(order)) or len(set(transactions)) != len(transactions):
            raise ValueError("GMX publications require unique canonical transaction ordering")
        return self


Record = InputRecord[MarketFrame, Block]


def key(target):
    return DataKey(
        connector=CONNECTOR,
        chain=target.fund.chain,
        source=target.allocation,
        product="risk_and_published_opportunities",
    )


def requirement(target, settings):
    return DataRequirement(
        key=key(target),
        start=settings.schedule.start,
        end=settings.schedule.deadline,
        semantics="changes",
        max_age_seconds=settings.data.max_age_seconds,
        unit=UNIT,
        visibility="published",
        payload_revision=payload_revision(settings),
    )


def payload_revision(settings):
    if settings.position_model:
        return PERPETUAL_REVISION
    return LIQUIDITY_REVISION if settings.liquidity_model else REVISION


def risk_family(settings):
    if settings.position_model:
        return PerpetualRiskState
    return LiquidityRiskState if settings.liquidity_model else SwapRiskState


def frame_market(target, family):
    """GM frames keep their exchange handlers; other families read the accounting market."""
    if family is LiquidityRiskState:
        return target.market
    return accounting_market(target.market)


def revisions(config):
    from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation
    from almanak.connectors.gmx_v2.models import Allocation

    if all(isinstance(value, LiquidityAllocation) for value in config.allocations):
        return LIQUIDITY_MODEL, LIQUIDITY_REVISION
    if all(isinstance(value, Allocation) for value in config.allocations):
        return PERPETUAL_MODEL, PERPETUAL_REVISION
    return MODEL, REVISION
