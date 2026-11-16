"""Synthetic GMX prepared inputs for offline type and wiring checks.

Every value here is invented. Nothing in this module is a captured chain
observation, a Reader comparison or evidence of historical coverage; the
numbers are chosen only to be internally consistent under the connector's
own validators so that serialization, union decoding and reader wiring can be
exercised without the git-ignored evidence tree.
"""

from pathlib import Path

from eth_abi import encode

from almanak.connectors.gmx_v2.domain import Target
from almanak.connectors.gmx_v2.economic_data import (
    MarketFrame,
    PerpetualOpportunity,
    PositionFeePolicies,
    SwapOpportunity,
    accounting_market,
)
from almanak.connectors.gmx_v2.economic_funding import (
    CollateralValues,
    FundingBounds,
    FundingIndexes,
    FundingParameters,
    FundingState,
)
from almanak.connectors.gmx_v2.economic_liquidity_risk import RequestControls
from almanak.connectors.gmx_v2.economic_perpetual_risk import (
    PerpetualRiskState,
    PerpetualSideRisk,
)
from almanak.connectors.gmx_v2.economic_pnl import PnlSide, PnlState
from almanak.connectors.gmx_v2.economic_position_fees import FeePolicy
from almanak.connectors.gmx_v2.economic_position_types import (
    ImpactSettlementState,
    PositionImpactParameters,
    PositionPricingState,
)
from almanak.connectors.gmx_v2.economic_precision import FLOAT
from almanak.connectors.gmx_v2.economic_solvency import MarketSolvency, TokenSolvency
from almanak.connectors.gmx_v2.economic_spot_types import (
    Currency,
    FeeParameters,
    ImpactParameters,
    Price,
    SpotState,
)
from almanak.connectors.gmx_v2.economic_swap_risk import SwapRiskState, SwapSideRisk
from almanak.connectors.gmx_v2.economic_valuation_types import (
    AccountingState,
    BorrowingSide,
    ImpactDistribution,
)
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.gmx_v2.oracle import ENVELOPE_TYPES, REPORT_TYPES
from almanak.connectors.gmx_v2.oracle_reader import StreamConfiguration
from almanak.connectors.gmx_v2.published_prices import PublishedPrices
from almanak.connectors.gmx_v2.published_source import CalldataSuffix, PublishedSource
from almanak.models import Account, Block
from scripts.legacy_configuration.author import read_author
from tests.legacy_references import harness_directory

ROOT = Path(__file__).parents[1]
FUND = "0x" + "77" * 20
SOURCE_ORACLE = "0x" + "5a" * 20
SOURCE_HANDLER = "0x" + "5b" * 20
PROVIDER = "0x" + "5c" * 20
VERIFIER = "0x" + "5d" * 20
CODE_HASH = "0x" + "5e" * 32
FEED = "0x" + "5f" * 32
BLOCK_HASH = "0x" + "ab" * 32
TIMESTAMP = 1788827746
NUMBER = 502851450
# Synthetic e30 USD prices per base unit: an 18-decimal index/long token near
# 2000 USD and a 6-decimal stable near 1 USD.
LONG_PRICE = (2_000 * 10**12, 2_001 * 10**12)
SHORT_PRICE = (10**24, 10**24 + 10**20)


def author(name, kind):
    return read_author(harness_directory(f"gmx_v2_{name}_{kind}") / "config.yaml")


def perpetual_target(name="arbitrum"):
    allocation = author(name, "position").allocations[0]
    market = allocation.market
    return Target(
        allocation=allocation.name,
        account=allocation.account,
        fund=Account(chain=market.chain, address=FUND),
        market=market,
        collateral_asset=allocation.capital.asset,
        side=allocation.side,
    )


def exchange_target(name="arbitrum"):
    allocation = author(name, "swap").allocations[0]
    market = allocation.market
    return ExchangeTarget(
        allocation=allocation.name,
        account=allocation.account,
        fund=Account(chain=market.chain, address=FUND),
        market=market,
        funding_asset=market.long_asset,
    )


def block(market, number=NUMBER, timestamp=TIMESTAMP):
    return Block(chain=market.chain, number=number, hash=BLOCK_HASH, timestamp=timestamp)


def prices(market):
    long = Price(asset=market.long_asset, minimum=LONG_PRICE[0], maximum=LONG_PRICE[1])
    short = Price(asset=market.short_asset, minimum=SHORT_PRICE[0], maximum=SHORT_PRICE[1])
    index = long.model_copy(update={"asset": market.index_asset})
    return index, long, short


def source():
    return PublishedSource(
        handler=SOURCE_HANDLER,
        oracle=SOURCE_ORACLE,
        request_kind="order",
        calldata_suffix=CalldataSuffix(prefix="0x", length=0),
    )


def signed_payload(price, observed_at):
    report = encode(
        REPORT_TYPES,
        (
            bytes.fromhex(FEED[2:]),
            observed_at,
            observed_at,
            0,
            0,
            observed_at + 60,
            price.midpoint,
            price.minimum,
            price.maximum,
        ),
    )
    envelope = ((bytes(32),) * 3, report, (b"\x01" * 32,), (b"\x02" * 32,), bytes(32))
    return "0x" + encode(ENVELOPE_TYPES, envelope).hex()


def binding(market, at, asset, oracle):
    return StreamConfiguration(
        block=at,
        asset=asset,
        data_store=market.contracts.data_store,
        oracle=oracle,
        provider=PROVIDER,
        provider_code_hash=CODE_HASH,
        verifier=VERIFIER,
        verifier_code_hash=CODE_HASH,
        feed_id=FEED,
        multiplier=FLOAT,
        spread_reduction=0,
        timestamp_adjustment=0,
    )


def publication(market, at, transaction="0x" + "cd" * 32):
    """A synthetic signed publication whose normalized bounds equal ``prices(market)``."""
    from almanak.connectors.gmx_v2.oracle import decode_report

    selected = source()
    assets = tuple(dict.fromkeys((market.index_asset, market.long_asset, market.short_asset)))
    by_asset = {price.asset: price for price in prices(market)}
    return PublishedPrices(
        block=at,
        source=selected,
        transaction=transaction,
        target_bindings=tuple(binding(market, at, a, market.contracts.oracle) for a in assets),
        source_bindings=tuple(binding(market, at, a, selected.oracle) for a in assets),
        reports=tuple(
            decode_report(
                asset,
                PROVIDER,
                bytes.fromhex(signed_payload(by_asset[asset], at.timestamp - 5)[2:]),
            )
            for asset in assets
        ),
    )


POOL = {"long": 1_000 * 10**18, "short": 2_000_000 * 10**6}
# Open interest split by collateral token: (long collateral, short collateral).
INTEREST = {"long": (300 * FLOAT, 200 * FLOAT), "short": (100 * FLOAT, 200 * FLOAT)}
INTEREST_TOKENS = {"long": 25 * 10**16, "short": 15 * 10**16}
IMPACT_POOL = 10**18


def spot(market, at):
    index, long, short = prices(market)
    return SpotState(
        pricing_type="SWAP",
        market=market,
        block=at,
        long=Currency(price=long, pool_amount=POOL["long"], impact_pool_amount=10**16),
        short=Currency(price=short, pool_amount=POOL["short"], impact_pool_amount=10**6),
        impact=ImpactParameters(
            positive_factor=10**20, negative_factor=2 * 10**20, exponent=2 * FLOAT
        ),
        fees=FeeParameters(
            positive_factor=5 * 10**26,
            negative_factor=7 * 10**26,
            receiver_factor=37 * 10**28,
            ui_factor=0,
        ),
        virtual_inventory=None,
    )


def borrowing_side(side, at):
    return BorrowingSide(
        open_interest_usd_e30=sum(INTEREST[side]),
        open_interest_tokens=INTEREST_TOKENS[side],
        cumulative_factor=10**28,
        updated_at=at.timestamp - 60,
        total_borrowing_usd_e30=0,
        optimal_usage_factor=0,
        base_factor=0,
        above_optimal_factor=0,
        factor=10**22,
        exponent=FLOAT,
        reserve_factor=9 * 10**29,
        maximum_pnl_factor=9 * 10**29,
    )


def accounting(market, at):
    state = spot(market, at)
    return AccountingState(
        spot=state,
        index_price=prices(market)[0],
        long=borrowing_side("long", at),
        short=borrowing_side("short", at),
        skip_borrowing_for_smaller_side=True,
        use_open_interest_tokens_for_balance=False,
        borrowing_fee_receiver_factor=37 * 10**28,
        impact=ImpactDistribution(
            pool_amount=IMPACT_POOL,
            minimum_amount=0,
            rate=0,
            distributed_at=at.timestamp - 100,
            lent_amount=0,
        ),
        share_supply=10**24,
    )


def impact_settlement(market, at):
    index, long, short = prices(market)
    pricing = PositionPricingState(
        market=market,
        block=at,
        index_price=index,
        impact=PositionImpactParameters(
            positive_factor=10**20,
            negative_factor=2 * 10**20,
            positive_exponent=2 * FLOAT,
            negative_exponent=2 * FLOAT,
            maximum_positive_factor=5 * 10**27,
        ),
        use_open_interest_in_tokens=False,
        long_open_interest=sum(INTEREST["long"]),
        short_open_interest=sum(INTEREST["short"]),
        virtual_inventory=None,
    )
    return ImpactSettlementState(
        pricing=pricing,
        maximum_negative_factor=5 * 10**27,
        impact_pool_amount=IMPACT_POOL,
        pool_usd_e30=POOL["long"] * long.minimum + POOL["short"] * short.minimum,
        maximum_lendable_factor=10**28,
        maximum_lendable_usd_e30=10**36,
        lent_impact_amount=0,
    )


def funding(market, at):
    _, long, short = prices(market)
    return FundingState(
        market=market,
        block=at,
        long_price=long,
        short_price=short,
        parameters=FundingParameters(
            factor=10**24,
            exponent=FLOAT,
            increase_per_second=0,
            minimum_increase_per_second=0,
            decrease_per_second=10,
            longs_pay=FundingBounds(minimum_per_second=0, maximum_per_second=10**25),
            shorts_pay=FundingBounds(minimum_per_second=0, maximum_per_second=10**25),
            stable_threshold=FLOAT // 4,
            decrease_threshold=FLOAT // 10,
        ),
        long_interest=CollateralValues(long=INTEREST["long"][0], short=INTEREST["long"][1]),
        short_interest=CollateralValues(long=INTEREST["short"][0], short=INTEREST["short"][1]),
        long_balance_usd_e30=sum(INTEREST["long"]),
        short_balance_usd_e30=sum(INTEREST["short"]),
        updated_at=at.timestamp - 30,
        saved_per_second=0,
    )


def funding_indexes():
    values = {}
    for index, field in enumerate(FundingIndexes.model_fields):
        values[field] = CollateralValues(long=(index + 1) * 10**15, short=(index + 1) * 10**3)
    return FundingIndexes(**values)


def pnl(market, at):
    index, long, short = prices(market)
    sides = {}
    for side, price in (("long", long), ("short", short)):
        sides[side] = PnlSide(
            open_interest_usd_e30=sum(INTEREST[side]),
            open_interest_tokens=INTEREST_TOKENS[side],
            pool_amount=POOL[side],
            pool_price=price,
            maximum_trader_pnl_factor=9 * 10**29,
        )
    return PnlState(market=market, block=at, index_price=index, **sides)


def fee_policy(position_factor):
    return FeePolicy(
        position_factor=position_factor,
        position_receiver_factor=37 * 10**28,
        borrowing_receiver_factor=37 * 10**28,
        liquidation_factor=2 * 10**27,
        liquidation_receiver_factor=37 * 10**28,
        ui_factor=0,
        referral_enabled=False,
        affiliate_factor=0,
        referral_discount_factor=0,
        minimum_affiliate_factor=0,
        pro_discount_factor=0,
    )


def fees():
    return PositionFeePolicies(improved=fee_policy(5 * 10**26), worsened=fee_policy(7 * 10**26))


def perpetual_risk(market, at):
    controls = RequestControls(
        creation_disabled=False, execution_disabled=False, cancellation_disabled=False
    )
    side = PerpetualSideRisk(
        maximum_open_interest_usd_e30=10**36,
        reserve_factor=95 * 10**28,
        open_interest_reserve_factor=9 * 10**29,
        collateral_open_interest_multiplier=10**20,
    )
    return PerpetualRiskState(
        market=market,
        block=at,
        increases=controls,
        decreases=controls,
        market_disabled=False,
        funding_claim_disabled=False,
        liquidation_disabled=False,
        holding_address_available=True,
        request_expiration_seconds=300,
        maximum_oracle_age_seconds=300,
        minimum_position_usd_e30=FLOAT,
        minimum_collateral_usd_e30=FLOAT,
        minimum_collateral_factor=10**28,
        liquidation_collateral_factor=10**28,
        maximum_liquidation_impact_factor=10**28,
        long=side,
        short=side,
    )


def solvency(market, at):
    state = spot(market, at)
    return MarketSolvency(
        market=market,
        block=at,
        **{
            name: TokenSolvency(
                asset=currency.price.asset,
                balance=10 * currency.pool_amount,
                minimum_balance=currency.pool_amount + currency.impact_pool_amount,
                collateral_amount=0,
                claimable_funding_amount=0,
            )
            for name, currency in (("long", state.long), ("short", state.short))
        },
    )


def perpetual_opportunity(market, at, transaction_index=0):
    return PerpetualOpportunity(
        publication=publication(market, at),
        transaction_index=transaction_index,
        accounting=accounting(market, at),
        impact=impact_settlement(market, at),
        funding=funding(market, at),
        funding_indexes=funding_indexes(),
        pnl=pnl(market, at),
        fees=fees(),
        risk=perpetual_risk(market, at),
        solvency=solvency(market, at),
    )


def perpetual_frame(name="arbitrum"):
    target = perpetual_target(name)
    at = block(target.market)
    risk = perpetual_risk(target.market, at)
    return MarketFrame(
        target=target, risk=(risk,), opportunities=(perpetual_opportunity(target.market, at),)
    )


def swap_risk(market, at):
    side = SwapSideRisk(
        maximum_pool_amount=10**40,
        reserve_factor=9 * 10**29,
        maximum_deposit_pnl_factor=9 * 10**29,
        maximum_withdrawal_pnl_factor=9 * 10**29,
        open_interest_usd_e30=sum(INTEREST["long"]),
        open_interest_tokens=INTEREST_TOKENS["long"],
    )
    return SwapRiskState(
        market=market,
        block=at,
        long=side,
        short=side,
        market_disabled=False,
        creation_disabled=False,
        execution_disabled=False,
        cancellation_disabled=False,
        request_expiration_seconds=300,
        maximum_oracle_age_seconds=300,
    )


def swap_frame(name="arbitrum"):
    target = exchange_target(name)
    market = accounting_market(target.market)
    at = block(market)
    risk = swap_risk(market, at)
    opportunity = SwapOpportunity(
        publication=publication(market, at),
        transaction_index=0,
        spot=spot(market, at),
        risk=risk,
        solvency=solvency(market, at),
    )
    return MarketFrame(target=target, risk=(risk,), opportunities=(opportunity,))
