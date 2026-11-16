"""Aave-owned prepared products and deterministic, cadence-independent rate curves."""

from bisect import bisect_right
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.dataset import DataKey, InputRecord, Timeline
from almanak.backtest.pricing import CoinGeckoPrice
from almanak.backtest.types import ConnectorId, UnixTime
from almanak.caip import AssetType
from almanak.connectors.aave_v4.domain import AaveReserve, ReserveObservation
from almanak.connectors.aave_v4.history import VIRTUAL, ceiling
from almanak.models import Address, Block, Chain, Model, UInt

MODEL = "aave-v4-economic-rates-v1"
CONNECTOR = ConnectorId("aave_v4")
RAY = 10**27
WAD = 10**18
BPS = 10_000
YEAR = 365 * 24 * 60 * 60


class AccountingSource(Model):
    chain: Chain
    hub: Address
    asset: AssetType
    hub_asset_id: UInt

    @model_validator(mode="after")
    def scope(self):
        if self.asset.chain != self.chain.chain_id or self.asset.namespace != "erc20":
            raise ValueError("Hub accounting requires an ERC-20 asset on the declared chain")
        return self


class Rates(Model):
    source: AccountingSource
    convention: Literal["annual_fraction_ray_365_days"]
    supply_apr_ray: UInt
    borrow_apr_ray: UInt
    supply_index_ray: Annotated[UInt, Field(gt=0)]
    borrow_index_ray: Annotated[UInt, Field(gt=0)]
    added_assets: UInt
    added_shares: UInt
    liquidity_fee_bps: Annotated[UInt, Field(le=BPS)]


class DynamicConfig(Model):
    key: UInt
    collateral_factor_bps: Annotated[UInt, Field(le=BPS)]
    max_liquidation_bonus_bps: Annotated[UInt, Field(ge=BPS)]
    liquidation_fee_bps: Annotated[UInt, Field(le=BPS)]

    @model_validator(mode="after")
    def solvency(self):
        if ceiling(self.max_liquidation_bonus_bps * self.collateral_factor_bps, BPS) >= BPS:
            raise ValueError("liquidation penalty must remain below collateral value")
        return self


class LiquidationConfig(Model):
    target_health_factor: Annotated[UInt, Field(ge=WAD)]
    health_factor_for_max_bonus: Annotated[UInt, Field(lt=WAD)]
    bonus_factor_bps: Annotated[UInt, Field(le=BPS)]


class RiskParameters(Model):
    reserve: ReserveObservation
    configs: Annotated[tuple[DynamicConfig, ...], Field(min_length=1)]
    liquidation: LiquidationConfig

    @model_validator(mode="after")
    def versions(self):
        keys = tuple(c.key for c in self.configs)
        if keys != tuple(range(self.reserve.dynamic_config_key + 1)):
            raise ValueError("risk history requires every applicable dynamic configuration key")
        if self.reserve.asset.decimals > 18:
            raise ValueError("Aave V4 risk values require at most 18 asset decimals")
        return self

    def config(self, key: int) -> DynamicConfig:
        if not 0 <= key < len(self.configs):
            raise ValueError("position configuration version is missing from historical evidence")
        return self.configs[key]


class OraclePrice(Model):
    source: AaveReserve
    oracle: Address
    feed: Address
    price: Annotated[UInt, Field(gt=0)]
    decimals: Literal[8] = 8
    quote: Literal["USD"] = "USD"
    dependencies: tuple[Address, ...]


class VendorRiskPrice(Model):
    source: AaveReserve
    vendor: CoinGeckoPrice
    price: Annotated[UInt, Field(gt=0)]
    decimals: Literal[8] = 8
    quote: Literal["USD"] = "USD"


RateRecord = InputRecord[Rates, Block]
RiskRecord = InputRecord[RiskParameters, Block]
PriceRecord = InputRecord[OraclePrice, Block]


def reserve_key(source: AaveReserve, product: str) -> DataKey:
    return DataKey(
        connector=CONNECTOR,
        chain=source.chain,
        source=f"{source.spoke}/{source.reserve_id}",
        product=product,
    )


def rate_key(source: AccountingSource) -> DataKey:
    return DataKey(
        connector=CONNECTOR,
        chain=source.chain,
        source=f"{source.hub}/{source.hub_asset_id}",
        product="interest",
    )


def grow(index: int, rate: int, elapsed: int, *, up: bool) -> int:
    if index <= 0 or rate < 0 or elapsed < 0:
        raise ValueError("index growth requires positive indices and nonnegative rate/time")
    # Rate boundaries, not observations or user actions, determine compounding.
    factor = RAY + rate * elapsed // YEAR
    numerator = index * factor
    return ceiling(numerator, RAY) if up else numerator // RAY


class RateCurve:
    def __init__(self, records: Timeline[RateRecord]):
        self.validate_inputs(records)
        self.records = records
        self.times = records.times
        first = records.rows[0].payload
        seed_age = records.rows[0].available_at - records.rows[0].source_time
        self.supply = [grow(first.supply_index_ray, first.supply_apr_ray, seed_age, up=False)]
        self.borrow = [grow(first.borrow_index_ray, first.borrow_apr_ray, seed_age, up=True)]
        for i, record in enumerate(records.rows[:-1]):
            elapsed = self.times[i + 1] - self.times[i]
            self.supply.append(
                grow(self.supply[-1], record.payload.supply_apr_ray, elapsed, up=False)
            )
            self.borrow.append(
                grow(self.borrow[-1], record.payload.borrow_apr_ray, elapsed, up=True)
            )

    @staticmethod
    def validate_inputs(records: Timeline[RateRecord]):
        requirement = records.requirement
        if requirement.sample_seconds is None or requirement.unit != "annual_fraction_ray":
            raise ValueError("Aave interest requires sampled annual fraction rates in RAY")
        expected = tuple(range(requirement.start, requirement.end + 1, requirement.sample_seconds))
        if records.times != expected:
            raise ValueError("rate curve is missing a fixed sampling boundary")
        source = records.rows[0].payload.source
        for record in records.rows:
            if record.payload.source != source or record.key != rate_key(source):
                raise ValueError("rate curve mixes different Hub assets or sources")
            if (
                record.reference.chain != source.chain
                or record.reference.timestamp != record.source_time
            ):
                raise ValueError("rate input differs from its historical chain evidence")

    def at(self, time: int) -> tuple[int, int]:
        i = bisect_right(self.times, time) - 1
        if i < 0:
            raise ValueError("interest input is unavailable before its initial anchor")
        rates = self.records.at(time).payload
        elapsed = time - self.times[i]
        return (
            grow(self.supply[i], rates.supply_apr_ray, elapsed, up=False),
            grow(self.borrow[i], rates.borrow_apr_ray, elapsed, up=True),
        )


def supply_rate(drawn_index, drawn_shares, premium_shares, rate, fees, added_assets):
    # The derivative of Hub claims includes premium shares, not their accrued
    # offset or deficits. Virtual assets dilute share-price growth as in V4.
    numerator = (drawn_shares + premium_shares) * drawn_index * rate * (BPS - fees)
    return numerator // (RAY * BPS * (added_assets + VIRTUAL))


def require_range(source: AaveReserve, start: UnixTime, end: UnixTime):
    from almanak.connectors.support import aave_support, historical_capability

    support = next((s for s in aave_support() if s.chain == source.chain.chain_id), None)
    if support is None:
        raise ValueError(f"economic Aave is unsupported on {source.chain.chain_id}")
    historical_capability(support, source.chain, CONNECTOR, MODEL).require(start)
    if end < start:
        raise ValueError("economic history requires an increasing time interval")
