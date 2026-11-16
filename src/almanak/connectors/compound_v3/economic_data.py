"""Prepared whole-Comet observations and cadence-independent integer index curves."""

from bisect import bisect_right
from dataclasses import dataclass
from typing import Annotated

from pydantic import Field, model_validator

from almanak.backtest.dataset import DataKey, DataRequirement, InputRecord, Timeline
from almanak.connectors.compound_v3.accounting import indices, rates
from almanak.connectors.compound_v3.domain import Market, MarketObservation
from almanak.connectors.compound_v3.math import accrue_index, present_value, uint
from almanak.connectors.identifiers import ConnectorId
from almanak.models import Block, Model

CONNECTOR = ConnectorId("compound_v3")
MODEL = "compound-v3-exogenous-integer-indices-v1"
REVISION = "compound-v3-prepared-whole-market-v1"


class RiskSample(Model):
    market: MarketObservation
    block_group: Annotated[tuple[Block, ...], Field(min_length=1)]
    following_block: Block

    @model_validator(mode="after")
    def continuity(self):
        block = self.market.block
        if self.block_group[-1] != block:
            raise ValueError("Comet risk sample requires the last block of its timestamp group")
        first = self.block_group[0].number
        for index, header in enumerate(self.block_group):
            if (header.chain, header.timestamp, header.number) != (
                block.chain,
                block.timestamp,
                first + index,
            ):
                raise ValueError("Comet risk timestamp group is incomplete or unordered")
        following = self.following_block
        if (
            following.chain != block.chain
            or following.number != block.number + 1
            or following.timestamp <= block.timestamp
        ):
            raise ValueError("Comet risk sample requires its immediate following time boundary")
        return self


RateRecord = InputRecord[MarketObservation, Block]
RiskRecord = InputRecord[RiskSample, Block]


def key(source: Market, product: str) -> DataKey:
    return DataKey(
        connector=CONNECTOR,
        chain=source.chain,
        source=f"{source.comet}/{source.base_asset}/{source.base_price_feed}",
        product=product,
    )


def requirement(source, settings, product):
    if product not in ("accounting", "risk"):
        raise ValueError("unsupported Comet prepared product")
    return DataRequirement(
        key=key(source, product),
        start=settings.schedule.start,
        end=settings.schedule.deadline,
        semantics="sampled" if product == "accounting" else "changes",
        sample_seconds=settings.data.sample_seconds if product == "accounting" else None,
        max_age_seconds=settings.data.max_age_seconds,
        unit="compound_v3_market_accounting"
        if product == "accounting"
        else "compound_v3_market_risk",
        visibility="end_of_block",
        payload_revision=REVISION,
    )


class RateCurve:
    def __init__(self, records: Timeline[RateRecord]):
        requirement = records.requirement
        if requirement.sample_seconds is None:
            raise ValueError("Comet accounting requires an explicit sampling interval")
        expected = tuple(range(requirement.start, requirement.end + 1, requirement.sample_seconds))
        if records.times != expected:
            raise ValueError("Comet accounting is missing a fixed sample boundary")
        self.records = records
        self.times = records.times
        first = records.rows[0]
        supply, borrow = indices(first.payload, int(first.available_at))
        self.supply, self.borrow = [], []
        for index, record in enumerate(records.rows):
            self.validate_record(record, first.payload.source)
            if index:
                previous = records.rows[index - 1]
                supply_rate, borrow_rate = rates(previous.payload)
                elapsed = record.available_at - previous.available_at
                supply = accrue_index(supply, supply_rate, elapsed)
                borrow = accrue_index(borrow, borrow_rate, elapsed)
            self.supply.append(supply)
            self.borrow.append(borrow)

    @staticmethod
    def validate_record(record, source):
        if record.key != key(source, "accounting") or record.payload.source != source:
            raise ValueError("Comet accounting curve mixes configured markets")
        if (
            record.reference != record.payload.block
            or record.source_time != record.reference.timestamp
        ):
            raise ValueError("Comet accounting differs from its pinned historical evidence")

    def at(self, time):
        record = self.records.at(time)
        index = bisect_right(self.times, time) - 1
        elapsed = time - record.available_at
        supply_rate, borrow_rate = rates(record.payload)
        return (
            accrue_index(self.supply[index], supply_rate, elapsed),
            accrue_index(self.borrow[index], borrow_rate, elapsed),
        )


@dataclass(frozen=True)
class EconomicMarket:
    sample: RiskSample
    supply_index: int
    borrow_index: int
    time: int

    def __post_init__(self):
        if self.time < self.sample.market.block.timestamp:
            raise ValueError("economic Comet market cannot use future risk evidence")
        if not uint(self.supply_index, 64) or not uint(self.borrow_index, 64):
            raise ValueError("economic Comet accounting indices must be positive")

    @property
    def observed(self):
        return self.sample.market

    def balance(self, principal):
        return present_value(principal, self.supply_index, self.borrow_index)
