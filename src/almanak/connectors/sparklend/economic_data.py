"""Prepared SparkLend market evidence and cadence-independent sampled accounting curves."""

from dataclasses import dataclass
from typing import Annotated

from pydantic import Field, model_validator

from almanak.backtest.dataset import DataKey, DataRequirement, InputRecord, Timeline
from almanak.backtest.types import UnixTime
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.sparklend.domain import Reserve, ReserveObservation
from almanak.connectors.sparklend.math import (
    debt_assets,
    normalized_indices,
    supplied_assets,
    uint,
)
from almanak.models import ZERO_ADDRESS, Address, Block, Model

CONNECTOR = ConnectorId("sparklend")
MODEL = "sparklend-revision4-economic-indices-v2"
REVISION = "sparklend-revision4-prepared-reserves-v1"


class RiskSample(Model):
    reserve: ReserveObservation
    oracle_sentinel: Address
    block_group: Annotated[tuple[Block, ...], Field(min_length=1)]
    following_block: Block

    @model_validator(mode="after")
    def continuity(self):
        validate_market(self.reserve)
        if self.oracle_sentinel != ZERO_ADDRESS:
            raise ValueError(
                "SparkLend prepared risk currently requires an observed absent sentinel"
            )
        block = self.reserve.block
        if self.block_group[-1] != block:
            raise ValueError("risk sample must retain the last block of its timestamp group")
        first = self.block_group[0].number
        for index, header in enumerate(self.block_group):
            if (header.chain, header.timestamp, header.number) != (
                block.chain,
                block.timestamp,
                first + index,
            ):
                raise ValueError("risk timestamp group is incomplete or unordered")
        following = self.following_block
        if (
            following.chain != block.chain
            or following.number != block.number + 1
            or following.timestamp <= block.timestamp
        ):
            raise ValueError("risk sample requires its immediate following timestamp boundary")
        return self


def validate_market(reserve):
    if reserve.total_stable_debt or reserve.unbacked:
        raise ValueError(
            "SparkLend prepared accounting excludes stable-rate debt and unbacked supply"
        )
    config = reserve.configuration
    if config.bits(212, 40) or config.bits(62, 1):
        raise ValueError("SparkLend prepared accounting excludes isolated and siloed reserves")


RateRecord = InputRecord[ReserveObservation, Block]
RiskRecord = InputRecord[RiskSample, Block]


def key(source: Reserve, product: str) -> DataKey:
    return DataKey(
        connector=CONNECTOR,
        chain=source.chain,
        source=f"{source.pool}/{source.asset}",
        product=product,
    )


def requirement(source, settings, product):
    if product not in ("accounting", "risk"):
        raise ValueError("unsupported SparkLend prepared product")
    return DataRequirement(
        key=key(source, product),
        start=settings.schedule.start,
        end=settings.schedule.deadline,
        semantics="sampled" if product == "accounting" else "changes",
        sample_seconds=settings.data.sample_seconds if product == "accounting" else None,
        max_age_seconds=settings.data.max_age_seconds,
        unit="sparklend_reserve_accounting" if product == "accounting" else "sparklend_pool_risk",
        visibility="end_of_block",
        payload_revision=REVISION,
    )


class RateCurve:
    """Supply and borrow indexes at any second, anchored at every sample.

    Each sample carries the reserve's own indexes at its block. The curve is the
    latest sample at or before the asked second, accrued at that sample's rates to
    that second, as the Pool's normalized income and debt views do while the reserve
    is not updated. At every sample boundary it therefore equals the indexes read on
    chain there, and no earlier sample has any effect after it.

    The curve may step down at a boundary. When the rates fell between two samples,
    the earlier sample's rates accrue more than the reserve did, and the next sample
    replaces that value with the lower one read on chain. The on-chain index never
    decreases; the step is the model's own excess of one interval, removed. A
    balance or a debt valued across such a boundary shrinks by that excess with no
    protocol event.
    """

    def __init__(self, records: Timeline[RateRecord]):
        requirement = records.requirement
        if requirement.sample_seconds is None:
            raise ValueError("SparkLend accounting requires an explicit sampling interval")
        expected = tuple(range(requirement.start, requirement.end + 1, requirement.sample_seconds))
        if records.times != expected:
            raise ValueError("SparkLend accounting is missing a fixed sample boundary")
        self.records = records
        source = records.rows[0].payload.source
        for record in records.rows:
            self.validate_record(record, source)

    @staticmethod
    def validate_record(record, source):
        validate_market(record.payload)
        if record.key != key(source, "accounting") or record.payload.source != source:
            raise ValueError("SparkLend accounting curve mixes configured reserves")
        if (
            record.reference != record.payload.block
            or record.source_time != record.reference.timestamp
        ):
            raise ValueError("SparkLend accounting differs from its pinned historical evidence")

    def at(self, time):
        supply, borrow = normalized_indices(self.records.at(time).payload, int(time))
        return uint(supply, 128), uint(borrow, 128)


@dataclass(frozen=True)
class Market:
    sample: RiskSample
    supply_index: int
    debt_index: int
    time: int

    def __post_init__(self):
        # Convert the shared simulation clock at the protocol arithmetic boundary.
        object.__setattr__(self, "time", int(UnixTime(self.time)))
        if self.time < self.sample.reserve.block.timestamp:
            raise ValueError("economic market cannot use a future risk observation")
        if not uint(self.supply_index, 128) or not uint(self.debt_index, 128):
            raise ValueError("economic accounting indices must be positive")

    @property
    def reserve(self):
        return self.sample.reserve

    def supplied(self, scaled):
        return supplied_assets(scaled, self.supply_index)

    def borrowed(self, scaled):
        return debt_assets(scaled, self.debt_index)
