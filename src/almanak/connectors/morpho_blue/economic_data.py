"""Prepared Morpho market accounting and canonical oracle timestamp boundaries."""

from typing import Annotated

from pydantic import Field, model_validator

from almanak.backtest.dataset import DataKey, DataRequirement, InputRecord
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue.domain import Market, MarketObservation
from almanak.models import Block, Model

CONNECTOR = ConnectorId("morpho_blue")
MODEL = "morpho-blue-archive-anchored-market-v1"
REVISION = "morpho-blue-prepared-market-v1"


class RiskSample(Model):
    market: MarketObservation
    block_group: Annotated[tuple[Block, ...], Field(min_length=1)]
    following_block: Block

    @model_validator(mode="after")
    def continuity(self):
        block = self.market.block
        if self.block_group[-1] != block:
            raise ValueError("Morpho risk sample requires the last block of its timestamp group")
        first = self.block_group[0].number
        for index, header in enumerate(self.block_group):
            if (header.chain, header.timestamp, header.number) != (
                block.chain,
                block.timestamp,
                first + index,
            ):
                raise ValueError("Morpho risk timestamp group is incomplete or unordered")
        following = self.following_block
        if (
            following.chain != block.chain
            or following.number != block.number + 1
            or following.timestamp <= block.timestamp
        ):
            raise ValueError("Morpho risk sample requires its immediate following time boundary")
        return self


RateRecord = InputRecord[MarketObservation, Block]
RiskRecord = InputRecord[RiskSample, Block]


def key(source: Market, product: str) -> DataKey:
    return DataKey(
        connector=CONNECTOR,
        chain=source.chain,
        source=f"{source.core}/{source.market_id}",
        product=product,
    )


def requirement(source, settings, product):
    if product not in ("accounting", "risk"):
        raise ValueError("unsupported Morpho prepared product")
    return DataRequirement(
        key=key(source, product),
        start=settings.schedule.start,
        end=settings.schedule.deadline,
        semantics="sampled" if product == "accounting" else "changes",
        sample_seconds=settings.data.sample_seconds if product == "accounting" else None,
        max_age_seconds=settings.data.max_age_seconds,
        unit="morpho_blue_market_accounting"
        if product == "accounting"
        else "morpho_blue_market_risk",
        visibility="end_of_block",
        payload_revision=REVISION,
    )
