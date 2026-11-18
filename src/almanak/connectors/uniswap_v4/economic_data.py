"""Hash-pinned pool samples and explicitly bounded swap coverage."""

from typing import Annotated

from pydantic import Field, model_validator

from almanak.backtest.dataset import DataKey, DataRequirement, Digest, InputRecord
from almanak.connectors.concentrated_pool import (
    Tick as Tick,
)
from almanak.connectors.concentrated_pool import (
    TickIndex as TickIndex,
)
from almanak.connectors.concentrated_pool import (
    Word as Word,
)
from almanak.connectors.concentrated_pool import (
    validate_coverage,
)
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.uniswap_v4.domain import PoolObservation
from almanak.connectors.uniswap_v4.hook_terms import HookTerms
from almanak.connectors.uniswap_v4.models import Target
from almanak.models import ZERO_ADDRESS, Block, Model, UInt

Uint256 = Annotated[UInt, Field(lt=2**256)]

MODEL = "uniswap-v4-economic-integer-fixed-growth-v1"
REVISION = "uniswap-v4-pool-samples-v1"
CONNECTOR = ConnectorId("uniswap_v4")


class GrowthTick(Tick):
    """One initialized tick with its outside growth, as bracketed band growth reads it."""

    outside0: Annotated[UInt, Field(lt=2**256)]
    outside1: Annotated[UInt, Field(lt=2**256)]


class PoolSample(Model):
    baseline_digest: Digest
    pool: PoolObservation
    lower: TickIndex
    upper: TickIndex
    words: tuple[Word, ...]
    ticks: tuple[Tick, ...]
    fee_growth: tuple[UInt, UInt] | None
    boundary_gross: tuple[UInt, UInt] | None
    # Band targets only: the global growth and each initialized tick's outside growth,
    # aligned with `ticks`. Fixed-range and trading samples keep their earlier shape.
    global_growth: tuple[Uint256, Uint256] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    outside: tuple[tuple[Uint256, Uint256], ...] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    # A reviewed hook's fee override or output haircut read at the sample block.
    # Hookless samples omit it and keep their earlier shape.
    hook: HookTerms | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def coverage(self):
        validate_coverage(self.pool, self.lower, self.upper, self.words, self.ticks)
        if (self.global_growth is None) != (self.outside is None) or (
            self.outside is not None and len(self.outside) != len(self.ticks)
        ):
            raise ValueError("band samples require global growth and every tick's outside growth")
        if (self.hook is None) != (self.pool.source.hooks == ZERO_ADDRESS):
            raise ValueError("hooked pool samples require their measured hook terms")
        return self

    def growth_ticks(self):
        if self.outside is None:
            raise ValueError("pool sample has no tick growth for bracketed band fees")
        return tuple(
            GrowthTick(**tick.model_dump(), outside0=outside[0], outside1=outside[1])
            for tick, outside in zip(self.ticks, self.outside, strict=True)
        )


Record = InputRecord[PoolSample, Block]


def key(target: Target):
    # Allocation identity also binds its optional LP range; pool identity is
    # validated inside every payload before the timeline is used.
    return DataKey(
        connector=CONNECTOR, chain=target.fund.chain, source=target.allocation, product="pool_state"
    )


def requirement(target, settings):
    return DataRequirement(
        key=key(target),
        start=settings.schedule.start,
        end=settings.schedule.deadline,
        semantics="sampled",
        sample_seconds=settings.data.sample_seconds,
        max_age_seconds=settings.data.max_age_seconds,
        unit="uniswap_v4_integer_pool",
        visibility="end_of_block",
        payload_revision=REVISION,
    )
