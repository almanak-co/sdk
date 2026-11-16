"""Author-facing liquidity band input shared by the verified V3 pool family."""

import math
from fractions import Fraction
from typing import Annotated

from pydantic import Field, model_validator

from almanak.connectors.v3_pool_domain import Tick
from almanak.models import Model, UInt

TICK_BASE = Fraction(10001, 10000)


class BandInput(Model):
    """The width of each position, and an optional tick envelope the gateway enforces.

    `width` is a tick count; `width_bps` is the price distance from the centre to each
    edge in basis points, so `width_bps: 500` opens positions spanning about ±5%.
    Without `lower` and `upper` the band recentres around the price indefinitely.
    """

    lower: Tick | None = None
    upper: Tick | None = None
    width: Annotated[int, Field(strict=True, gt=0)] | None = None
    width_bps: Annotated[UInt, Field(gt=0)] | None = None

    @model_validator(mode="after")
    def one_width(self):
        if (self.width is None) == (self.width_bps is None):
            raise ValueError("a liquidity band declares exactly one of width or width_bps")
        if (self.lower is None) != (self.upper is None):
            raise ValueError("a liquidity band envelope declares both lower and upper, or neither")
        return self

    def resolve(self, tick_spacing: int) -> dict:
        if self.width is not None:
            width = self.width
        else:
            width = width_from_bps(self.width_bps or 0, tick_spacing)
        envelope = {} if self.lower is None else {"lower": self.lower, "upper": self.upper}
        return {**envelope, "width": width}


def width_from_bps(bps: int, tick_spacing: int) -> int:
    """Ticks covering at least ±bps around the centre, each half rounded up to the spacing."""
    ratio = 1 + Fraction(bps, 10000)
    estimate = math.log(1 + bps / 10000) / math.log(1.0001)
    half = max(tick_spacing, math.ceil(estimate / tick_spacing) * tick_spacing)
    while half > tick_spacing and TICK_BASE ** (half - tick_spacing) >= ratio:
        half -= tick_spacing
    while TICK_BASE**half < ratio:
        half += tick_spacing
    return 2 * half


def validate_shape_inputs(range_input, band_input):
    if range_input is not None and band_input is not None:
        raise ValueError("a liquidity allocation declares either range or band, not both")
