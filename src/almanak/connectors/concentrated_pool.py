"""Bounded bitmap and liquidity records for the verified V3/V4 integer pool families."""

from typing import Annotated

from pydantic import Field, model_validator

from almanak.connectors.concentrated_math import sqrt_at_tick
from almanak.models import Model, UInt

TickIndex = Annotated[int, Field(strict=True, ge=-887272, le=887272)]


class Word(Model):
    index: Annotated[int, Field(strict=True, ge=-32768, le=32767)]
    bitmap: Annotated[UInt, Field(lt=2**256)]


class Tick(Model):
    index: TickIndex
    gross: Annotated[UInt, Field(gt=0, lt=2**128)]
    net: Annotated[int, Field(strict=True, ge=-(2**127), lt=2**127)]

    @model_validator(mode="after")
    def liquidity(self):
        if abs(self.net) > self.gross:
            raise ValueError("tick net liquidity exceeds gross liquidity")
        return self


def initialized_ticks(words, spacing):
    return tuple(
        ((word.index << 8) + bit) * spacing
        for word in words
        for bit in range(256)
        if word.bitmap & (1 << bit)
    )


def validate_coverage(pool, lower, upper, words, ticks):
    if not lower <= pool.tick < upper:
        raise ValueError("sampled price is outside configured tick coverage")
    if not sqrt_at_tick(pool.tick) <= pool.sqrt_price_x96 <= sqrt_at_tick(pool.tick + 1):
        raise ValueError("sampled price and tick are inconsistent")
    if pool.liquidity >= 2**128:
        raise ValueError("active liquidity exceeds uint128")
    spacing = pool.source.tick_spacing
    expected = tuple(range((lower // spacing) >> 8, ((upper // spacing) >> 8) + 1))
    if tuple(word.index for word in words) != expected:
        raise ValueError("prepared swap coverage has missing or unordered bitmap words")
    if tuple(tick.index for tick in ticks) != initialized_ticks(words, spacing):
        raise ValueError("initialized tick liquidity does not match prepared bitmap")
