"""Compatible market identities for verified Uniswap and Pancake V3 pool families."""

from abc import abstractmethod
from fractions import Fraction
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.connectors.concentrated_math import sqrt_at_tick
from almanak.gateway.evm import erc20_address
from almanak.models import ZERO_ADDRESS, Address, Asset, Block, Chain, Model, UInt

MIN_TICK, MAX_TICK = -887272, 887272
Tick = Annotated[int, Field(strict=True, ge=MIN_TICK, le=MAX_TICK)]
Liquidity = Annotated[UInt, Field(lt=2**128)]
Uint256 = Annotated[UInt, Field(lt=2**256)]


class ConcentratedMarket(Model):
    kind: Literal["uniswap_v3", "pancakeswap_v3", "aerodrome_slipstream"]
    chain: Chain
    factory: Address
    pool: Address
    currency0: AssetType
    currency1: AssetType

    @model_validator(mode="after")
    def identity(self):
        if erc20_address(self.currency0, self.chain.chain_id) >= erc20_address(
            self.currency1, self.chain.chain_id
        ):
            raise ValueError("V3 currencies must be distinct ERC-20 assets in address order")
        if ZERO_ADDRESS in (self.factory, self.pool) or self.factory == self.pool:
            raise ValueError("V3 factory and pool require distinct contract addresses")
        return self


class ConcentratedPool(ConcentratedMarket):
    kind: Literal["uniswap_v3", "pancakeswap_v3"]
    fee: Annotated[UInt, Field(lt=1_000_000)]
    tick_spacing: Annotated[int, Field(strict=True, ge=1, lt=16384)]


class Contracts(Model):
    router: Address
    position_manager: Address
    quoter: Address

    @model_validator(mode="after")
    def identity(self):
        addresses = self.router, self.position_manager, self.quoter
        if ZERO_ADDRESS in addresses or len(set(addresses)) != len(addresses):
            raise ValueError("V3 periphery requires distinct contract addresses")
        return self


class TickRange(Model):
    lower: Tick
    upper: Tick

    @model_validator(mode="after")
    def ordered(self):
        if self.lower >= self.upper:
            raise ValueError("liquidity range must have increasing tick bounds")
        return self

    def validate_pool(self, pool):
        if self.lower % pool.tick_spacing or self.upper % pool.tick_spacing:
            raise ValueError("liquidity range must align to the configured pool tick spacing")


class TickBand(Model):
    """The width of each position, and an optional tick envelope the gateway enforces.

    Without an envelope the band follows the price for the whole deployment: each
    position opens centred on the observed tick, limited only by the protocol's ticks.
    """

    lower: Tick | None = Field(default=None, exclude_if=lambda v: v is None)
    upper: Tick | None = Field(default=None, exclude_if=lambda v: v is None)
    width: Annotated[int, Field(strict=True, gt=0)]

    @model_validator(mode="after")
    def ordered(self):
        if (self.lower is None) != (self.upper is None):
            raise ValueError("a liquidity band envelope declares both lower and upper, or neither")
        if (
            self.lower is not None
            and self.upper is not None
            and (self.lower >= self.upper or self.width > self.upper - self.lower)
        ):
            raise ValueError("liquidity band requires increasing bounds and a width inside them")
        return self

    @property
    def bounded(self) -> bool:
        return self.lower is not None

    def limits(self, spacing: int) -> tuple[int, int]:
        """The ticks any position may span: the envelope, or the aligned protocol range."""
        if self.lower is not None and self.upper is not None:
            return self.lower, self.upper
        edge = MAX_TICK // spacing * spacing
        return -edge, edge

    def validate_pool(self, pool):
        spacing = pool.tick_spacing
        if any(value % spacing for value in (self.lower or 0, self.upper or 0, self.width)):
            raise ValueError("liquidity band bounds and width must align to the pool tick spacing")
        lower, upper = self.limits(spacing)
        if self.width > upper - lower:
            raise ValueError("liquidity band width exceeds the pool's usable tick range")

    def centered(self, tick: int, spacing: int) -> TickRange:
        # The largest aligned tick at or below the current tick, less half the width,
        # clamped so the whole position stays inside the limits. Integer division
        # floors for negative ticks, so the result contains the tick unless clamped.
        floor, ceiling = self.limits(spacing)
        half = (self.width // 2) // spacing * spacing
        lower = (tick // spacing) * spacing - half
        lower = max(floor, min(lower, ceiling - self.width))
        return TickRange(lower=lower, upper=lower + self.width)

    def validate_range(self, bounds, spacing: int) -> TickRange:
        selected = TickRange(lower=bounds.lower, upper=bounds.upper)
        if selected.lower % spacing or selected.upper % spacing:
            raise ValueError("liquidity range must align to the configured pool tick spacing")
        floor, ceiling = self.limits(spacing)
        if (
            selected.lower < floor
            or selected.upper > ceiling
            or selected.upper - selected.lower != self.width
        ):
            raise ValueError("liquidity range must be exactly one band width inside the band")
        return selected

    def validate_centered(self, bounds, tick: int, spacing: int) -> None:
        """An envelope-less band has no author bound on where it mints, so an opened
        range must be exactly the connector's centred selection at the decision tick."""
        if self.bounded:
            return
        expected = self.centered(tick, spacing)
        if (bounds.lower, bounds.upper) != (expected.lower, expected.upper):
            raise ValueError("unbounded band liquidity must open centred on the decision tick")


class PriceRequest[Pool: ConcentratedMarket](Model):
    source: Pool
    base: AssetType
    quote: AssetType

    @model_validator(mode="after")
    def pair(self):
        if self.base == self.quote or {self.base, self.quote} != {
            self.source.currency0,
            self.source.currency1,
        }:
            raise ValueError("pool price units must identify its two distinct currencies")
        return self


class ConcentratedObservation[Pool: ConcentratedMarket](Model):
    source: Pool
    block: Block
    currency0: Asset
    currency1: Asset
    sqrt_price_x96: Annotated[UInt, Field(gt=0, lt=2**160)]
    tick: Tick
    liquidity: Liquidity

    @property
    @abstractmethod
    def is_unlocked(self) -> bool:
        """A prepared observation must be outside the pool's execution lock."""
        raise NotImplementedError

    @model_validator(mode="after")
    def scope(self):
        if (
            not self.source.chain
            == self.block.chain
            == self.currency0.chain
            == self.currency1.chain
        ):
            raise ValueError("pool observation must retain one chain and environment")
        if (self.currency0.identifier, self.currency1.identifier) != (
            self.source.currency0,
            self.source.currency1,
        ):
            raise ValueError("pool observation currencies differ from its source")
        if self.tick == 887272 or not (
            sqrt_at_tick(self.tick) <= self.sqrt_price_x96 <= sqrt_at_tick(self.tick + 1)
        ):
            raise ValueError("pool price and tick are inconsistent")
        return self

    def spot_price(self, request):
        if request.source != self.source or not self.liquidity or not self.is_unlocked:
            raise ValueError("pool price requires its requested liquid, unlocked source")
        ratio = Fraction(self.sqrt_price_x96**2, 2**192)
        ratio *= Fraction(10**self.currency0.decimals, 10**self.currency1.decimals)
        return ratio if request.base == self.currency0.identifier else 1 / ratio


class Observation[Pool: ConcentratedPool](ConcentratedObservation[Pool]):
    protocol_fee: UInt
    unlocked: bool

    @property
    def is_unlocked(self) -> bool:
        return self.unlocked
