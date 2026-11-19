"""Fund-owned V3 NFT obligations and explicitly measured inventory."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetId, AssetType
from almanak.connectors.v3_pool_domain import Liquidity, TickRange, Uint256
from almanak.connectors.v3_pool_models import Target
from almanak.models import Address, AssetAmount, Block, Model


class Position(Model):
    nft: AssetId
    liquidity: Liquidity
    owed0: Uint256
    owed1: Uint256
    fee_growth: tuple[Uint256, Uint256]
    # A banded position retains the range read from the position manager; a fixed-range
    # position leaves it absent because its target already pins the range.
    range: TickRange | None = Field(default=None, exclude_if=lambda v: v is None)

    @property
    def token_id(self):
        return int(self.nft.token_id)


class SimulatedPosition(Model):
    kind: Literal["simulated_liquidity"] = "simulated_liquidity"
    identity: Annotated[str, Field(min_length=1)]
    liquidity: Liquidity
    owed0: Uint256
    owed1: Uint256
    fee_growth: tuple[Uint256, Uint256]
    range: TickRange | None = Field(default=None, exclude_if=lambda v: v is None)
    # A banded position credits the lower bracket of historical growth (`fee_growth`,
    # `owed0/1`) and retains the upper bracket separately so reports can show how much
    # growth the sampled ticks could not attribute. Fixed ranges are exact and omit both.
    bound_growth: tuple[Uint256, Uint256] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    bound_owed: tuple[Uint256, Uint256] | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def bracket(self):
        if (self.bound_growth is None) != (self.bound_owed is None) or (
            self.bound_growth is not None and self.range is None
        ):
            raise ValueError("a bracketed simulated position requires its range and both bounds")
        if self.bound_owed is not None and any(
            most < least
            for most, least in zip(self.bound_owed, (self.owed0, self.owed1), strict=True)
        ):
            raise ValueError("upper-bound fees cannot be below the credited fees")
        return self


class Ownership[T: Target](Model):
    target: T
    inventory0: AssetAmount
    inventory1: AssetAmount
    position: Position | SimulatedPosition | None = None
    entry_complete: bool = False
    # The NFT an operator deployment takes over at its first observation, as its
    # allocation states it. It is not owned yet: adoption replaces it with the position
    # read from the chain. Absent for every strategy and once adopted.
    adopts: AssetId | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def scope(self):
        if (self.inventory0.asset, self.inventory1.asset) != self.target.assets:
            raise ValueError("inventory must retain the target's currency order")
        if self.position is not None and not self.target.liquidity_enabled:
            raise ValueError("owned liquidity requires a configured range or band")
        expected = AssetType(
            f"{self.target.fund.chain.chain_id}/erc721:{self.target.contracts.position_manager}"
        )
        if isinstance(self.position, Position):
            if self.position.nft.asset_type != expected:
                raise ValueError("owned NFT requires its exact manager, chain and configured range")
            validate_position_range(self.target, self.position.range)
        if self.adopts is not None and (
            self.position is not None
            or self.target.range is None
            or self.adopts.asset_type != expected
        ):
            raise ValueError("an NFT to adopt requires its manager, a fixed range and no position")
        return self


def validate_position_range(target, selected):
    if target.band is not None:
        if selected is None:
            raise ValueError("banded liquidity ownership requires the retained position range")
        target.band.validate_range(selected, target.pool.tick_spacing)
    elif selected is not None and selected != target.range:
        raise ValueError("owned NFT range differs from the configured fixed range")


class Allowance(Model):
    asset: AssetType
    spender: Address
    amount: Uint256


class Quote[T: Target](Model):
    source: T
    block: Block
    amount_in: AssetAmount
    amount_out: AssetAmount
    sqrt_price_after: Uint256
    initialized_ticks_crossed: int
    calculation: Literal["contract_quoter", "prepared_integer_pool"]
    inputs: tuple[str, ...] = ()

    @model_validator(mode="after")
    def units(self):
        if self.source.fund.chain != self.block.chain or {
            self.amount_in.asset,
            self.amount_out.asset,
        } != set(self.source.assets):
            raise ValueError("quote must retain its exact source, execution chain and units")
        # A zero output is a measurement: the whole input produced nothing. Snapshots
        # admit it only as an exit residual, never as a quote a swap can follow.
        if self.amount_in.amount <= 0:
            raise ValueError("quote requires a positive measured input")
        return self
