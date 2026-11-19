"""Attributed V4 ownership and observations independent of durable work records."""

from datetime import UTC, datetime
from typing import Annotated

from pydantic import Field, model_validator

from almanak.caip import AssetId, AssetType
from almanak.connectors.dex_residual import validate_entry_error
from almanak.connectors.uniswap_v4.domain import PoolObservation
from almanak.connectors.uniswap_v4.hook_terms import HookTerms
from almanak.connectors.uniswap_v4.models import Range, Target, validate_position_range
from almanak.connectors.uniswap_v4.reader import Quote
from almanak.models import Account, Address, AssetAmount, Model, UInt


class LiquidityPosition(Model):
    nft: AssetId
    liquidity: Annotated[UInt, Field(gt=0, lt=2**128)]
    fee_growth: tuple[UInt, UInt] | None
    # A banded position retains the range read from the position manager; a fixed-range
    # position leaves it absent because its target already pins the range.
    range: Range | None = Field(default=None, exclude_if=lambda v: v is None)

    @property
    def token_id(self):
        return int(str(self.nft).rsplit("/", 1)[1])


class SimulatedLiquidity(Model):
    simulation_id: Annotated[str, Field(min_length=1, pattern=r"^\S+$")]
    liquidity: Annotated[UInt, Field(gt=0, lt=2**128)]
    fee_growth: tuple[UInt, UInt]
    range: Range | None = Field(default=None, exclude_if=lambda v: v is None)
    # A banded position credits the lower bracket of historical growth (`fee_growth`)
    # and retains the upper bracket checkpoint so reports can show what the sampled
    # ticks could not attribute. Fixed ranges are exact and omit it.
    bound_growth: tuple[UInt, UInt] | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def bracket(self):
        if self.bound_growth is not None and self.range is None:
            raise ValueError("a bracketed simulated position requires its concrete range")
        return self


class Ownership(Model):
    target: Target
    inventory0: AssetAmount
    inventory1: AssetAmount
    position: LiquidityPosition | SimulatedLiquidity | None = None
    entry_complete: bool = False
    # The NFT an operator deployment takes over at its first observation, as its
    # allocation states it. It is not owned yet: adoption replaces it with the position
    # read from the chain. Absent for every strategy and once adopted.
    adopts: AssetId | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def scope(self):
        if (self.inventory0.asset, self.inventory1.asset) != self.target.assets:
            raise ValueError("owned inventory must identify the target currencies")
        expected = AssetType(
            f"{self.target.fund.chain.chain_id}/erc721:{self.target.contracts.position_manager}"
        )
        if self.adopts is not None and (
            self.position is not None
            or self.target.range is None
            or self.adopts.asset_type != expected
        ):
            raise ValueError("an NFT to adopt requires its manager, a fixed range and no position")
        if isinstance(self.position, LiquidityPosition):
            if self.position.nft.asset_type != expected or not self.target.liquidity_enabled:
                raise ValueError("owned NFT must identify the retained PositionManager and range")
        if self.position and not self.target.liquidity_enabled:
            raise ValueError("liquidity ownership requires an explicit range or band")
        if self.position is not None:
            validate_position_range(self.target, self.position.range)
        return self


class Allowance(Model):
    asset: AssetType
    spender: Address
    amount: UInt
    expiration: UInt | None = None
    nonce: UInt | None = None

    def needs_cleanup(self, timestamp):
        return self.amount > 0 or (self.expiration is not None and self.expiration > timestamp)


class Snapshot(Model):
    target: Target
    pool: PoolObservation
    owned: Ownership
    wallet0: AssetAmount
    wallet1: AssetAmount
    allowances: tuple[Allowance, ...]
    native_balance: AssetAmount | None
    payer: Account | None
    entry_quote: Quote | None = None
    exit_quote: Quote | None = None
    rebalance_quote: Quote | None = Field(default=None, exclude_if=lambda v: v is None)
    # Counter inventory whose exact-size exit quote consumed its whole input and
    # returned nothing. It is measured residual dust, never a quote a swap can follow,
    # and it is absent unless measured so other snapshots keep their shape.
    exit_residual: Quote | None = Field(default=None, exclude_if=lambda v: v is None)
    # Why no entry quote is offered although an entry slice remains, when the market
    # cannot fill it at this block; absent otherwise so other snapshots keep their shape.
    entry_quote_error: str | None = Field(default=None, exclude_if=lambda v: v is None)
    position_liquidity: UInt = 0
    fee_growth: tuple[UInt, UInt] | None = None
    position_growth: tuple[UInt, UInt] | None = None
    fee_error: str | None = None
    # A reviewed hook's terms at this block, or why they are unmeasured. Hookless
    # observations omit both so their retained shape is unchanged.
    hook_terms: HookTerms | None = Field(default=None, exclude_if=lambda v: v is None)
    hook_error: str | None = Field(default=None, exclude_if=lambda v: v is None)
    quotes_included: bool = True
    simulated: bool = False

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def block(self):
        return self.pool.block

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.block.timestamp, UTC)

    @property
    def residuals(self):
        """Measured zero-output exit quotes of residual counter dust."""
        return () if self.exit_residual is None else (self.exit_residual,)

    @property
    def needs_cleanup(self):
        return any(a.needs_cleanup(self.block.timestamp) for a in self.allowances)

    @model_validator(mode="after")
    def scope(self):
        if self.owned.target != self.target or self.pool.source != self.target.pool:
            raise ValueError("observation must retain the exact target and ownership reference")
        if (self.wallet0.asset, self.wallet1.asset) != self.target.assets:
            raise ValueError("wallet observations must identify the selected currencies")
        if self.payer is not None and self.payer.chain != self.block.chain:
            raise ValueError("gas payer must retain the observed chain/environment")
        if any(
            quote is not None and not quote.output.amount
            for quote in (self.entry_quote, self.exit_quote, self.rebalance_quote)
        ):
            raise ValueError("a decision quote requires a positive measured output")
        validate_residual(self.owned, self.exit_quote, self.exit_residual, self.block)
        validate_entry_error(self.entry_quote, self.entry_quote_error)
        return self


def counter_of(owned):
    return next(
        value
        for value in (owned.inventory0, owned.inventory1)
        if value.asset == owned.target.counter_currency
    )


def validate_residual(owned, exit_quote, residual, block):
    if residual is None:
        return
    if residual.block != block or residual.output.asset != owned.target.funding_currency:
        raise ValueError("an exit residual must retain the observed block and funding asset")
    if (
        exit_quote is not None
        or owned.position is not None
        or residual.output.amount
        or residual.amount != counter_of(owned)
    ):
        raise ValueError("an exit residual measures all closed counter inventory with no output")


def counter_settled(owned, residual):
    """Counter inventory needs no exit swap: it is zero or exactly the measured residual."""
    counter = counter_of(owned)
    return not counter.amount or (residual is not None and residual.amount == counter)
