"""Compatible V3 pool targets and allocations retain concrete connector sources."""

from typing import Annotated

from pydantic import Field, model_validator

from almanak.caip import AssetId, AssetType
from almanak.connectors.entry_swap import EntrySwapBps, require_trading_entry
from almanak.connectors.v3_pool_domain import ConcentratedMarket, Contracts, TickBand, TickRange
from almanak.dex_intents import DexTarget, Name
from almanak.gateway.evm import native_asset
from almanak.models import AssetAmount, Model, UInt
from almanak.triggers import HOLD_SECONDS_DESCRIPTION, PositiveSeconds


class Target[Pool: ConcentratedMarket](DexTarget):
    pool: Pool
    contracts: Contracts
    range: TickRange | None
    band: TickBand | None = Field(default=None, exclude_if=lambda v: v is None)
    funding_asset: AssetType

    @property
    def assets(self):
        return self.pool.currency0, self.pool.currency1

    @property
    def liquidity_enabled(self):
        return self.range is not None or self.band is not None

    @property
    def funding_currency(self):
        return self.funding_asset

    @property
    def counter_currency(self):
        return next(asset for asset in self.assets if asset != self.funding_asset)

    @model_validator(mode="after")
    def scope(self):
        if self.fund.chain != self.pool.chain or self.funding_asset not in self.assets:
            raise ValueError("V3 target must retain its fund chain and explicit pool funding asset")
        validate_liquidity_shape(self.pool, self.range, self.band)
        return self


class Adoption(Model):
    """What an operator deployment owns at its start, stated by a direct action.

    A strategy never states one: its deployment starts from funded capital alone.

    `counter` is base units of the pool currency that is not the capital asset. With
    the capital amount it is the inventory attributed to the deployment, and the wallet
    must cover both. `nft` is the position-manager NFT the deployment takes over. Its
    owner, pool and range are read from the chain at the deployment's first observation
    and must be the bound wallet, the allocation's pool and the allocation's range.
    """

    counter: Annotated[UInt, Field(lt=2**127)]
    nft: AssetId | None = Field(default=None, exclude_if=lambda v: v is None)


class Allocation[Pool: ConcentratedMarket](Model):
    name: Name
    account: Name
    pool: Pool
    contracts: Contracts
    range: TickRange | None
    band: TickBand | None = Field(default=None, exclude_if=lambda v: v is None)
    capital: AssetAmount
    # Absent on the wire unless stated, so a strategy allocation keeps its form.
    adopt: Adoption | None = Field(default=None, exclude_if=lambda v: v is None)
    entry_swap_bps: EntrySwapBps
    hold_seconds: PositiveSeconds | None = Field(
        default=None, exclude_if=lambda v: v is None, description=HOLD_SECONDS_DESCRIPTION
    )
    slippage_bps: Annotated[UInt, Field(lt=10000)]
    deadline_seconds: PositiveSeconds
    native_asset: AssetType
    minimum_native_balance: UInt

    @property
    def liquidity_enabled(self):
        return self.range is not None or self.band is not None

    @property
    def chain(self):
        return self.pool.chain

    @model_validator(mode="after")
    def units(self):
        if self.native_asset != native_asset(self.pool.chain.chain_id):
            raise ValueError("payer policy must identify the chain native asset")
        if self.capital.asset not in (self.pool.currency0, self.pool.currency1):
            raise ValueError("capital must identify the explicit funding currency")
        if self.adopt is None:
            require_trading_entry(self.liquidity_enabled, self.entry_swap_bps)
        else:
            self.validate_adoption(self.adopt)
        # Only an adopting allocation may state no capital: it can act on a position
        # without bringing inventory.
        least = 1 if self.adopt is None else 0
        if not least <= self.capital.amount < 2**127:
            raise ValueError("V3 capital requires a positive supported int128 quantity")
        validate_liquidity_shape(self.pool, self.range, self.band)
        return self

    def validate_adoption(self, adopt):
        """An adopting allocation trades nothing: it has no entry swap and no band."""
        if self.entry_swap_bps or self.band is not None:
            raise ValueError("an adopting allocation states no entry swap and no band")
        if adopt.nft is None:
            return
        manager = AssetType(f"{self.pool.chain.chain_id}/erc721:{self.contracts.position_manager}")
        if self.range is None or adopt.nft.asset_type != manager:
            raise ValueError("an adopted NFT requires its position manager and a fixed range")


def validate_liquidity_shape(pool, range, band):
    if range is not None and band is not None:
        raise ValueError("a liquidity allocation declares either a fixed range or a band")
    for shape in (range, band):
        if shape is not None:
            shape.validate_pool(pool)
