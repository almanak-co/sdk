"""Compatible V3 pool targets and allocations retain concrete connector sources."""

from typing import Annotated

from pydantic import Field, model_validator

from almanak.caip import AssetType
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


class Allocation[Pool: ConcentratedMarket](Model):
    name: Name
    account: Name
    pool: Pool
    contracts: Contracts
    range: TickRange | None
    band: TickBand | None = Field(default=None, exclude_if=lambda v: v is None)
    capital: AssetAmount
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
        require_trading_entry(self.liquidity_enabled, self.entry_swap_bps)
        if self.capital.asset not in (self.pool.currency0, self.pool.currency1):
            raise ValueError("capital must identify the explicit funding currency")
        if not 0 < self.capital.amount < 2**127:
            raise ValueError("V3 capital requires a positive supported int128 quantity")
        validate_liquidity_shape(self.pool, self.range, self.band)
        return self


def validate_liquidity_shape(pool, range, band):
    if range is not None and band is not None:
        raise ValueError("a liquidity allocation declares either a fixed range or a band")
    for shape in (range, band):
        if shape is not None:
            shape.validate_pool(pool)
