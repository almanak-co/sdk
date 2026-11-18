"""Explicit Comet markets, asset targets and chain-scoped protocol observations."""

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.gateway.evm import erc20_address
from almanak.gateway.permission_models import ContractIdentity
from almanak.lending_intents import LendingTarget
from almanak.models import ZERO_ADDRESS, Account, Address, Asset, Block, Chain, Model, UInt

Uint256 = Annotated[UInt, Field(lt=2**256)]
Uint128 = Annotated[UInt, Field(lt=2**128)]
Uint104 = Annotated[UInt, Field(lt=2**104)]
Uint64 = Annotated[UInt, Field(lt=2**64)]
Positive = Annotated[Uint256, Field(gt=0)]
Principal = Annotated[int, Field(strict=True, ge=-(2**103), lt=2**103)]


class Revision(StrEnum):
    STANDARD = "extended_asset_list"
    CONTROLS = "extended_asset_list_with_controls"


class Market(Model):
    chain: Chain
    comet: Address
    base_asset: AssetType
    base_price_feed: Address

    @model_validator(mode="after")
    def scope(self):
        erc20_address(self.base_asset, self.chain.chain_id)
        if ZERO_ADDRESS in (self.comet, self.base_price_feed):
            raise ValueError("Compound requires an explicit Comet and base price feed")
        return self


class Reserve(Model):
    kind: Literal["compound_v3"] = "compound_v3"
    market: Market
    asset: AssetType
    price_feed: Address

    @property
    def chain(self):
        return self.market.chain

    @property
    def is_base(self):
        return self.asset == self.market.base_asset

    @model_validator(mode="after")
    def scope(self):
        erc20_address(self.asset, self.chain.chain_id)
        if self.price_feed == ZERO_ADDRESS:
            raise ValueError("Compound asset observations require an explicit price feed")
        if self.is_base and self.price_feed != self.market.base_price_feed:
            raise ValueError("Compound base target differs from the configured market feed")
        return self


class Target(LendingTarget):
    reserve: Reserve

    @property
    def asset(self) -> AssetType:
        return self.reserve.asset

    @property
    def identity(self):
        return self.fund, self.reserve.market.comet, self.asset

    @model_validator(mode="after")
    def scope(self):
        if self.fund.chain != self.reserve.chain:
            raise ValueError(
                "Compound target fund and market have different chain/environment scope"
            )
        return self


class OracleQuote(Model):
    block: Block
    asset: AssetType
    comet: Address
    feed: Address
    price: Positive
    price_scale: Literal[100000000] = 100000000
    quote_unit: Literal["compound_usd"] = "compound_usd"

    @model_validator(mode="after")
    def scope(self):
        erc20_address(self.asset, self.block.chain.chain_id)
        if ZERO_ADDRESS in (self.comet, self.feed):
            raise ValueError("Compound price evidence requires its exact market and feed")
        return self


class RateCurve(Model):
    kink: Uint256
    base_per_second: Uint256
    slope_low_per_second: Uint256
    slope_high_per_second: Uint256


class Accounting(Model):
    supply_index: Annotated[Uint64, Field(gt=0)]
    borrow_index: Annotated[Uint64, Field(gt=0)]
    tracking_supply_index: Uint64
    tracking_borrow_index: Uint64
    total_supply_principal: Uint104
    total_borrow_principal: Uint104
    last_accrual_time: Annotated[UInt, Field(lt=2**40)]
    pause_flags: Annotated[UInt, Field(lt=2**8)]


class ExtendedControls(Model):
    pause_flags: Annotated[UInt, Field(lt=2**24)]
    collateral_supply_flags: Annotated[UInt, Field(lt=2**24)]
    collateral_withdraw_flags: Annotated[UInt, Field(lt=2**24)]
    collateral_transfer_flags: Annotated[UInt, Field(lt=2**24)]
    deactivated_collaterals: Annotated[UInt, Field(lt=2**24)]
    maximum_borrow_utilization: Literal[2000000000000000000] = 2000000000000000000


class CollateralObservation(Model):
    asset: Asset
    offset: Annotated[UInt, Field(lt=24)]
    price_feed: Address
    scale: Annotated[Uint64, Field(gt=0)]
    borrow_factor: Annotated[Uint64, Field(le=10**18)]
    liquidate_factor: Annotated[Uint64, Field(le=10**18)]
    liquidation_factor: Annotated[Uint64, Field(le=10**18)]
    supply_cap: Uint128
    total_supply: Uint128
    price: OracleQuote | None
    price_error: str | None = Field(default=None, exclude_if=lambda v: v is None)

    @property
    def asset_type(self) -> AssetType:
        identifier = self.asset.identifier
        if not isinstance(identifier, AssetType):
            raise ValueError("Compound collateral requires a fungible asset identity")
        return identifier

    @model_validator(mode="after")
    def scope(self):
        erc20_address(self.asset_type, self.asset.chain.chain_id)
        if self.scale != 10**self.asset.decimals:
            raise ValueError("Compound collateral scale differs from asset precision")
        if (self.price is None) != (self.price_error is not None):
            raise ValueError("missing Compound collateral prices require a visible reason")
        if self.price is not None and (
            self.price.asset != self.asset.identifier or self.price.feed != self.price_feed
        ):
            raise ValueError("Compound collateral price has another asset or feed")
        return self


class MarketObservation(Model):
    source: Market
    block: Block
    revision: Revision
    contracts: Annotated[tuple[ContractIdentity, ...], Field(min_length=4, max_length=4)]
    base_asset: Asset
    accounting: Accounting
    controls: ExtendedControls | None
    supply_curve: RateCurve
    borrow_curve: RateCurve
    base_borrow_minimum: Uint256
    base_available: Uint256
    base_price: OracleQuote | None
    base_price_error: str | None = Field(default=None, exclude_if=lambda v: v is None)
    collaterals: tuple[CollateralObservation, ...]

    @model_validator(mode="after")
    def scope(self):
        if self.source.chain != self.block.chain or self.base_asset.chain != self.block.chain:
            raise ValueError("Compound market observation has mixed chain/environment scope")
        if self.base_asset.identifier != self.source.base_asset:
            raise ValueError("Compound observation has another market base asset")
        if (
            self.contracts[0].address != self.source.comet
            or len({c.address for c in self.contracts}) != 4
        ):
            raise ValueError(
                "Compound observation requires four distinct correlated deployment roles"
            )
        if self.accounting.last_accrual_time > self.block.timestamp:
            raise ValueError("Compound accounting cannot follow its observed block")
        if (self.controls is not None) != (self.revision is Revision.CONTROLS):
            raise ValueError("Compound revision requires its own observed control policy")
        self.validate_prices()
        self.validate_collaterals()
        return self

    def validate_prices(self):
        if (self.base_price is None) != (self.base_price_error is not None):
            raise ValueError("missing Compound base price requires a visible reason")
        if self.base_price is not None and (
            self.base_price.asset != self.source.base_asset
            or self.base_price.feed != self.source.base_price_feed
            or self.base_price.comet != self.source.comet
            or self.base_price.block != self.block
        ):
            raise ValueError("Compound base quote differs from its configured market")

    def validate_collaterals(self):
        if len(self.collaterals) > 24 or tuple(c.offset for c in self.collaterals) != tuple(
            range(len(self.collaterals))
        ):
            raise ValueError("Compound collateral observations require their ordered asset list")
        assets = [c.asset.identifier for c in self.collaterals]
        if len(set(assets)) != len(assets) or self.source.base_asset in assets:
            raise ValueError("Compound collateral identities must be unique and exclude base")
        for collateral in self.collaterals:
            if collateral.asset.chain != self.block.chain:
                raise ValueError("Compound collateral has another chain/environment scope")
            if collateral.price is not None and (
                collateral.price.block != self.block or collateral.price.comet != self.source.comet
            ):
                raise ValueError("Compound collateral price differs from its observed market")


class CollateralBalance(Model):
    asset: AssetType
    balance: Uint128


class AccountObservation(Model):
    market: MarketObservation
    fund: Account
    principal: Principal
    tracking_index: Uint64
    tracking_accrued: Uint64
    assets_in: Annotated[UInt, Field(lt=2**16)]
    extended_assets_in: Annotated[UInt, Field(lt=2**8)]
    supplied_base: Uint256
    borrowed_base: Uint256
    collaterals: tuple[CollateralBalance, ...]

    @model_validator(mode="after")
    def scope(self):
        if self.fund.chain != self.market.block.chain:
            raise ValueError("Compound fund and market observation have different scope")
        if tuple(c.asset for c in self.collaterals) != tuple(
            c.asset.identifier for c in self.market.collaterals
        ):
            raise ValueError("Compound fund observations require every configured collateral")
        if self.supplied_base and self.borrowed_base:
            raise ValueError("Compound signed base principal cannot supply and borrow together")
        return self
