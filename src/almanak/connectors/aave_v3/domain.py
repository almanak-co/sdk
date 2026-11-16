"""Configured V3 reserves and pinned accounting, with explicit fund and price scope."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.connectors.pool_lending_domain import AccountRisk as AccountRisk
from almanak.connectors.pool_lending_domain import PoolOracleQuote, PoolReserve, PoolTarget
from almanak.connectors.pool_lending_domain import ReserveConfiguration as ReserveConfiguration
from almanak.gateway.evm import erc20_address
from almanak.models import Account, Asset, Block, Model, UInt

Uint256 = Annotated[UInt, Field(lt=2**256)]
Positive = Annotated[Uint256, Field(gt=0)]
ScaledBalance = Annotated[Uint256, Field(lt=2**120)]


class Reserve(PoolReserve):
    kind: Literal["aave_v3"] = "aave_v3"


class Target(PoolTarget[Reserve]):
    pass


class OracleQuote(PoolOracleQuote):
    quote_unit: Literal["aave_v3_oracle_base_currency"] = "aave_v3_oracle_base_currency"


class ReserveObservation(Model):
    source: Reserve
    block: Block
    asset: Asset
    configuration: ReserveConfiguration
    reserve_id: Annotated[UInt, Field(lt=128)]
    liquidity_index_ray: Positive
    variable_borrow_index_ray: Positive
    liquidity_rate_ray: Uint256
    variable_borrow_rate_ray: Uint256
    last_update_timestamp: UInt
    a_token: AssetType
    variable_debt_token: AssetType
    accrued_to_treasury_scaled: Uint256
    virtual_underlying_balance: Uint256
    available_underlying: Uint256
    total_scaled_supply: Uint256
    total_scaled_debt: Uint256
    oracle: OracleQuote | None
    oracle_error: str | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def scope(self):
        if self.source.chain != self.block.chain or self.asset.chain != self.block.chain:
            raise ValueError("reserve observation has mixed chain/environment scope")
        if self.asset.identifier != self.source.asset:
            raise ValueError("reserve observation has another underlying asset")
        if self.configuration.decimals != self.asset.decimals:
            raise ValueError("reserve and underlying asset precision differ")
        for token in (self.a_token, self.variable_debt_token):
            erc20_address(token, self.source.chain.chain_id)
        if (self.oracle is None) != (self.oracle_error is not None):
            raise ValueError("missing oracle evidence requires a visible reason")
        if self.oracle is not None and (
            self.oracle.block != self.block or self.oracle.asset != self.source.asset
        ):
            raise ValueError("reserve quote must retain the same asset and pinned block")
        if self.last_update_timestamp > self.block.timestamp:
            raise ValueError("reserve accounting update cannot follow its observed block")
        return self


class PositionObservation(Model):
    reserve: ReserveObservation
    fund: Account
    supplied_assets: Uint256
    supplied_scaled: ScaledBalance
    debt_assets: Uint256
    debt_scaled: ScaledBalance
    collateral_enabled: bool
    user_configuration: Uint256
    risk: AccountRisk | None
    risk_error: str | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def scope(self):
        if self.fund.chain != self.reserve.block.chain:
            raise ValueError("position fund has another chain/environment scope")
        if (self.risk is None) != (self.risk_error is not None):
            raise ValueError("missing account risk requires a visible reason")
        enabled = bool(self.user_configuration & (1 << (2 * self.reserve.reserve_id + 1)))
        if self.collateral_enabled != enabled:
            raise ValueError("position collateral flag differs from the observed user bitmap")
        return self
