"""Compatible Pool market scope shared by Aave V3 and SparkLend concrete targets."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.gateway.evm import erc20_address
from almanak.lending_intents import LendingTarget
from almanak.models import ZERO_ADDRESS, Address, Block, Chain, Model, UInt

Uint256 = Annotated[UInt, Field(lt=2**256)]
Positive = Annotated[Uint256, Field(gt=0)]


class PoolReserve(Model):
    kind: Literal["aave_v3", "sparklend"]
    chain: Chain
    pool: Address
    asset: AssetType

    @model_validator(mode="after")
    def scope(self):
        erc20_address(self.asset, self.chain.chain_id)
        if self.pool == ZERO_ADDRESS:
            raise ValueError("Pool lending requires an explicit Pool contract")
        return self


class PoolTarget[Reserve: PoolReserve](LendingTarget):
    reserve: Reserve

    @property
    def asset(self) -> AssetType:
        return self.reserve.asset

    @property
    def identity(self):
        return self.fund, self.reserve.pool, self.asset

    @model_validator(mode="after")
    def environment(self):
        if self.fund.chain != self.reserve.chain:
            raise ValueError("Pool lending fund and reserve have different chain/environment scope")
        return self


class ReserveConfiguration(Model):
    raw: Uint256

    def bits(self, start: int, width: int) -> int:
        return (self.raw >> start) & ((1 << width) - 1)

    @property
    def ltv_bps(self) -> int:
        return self.bits(0, 16)

    @property
    def first_supply_enables_collateral(self) -> bool:
        """Whether the Pool enables a supply into an empty position as collateral.

        Exact only for the ordinary-mode reserves the SDK admits: on Aave V3 revision 11 the
        Pool tests the user's effective LTV (eMode can change it), and on SparkLend a reserve
        with a nonzero debt ceiling (isolated collateral) also needs the
        ISOLATED_COLLATERAL_SUPPLIER role. eMode for new exposure and isolated or siloed
        SparkLend reserves are refused elsewhere, which leaves reserve LTV > 0.
        """
        return bool(self.ltv_bps)

    @property
    def liquidation_threshold_bps(self) -> int:
        return self.bits(16, 16)

    @property
    def liquidation_bonus_bps(self) -> int:
        return self.bits(32, 16)

    @property
    def decimals(self) -> int:
        return self.bits(48, 8)

    @property
    def active(self) -> bool:
        return bool(self.bits(56, 1))

    @property
    def frozen(self) -> bool:
        return bool(self.bits(57, 1))

    @property
    def borrowing(self) -> bool:
        return bool(self.bits(58, 1))

    @property
    def paused(self) -> bool:
        return bool(self.bits(60, 1))

    @property
    def reserve_factor_bps(self) -> int:
        return self.bits(64, 16)

    @property
    def borrow_cap(self) -> int:
        return self.bits(80, 36) * 10**self.decimals

    @property
    def supply_cap(self) -> int:
        return self.bits(116, 36) * 10**self.decimals

    @property
    def liquidation_protocol_fee_bps(self) -> int:
        return self.bits(152, 16)


class AccountRisk(Model):
    total_collateral_base: Uint256
    total_debt_base: Uint256
    available_borrows_base: Uint256
    liquidation_threshold_bps: Uint256
    ltv_bps: Uint256
    health_factor_wad: Uint256
    emode_category: Uint256


class PoolOracleQuote(Model):
    block: Block
    asset: AssetType
    oracle: Address
    feed: Address
    base_currency: Address
    base_currency_unit: Positive
    price: Positive
    quote_unit: Literal["aave_v3_oracle_base_currency", "sparklend_oracle_base_currency"]

    @model_validator(mode="after")
    def scope(self):
        if self.asset.chain != self.block.chain.chain_id:
            raise ValueError("oracle quote must retain its observed chain and asset")
        return self
