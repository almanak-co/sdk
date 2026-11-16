"""Explicit Morpho Blue markets and independently scoped lender and borrower balances."""

from typing import Annotated, ClassVar, Literal

from eth_abi import encode
from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.caip import AssetType, Identifier
from almanak.connectors.morpho_blue.math import ORACLE_SCALE
from almanak.gateway.evm import erc20_address
from almanak.gateway.permission_models import ContractIdentity
from almanak.lending_intents import LendingTarget
from almanak.models import ZERO_ADDRESS, Account, Address, Asset, Block, Chain, Model, UInt

Uint256 = Annotated[UInt, Field(lt=2**256)]
Uint128 = Annotated[UInt, Field(lt=2**128)]
Positive = Annotated[Uint256, Field(gt=0)]
PARAMS_ABI = "(address,address,address,address,uint256)"


class MarketId(Identifier):
    __slots__ = ()
    pattern: ClassVar[str] = r"0x[0-9a-f]{64}"


class Market(Model):
    chain: Chain
    core: Address
    market_id: MarketId
    loan_asset: AssetType
    collateral_asset: AssetType
    oracle: Address
    irm: Address
    lltv: Annotated[UInt, Field(gt=0, lt=10**18)]

    @property
    def parameters(self):
        return (
            erc20_address(self.loan_asset, self.chain.chain_id),
            erc20_address(self.collateral_asset, self.chain.chain_id),
            self.oracle,
            self.irm,
            self.lltv,
        )

    @model_validator(mode="after")
    def scope(self):
        if ZERO_ADDRESS in (self.core, self.oracle, self.irm):
            raise ValueError("Morpho requires an explicit core, oracle and adaptive IRM")
        if self.loan_asset == self.collateral_asset:
            raise ValueError("Morpho lending targets require distinct loan and collateral assets")
        expected = MarketId("0x" + keccak(encode([PARAMS_ABI], [self.parameters])).hex())
        if self.market_id != expected:
            raise ValueError("Morpho market ID differs from its configured parameter tuple")
        return self


class Reserve(Model):
    kind: Literal["morpho_blue"] = "morpho_blue"
    market: Market
    asset: AssetType

    @property
    def chain(self):
        return self.market.chain

    @property
    def is_loan(self):
        return self.asset == self.market.loan_asset

    @model_validator(mode="after")
    def scope(self):
        if self.asset not in (self.market.loan_asset, self.market.collateral_asset):
            raise ValueError("Morpho target asset is outside its configured market")
        return self


class Target(LendingTarget):
    reserve: Reserve

    @property
    def asset(self) -> AssetType:
        return self.reserve.asset

    @property
    def identity(self):
        return self.fund, self.reserve.market.core, self.reserve.market.market_id, self.asset

    @model_validator(mode="after")
    def scope(self):
        if self.fund.chain != self.reserve.chain:
            raise ValueError("Morpho fund and market have different chain/environment scope")
        return self


class Accounting(Model):
    supply_assets: Uint128
    supply_shares: Uint128
    borrow_assets: Uint128
    borrow_shares: Uint128
    last_update: Uint128
    fee: Annotated[Uint128, Field(le=250000000000000000)]

    @model_validator(mode="after")
    def scope(self):
        if self.borrow_assets > self.supply_assets:
            raise ValueError("Morpho market borrowing exceeds supplied assets")
        return self


class Position(Model):
    supply_shares: Uint256
    borrow_shares: Uint128
    collateral: Uint128


class OracleQuote(Model):
    source: Market
    block: Block
    price: Uint256
    price_scale: Annotated[UInt, Field(ge=ORACLE_SCALE, le=ORACLE_SCALE)] = ORACLE_SCALE
    quote_unit: Literal["loan_base_units_per_collateral_base_unit"] = (
        "loan_base_units_per_collateral_base_unit"
    )

    @model_validator(mode="after")
    def scope(self):
        if self.source.chain != self.block.chain:
            raise ValueError("Morpho oracle evidence has another chain/environment scope")
        return self


class MarketObservation(Model):
    source: Market
    block: Block
    contracts: Annotated[tuple[ContractIdentity, ...], Field(min_length=3, max_length=3)]
    loan_asset: Asset
    collateral_asset: Asset
    accounting: Accounting
    rate_at_target: Uint256
    borrow_rate: Uint256
    fee_recipient: Address
    core_loan_balance: Uint256
    price: OracleQuote | None
    price_error: str | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def scope(self):
        if any(
            chain != self.block.chain
            for chain in (self.source.chain, self.loan_asset.chain, self.collateral_asset.chain)
        ):
            raise ValueError("Morpho observation has mixed chain/environment scope")
        if (self.loan_asset.identifier, self.collateral_asset.identifier) != (
            self.source.loan_asset,
            self.source.collateral_asset,
        ):
            raise ValueError("Morpho observed assets differ from the configured market")
        if tuple(contract.address for contract in self.contracts) != (
            self.source.core,
            self.source.irm,
            self.source.oracle,
        ):
            raise ValueError("Morpho observed contracts differ from their configured roles")
        if self.accounting.last_update == 0 or self.accounting.last_update > self.block.timestamp:
            raise ValueError("Morpho market is uninitialized or its accounting follows the block")
        if (self.price is None) != (self.price_error is not None):
            raise ValueError("Missing Morpho oracle data requires a visible reason")
        if self.price is not None and (
            self.price.source != self.source or self.price.block != self.block
        ):
            raise ValueError("Morpho price evidence differs from its observed market/block")
        return self


class AccountObservation(Model):
    market: MarketObservation
    fund: Account
    position: Position

    @model_validator(mode="after")
    def scope(self):
        if self.fund.chain != self.market.block.chain:
            raise ValueError("Morpho fund observation has another chain/environment scope")
        if self.fund.address == self.market.fee_recipient:
            raise ValueError("Morpho protocol fee recipient ownership is unsupported")
        if (
            self.position.supply_shares > self.market.accounting.supply_shares
            or self.position.borrow_shares > self.market.accounting.borrow_shares
        ):
            raise ValueError("Morpho owned shares exceed their observed market totals")
        return self
