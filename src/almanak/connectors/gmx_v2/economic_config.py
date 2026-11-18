"""Prepared GMX runs select publication sources, bounded collection and request costs."""

from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from almanak.backtest.config import ExcludedCosts
from almanak.backtest.inventory import InventoryConfig
from almanak.backtest.strategy_prices import StrategyPriceData
from almanak.caip import ChainId
from almanak.connectors.gmx_v2.published_collection import PublicationLimits
from almanak.connectors.gmx_v2.published_source import PublishedSource
from almanak.connectors.identifiers import Name
from almanak.gateway.evm import native_asset
from almanak.models import AssetAmount, Model, UInt
from almanak.triggers import PositiveSeconds

Positive = Annotated[UInt, Field(gt=0)]


class SourceBinding(Model):
    allocation: Name
    source: PublishedSource


class DataPolicy(StrategyPriceData):
    source: Literal["gmx_published_signed_reports"]
    publications: Annotated[tuple[SourceBinding, ...], Field(min_length=1)]
    max_age_seconds: PositiveSeconds
    maximum_risk_blocks: Positive
    maximum_report_age_seconds: PositiveSeconds
    publication_limits: PublicationLimits

    @field_validator("publications", mode="before")
    @classmethod
    def arrays(cls, value):
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def bounds(self):
        names = [binding.allocation for binding in self.publications]
        if len(set(names)) != len(names):
            raise ValueError("GMX publication sources must identify unique allocations")
        return self

    def publication(self, name):
        for binding in self.publications:
            if binding.allocation == name:
                return binding.source
        raise ValueError("GMX allocation has no explicit signed-report publication source")


class NativeCosts(Model):
    chain: ChainId
    acceptance: AssetAmount
    cancellation: AssetAmount
    revocation: AssetAmount
    claim: AssetAmount

    @model_validator(mode="after")
    def units(self):
        if any(
            value.asset != native_asset(self.chain)
            for value in (self.acceptance, self.cancellation, self.revocation, self.claim)
        ):
            raise ValueError("GMX submitter costs require their exact native chain units")
        return self


class FixedCosts(Model):
    kind: Literal["fixed_native"]
    chains: Annotated[tuple[NativeCosts, ...], Field(min_length=1)]

    @field_validator("chains", mode="before")
    @classmethod
    def arrays(cls, value):
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def unique(self):
        if len({value.chain for value in self.chains}) != len(self.chains):
            raise ValueError("GMX fixed submitter costs require unique chains")
        return self


class KeeperCost(Model):
    chain: ChainId
    payment: AssetAmount

    @model_validator(mode="after")
    def units(self):
        if self.payment.asset != native_asset(self.chain):
            raise ValueError("GMX keeper payment requires explicit native chain units")
        return self


class KeeperPolicy(Model):
    model: Literal["published_opportunity_fixed_native_payment"]
    minimum_latency_seconds: PositiveSeconds
    payments: Annotated[tuple[KeeperCost, ...], Field(min_length=1)]

    @field_validator("payments", mode="before")
    @classmethod
    def arrays(cls, value):
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def unique(self):
        if len({value.chain for value in self.payments}) != len(self.payments):
            raise ValueError("GMX keeper payments require unique chain identities")
        return self


class EconomicConfig(InventoryConfig):
    market_model: Literal["economic"]
    swap_model: Literal["prepared_integer_market"]
    liquidity_model: Literal["fixed_external_gm_valuation"] | None
    position_model: Literal["prepared_position_accounting"] | None
    costs: Annotated[ExcludedCosts | FixedCosts, Field(discriminator="kind")]
    keeper: KeeperPolicy
    data: DataPolicy

    @model_validator(mode="after")
    def execution_scopes(self):
        chains = {fund.fund.chain.chain_id for fund in self.funds}
        if {payment.chain for payment in self.keeper.payments} != chains:
            raise ValueError("GMX keeper payments must cover each configured fund chain")
        if (
            self.costs.kind == "fixed_native"
            and {cost.chain for cost in self.costs.chains} != chains
        ):
            raise ValueError("GMX submitter costs must cover each configured fund chain")
        if not self.payers and any(fund.profile.submitter != fund.fund for fund in self.funds):
            raise ValueError("GMX economic runs require explicit separate submitter inventory")
        return self
