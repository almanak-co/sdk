"""Explicit historical provider and inventory bindings shared by connector models."""

from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.collection import CollectionLimits
from almanak.backtest.engine import Schedule
from almanak.backtest.pricing import PriceBinding, VendorBinding
from almanak.backtest.sampling import Sampling, SamplingOverride
from almanak.backtest.strategy_prices import StrategyPriceData
from almanak.backtest.types import Name
from almanak.effects import Balance, WalletAsset
from almanak.gateway.providers import Provider
from almanak.models import Account, AssetAmount, Chain, Model, UInt
from almanak.triggers import PositiveSeconds


class HistoryBinding(Model):
    chain: Chain
    provider: Provider
    collection: CollectionLimits = CollectionLimits()


class Fund(Model):
    name: Name
    fund: Account
    balances: tuple[AssetAmount, ...]

    @model_validator(mode="before")
    @classmethod
    def arrays(cls, value):
        if isinstance(value, dict) and isinstance(value.get("balances"), list):
            return {**value, "balances": tuple(value["balances"])}
        return value

    def inventory(self) -> tuple[Balance, ...]:
        return tuple(
            Balance(
                key=WalletAsset(account=self.name, chain=self.fund.chain, asset=amount.asset),
                amount=amount.amount,
            )
            for amount in self.balances
        )


class ExcludedCosts(Model):
    kind: Literal["excluded"]


class FixedCosts(Model):
    kind: Literal["fixed_native"]
    approve: UInt
    supply: UInt
    withdraw: UInt
    revoke: UInt


class BacktestSettings(Model):
    schedule: Schedule
    history: Annotated[tuple[HistoryBinding, ...], Field(min_length=1)]
    funds: Annotated[tuple[Fund, ...], Field(min_length=1)]

    @model_validator(mode="before")
    @classmethod
    def arrays(cls, value):
        if not isinstance(value, dict):
            return value
        raw = dict(value)
        for field in ("history", "funds"):
            if isinstance(raw.get(field), list):
                raw[field] = tuple(raw[field])
        return raw

    @model_validator(mode="after")
    def unique_scope(self):
        if len({h.chain for h in self.history}) != len(self.history):
            raise ValueError("historical provider bindings must be unique by chain/environment")
        if len({f.name for f in self.funds}) != len(self.funds):
            raise ValueError("historical fund names must be unique")
        if len({f.fund for f in self.funds}) != len(self.funds):
            raise ValueError("historical fund accounts must have distinct bindings")
        return self


class AaveBacktestConfig(BacktestSettings):
    market_model: Literal["historical_path"]
    costs: Annotated[ExcludedCosts | FixedCosts, Field(discriminator="kind")]
    # The strategy's declared series, folded in at resolution; omitted when none.
    data: StrategyPriceData = Field(
        default_factory=StrategyPriceData, exclude_if=lambda d: not d.prices
    )


class EconomicDataPolicy(Model):
    source: Literal["aave_v4_archive"]
    rate_sample_seconds: PositiveSeconds
    risk_prices: Literal["spoke_oracle_events", "configured"]
    prices: tuple[PriceBinding, ...] = ()
    vendors: tuple[VendorBinding, ...] = ()
    sampling_overrides: tuple[SamplingOverride, ...] = ()

    @model_validator(mode="before")
    @classmethod
    def arrays(cls, value):
        if isinstance(value, dict):
            value = dict(value)
            for name in ("sampling_overrides", "prices", "vendors"):
                if isinstance(value.get(name), list):
                    value[name] = tuple(value[name])
        return value

    @model_validator(mode="after")
    def distinct_sampling(self):
        if len({o.key for o in self.sampling_overrides}) != len(self.sampling_overrides):
            raise ValueError("sampling overrides require distinct data keys")
        identities = [(b.purpose, b.chain, b.asset, b.market) for b in self.prices]
        if len(set(identities)) != len(identities):
            raise ValueError("price bindings must have unique purpose and market identities")
        if len({v.name for v in self.vendors}) != len(self.vendors):
            raise ValueError("vendor names must be unique")
        if self.risk_prices == "spoke_oracle_events" and any(
            b.purpose == "risk" for b in self.prices
        ):
            raise ValueError("choose oracle event risk pricing or configured risk bindings")
        return self

    def sampling(self, key):
        for override in self.sampling_overrides:
            if override.key == key:
                return override
        return Sampling(
            sample_seconds=self.rate_sample_seconds, max_age_seconds=self.rate_sample_seconds + 60
        )


class EconomicFixedCosts(Model):
    kind: Literal["fixed_native"]
    supply: UInt
    withdraw: UInt
    borrow: UInt
    repay: UInt
    collateral: UInt
    refresh: UInt


class EconomicBacktestConfig(BacktestSettings):
    market_model: Literal["economic"]
    data: EconomicDataPolicy
    costs: Annotated[ExcludedCosts | EconomicFixedCosts, Field(discriminator="kind")]


BacktestConfig = Annotated[
    AaveBacktestConfig | EconomicBacktestConfig, Field(discriminator="market_model")
]


def load_settings(path: Path, connector="aave_v4", *, author=None):
    if path.suffix in (".yaml", ".yml"):
        raise ValueError("runtime backtest settings must be retained JSON")

    import json

    from almanak.connectors.composition import connector_from_wire

    raw = path.read_text()
    binding = connector_from_wire({"connector": connector}).backtest(
        json.loads(raw).get("market_model")
    )
    return binding.settings_type.model_validate_json(raw)


def financial_policy(data):
    import hashlib

    payload = data.model_dump_json(exclude={"vendors": {"__all__": {"provider": {"collection"}}}})
    return hashlib.sha256(payload.encode()).hexdigest()
