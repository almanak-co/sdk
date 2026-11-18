"""Typed strategy-hook parameters decoded from retained runtime wire values."""

import json
from functools import lru_cache
from typing import Annotated, Any, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.configuration.strategy_parameters import (
    ParametersField,
    StrategyParameters,
    unset,
)
from almanak.configuration.yaml import ConfigurationError
from almanak.gateway.production_config import GasPolicy
from almanak.models import AssetMetadata, Chain, Model


class AuthorAccount(Model):
    chain: Chain


class AuthorBase(Model):
    """Archived author-harness wire, outside the public resolved parameter contract."""

    connector: (
        Literal[
            "aave_v4",
            "uniswap_v4",
            "aave_v3",
            "uniswap_v3",
            "sparklend",
            "pancakeswap_v3",
            "aerodrome_slipstream",
            "aerodrome",
            "compound_v3",
            "morpho_blue",
            "curve",
            "gmx_v2",
            "hyperliquid",
        ]
        | None
    )
    decision_trigger: Any
    allocations: tuple[Any, ...]
    asset_metadata: dict[AssetType, AssetMetadata]
    author_accounts: dict[str, AuthorAccount]
    execution: dict = Field(default_factory=dict)
    gas: tuple[GasPolicy, ...] = Field(default=(), exclude_if=lambda value: not value)
    parameters: ParametersField = Field(default_factory=StrategyParameters, exclude_if=unset)

    @property
    def forks(self):
        return self.fork_config().forks

    @property
    def production(self):
        from almanak.gateway.production_config import ProductionExecution

        raw = self.execution.get("production")
        return ProductionExecution.model_validate_json(json.dumps(raw)) if raw else None

    def fork_config(self):
        from almanak.connectors.composition import author_configuration_from_json

        fork = self.execution.get("fork")
        if fork is None:
            raise ConfigurationError("execution.fork is required for fork execution")
        wire = self.parameter_wire()
        wire.update(forks=fork["targets"], funds=fork["funds"], operators=fork.get("operators", []))
        return author_configuration_from_json(json.dumps(wire))

    def parameter_wire(self):
        wire = self.model_dump(mode="json", exclude={"author_accounts", "execution"})
        if self.connector == "aave_v4":
            wire.pop("connector")
        return wire

    def allocation(self, name):
        for allocation in self.positions:
            if allocation.name == name:
                return allocation
        raise ValueError(f"unknown allocation: {name}")

    @property
    def positions(self):
        return (*self.allocations, *getattr(self, "debts", ()))

    @model_validator(mode="after")
    def scope(self):
        from almanak.configuration.metadata import verify_native

        verify_native(self)
        names = [a.name for a in self.positions]
        if len(set(names)) != len(names):
            raise ValueError("allocation names must be unique")
        for allocation in self.positions:
            account = self.author_accounts.get(allocation.account)
            if account is None or account.chain != allocation.chain:
                raise ValueError(f"{allocation.name}: account and market chain differ")
        return self


@lru_cache(maxsize=16)
def author_type(connector):
    if connector is None:
        from almanak.configuration.composed import Author

        return Author
    from almanak.connectors.aave_v3.models import Allocation as V3Allocation
    from almanak.connectors.aave_v4.models import Allocation as LendingAllocation
    from almanak.connectors.aave_v4.models import DebtAllocation
    from almanak.connectors.aerodrome.models import Allocation as AerodromeAllocation
    from almanak.connectors.aerodrome_slipstream.models import Allocation as SlipstreamAllocation
    from almanak.connectors.compound_v3.models import Allocation as CompoundAllocation
    from almanak.connectors.curve.models import Allocation as CurveAllocation
    from almanak.connectors.gmx_v2.exchange_models import SwapAllocation as GMXSwapAllocation
    from almanak.connectors.gmx_v2.liquidity_models import (
        LiquidityAllocation as GMXLiquidityAllocation,
    )
    from almanak.connectors.gmx_v2.models import Allocation as GMXAllocation
    from almanak.connectors.hyperliquid.models import Allocation as HyperliquidAllocation
    from almanak.connectors.morpho_blue.models import Allocation as MorphoAllocation
    from almanak.connectors.pancakeswap_v3.models import Allocation as PancakeAllocation
    from almanak.connectors.sparklend.models import Allocation as SparkAllocation
    from almanak.connectors.uniswap_v3.models import Allocation as V3ExchangeAllocation
    from almanak.connectors.uniswap_v4.models import Allocation as ExchangeAllocation
    from almanak.triggers import IntervalTrigger

    class AaveAuthor(AuthorBase):
        connector: Literal["aave_v4"] = "aave_v4"
        decision_trigger: IntervalTrigger
        allocations: Annotated[tuple[LendingAllocation, ...], Field(min_length=1)]
        debts: tuple[DebtAllocation, ...] = ()

    class UniswapAuthor(AuthorBase):
        connector: Literal["uniswap_v4"] = "uniswap_v4"
        decision_trigger: IntervalTrigger
        allocations: Annotated[tuple[ExchangeAllocation, ...], Field(min_length=1)]

    class AaveV3Author(AuthorBase):
        connector: Literal["aave_v3"] = "aave_v3"
        decision_trigger: IntervalTrigger
        allocations: Annotated[tuple[V3Allocation, ...], Field(min_length=1)]

    class UniswapV3Author(AuthorBase):
        connector: Literal["uniswap_v3"] = "uniswap_v3"
        decision_trigger: IntervalTrigger
        allocations: Annotated[tuple[V3ExchangeAllocation, ...], Field(min_length=1)]

    class SparkAuthor(AuthorBase):
        connector: Literal["sparklend"] = "sparklend"
        decision_trigger: IntervalTrigger
        allocations: Annotated[tuple[SparkAllocation, ...], Field(min_length=1)]

    class PancakeAuthor(AuthorBase):
        connector: Literal["pancakeswap_v3"] = "pancakeswap_v3"
        decision_trigger: IntervalTrigger
        allocations: Annotated[tuple[PancakeAllocation, ...], Field(min_length=1)]

    class SlipstreamAuthor(AuthorBase):
        connector: Literal["aerodrome_slipstream"] = "aerodrome_slipstream"
        decision_trigger: IntervalTrigger
        allocations: Annotated[tuple[SlipstreamAllocation, ...], Field(min_length=1)]

    class AerodromeAuthor(AuthorBase):
        connector: Literal["aerodrome"] = "aerodrome"
        decision_trigger: IntervalTrigger
        allocations: Annotated[tuple[AerodromeAllocation, ...], Field(min_length=1)]

    class CompoundAuthor(AuthorBase):
        connector: Literal["compound_v3"] = "compound_v3"
        decision_trigger: IntervalTrigger
        allocations: Annotated[tuple[CompoundAllocation, ...], Field(min_length=1)]

    class MorphoAuthor(AuthorBase):
        connector: Literal["morpho_blue"] = "morpho_blue"
        decision_trigger: IntervalTrigger
        allocations: Annotated[tuple[MorphoAllocation, ...], Field(min_length=1)]

    class CurveAuthor(AuthorBase):
        connector: Literal["curve"] = "curve"
        decision_trigger: IntervalTrigger
        allocations: Annotated[tuple[CurveAllocation, ...], Field(min_length=1)]

    class GMXAuthor(AuthorBase):
        connector: Literal["gmx_v2"] = "gmx_v2"
        decision_trigger: IntervalTrigger
        allocations: Annotated[
            tuple[GMXAllocation | GMXSwapAllocation | GMXLiquidityAllocation, ...],
            Field(min_length=1),
        ]

    class HyperliquidAuthor(AuthorBase):
        connector: Literal["hyperliquid"] = "hyperliquid"
        decision_trigger: IntervalTrigger
        allocations: Annotated[tuple[HyperliquidAllocation, ...], Field(min_length=1)]

    return {
        "gmx_v2": GMXAuthor,
        "hyperliquid": HyperliquidAuthor,
        "curve": CurveAuthor,
        "morpho_blue": MorphoAuthor,
        "compound_v3": CompoundAuthor,
        "aerodrome": AerodromeAuthor,
        "aerodrome_slipstream": SlipstreamAuthor,
        "aave_v4": AaveAuthor,
        "uniswap_v4": UniswapAuthor,
        "aave_v3": AaveV3Author,
        "uniswap_v3": UniswapV3Author,
        "sparklend": SparkAuthor,
        "pancakeswap_v3": PancakeAuthor,
    }[connector]


def parameters_from_wire(raw):
    """Decode the retained hook wire without reading author YAML."""
    return author_type(raw["connector"]).model_validate_json(json.dumps(raw))


def hook_parameters(config):
    """Keep resolved parameters authoritative, excluding the execution envelope."""
    from almanak.configuration.gas_parameters import PreparedConfig
    from almanak.configuration.parameter_types import PARAMETER_TYPES, parameter_tag

    # Archived author fixtures precede public resolution and have no bound accounts.
    if isinstance(config, AuthorBase):
        return validation_parameters(config)
    cls = PARAMETER_TYPES[parameter_tag(config)]
    if not isinstance(config, cls):
        raise ValueError("strategy validation requires resolved connector parameters")
    if not isinstance(config, PreparedConfig):
        return config
    return cls.model_validate({name: getattr(config, name) for name in cls.model_fields})


def validation_parameters(config):
    """Project archived author-harness inputs, outside the resolved public hook contract."""
    if isinstance(config, AuthorBase) or (
        hasattr(config, "author_accounts") and hasattr(config, "execution")
    ):
        return config.model_copy(update={"execution": {}})
    wire = config.model_dump(mode="json")
    accounts = {
        binding.name: {"chain": binding.profile.fund.chain.model_dump(mode="json")}
        for binding in config.accounts.bindings
    }
    raw = {
        "connector": wire.get("connector", "aave_v4"),
        "decision_trigger": wire["decision_trigger"],
        "allocations": wire["allocations"],
        "asset_metadata": wire.get("asset_metadata", {}),
        "author_accounts": accounts,
    }
    for field in ("debts", "inventory", "gas", "parameters"):
        if field in wire:
            raw[field] = wire[field]
    return parameters_from_wire(origin_scope(raw))


def origin_scope(value):
    if isinstance(value, list):
        return [origin_scope(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {key: origin_scope(item) for key, item in value.items()}
    if set(result) == {"chain_id", "environment"} and result["environment"].startswith("anvil:"):
        result["environment"] = "mainnet"
    return result


__all__ = [
    "AuthorAccount",
    "AuthorBase",
    "author_type",
    "hook_parameters",
    "origin_scope",
    "parameters_from_wire",
    "validation_parameters",
]
