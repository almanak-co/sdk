"""Author fork targets and explicit provider/funding choices, without credentials."""

import json
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from almanak.caip import AssetType, ChainId
from almanak.configuration.strategy_parameters import (
    ParametersField,
    StrategyParameters,
    unset,
)
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.evm import chain_number
from almanak.gateway.production_config import ProductionExecution
from almanak.gateway.providers import (
    AlchemyProvider as AlchemyProvider,
)
from almanak.gateway.providers import (
    Provider as Provider,
)
from almanak.gateway.providers import (
    UrlProvider as UrlProvider,
)
from almanak.gateway.providers import (
    provider_endpoint as provider_endpoint,
)
from almanak.gateway.providers import (
    validate_provider_url as validate_provider_url,
)
from almanak.models import AssetAmount, AssetMetadata, Block, Chain, Model, UInt
from almanak.triggers import IntervalTrigger

Name = Annotated[str, Field(min_length=1, pattern=r"^\S+$")]


class ForkTarget(Model):
    chain: Chain
    provider: Provider
    block: Literal["latest"] | Block

    @model_validator(mode="after")
    def scope(self) -> Self:
        chain_number(self.chain.chain_id)
        if isinstance(self.block, Block) and self.block.chain != self.chain:
            raise ValueError("pinned block must identify its target chain/environment")
        return self

    def resolve(self, env_file: Path) -> Block:
        endpoint = provider_endpoint(self.provider, env_file)
        with EthereumReader(self.chain, endpoint) as reader:
            selector = self.block.number if isinstance(self.block, Block) else "latest"
            resolved = reader.pin_block(selector)
        if isinstance(self.block, Block) and resolved != self.block:
            raise ValueError("provider block differs from the pinned origin")
        return resolved


class ForkFund(Model):
    account: Name
    native_balance: AssetAmount
    token_balance: UInt
    funding: Literal["native", "wrap_native"]
    foundry_account_index: Annotated[int, Field(ge=0, lt=2**31)] | None = None

    @model_validator(mode="after")
    def scope(self) -> Self:
        asset = self.native_balance.asset
        if asset.namespace != "slip44":
            raise ValueError("funding must identify a native gas asset")
        if self.funding == "native":
            if self.token_balance != 0:
                raise ValueError("native funding requires zero token_balance")
        else:
            self.validate_wrapping()
        return self

    def validate_wrapping(self):
        if self.native_balance.amount <= self.token_balance:
            raise ValueError("fork native funding must cover wrapping and gas")


class ForkAssetFunding(Model):
    balance: AssetAmount
    # Retained plans and author configs pin this name; "local_storage" selects any ERC-20 route.
    method: Literal["wrap_native", "local_storage"]


class ForkEoaFund(Model):
    account: Name
    native_balance: AssetAmount
    assets: tuple[ForkAssetFunding, ...]
    foundry_account_index: Annotated[int, Field(ge=0, lt=2**31)]


class ForkSafeFund(Model):
    account: Name
    operator: Name
    assets: tuple[ForkAssetFunding, ...]


class ForkOperator(Model):
    name: Name
    native_balance: AssetAmount
    foundry_account_index: Annotated[int, Field(ge=0, lt=2**31)]


class ForkConfigBase[Allocation](Model):
    asset_metadata: dict[AssetType, AssetMetadata] = Field(
        default_factory=dict, exclude_if=lambda v: not v
    )
    decision_trigger: IntervalTrigger
    parameters: ParametersField = Field(default_factory=StrategyParameters, exclude_if=unset)
    production: ProductionExecution | None = Field(default=None, exclude_if=lambda v: v is None)
    forks: Annotated[tuple[ForkTarget, ...], Field(min_length=1)]
    funds: Annotated[tuple[ForkFund | ForkEoaFund | ForkSafeFund, ...], Field(min_length=1)]
    operators: tuple[ForkOperator, ...] = ()
    allocations: Annotated[tuple[Allocation, ...], Field(min_length=1)]

    @model_validator(mode="before")
    @classmethod
    def production_template(cls, value):
        if isinstance(value, dict) and isinstance(value.get("production"), dict):
            return {
                **value,
                "production": ProductionExecution.model_validate_json(
                    json.dumps(value["production"], default=str)
                ),
            }
        return value

    @model_validator(mode="after")
    def common_scope(self) -> Self:
        require_unique([t.chain.chain_id for t in self.forks], "fork target chains")
        require_unique([f.account for f in self.funds], "fork fund names")
        indices = [
            (f.native_balance.asset.chain, f.foundry_account_index)
            for f in self.funds
            if isinstance(f, (ForkFund, ForkEoaFund)) and f.foundry_account_index is not None
        ]
        indices.extend(
            (o.native_balance.asset.chain, o.foundry_account_index) for o in self.operators
        )
        require_unique(indices, "Foundry account indices")
        require_unique([o.name for o in self.operators], "operator names")
        if {f.operator for f in self.funds if isinstance(f, ForkSafeFund)} != {
            o.name for o in self.operators
        }:
            raise ValueError("Safe funds must reference exactly the configured operators")
        self.validate_support()
        return self

    def validate_support(self):
        from almanak.connectors.composition import connector_from_wire
        from almanak.connectors.support import RunMode
        from almanak.connectors.target_routing import allocation_scope

        scopes = {allocation_scope(a) for a in self.allocations}
        if {chain for _, chain in scopes} != {target.chain.chain_id for target in self.forks}:
            raise ValueError("fork targets must match the configured allocation chains")
        for allocation in self.allocations:
            identity, chain = allocation_scope(allocation)
            connector = connector_from_wire({"connector": str(identity)})
            connector.require_target(chain, allocation, RunMode.FORK)
            if self.production is not None:
                connector.require_target(chain, allocation, RunMode.PRODUCTION)
        if self.production is not None:
            if {t.chain for t in self.forks} != {n.chain for n in self.production.networks}:
                raise ValueError("production and fork profiles must cover the same author chains")

    def single_target(self) -> ForkTarget:
        if len(self.forks) != 1:
            raise ValueError(
                "this scalar accessor requires exactly one target; use the fork collection"
            )
        return self.forks[0]

    @property
    def origin(self) -> Block:
        block = self.single_target().block
        if not isinstance(block, Block):
            raise ValueError("resolve latest before preparing a fork deployment")
        return block

    def resolved(self, block: Block) -> Self:
        target = self.single_target()
        resolved = ForkTarget.model_validate({**target.model_dump(), "block": block})
        return type(self).model_validate({**self.model_dump(), "forks": (resolved,)})

    def resolve_all(self, env_file: Path, cache: dict) -> Self:
        targets = []
        for target in self.forks:
            key = target.model_dump_json()
            if key not in cache:
                cache[key] = target.resolve(env_file)
            targets.append(target.model_copy(update={"block": cache[key]}))
        return type(self).model_validate({**self.model_dump(), "forks": tuple(targets)})


def require_unique(values, label):
    if len(values) != len(set(values)):
        raise ValueError(f"{label} must be unique")


def remap_asset(asset: AssetType, chain: ChainId) -> AssetType:
    return AssetType(f"{chain}/{asset.namespace}:{asset.reference}")
