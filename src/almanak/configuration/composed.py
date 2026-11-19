"""One immutable parameter instance binds heterogeneous markets to named funds."""

from collections.abc import Mapping
from functools import cached_property
from typing import Annotated, Any, Self

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.configuration.gas_parameters import AuthorGas
from almanak.configuration.parameter_codec import AuthorBase
from almanak.connectors.aave_v3.models import Allocation as LendingAllocation
from almanak.connectors.aave_v4.models import Allocation as AaveV4Allocation
from almanak.connectors.aave_v4.models import DebtAllocation as AaveV4Debt
from almanak.connectors.aerodrome.models import Allocation as AerodromeAllocation
from almanak.connectors.aerodrome_slipstream.models import Allocation as SlipstreamAllocation
from almanak.connectors.compound_v3.models import Allocation as CompoundAllocation
from almanak.connectors.curve.models import Allocation as CurveAllocation
from almanak.connectors.gmx_v2.exchange_models import SwapAllocation as GmxSwapAllocation
from almanak.connectors.gmx_v2.liquidity_models import (
    LiquidityAllocation as GmxLiquidityAllocation,
)
from almanak.connectors.gmx_v2.models import Allocation as GmxAllocation
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue.models import Allocation as MorphoAllocation
from almanak.connectors.pancakeswap_v3.models import Allocation as PancakeAllocation
from almanak.connectors.sparklend.models import Allocation as SparkAllocation
from almanak.connectors.target_routing import allocation_connector
from almanak.connectors.uniswap_v3.models import Allocation as ExchangeAllocation
from almanak.connectors.uniswap_v4.models import Allocation as V4Allocation
from almanak.effects import Balance
from almanak.models import AssetMetadata, StrategyAccounts
from almanak.operator_deployment import refuse_adoption
from almanak.triggers import IntervalTrigger

Allocation = (
    LendingAllocation
    | AaveV4Debt
    | AaveV4Allocation
    | SparkAllocation
    | V4Allocation
    | ExchangeAllocation
    | PancakeAllocation
    | AerodromeAllocation
    | SlipstreamAllocation
    | MorphoAllocation
    | CompoundAllocation
    | CurveAllocation
    | GmxAllocation
    | GmxSwapAllocation
    | GmxLiquidityAllocation
)


class Author(AuthorBase):
    connector: None = None
    decision_trigger: IntervalTrigger
    allocations: Annotated[tuple[Allocation, ...], Field(min_length=1)]
    inventory: tuple[Balance, ...] = ()


class Parameters(AuthorGas):
    # Null selects a configuration codec, never a protocol or execution permission.
    connector: None = None
    decision_trigger: IntervalTrigger
    asset_metadata: dict[AssetType, AssetMetadata] = Field(default_factory=dict)
    accounts: StrategyAccounts
    allocations: Annotated[tuple[Allocation, ...], Field(min_length=1)]

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        copied = super().model_copy(update=update, deep=deep)
        for key in ("parameter_components", "execution_components"):
            copied.__dict__.pop(key, None)
        return copied

    @property
    def positions(self):
        return self.allocations

    def allocation(self, name):
        for item in self.allocations:
            if item.name == name:
                return item
        raise ValueError(f"unknown configured allocation: {name}")

    def target(self, name):
        identity = allocation_connector(self.allocation(name))
        return self.component(identity).target(name)

    def component(self, identity: ConnectorId):
        if not isinstance(identity, ConnectorId):
            raise ValueError("component selection requires a validated connector identity")
        try:
            return self.parameter_components[identity]
        except KeyError:
            raise ValueError("connector has no configured allocations") from None

    @cached_property
    def parameter_components(self):
        groups: dict[ConnectorId, list] = {}
        for item in self.allocations:
            groups.setdefault(allocation_connector(item), []).append(item)
        return {
            identity: self.bind_component(identity, tuple(items))
            for identity, items in groups.items()
        }

    def bind_component(self, identity, items):
        from almanak.configuration.parameter_types import (
            component_allocations,
            parameter_type,
        )

        accounts = {a.account for a in items}
        return parameter_type(identity).model_validate(
            {
                "decision_trigger": self.decision_trigger,
                "asset_metadata": self.asset_metadata,
                "accounts": StrategyAccounts(
                    bindings=tuple(b for b in self.accounts.bindings if b.name in accounts)
                ),
                **component_allocations(identity, items),
            }
        )

    @model_validator(mode="after")
    def scope(self):
        names = [item.name for item in self.allocations]
        if len(set(names)) != len(names):
            raise ValueError("allocation names must be unique across connectors")
        if {a.account for a in self.allocations} != {b.name for b in self.accounts.bindings}:
            raise ValueError("configured markets must identify every named fund")
        # A composed configuration is a strategy's or a backtest's, never a direct action's.
        refuse_adoption(self.allocations)
        # These immutable views share the validated allocation objects. They are
        # never separately persisted or passed to additional strategy hooks.
        _ = self.parameter_components
        return self
