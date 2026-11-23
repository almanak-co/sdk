"""Shared execution inventory and immutable connector views for a composed deployment."""

from functools import cached_property

from pydantic import model_validator

from almanak.configuration.composed import Parameters
from almanak.configuration.gas_parameters import PreparedConfig
from almanak.connectors.aave_v3.models import Config as LendingConfig
from almanak.connectors.aave_v4.models import LendingConfig as AaveV4Config
from almanak.connectors.across.models import Config as AcrossConfig
from almanak.connectors.aerodrome.models import Config as AerodromeConfig
from almanak.connectors.aerodrome_slipstream.models import Config as SlipstreamConfig
from almanak.connectors.compound_v3.models import Config as CompoundConfig
from almanak.connectors.curve.models import Config as CurveConfig
from almanak.connectors.gmx_v2.models import Config as GmxConfig
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue.models import Config as MorphoConfig
from almanak.connectors.pancakeswap_v3.models import Config as PancakeConfig
from almanak.connectors.sparklend.models import Config as SparkConfig
from almanak.connectors.target_routing import allocation_connector
from almanak.connectors.uniswap_v3.models import Config as ExchangeConfig
from almanak.connectors.uniswap_v4.models import Config as V4Config
from almanak.effects import Balance, WalletAsset
from almanak.gateway.environments import ForkBinding, validate_environments
from almanak.gateway.production_config import ProductionExecution


class OwnedParameters(Parameters):
    inventory: tuple[Balance, ...]

    @model_validator(mode="after")
    def inventory_scope(self):
        validate_inventory(self, self.inventory)
        return self


class Config(OwnedParameters, PreparedConfig):
    fork_bindings: tuple[ForkBinding, ...] = ()
    production: ProductionExecution | None = None

    @cached_property
    def execution_components(self):
        return {
            identity: execution_view(self, identity, parameters)
            for identity, parameters in self.parameter_components.items()
        }

    def component(self, identity: ConnectorId):
        if not isinstance(identity, ConnectorId):
            raise ValueError("component selection requires a validated connector identity")
        try:
            return self.execution_components[identity]
        except KeyError:
            raise ValueError("connector has no configured allocations") from None

    @model_validator(mode="after")
    def execution_scope(self):
        validate_environments(self)
        return self


def execution_view(config, identity, parameters):
    classes: dict[
        ConnectorId,
        type[LendingConfig]
        | type[AaveV4Config]
        | type[SparkConfig]
        | type[ExchangeConfig]
        | type[PancakeConfig]
        | type[AerodromeConfig]
        | type[SlipstreamConfig]
        | type[V4Config]
        | type[MorphoConfig]
        | type[CompoundConfig]
        | type[CurveConfig]
        | type[GmxConfig]
        | type[AcrossConfig],
    ] = {
        ConnectorId("aave_v3"): LendingConfig,
        ConnectorId("aave_v4"): AaveV4Config,
        ConnectorId("sparklend"): SparkConfig,
        ConnectorId("uniswap_v3"): ExchangeConfig,
        ConnectorId("pancakeswap_v3"): PancakeConfig,
        ConnectorId("aerodrome"): AerodromeConfig,
        ConnectorId("aerodrome_slipstream"): SlipstreamConfig,
        ConnectorId("uniswap_v4"): V4Config,
        ConnectorId("morpho_blue"): MorphoConfig,
        ConnectorId("compound_v3"): CompoundConfig,
        ConnectorId("curve"): CurveConfig,
        ConnectorId("gmx_v2"): GmxConfig,
        ConnectorId("across"): AcrossConfig,
    }
    cls = classes[identity]
    fields = {name: getattr(parameters, name) for name in type(parameters).model_fields}
    chains = {b.profile.fund.chain for b in parameters.accounts.bindings}
    return cls.model_construct(
        **fields,
        fork_bindings=tuple(b for b in config.fork_bindings if b.chain in chains),
        fork_gas=config.fork_gas,
        unenforced_gas=config.unenforced_gas,
        production=config.production,
    )


def wallet_key(target, asset):
    return WalletAsset(account=target.account, chain=target.fund.chain, asset=asset)


def target_assets(target):
    """The assets a target holds in its own fund's wallet."""
    from almanak.bridge_intents import BridgeTarget
    from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget as GmxTarget
    from almanak.lending_intents import LendingTarget

    if isinstance(target, GmxTarget):
        return target.accounting_assets
    return (target.asset,) if isinstance(target, (LendingTarget, BridgeTarget)) else target.assets


def arrival_key(target) -> WalletAsset | None:
    """The wallet asset a bridge target's transfers arrive in; None for any other target."""
    from almanak.bridge_intents import arrival_wallet

    return arrival_wallet(target)


def configured_targets(config):
    return tuple(config.target(allocation.name) for allocation in config.positions)


def inventory_keys(config):
    return {
        wallet_key(target, asset)
        for target in configured_targets(config)
        for asset in target_assets(target)
    }


def validate_inventory(config, inventory):
    keys = [balance.key for balance in inventory]
    if len(set(keys)) != len(keys):
        raise ValueError("composed fund inventory cannot duplicate a wallet asset")
    held = inventory_keys(config)
    arrivals = {arrival_key(target) for target in configured_targets(config)} - {None}
    if not arrivals <= held:
        # An arrival is credited to owned inventory, which only an allocation on that
        # chain measures and spends. A bridge towards a wallet asset nothing holds
        # would credit cash no observation covers. Checked before the coverage of the
        # inventory, which would otherwise report it as a surplus entry.
        raise ValueError(
            "a composed bridge must arrive in a wallet asset that an allocation on the "
            "destination account holds"
        )
    if set(keys) != held:
        raise ValueError("composed inventory must explicitly cover every configured fund asset")


def component_for_allocation(config, name):
    return config.component(allocation_connector(config.allocation(name)))


def components_for_account(config, account):
    return tuple(
        (identity, config.component(identity))
        for identity, parameters in config.parameter_components.items()
        if any(a.account == account for a in parameters.positions)
    )
