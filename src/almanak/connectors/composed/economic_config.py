"""One run schedule and inventory with explicit connector-owned model policies."""

import json
from typing import Annotated, Literal

from pydantic import Field, ValidationError, ValidationInfo, field_validator, model_validator

from almanak.backtest.config import ExcludedCosts
from almanak.backtest.inventory import InventoryConfig
from almanak.backtest.strategy_prices import StrategyPriceData
from almanak.connectors.aave_v3 import economic_config as lending
from almanak.connectors.across import economic_config as across
from almanak.connectors.aerodrome import economic_config as aerodrome
from almanak.connectors.aerodrome_slipstream import economic_config as slipstream
from almanak.connectors.composed.aave_v4_economic import Policy as AaveV4Policy
from almanak.connectors.composed.aave_v4_economic import Settings as AaveV4Settings
from almanak.connectors.compound_v3 import economic_config as compound
from almanak.connectors.curve import economic_config as curve
from almanak.connectors.gmx_v2 import economic_config as gmx
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue import economic_config as morpho
from almanak.connectors.pancakeswap_v3 import economic_config as pancake
from almanak.connectors.sparklend import economic_config as spark
from almanak.connectors.uniswap_v3 import economic_config as exchange
from almanak.connectors.uniswap_v4 import economic_config as v4
from almanak.models import Model


class PolicyRefused(ValueError):
    """One connector's policy refused, with the path of the refused field inside it."""

    def __init__(self, connector: str, error: ValidationError):
        detail = error.errors()[0]
        self.location = (connector, *(str(part) for part in detail["loc"]))
        self.reason = detail["msg"].removeprefix("Value error, ")
        super().__init__(f"{'.'.join(self.location)}: {self.reason}")


def decode_policy(policy, connector: str, value, wire: bool):
    try:
        return (
            policy.model_validate_json(json.dumps(value)) if wire else policy.model_validate(value)
        )
    except ValidationError as error:
        raise PolicyRefused(connector, error) from error


class LendingPolicy(Model):
    data: lending.DataPolicy
    costs: Annotated[ExcludedCosts | lending.Costs, Field(discriminator="kind")]
    liquidation_model: Literal["immediate_underlying_v3_revision11"]


class SparkPolicy(Model):
    data: spark.DataPolicy
    costs: Annotated[ExcludedCosts | spark.Costs, Field(discriminator="kind")]
    liquidation_model: Literal["immediate_underlying_sparklend_revision4"]


class ExchangePolicy(Model):
    data: exchange.DataPolicy
    costs: Annotated[ExcludedCosts | exchange.Costs, Field(discriminator="kind")]
    swap_model: Literal["prepared_integer_pool"]
    liquidity_model: Literal["fixed_historical_fee_growth"] | None


class PancakePolicy(Model):
    data: pancake.DataPolicy
    costs: Annotated[ExcludedCosts | pancake.Costs, Field(discriminator="kind")]
    swap_model: Literal["prepared_integer_pool"]
    liquidity_model: Literal["fixed_historical_fee_growth"] | None


class AerodromePolicy(Model):
    data: aerodrome.DataPolicy
    costs: Annotated[ExcludedCosts | aerodrome.Costs, Field(discriminator="kind")]
    swap_model: Literal["prepared_integer_pool"]
    liquidity_model: Literal["fixed_external_shares_and_fee_indices"] | None


class SlipstreamPolicy(Model):
    data: slipstream.DataPolicy
    costs: Annotated[ExcludedCosts | slipstream.Costs, Field(discriminator="kind")]
    swap_model: Literal["prepared_integer_pool"]
    liquidity_model: Literal["fixed_historical_fee_growth"] | None


class V4Policy(Model):
    data: v4.PoolDataPolicy
    costs: Annotated[ExcludedCosts | v4.EconomicCosts, Field(discriminator="kind")]
    swap_model: Literal["prepared_integer_pool"]
    liquidity_model: Literal["fixed_historical_fee_growth"] | None


class MorphoPolicy(Model):
    data: morpho.DataPolicy
    costs: Annotated[ExcludedCosts | morpho.Costs, Field(discriminator="kind")]
    liquidation_model: Literal["immediate_morpho_liquidation"]


class CompoundPolicy(Model):
    data: compound.DataPolicy
    costs: Annotated[ExcludedCosts | compound.Costs, Field(discriminator="kind")]
    liquidation_model: Literal["immediate_comet_absorb"]


class CurvePolicy(Model):
    data: curve.DataPolicy
    costs: Annotated[ExcludedCosts | curve.Costs, Field(discriminator="kind")]
    swap_model: Literal["prepared_integer_pool"]
    liquidity_model: Literal["fixed_external_unit_balances"] | None


class GmxPolicy(Model):
    data: gmx.DataPolicy
    costs: Annotated[ExcludedCosts | gmx.FixedCosts, Field(discriminator="kind")]
    keeper: gmx.KeeperPolicy
    swap_model: Literal["prepared_integer_market"]
    liquidity_model: Literal["fixed_external_gm_valuation"] | None
    position_model: Literal["prepared_position_accounting"] | None


class AcrossPolicy(Model):
    # The stated arrival latency; the bridge model reads no history.
    data: across.BridgePolicy
    costs: Annotated[ExcludedCosts | across.Costs, Field(discriminator="kind")]
    bridge_model: across.BridgeModel

    @model_validator(mode="before")
    @classmethod
    def stated(cls, values):
        return across.stated_policy(values)


class EconomicConfig(InventoryConfig):
    market_model: Literal["economic"]
    models: dict[
        ConnectorId,
        LendingPolicy
        | AaveV4Policy
        | SparkPolicy
        | V4Policy
        | ExchangePolicy
        | PancakePolicy
        | AerodromePolicy
        | SlipstreamPolicy
        | MorphoPolicy
        | CompoundPolicy
        | CurvePolicy
        | GmxPolicy
        | AcrossPolicy,
    ]
    # The strategy's declared series, folded in at resolution; absent when it declares none.
    data: StrategyPriceData = Field(
        default_factory=StrategyPriceData,
        exclude_if=lambda value: not value.prices,
    )

    @field_validator("models", mode="before")
    @classmethod
    def decode_policies(cls, values, info: ValidationInfo):
        if not isinstance(values, dict):
            return values
        classes: dict[str, type[Model]] = {
            "aave_v3": LendingPolicy,
            "aave_v4": AaveV4Policy,
            "sparklend": SparkPolicy,
            "uniswap_v3": ExchangePolicy,
            "pancakeswap_v3": PancakePolicy,
            "aerodrome": AerodromePolicy,
            "aerodrome_slipstream": SlipstreamPolicy,
            "uniswap_v4": V4Policy,
            "morpho_blue": MorphoPolicy,
            "compound_v3": CompoundPolicy,
            "curve": CurvePolicy,
            "gmx_v2": GmxPolicy,
            "across": AcrossPolicy,
        }
        # Excluded-cost exchange policies have identical shapes. Connector identity
        # selects their concrete type before the union can erase that distinction.
        decoded = {}
        for key, value in values.items():
            policy = classes.get(str(key))
            if policy is not None:
                value = decode_policy(policy, str(key), value, info.mode == "json")
            decoded[key] = value
        return decoded

    @model_validator(mode="after")
    def policies(self):
        if not self.models:
            raise ValueError("composed backtesting requires explicit connector model policies")
        expected = {
            ConnectorId("aave_v3"): LendingPolicy,
            ConnectorId("aave_v4"): AaveV4Policy,
            ConnectorId("sparklend"): SparkPolicy,
            ConnectorId("uniswap_v3"): ExchangePolicy,
            ConnectorId("pancakeswap_v3"): PancakePolicy,
            ConnectorId("aerodrome"): AerodromePolicy,
            ConnectorId("aerodrome_slipstream"): SlipstreamPolicy,
            ConnectorId("uniswap_v4"): V4Policy,
            ConnectorId("morpho_blue"): MorphoPolicy,
            ConnectorId("compound_v3"): CompoundPolicy,
            ConnectorId("curve"): CurvePolicy,
            ConnectorId("gmx_v2"): GmxPolicy,
            ConnectorId("across"): AcrossPolicy,
        }
        if any(
            type(policy) is not expected.get(identity) for identity, policy in self.models.items()
        ):
            raise ValueError("economic model policy differs from its connector identity")
        if any(
            binding.purpose == "strategy"
            for policy in self.models.values()
            for binding in getattr(policy.data, "prices", ())
        ):
            raise ValueError("strategy price series belong to the composition, not a component")
        return self

    def component(self, identity, parameters):
        policy = self.models[identity]
        accounts = {a.account for a in parameters.positions}
        # A bridge also binds the account that receives it, on another chain.
        accounts |= {
            a.destination_account
            for a in parameters.positions
            if getattr(a, "destination_account", None) is not None
        }
        funds = tuple(f for f in self.funds if f.name in accounts)
        payers = {f.profile.submitter for f in funds if f.profile.submitter != f.fund}
        chains = {a.chain for a in parameters.positions}
        cls = {
            LendingPolicy: lending.EconomicConfig,
            AaveV4Policy: AaveV4Settings,
            SparkPolicy: spark.EconomicConfig,
            ExchangePolicy: exchange.EconomicConfig,
            PancakePolicy: pancake.EconomicConfig,
            AerodromePolicy: aerodrome.EconomicConfig,
            SlipstreamPolicy: slipstream.EconomicConfig,
            V4Policy: v4.EconomicConfig,
            MorphoPolicy: morpho.EconomicConfig,
            CompoundPolicy: compound.EconomicConfig,
            CurvePolicy: curve.EconomicConfig,
            GmxPolicy: gmx.EconomicConfig,
            AcrossPolicy: across.EconomicConfig,
        }[type(policy)]
        return cls(
            schedule=self.schedule,
            history=tuple(h for h in self.history if h.chain in chains),
            funds=funds,
            payers=tuple(p for p in self.payers if p.fund in payers),
            market_model=self.market_model,
            **{name: getattr(policy, name) for name in type(policy).model_fields},
        )
