"""The explicit SDK composition root, selected only from validated connector types."""

import json

from almanak.connectors.identifiers import ConnectorId


def installed_connectors():
    """Enumerate the same trusted bindings used for wire dispatch."""
    return tuple((key, connector_from_wire({"connector": str(key)})) for key in CONNECTOR_IDS)


CONNECTOR_IDS = (
    ConnectorId("aave_v4"),
    ConnectorId("uniswap_v4"),
    ConnectorId("aave_v3"),
    ConnectorId("uniswap_v3"),
    ConnectorId("sparklend"),
    ConnectorId("pancakeswap_v3"),
    ConnectorId("aerodrome_slipstream"),
    ConnectorId("aerodrome"),
    ConnectorId("compound_v3"),
    ConnectorId("morpho_blue"),
    ConnectorId("curve"),
    ConnectorId("gmx_v2"),
    ConnectorId("hyperliquid"),
    ConnectorId("across"),
)


def connector_for(config):
    from almanak.configuration.composed import Parameters as ComposedParameters
    from almanak.configuration.parameter_codec import AuthorBase
    from almanak.connectors.composed.fork import ForkConfig as ComposedForkConfig

    if isinstance(config, (ComposedParameters, ComposedForkConfig)):
        from almanak.connectors.composed.connector import connector as composed

        return composed()

    if isinstance(config, AuthorBase):
        return connector_from_wire({"connector": config.connector})

    from almanak.connectors.aave_v3.fork import ForkConfig as AaveV3ForkConfig
    from almanak.connectors.aave_v3.models import Parameters as AaveV3Parameters
    from almanak.connectors.aave_v4.fork import ForkConfig as AaveForkConfig
    from almanak.connectors.aave_v4.models import LendingParameters
    from almanak.connectors.aerodrome.fork import ForkConfig as AerodromeForkConfig
    from almanak.connectors.aerodrome.models import Parameters as AerodromeParameters
    from almanak.connectors.aerodrome_slipstream.fork import ForkConfig as SlipstreamForkConfig
    from almanak.connectors.aerodrome_slipstream.models import Parameters as SlipstreamParameters
    from almanak.connectors.compound_v3.fork import ForkConfig as CompoundForkConfig
    from almanak.connectors.compound_v3.models import Parameters as CompoundParameters
    from almanak.connectors.curve.fork import ForkConfig as CurveForkConfig
    from almanak.connectors.curve.models import Parameters as CurveParameters
    from almanak.connectors.gmx_v2.fork import ForkConfig as GMXForkConfig
    from almanak.connectors.gmx_v2.models import Parameters as GMXParameters
    from almanak.connectors.hyperliquid.fork import ForkConfig as HyperliquidForkConfig
    from almanak.connectors.hyperliquid.models import Parameters as HyperliquidParameters
    from almanak.connectors.morpho_blue.fork import ForkConfig as MorphoForkConfig
    from almanak.connectors.morpho_blue.models import Parameters as MorphoParameters
    from almanak.connectors.pancakeswap_v3.fork import ForkConfig as PancakeForkConfig
    from almanak.connectors.pancakeswap_v3.models import Parameters as PancakeParameters
    from almanak.connectors.sparklend.fork import ForkConfig as SparkForkConfig
    from almanak.connectors.sparklend.models import Parameters as SparkParameters
    from almanak.connectors.uniswap_v3.fork import ForkConfig as UniswapV3ForkConfig
    from almanak.connectors.uniswap_v3.models import Parameters as UniswapV3Parameters
    from almanak.connectors.uniswap_v4.fork import ForkConfig as UniswapForkConfig
    from almanak.connectors.uniswap_v4.models import Parameters

    if isinstance(config, (GMXParameters, GMXForkConfig)):
        from almanak.connectors.gmx_v2.connector import connector as gmx

        return gmx()

    if isinstance(config, (HyperliquidParameters, HyperliquidForkConfig)):
        from almanak.connectors.hyperliquid.connector import connector as hyperliquid

        return hyperliquid()

    from almanak.connectors.across.fork import ForkConfig as AcrossForkConfig
    from almanak.connectors.across.models import Parameters as AcrossParameters

    if isinstance(config, (AcrossParameters, AcrossForkConfig)):
        from almanak.connectors.across.connector import connector as across

        return across()

    if isinstance(config, (CurveParameters, CurveForkConfig)):
        from almanak.connectors.curve.connector import connector as curve

        return curve()

    if isinstance(config, (MorphoParameters, MorphoForkConfig)):
        from almanak.connectors.morpho_blue.connector import connector as morpho

        return morpho()

    if isinstance(config, (CompoundParameters, CompoundForkConfig)):
        from almanak.connectors.compound_v3.connector import connector as compound

        return compound()

    if isinstance(config, (AerodromeParameters, AerodromeForkConfig)):
        from almanak.connectors.aerodrome.connector import connector as aerodrome

        return aerodrome()

    if isinstance(config, (SlipstreamParameters, SlipstreamForkConfig)):
        from almanak.connectors.aerodrome_slipstream.connector import connector as slipstream

        return slipstream()

    if isinstance(config, (PancakeParameters, PancakeForkConfig)):
        from almanak.connectors.pancakeswap_v3.connector import connector as pancake

        return pancake()

    if isinstance(config, (SparkParameters, SparkForkConfig)):
        from almanak.connectors.sparklend.connector import connector as sparklend

        return sparklend()

    if isinstance(config, (UniswapV3Parameters, UniswapV3ForkConfig)):
        from almanak.connectors.uniswap_v3.connector import connector as uniswap_v3

        return uniswap_v3()

    if isinstance(config, (AaveV3Parameters, AaveV3ForkConfig)):
        from almanak.connectors.aave_v3.connector import connector as aave_v3

        return aave_v3()

    if isinstance(config, (LendingParameters, AaveForkConfig)):
        from almanak.connectors.aave_v4.connector import connector as aave

        return aave()
    if isinstance(config, (Parameters, UniswapForkConfig)):
        from almanak.connectors.uniswap_v4.connector import connector as uniswap

        return uniswap()
    raise ValueError("no trusted connector binding for the supplied configuration")


def connector_from_wire(config):
    kind = config.get("connector", "aave_v4")
    if kind is None:
        from almanak.connectors.composed.connector import connector as composed

        return composed()
    if kind not in tuple(map(str, CONNECTOR_IDS)):
        raise ValueError("unsupported connector discriminator")
    if kind == "across":
        from almanak.connectors.across.connector import connector as across

        return across()
    if kind == "gmx_v2":
        from almanak.connectors.gmx_v2.connector import connector as gmx

        return gmx()
    if kind == "hyperliquid":
        from almanak.connectors.hyperliquid.connector import connector as hyperliquid

        return hyperliquid()
    if kind == "curve":
        from almanak.connectors.curve.connector import connector as curve

        return curve()
    if kind == "morpho_blue":
        from almanak.connectors.morpho_blue.connector import connector as morpho

        return morpho()
    if kind == "compound_v3":
        from almanak.connectors.compound_v3.connector import connector as compound

        return compound()
    if kind == "aave_v4":
        from almanak.connectors.aave_v4.connector import connector as aave

        return aave()
    if kind == "aerodrome":
        from almanak.connectors.aerodrome.connector import connector as aerodrome

        return aerodrome()
    if kind == "uniswap_v4":
        from almanak.connectors.uniswap_v4.connector import connector as uniswap

        return uniswap()
    if kind == "aave_v3":
        from almanak.connectors.aave_v3.connector import connector as aave_v3

        return aave_v3()
    if kind == "uniswap_v3":
        from almanak.connectors.uniswap_v3.connector import connector as uniswap_v3

        return uniswap_v3()
    if kind == "aerodrome_slipstream":
        from almanak.connectors.aerodrome_slipstream.connector import connector as slipstream

        return slipstream()
    if kind == "pancakeswap_v3":
        from almanak.connectors.pancakeswap_v3.connector import connector as pancake

        return pancake()
    if kind == "sparklend":
        from almanak.connectors.sparklend.connector import connector as sparklend

        return sparklend()
    raise ValueError("unsupported connector discriminator")


def configuration_from_json(value):
    return connector_from_wire(json.loads(value)).codec.config.model_validate_json(value)


def author_configuration_from_json(value):
    wire = json.loads(value)
    if "production" in wire and "forks" not in wire:
        return configuration_from_json(value)
    kind = wire.get("connector", "aave_v4")
    if kind == "gmx_v2":
        from almanak.connectors.gmx_v2.fork import ForkConfig as GMXForkConfig

        return GMXForkConfig.model_validate_json(value)
    if kind == "hyperliquid":
        from almanak.connectors.hyperliquid.fork import ForkConfig as HyperliquidForkConfig

        return HyperliquidForkConfig.model_validate_json(value)
    if kind == "curve":
        from almanak.connectors.curve.fork import ForkConfig as CurveForkConfig

        return CurveForkConfig.model_validate_json(value)
    if kind == "morpho_blue":
        from almanak.connectors.morpho_blue.fork import ForkConfig as MorphoForkConfig

        return MorphoForkConfig.model_validate_json(value)
    if kind == "compound_v3":
        from almanak.connectors.compound_v3.fork import ForkConfig as CompoundForkConfig

        return CompoundForkConfig.model_validate_json(value)
    if kind == "across":
        from almanak.connectors.across.fork import ForkConfig as AcrossForkConfig

        return AcrossForkConfig.model_validate_json(value)
    if kind == "aerodrome":
        from almanak.connectors.aerodrome.fork import ForkConfig as AerodromeForkConfig

        return AerodromeForkConfig.model_validate_json(value)
    if kind is None:
        from almanak.connectors.composed.fork import ForkConfig as ComposedForkConfig

        return ComposedForkConfig.model_validate_json(value)
    if kind == "aave_v4":
        from almanak.connectors.aave_v4.fork import ForkConfig as AaveForkConfig

        return AaveForkConfig.model_validate_json(value)
    if kind == "uniswap_v4":
        from almanak.connectors.uniswap_v4.fork import ForkConfig as UniswapForkConfig

        return UniswapForkConfig.model_validate_json(value)
    if kind == "aave_v3":
        from almanak.connectors.aave_v3.fork import ForkConfig as AaveV3ForkConfig

        return AaveV3ForkConfig.model_validate_json(value)
    if kind == "uniswap_v3":
        from almanak.connectors.uniswap_v3.fork import ForkConfig as UniswapV3ForkConfig

        return UniswapV3ForkConfig.model_validate_json(value)
    if kind == "aerodrome_slipstream":
        from almanak.connectors.aerodrome_slipstream.fork import (
            ForkConfig as SlipstreamForkConfig,
        )

        return SlipstreamForkConfig.model_validate_json(value)
    if kind == "pancakeswap_v3":
        from almanak.connectors.pancakeswap_v3.fork import ForkConfig as PancakeForkConfig

        return PancakeForkConfig.model_validate_json(value)
    if kind == "sparklend":
        from almanak.connectors.sparklend.fork import ForkConfig as SparkForkConfig

        return SparkForkConfig.model_validate_json(value)
    raise ValueError("unsupported author connector discriminator")


def checkpoint_from_json(value):
    wire = json.loads(value)
    codec = connector_from_wire(wire["config"]).codec
    if wire.get("schema_version") in codec.legacy_schemas:
        connector = wire["config"].get("connector", "composed")
        raise ValueError(
            f"historical checkpoint is audit-only; execution requires its current codec "
            f"({connector} schema version {wire['schema_version']} is no longer restored)"
        )
    from almanak.connectors.across.settlement_types import refuse_earlier_pages

    refuse_earlier_pages(wire.get("external_scans", ()))
    return codec.checkpoint.model_validate_json(value)


def legacy_checkpoint_info(payload):
    schemas = connector_from_wire(payload.get("config", {})).codec.legacy_schemas
    if payload.get("schema_version") not in schemas:
        return None
    return {
        "audit_only": True,
        "caip_compliance": schemas[payload["schema_version"]],
        "legacy_checkpoint": payload,
    }


def historical_manifest_from_json(value):
    wire = json.loads(value)
    adapter = connector_from_wire(wire["config"]).backtest("historical_path")
    return adapter.manifest_type.model_validate_json(value)
