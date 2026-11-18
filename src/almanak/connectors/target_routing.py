"""Trusted connector selection follows validated targets rather than strategy identity."""

from functools import cache

from almanak.caip import ChainId
from almanak.connectors.identifiers import ConnectorId


def allocation_connector(allocation) -> ConnectorId:
    from almanak.connectors.aave_v3.models import Allocation as AaveV3
    from almanak.connectors.aave_v4.models import Position as AaveV4
    from almanak.connectors.aerodrome.models import Allocation as Aerodrome
    from almanak.connectors.aerodrome_slipstream.models import Allocation as Slipstream
    from almanak.connectors.compound_v3.models import Allocation as Compound
    from almanak.connectors.curve.models import Allocation as Curve
    from almanak.connectors.gmx_v2.exchange_models import SwapAllocation as GMXSwap
    from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation as GMXLiquidity
    from almanak.connectors.gmx_v2.models import Allocation as GMX
    from almanak.connectors.hyperliquid.models import Allocation as Hyperliquid
    from almanak.connectors.morpho_blue.models import Allocation as Morpho
    from almanak.connectors.pancakeswap_v3.models import Allocation as Pancake
    from almanak.connectors.sparklend.models import Allocation as Spark
    from almanak.connectors.uniswap_v3.models import Allocation as UniswapV3
    from almanak.connectors.uniswap_v4.models import Allocation as UniswapV4

    return trusted_type(
        allocation,
        (
            (AaveV3, "aave_v3"),
            (AaveV4, "aave_v4"),
            (Aerodrome, "aerodrome"),
            (Compound, "compound_v3"),
            (Morpho, "morpho_blue"),
            (Curve, "curve"),
            (GMX, "gmx_v2"),
            (GMXSwap, "gmx_v2"),
            (GMXLiquidity, "gmx_v2"),
            (Hyperliquid, "hyperliquid"),
            (Spark, "sparklend"),
            (Pancake, "pancakeswap_v3"),
            (Slipstream, "aerodrome_slipstream"),
            (UniswapV3, "uniswap_v3"),
            (UniswapV4, "uniswap_v4"),
        ),
    )


def target_connector(target) -> ConnectorId:
    return trusted_type(target, target_bindings())


@cache
def target_bindings():
    from almanak.connectors.aave_v3.domain import Target as AaveV3
    from almanak.connectors.aave_v4.models import SupplyTarget as AaveV4
    from almanak.connectors.aerodrome.models import Target as Aerodrome
    from almanak.connectors.aerodrome_slipstream.models import Target as Slipstream
    from almanak.connectors.compound_v3.domain import Target as Compound
    from almanak.connectors.curve.models import Target as Curve
    from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget as GMXExchange
    from almanak.connectors.gmx_v2.models import Target as GMX
    from almanak.connectors.hyperliquid.models import Target as Hyperliquid
    from almanak.connectors.morpho_blue.domain import Target as Morpho
    from almanak.connectors.pancakeswap_v3.models import Target as Pancake
    from almanak.connectors.sparklend.domain import Target as Spark
    from almanak.connectors.uniswap_v3.models import Target as UniswapV3
    from almanak.connectors.uniswap_v4.models import Target as UniswapV4

    return (
        (AaveV3, "aave_v3"),
        (AaveV4, "aave_v4"),
        (Aerodrome, "aerodrome"),
        (Compound, "compound_v3"),
        (Morpho, "morpho_blue"),
        (Curve, "curve"),
        (GMX, "gmx_v2"),
        (GMXExchange, "gmx_v2"),
        (Hyperliquid, "hyperliquid"),
        (Spark, "sparklend"),
        (Pancake, "pancakeswap_v3"),
        (Slipstream, "aerodrome_slipstream"),
        (UniswapV3, "uniswap_v3"),
        (UniswapV4, "uniswap_v4"),
    )


def trusted_type(value, bindings) -> ConnectorId:
    for cls, identity in bindings:
        if isinstance(value, cls):
            return ConnectorId(identity)
    raise ValueError("no trusted connector binding for the validated target type")


def connector_for_target(target):
    from almanak.connectors.composition import connector_from_wire

    return connector_from_wire({"connector": str(target_connector(target))})


def allocation_scope(allocation) -> tuple[ConnectorId, ChainId]:
    identity = allocation_connector(allocation)
    return identity, allocation.chain.chain_id
