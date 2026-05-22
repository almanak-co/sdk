"""Lazy registry for connector-owned intent compilers."""

from __future__ import annotations

import importlib
from typing import ClassVar

from almanak.framework.connectors.base.compiler import BaseProtocolCompiler


class CompilerRegistry:
    """Protocol-name to connector compiler registry."""

    _BUILTIN_LOADERS: ClassVar[dict[str, tuple[str, str]]] = {
        "uniswap_v3": (
            "almanak.framework.connectors.uniswap_v3.compiler",
            "UniswapV3Compiler",
        ),
        "sushiswap_v3": (
            "almanak.framework.connectors.uniswap_v3.compiler",
            "UniswapV3Compiler",
        ),
        "pancakeswap_v3": (
            "almanak.framework.connectors.uniswap_v3.compiler",
            "UniswapV3Compiler",
        ),
        "agni_finance": (
            "almanak.framework.connectors.uniswap_v3.compiler",
            "UniswapV3Compiler",
        ),
        "curve": (
            "almanak.framework.connectors.curve.compiler",
            "CurveCompiler",
        ),
        "fluid": (
            "almanak.framework.connectors.fluid.compiler",
            "FluidCompiler",
        ),
        "camelot": (
            "almanak.framework.connectors.camelot.compiler",
            "CamelotCompiler",
        ),
        "uniswap_v4": (
            "almanak.framework.connectors.uniswap_v4.compiler",
            "UniswapV4Compiler",
        ),
        "traderjoe_v2": (
            "almanak.framework.connectors.traderjoe_v2.compiler",
            "TraderJoeV2Compiler",
        ),
        "aerodrome": (
            "almanak.framework.connectors.aerodrome.compiler",
            "AerodromeCompiler",
        ),
        "aerodrome_slipstream": (
            "almanak.framework.connectors.aerodrome.compiler",
            "AerodromeCompiler",
        ),
        "pendle": (
            "almanak.framework.connectors.pendle.compiler",
            "PendleCompiler",
        ),
        "aave_v3": (
            "almanak.framework.connectors.aave_v3.compiler",
            "AaveV3Compiler",
        ),
        "radiant_v2": (
            "almanak.framework.connectors.radiant_v2.compiler",
            "RadiantV2Compiler",
        ),
        "compound_v3": (
            "almanak.framework.connectors.compound_v3.compiler",
            "CompoundV3Compiler",
        ),
        "morpho": (
            "almanak.framework.connectors.morpho_blue.compiler",
            "MorphoBlueCompiler",
        ),
        "morpho_blue": (
            "almanak.framework.connectors.morpho_blue.compiler",
            "MorphoBlueCompiler",
        ),
        "spark": (
            "almanak.framework.connectors.spark.compiler",
            "SparkCompiler",
        ),
        "silo_v2": (
            "almanak.framework.connectors.silo_v2.compiler",
            "SiloV2Compiler",
        ),
        "euler_v2": (
            "almanak.framework.connectors.euler_v2.compiler",
            "EulerV2Compiler",
        ),
        "benqi": (
            "almanak.framework.connectors.benqi.compiler",
            "BenqiCompiler",
        ),
        "curvance": (
            "almanak.framework.connectors.curvance.compiler",
            "CurvanceCompiler",
        ),
        "jupiter_lend": (
            "almanak.framework.connectors.jupiter_lend.compiler",
            "JupiterLendCompiler",
        ),
        "kamino": (
            "almanak.framework.connectors.kamino.compiler",
            "KaminoCompiler",
        ),
        "gmx_v2": (
            "almanak.framework.connectors.gmx_v2.compiler",
            "GMXV2Compiler",
        ),
        "aster_perps": (
            "almanak.framework.connectors.aster_perps.compiler",
            "AsterPerpsCompiler",
        ),
        "pancakeswap_perps": (
            "almanak.framework.connectors.aster_perps.compiler",
            "AsterPerpsCompiler",
        ),
        "drift": (
            "almanak.framework.connectors.drift.compiler",
            "DriftCompiler",
        ),
        "hyperliquid": (
            "almanak.framework.connectors.hyperliquid.compiler",
            "HyperliquidCompiler",
        ),
    }
    _cache: ClassVar[dict[str, BaseProtocolCompiler]] = {}

    @classmethod
    def get(cls, protocol: str) -> BaseProtocolCompiler | None:
        """Return a compiler instance for ``protocol`` when one is registered."""
        key = protocol.lower().replace("-", "_")
        if key in cls._cache:
            return cls._cache[key]
        loader = cls._BUILTIN_LOADERS.get(key)
        if loader is None:
            return None
        module_path, class_name = loader
        module = importlib.import_module(module_path)
        compiler_cls = getattr(module, class_name)
        compiler = compiler_cls()
        if not isinstance(compiler, BaseProtocolCompiler):
            raise TypeError(f"{module_path}.{class_name} is not a BaseProtocolCompiler")
        cls._cache[key] = compiler
        return compiler

    @classmethod
    def has(cls, protocol: str) -> bool:
        """Return True when ``protocol`` has a connector compiler."""
        return protocol.lower().replace("-", "_") in cls._BUILTIN_LOADERS

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Return all protocol names with connector-owned compilers."""
        return tuple(sorted(cls._BUILTIN_LOADERS))


def get_compiler(protocol: str) -> BaseProtocolCompiler | None:
    """Module-level convenience wrapper."""
    return CompilerRegistry.get(protocol)


def supported_protocols() -> tuple[str, ...]:
    """Module-level convenience wrapper."""
    return CompilerRegistry.supported_protocols()


__all__ = ["CompilerRegistry", "get_compiler", "supported_protocols"]
