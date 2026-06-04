"""Lazy registry for connector-owned intent compilers."""

from __future__ import annotations

import importlib
from typing import Any, ClassVar

from almanak.connectors._strategy_base.base.compiler import BaseProtocolCompiler


class CompilerRegistry:
    """Protocol-name to connector compiler registry."""

    # Connector-name defaults for dispatch keys whose protocol isn't carried on
    # the intent itself. Keeps the strings connector-adjacent so framework code
    # (``intents/compiler.py``) doesn't hardcode protocol names. Add a new key
    # when a new dispatch fallback emerges; remove a key when the underlying
    # decision moves onto the intent vocabulary.
    _DEFAULT_BY_KEY: ClassVar[dict[str, str]] = {
        # BridgeIntent.preferred_bridge=None falls back to this.
        "BRIDGE": "across",
        # SwapIntent.protocol=None on a cross-chain swap falls back to this.
        "SWAP_CROSS_CHAIN": "enso",
        # Prediction intents (PredictionBuy/Sell/Redeem) with protocol=None fall
        # back to this (VIB-4989: relocated from the intent-vocabulary defaults).
        "PREDICTION": "polymarket",
    }

    _BUILTIN_LOADERS: ClassVar[dict[str, tuple[str, str]]] = {
        "uniswap_v3": (
            "almanak.connectors.uniswap_v3.compiler",
            "UniswapV3Compiler",
        ),
        "sushiswap_v3": (
            "almanak.connectors.uniswap_v3.compiler",
            "UniswapV3Compiler",
        ),
        "pancakeswap_v3": (
            "almanak.connectors.uniswap_v3.compiler",
            "UniswapV3Compiler",
        ),
        "agni_finance": (
            "almanak.connectors.uniswap_v3.compiler",
            "UniswapV3Compiler",
        ),
        "curve": (
            "almanak.connectors.curve.compiler",
            "CurveCompiler",
        ),
        "fluid": (
            "almanak.connectors.fluid.compiler",
            "FluidCompiler",
        ),
        "camelot": (
            "almanak.connectors.camelot.compiler",
            "CamelotCompiler",
        ),
        "uniswap_v4": (
            "almanak.connectors.uniswap_v4.compiler",
            "UniswapV4Compiler",
        ),
        "traderjoe_v2": (
            "almanak.connectors.traderjoe_v2.compiler",
            "TraderJoeV2Compiler",
        ),
        "aerodrome": (
            "almanak.connectors.aerodrome.compiler",
            "AerodromeCompiler",
        ),
        "aerodrome_slipstream": (
            "almanak.connectors.aerodrome.compiler",
            "AerodromeCompiler",
        ),
        "pendle": (
            "almanak.connectors.pendle.compiler",
            "PendleCompiler",
        ),
        "lido": (
            "almanak.connectors.lido.compiler",
            "LidoCompiler",
        ),
        "ethena": (
            "almanak.connectors.ethena.compiler",
            "EthenaCompiler",
        ),
        "gimo": (
            "almanak.connectors.gimo.compiler",
            "GimoCompiler",
        ),
        "aave_v3": (
            "almanak.connectors.aave_v3.compiler",
            "AaveV3Compiler",
        ),
        "compound_v3": (
            "almanak.connectors.compound_v3.compiler",
            "CompoundV3Compiler",
        ),
        "morpho": (
            "almanak.connectors.morpho_blue.compiler",
            "MorphoBlueCompiler",
        ),
        "morpho_blue": (
            "almanak.connectors.morpho_blue.compiler",
            "MorphoBlueCompiler",
        ),
        "spark": (
            "almanak.connectors.spark.compiler",
            "SparkCompiler",
        ),
        "silo_v2": (
            "almanak.connectors.silo_v2.compiler",
            "SiloV2Compiler",
        ),
        "euler_v2": (
            "almanak.connectors.euler_v2.compiler",
            "EulerV2Compiler",
        ),
        "benqi": (
            "almanak.connectors.benqi.compiler",
            "BenqiCompiler",
        ),
        "curvance": (
            "almanak.connectors.curvance.compiler",
            "CurvanceCompiler",
        ),
        "jupiter_lend": (
            "almanak.connectors.jupiter_lend.compiler",
            "JupiterLendCompiler",
        ),
        "jupiter": (
            "almanak.connectors.jupiter.compiler",
            "JupiterCompiler",
        ),
        "kamino": (
            "almanak.connectors.kamino.compiler",
            "KaminoCompiler",
        ),
        "gmx_v2": (
            "almanak.connectors.gmx_v2.compiler",
            "GMXV2Compiler",
        ),
        "aster_perps": (
            "almanak.connectors.aster_perps.compiler",
            "AsterPerpsCompiler",
        ),
        "pancakeswap_perps": (
            "almanak.connectors.aster_perps.compiler",
            "AsterPerpsCompiler",
        ),
        "drift": (
            "almanak.connectors.drift.compiler",
            "DriftCompiler",
        ),
        "hyperliquid": (
            "almanak.connectors.hyperliquid.compiler",
            "HyperliquidCompiler",
        ),
        "enso": (
            "almanak.connectors.enso.compiler",
            "EnsoCompiler",
        ),
        "lifi": (
            "almanak.connectors.lifi.compiler",
            "LiFiCompiler",
        ),
        "across": (
            "almanak.connectors._strategy_base.bridge_compiler",
            "BridgeCompiler",
        ),
        "stargate": (
            "almanak.connectors._strategy_base.bridge_compiler",
            "BridgeCompiler",
        ),
        "meteora_dlmm": (
            "almanak.connectors.meteora.compiler",
            "MeteoraCompiler",
        ),
        "orca_whirlpools": (
            "almanak.connectors.orca.compiler",
            "OrcaCompiler",
        ),
        "raydium_clmm": (
            "almanak.connectors.raydium.compiler",
            "RaydiumCompiler",
        ),
        "metamorpho": (
            "almanak.connectors.morpho_vault.compiler",
            "MorphoVaultCompiler",
        ),
        "morpho_vault": (
            "almanak.connectors.morpho_vault.compiler",
            "MorphoVaultCompiler",
        ),
        "polymarket": (
            "almanak.connectors.polymarket.compiler",
            "PolymarketCompiler",
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

    @classmethod
    def _load_class(cls, key: str) -> type[BaseProtocolCompiler] | None:
        """Import a connector compiler class without instantiating it."""
        loader = cls._BUILTIN_LOADERS.get(key)
        if loader is None:
            return None
        module_path, class_name = loader
        module = importlib.import_module(module_path)
        compiler_cls = getattr(module, class_name)
        if not isinstance(compiler_cls, type) or not issubclass(compiler_cls, BaseProtocolCompiler):
            raise TypeError(f"{module_path}.{class_name} is not a BaseProtocolCompiler class")
        return compiler_cls

    @classmethod
    def protocols_for_intent(cls, intent_type: Any) -> tuple[str, ...]:
        """Return loader-key protocol names whose connector declares ``intent_type``.

        Backs error-message hints in framework code ("Supported: ...") so
        per-intent lists don't have to be hand-maintained in
        ``intents/compiler.py``.
        """
        out: list[str] = []
        for key in sorted(cls._BUILTIN_LOADERS):
            compiler_cls = cls._load_class(key)
            if compiler_cls is None:
                continue
            if intent_type in compiler_cls.intents:
                out.append(key)
        return tuple(out)

    @classmethod
    def default_protocol(cls, dispatch_key: str) -> str | None:
        """Return the configured fallback protocol for a dispatch key, or None."""
        return cls._DEFAULT_BY_KEY.get(dispatch_key)


def get_compiler(protocol: str) -> BaseProtocolCompiler | None:
    """Module-level convenience wrapper."""
    return CompilerRegistry.get(protocol)


def supported_protocols() -> tuple[str, ...]:
    """Module-level convenience wrapper."""
    return CompilerRegistry.supported_protocols()


__all__ = ["CompilerRegistry", "get_compiler", "supported_protocols"]
