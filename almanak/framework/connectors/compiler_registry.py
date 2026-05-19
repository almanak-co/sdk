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


def get_compiler(protocol: str) -> BaseProtocolCompiler | None:
    """Module-level convenience wrapper."""
    return CompilerRegistry.get(protocol)


__all__ = ["CompilerRegistry", "get_compiler"]
