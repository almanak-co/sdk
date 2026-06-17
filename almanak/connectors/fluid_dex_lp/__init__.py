"""Fluid DEX LP (SmartLending) — thin third manifest over the fluid package.

Phase 4 (VIB-5032): Fluid SmartLending wrappers are fungible ERC-20-share,
two-token DEX-LP positions (no NFT, no tick range). Direct pool LP is
whitelist-gated (``DexT1__UserSupplyInNotOn`` 51013, Phase-0 §V4) — the wrapper
IS the whitelisted supplier, so an EOA/Safe LPs through it. Valued resolver-side
(``SmartLendingResolver.getSmartLendingEntireData`` → per-share token0/token1).

Distinct protocol key (``fluid_dex_lp``) keeps LP accounting keys
(``lp:fluid_dex_lp:{chain}:{wallet}:{wrapper}``) separate from the fToken
lending (``fluid``) and vault borrow (``fluid_vault``) surfaces. One codebase
(all implementation in ``almanak.connectors.fluid``), three manifests.

Example:
    from decimal import Decimal

    from almanak.framework.intents import LPOpenIntent

    intent = LPOpenIntent(
        protocol="fluid_dex_lp",
        pool="0x1F0bFd9862ae58208d26db0d80797974434EC013",  # arbitrum fSL9 sUSDai/USDC
        amount0=Decimal("0"),       # token0 (sUSDai)
        amount1=Decimal("2000"),    # token1 (USDC) — single-sided OK
        range_lower=Decimal("0.5"),  # dummy positive bounds (fungible: no range)
        range_upper=Decimal("2"),
        chain="arbitrum",
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.connectors._fluid_core.dex_lp_compiler import FluidDexLpCompiler
    from almanak.connectors._fluid_core.smart_lending_sdk import FluidSmartLendingSDK

__all__ = [
    "FluidDexLpCompiler",
    "FluidSmartLendingSDK",
]

_LAZY: dict[str, tuple[str, str]] = {
    "FluidDexLpCompiler": ("almanak.connectors._fluid_core.dex_lp_compiler", "FluidDexLpCompiler"),
    "FluidSmartLendingSDK": ("almanak.connectors._fluid_core.smart_lending_sdk", "FluidSmartLendingSDK"),
}


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access (no registration side effects here)."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_path, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(module_path)
    value = getattr(module, attr)
    globals()[name] = value
    return value
