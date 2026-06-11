"""Fluid contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the Fluid entries previously held in
``almanak.framework.intents.compiler_constants.LP_POSITION_MANAGERS``
(VIB-4872 / epic VIB-4851).

Fluid deploys deterministically — the factory and resolver addresses are
identical on every supported chain (verified on-chain per chain at
Phase 0 / Phase 1, VIB-5028 / VIB-5029). Per-pool addresses are resolved
dynamically at runtime via the DexReservesResolver, not stored here.

The contract-kind vocabulary is connector-private — callers outside
this folder should consume the registry, not guess key names.
"""

from __future__ import annotations

_FLUID_CHAIN_ENTRY: dict[str, str] = {
    # Fluid DexFactory — pools are resolved dynamically against this.
    "dex_factory": "0x91716C4EDA1Fb55e84Bf8b4c7085f84285c19085",
    # DexReservesResolver — pool enumeration + estimateSwapIn quotes.
    "dex_reserves_resolver": "0x05Bd8269A20C472b148246De20E6852091BF16Ff",
}

FLUID: dict[str, dict[str, str]] = {
    "arbitrum": dict(_FLUID_CHAIN_ENTRY),
    "base": dict(_FLUID_CHAIN_ENTRY),
    "ethereum": dict(_FLUID_CHAIN_ENTRY),
    "polygon": dict(_FLUID_CHAIN_ENTRY),
}
__all__ = ["FLUID"]
