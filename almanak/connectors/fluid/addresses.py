"""Fluid contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the Fluid entries previously held in
``almanak.framework.intents.compiler_constants.LP_POSITION_MANAGERS``
(VIB-4872 / epic VIB-4851).

Fluid uses a DexFactory entry point — the per-pool addresses are
resolved dynamically at runtime, not stored here. The legacy central
dict surfaced the factory under the ``LP_POSITION_MANAGERS`` slot so
permission discovery / synthetic intents could authorise calls against
it; the same address lives here under the connector-private
``dex_factory`` kind.

The contract-kind vocabulary is connector-private — callers outside
this folder should consume the registry, not guess key names.
"""

from __future__ import annotations

FLUID: dict[str, dict[str, str]] = {
    "arbitrum": {
        # Fluid DexFactory — pools are resolved dynamically against this.
        "dex_factory": "0x91716C4EDA1Fb55e84Bf8b4c7085f84285c19085",
    },
}


__all__ = ["FLUID"]
