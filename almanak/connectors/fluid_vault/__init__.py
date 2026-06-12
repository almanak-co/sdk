"""Fluid vault (NFT-CDP) connector — thin second manifest over the fluid package.

Phase 3 (VIB-5031, ADR r2 Q0): Fluid vault borrow positions are NFT-CDPs
driven by a single signed-delta ``operate()`` entrypoint per vault, with
``market_id`` (the vault address) REQUIRED — while the shipped Phase-2
fToken surface (``protocol="fluid"`` / ``"fluid_lending"``) REJECTS any
``market_id``. One protocol key cannot demand and forbid ``market_id``
simultaneously, so vault lending is its own connector registration:
``protocol="fluid_vault"``, one codebase (all implementation modules live
in ``almanak.connectors.fluid``), two manifests.

Scope (Checkpoint-1): arbitrum + base, type-1 vaults only. Position keys:
``lending:{chain}:fluid_vault:{wallet}:{vault}:{asset}`` (vault lowercased;
the nftId is metadata in ``extracted_data_json``, never a key segment).

Example:
    from decimal import Decimal

    from almanak.framework.intents import BorrowIntent

    intent = BorrowIntent(
        protocol="fluid_vault",
        market_id="0xeAbBfca72F8a8bf14C4ac59e69ECB2eB69F0811C",  # arbitrum vault id 1
        collateral_token="ETH",
        collateral_amount=Decimal("1"),
        borrow_token="USDC",
        borrow_amount=Decimal("500"),
        chain="arbitrum",
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.connectors.fluid.vault_compiler import FluidVaultCompiler
    from almanak.connectors.fluid.vault_sdk import FluidVaultSDK

__all__ = [
    "FluidVaultCompiler",
    "FluidVaultSDK",
]

_LAZY: dict[str, tuple[str, str]] = {
    "FluidVaultCompiler": ("almanak.connectors.fluid.vault_compiler", "FluidVaultCompiler"),
    "FluidVaultSDK": ("almanak.connectors.fluid.vault_sdk", "FluidVaultSDK"),
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
