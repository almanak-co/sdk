"""Shared Fluid implementation (foundation core).

Single source of truth for the Fluid protocol surfaces, consumed by the thin
``fluid`` (swap + fToken lending), ``fluid_dex_lp`` (SmartLending DEX-LP), and
``fluid_vault`` (vault borrow) connector manifests. Underscore-prefixed so it is
never discovered as a connector and is treated as foundation by the
connector-isolation guards: deleting any one Fluid leaf manifest must not strand
this implementation in the others.

The package-level lazy exports below mirror the public API the ``fluid`` leaf
exposes; the implementation lives in the sibling modules (``sdk``, ``adapter``,
``compiler``, ``receipt_parser``, the ``dex_lp_*`` / ``vault_*`` modules,
``addresses``, ``gateway``, …). Lazy access keeps importing a submodule (e.g.
``addresses`` during descriptor discovery) free of registration side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        FluidAdapter,
        FluidConfig,
    )
    from .compiler import FluidCompiler
    from .receipt_parser import FluidReceiptParser
    from .sdk import FluidSDK

__all__ = [
    "FluidAdapter",
    "FluidCompiler",
    "FluidConfig",
    "FluidReceiptParser",
    "FluidSDK",
]

_LAZY: dict[str, tuple[str, str]] = {
    "FluidAdapter": (".adapter", "FluidAdapter"),
    "FluidCompiler": (".compiler", "FluidCompiler"),
    "FluidConfig": (".adapter", "FluidConfig"),
    "FluidReceiptParser": (".receipt_parser", "FluidReceiptParser"),
    "FluidSDK": (".sdk", "FluidSDK"),
}


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    return value
