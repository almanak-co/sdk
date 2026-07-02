"""Single source of truth for per-chain configuration.

This package replaces the ~8 chain-keyed dicts that were previously
scattered across ``almanak/core/enums.py``, ``almanak/core/constants.py``,
``almanak/gateway/validation.py``,
``almanak/framework/execution/gas/constants.py``,
``almanak/gateway/services/onchain_lookup.py``, and others.

Per-chain descriptor files (``ethereum.py``, ``arbitrum.py``, …) register
themselves into :class:`ChainRegistry` at import time. Importing this
package **auto-discovers** every descriptor module in the directory, so
adding a chain is creating ONE file — there is no hand-maintained import
list. Discovery validates that each module defines a registered
``DESCRIPTOR`` whose canonical ``name`` matches the module stem; support
modules (``caip``, ``defaults``) and ``_``-prefixed infrastructure modules
are excluded.

VIB-4801 (parent epic VIB-4800).

Public API::

    from almanak.core.chains import (
        ChainDescriptor, ChainRegistry, GasProfile,
        NativeToken, Timeouts, register_chain,
    )
"""

from __future__ import annotations

import importlib
import pkgutil

# Public types and the registry singleton must be importable BEFORE we
# trigger any per-chain registration, because the chain modules import
# ``ChainDescriptor`` etc. from these private submodules.
from ._descriptor import (
    ChainDescriptor,
    GasProfile,
    NativeToken,
    RpcProfile,
    SimulationProfile,
    Timeouts,
)
from ._registry import ChainRegistry, register_chain

# Modules in this package that are not per-chain descriptor files.
_SUPPORT_MODULES = frozenset({"__init__", "caip", "defaults"})


def _is_descriptor_module(stem: str) -> bool:
    return not stem.startswith("_") and stem not in _SUPPORT_MODULES


def _import_descriptor_modules() -> None:
    """Import every descriptor module (side effect: ``register_chain``).

    The file IS the registration: a misnamed, unregistered, or malformed
    descriptor module fails loudly here at import time instead of silently
    dropping a chain from the registry.
    """
    discovered = 0
    for module_info in pkgutil.iter_modules(__path__):
        stem = module_info.name
        if not _is_descriptor_module(stem):
            continue
        module = importlib.import_module(f"{__name__}.{stem}")
        descriptor = getattr(module, "DESCRIPTOR", None)
        if not isinstance(descriptor, ChainDescriptor):
            raise RuntimeError(
                f"Chain descriptor module {module.__name__} must define a "
                "module-level DESCRIPTOR = register_chain(ChainDescriptor(...))"
            )
        if descriptor.name != stem:
            raise RuntimeError(
                f"Chain descriptor module {module.__name__} declares "
                f"name={descriptor.name!r}; expected {stem!r} (module stem and "
                "canonical chain name must match)"
            )
        if ChainRegistry.try_resolve(stem) is not descriptor:
            raise RuntimeError(
                f"Chain descriptor module {module.__name__} defines DESCRIPTOR "
                "but did not register it — wrap it in register_chain(...)"
            )
        discovered += 1

    if discovered == 0:
        raise RuntimeError(
            "Chain descriptor discovery registered zero chains — the "
            "almanak.core.chains package is broken or mispackaged."
        )


_import_descriptor_modules()

# Completeness is owned by the frozen inventory in
# tests/unit/core/test_chain_identity_freeze.py and the file<->registry
# bijection in tests/unit/core/test_chain_discovery.py (the legacy Chain
# enum cross-check was removed with the enum — VIB-4851).
from .caip import parse_caip2, to_caip2  # noqa: E402
from .defaults import DEFAULT_CHAIN, DEFAULT_VAULT_CHAIN, LEGACY_SERIALIZED_CHAIN  # noqa: E402

__all__ = [
    "DEFAULT_CHAIN",
    "DEFAULT_VAULT_CHAIN",
    "LEGACY_SERIALIZED_CHAIN",
    "ChainDescriptor",
    "ChainRegistry",
    "GasProfile",
    "NativeToken",
    "RpcProfile",
    "SimulationProfile",
    "Timeouts",
    "parse_caip2",
    "register_chain",
    "to_caip2",
]
