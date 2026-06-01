"""Strategy-side bridge provider registry (VIB-4837).

Sibling of the other ``_strategy_base`` registries
(:class:`~almanak.connectors._strategy_base.flash_loan_registry.FlashLoanProviderRegistry`,
:class:`AddressRegistry`, …). It owns the bridge-name → adapter-factory mapping
so the bridge compiler's selector dispatch
(``almanak.connectors._strategy_base.bridge_compiler.BridgeCompiler._build_selector``)
names no specific connector. The compiler asks the registry to build the
selector's candidate adapters and never imports ``AcrossBridgeAdapter`` /
``StargateBridgeAdapter`` itself.

Asymmetry vs. the flash-loan registry
======================================

The flash-loan registry stores a zero-arg ``make_provider`` factory plus a
``build`` callable per provider. Bridge adapters are **not** zero-arg:
``AcrossBridgeAdapter(config=None, token_resolver=None)`` and
``StargateBridgeAdapter(config=None, token_resolver=None)`` take a
``token_resolver=`` that is only available at ``_build_selector(ctx)`` call
time. So this registry stores provider **factories** — callables taking
``token_resolver=`` and returning a fresh :class:`BridgeAdapter` — and
:meth:`build_all` threads the call-time resolver through each one. The concrete
adapter class itself *is* a valid factory (its ``__init__`` accepts
``token_resolver=``), so registration is just ``register(name=…, factory=Cls)``.

This module stays protocol-clean — it imports only
:mod:`~almanak.connectors._strategy_base.bridge_base` (the ``BridgeAdapter``
ABC). The concrete adapters are registered by the boot file
``almanak/connectors/_strategy_bridge_registry.py`` (mirrors
``_strategy_receipt_registry.py`` / ``_strategy_flash_loan_registry.py``), which
lives one level up so ``_strategy_base/`` never imports a concrete connector.

Registration order (across, then stargate) is preserved (insertion-ordered
``dict``) so the selector's candidate order — and therefore the selection for a
given input — stays byte-stable across the refactor. It matches the legacy
hand-built ``[AcrossBridgeAdapter(...), StargateBridgeAdapter(...)]`` list.

Names are normalized (stripped + lowercased) at registration, so the stored key
is always the canonical slug and a case-/whitespace-variant cannot bypass the
duplicate check. Duplicate registration is a **hard error**: two connectors
claiming the same bridge name is a programming error (a collision in the
selector's candidate set), so the registry refuses to silently shadow one with
the other. (This is stricter than the flash-loan registry, which logs and
replaces; the bridge selector has no "auto" provider-resolution path that would
mask a duplicate. It matches the ``_strategy_receipt_registry`` precedent, which
also raises on collision.)

Gateway-boundary note: this module is strategy-side and performs no network
egress. The connector ``adapter`` modules the boot file imports are pure
quote/transaction builders; the gateway-routed submission happens later, from
the execution pipeline.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from almanak.connectors._strategy_base.bridge_base import BridgeAdapter

__all__ = [
    "BRIDGE_PROVIDER_REGISTRY",
    "BridgeAdapterFactory",
    "BridgeProviderRegistry",
]


#: A bridge-adapter factory: a callable taking the call-time ``token_resolver``
#: and returning a fresh :class:`BridgeAdapter`. The concrete adapter classes
#: (``AcrossBridgeAdapter`` / ``StargateBridgeAdapter``) satisfy this directly —
#: the class *is* the factory, since its ``__init__`` accepts ``token_resolver=``.
BridgeAdapterFactory = Callable[..., BridgeAdapter]


class BridgeProviderRegistry:
    """Bridge-name → adapter-factory dispatch.

    Populated once at boot by ``_strategy_bridge_registry``. The bridge compiler
    consumes :meth:`build_all` (to mint the selector's candidate adapters)
    without naming a connector.
    """

    def __init__(self) -> None:
        # Insertion-ordered: the selector candidate order (and therefore the
        # selection for a given input) depends on registration order.
        self._factories: dict[str, BridgeAdapterFactory] = {}

    def register(self, *, name: str, factory: BridgeAdapterFactory) -> None:
        """Register one connector's bridge-adapter factory.

        Args:
            name: Bridge identifier — the lowercase protocol slug (e.g.
                ``"across"`` / ``"stargate"``), matching
                ``BridgeCompiler.protocols``. (Distinct from the adapter's
                display ``name`` property, ``"Across"`` / ``"Stargate"``, which
                the selector uses for reliability lookup and preferred/excluded
                matching.) Normalized — stripped and lowercased — before use, so
                the stored key is always the canonical slug and the duplicate
                check is case- / whitespace-insensitive.
            factory: Callable taking ``token_resolver=`` and returning a fresh
                :class:`BridgeAdapter`. The concrete adapter class satisfies
                this directly.

        Raises:
            ValueError: if ``name`` is empty/blank (after normalization), or is
                already registered. A duplicate is a programming error (selector
                candidate collision), never a silent overwrite.
        """
        normalized_name = name.strip().lower()
        if not normalized_name:
            raise ValueError(f"Bridge provider name must be a non-empty string, got {name!r}.")
        if normalized_name in self._factories:
            raise ValueError(
                f"Bridge provider {normalized_name!r} is already registered; "
                "two connectors cannot claim the same bridge name."
            )
        self._factories[normalized_name] = factory

    def names(self) -> tuple[str, ...]:
        """Registered bridge identifiers, in registration order."""
        return tuple(self._factories)

    def factories(self) -> tuple[BridgeAdapterFactory, ...]:
        """Registered adapter factories, in registration order."""
        return tuple(self._factories.values())

    def build_all(self, token_resolver: Any) -> list[BridgeAdapter]:
        """Build a fresh adapter per registration, in registration order.

        Threads the call-time ``token_resolver`` through every factory — one
        instance per registration, matching the legacy hand-built
        ``[AcrossBridgeAdapter(token_resolver=…), StargateBridgeAdapter(token_resolver=…)]``
        list. A fresh instance per call keeps each selection's candidates
        independent.
        """
        return [factory(token_resolver=token_resolver) for factory in self._factories.values()]

    def clear(self) -> None:
        """Test helper — clear registrations. NOT used in production paths."""
        self._factories.clear()


#: The single in-process registry. Concrete connectors are registered into it
#: by ``almanak.connectors._strategy_bridge_registry`` (the boot file).
BRIDGE_PROVIDER_REGISTRY = BridgeProviderRegistry()
