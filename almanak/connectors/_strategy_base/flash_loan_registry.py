"""Strategy-side flash-loan provider registry (VIB-4837).

Sibling of the other ``_strategy_base`` registries (:class:`AddressRegistry`,
:class:`LendingReadRegistry`, …). It owns the provider-name → connector
flash-loan opt-in mapping so the intent compiler's flash-loan path
(``almanak.framework.intents.compiler_flash_loan``) names no specific
connector. The compiler asks the registry for the candidate providers (to feed
:class:`~almanak.framework.intents.flash_loan_selector.FlashLoanSelector`) and
dispatches the build step through it.

Each flash-loan connector ships two artifacts the registry exposes:

* a ``FlashLoanProvider`` subclass (the selector's *quote* candidate), and
* a ``build_<x>_flash_loan(compiler, *, token_info, amount_wei, callback_params,
  callback_gas_total) -> dict`` callable (the *build* step).

This module stays protocol-clean — it imports only
:mod:`~almanak.connectors._strategy_base.flash_loan_base`. The concrete
connectors are registered by the boot file
``almanak/connectors/_strategy_flash_loan_registry.py`` (mirrors
``_strategy_receipt_registry.py``), which lives one level up so
``_strategy_base/`` never imports a concrete connector.

Registration order is preserved (insertion-ordered ``dict``) so both the
selector's candidate order and the compiler's ``"Supported providers: …"``
error string stay byte-stable across the refactor.

Gateway-boundary note: this module is strategy-side and performs no network
egress. The connector ``flash_loan`` / ``flash_loan_provider`` modules the boot
file imports are pure data + pure transaction builders; the gateway-routed
submission happens later, from the execution pipeline.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from almanak.connectors._strategy_base.flash_loan_base import FlashLoanProvider

logger = logging.getLogger(__name__)

__all__ = [
    "FLASH_LOAN_PROVIDER_REGISTRY",
    "FlashLoanProviderRegistration",
    "FlashLoanProviderRegistry",
]


@dataclass(frozen=True)
class FlashLoanProviderRegistration:
    """One connector's flash-loan opt-in.

    Attributes:
        name: Provider identifier — must equal the connector's
            ``FlashLoanProvider.name`` (e.g. ``"aave"``, ``"balancer"``,
            ``"morpho"``). This is the key the compiler routes on.
        make_provider: Zero-arg factory returning a fresh
            :class:`FlashLoanProvider` — the selector's quote candidate. A
            factory (rather than a shared instance) keeps each selection's
            candidates independent.
        build: The connector's ``build_<x>_flash_loan`` callable. Invoked as
            ``build(compiler, token_info=…, amount_wei=…, callback_params=…,
            callback_gas_total=…)`` and returns the build-result dict.
    """

    name: str
    make_provider: Callable[[], FlashLoanProvider]
    build: Callable[..., dict[str, Any]]


class FlashLoanProviderRegistry:
    """Provider-name → connector flash-loan registration dispatch.

    Populated once at boot by ``_strategy_flash_loan_registry``. The compiler
    consumes :meth:`providers` (selector candidates), :meth:`names` (validation
    + error message), and :meth:`build` (build-step dispatch) without naming a
    connector.
    """

    def __init__(self) -> None:
        # Insertion-ordered: callers rely on registration order for the
        # selector candidate list and the byte-stable error string.
        self._registrations: dict[str, FlashLoanProviderRegistration] = {}

    def register(self, registration: FlashLoanProviderRegistration) -> None:
        """Register (or replace) one connector's flash-loan opt-in."""
        if registration.name in self._registrations:
            logger.debug("Re-registering flash-loan provider %r", registration.name)
        self._registrations[registration.name] = registration

    def names(self) -> tuple[str, ...]:
        """Registered provider identifiers, in registration order."""
        return tuple(self._registrations)

    def has(self, name: str) -> bool:
        """Whether ``name`` is a registered flash-loan provider."""
        return name in self._registrations

    def providers(self) -> list[FlashLoanProvider]:
        """Fresh ``FlashLoanProvider`` candidates for the selector.

        One instance per registration, in registration order — matches the
        legacy hand-built ``[Aave(), Balancer(), Morpho()]`` list.
        """
        return [reg.make_provider() for reg in self._registrations.values()]

    def build(self, name: str, compiler: Any, **kwargs: Any) -> dict[str, Any]:
        """Dispatch the build step to the provider registered under ``name``.

        The compiler validates membership via :meth:`names` before calling, so
        an unregistered ``name`` is a programming error and raises ``KeyError``
        rather than silently building a different provider.
        """
        return self._registrations[name].build(compiler, **kwargs)


#: The single in-process registry. Concrete connectors are registered into it
#: by ``almanak.connectors._strategy_flash_loan_registry`` (the boot file).
FLASH_LOAN_PROVIDER_REGISTRY = FlashLoanProviderRegistry()
