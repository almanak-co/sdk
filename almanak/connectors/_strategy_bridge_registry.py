"""Strategy-side bridge provider registration site (VIB-4837).

Sibling of :mod:`almanak.connectors._strategy_receipt_registry` and
:mod:`almanak.connectors._strategy_flash_loan_registry`, scoped to the
bridge-provider concern.

Lives one level up from ``_strategy_base/`` because it imports each bridge
connector's ``adapter`` module — and ``_strategy_base/`` must stay
protocol-clean (no concrete connector imports). Adding a new bridge connector
means one import block + one ``BRIDGE_PROVIDER_REGISTRY.register`` line below —
no edit anywhere in the framework.

Registration order (across, stargate) is load-bearing: it fixes the selector's
candidate order and therefore the selection for a given input. Keep it stable
unless intentionally changing that surface.

The factory is the adapter **class** itself: ``AcrossBridgeAdapter`` /
``StargateBridgeAdapter`` accept ``token_resolver=`` (plus an optional
``config=``), so ``BRIDGE_PROVIDER_REGISTRY.build_all(token_resolver)`` calls
the class directly to mint a fresh adapter per selection. No
``BridgeProviderRegistration`` dataclass is needed (unlike the flash-loan
registry) because a bridge contributes exactly one artifact — the adapter.

No completeness guard (unlike ``_strategy_flash_loan_registry``): the flash-loan
boot file pairs with a CI test asserting every connector that ships a
``flash_loan_provider.py`` registers, keyed on that discriminating filename.
Bridges have no equivalent discriminator — every connector ships an
``adapter.py``, and ``IntentType.BRIDGE`` is not a clean signal either (``lifi``
declares ``intents=(SWAP, BRIDGE)`` but is a swap-aggregator routed through its
own path, not a ``BridgeAdapter``). A discovery-based guard would have to import
every connector or would falsely flag ``lifi``, so the small residual risk — a
future pure ``BridgeAdapter`` that forgets to register is silently absent from
the selector — is accepted rather than chased with a brittle test.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.bridge_registry import BRIDGE_PROVIDER_REGISTRY

__all__ = ["BRIDGE_PROVIDER_REGISTRY"]


def _register_all() -> None:
    """Register every strategy-side bridge provider.

    Imports are local to the function so that loading this module does not
    transitively import each connector's quote/transaction builders until the
    registry is actually constructed (they pull in connector-side address
    tables and ABI helpers we don't want loaded just to know "this connector
    exists").

    Each adapter is imported from its ``adapter`` submodule directly (never the
    connector package ``almanak.connectors.<x>``), so this never triggers a
    connector ``__init__.py``'s PEP 562 ``__getattr__`` lazy-registration
    machinery — preserving import-order / circular-import safety.
    """
    from almanak.connectors.across.adapter import AcrossBridgeAdapter
    from almanak.connectors.stargate.adapter import StargateBridgeAdapter

    BRIDGE_PROVIDER_REGISTRY.register(name="across", factory=AcrossBridgeAdapter)
    BRIDGE_PROVIDER_REGISTRY.register(name="stargate", factory=StargateBridgeAdapter)


_register_all()
