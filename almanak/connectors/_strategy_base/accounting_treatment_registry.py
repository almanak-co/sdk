"""Strategy-side dispatch registry for connector-owned accounting treatments (VIB-4931).

Sibling of the other ``_strategy_base`` registries (:class:`PrimitiveRegistry`,
:class:`LendingReadRegistry`, …). It lets the framework accounting dispatcher
route a venue's events to connector-owned treatment functions — and derive a
venue's outbox position key — without the framework naming the protocol or
carrying a protocol-named ``AccountingCategory`` member.

Each connector with custom accounting publishes a module-level
:data:`ACCOUNTING_TREATMENT_SPEC` (an
:class:`~almanak.connectors._strategy_base.accounting_treatment_base.AccountingTreatmentSpec`)
from its ``CONNECTOR.accounting_treatment`` manifest reference. The boot file
``almanak.connectors._strategy_accounting_treatment_registry`` registers those
lazy references into this base registry. Adding a venue with special accounting is
one connector-local ``accounting_spec.py`` plus one manifest reference — no
framework edit and no central protocol row.

Dispatch is by *spec*, not by an exact protocol key: a connector's ``categorize``
decides whether it claims a given ``(intent_type, protocol, token_out)`` (Pendle,
for instance, matches ``"pendle" in protocol`` — a substring, not an exact key),
so the registry iterates the published specs and the first that claims an event
wins. The treatment index (``treatment_key -> fn``) is built once across every
spec; two connectors publishing the same ``treatment_key`` is a hard
``ValueError`` (the registry cannot silently pick a side).

Broken-connector isolation: each spec is imported lazily and per-connector inside
its own ``try``/``except`` — a broken or missing sibling connector is skipped with
a warning and cannot poison categorization / position-key resolution for healthy
connectors (mirrors :class:`PrimitiveRegistry`). The one error that is *not*
swallowed is a ``treatment_key`` collision (a programming error the registry
cannot resolve).

Gateway-boundary note: this module is strategy-side and performs no network
egress. The connector ``accounting_spec`` modules it imports are pure data + pure
functions. It does not import ``almanak.framework`` at runtime — a decision's
``category`` (a generic ``AccountingCategory``) is handled opaquely.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Iterator
from typing import ClassVar

from almanak.connectors._strategy_base.accounting_treatment_base import (
    AccountingCategoryDecision,
    AccountingTreatmentSpec,
    TreatmentFn,
)

logger = logging.getLogger(__name__)

# ``AccountingCategoryDecision`` / ``AccountingTreatmentSpec`` are re-exported so
# callers can name the seam types without reaching into the base module.
__all__ = [
    "AccountingCategoryDecision",
    "AccountingTreatmentRegistry",
    "AccountingTreatmentSpec",
]


class AccountingTreatmentRegistry:
    """Connector folder name → published accounting-treatment-spec dispatch.

    Empty of behaviour until the strategy-side boot file registers connector
    manifest references that publish an ``ACCOUNTING_TREATMENT_SPEC``.
    """

    # Connector folder name -> (module path, attribute) naming the connector's
    # published ``AccountingTreatmentSpec``. Populated by
    # ``almanak.connectors._strategy_accounting_treatment_registry`` from
    # connector manifests. Tests may still replace this table to exercise
    # broken-connector isolation and collision handling.
    _SPEC_LOADERS: ClassVar[dict[str, tuple[str, str]]] = {}

    # Per-connector resolved-spec cache (lazy, process-lifetime — adding a loader
    # row is a code change, not a runtime operation).
    _spec_cache: ClassVar[dict[str, AccountingTreatmentSpec]] = {}
    # Aggregated ``{treatment_key: fn}`` index, built once across every spec and
    # cached. ``None`` means "not built yet".
    _treatment_cache: ClassVar[dict[str, TreatmentFn] | None] = None

    @classmethod
    def supported_connectors(cls) -> tuple[str, ...]:
        """Return every connector folder name with a published treatment spec."""
        return tuple(sorted(cls._SPEC_LOADERS))

    @classmethod
    def register_loader(cls, connector: str, module_path: str, attribute: str) -> None:
        """Register one lazy accounting-treatment spec reference.

        Re-registering the identical reference is a no-op so repeated imports of
        the boot file stay idempotent. A conflicting reference for the same
        connector is a programming error: the framework cannot safely pick one.
        """
        if not connector or not isinstance(connector, str):
            raise ValueError(f"connector must be a non-empty string, got {connector!r}")
        if not module_path or not isinstance(module_path, str):
            raise ValueError(f"module_path must be a non-empty string, got {module_path!r}")
        if not attribute or not isinstance(attribute, str):
            raise ValueError(f"attribute must be a non-empty string, got {attribute!r}")

        existing = cls._SPEC_LOADERS.get(connector)
        ref = (module_path, attribute)
        if existing is not None:
            if existing == ref:
                return
            raise ValueError(
                f"accounting treatment connector {connector!r} already registered "
                f"as {existing[0]}.{existing[1]}; refusing {module_path}.{attribute}"
            )
        cls._SPEC_LOADERS[connector] = ref
        cls.reset_cache()

    @classmethod
    def _load_spec(cls, connector: str) -> AccountingTreatmentSpec:
        """Import one connector's ``accounting_spec`` and return its published spec.

        Raises on a missing loader entry, a failed import, or a wrong-type
        attribute — :meth:`_iter_specs` catches these to isolate one broken
        connector from the rest.
        """
        cached = cls._spec_cache.get(connector)
        if cached is not None:
            return cached
        module_path, attribute = cls._SPEC_LOADERS[connector]
        module = importlib.import_module(module_path)
        spec = getattr(module, attribute, None)
        if not isinstance(spec, AccountingTreatmentSpec):
            raise TypeError(
                f"Registry maps {connector!r} to {module_path}.{attribute}, but that "
                f"attribute is {type(spec).__name__}, not an AccountingTreatmentSpec."
            )
        cls._spec_cache[connector] = spec
        return spec

    @classmethod
    def _iter_specs(cls) -> Iterator[tuple[str, AccountingTreatmentSpec]]:
        """Yield ``(connector, spec)`` for each loader, isolating broken siblings.

        A connector whose module fails to import or whose attribute is the wrong
        type is skipped with a warning (its events fall through to the generic
        accounting path); healthy connectors are unaffected.
        """
        for connector in cls._SPEC_LOADERS:
            try:
                spec = cls._load_spec(connector)
            except Exception:  # noqa: BLE001 — isolate one broken connector
                logger.warning(
                    "Skipping accounting-treatment spec for connector %r: its module "
                    "failed to import or published an invalid ACCOUNTING_TREATMENT_SPEC. "
                    "Its events fall through to the generic accounting path; unrelated "
                    "connectors are unaffected.",
                    connector,
                    exc_info=True,
                )
                continue
            yield connector, spec

    @classmethod
    def categorize(cls, intent_type: str, protocol: str, token_out: str) -> AccountingCategoryDecision | None:
        """Resolve the connector decision for one event, or ``None`` if unclaimed.

        Iterates the published specs in registration order; the first connector
        whose ``categorize`` claims ``(intent_type, protocol, token_out)`` wins. A
        connector whose ``categorize`` raises is isolated (logged + skipped) so a
        buggy sibling cannot break dispatch for healthy connectors. Returns
        ``None`` when no connector claims the event — the framework dispatcher then
        routes it through the generic category path.
        """
        for connector, spec in cls._iter_specs():
            try:
                decision = spec.categorize(intent_type, protocol, token_out)
            except Exception:  # noqa: BLE001 — isolate one broken connector
                logger.warning(
                    "Connector %r categorize() raised for intent_type=%r protocol=%r; "
                    "treating the event as unclaimed (generic accounting path).",
                    connector,
                    intent_type,
                    protocol,
                    exc_info=True,
                )
                continue
            if decision is not None:
                return decision
        return None

    @classmethod
    def _treatment_index(cls) -> dict[str, TreatmentFn]:
        """Build (once) and return the ``{treatment_key: fn}`` index across all specs.

        A ``treatment_key`` claimed by two connectors is a hard ``ValueError`` —
        the registry cannot silently pick a side (mirrors
        :meth:`PrimitiveRegistry._build_label_map`'s collision rule).
        """
        cache = cls._treatment_cache
        if cache is None:
            cache = {}
            owner: dict[str, str] = {}
            for connector, spec in cls._iter_specs():
                for key, fn in spec.treatments.items():
                    existing = owner.get(key)
                    if existing is not None and existing != connector:
                        raise ValueError(
                            f"accounting treatment_key {key!r} claimed by both {existing!r} and {connector!r}"
                        )
                    owner[key] = connector
                    cache[key] = fn
            cls._treatment_cache = cache
        return cache

    @classmethod
    def treatment_for(cls, treatment_key: str) -> TreatmentFn | None:
        """Resolve a connector treatment fn by its opaque key, or ``None``."""
        return cls._treatment_index().get(treatment_key)

    @classmethod
    def position_key_for(
        cls,
        protocol: str,
        *,
        intent_type: str,
        chain: str,
        wallet: str,
        intent: object,
    ) -> tuple[str, str] | None:
        """Resolve the connector ``(position_key, market_id)`` for an event, or ``None``.

        Iterates the published specs; the first whose ``position_key`` returns a
        non-``None`` pair (i.e. owns ``(protocol, intent_type)``) wins. Returns
        ``None`` when no connector contributes a custom position key — the runner
        then falls back to its generic key derivation. A connector whose
        ``position_key`` raises is isolated (logged + skipped).
        """
        for connector, spec in cls._iter_specs():
            if spec.position_key is None:
                continue
            try:
                key = spec.position_key(
                    protocol=protocol, intent_type=intent_type, chain=chain, wallet=wallet, intent=intent
                )
            except Exception:  # noqa: BLE001 — isolate one broken connector
                logger.warning(
                    "Connector %r position_key() raised for intent_type=%r protocol=%r; skipping.",
                    connector,
                    intent_type,
                    protocol,
                    exc_info=True,
                )
                continue
            if key is not None:
                return key
        return None

    @classmethod
    def reset_cache(cls) -> None:
        """Test helper: drop the resolved-spec + treatment-index caches.

        Production code should never call this; it exists for narrow test setups
        that intentionally re-trigger a connector import or swap ``_SPEC_LOADERS``.
        """
        cls._spec_cache.clear()
        cls._treatment_cache = None
