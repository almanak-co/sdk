"""Lazy registry for connector-owned ``Primitive`` declarations (VIB-4162 follow-up).

Each connector that owns a tracked on-chain position declares its top-level
:class:`~almanak.framework.primitives.types.Primitive` in a tiny
``primitive.py`` module exporting a module-level
:class:`PrimitiveDeclaration`. This registry imports those modules on demand
and resolves a *position-type label string* (a protocol alias such as
``"AAVE_V3"`` / ``"UNI_V3"`` / ``"GMX_V2"``) to the owning connector's
``Primitive``.

Why a dedicated registry (and not :data:`PROTOCOL_CAPABILITIES`)
===============================================================

The position-state materializer in
``almanak.framework.accounting.position_state._classify_position`` (an
accounting-critical consumer — a misclassification corrupts the books) used
to branch on an upper-cased protocol-name if-ladder hard-coded in
``almanak.framework.primitives.taxonomy.materializer_primitive_for``. Per
``docs/internal/blueprints/22-connector-self-containment.md`` the per-protocol
``protocol -> Primitive`` knowledge belongs **on the connector**, resolved
through a strategy-side registry, not in a framework dispatch ladder.

This is kept separate from
:class:`~almanak.connectors._strategy_base.capabilities_registry.CapabilitiesRegistry`
deliberately: that registry's value-dicts feed the intent-validation surface
(``vocabulary.PROTOCOL_CAPABILITIES``) and carry a strict identity contract.
Folding a ``primitive`` key into those dicts would entangle two unrelated
concerns and force every protocol with a capability entry to also be a
position-bearing primitive (Enso, Pendle, the ERC-4626 vaults, the Solana LP
venues, …). A dedicated registry keeps the primitive concern isolated and
lets each connector publish *only* the alias strings the materializer ever
sees.

Broken-connector isolation
===========================

The aggregated ``{label: Primitive}`` map is built **once**, lazily, on first
lookup, and cached for the process lifetime (``register`` of a new built-in is
a code change, not a runtime operation). Building it imports every registered
connector's ``primitive`` module, but it does so **per-connector in isolation**:
each connector's declaration is loaded inside its own ``try``/``except``, so a
broken or missing sibling connector (failed import, malformed declaration) is
skipped with a warning rather than poisoning the whole map. A broken connector
therefore cannot break lookups for unrelated, healthy connectors — its own
labels simply resolve to ``None`` (the caller's "unrecognised" path) while
every other connector's labels resolve normally.

The one error that is **not** swallowed is a genuine label *collision* — two
connectors claiming the same alias string. That is a programming error in the
registry the materializer cannot silently pick a side on, so it is raised
loudly at build time (see :meth:`PrimitiveRegistry._build_label_map`).

Generic (non-protocol) labels — ``LP`` / ``LENDING`` / ``SUPPLY`` / ``PERP``
/ ``VAULT`` / ``STAKE`` / ``PREDICTION`` / ``CEX`` / ``TOKEN`` / … — are NOT
owned by any single connector (they are the taxonomy's own vocabulary, shared
across every venue). They stay in the taxonomy module's canonical generic
table and are intentionally NOT registered here.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    # Imported under TYPE_CHECKING only. A module-level runtime import of
    # ``Primitive`` pulls in ``almanak.framework.primitives.__init__`` →
    # ``taxonomy`` → this module, a connectors→framework import cycle that
    # makes ``import almanak.connectors._strategy_base.primitive_registry``
    # fail in a fresh interpreter unless ``taxonomy`` happened to load first
    # (order-dependent). Nothing here references ``Primitive`` at runtime —
    # every use is an annotation, which ``from __future__ import annotations``
    # keeps as a string — so the type-checking-only import is sufficient.
    # ``ImportRef`` likewise only appears in annotations; the runtime value
    # arrives from the connector manifest.
    from almanak.connectors._connector_descriptor import ImportRef
    from almanak.framework.primitives.types import Primitive

logger = logging.getLogger(__name__)

__all__ = [
    "PrimitiveDeclaration",
    "PrimitiveRegistry",
    "primitive_for_position_label",
]


@dataclass(frozen=True)
class PrimitiveDeclaration:
    """A connector's declaration of the primitive it owns + its alias strings.

    ``primitive`` is the top-level :class:`Primitive` every position this
    connector tracks rolls up to. ``position_type_aliases`` is the set of
    *position-type label strings* the position-state materializer may observe
    for this connector — protocol-name spellings used by older teardown /
    accounting callers (e.g. ``{"AAVE_V3", "AAVE"}`` for the Aave V3
    connector). Labels are matched case-insensitively (normalised to
    upper-case at the registry boundary), so declare them in the canonical
    upper-case form for readability.

    A connector with no position-type aliases (a pure swap/bridge venue) does
    not ship a ``primitive.py`` and is simply absent from this registry.
    """

    primitive: Primitive
    position_type_aliases: frozenset[str]

    def __post_init__(self) -> None:
        """Coerce ``position_type_aliases`` to a ``frozenset[str]`` and validate.

        Python does not enforce the annotation, so a connector author could
        pass a bare string (e.g. ``position_type_aliases="DRIFT"``). Because a
        ``str`` is iterable, it would silently register each *character*
        (``"D"``, ``"R"``, …) as an alias — a severe, hard-to-debug routing
        bug. We therefore reject a bare ``str``/``bytes`` outright, coerce any
        other iterable of strings to a ``frozenset``, and require every member
        to be a non-empty string. The dataclass is frozen, so we set the
        coerced value via ``object.__setattr__``.
        """
        aliases = self.position_type_aliases
        if isinstance(aliases, str | bytes):
            raise TypeError(
                "position_type_aliases must be a frozenset[str], not a bare "
                f"{type(aliases).__name__} (a bare string would be iterated "
                "character-by-character and register each letter as an alias); "
                "pass e.g. frozenset({'DRIFT'})."
            )
        try:
            coerced = frozenset(aliases)
        except TypeError as exc:
            raise TypeError(
                "position_type_aliases must be an iterable of strings "
                f"(coercible to frozenset[str]), got {type(aliases).__name__}."
            ) from exc
        for member in coerced:
            if not isinstance(member, str) or not member:
                raise TypeError(f"position_type_aliases members must be non-empty strings, got {member!r}.")
        object.__setattr__(self, "position_type_aliases", coerced)


class PrimitiveRegistry:
    """Position-type-label to connector-owned :class:`Primitive` registry.

    The set of declaring connectors is derived from connector manifests: each
    position-bearing connector sets
    ``primitive=ImportRef(<module>, "PRIMITIVE")`` on its ``CONNECTOR``. The
    registry imports a declaration module on demand, validates its
    ``PRIMITIVE`` declaration, and indexes every alias string it declares.
    Two connectors may not claim the same alias string — that is a hard error
    surfaced at aggregated-map build time. Adding a connector requires no
    edit here — the manifest declaration in the connector's own folder is the
    registration.
    """

    # Aggregated {normalized_label: Primitive} map, built lazily and cached.
    # ``None`` means "not built yet".
    _label_map: ClassVar[dict[str, Primitive] | None] = None

    @staticmethod
    def _normalize(label: str) -> str:
        return label.upper().strip()

    @classmethod
    def _load_declaration(cls, ref: ImportRef) -> PrimitiveDeclaration:
        """Load a connector's manifest-declared primitive declaration."""
        target = f"{ref.module}.{ref.attribute}"
        decl = ref.load()
        if not isinstance(decl, PrimitiveDeclaration):
            raise TypeError(f"{target} must be a PrimitiveDeclaration, got {type(decl).__name__}")
        if not decl.position_type_aliases:
            raise ValueError(
                f"{target} declares no position_type_aliases; a "
                "connector that ships a primitive.py must claim at least one "
                "label string (otherwise it would be silently unroutable)."
            )
        return decl

    @classmethod
    def _build_label_map(cls) -> dict[str, Primitive]:
        """Import every manifest-declared primitive (in isolation) and index its labels.

        Each connector's declaration is loaded inside its own ``try``/``except``
        so a broken or missing sibling (failed import, malformed declaration)
        is skipped with a warning instead of aborting the whole build. This is
        the broken-connector-isolation guarantee: one bad connector cannot
        break lookups for unrelated, healthy connectors — its labels simply
        resolve to ``None``.

        A label claimed by two connectors is the one error that is **not**
        swallowed: it is a hard ``ValueError``. The registry has no way to pick
        between them, and the design point is that exactly one connector owns
        each protocol-alias label, so a collision is a programming error the
        accounting materializer must not silently resolve.
        """
        # Deferred import: keeps this module's import graph minimal (it is
        # imported by ``framework.primitives.taxonomy``) and mirrors the other
        # manifest-derived metadata registries.
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        label_map: dict[str, Primitive] = {}
        owner_of: dict[str, str] = {}
        for connector_manifest in CONNECTOR_REGISTRY.with_primitive():
            connector = connector_manifest.name
            ref = connector_manifest.primitive
            assert ref is not None
            try:
                decl = cls._load_declaration(ref)
            except Exception:  # noqa: BLE001 — isolate one broken connector
                logger.warning(
                    "Skipping primitive declaration for connector %r (%r): its "
                    "module failed to import or declared an invalid PRIMITIVE. "
                    "Its position-type labels will resolve to None; unrelated "
                    "connectors are unaffected.",
                    connector,
                    ref.module,
                    exc_info=True,
                )
                continue
            for raw_alias in decl.position_type_aliases:
                alias = cls._normalize(raw_alias)
                existing_owner = owner_of.get(alias)
                if existing_owner is not None and existing_owner != connector:
                    raise ValueError(
                        f"position-type label {alias!r} claimed by both {existing_owner!r} and {connector!r}"
                    )
                owner_of[alias] = connector
                label_map[alias] = decl.primitive
        return label_map

    @classmethod
    def _ensure_label_map(cls) -> dict[str, Primitive]:
        if cls._label_map is None:
            cls._label_map = cls._build_label_map()
        return cls._label_map

    @classmethod
    def primitive_for_label(cls, position_type_label: str) -> Primitive | None:
        """Resolve a protocol-alias position-type label to its ``Primitive``.

        Returns ``None`` when no connector claims the (normalised) label —
        the caller is expected to consult the taxonomy's generic-label table
        and, failing that, treat the position as unrecognised.
        """
        label_map = cls._ensure_label_map()
        return label_map.get(cls._normalize(position_type_label))

    @classmethod
    def label_map(cls) -> dict[str, Primitive]:
        """Return a copy of the full ``{normalized_label: Primitive}`` map."""
        return dict(cls._ensure_label_map())

    @classmethod
    def reset_cache(cls) -> None:
        """Test helper: drop the cached aggregated label map."""
        cls._label_map = None


def primitive_for_position_label(position_type_label: str) -> Primitive | None:
    """Module-level convenience wrapper for :meth:`PrimitiveRegistry.primitive_for_label`."""
    return PrimitiveRegistry.primitive_for_label(position_type_label)
