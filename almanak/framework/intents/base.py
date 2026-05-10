"""Base class for every Almanak Intent dataclass (VIB-4192 / T06).

This module defines :class:`BaseIntent`, the single shared parent every
concrete intent class — ``SwapIntent``, ``LPOpenIntent``, ``BorrowIntent``,
``PerpOpenIntent``, ``BridgeIntent``, etc. — inherits from. ``BaseIntent``
extends :class:`almanak.framework.models.base.AlmanakImmutableModel` and
adds exactly two responsibilities:

1. **Reserved field** — ``registry_handle: str | None = None`` (VIB-4192).

   This is the strategy-author-supplied alias the multi-position-tracking
   epic (VIB-4185) uses to pair an open intent with its later close intent
   when a strategy intentionally runs more than one position on the same
   ``(primitive, semantic_group)``. T06 reserves the field on every intent
   class via single-point inheritance — no per-primitive class redeclares
   it (Acceptance Criterion #3).

2. **Strict validation on construction** — when ``registry_handle`` is
   set, the field-side ``model_validator`` calls
   :func:`almanak.framework.primitives.taxonomy.record_for` on the
   intent's resolved ``intent_type.value``. ``record_for`` raises
   :class:`~almanak.framework.primitives.taxonomy.UnknownIntentTypeError`
   when the intent type is not declared in :data:`TAXONOMY` — soft
   :func:`classify` is intentionally NOT used (Acceptance Criterion #2).
   Both T2 (VIB-4162) and T4 (VIB-4164) precedent rejected the soft
   path for the same silent-classify reason.

Notes:

- ``BaseIntent`` does NOT wire any collision-guard logic. T14 (VIB-4197)
  owns the auto-mode collision guard against this field. Two intents
  with the same handle today construct successfully — see
  ``docs/internal/uat-cards/VIB-4192.md`` D3.F7 for the negative-claim
  proof.

- The validator also fires on the documented decide-result emission
  chokepoint :meth:`Intent.serialize_result` (defense-in-depth against
  Pydantic's documented ``model_construct`` / ``model_copy`` bypass
  paths). The chokepoint is implemented in
  ``almanak/framework/intents/vocabulary.py``; see D3.F10 in the UAT
  card for the matrix that proves it across (intent class × result
  shape).
"""

from __future__ import annotations

from typing import Any

from pydantic import model_validator

from almanak.framework.models.base import AlmanakImmutableModel
from almanak.framework.primitives.taxonomy import (
    UnknownIntentTypeError,
    record_for,
)

# Sentinel used for null intent_type messaging — see D3.F6 null-guard case.
_NULL_INTENT_TYPE_SENTINEL = "<None>"


def _resolve_intent_type_string(intent_type: Any) -> str:
    """Resolve an ``intent_type`` value to its canonical string form.

    Every concrete intent surfaces ``intent_type`` as either an
    :class:`enum.Enum` member (the common case — ``IntentType.SWAP``,
    ``BridgeIntentType.BRIDGE``, etc.) or a raw string. Both shapes
    flow into :func:`record_for` as the canonical upper-case key.

    ``None`` is treated as a hard error (callers — including the
    construction-side validator and the emission chokepoint — surface it
    as ``UnknownIntentTypeError(_NULL_INTENT_TYPE_SENTINEL)``).
    """
    if intent_type is None:
        raise UnknownIntentTypeError(_NULL_INTENT_TYPE_SENTINEL)
    # Enum members carry the canonical string on .value; raw strings flow
    # straight through. Falling back to str(intent_type) covers any
    # caller-defined wrapper.
    return getattr(intent_type, "value", None) or str(intent_type)


def assert_registry_handle_known(intent: Any) -> None:
    """Validate ``intent.registry_handle`` against TAXONOMY at the emission
    chokepoint.

    Used by :meth:`Intent.serialize_result` to re-run the strict
    ``record_for`` lookup on every intent in the result tree. This is the
    second of the two reinforcing chokepoints described in the UAT card
    Feature contract — the first is the construction-side
    ``model_validator`` on this class. Defending both layers closes
    Pydantic's documented ``model_construct`` / ``model_copy(validate=False)``
    bypass paths at the framework boundary.

    Performs the SAME shape + TAXONOMY checks as the construction-side
    validator:

    - ``None`` handle → no-op.
    - Non-string handle → raises ``TypeError`` (model_construct can ship
      an int / list / object that the field-type guard didn't reject).
    - Empty / whitespace-only handle → raises ``ValueError``.
    - Set handle whose intent's resolved ``intent_type`` is not in
      TAXONOMY (or is ``None``) → raises ``UnknownIntentTypeError``.

    Earlier revisions of this helper validated ONLY the intent_type;
    that left a hole where ``model_construct(registry_handle="")`` would
    silently emit through the chokepoint. CodeRabbit's PR #2205 review
    surfaced this — the helper now mirrors the construction-side checks
    in full.
    """
    handle = getattr(intent, "registry_handle", None)
    if handle is None:
        return
    if not isinstance(handle, str):
        raise TypeError(f"registry_handle must be a string, got {type(handle).__name__}: {handle!r}")
    if not handle.strip():
        raise ValueError("registry_handle must be a non-empty, non-whitespace string")
    intent_type_str = _resolve_intent_type_string(getattr(intent, "intent_type", None))
    record_for(intent_type_str)  # raises UnknownIntentTypeError on miss


class BaseIntent(AlmanakImmutableModel):
    """Shared parent class for every concrete intent dataclass.

    Adds the reserved :attr:`registry_handle` field and a strict
    construction-time validator that enforces (a) non-empty / non-whitespace
    handles, and (b) presence of the intent's ``intent_type`` in
    :data:`~almanak.framework.primitives.taxonomy.TAXONOMY` via
    :func:`~almanak.framework.primitives.taxonomy.record_for`.

    See module docstring for design rationale and links to the UAT card.
    """

    # NOTE: we deliberately use the simple `str | None` annotation rather
    # than a typed wrapper. Pydantic v2 in `strict=True` mode (inherited from
    # AlmanakImmutableModel) rejects non-string inputs at the type-validation
    # stage — int/float/list/dict/object on `registry_handle` raise
    # ValidationError before the `model_validator` below ever runs. The
    # validator handles the additional empty/whitespace and TAXONOMY checks
    # that pure type-level validation cannot express.
    registry_handle: str | None = None

    @property
    def intent_type(self) -> Any:
        """Intent type enum / string surfaced by every concrete subclass.

        Each concrete intent class (``SwapIntent``, ``LPOpenIntent``, …)
        overrides this property to return its own enum member
        (``IntentType.SWAP``, ``IntentType.LP_OPEN``, …). Pre-VIB-4192 the
        property was implicitly defined per-class with no shared
        declaration; this base-class stub exists so that
        :class:`BaseIntent`'s ``model_validator`` can statically reference
        ``self.intent_type`` (otherwise mypy reports
        ``"BaseIntent" has no attribute "intent_type"``).

        The ``Any`` return is intentional: subclasses surface different
        enum types (``IntentType``, ``BridgeIntentType``,
        ``EnsureBalanceIntentType``) and the validator only ever needs
        ``.value`` or ``str()``. Promoting to a typed Protocol would
        force every intent enum into a single hierarchy — out of scope.

        Subclasses that *don't* override this raise NotImplementedError
        on access. In practice every concrete subclass overrides; the
        D3.F6 null-guard test in
        ``tests/unit/intent/test_registry_handle.py`` covers the
        pathological-subclass-returning-None case (the validator still
        raises).
        """
        raise NotImplementedError(f"{type(self).__name__} did not override BaseIntent.intent_type")

    @model_validator(mode="after")
    def _validate_registry_handle(self) -> BaseIntent:
        """Strict-on-construction validation of ``registry_handle``.

        - ``None`` → no-op (the field is optional).
        - Empty / whitespace-only string → raises ``ValueError`` (D3.F1).
        - Set to a value but the intent's ``intent_type`` is not in
          TAXONOMY (or is ``None``) → raises ``UnknownIntentTypeError``
          via :func:`record_for` (D3.F6).

        The strict ``record_for`` call is load-bearing for AC #2: the
        soft :func:`classify` returns ``AccountingCategory.NO_ACCOUNTING``
        for unknown intents, which would silently classify a registry
        write into the catch-all bucket and leak unbounded.
        ``record_for`` raises instead.
        """
        handle = self.registry_handle
        if handle is None:
            return self
        # Strict mode caught non-strings already; we only need to filter
        # empty / whitespace-only strings here.
        if not handle.strip():
            raise ValueError("registry_handle must be a non-empty, non-whitespace string")
        # Strict TAXONOMY lookup. Raises UnknownIntentTypeError if the
        # intent's resolved intent_type is not declared in the taxonomy
        # (or is None).
        intent_type_str = _resolve_intent_type_string(self.intent_type)
        record_for(intent_type_str)
        return self


__all__ = [
    "BaseIntent",
    "assert_registry_handle_known",
]
