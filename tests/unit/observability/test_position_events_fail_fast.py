"""D3.F6 — silent-error guard. Unknown intents must fail loudly, not return a default-LP PositionEvent.

VIB-4162 (T2). The pre-T2 silent-LP fallback at ``position_events.py:292``
(``INTENT_TO_POSITION_TYPE.get(intent_type, PositionType.LP)``) is the
canonical class-of-bug this gate prevents.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from almanak.framework.observability import position_events as pe
from almanak.framework.primitives import taxonomy as primitives_taxonomy
from almanak.framework.primitives.taxonomy import UnknownIntentTypeError


@dataclass
class _StubIntent:
    intent_type: Any


def _build_ctx(intent_type: Any) -> pe.IntentEventContext:
    return pe.IntentEventContext(
        intent=_StubIntent(intent_type=intent_type),
        result=None,
        extracted={},
        deployment_id="d",
        chain="arbitrum",
        ledger_entry_id="le-1",
    )


# ─── unknown intents must raise (or seed-None for non-position intents) ──


@pytest.mark.parametrize(
    "intent_type",
    [
        "LP_OEPN",  # misspelled
        "LPOPEN",  # missing underscore
        "LIQUIDATE",  # future placeholder pre-T5
        "OPEN_CDP",
        "123",  # numeric/garbage
    ],
)
def test_unknown_intent_in_event_type_table_raises_unknown_intent(
    intent_type: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the intent passes INTENT_TO_EVENT_TYPE, the strict lookup raises.

    None of these intent strings appear in INTENT_TO_EVENT_TYPE (so
    ``_seed_event`` returns None before reaching the strict path). The
    test asserts the first-layer guard (returning None for non-position
    intents) AND the second-layer raise behaviour by directly driving
    ``_resolve_position_type``.
    """
    # Layer 1: seed returns None — intent isn't position-producing.
    assert pe._seed_event(_build_ctx(intent_type)) is None

    # Layer 2: strict resolve raises UnknownIntentTypeError.
    with pytest.raises(UnknownIntentTypeError):
        pe._resolve_position_type(intent_type)


def test_empty_string_raises_strict() -> None:
    with pytest.raises(UnknownIntentTypeError):
        pe._resolve_position_type("")


def test_whitespace_only_raises_strict() -> None:
    with pytest.raises(UnknownIntentTypeError):
        pe._resolve_position_type("   ")


def test_none_raises_attribute_error() -> None:
    """``None`` fails fast at the first ``.upper()`` call inside taxonomy.

    Locking the assertion to ``AttributeError`` (rather than the broader
    ``(TypeError, AttributeError)``) catches a regression where a future
    refactor adds a permissive ``str(intent_type)`` coercion that would
    silently rewrite ``None`` to the literal string ``"None"`` and then
    fall through to a soft path. ``str.upper()`` on ``None`` is the only
    documented strict behavior here; nothing else is acceptable.
    """
    with pytest.raises(AttributeError):
        pe._resolve_position_type(None)  # type: ignore[arg-type]


# ─── lowercase / mixed case / alias paths (success) ──────────────────────


def test_lowercase_known_intent_resolves() -> None:
    pt = pe._resolve_position_type("lp_open")
    assert pt is pe.PositionType.LP


def test_mixed_case_known_intent_resolves() -> None:
    pt = pe._resolve_position_type("Lp_Open")
    assert pt is pe.PositionType.LP


def test_alias_resolution_path_exercised(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patches `_resolve_alias` with a counting wrapper to prove the path is exercised.

    Uses ``lp_open`` (lowercase canonical, not an alias) and ``VAULT_WITHDRAW``
    (legitimate alias for ``VAULT_REDEEM``). VAULT_WITHDRAW is not in
    ``INTENT_TO_EVENT_TYPE`` so ``_seed_event`` short-circuits and never
    reaches the strict resolver — but the soft router (``record_for``) sees
    the alias resolution. Driving the alias step directly via
    ``primitives_taxonomy.record_for`` gives the cleanest assertion.
    """
    calls: list[str] = []
    original = primitives_taxonomy._resolve_alias

    def _spy(intent_type: str) -> str:
        calls.append(intent_type)
        return original(intent_type)

    monkeypatch.setattr(primitives_taxonomy, "_resolve_alias", _spy)

    # Lowercase canonical: should resolve via _resolve_alias.upper().
    rec = primitives_taxonomy.record_for("lp_open")
    assert rec.intent_type == "LP_OPEN"
    assert "lp_open" in calls

    # Legacy alias path.
    rec_alias = primitives_taxonomy.record_for("VAULT_WITHDRAW")
    assert rec_alias.intent_type == "VAULT_REDEEM"
    assert "VAULT_WITHDRAW" in calls
