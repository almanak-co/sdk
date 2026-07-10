"""ALL_ACCOUNTING_EVENT_TYPES whitelist contract (VIB-4164, T4).

The gateway's `SaveAccountingEvent` whitelist at
`almanak/gateway/services/state_service.py:1396` is built from
`almanak.framework.accounting.models.ALL_ACCOUNTING_EVENT_TYPES`. T4 widens this
union to include `TransferEventType` so the gateway accepts
`event_type='TRANSFER'`. The Phase 1 spec critique (Codex round 1) flagged that
a *membership* check is too weak — the whitelist could be silently widened by
literal strings. These tests pin the whitelist's exact shape against the
typed-StrEnum union so a future PR that smuggles in a literal or drops one of
the existing 8 enum families turns red.
"""

from __future__ import annotations

import pytest

from almanak.framework.accounting.models import (
    ALL_ACCOUNTING_EVENT_TYPES,
    LendingEventType,
    LPEventType,
    PendleEventType,
    PerpEventType,
    PredictionEventType,
    SettlementEventType,
    SwapEventType,
    TransferEventType,
    VaultEventType,
)


def _expected_union() -> frozenset[str]:
    """Compute the expected whitelist from the typed StrEnums declared in models.py."""
    return frozenset(
        e.value
        for cls in (
            LendingEventType,
            PendleEventType,
            LPEventType,
            PerpEventType,
            VaultEventType,
            SettlementEventType,
            SwapEventType,
            PredictionEventType,
            TransferEventType,
        )
        for e in cls
    )


def test_transfer_in_whitelist() -> None:
    """D1.S3 (positive) — `TransferEventType.TRANSFER` is now accepted by the gateway whitelist."""
    assert "TRANSFER" in ALL_ACCOUNTING_EVENT_TYPES
    assert TransferEventType.TRANSFER.value in ALL_ACCOUNTING_EVENT_TYPES


def test_transfer_event_type_size_pinned() -> None:
    """Defensive supplement to the union-equality test.

    Both sides of ``test_whitelist_equals_typed_enum_union_exactly`` derive
    from the same enum import, so adding e.g. ``TRANSFER_OUT`` to
    ``TransferEventType`` would silently widen the gateway whitelist with
    both expected and actual moving in lockstep. This test pins the absolute
    size to 1 so any addition forces a deliberate update here AND a
    security-perimeter review per AGENTS.md "Hosted Deployment Awareness".
    """
    assert len(TransferEventType) == 1, (
        f"TransferEventType has {len(TransferEventType)} members; expected 1. "
        "Adding a member is a hosted security-perimeter widening (Infra label). "
        "Update this test deliberately and confirm state_service.py:1396 still "
        "behaves correctly with the new value."
    )


def test_whitelist_equals_typed_enum_union_exactly() -> None:
    """D1.S3 (exact-equality, security-perimeter contract).

    The frozenset must equal the union of every member of the 8 declared
    StrEnums — no extra strings, no enum family dropped. A regression that
    *widens* the whitelist beyond the typed-enum union (perimeter regression)
    or *drops* one of the 8 enum families (silent rejection of legitimate
    events) trips this test.
    """
    expected = _expected_union()
    assert ALL_ACCOUNTING_EVENT_TYPES == expected, (
        f"Whitelist drift: extras={sorted(ALL_ACCOUNTING_EVENT_TYPES - expected)!r}, "
        f"missing={sorted(expected - ALL_ACCOUNTING_EVENT_TYPES)!r}"
    )


def test_whitelist_is_exactly_the_typed_enum_union() -> None:
    """D3.F2 (size-equality form). The size must match the sum of enum-family sizes."""
    expected_size = sum(
        len(cls)
        for cls in (
            LendingEventType,
            PendleEventType,
            LPEventType,
            PerpEventType,
            VaultEventType,
            SettlementEventType,
            SwapEventType,
            PredictionEventType,
            TransferEventType,
        )
    )
    assert len(ALL_ACCOUNTING_EVENT_TYPES) == expected_size


@pytest.mark.parametrize(
    "garbage",
    [
        "TRANSFER_GARBAGE",
        "transfer",  # case-smuggling sentinel — StrEnum values are uppercase
        "TRANS",
        "BRIDGE",  # the IntentType key — must NOT be in the whitelist (gateway accepts event_types only)
        "",
        "TRANSFER_OUT",
        "TRANSFER_IN",
    ],
)
def test_garbage_event_type_still_rejected(garbage: str) -> None:
    """D3.F2 (negative-control). Adding TRANSFER did NOT widen the whitelist beyond the new enum."""
    assert garbage not in ALL_ACCOUNTING_EVENT_TYPES


def test_gateway_state_service_imports_same_constant() -> None:
    """The gateway's `_ALL_ACCOUNTING_EVENT_TYPES` (built at module load in state_service.py)
    is imported from this same models.ALL_ACCOUNTING_EVENT_TYPES, so the contract carries to
    the gateway boundary without a separate test on the gateway side.
    """
    from almanak.gateway.services.state_service import _ALL_ACCOUNTING_EVENT_TYPES

    assert _ALL_ACCOUNTING_EVENT_TYPES == ALL_ACCOUNTING_EVENT_TYPES, (
        "Gateway whitelist diverged from models.ALL_ACCOUNTING_EVENT_TYPES — "
        "the import in state_service.py:55-58 must remain the single source of truth."
    )
    assert "TRANSFER" in _ALL_ACCOUNTING_EVENT_TYPES
