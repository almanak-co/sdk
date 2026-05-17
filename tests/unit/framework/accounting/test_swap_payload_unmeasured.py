"""VIB-4490: SwapEventPayload tolerates unmeasured amount_in / amount_out.

When the receipt parser cannot resolve token decimals (or a teardown sell-back
hits a path the parser does not decode), it emits ``None`` for amount_in and/or
amount_out and stamps the reason on ``unavailable_reason``. Per the framework
rule Empty ≠ Zero (AGENTS.md §Accounting), ``None`` is a valid measured-but-
unmeasured state; the schema must accept it.

Pre-fix the schema required ``Decimal`` for both, so the writer's
``_typed_acct_payloads`` step rejected the whole row with a Pydantic error and
G6 / G13 / L6 short-circuited to FAIL on "cell data unusable" even when the
SWAP's amount_in side was measured and reconcilable.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from almanak.framework.accounting.payload_schemas import (
    SwapEventPayload,
    validate_payload,
)


def _base_kwargs() -> dict:
    return {
        "protocol": "uniswap_v3",
        "token_in": "wstETH",
        "token_out": "USDC",
        "confidence": "ESTIMATED",
    }


def test_amount_out_none_validates() -> None:
    """Teardown sell-back: parser couldn't decode amount_out."""
    p = SwapEventPayload(
        **_base_kwargs(),
        amount_in=Decimal("0.0034"),
        amount_out=None,
        unavailable_reason="swap amounts unmeasured (token decimals could not be resolved by receipt parser)",
    )
    assert p.amount_in == Decimal("0.0034")
    assert p.amount_out is None
    assert "unmeasured" in (p.unavailable_reason or "")


def test_both_amounts_none_validates() -> None:
    """Fully unmeasured SWAP — fail-loud reason carries the audit trail."""
    p = SwapEventPayload(
        **_base_kwargs(),
        amount_in=None,
        amount_out=None,
        unavailable_reason="receipt parsing failed entirely",
    )
    assert p.amount_in is None
    assert p.amount_out is None


def test_amount_in_none_validates() -> None:
    """Asymmetric case (rare but legitimate when only the output decimals decode)."""
    p = SwapEventPayload(
        **_base_kwargs(),
        amount_in=None,
        amount_out=Decimal("18.85"),
        unavailable_reason="amount_in decimals unresolved",
    )
    assert p.amount_in is None
    assert p.amount_out == Decimal("18.85")


def test_measured_payload_still_validates() -> None:
    """The happy path is unchanged — fully measured swap validates as before."""
    p = SwapEventPayload(
        **_base_kwargs(),
        amount_in=Decimal("18.85"),
        amount_out=Decimal("0.0069"),
        amount_in_usd=Decimal("18.85"),
        amount_out_usd=Decimal("18.85"),
    )
    assert p.amount_in == Decimal("18.85")
    assert p.amount_out == Decimal("0.0069")


def test_unmeasured_payload_passes_writer_validation_chokepoint() -> None:
    """End-to-end: the writer's validate_payload() (the actual entry point used
    by _typed_acct_payloads) accepts the unmeasured shape — proves the fix
    flows through to the production-path validator, not just the Pydantic
    model in isolation."""
    payload = {
        "event_type": "SWAP",
        "protocol": "uniswap_v3",
        "token_in": "wstETH",
        "token_out": "USDC",
        "amount_in": "0.0034",
        "amount_out": None,
        "confidence": "ESTIMATED",
        "unavailable_reason": "swap amounts unmeasured (token decimals could not be resolved by receipt parser)",
    }
    # Should NOT raise.
    result = validate_payload("SWAP", payload)
    assert result is not None
    assert result.amount_in == Decimal("0.0034")
    assert result.amount_out is None


def test_empty_string_amount_out_is_not_accepted() -> None:
    """Empty-string is parser-didn't-emit territory and should NOT silently
    coerce to None — Pydantic rejects it. This preserves the "" ≠ None rule
    per AGENTS.md (parser-empty is a stronger signal than measured-unmeasured).

    Asserts the specific ``ValidationError`` (CodeRabbit audit PR #2338) so an
    unrelated exception (e.g. an import-time failure) does not accidentally
    satisfy the assertion.
    """
    with pytest.raises(ValidationError):
        SwapEventPayload(
            **_base_kwargs(),
            amount_in=Decimal("0.0034"),
            amount_out="",
        )


def test_amount_in_key_omitted_raises() -> None:
    """Codex audit PR #2338 — Empty ≠ Zero requires that the writer
    EXPLICITLY emits ``amount_in``, even if its value is ``None``. Dropping
    the key entirely is contract drift (the '``""``' shape per AGENTS.md
    Empty ≠ Zero) and must FAIL loud rather than silently fill in ``None``
    via a Pydantic default. The fields are widened to ``Decimal | None``
    via ``Field(...)`` precisely to forbid that silent-default path.
    """
    with pytest.raises(ValidationError):
        SwapEventPayload(
            **_base_kwargs(),
            amount_out=Decimal("18.85"),
            unavailable_reason="amount_in decimals unresolved",
        )


def test_amount_out_key_omitted_raises() -> None:
    """Mirror of ``test_amount_in_key_omitted_raises`` for ``amount_out``."""
    with pytest.raises(ValidationError):
        SwapEventPayload(
            **_base_kwargs(),
            amount_in=Decimal("0.0034"),
            unavailable_reason="amount_out decimals unresolved",
        )


def test_amount_none_without_reason_raises() -> None:
    """Gemini audit PR #2338 — when an amount is ``None``, the schema
    requires ``unavailable_reason`` to be populated (Empty ≠ Zero audit
    trail). The production-path SWAP writer
    (``swap_handler._determine_confidence``) already does this — the
    validator is a structural safety net for future writers.
    """
    with pytest.raises(ValidationError):
        SwapEventPayload(
            **_base_kwargs(),
            amount_in=Decimal("0.0034"),
            amount_out=None,
        )


def test_both_amounts_none_without_reason_raises() -> None:
    """Mirror of ``test_amount_none_without_reason_raises`` — both legs
    unmeasured still requires a reason. Covers the fully-degraded path."""
    with pytest.raises(ValidationError):
        SwapEventPayload(
            **_base_kwargs(),
            amount_in=None,
            amount_out=None,
        )
