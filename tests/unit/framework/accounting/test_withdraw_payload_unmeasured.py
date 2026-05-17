"""VIB-4539: WithdrawEventPayload tolerates unmeasured amount.

Same Empty ≠ Zero rule as the SwapEventPayload widening tracked under
VIB-4490 / PR #2338. Without this fix, the projection helper at
``accountant_test.py:_project_payload_for_v1_validation`` cannot supply
an ``amount`` key when the writer emitted ``amount_token=None``, and the
schema's ``Decimal`` requirement causes the row to fail Pydantic
validation and block G6 / G13 / L1 / L4 / L6 with "cell data unusable".

The Morpho receipt parser cannot always resolve the assets amount when
shares-mode withdraws are used or when loan_token decimals are
unresolved; ``None`` is a valid measured-unmeasured state and must
flow end-to-end through validation.

See ``docs/internal/MorphoStatusMay17.md`` Implementation Plan Item 3.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from almanak.framework.accounting.accountant_test import (
    _project_payload_for_v1_validation,
)
from almanak.framework.accounting.payload_schemas import (
    WithdrawEventPayload,
    validate_payload,
)


def _base_kwargs() -> dict:
    """Minimum required fields for ``WithdrawEventPayload`` without amount."""
    return {
        "protocol": "morpho_blue",
        "asset": "wstETH",
        "confidence": "ESTIMATED",
    }


class TestWithdrawAmountUnmeasured:
    def test_amount_none_validates(self) -> None:
        """Teardown WITHDRAW path: parser couldn't decode the assets amount.
        ``None`` must be acceptable per AGENTS.md Empty ≠ Zero."""
        p = WithdrawEventPayload(
            **_base_kwargs(),
            amount=None,
            unavailable_reason=(
                "withdraw amount unmeasured (Morpho receipt parser could not "
                "resolve token decimals)"
            ),
        )
        assert p.amount is None
        assert "unmeasured" in (p.unavailable_reason or "")

    def test_amount_zero_decimal_still_validates(self) -> None:
        """Measured zero is distinct from None (Empty ≠ Zero rule)."""
        p = WithdrawEventPayload(**_base_kwargs(), amount=Decimal("0"))
        assert p.amount == Decimal("0")
        assert p.unavailable_reason is None

    def test_amount_omitted_raises(self) -> None:
        """Audit PR #2343 (CodeRabbit): the schema requires the ``amount``
        key to be present (default ``...`` / Ellipsis), even though it
        accepts ``None`` as a value. Omitting the key entirely indicates
        a parser bug ('``""``' shape per AGENTS.md Empty ≠ Zero) and
        must FAIL loudly rather than silently fill in ``None``."""
        with pytest.raises(ValidationError):
            WithdrawEventPayload(**_base_kwargs())

    def test_empty_string_amount_rejected(self) -> None:
        """Parser-empty ('') is stronger than measured-unmeasured; reject.

        Empty ≠ Zero distinguishes Decimal('0') (measured), None
        (measured-but-unavailable), and '' (parser bug — never emit). The
        first two are valid states; the third is a contract violation.
        Asserts the specific Pydantic error class so an unrelated
        exception (e.g. an unrelated import failure) doesn't accidentally
        satisfy the test (CodeRabbit review on PR #2343)."""
        with pytest.raises(ValidationError):
            WithdrawEventPayload(**_base_kwargs(), amount="")

    def test_validate_payload_chokepoint_accepts_unmeasured_withdraw(self) -> None:
        """End-to-end via ``validate_payload`` — the production entry point
        used by the writer chokepoint and by the Accountant Test's typed
        payload reader."""
        result = validate_payload(
            "WITHDRAW",
            {
                "event_type": "WITHDRAW",
                "protocol": "morpho_blue",
                "asset": "wstETH",
                "amount": None,
                "confidence": "ESTIMATED",
                "unavailable_reason": "decimals unresolved",
            },
        )
        assert result is not None
        assert result.amount is None

    def test_projection_pipeline_forwards_none_amount_token(self) -> None:
        """The writer emits ``amount_token``; the projection at
        ``_project_payload_for_v1_validation`` currently aliases
        ``amount_token → amount`` only when ``amount_token is not None``.
        After this fix, a writer row with ``amount_token=None`` must
        produce a projected payload that validates against the (widened)
        schema — whether the projection sets ``amount=None`` explicitly
        or leaves the key absent (schema default fills in None).

        Asserting at the model level lets the projection implementation
        pick either shape without forcing a contract on this test."""
        row = {"protocol": "morpho_blue", "event_type": "WITHDRAW"}
        writer_payload = {
            "event_type": "WITHDRAW",
            "protocol": "morpho_blue",
            "asset": "wstETH",
            "amount_token": None,
            "confidence": "ESTIMATED",
            "unavailable_reason": "decimals unresolved",
        }
        projected = _project_payload_for_v1_validation(writer_payload, row)
        # Whichever shape the projection chose, the validated model must
        # carry amount=None — the saved DB's failing WITHDRAW rows go
        # through this exact pipeline.
        model = WithdrawEventPayload.model_validate(projected)
        assert model.amount is None

    def test_measured_amount_still_aliased_through_projection(self) -> None:
        """Regression: when ``amount_token`` IS measured, the projection
        must still forward it to ``amount`` (the v1 spec name). This is the
        pre-VIB-4539 happy path — must not regress."""
        row = {"protocol": "morpho_blue", "event_type": "WITHDRAW"}
        writer_payload = {
            "event_type": "WITHDRAW",
            "protocol": "morpho_blue",
            "asset": "wstETH",
            "amount_token": Decimal("0.014"),
            "confidence": "HIGH",
        }
        projected = _project_payload_for_v1_validation(writer_payload, row)
        model = WithdrawEventPayload.model_validate(projected)
        assert model.amount == Decimal("0.014")
