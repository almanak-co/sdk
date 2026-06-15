"""VIB-5117: LPCloseEventPayload tolerates an unmeasured (None) principal leg.

Same Empty ≠ Zero rule as the WithdrawEventPayload widening (VIB-4539). A
Uniswap V4 NATIVE close leg is returned to the wallet as raw ETH (TAKE_PAIR)
with NO Transfer, so the burn-receipt parser leaves ``amount{0,1}_collected``
``None`` (unmeasured). The runner normally fills it from a pre-burn
``QueryV4PositionState`` read (``_stamp_v4_lp_close_native_principal``); but if
that read genuinely fails the leg stays ``None``. Forcing it to a fabricated
zero would understate realized PnL by the full native principal, so the schema
must accept explicit ``None`` while still FAILing loud on a missing key (the
``""`` parser-bug shape).

Before this widening such a row failed Pydantic validation with
``Decimal input should be an integer ... input_value=None``, blocking
G6 / G13 / LP4 on the Accountant Test.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from almanak.framework.accounting.payload_schemas import (
    LPCloseEventPayload,
    validate_payload,
)


def _base_kwargs() -> dict:
    """Minimum required fields for ``LPCloseEventPayload`` without amounts."""
    return {
        "protocol": "uniswap_v4",
        "position_key": "lp:uniswap_v4:base:0xabc:0xpool",
        "pool_address": "0x" + "a" * 64,
        "token0": "ETH",
        "token1": "USDC",
        "confidence": "ESTIMATED",
    }


class TestLPCloseAmountUnmeasured:
    def test_native_leg_none_validates(self) -> None:
        """Native close leg: pre-burn read failed → amount0 unmeasured.

        ``None`` must be acceptable per AGENTS.md Empty ≠ Zero — and distinct
        from a measured zero on the ERC-20 leg.
        """
        p = LPCloseEventPayload(
            **_base_kwargs(),
            amount0=None,
            amount1=Decimal("2.25"),
            unavailable_reason="native principal unmeasured (pre-burn position read failed)",
        )
        assert p.amount0 is None
        assert p.amount1 == Decimal("2.25")

    def test_measured_zero_distinct_from_none(self) -> None:
        """An ERC-20 leg that truly withdrew nothing is a measured zero."""
        p = LPCloseEventPayload(**_base_kwargs(), amount0=Decimal("0"), amount1=Decimal("2.25"))
        assert p.amount0 == Decimal("0")
        # Measured zero is NOT None — the two states stay distinguishable.
        assert p.amount0 is not None

    def test_both_legs_measured_happy_path(self) -> None:
        """The normal path: the runner filled the native leg from the pre-burn read."""
        p = LPCloseEventPayload(**_base_kwargs(), amount0=Decimal("0.00109"), amount1=Decimal("2.25"))
        assert p.amount0 == Decimal("0.00109")
        assert p.amount1 == Decimal("2.25")

    def test_amount0_omitted_raises(self) -> None:
        """The schema requires the ``amount0`` KEY (Field(...)), even though it
        accepts ``None`` as a value. Omitting the key is the ``""`` parser-bug
        shape and must FAIL loud (mirror of the WithdrawEventPayload contract)."""
        with pytest.raises(ValidationError):
            LPCloseEventPayload(**_base_kwargs(), amount1=Decimal("2.25"))

    def test_amount1_omitted_raises(self) -> None:
        with pytest.raises(ValidationError):
            LPCloseEventPayload(**_base_kwargs(), amount0=Decimal("0.001"))

    def test_empty_string_amount_rejected(self) -> None:
        """Parser-empty ('') is stronger than measured-unmeasured; reject."""
        with pytest.raises(ValidationError):
            LPCloseEventPayload(**_base_kwargs(), amount0="", amount1=Decimal("2.25"))

    def test_validate_payload_chokepoint_accepts_unmeasured_native_leg(self) -> None:
        """End-to-end via ``validate_payload`` — the production entry point used
        by the writer chokepoint and the Accountant Test's typed payload reader."""
        result = validate_payload(
            "LP_CLOSE",
            {
                "event_type": "LP_CLOSE",
                "protocol": "uniswap_v4",
                "position_key": "lp:uniswap_v4:base:0xabc:0xpool",
                "pool_address": "0x" + "a" * 64,
                "token0": "ETH",
                "token1": "USDC",
                "amount0": None,
                "amount1": "2.25",
                "realized_pnl_usd": "-1.76",
                "confidence": "ESTIMATED",
                "unavailable_reason": "native principal unmeasured",
            },
        )
        assert result is not None
        assert result.amount0 is None
        assert result.amount1 == Decimal("2.25")
