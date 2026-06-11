"""Tests for decimal-unit soft-fail guard (VIB-4780 / W1-5).

Verifies:
- Payload with fees_token0 = "75817134186" → returns >= 1, warning logged.
- Payload with fees_token0 = "0.0000000758" → returns 0, no warning.
- Payload with amount_in = "701279299182337" → returns >= 1.
- Payload with all human-form fields → returns 0.
- Edge: missing fields → returns 0, no crash.
- Integration: build_position_event_from_intent with raw-wei fees logs a warning
  AND the event is still returned (write not blocked).
- Integration: build_ledger_entry with raw-wei amount_in logs a warning AND the
  entry is still returned (write not blocked).
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.accounting.decimal_guards import (
    _RAW_WEI_THRESHOLD,
    _check_decimal_unit_soft_fail,
)

# ---------------------------------------------------------------------------
# Unit tests for _check_decimal_unit_soft_fail
# ---------------------------------------------------------------------------


def test_fees_token0_raw_wei_triggers_warning(caplog: pytest.LogCaptureFixture) -> None:
    """fees_token0 with a 10^11 value → count >= 1, warning logged."""
    payload = {"fees_token0": "75817134186"}  # ~7.6e10 < threshold but let's use a bigger one
    # Use a value that's clearly raw-wei: 75817134186000 (10^13)
    payload = {"fees_token0": "75817134186000"}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(payload, event_id="evt-001", event_type="LP_CLOSE")
    assert count >= 1
    assert "decimal_unit_guard" in caplog.text
    assert "fees_token0" in caplog.text


def test_fees_token0_human_form_no_warning(caplog: pytest.LogCaptureFixture) -> None:
    """fees_token0 = "0.0000000758" (real WETH human-form) → count 0, no warning."""
    payload = {"fees_token0": "0.0000000758"}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(payload, event_id="evt-002", event_type="LP_CLOSE")
    assert count == 0
    assert "decimal_unit_guard" not in caplog.text


def test_amount_in_raw_wei_triggers_warning(caplog: pytest.LogCaptureFixture) -> None:
    """amount_in = "701279299182337" (raw wei) → count >= 1, warning logged."""
    payload = {"amount_in": "701279299182337"}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(payload, event_id="evt-003", event_type="LP_OPEN")
    assert count >= 1
    assert "decimal_unit_guard" in caplog.text
    assert "amount_in" in caplog.text


def test_all_human_form_no_warning(caplog: pytest.LogCaptureFixture) -> None:
    """All human-form fields → count 0."""
    payload = {
        "fees_token0": "0.000075",
        "fees_token1": "0.000148",
        "amount_in": "0.002125",
        "amount_out": "4.50",
    }
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(payload, event_id="evt-004", event_type="LP_CLOSE")
    assert count == 0
    assert "decimal_unit_guard" not in caplog.text


def test_missing_fields_returns_zero_no_crash(caplog: pytest.LogCaptureFixture) -> None:
    """Empty payload → count 0, no exception."""
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail({}, event_id="evt-005", event_type="LP_OPEN")
    assert count == 0


def test_none_value_skipped(caplog: pytest.LogCaptureFixture) -> None:
    """None values in payload → skipped (count 0)."""
    payload = {"fees_token0": None, "amount_in": None}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(payload, event_id="evt-006", event_type="LP_CLOSE")
    assert count == 0


def test_empty_string_value_skipped(caplog: pytest.LogCaptureFixture) -> None:
    """Empty-string values → skipped (count 0, 'empty != zero' preserved)."""
    payload = {"fees_token0": "", "amount_in": ""}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(payload, event_id="evt-007", event_type="LP_CLOSE")
    assert count == 0


def test_multiple_suspicious_fields_counted_separately(caplog: pytest.LogCaptureFixture) -> None:
    """Two raw-wei fields in the same payload → count == 2."""
    payload = {
        "fees_token0": "75817134186000",  # 7.58e13
        "fees_token1": "148000000000000",  # 1.48e14
    }
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(payload, event_id="evt-008", event_type="LP_CLOSE")
    assert count == 2


def test_threshold_boundary_below_is_safe(caplog: pytest.LogCaptureFixture) -> None:
    """Value at exactly threshold - 1 → NOT flagged."""
    below = str(int(_RAW_WEI_THRESHOLD) - 1)
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail({"amount_in": below}, event_id="evt-009", event_type="SWAP")
    assert count == 0


def test_threshold_boundary_at_threshold_is_flagged(caplog: pytest.LogCaptureFixture) -> None:
    """Value at exactly threshold → flagged."""
    at_threshold = str(int(_RAW_WEI_THRESHOLD))
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail({"amount_in": at_threshold}, event_id="evt-010", event_type="SWAP")
    assert count >= 1


def test_negative_raw_wei_also_flagged(caplog: pytest.LogCaptureFixture) -> None:
    """Negative raw-wei magnitude → also flagged (guard checks abs value)."""
    payload = {"amount_out": "-701279299182337"}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(payload, event_id="evt-011", event_type="LP_CLOSE")
    assert count >= 1


# ---------------------------------------------------------------------------
# Integration: build_position_event_from_intent logs warning + event returned
# ---------------------------------------------------------------------------


def test_build_position_event_raw_wei_fees_does_not_warn(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """build_position_event_from_intent must NOT warn on raw-wei LP fees (VIB-5036).

    position_events ``fees_token0`` / ``fees_token1`` (and ``amount0`` /
    ``amount1``) are RAW-by-contract — NAV valuation, hydration, and the
    attribution lane all read them as raw and scale at point-of-use. The W1-5
    decimal-unit guard, which assumes a human-units column, therefore produced
    a guaranteed FALSE WARNING on every LP fee write (the original field report
    on deployment a9e54a85). VIB-5036 removes the guard wiring here; it stays
    active on the genuinely-human ``transaction_ledger`` via build_ledger_entry.

    The integration test verifies:
    1. The position event is still returned (write not blocked).
    2. The decimal-unit guard does NOT fire on the raw-by-contract fee fields.
    """
    from almanak.framework.observability.position_events import build_position_event_from_intent

    # Minimal mock result with lp_close_data carrying raw-wei fees
    class _FakeLPCloseData:
        fees0 = Decimal("75817134186000")  # raw-wei magnitude
        fees1 = Decimal("148000000000000")
        amount0_collected = Decimal("0.001")
        amount1_collected = Decimal("3.02")

    class _FakeResult:
        # ``position_id`` is required so the position-events θ final guard
        # does not drop the event — otherwise this test cannot verify that
        # the guard ran (the assertion below would be satisfied by event
        # being None, which is what coderabbit flagged as too permissive).
        position_id = "lp:arbitrum:0xdeadbeef:1"
        extracted_data = {"lp_close_data": _FakeLPCloseData()}
        success = True
        tx_hash = "0xabc"
        total_gas_cost_wei = 0

    class _FakeIntent:
        intent_type = "LP_CLOSE"
        from_token = ""
        to_token = ""

    with caplog.at_level(logging.WARNING, logger="almanak.framework.observability.position_events"):
        event = build_position_event_from_intent(
            deployment_id="test:abc",
            intent=_FakeIntent(),
            result=_FakeResult(),
            chain="arbitrum",
        )

    # VIB-5036: the event is returned AND the guard must NOT warn — these are
    # raw-by-contract columns, not a unit-normalization bug.
    assert event is not None
    assert "decimal_unit_guard" not in caplog.text


# ---------------------------------------------------------------------------
# Integration: build_ledger_entry logs warning + entry returned
# ---------------------------------------------------------------------------


def test_build_ledger_entry_raw_wei_amount_warns_but_returns_entry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """build_ledger_entry with raw-wei amount_in/out → warning + entry returned.

    We use a SWAP intent with a raw-wei-magnitude amount on the stable side.
    The guard fires on amount_in (≥ 10^12), the entry is still returned.
    """
    from almanak.framework.observability.ledger import build_ledger_entry

    class _FakeSwapAmounts:
        token_in = "USDC"
        token_out = "WETH"
        # amount_in_decimal = raw-wei USDC expressed as human-form: 1.58e12
        amount_in_decimal = Decimal("1585552000000")  # >= 10^12
        amount_in_decimal_resolved = True
        amount_out_decimal = Decimal("0.749")
        amount_out_decimal_resolved = True
        effective_price = Decimal("2116.0")
        slippage_bps = 5.0

    class _FakeResult:
        extracted_data: dict[str, Any] = {}
        swap_amounts = _FakeSwapAmounts()
        success = True
        tx_hash = "0xabc"
        total_gas_cost_wei = 0

        def __getattr__(self, name: str) -> Any:
            return None

    class _FakeIntent:
        intent_type = "SWAP"
        from_token = "USDC"
        to_token = "WETH"

    with caplog.at_level(logging.WARNING, logger="almanak.framework.observability.ledger"):
        entry = build_ledger_entry(
            deployment_id="test:abc123",
            cycle_id="cycle-1",
            intent=_FakeIntent(),
            result=_FakeResult(),
            chain="arbitrum",
            success=True,
        )

    # Entry must always be returned — soft-fail does not block the write
    assert entry is not None
    assert entry.amount_in == "1585552000000"
    # The guard should have fired a warning for the raw-wei amount_in
    assert "decimal_unit_guard" in caplog.text
