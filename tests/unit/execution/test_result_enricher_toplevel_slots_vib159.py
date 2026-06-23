"""VIB-159 — enriched fields must reach the strategy callback as top-level slots.

Pre-fix, ``ResultEnricher._attach_to_result`` set ``position_id`` /
``swap_amounts`` / ``lp_close_data`` / ``bridge_data`` directly on the result,
but ``bin_ids`` (declared on ``ExecutionResult`` yet never assigned),
``protocol_fees`` (only rejected the bad case, never attached the good one), and
the connector-declared ``primitive_money_legs`` all fell through to
``result.extracted_data[field] = value`` ONLY. The strategy callback reads
top-level attributes, so those enriched values were unreachable as
``result.bin_ids`` / ``result.protocol_fees`` / ``result.primitive_money_legs``.

The fix gives each a real top-level assignment slot while keeping the
``extracted_data`` entry intact (the ledger dispatcher's
``_declared_money_legs`` fallback still reads ``extracted_data``).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from almanak.connectors._strategy_base.primitive_money_leg import (
    MoneyLegRole,
    PrimitiveMoneyLeg,
    PrimitiveMoneyLegs,
)
from almanak.framework.accounting.measured import MeasuredMoney
from almanak.framework.execution.extracted_data import ProtocolFees
from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult
from almanak.framework.execution.result_enricher import ResultEnricher


def _result() -> ExecutionResult:
    return ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE)


def _enricher() -> ResultEnricher:
    # No registry needed: we exercise _attach_to_result directly.
    return ResultEnricher(parser_registry=None)


def test_bin_ids_attached_to_top_level_slot():
    enricher = _enricher()
    result = _result()

    ok = enricher._attach_to_result(result, "bin_ids", [8388608, 8388609], "LP_OPEN")

    assert ok is True
    # Top-level slot now reachable from the strategy callback.
    assert result.bin_ids == [8388608, 8388609]
    # And still mirrored into extracted_data.
    assert result.extracted_data["bin_ids"] == [8388608, 8388609]


def test_bin_ids_empty_list_is_measured_not_coerced():
    """Empty != Zero: an empty list is a measured "no bins" and is preserved."""
    enricher = _enricher()
    result = _result()

    ok = enricher._attach_to_result(result, "bin_ids", [], "LP_OPEN")

    assert ok is True
    assert result.bin_ids == []
    assert result.extracted_data["bin_ids"] == []


def test_bin_ids_wrong_type_rejected_and_slot_left_none():
    enricher = _enricher()
    result = _result()

    ok = enricher._attach_to_result(result, "bin_ids", "not-a-list", "LP_OPEN")

    assert ok is False
    assert result.bin_ids is None
    assert "bin_ids" not in result.extracted_data


@pytest.mark.parametrize("bad", [[True], [False], [1, True], [True, 2]])
def test_bin_ids_booleans_rejected_and_slot_left_none(bad):
    """``bool`` is an ``int`` subclass, but a boolean is never a valid bin id.

    Without the explicit ``not isinstance(b, bool)`` guard, ``[True]`` /
    ``[False]`` would sail through the ``isinstance(b, int)`` check and reach
    ``result.bin_ids`` / ``extracted_data`` as invalid bin identifiers.
    """
    enricher = _enricher()
    result = _result()

    ok = enricher._attach_to_result(result, "bin_ids", bad, "LP_OPEN")

    assert ok is False
    assert result.bin_ids is None
    assert "bin_ids" not in result.extracted_data


def test_protocol_fees_attached_to_top_level_slot():
    enricher = _enricher()
    result = _result()
    fees = ProtocolFees(total_usd=Decimal("0.05"), swap_fee_usd=Decimal("0.05"))

    ok = enricher._attach_to_result(result, "protocol_fees", fees, "SWAP")

    assert ok is True
    assert result.protocol_fees is fees
    assert result.extracted_data["protocol_fees"] is fees


def test_protocol_fees_wrong_type_rejected_and_slot_left_none():
    enricher = _enricher()
    result = _result()

    ok = enricher._attach_to_result(result, "protocol_fees", {"total_usd": "0.05"}, "SWAP")

    assert ok is False
    assert result.protocol_fees is None
    assert "protocol_fees" not in result.extracted_data


def test_primitive_money_legs_attached_to_top_level_slot():
    enricher = _enricher()
    result = _result()
    legs = PrimitiveMoneyLegs(
        legs=[
            PrimitiveMoneyLeg(
                role=MoneyLegRole.OUTPUT,
                token="USDC",
                amount=MeasuredMoney.measured(Decimal("100")),
            )
        ]
    )

    ok = enricher._attach_to_result(result, "primitive_money_legs", legs, "LP_CLOSE")

    assert ok is True
    # Reachable from the strategy callback as a typed attribute...
    assert result.primitive_money_legs is legs
    # ...AND preserved in extracted_data so ledger._declared_money_legs'
    # extracted_data fallback (VIB-5212/5218) still resolves it.
    assert result.extracted_data["primitive_money_legs"] is legs


def test_primitive_money_legs_wrong_type_rejected_and_slot_left_none():
    enricher = _enricher()
    result = _result()

    ok = enricher._attach_to_result(result, "primitive_money_legs", {"legs": []}, "LP_CLOSE")

    assert ok is False
    assert result.primitive_money_legs is None
    assert "primitive_money_legs" not in result.extracted_data


# ---------------------------------------------------------------------------
# Reject-then-continue-scanning: the runtime contract _attach_to_result's
# ``return False`` exists to serve. A strict-typed field whose first receipt
# yields the wrong type must NOT short-circuit the bundle scan — a valid value
# in a later receipt has to still populate the top-level slot AND the mirror.
# (CodeRabbit on PR #2985: the slot-rejection tests above prove the False, but
# only _extract_field proves the scan keeps going after it.)
# ---------------------------------------------------------------------------


def _valid_protocol_fees() -> ProtocolFees:
    return ProtocolFees(total_usd=Decimal("0.05"), swap_fee_usd=Decimal("0.05"))


def _valid_money_legs() -> PrimitiveMoneyLegs:
    return PrimitiveMoneyLegs(
        legs=[
            PrimitiveMoneyLeg(
                role=MoneyLegRole.OUTPUT,
                token="USDC",
                amount=MeasuredMoney.measured(Decimal("100")),
            )
        ]
    )


class _TwoReceiptParser:
    """Legacy-style parser whose ``extract_{field}`` returns a different value
    per receipt: a wrong-typed value first, then a valid one.

    Returning raw values (not ExtractOk/Missing/Error) drives the enricher's
    legacy-wrapping path in ``_invoke_extract``, so each non-None raw value is
    wrapped as ExtractOk and offered to ``_attach_to_result`` in receipt order.
    """

    def __init__(self, field: str, values: list[Any]) -> None:
        self.SUPPORTED_EXTRACTIONS = {field}
        self._method_name = f"extract_{field}"
        self._values = iter(values)
        setattr(self, self._method_name, self._extract)

    def _extract(self, receipt: dict[str, Any]) -> Any:
        return next(self._values)


@pytest.mark.parametrize(
    ("field", "attr", "intent_type", "wrong", "valid_factory"),
    [
        ("bin_ids", "bin_ids", "LP_OPEN", "not-a-list", lambda: [8388608, 8388609]),
        ("protocol_fees", "protocol_fees", "SWAP", {"total_usd": "0.05"}, _valid_protocol_fees),
        ("primitive_money_legs", "primitive_money_legs", "LP_CLOSE", {"legs": []}, _valid_money_legs),
    ],
)
def test_strict_typed_field_rejection_continues_to_later_receipt(field, attr, intent_type, wrong, valid_factory):
    enricher = _enricher()
    result = _result()
    valid = valid_factory()
    parser = _TwoReceiptParser(field, [wrong, valid])

    # Two receipts: #1 yields the wrong type (rejected, scan continues), #2
    # yields the valid typed value (attached to the top-level slot + mirror).
    enricher._extract_field(result, parser, [{}, {}], field, intent_type)

    assert getattr(result, attr) == valid
    assert result.extracted_data[field] == valid
