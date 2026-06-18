"""VIB-5213 (US-007) — MeasuredMoney at the accounting-payload boundary.

The accounting-payload boundary codec carries the Empty≠Zero distinction
(measured-zero / unmeasured / parser-absent) explicitly across the
serialize→deserialize seam into typed accounting events, while keeping the
PERSISTED on-the-wire representation byte-compatible with the legacy
``Decimal`` → value / ``None`` → null / ``""`` → absent column semantics.

Contract under test:

1. ``MeasuredMoney.to_payload`` / ``.from_payload`` round-trip preserves all
   three states; ``""`` is NEVER coerced to ``Decimal("0")``.
2. ``encode_money_payload`` / ``decode_money_payload`` (the legacy-field
   bridge) are byte-compatible with the historical encode/decode for finite
   Decimals and ``None``, and map a stray ``""`` to ``None`` (not zero).
3. Typed accounting events round-trip through ``to_payload_json`` /
   ``from_payload_json`` preserving measured-zero vs unmeasured — never
   conflated — with version stamps intact.
4. The persisted bytes are unchanged (measured → ``str``, unmeasured →
   JSON ``null``).
5. ``AccountingWriter`` remains the only writer (typed models never call
   ``save_accounting_event``).
"""

from __future__ import annotations

import inspect
import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.accounting import lp_accounting as lp_module
from almanak.framework.accounting import models as models_module
from almanak.framework.accounting import perp_accounting as perp_module
from almanak.framework.accounting import vault_accounting as vault_module
from almanak.framework.accounting.measured import (
    MeasuredMoney,
    MeasuredState,
    decode_money_payload,
    encode_money_payload,
)
from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    LendingAccountingEvent,
    LendingEventType,
    SwapAccountingEvent,
    SwapEventType,
)

_NOW = datetime(2026, 6, 18, tzinfo=UTC)


def _identity(*, mode: str = "paper") -> AccountingIdentity:
    return AccountingIdentity(
        id=str(uuid.uuid4()),
        deployment_id="strat-vib-5213",
        cycle_id="cycle-1",
        execution_mode=mode,
        timestamp=_NOW,
        chain="arbitrum",
        protocol="test_proto",
        wallet_address="0x" + "0" * 40,
        tx_hash="0x" + "1" * 64,
        ledger_entry_id="le-1",
    )


# --------------------------------------------------------------------------- #
# 1. MeasuredMoney.to_payload / from_payload — the boundary codec
# --------------------------------------------------------------------------- #


def test_to_payload_maps_three_states_to_legacy_wire_form() -> None:
    assert MeasuredMoney.measured(Decimal("1.5")).to_payload() == "1.5"
    # measured zero is a VALUE — serializes to the string "0", never null/"".
    assert MeasuredMoney.measured(Decimal("0")).to_payload() == "0"
    assert MeasuredMoney.unmeasured().to_payload() is None
    assert MeasuredMoney.absent().to_payload() == ""


def test_from_payload_maps_legacy_wire_form_to_three_states() -> None:
    assert MeasuredMoney.from_payload("1.5") == MeasuredMoney.measured(Decimal("1.5"))
    assert MeasuredMoney.from_payload("0") == MeasuredMoney.measured(Decimal("0"))
    assert MeasuredMoney.from_payload(None).is_unmeasured
    assert MeasuredMoney.from_payload("").is_absent


def test_from_payload_never_coerces_empty_to_measured_zero() -> None:
    absent = MeasuredMoney.from_payload("")
    assert absent.state is MeasuredState.ABSENT
    assert not absent.is_measured
    # The absent state carries no amount and is NOT equal to a measured zero.
    assert absent != MeasuredMoney.measured(Decimal("0"))
    assert absent != MeasuredMoney.unmeasured()


@pytest.mark.parametrize(
    "mm",
    [
        MeasuredMoney.measured(Decimal("0")),
        MeasuredMoney.measured(Decimal("-12.3456789")),
        MeasuredMoney.measured(Decimal("1E-8")),
        MeasuredMoney.unmeasured(),
        MeasuredMoney.absent(),
    ],
)
def test_codec_round_trip_preserves_state(mm: MeasuredMoney) -> None:
    """measured-zero ≠ unmeasured ≠ absent must all survive serialize→deserialize."""
    assert MeasuredMoney.from_payload(mm.to_payload()) == mm


def test_three_states_are_mutually_distinct_through_round_trip() -> None:
    states = [
        MeasuredMoney.measured(Decimal("0")),
        MeasuredMoney.unmeasured(),
        MeasuredMoney.absent(),
    ]
    round_tripped = [MeasuredMoney.from_payload(s.to_payload()) for s in states]
    # All three remain pairwise distinct — none collapsed into another.
    assert len({(r.state, r.amount) for r in round_tripped}) == 3


# --------------------------------------------------------------------------- #
# 2. encode_money_payload / decode_money_payload — legacy Decimal|None bridge
# --------------------------------------------------------------------------- #


def test_encode_money_payload_is_byte_compatible_with_legacy_encode() -> None:
    # Legacy encode was: ``str(v) if isinstance(v, Decimal) else None``.
    for v in (Decimal("0"), Decimal("123.456"), Decimal("-7"), Decimal("1E-9")):
        assert encode_money_payload(v) == str(v)
    assert encode_money_payload(None) is None


def test_decode_money_payload_is_byte_compatible_with_legacy_decode() -> None:
    # Legacy decode was: ``Decimal(v) if v is not None else None``.
    assert decode_money_payload("0") == Decimal("0")
    assert decode_money_payload("123.456") == Decimal("123.456")
    assert decode_money_payload(None) is None


def test_decode_money_payload_accepts_legacy_json_numbers() -> None:
    # Legacy rows may have stored money as a raw JSON number (``int`` / ``float``);
    # the historical ``Decimal(v)`` decode accepted those, so the read boundary
    # must too (converting via ``Decimal(str(x))`` to avoid float artifacts)
    # rather than crashing with a TypeError.
    assert decode_money_payload(0) == Decimal("0")
    assert decode_money_payload(123) == Decimal("123")
    assert decode_money_payload(123.456) == Decimal("123.456")
    # bool is an int subclass but is never money — it must still be rejected.
    with pytest.raises(TypeError):
        decode_money_payload(True)


def test_decode_money_payload_absent_maps_to_none_never_zero() -> None:
    # A stray "" (parser-absent) collapses to None at the legacy Decimal|None
    # field — NEVER to Decimal("0"). The old decode crashed on "".
    assert decode_money_payload("") is None
    assert decode_money_payload("") != Decimal("0")


def test_encode_decode_round_trip_preserves_measured_zero_vs_unmeasured() -> None:
    assert decode_money_payload(encode_money_payload(Decimal("0"))) == Decimal("0")
    assert decode_money_payload(encode_money_payload(None)) is None


def test_encode_money_payload_rejects_non_finite() -> None:
    # Non-finite money is corruption; routing through MeasuredMoney fails closed
    # instead of persisting "NaN" / "Infinity" into the books.
    for bad in (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")):
        with pytest.raises(ValueError):
            encode_money_payload(bad)


# --------------------------------------------------------------------------- #
# 3. Typed event round-trip — measured-zero vs unmeasured never conflated
# --------------------------------------------------------------------------- #


def _swap_event(*, amount_in, amount_out, amount_in_usd) -> SwapAccountingEvent:
    return SwapAccountingEvent(
        identity=_identity(),
        event_type=SwapEventType.SWAP,
        protocol="enso",
        token_in="USDC",
        token_out="WETH",
        amount_in=amount_in,
        amount_out=amount_out,
        amount_in_usd=amount_in_usd,
        amount_out_usd=None,
        effective_price=None,
        slippage_bps=None,
        realized_pnl_usd=None,
        cost_basis_recorded=False,
        gas_usd=None,
        confidence=AccountingConfidence.HIGH,
        unavailable_reason="",
    )


def test_swap_event_round_trip_preserves_measured_zero_and_unmeasured() -> None:
    # amount_in = measured zero (a real, measured 0), amount_out = unmeasured,
    # amount_in_usd = a measured value.
    ev = _swap_event(amount_in=Decimal("0"), amount_out=None, amount_in_usd=Decimal("99.5"))
    restored = SwapAccountingEvent.from_payload_json(ev.identity, ev.to_payload_json())

    # measured zero stays Decimal("0") — NOT conflated with None.
    assert restored.amount_in == Decimal("0")
    assert restored.amount_in is not None
    # unmeasured stays None — NOT conflated with Decimal("0").
    assert restored.amount_out is None
    assert restored.amount_in_usd == Decimal("99.5")


def test_swap_event_payload_bytes_are_unchanged() -> None:
    ev = _swap_event(amount_in=Decimal("0"), amount_out=None, amount_in_usd=Decimal("99.5"))
    d = json.loads(ev.to_payload_json())
    # measured zero → the string "0"; unmeasured → JSON null (None).
    assert d["amount_in"] == "0"
    assert d["amount_out"] is None
    assert d["amount_in_usd"] == "99.5"


def test_lending_event_round_trip_preserves_distinction_and_version_stamps() -> None:
    ev = LendingAccountingEvent(
        identity=_identity(),
        event_type=LendingEventType.SUPPLY,
        position_key="pos-1",
        market_id="mkt-1",
        asset="USDC",
        collateral_value_before_usd=Decimal("0"),  # measured zero
        collateral_value_after_usd=None,  # unmeasured
        debt_value_before_usd=None,
        debt_value_after_usd=Decimal("0"),
        net_equity_before_usd=None,
        net_equity_after_usd=Decimal("123.45"),
        health_factor_before=None,
        health_factor_after=Decimal("2.5"),
        liquidation_threshold=None,
        lltv=None,
        supply_apr_bps=None,
        borrow_apr_bps=None,
        principal_delta_usd=None,
        interest_delta_usd=None,
        gas_usd=None,
    )
    restored = LendingAccountingEvent.from_payload_json(ev.identity, ev.to_payload_json())

    assert restored.collateral_value_before_usd == Decimal("0")  # measured zero kept
    assert restored.collateral_value_after_usd is None  # unmeasured kept
    assert restored.debt_value_after_usd == Decimal("0")
    assert restored.net_equity_after_usd == Decimal("123.45")
    assert restored.health_factor_after == Decimal("2.5")
    assert restored.health_factor_before is None
    # Version stamps survive the round-trip and remain mandatory ints.
    assert isinstance(restored.schema_version, int)
    assert isinstance(restored.primitive_version, int)


# --------------------------------------------------------------------------- #
# 4. AccountingWriter remains the only writer
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "module",
    [models_module, lp_module, perp_module, vault_module],
)
def test_typed_event_modules_never_call_save_accounting_event(module) -> None:
    """The boundary codec lives in the typed-event modules; it must not add a
    second write path. AccountingWriter (via the state backends) stays the
    only caller of ``save_accounting_event``.
    """
    src = inspect.getsource(module)
    assert "save_accounting_event" not in src
