"""VIB-5941 — perp accounting payloads carry intent-known ``is_long`` + ``size``.

Before this fix the GMX V2 perp category handler emitted payloads with
``is_long=None`` and the size under the wrong key (``size_usd`` instead of the
schema's ``size``), so every PERP_OPEN / PERP_CLOSE row FAILed its own frozen
Pydantic schema — silently blocking G6 / G13 / P3 / P5 on the perp Accountant
Test — and no ``position_events`` row was ever written (the perp had no stable
``position_id`` so the θ final-guard dropped it), leaving P1 XFAIL.

These tests exercise the REAL builder path end to end:

    runner seam (``_stamp_perp_intent_fields``)  →  extracted_data
        →  perp category handler (``handle_perp``)  →  typed payload
        →  frozen schema (``validate_payload``)

plus ``build_position_event_from_intent`` for the position_events lane.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.accounting.category_handlers.perp_handler import handle_perp
from almanak.framework.accounting.payload_schemas import validate_payload
from almanak.framework.intents.perp_intents import PerpCloseIntent, PerpOpenIntent
from almanak.framework.observability.position_events import build_position_event_from_intent
from almanak.framework.runner.strategy_runner import StrategyRunner


class _Result:
    """Minimal execution-result double carrying a mutable ``extracted_data``."""

    def __init__(self, extracted: dict[str, Any] | None = None, tx_hash: str = "0xabc") -> None:
        self.extracted_data = extracted if extracted is not None else {}
        self.tx_hash = tx_hash
        self.transaction_results: list[Any] = []
        self.position_id = ""


def _outbox(intent_type: str, market: str = "eth/usd") -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "ledger_entry_id": "led-1",
        "intent_type": intent_type,
        "wallet_address": "0xWALLET",
        "position_key": f"perp:gmx_v2:arbitrum:0xwallet:{market}",
        "market_id": market,
        "status": "pending",
        "attempts": 0,
        "error": "",
        "created_at": datetime.now(UTC).isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _ledger(intent_type: str, extracted: dict[str, Any], token_in: str = "USDC", amount_in: str = "10") -> dict[str, Any]:
    return {
        "id": "led-1",
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "timestamp": datetime.now(UTC).isoformat(),
        "intent_type": intent_type,
        "token_in": token_in,
        "amount_in": amount_in,
        "token_out": "",
        "amount_out": "",
        "effective_price": "",
        "slippage_bps": None,
        "gas_used": 0,
        "gas_usd": "0.01",
        "tx_hash": "0xabc",
        "chain": "arbitrum",
        "protocol": "gmx_v2",
        "success": True,
        "error": "",
        "extracted_data_json": json.dumps(extracted),
        "price_inputs_json": "",
        "pre_state_json": "",
        "post_state_json": "",
    }


def _stamp(intent: Any) -> dict[str, Any]:
    """Run the real runner seam and return the resulting extracted_data."""
    result = _Result()
    StrategyRunner._stamp_perp_requested_leverage(result, intent)
    StrategyRunner._stamp_perp_intent_fields(result, intent)
    return result.extracted_data


# ── The runner seam stamps the intent-known identity fields ──────────────────


def test_stamp_perp_intent_fields_open() -> None:
    intent = PerpOpenIntent(
        market="ETH/USD", collateral_token="USDC", collateral_amount=Decimal("10"),
        size_usd=Decimal("20"), is_long=True, leverage=Decimal("2"), protocol="gmx_v2",
    )
    extracted = _stamp(intent)
    assert extracted["intent_is_long"] is True
    assert extracted["intent_size_usd"] == "20"


def test_stamp_perp_intent_fields_short() -> None:
    intent = PerpOpenIntent(
        market="ETH/USD", collateral_token="USDC", collateral_amount=Decimal("10"),
        size_usd=Decimal("20"), is_long=False, leverage=Decimal("2"), protocol="gmx_v2",
    )
    extracted = _stamp(intent)
    # Empty ≠ Zero: a short is a real ``False``, not a dropped/None value.
    assert extracted["intent_is_long"] is False


def test_stamp_perp_intent_fields_full_close_leaves_size_unmeasured() -> None:
    intent = PerpCloseIntent(market="ETH/USD", collateral_token="USDC", is_long=True, protocol="gmx_v2")
    extracted = _stamp(intent)
    assert extracted["intent_is_long"] is True
    # A full close does not declare a size — it stays UNMEASURED, never zero.
    assert "intent_size_usd" not in extracted


def test_stamp_is_noop_for_non_perp() -> None:
    class _Swap:
        intent_type = "SWAP"

    result = _Result()
    StrategyRunner._stamp_perp_intent_fields(result, _Swap())
    assert result.extracted_data == {}


# ── The handler emits a schema-valid payload with real is_long + size ────────


def test_perp_open_payload_is_schema_valid_with_identity() -> None:
    intent = PerpOpenIntent(
        market="ETH/USD", collateral_token="USDC", collateral_amount=Decimal("10"),
        size_usd=Decimal("20"), is_long=True, leverage=Decimal("2"), protocol="gmx_v2",
    )
    ledger = _ledger("PERP_OPEN", _stamp(intent))
    event = handle_perp(_outbox("PERP_OPEN"), ledger)
    assert event is not None
    payload = json.loads(event.to_payload_json())

    assert payload["is_long"] is True
    assert payload["size"] == "20"
    assert "size_usd" not in payload  # canonical schema key only

    # VIB-5941 (B3): the perp payload contract bumped to primitive_version 2.
    assert payload["primitive_version"] == 2

    # Validates against the frozen schema (with the row-protocol projection).
    validated = validate_payload("PERP_OPEN", {**payload, "protocol": "gmx_v2"})
    assert validated is not None
    assert validated.is_long is True
    assert validated.size == Decimal("20")


def test_perp_full_close_payload_is_schema_valid_with_none_size() -> None:
    intent = PerpCloseIntent(market="ETH/USD", collateral_token="USDC", is_long=False, protocol="gmx_v2")
    ledger = _ledger("PERP_CLOSE", _stamp(intent), token_in="", amount_in="")
    event = handle_perp(_outbox("PERP_CLOSE"), ledger)
    assert event is not None
    payload = json.loads(event.to_payload_json())

    assert payload["is_long"] is False
    assert payload["size"] is None  # full close: size unmeasured, not zero

    # Required-but-nullable ``size`` validates because an unavailable_reason is set.
    validated = validate_payload("PERP_CLOSE", {**payload, "protocol": "gmx_v2"})
    assert validated is not None
    assert validated.size is None
    assert validated.unavailable_reason
    # The reason must NAME the missing size, not only entry/realized PnL.
    assert "size" in validated.unavailable_reason.lower()


def test_close_size_none_rejects_whitespace_only_reason() -> None:
    """A whitespace-only unavailable_reason must NOT satisfy the size-None audit
    trail — it is stripped before the check (a blank reason explains nothing).
    """
    base = {
        "event_type": "PERP_CLOSE", "protocol": "gmx_v2", "position_key": "p",
        "market": "eth/usd", "is_long": True, "size": None,
        "confidence": "ESTIMATED",
    }
    # Whitespace-only → rejected.
    with pytest.raises(ValueError, match="unavailable_reason"):
        validate_payload("PERP_CLOSE", {**base, "unavailable_reason": "   "})
    # Empty string → rejected.
    with pytest.raises(ValueError, match="unavailable_reason"):
        validate_payload("PERP_CLOSE", {**base, "unavailable_reason": ""})
    # A real reason → accepted.
    ok = validate_payload("PERP_CLOSE", {**base, "unavailable_reason": "size pending (VIB-5717)"})
    assert ok is not None and ok.size is None


def test_venue_measured_perp_data_overrides_intent_size_and_side(monkeypatch: Any) -> None:
    """A venue read (perp_data) is authoritative over the intent's request.

    ``handle_perp`` imports ``deserialize_extracted_data`` at call time, so we
    stub it to return a struct carrying the venue-measured ``perp_data``.
    """
    intent = PerpOpenIntent(
        market="ETH/USD", collateral_token="USDC", collateral_amount=Decimal("10"),
        size_usd=Decimal("20"), is_long=True, leverage=Decimal("2"), protocol="gmx_v2",
    )
    extracted = _stamp(intent)

    class _PerpData:
        size_delta = Decimal("19.5")
        is_long = False  # venue observed the OPPOSITE side of the request

    extracted["perp_data"] = _PerpData()
    monkeypatch.setattr(
        "almanak.framework.observability.ledger.deserialize_extracted_data",
        lambda _s: extracted,
    )
    event = handle_perp(_outbox("PERP_OPEN"), _ledger("PERP_OPEN", {}))
    assert event is not None
    payload = json.loads(event.to_payload_json())
    assert payload["size"] == "19.5"  # venue size wins over intent size
    assert payload["is_long"] is False  # venue side wins over intent side


def test_malformed_venue_size_does_not_clobber_intent_size(monkeypatch: Any) -> None:
    """A present-but-malformed/NaN venue size_delta must NOT overwrite the valid
    intent-known size with None (which would drop known data → schema-invalid open).
    """
    intent = PerpOpenIntent(
        market="ETH/USD", collateral_token="USDC", collateral_amount=Decimal("10"),
        size_usd=Decimal("20"), is_long=True, leverage=Decimal("2"), protocol="gmx_v2",
    )
    extracted = _stamp(intent)

    class _PerpData:
        size_delta = float("nan")  # malformed → _safe_decimal returns None
        is_long = None

    extracted["perp_data"] = _PerpData()
    monkeypatch.setattr(
        "almanak.framework.observability.ledger.deserialize_extracted_data",
        lambda _s: extracted,
    )
    event = handle_perp(_outbox("PERP_OPEN"), _ledger("PERP_OPEN", {}))
    assert event is not None
    payload = json.loads(event.to_payload_json())
    # Intent truth survives; a measured NaN did not clobber it to None.
    assert payload["size"] == "20"
    assert validate_payload("PERP_OPEN", {**payload, "protocol": "gmx_v2"}) is not None


def test_venue_measured_zero_size_overrides_intent(monkeypatch: Any) -> None:
    """A venue Decimal('0') is a valid measured-zero override (Empty ≠ Zero)."""
    intent = PerpOpenIntent(
        market="ETH/USD", collateral_token="USDC", collateral_amount=Decimal("10"),
        size_usd=Decimal("20"), is_long=True, leverage=Decimal("2"), protocol="gmx_v2",
    )
    extracted = _stamp(intent)

    class _PerpData:
        size_delta = Decimal("0")
        is_long = None

    extracted["perp_data"] = _PerpData()
    monkeypatch.setattr(
        "almanak.framework.observability.ledger.deserialize_extracted_data",
        lambda _s: extracted,
    )
    event = handle_perp(_outbox("PERP_OPEN"), _ledger("PERP_OPEN", {}))
    assert event is not None
    assert json.loads(event.to_payload_json())["size"] == "0"


# ── position_events: perps now acquire a stable position_id and are written ──


def _position_id(intent: Any) -> str:
    result = _Result(_stamp(intent))
    event = build_position_event_from_intent(
        deployment_id="dep-1", intent=intent, result=result,
        ledger_entry_id="led-1", chain="arbitrum", wallet_address="0xWALLET",
    )
    assert event is not None, "perp event must not be dropped by the θ guard"
    return event.position_id


def test_perp_open_position_event_has_deterministic_id() -> None:
    intent = PerpOpenIntent(
        market="ETH/USD", collateral_token="USDC", collateral_amount=Decimal("10"),
        size_usd=Decimal("20"), is_long=True, leverage=Decimal("2"), protocol="gmx_v2", chain="arbitrum",
    )
    result = _Result(_stamp(intent))
    event = build_position_event_from_intent(
        deployment_id="dep-1", intent=intent, result=result,
        ledger_entry_id="led-1", chain="arbitrum", wallet_address="0xWALLET",
    )
    assert event is not None, "perp OPEN must not be dropped by the θ guard"
    assert event.position_type == "PERP"
    assert event.event_type == "OPEN"
    # id discriminated by (market, side, collateral_token) — GMX's own key.
    assert event.position_id == "perp:arbitrum:gmx_v2:0xwallet:eth/usd:long:usdc"


def test_perp_open_and_close_share_position_id() -> None:
    common = {"market": "ETH/USD", "collateral_token": "USDC", "protocol": "gmx_v2", "chain": "arbitrum"}
    open_intent = PerpOpenIntent(
        collateral_amount=Decimal("10"), size_usd=Decimal("20"), is_long=True, leverage=Decimal("2"), **common
    )
    close_intent = PerpCloseIntent(is_long=True, **common)
    # Same (market, side, collateral) → same id → the lifecycle pairs.
    assert _position_id(open_intent) == _position_id(close_intent)


def test_long_and_short_same_market_are_distinct_positions() -> None:
    """A long and a short in the SAME market must NOT collapse into one lifecycle."""
    common = {"market": "ETH/USD", "collateral_token": "USDC", "protocol": "gmx_v2", "chain": "arbitrum"}
    lev = {"collateral_amount": Decimal("10"), "size_usd": Decimal("20"), "leverage": Decimal("2")}
    long_open = PerpOpenIntent(is_long=True, **lev, **common)
    short_open = PerpOpenIntent(is_long=False, **lev, **common)
    assert _position_id(long_open) != _position_id(short_open)
    # And each still pairs with its own same-side close.
    assert _position_id(long_open) == _position_id(PerpCloseIntent(is_long=True, **common))
    assert _position_id(short_open) == _position_id(PerpCloseIntent(is_long=False, **common))


def test_same_market_side_different_collateral_are_distinct_positions() -> None:
    """Two longs on the same market backed by DIFFERENT collateral are distinct."""
    a = PerpOpenIntent(
        market="ETH/USD", collateral_token="USDC", collateral_amount=Decimal("10"),
        size_usd=Decimal("20"), is_long=True, leverage=Decimal("2"), protocol="gmx_v2", chain="arbitrum",
    )
    b = PerpOpenIntent(
        market="ETH/USD", collateral_token="WETH", collateral_amount=Decimal("0.01"),
        size_usd=Decimal("20"), is_long=True, leverage=Decimal("2"), protocol="gmx_v2", chain="arbitrum",
    )
    assert _position_id(a) != _position_id(b)


def test_position_id_normalizes_whitespace_padded_market() -> None:
    """A padded market (" ETH/USD ") must normalize to the SAME id as the clean one —
    strip THEN replace, so whitespace-differing payloads can't mint distinct ids.
    """
    clean = PerpOpenIntent(
        market="ETH/USD", collateral_token="USDC", collateral_amount=Decimal("10"),
        size_usd=Decimal("20"), is_long=True, leverage=Decimal("2"), protocol="gmx_v2", chain="arbitrum",
    )
    padded = PerpOpenIntent(
        market="  ETH/USD  ", collateral_token="USDC", collateral_amount=Decimal("10"),
        size_usd=Decimal("20"), is_long=True, leverage=Decimal("2"), protocol="gmx_v2", chain="arbitrum",
    )
    assert _position_id(clean) == _position_id(padded)
    assert _position_id(clean).endswith(":eth/usd:long:usdc")
