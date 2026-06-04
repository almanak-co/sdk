"""Unit tests for handle_pendle_lp and handle_pendle_pt category handlers (VIB-3467)."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from almanak.connectors.pendle.accounting_spec import (
    handle_pendle_lp,
    handle_pendle_pt,
)
from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.models import AccountingConfidence, PendleEventType

_SCALE_18 = Decimal(10**18)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _outbox(
    intent_type: str = "LP_OPEN",
    wallet_address: str = "0xwallet",
    position_key: str = "pendle_lp:arbitrum:0xwallet:0xmarket",
    market_id: str = "0xmarket",
) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "intent_type": intent_type,
        "wallet_address": wallet_address,
        "position_key": position_key,
        "market_id": market_id,
    }


def _ledger(
    intent_type: str = "LP_OPEN",
    protocol: str = "pendle",
    extracted_data_json: str = "",
    token_out: str = "",
    tx_hash: str = "0xdeadbeef",
) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "timestamp": datetime.now(UTC).isoformat(),
        "intent_type": intent_type,
        "protocol": protocol,
        "chain": "arbitrum",
        "token_in": "USDC",
        "token_out": token_out,
        "tx_hash": tx_hash,
        "gas_usd": "0.01",
        "extracted_data_json": extracted_data_json,
        "price_inputs_json": "",
        "pre_state_json": "",
        "post_state_json": "",
    }


# ──────────────────────────────────────────────────────────────────────────────
# handle_pendle_lp
# ──────────────────────────────────────────────────────────────────────────────


def test_handle_pendle_lp_open_returns_event() -> None:
    """LP_OPEN with lp_open_data returns a PENDLE_LP_OPEN event with scaled amounts."""
    sy_raw = int(1.5 * 10**18)  # 1.5 SY tokens
    pt_raw = int(2.0 * 10**18)  # 2.0 PT tokens
    extracted = json.dumps({"lp_open_data": {"amount0": sy_raw, "amount1": pt_raw}})

    event = handle_pendle_lp(_outbox("LP_OPEN"), _ledger("LP_OPEN", extracted_data_json=extracted))

    assert event is not None
    assert event.event_type == PendleEventType.PENDLE_LP_OPEN
    assert event.sy_amount == Decimal(sy_raw) / _SCALE_18
    assert event.pt_amount == Decimal(pt_raw) / _SCALE_18
    assert event.confidence == AccountingConfidence.ESTIMATED
    assert event.identity.chain == "arbitrum"
    assert event.identity.protocol == "pendle"


def test_handle_pendle_lp_close_returns_event() -> None:
    """LP_CLOSE with lp_close_data returns a PENDLE_LP_CLOSE event."""
    sy_raw = int(0.8 * 10**18)
    pt_raw = int(1.2 * 10**18)
    extracted = json.dumps(
        {"lp_close_data": {"amount0_collected": sy_raw, "amount1_collected": pt_raw}}
    )

    event = handle_pendle_lp(_outbox("LP_CLOSE"), _ledger("LP_CLOSE", extracted_data_json=extracted))

    assert event is not None
    assert event.event_type == PendleEventType.PENDLE_LP_CLOSE
    assert event.sy_amount == Decimal(sy_raw) / _SCALE_18
    assert event.pt_amount == Decimal(pt_raw) / _SCALE_18


def test_handle_pendle_lp_non_pendle_returns_none() -> None:
    """Non-Pendle protocol is rejected."""
    event = handle_pendle_lp(_outbox("LP_OPEN"), _ledger("LP_OPEN", protocol="uniswap_v3"))
    assert event is None


def test_handle_pendle_lp_wrong_intent_returns_none() -> None:
    """Non-LP intent type returns None."""
    event = handle_pendle_lp(_outbox("SUPPLY"), _ledger("SUPPLY", protocol="pendle"))
    assert event is None


def test_handle_pendle_lp_no_extracted_data_returns_event_with_none_amounts() -> None:
    """Missing extracted_data → amounts are None but event is still returned."""
    event = handle_pendle_lp(_outbox("LP_OPEN"), _ledger("LP_OPEN", extracted_data_json=""))
    assert event is not None
    assert event.sy_amount is None
    assert event.pt_amount is None
    assert event.confidence == AccountingConfidence.ESTIMATED


def test_handle_pendle_lp_uses_market_id_from_outbox() -> None:
    """market_id from outbox_row is stored in event.market_id."""
    event = handle_pendle_lp(
        _outbox("LP_OPEN", market_id="0xABCDEF"),
        _ledger("LP_OPEN"),
    )
    assert event is not None
    assert event.market_id == "0xABCDEF"


# ──────────────────────────────────────────────────────────────────────────────
# handle_pendle_pt
# ──────────────────────────────────────────────────────────────────────────────


def test_handle_pendle_pt_basic_with_maturity() -> None:
    """Valid Pendle PT swap produces PT_BUY event with HIGH confidence and APR."""
    sy_in = int(0.9 * 10**18)   # 0.9 SY (price of the PT)
    pt_out = int(1.0 * 10**18)  # 1.0 PT received
    extracted = json.dumps({"swap_amounts": {"amount_in": sy_in, "amount_out": pt_out}})

    # Use a dynamically computed far-future date so this test never becomes a date bomb.
    future = datetime.now(UTC) + timedelta(days=730)  # ~2 years out
    pt_symbol = f"PT-wstETH-{future.day:02d}{future.strftime('%b').upper()}{future.year}"

    ob = _outbox("SWAP", position_key="pendle_pt:arbitrum:0xwallet:0xmarket", market_id="0xmarket")
    led = _ledger(
        "SWAP",
        protocol="pendle",
        token_out=pt_symbol,
        extracted_data_json=extracted,
    )

    event = handle_pendle_pt(ob, led)

    assert event is not None
    assert event.event_type == PendleEventType.PT_BUY
    assert event.pt_token == pt_symbol
    assert event.sy_amount is not None
    assert event.pt_amount is not None
    assert event.pt_price is not None
    # maturity is parseable → days_to_maturity set → APR computed → HIGH
    assert event.confidence == AccountingConfidence.HIGH
    assert event.implied_apr_bps is not None


def test_handle_pendle_pt_missing_amounts_confidence_estimated() -> None:
    """No swap_amounts in extracted data → confidence ESTIMATED (missing amounts path).

    Uses a non-expired PT symbol so the ESTIMATED confidence comes specifically from
    the missing amounts, not from expired maturity.
    """
    future = datetime.now(UTC) + timedelta(days=365)
    pt_symbol = f"PT-sUSDe-{future.day:02d}{future.strftime('%b').upper()}{future.year}"
    ob = _outbox("SWAP")
    led = _ledger("SWAP", protocol="pendle", token_out=pt_symbol, extracted_data_json="")

    event = handle_pendle_pt(ob, led)

    assert event is not None
    assert event.confidence == AccountingConfidence.ESTIMATED
    assert event.sy_amount is None
    assert event.pt_amount is None


def test_handle_pendle_pt_expired_maturity_no_apr() -> None:
    """Past-maturity PT → days_to_maturity <= 0 → implied_apr_bps is None → ESTIMATED."""
    sy_in = int(0.95 * 10**18)
    pt_out = int(1.0 * 10**18)
    extracted = json.dumps({"swap_amounts": {"amount_in": sy_in, "amount_out": pt_out}})

    ob = _outbox("SWAP")
    # Use a maturity date in the past
    led = _ledger(
        "SWAP",
        protocol="pendle",
        token_out="PT-stETH-01JAN2020",  # well in the past
        extracted_data_json=extracted,
    )

    event = handle_pendle_pt(ob, led)

    assert event is not None
    assert event.implied_apr_bps is None
    # maturity parsed but expired → ESTIMATED
    assert event.confidence == AccountingConfidence.ESTIMATED


def test_handle_pendle_pt_unparseable_maturity_estimated() -> None:
    """PT symbol without parseable maturity → ESTIMATED, APR is None."""
    sy_in = int(0.9 * 10**18)
    pt_out = int(1.0 * 10**18)
    extracted = json.dumps({"swap_amounts": {"amount_in": sy_in, "amount_out": pt_out}})

    ob = _outbox("SWAP")
    led = _ledger(
        "SWAP",
        protocol="pendle",
        token_out="PT-UNKNOWN",  # no parseable maturity date
        extracted_data_json=extracted,
    )

    event = handle_pendle_pt(ob, led)

    assert event is not None
    assert event.pt_token == "PT-UNKNOWN"
    assert event.maturity_timestamp is None
    assert event.implied_apr_bps is None
    assert event.confidence == AccountingConfidence.ESTIMATED


def test_handle_pendle_pt_non_pt_token_out_returns_none() -> None:
    """token_out without PT- prefix → not a PT buy → None."""
    ob = _outbox("SWAP")
    led = _ledger("SWAP", protocol="pendle", token_out="SY-wstETH")

    event = handle_pendle_pt(ob, led)

    assert event is None


def test_handle_pendle_pt_non_pendle_protocol_returns_none() -> None:
    """Non-Pendle protocol SWAP → None."""
    ob = _outbox("SWAP")
    led = _ledger("SWAP", protocol="uniswap_v3", token_out="PT-wstETH-25JUN2026")

    event = handle_pendle_pt(ob, led)

    assert event is None


def test_handle_pendle_pt_wrong_intent_returns_none() -> None:
    """Non-SWAP intent → None."""
    ob = _outbox("SUPPLY")
    led = _ledger("SUPPLY", protocol="pendle", token_out="PT-wstETH-25JUN2030")

    event = handle_pendle_pt(ob, led)

    assert event is None


def test_handle_pendle_pt_records_fifo_lot_in_basis_store() -> None:
    """When basis_store is provided and amounts present, a PT lot is recorded."""
    sy_in = int(0.9 * 10**18)
    pt_out = int(1.0 * 10**18)
    extracted = json.dumps({"swap_amounts": {"amount_in": sy_in, "amount_out": pt_out}})

    basis = FIFOBasisStore()
    ob = _outbox("SWAP", position_key="pendle_pt:arbitrum:0xwallet:0xmarket", market_id="0xmarket")
    led = _ledger(
        "SWAP",
        protocol="pendle",
        token_out="PT-wstETH-25JUN2030",
        extracted_data_json=extracted,
    )

    event = handle_pendle_pt(ob, led, basis_store=basis)

    assert event is not None
    # FIFOBasisStore._key lowercases the token: "PT-wstETH-25JUN2026" → "pt-wsteth-25jun2026"
    pt_token_key = (event.pt_token or "PT").lower()
    position_key = event.position_key
    deployment_id = event.identity.deployment_id
    key = f"{deployment_id}:{position_key}:{pt_token_key}"
    lots = basis._lots.get(key, [])
    assert len(lots) == 1, f"Expected 1 PT lot, found {len(lots)} (keys={list(basis._lots)})"
    assert lots[0]["remaining_pt"] > 0


def test_handle_pendle_pt_no_basis_store_no_fifo_side_effect() -> None:
    """Without basis_store, handle_pendle_pt still returns an event (no crash)."""
    sy_in = int(0.9 * 10**18)
    pt_out = int(1.0 * 10**18)
    extracted = json.dumps({"swap_amounts": {"amount_in": sy_in, "amount_out": pt_out}})

    ob = _outbox("SWAP")
    led = _ledger("SWAP", protocol="pendle", token_out="PT-wstETH-25JUN2030", extracted_data_json=extracted)

    event = handle_pendle_pt(ob, led, basis_store=None)

    assert event is not None
    assert event.event_type == PendleEventType.PT_BUY


def test_handle_pendle_pt_propagates_source_ledger_entry_id() -> None:
    """PT lot created by handle_pendle_pt carries source_ledger_entry_id from ledger row."""
    sy_in = int(0.9 * 10**18)
    pt_out = int(1.0 * 10**18)
    extracted = json.dumps({"swap_amounts": {"amount_in": sy_in, "amount_out": pt_out}})
    future = datetime.now(UTC) + timedelta(days=730)
    pt_symbol = f"PT-wstETH-{future.day:02d}{future.strftime('%b').upper()}{future.year}"

    basis = FIFOBasisStore()
    ob = _outbox("SWAP", position_key="pendle_pt:arbitrum:0xwallet:0xmarket", market_id="0xmarket")
    led = _ledger(
        "SWAP",
        protocol="pendle",
        token_out=pt_symbol,
        extracted_data_json=extracted,
        tx_hash="0xdeadbeef",
    )
    # handler reads ledger_entry_id from led["id"] (the ledger row primary key)
    expected_ledger_entry_id = led["id"]

    event = handle_pendle_pt(ob, led, basis_store=basis)

    assert event is not None
    pt_token_key = (event.pt_token or "PT").lower()
    key = f"{event.identity.deployment_id}:{event.position_key}:{pt_token_key}"
    lots = basis._lots.get(key, [])
    assert len(lots) == 1
    assert lots[0]["source_ledger_entry_id"] == expected_ledger_entry_id
