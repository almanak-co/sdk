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
    token_in: str = "USDC",
    tx_hash: str = "0xdeadbeef",
    price_inputs_json: str = "",
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
        "token_in": token_in,
        "token_out": token_out,
        "tx_hash": tx_hash,
        "gas_usd": "0.01",
        "extracted_data_json": extracted_data_json,
        "price_inputs_json": price_inputs_json,
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
    extracted = json.dumps({"lp_close_data": {"amount0_collected": sy_raw, "amount1_collected": pt_raw}})

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
    sy_in = int(0.9 * 10**18)  # 0.9 SY (price of the PT)
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


# ──────────────────────────────────────────────────────────────────────────────
# R1 — PT_BUY records the FIFO lot in HUMAN units (VIB-4988)
# ──────────────────────────────────────────────────────────────────────────────


def test_pt_buy_records_lot_in_human_units() -> None:
    """R1: the PT_BUY event PAYLOAD and the in-memory PT lot are both HUMAN units
    (uniform PT convention, VIB-4988 v3→v4) so they match _replay_pt_buy."""
    sy_in = int(0.9 * 10**18)
    pt_out = int(1.0 * 10**18)
    extracted = json.dumps({"swap_amounts": {"amount_in": sy_in, "amount_out": pt_out}})
    future = datetime.now(UTC) + timedelta(days=730)
    pt_symbol = f"PT-wstETH-{future.day:02d}{future.strftime('%b').upper()}{future.year}"

    basis = FIFOBasisStore()
    ob = _outbox("SWAP", position_key="pendle_pt:arbitrum:0xwallet:0xmarket", market_id="0xmarket")
    led = _ledger("SWAP", protocol="pendle", token_out=pt_symbol, extracted_data_json=extracted)

    event = handle_pendle_pt(ob, led, basis_store=basis)

    assert event is not None
    # Event payload is HUMAN units (raw / 1e18).
    assert event.pt_amount == Decimal("1")  # 1.0 PT human
    assert event.sy_amount == Decimal("0.9")  # 0.9 SY human
    # Lot is HUMAN (raw / 1e18) — same convention as the payload.
    key = f"{event.identity.deployment_id}:{event.position_key}:{(event.pt_token or 'PT').lower()}"
    lot = basis._lots[key][0]
    assert lot["remaining_pt"] == Decimal("1")  # 1.0 PT human
    assert lot["cost_per_pt"] == Decimal("0.9")


# ──────────────────────────────────────────────────────────────────────────────
# PT_SELL (token_in is PT-)  (VIB-4988)
# ──────────────────────────────────────────────────────────────────────────────


def _future_pt(days: int = 730, base: str = "wstETH") -> str:
    future = datetime.now(UTC) + timedelta(days=days)
    return f"PT-{base}-{future.day:02d}{future.strftime('%b').upper()}{future.year}"


def _buy_lot(basis: FIFOBasisStore, pt_symbol: str, sy: float = 0.9, pt: float = 1.0) -> None:
    """Seed a PT buy lot (HUMAN) via the real buy path."""
    extracted = json.dumps({"swap_amounts": {"amount_in": int(sy * 10**18), "amount_out": int(pt * 10**18)}})
    ob = _outbox("SWAP", position_key="pendle_pt:arbitrum:0xwallet:0xmarket", market_id="0xmarket")
    led = _ledger("SWAP", protocol="pendle", token_out=pt_symbol, extracted_data_json=extracted)
    handle_pendle_pt(ob, led, basis_store=basis)


def test_pt_sell_realized_yield_usd_high_confidence() -> None:
    """Buy 1.0 PT @ 0.9 SY, sell 1.0 PT for 0.95 base @ $1 → realized yield = 0.05 USD, HIGH."""
    pt_symbol = _future_pt()
    basis = FIFOBasisStore()
    _buy_lot(basis, pt_symbol)

    sell_ext = json.dumps({"swap_amounts": {"amount_in": int(1.0 * 10**18), "amount_out": int(0.95 * 10**18)}})
    ob = _outbox("SWAP", position_key="pendle_pt:arbitrum:0xwallet:0xmarket", market_id="0xmarket")
    led = _ledger(
        "SWAP",
        protocol="pendle",
        token_in=pt_symbol,
        token_out="USDC",
        extracted_data_json=sell_ext,
        price_inputs_json=json.dumps({"USDC": "1.0"}),
    )

    event = handle_pendle_pt(ob, led, basis_store=basis)

    assert event is not None
    assert event.event_type == PendleEventType.PT_SELL
    # human amounts on the event (uniform PT convention)
    assert event.pt_amount == Decimal("1")
    assert event.sy_amount == Decimal("0.95")
    assert event.realized_yield_usd == Decimal("0.05")
    assert event.realized_yield_sy == Decimal("0.05")  # measured SY primitive
    assert event.confidence == AccountingConfidence.HIGH
    assert event.basis_lot_id is not None
    assert event.implied_apr_bps is None


def test_pt_sell_break_even_is_measured_zero() -> None:
    """Sell exactly at cost → realized yield == Decimal('0') (measured break-even), HIGH."""
    pt_symbol = _future_pt()
    basis = FIFOBasisStore()
    _buy_lot(basis, pt_symbol, sy=0.9, pt=1.0)

    sell_ext = json.dumps({"swap_amounts": {"amount_in": int(1.0 * 10**18), "amount_out": int(0.9 * 10**18)}})
    ob = _outbox("SWAP", position_key="pendle_pt:arbitrum:0xwallet:0xmarket", market_id="0xmarket")
    led = _ledger(
        "SWAP",
        protocol="pendle",
        token_in=pt_symbol,
        token_out="USDC",
        extracted_data_json=sell_ext,
        price_inputs_json=json.dumps({"USDC": "1.0"}),
    )

    event = handle_pendle_pt(ob, led, basis_store=basis)

    assert event is not None
    assert event.realized_yield_usd == Decimal("0")  # measured zero, NOT None
    assert event.realized_yield_sy == Decimal("0")  # measured SY zero (break-even)
    assert event.confidence == AccountingConfidence.HIGH


def test_pt_sell_unmatched_returns_none_estimated() -> None:
    """Sell with no prior buy lot → realized_yield None + ESTIMATED (Empty≠Zero)."""
    pt_symbol = _future_pt()
    basis = FIFOBasisStore()  # no buy lot

    sell_ext = json.dumps({"swap_amounts": {"amount_in": int(1.0 * 10**18), "amount_out": int(0.95 * 10**18)}})
    ob = _outbox("SWAP", position_key="pendle_pt:arbitrum:0xwallet:0xmarket", market_id="0xmarket")
    led = _ledger(
        "SWAP",
        protocol="pendle",
        token_in=pt_symbol,
        token_out="USDC",
        extracted_data_json=sell_ext,
        price_inputs_json=json.dumps({"USDC": "1.0"}),
    )

    event = handle_pendle_pt(ob, led, basis_store=basis)

    assert event is not None
    assert event.event_type == PendleEventType.PT_SELL
    assert event.realized_yield_usd is None
    assert event.realized_yield_sy is None  # no lot matched → both unmeasured
    assert event.confidence == AccountingConfidence.ESTIMATED
    assert event.basis_lot_id is None


def test_pt_sell_missing_sy_price_usd_none_sy_carried() -> None:
    """VIB-5314: sy_price missing → realized_yield_usd is None (STRICTLY USD-or-None,
    never SY-units in the *_usd field); the SY-denominated value (0.95 - 0.9 = 0.05 SY)
    rides realized_yield_sy; ESTIMATED + explicit reason."""
    pt_symbol = _future_pt()
    basis = FIFOBasisStore()
    _buy_lot(basis, pt_symbol)

    sell_ext = json.dumps({"swap_amounts": {"amount_in": int(1.0 * 10**18), "amount_out": int(0.95 * 10**18)}})
    ob = _outbox("SWAP", position_key="pendle_pt:arbitrum:0xwallet:0xmarket", market_id="0xmarket")
    led = _ledger(
        "SWAP",
        protocol="pendle",
        token_in=pt_symbol,
        token_out="USDC",
        extracted_data_json=sell_ext,
        price_inputs_json="",  # no price
    )

    event = handle_pendle_pt(ob, led, basis_store=basis)

    assert event is not None
    # No measured USD price → the USD projection is unmeasured (None), NOT the SY value.
    assert event.realized_yield_usd is None
    # The measured SY-denominated primitive is preserved separately.
    assert event.realized_yield_sy == Decimal("0.05")
    assert event.confidence == AccountingConfidence.ESTIMATED
    assert "SY-denominated" in event.unavailable_reason


def test_pt_sell_stores_human_amounts() -> None:
    """PT_SELL event payload stores HUMAN amounts (matches _replay_pt_sell)."""
    pt_symbol = _future_pt()
    basis = FIFOBasisStore()
    _buy_lot(basis, pt_symbol)

    pt_raw = int(1.0 * 10**18)
    sy_raw = int(0.95 * 10**18)
    sell_ext = json.dumps({"swap_amounts": {"amount_in": pt_raw, "amount_out": sy_raw}})
    ob = _outbox("SWAP", position_key="pendle_pt:arbitrum:0xwallet:0xmarket", market_id="0xmarket")
    led = _ledger(
        "SWAP",
        protocol="pendle",
        token_in=pt_symbol,
        token_out="USDC",
        extracted_data_json=sell_ext,
        price_inputs_json=json.dumps({"USDC": "1.0"}),
    )

    event = handle_pendle_pt(ob, led, basis_store=basis)

    assert event is not None
    assert event.pt_amount == Decimal("1")  # human (pt_raw / 1e18)
    assert event.sy_amount == Decimal("0.95")  # human (sy_raw / 1e18)


# ──────────────────────────────────────────────────────────────────────────────
# PT_REDEEM (WITHDRAW)  (VIB-4988)
# ──────────────────────────────────────────────────────────────────────────────


def test_pt_redeem_at_maturity_human_amounts_and_yield() -> None:
    """WITHDRAW redeem: amounts stored HUMAN (raw/1e18); realized yield in USD."""
    pt_symbol = _future_pt()
    basis = FIFOBasisStore()
    _buy_lot(basis, pt_symbol, sy=0.9, pt=1.0)

    py_raw = int(1.0 * 10**18)
    sy_raw = int(1.0 * 10**18)
    red_ext = json.dumps({"redemption_amounts": {"py_redeemed": py_raw, "sy_received": sy_raw}})
    ob = _outbox("WITHDRAW", position_key="pendle_pt:arbitrum:0xwallet:0xmarket", market_id="0xmarket")
    led = _ledger(
        "WITHDRAW",
        protocol="pendle",
        token_in=pt_symbol,
        token_out="USDC",
        extracted_data_json=red_ext,
        price_inputs_json=json.dumps({"USDC": "1.0"}),
    )

    event = handle_pendle_pt(ob, led, basis_store=basis)

    assert event is not None
    assert event.event_type == PendleEventType.PT_REDEEM
    # HUMAN amounts (== raw / 1e18), NOT raw-18.
    assert event.pt_amount == Decimal(py_raw) / _SCALE_18
    assert event.sy_amount == Decimal(sy_raw) / _SCALE_18
    # Yield = (1.0 received - 0.9 cost) * $1 = 0.10 USD.
    assert event.realized_yield_usd == Decimal("0.1")
    assert event.realized_yield_sy == Decimal("0.1")  # measured SY primitive (USDC @ $1)
    assert event.basis_lot_id is not None


def test_pt_redeem_sources_pt_count_from_declared_legs_pen6() -> None:
    """PEN6: pt_amount comes from the DECLARED INPUT money leg (PT count), NOT
    ``redemption_amounts['py_redeemed']`` (post-maturity the SY-ASSET amount).

    The legs INPUT (PT count) is basis-identical to the PT_BUY's PT ``amount_out``
    so PT quantity conserves through the FIFO match; ``redemption_amounts`` here
    carries the SMALLER SY-asset amount the legacy path would have mis-booked
    (the exact basis break VIB-4988 closes). The legs round-trip through
    ``serialize_extracted_data`` → handler deserialize, mirroring production.
    """
    from almanak.connectors._strategy_base.primitive_money_leg import (
        PrimitiveMoneyLeg,
        PrimitiveMoneyLegs,
    )
    from almanak.framework.accounting.measured import MeasuredMoney
    from almanak.framework.observability.ledger import serialize_extracted_data

    pt_symbol = _future_pt()
    basis = FIFOBasisStore()
    _buy_lot(basis, pt_symbol, sy=0.9, pt=0.012378)

    pt_count = Decimal("0.012378")
    underlying_out = Decimal("0.010003")  # SY-asset amount — smaller than PT count
    legs = PrimitiveMoneyLegs.of(
        PrimitiveMoneyLeg.input(pt_symbol, MeasuredMoney.measured(pt_count)),
        PrimitiveMoneyLeg.output("WSTETH", MeasuredMoney.measured(underlying_out)),
    )
    extracted = serialize_extracted_data(
        {
            "primitive_money_legs": legs,
            # Legacy redemption_amounts carries the WRONG (SY-asset) PT amount;
            # the declared legs INPUT must win.
            "redemption_amounts": {
                "py_redeemed": int(underlying_out * 10**18),
                "sy_received": int(underlying_out * 10**18),
            },
        }
    )
    ob = _outbox("WITHDRAW", position_key="pendle_pt:arbitrum:0xwallet:0xmarket", market_id="0xmarket")
    led = _ledger(
        "WITHDRAW",
        protocol="pendle",
        token_in=pt_symbol,
        token_out="WSTETH",
        extracted_data_json=extracted,
        price_inputs_json=json.dumps({"WSTETH": "4000.0"}),
    )

    event = handle_pendle_pt(ob, led, basis_store=basis)

    assert event is not None
    assert event.event_type == PendleEventType.PT_REDEEM
    # PEN6: PT count from the INPUT leg, NOT the SY-asset amount.
    assert event.pt_amount == pt_count
    # Underlying received from the OUTPUT leg.
    assert event.sy_amount == underlying_out
    # Full-quantity FIFO match (PT count == lot size) → a matched lot.
    assert event.basis_lot_id is not None


def test_pt_redeem_non_pt_token_in_declines_vib5330() -> None:
    """VIB-5330: a Pendle WITHDRAW whose token_in is NOT a PT- symbol (the
    pt_address-degrade path) must NOT be booked as a PT_REDEEM.

    Pre-fix, the dispatcher routed EVERY Pendle WITHDRAW to _build_pt_redeem and
    emitted a PT_REDEEM carrying a non-PT pt_token + ran a FIFO match on a bogus
    key — polluting the PT realized-yield lane with a phantom redemption. The
    dispatcher now declines (returns None) so the event books via the generic
    SWAP/category path, matching the position-event lane's PT/non-PT predicate
    (``observability/position_events.py:_pendle_pt_event`` declines the same shape).

    Empty != Zero: the FIFO store must be untouched (no match consumed against a
    real PT lot), so a later genuine PT redeem still matches the full buy lot.
    """
    pt_symbol = _future_pt()
    basis = FIFOBasisStore()
    _buy_lot(basis, pt_symbol)
    lots_before = json.dumps({k: len(v) for k, v in basis._lots.items()}, sort_keys=True)

    red_ext = json.dumps({"redemption_amounts": {"py_redeemed": int(1e18), "sy_received": int(1e18)}})
    ob = _outbox("WITHDRAW", position_key="pendle_pt:arbitrum:0xwallet:0xmarket", market_id="0xmarket")
    led = _ledger(
        "WITHDRAW",
        protocol="pendle",
        token_in="USDC",  # NOT a PT- symbol (degrade path)
        token_out="USDC",
        extracted_data_json=red_ext,
        price_inputs_json=json.dumps({"USDC": "1.0"}),
    )

    event = handle_pendle_pt(ob, led, basis_store=basis)

    assert event is None  # declined → generic SWAP path, no phantom PT_REDEEM
    # FIFO lane untouched: the seeded PT buy lot is still intact (no spurious match).
    assert json.dumps({k: len(v) for k, v in basis._lots.items()}, sort_keys=True) == lots_before


def test_pt_redeem_yt_token_in_declines_vib5330() -> None:
    """VIB-5330: a Pendle WITHDRAW whose token_in is a YT- symbol declines too —
    a YT leg is not a PT redeem and must not seed/consume the PT FIFO lane."""
    basis = FIFOBasisStore()
    red_ext = json.dumps({"redemption_amounts": {"py_redeemed": int(1e18), "sy_received": int(1e18)}})
    ob = _outbox("WITHDRAW", position_key="pendle_pt:arbitrum:0xwallet:0xmarket", market_id="0xmarket")
    led = _ledger(
        "WITHDRAW",
        protocol="pendle",
        token_in="YT-wstETH-25JUN2026",  # YT, not PT
        token_out="WSTETH",
        extracted_data_json=red_ext,
        price_inputs_json=json.dumps({"WSTETH": "4000.0"}),
    )

    event = handle_pendle_pt(ob, led, basis_store=basis)

    assert event is None
    assert not basis._lots  # no lot recorded for the YT symbol
