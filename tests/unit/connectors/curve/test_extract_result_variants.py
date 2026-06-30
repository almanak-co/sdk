"""VIB-5432: Curve three-variant extract contract tests.

Curve was the last receipt parser still returning raw ``None`` for BOTH a
genuinely-absent event and a decode crash, so the ResultEnricher booked a parse
failure as a benign "no event" — the ghost-position class (VIB-3159 / VIB-5368).

These tests pin the migrated ``extract_<field>_result`` wrappers across all three
variants with REALISTIC inputs:

* a genuinely-absent event              -> ``ExtractMissing`` (benign, unchanged)
* a value-bearing receipt               -> ``ExtractOk``
* an event PRESENT but un-decodable     -> ``ExtractError`` (accounting-critical)
* ``protocol_fees``                     -> always ``ExtractOk`` (never returns None)

The ``ExtractError`` cases are the crux of VIB-5432: Curve's raw extractors
*swallow* their own exceptions (``try/except Exception: return None``), so a
field-level decode crash on a PRESENT event returns ``None``. A bare
``value is None -> ExtractMissing`` would therefore re-open the ghost class at the
field level. The wrappers close it by disambiguating the two ``None``s with a
PRESENCE signal derived from the parsed ``ParseResult`` (or, for the LP-token
mint extractors, the mint-Transfer scan). The previous version of this file
monkeypatched the extractor to *raise* — which the real methods never do — and so
gave false confidence; these tests drive the real swallow-and-return-None path.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from almanak.connectors.curve.receipt_parser import (
    CurveEvent,
    CurveEventType,
    CurveReceiptParser,
    ParseResult,
)
from almanak.framework.execution.extract_result import (
    ExtractError,
    ExtractMissing,
    ExtractOk,
)

# Realistic synthetic receipts shared with the raw-extractor unit tests.
from tests.unit.connectors.curve.test_receipt_parser import (
    _build_add_liquidity_receipt,
    _build_remove_liquidity_receipt,
    _build_swap_receipt,
)

# USDC (6) / DAI (18) — the tokens _build_swap_receipt wires as token_in/out.
_USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
_DAI = "0x6b175474e89094c44da98b954eedeac495271d0f"


@pytest.fixture
def parser() -> CurveReceiptParser:
    return CurveReceiptParser(chain="ethereum")


def _curve_event(event_type: CurveEventType, data: dict[str, Any], pool: str = "0x" + "11" * 20) -> CurveEvent:
    """Build a minimal parsed ``CurveEvent`` for ParseResult-stub tests."""
    return CurveEvent(
        event_type=event_type,
        event_name=event_type.value,
        log_index=0,
        transaction_hash="0x" + "ab" * 32,
        block_number=19_000_000,
        contract_address=pool,
        data=data,
    )


# ---------------------------------------------------------------------------
# ExtractMissing — genuinely-absent event (benign, unchanged behaviour)
# ---------------------------------------------------------------------------

# Field extractors that return ``None`` when their event is absent -> ExtractMissing.
_MISSING_FIELDS = [
    "swap_amounts",
    "position_id",
    "liquidity",
    "lp_tokens_received",
    "lp_open_data",
    "primitive_money_legs",
    "lp_close_data",
]


@pytest.mark.parametrize("field", _MISSING_FIELDS)
def test_empty_receipt_is_missing(parser: CurveReceiptParser, field: str) -> None:
    """An empty (event-less) receipt is a benign ExtractMissing, not an error."""
    result_method = getattr(parser, f"extract_{field}_result")
    assert isinstance(result_method({"logs": []}), ExtractMissing)


def test_protocol_fees_empty_receipt_is_ok(parser: CurveReceiptParser) -> None:
    """``extract_protocol_fees`` never returns None (UNAVAILABLE-with-reason per
    VIB-3495), so its result wrapper is ExtractOk even on an empty receipt."""
    out = parser.extract_protocol_fees_result({"logs": []})
    assert isinstance(out, ExtractOk)
    assert out.value.total_usd is None
    assert out.value.unavailable_reason == "protocol_fee_not_emitted_in_receipt"


# ---------------------------------------------------------------------------
# ExtractOk — value-bearing receipt
# ---------------------------------------------------------------------------


def test_swap_amounts_value_is_ok(parser: CurveReceiptParser) -> None:
    """A full swap receipt with resolvable decimals -> ExtractOk(SwapAmounts)."""
    receipt = _build_swap_receipt()
    parser._resolve_decimals = lambda addr: {_USDC: 6, _DAI: 18}.get(addr.lower())  # type: ignore[method-assign]
    out = parser.extract_swap_amounts_result(receipt)
    assert isinstance(out, ExtractOk)
    assert out.value.amount_in == 100_000_000


def test_position_id_value_is_ok(parser: CurveReceiptParser) -> None:
    """An AddLiquidity receipt's mint Transfer yields the LP token address."""
    out = parser.extract_position_id_result(_build_add_liquidity_receipt())
    assert isinstance(out, ExtractOk)
    assert isinstance(out.value, str) and out.value.startswith("0x") and len(out.value) == 42


@pytest.mark.parametrize("field", ["liquidity", "lp_tokens_received"])
def test_lp_tokens_value_is_ok(parser: CurveReceiptParser, field: str) -> None:
    """LP tokens minted decode to a human-readable Decimal (18-decimal invariant)."""
    out = getattr(parser, f"extract_{field}_result")(_build_add_liquidity_receipt())
    assert isinstance(out, ExtractOk)
    assert isinstance(out.value, Decimal) and out.value > 0


def test_lp_open_data_value_is_ok(parser: CurveReceiptParser) -> None:
    out = parser.extract_lp_open_data_result(_build_add_liquidity_receipt())
    assert isinstance(out, ExtractOk)
    assert out.value.pool_address  # canonical Curve pool address stamped


def test_lp_close_data_value_is_ok(parser: CurveReceiptParser) -> None:
    out = parser.extract_lp_close_data_result(_build_remove_liquidity_receipt())
    assert isinstance(out, ExtractOk)
    assert out.value.pool_address


# ---------------------------------------------------------------------------
# ExtractError — event PRESENT but un-decodable (the ghost-position class)
# ---------------------------------------------------------------------------


def test_swap_present_but_undecodable_is_error(parser: CurveReceiptParser) -> None:
    """A real TokenExchange event whose token decimals cannot be resolved is the
    documented ``extract_swap_amounts`` None path. Because the swap EVENT is
    present, that None is a decode failure -> ExtractError, not ExtractMissing."""
    receipt = _build_swap_receipt()
    parser._resolve_decimals = lambda _addr: None  # type: ignore[method-assign]
    out = parser.extract_swap_amounts_result(receipt)
    assert isinstance(out, ExtractError)
    assert "swap_amounts" in out.error


def test_position_id_present_but_malformed_is_error(parser: CurveReceiptParser) -> None:
    """A mint Transfer whose LP-token contract address is malformed (not a 20-byte
    address) makes ``extract_position_id`` return None. The mint is PRESENT, so
    this is a decode failure -> ExtractError."""
    receipt = _build_add_liquidity_receipt()
    # logs[1] is the mint Transfer (from the zero address); corrupt its emitter.
    receipt["logs"][1]["address"] = "0x1234"
    out = parser.extract_position_id_result(receipt)
    assert isinstance(out, ExtractError)
    assert "position_id" in out.error


@pytest.mark.parametrize("field", ["liquidity", "lp_tokens_received"])
def test_lp_tokens_present_but_undecodable_is_error(parser: CurveReceiptParser, field: str) -> None:
    """A mint Transfer carrying non-hex amount data raises inside the uint256
    decode, which the raw extractor swallows to None. The mint is PRESENT, so it
    is a decode failure -> ExtractError."""
    receipt = _build_add_liquidity_receipt()
    receipt["logs"][1]["data"] = "0xZZZZ"  # non-hex -> int(..., 16) raises
    out = getattr(parser, f"extract_{field}_result")(receipt)
    assert isinstance(out, ExtractError)
    assert field in out.error


def test_lp_open_data_present_but_undecodable_is_error(parser: CurveReceiptParser) -> None:
    """An AddLiquidity event whose decoded ``token_amounts`` is a non-sequence
    crashes ``extract_lp_open_data`` (``len`` on an int), swallowed to None. The
    event is PRESENT -> ExtractError (the LP_OPEN ghost case)."""
    parsed = ParseResult(success=True, events=[_curve_event(CurveEventType.ADD_LIQUIDITY, {"token_amounts": 12345})])
    parser.parse_receipt = lambda _r: parsed  # type: ignore[assignment, return-value]
    out = parser.extract_lp_open_data_result({"logs": []})
    assert isinstance(out, ExtractError)
    assert "lp_open_data" in out.error


def test_lp_close_data_present_but_undecodable_is_error(parser: CurveReceiptParser) -> None:
    """A RemoveLiquidity event whose decoded ``token_amounts`` is a non-sequence
    crashes ``extract_lp_close_data``, swallowed to None. The event is PRESENT ->
    ExtractError (the LP_CLOSE ghost case)."""
    parsed = ParseResult(success=True, events=[_curve_event(CurveEventType.REMOVE_LIQUIDITY, {"token_amounts": 999})])
    parser.parse_receipt = lambda _r: parsed  # type: ignore[assignment, return-value]
    out = parser.extract_lp_close_data_result({"logs": []})
    assert isinstance(out, ExtractError)
    assert "lp_close_data" in out.error


# ---------------------------------------------------------------------------
# ExtractError — present event that DECODES TO A FABRICATED FALLBACK (VIB-5432
# round 2). The raw extractor returns a NON-None default object (not None), so
# the legacy ``value is not None -> ExtractOk`` check mis-tagged it. These pin the
# decode-failure sentinel (``{"raw_data": ...}`` payload) that reclassifies them.
# ---------------------------------------------------------------------------


def test_swap_present_but_decode_fallback_is_error(parser: CurveReceiptParser) -> None:
    """A TokenExchange whose data fails to decode falls back to ``{"raw_data": ...}``
    in ``_decode_swap_data``; ``_parse_swap_event`` then manufactures a ZERO-DEFAULT
    ``SwapEventData`` and ``extract_swap_amounts`` returns a FABRICATED zero
    ``SwapAmounts`` (NON-None) once decimals resolve. The legacy value-not-None check
    would mis-tag that ``ExtractOk``; the round-2 sentinel makes it ``ExtractError``."""
    receipt = _build_swap_receipt()
    # logs[1] is the TokenExchange; non-hex data -> decode_int128 raises -> raw_data.
    receipt["logs"][1]["data"] = "0x" + "zz" * 64
    parser._resolve_decimals = lambda addr: {_USDC: 6, _DAI: 18}.get(addr.lower())  # type: ignore[method-assign]
    # The raw extractor really does fabricate a non-None zero SwapAmounts here —
    # this is the exact gap the legacy ExtractOk path could not see.
    raw = parser.extract_swap_amounts(receipt)
    assert raw is not None and raw.amount_in == 0 and raw.amount_out == 0
    out = parser.extract_swap_amounts_result(receipt)
    assert isinstance(out, ExtractError)
    assert "raw_data" in out.error


def test_lp_open_present_but_decode_fallback_is_error(parser: CurveReceiptParser) -> None:
    """An AddLiquidity whose data fails to decode falls back to ``{"raw_data": ...}``
    (no ``token_amounts``); ``extract_lp_open_data`` then builds a NON-None
    ``LPOpenData`` with ``amount0``/``amount1`` ``None`` — a fabricated open the
    legacy check mis-tagged ``ExtractOk``. Round-2 sentinel -> ``ExtractError``."""
    receipt = _build_add_liquidity_receipt()
    # logs[0] is the AddLiquidity3 event; corrupt its data to force the fallback.
    receipt["logs"][0]["data"] = "0x" + "zz" * 64
    raw = parser.extract_lp_open_data(receipt)
    assert raw is not None and raw.amount0 is None and raw.amount1 is None
    out = parser.extract_lp_open_data_result(receipt)
    assert isinstance(out, ExtractError)
    assert "raw_data" in out.error


def test_lp_close_present_but_decode_fallback_is_error(parser: CurveReceiptParser) -> None:
    """A RemoveLiquidity whose data fails to decode falls back to ``{"raw_data": ...}``
    (no ``token_amounts``); ``extract_lp_close_data`` then builds a NON-None
    ``LPCloseData`` with ``None`` (unmeasured — Empty ≠ Zero, VIB-5491) collected
    amounts — still a non-None close the legacy presence-check mis-tagged
    ``ExtractOk``. The Round-2 raw_data sentinel -> ``ExtractError`` regardless."""
    receipt = _build_remove_liquidity_receipt()
    # logs[0] is the RemoveLiquidity3 event; corrupt its data to force the fallback.
    receipt["logs"][0]["data"] = "0x" + "zz" * 64
    raw = parser.extract_lp_close_data(receipt)
    # Empty ≠ Zero: unmeasured legs are None, never a fabricated measured 0.
    assert raw is not None and raw.amount0_collected is None and raw.amount1_collected is None
    out = parser.extract_lp_close_data_result(receipt)
    assert isinstance(out, ExtractError)
    assert "raw_data" in out.error


def test_remove_liquidity_one_raw_data_stays_ok(parser: CurveReceiptParser) -> None:
    """OVER-REJECTION GUARD: ``RemoveLiquidityOne`` / ``RemoveLiquidityImbalance``
    have NO structured decoder, so their event data is the ``{"raw_data": ...}``
    passthrough BY DESIGN — not a decode failure. ``extract_lp_close_data`` returns a
    valid (pool-address-stamped) ``LPCloseData`` for them, and the decode-failure
    sentinel (scoped to ``REMOVE_LIQUIDITY`` only) must leave them ``ExtractOk``.
    Flagging them would convert real single-coin withdrawals into accounting halts."""
    one = _curve_event(CurveEventType.REMOVE_LIQUIDITY_ONE, {"raw_data": "00" * 32})
    parsed = ParseResult(success=True, events=[one])
    parser.parse_receipt = lambda _r: parsed  # type: ignore[assignment, return-value]
    out = parser.extract_lp_close_data_result({"logs": []})
    assert isinstance(out, ExtractOk)
    assert out.value.pool_address  # canonical pool address still stamped


def test_primitive_money_legs_unknown_pool_is_missing(parser: CurveReceiptParser) -> None:
    """DOCUMENTED PER-FIELD EXCEPTION: ``extract_primitive_money_legs`` returns
    None *by design* (legacy two-slot fallback) when the AddLiquidity pool's coin
    metadata is unknown — a common, benign case, NOT a decode crash. So even with
    the AddLiquidity event PRESENT, this maps to ExtractMissing. The LP_OPEN
    ghost-position guard lives on ``lp_open_data_result`` (proven above), which
    fail-closes on a present-but-undecodable AddLiquidity. Mapping this field's
    None to ExtractError would convert every unregistered-pool deposit into a
    fatal accounting halt."""
    add = _curve_event(CurveEventType.ADD_LIQUIDITY, {"token_amounts": [1, 2]}, pool="0x" + "ee" * 20)
    parsed = ParseResult(success=True, events=[add])
    parser.parse_receipt = lambda _r: parsed  # type: ignore[assignment, return-value]
    out = parser.extract_primitive_money_legs_result({"logs": []})
    assert isinstance(out, ExtractMissing)


# ---------------------------------------------------------------------------
# parse_receipt-level failures (short-circuit before the field extractor)
# ---------------------------------------------------------------------------


def test_parse_receipt_crash_short_circuits_to_error(parser: CurveReceiptParser) -> None:
    """A crash in ``parse_receipt`` itself (the most common ghost source) is
    caught by ``_strict_parse`` and surfaced as ExtractError before the
    field-specific extractor even runs."""

    def boom(_receipt: dict[str, Any]) -> Any:
        raise ValueError("curve parse_receipt blew up")

    parser.parse_receipt = boom  # type: ignore[method-assign]
    out = parser.extract_swap_amounts_result({"logs": []})
    assert isinstance(out, ExtractError)
    assert "curve parse_receipt blew up" in out.error
    assert isinstance(out.exception, ValueError)


def test_parse_receipt_none_is_error(parser: CurveReceiptParser) -> None:
    """Curve's ``parse_receipt`` has ``return None`` paths; an un-parseable
    receipt is an ExtractError, not a missing event (defensive guard added in
    VIB-5432, beyond the aerodrome reference)."""

    parser.parse_receipt = lambda _receipt: None  # type: ignore[assignment, return-value]
    out = parser.extract_lp_close_data_result({"logs": []})
    assert isinstance(out, ExtractError)
    assert "returned None" in out.error


def test_parse_receipt_success_false_is_error(parser: CurveReceiptParser) -> None:
    """A ``ParseResult`` reporting ``success == False`` (parser caught an internal
    error) is an ExtractError — never a benign missing event."""
    parser.parse_receipt = lambda _r: ParseResult(success=False, error="decode boom")  # type: ignore[assignment, return-value]
    out = parser.extract_swap_amounts_result({"logs": []})
    assert isinstance(out, ExtractError)
    assert "decode boom" in out.error


# ---------------------------------------------------------------------------
# Capability surface
# ---------------------------------------------------------------------------


def test_supported_extractions_declared(parser: CurveReceiptParser) -> None:
    """The capability surface must cover every served field so declaring it does
    not newly skip a field the enricher previously called (behaviour-preserving).
    Every entry has a matching ``extract_<field>`` method."""
    expected = {
        "swap_amounts",
        "position_id",
        "liquidity",
        "lp_tokens_received",
        "lp_open_data",
        "primitive_money_legs",
        "lp_close_data",
        "protocol_fees",
    }
    assert CurveReceiptParser.SUPPORTED_EXTRACTIONS == expected
    for field in expected:
        assert hasattr(parser, f"extract_{field}")
        assert hasattr(parser, f"extract_{field}_result")


@pytest.mark.parametrize(
    "logs",
    [
        ["bad"],  # non-mapping log entry
        [{"topics": "notalist"}],  # non-sequence topics
        [{"no_topics": 1}],  # missing topics
        "notalist",  # logs itself not a sequence
        [{"topics": []}],  # empty topics
    ],
)
def test_has_mint_transfer_non_raising_on_malformed_logs(parser: CurveReceiptParser, logs: Any) -> None:
    """``parse_receipt`` can succeed while ``_parse_log`` swallows individual
    malformed logs, so the LP-token presence scan must tolerate the same shapes
    and return False rather than raise (VIB-5432)."""
    assert parser._has_mint_transfer({"logs": logs}) is False
