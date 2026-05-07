"""Characterization tests for ``build_ledger_entry`` (Phase 5k-chars).

These tests pin the CURRENT behaviour of ``build_ledger_entry`` so that the
upcoming phase extraction (Phase 5k, mirroring Phase 5i's pattern for
``build_position_event_from_intent``) is a provable behaviour-preserving
refactor. They exercise every branch in the function:

    α  intent-type dispatch        (line 122-125)
    β  swap-amounts enrichment     (line 136-144)
    β' fallback token extraction   (line 146-164)
    γ  tx/gas extraction           (line 171-177)
    δ  error coalescing            (line 179-180)
    ε  extracted_data serialization (line 183-185)
    ζ  multi-tx bundle capture     (line 188-207)

The SQLite INSERT column order at ``backends/sqlite.py:2291-2322`` binds
``LedgerEntry`` field semantics to a positional write contract — any helper
refactor must keep the field-value semantics byte-identical.
"""

import json
from dataclasses import asdict, fields
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.execution.extracted_data import (
    BorrowData,
    LPCloseData,
    LPOpenData,
    SupplyData,
    SwapAmounts,
)
from almanak.framework.observability.ledger import (
    LedgerEntry,
    build_ledger_entry,
    deserialize_extracted_data,
)

# ---------------------------------------------------------------------------
# Duck-typed helpers (matches the ``_Attrs`` pattern used in
# tests/framework/observability/test_position_events.py).
# ---------------------------------------------------------------------------


class _Attrs:
    """Attribute holder that exposes ONLY attributes explicitly set on it.

    Lets each test selectively omit attributes so we can exercise every
    ``getattr(..., default)`` branch in the production code without inventing
    subclasses for each case.
    """

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _make_intent(
    intent_type: str = "SWAP",
    protocol: str = "",
    **extra,
) -> SimpleNamespace:
    """Build a duck-typed intent with ``.intent_type.value`` semantics."""
    ns = SimpleNamespace(
        intent_type=SimpleNamespace(value=intent_type),
        protocol=protocol,
    )
    for k, v in extra.items():
        setattr(ns, k, v)
    return ns


def _make_tx_result(tx_hash: str = "0xabc", gas_used: int = 0, success: bool = True):
    return SimpleNamespace(tx_hash=tx_hash, gas_used=gas_used, success=success)


# ---------------------------------------------------------------------------
# Phase α — intent-type dispatch (lines 122-125).
# ---------------------------------------------------------------------------


class TestIntentTypeDispatch:
    """``intent_type`` is read from ``intent.intent_type``; enum-with-.value
    and raw-string shapes are both supported; missing attr → ''.
    """

    def test_enum_like_intent_type_with_value_attr(self):
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=None)
        assert entry.intent_type == "SWAP"

    def test_raw_string_intent_type_falls_through_to_str(self):
        """A plain-string intent_type (no .value) is stringified directly."""

        class StrTypeIntent:
            intent_type = "LP_OPEN"  # not an enum
            protocol = "uniswap_v3"

        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=StrTypeIntent(), result=None)
        assert entry.intent_type == "LP_OPEN"

    def test_missing_intent_type_attr_yields_empty_string(self):
        class NoType:
            protocol = ""

        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=NoType(), result=None)
        assert entry.intent_type == ""


# ---------------------------------------------------------------------------
# Happy-path tests, one per intent-type family.
# ---------------------------------------------------------------------------


class TestHappyPathsPerIntentType:
    """One test per intent-type family covering the extraction path through
    the main branch. Each asserts the token/amount/protocol fields end up in
    the LedgerEntry slots that the SQLite INSERT then writes positionally.
    """

    def test_swap_happy_path(self):
        swap = SwapAmounts(
            amount_in=1000_000,
            amount_out=500_000_000_000_000_000,
            amount_in_decimal=Decimal("1000"),
            amount_out_decimal=Decimal("0.5"),
            effective_price=Decimal("2000"),
            slippage_bps=5,
            token_in="USDC",
            token_out="ETH",
        )
        result = SimpleNamespace(
            swap_amounts=swap,
            transaction_results=[_make_tx_result("0xabc")],
            total_gas_used=150_000,
            gas_cost_usd=Decimal("0.50"),
            extracted_data={"swap_amounts": swap},
        )
        intent = _make_intent(
            "SWAP",
            protocol="uniswap_v3",
            from_token="USDC",
            to_token="ETH",
        )
        entry = build_ledger_entry(
            strategy_id="s",
            cycle_id="c",
            intent=intent,
            result=result,
            chain="arbitrum",
        )
        assert entry.intent_type == "SWAP"
        assert entry.token_in == "USDC"
        assert entry.token_out == "ETH"
        assert entry.amount_in == "1000"
        assert entry.amount_out == "0.5"
        assert entry.effective_price == "2000"
        assert entry.slippage_bps == 5
        assert entry.gas_used == 150_000
        assert entry.gas_usd == "0.50"
        assert entry.tx_hash == "0xabc"
        assert entry.chain == "arbitrum"
        assert entry.protocol == "uniswap_v3"
        assert entry.success is True

    def test_lp_open_happy_path(self):
        """LP_OPEN with LPOpenData in extracted_data -- VIB-3450.

        Amounts must come from LPOpenData.amount0/amount1 (on-chain actuals,
        raw integers). Tokens come from from_token/to_token on the intent
        (the LP intent exposes the pair via those attrs when present).
        """
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[_make_tx_result("0xlpo")],
            total_gas_used=300_000,
            gas_cost_usd=Decimal("1.25"),
            extracted_data={
                "lp_open_data": LPOpenData(
                    position_id=12345,
                    tick_lower=-60,
                    tick_upper=60,
                    liquidity=1_000_000,
                    amount0=10_000,
                    amount1=20_000,
                )
            },
        )
        intent = _make_intent(
            "LP_OPEN",
            protocol="uniswap_v3",
            from_token="WETH",
            to_token="USDC",
            amount=Decimal("100"),
        )
        entry = build_ledger_entry(
            strategy_id="s",
            cycle_id="c",
            intent=intent,
            result=result,
            chain="arbitrum",
        )
        assert entry.intent_type == "LP_OPEN"
        assert entry.protocol == "uniswap_v3"
        assert entry.token_in == "WETH"
        assert entry.token_out == "USDC"
        # VIB-3450: amount_in/amount_out now come from LPOpenData on-chain actuals.
        assert entry.amount_in == "10000"
        assert entry.amount_out == "20000"
        assert entry.tx_hash == "0xlpo"
        assert entry.gas_used == 300_000
        assert entry.gas_usd == "1.25"
        # extracted_data serialization captured it.
        assert entry.extracted_data_json
        restored = deserialize_extracted_data(entry.extracted_data_json)
        assert isinstance(restored["lp_open_data"], LPOpenData)

    def test_lp_close_happy_path_with_swap_amounts(self):
        """LP_CLOSE typically ships a SwapAmounts describing received tokens."""
        swap = SwapAmounts(
            amount_in=0,
            amount_out=500_000,
            amount_in_decimal=Decimal("0"),
            amount_out_decimal=Decimal("500"),
            effective_price=None,
            slippage_bps=None,
            token_in="NFT-12345",
            token_out="USDC",
        )
        result = SimpleNamespace(
            swap_amounts=swap,
            transaction_results=[_make_tx_result("0xlpc")],
            total_gas_used=250_000,
            gas_cost_usd=Decimal("1.00"),
            extracted_data={
                "lp_close_data": LPCloseData(
                    amount0_collected=480_000,
                    amount1_collected=170_000_000_000_000_000,
                    fees0=5_000,
                    fees1=2_000_000_000_000_000,
                    liquidity_removed=1_000_000,
                )
            },
        )
        intent = _make_intent("LP_CLOSE", protocol="uniswap_v3")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.intent_type == "LP_CLOSE"
        assert entry.token_in == "NFT-12345"
        assert entry.token_out == "USDC"
        # Regression for issue #1768 (sibling of #1710 fixed in #1751).
        # Decimal("0") is falsy so truthiness coercion would silently drop
        # a measured-zero amount_in to "". The fix uses ``is not None``
        # checks so "measured zero" is distinguishable from "unknown".
        assert entry.amount_in == "0"
        assert entry.amount_out == "500"

    def test_supply_happy_path(self):
        """SUPPLY: no swap_amounts; falls back to intent.token + intent.amount."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[_make_tx_result("0xsup")],
            total_gas_used=180_000,
            gas_cost_usd=Decimal("0.75"),
            extracted_data={
                "supply": SupplyData(
                    supply_amount=5_000_000,
                    a_token_received=4_999_000,
                    supply_rate=Decimal("0.025"),
                )
            },
        )
        intent = _make_intent(
            "SUPPLY",
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("500"),
        )
        entry = build_ledger_entry(
            strategy_id="s",
            cycle_id="c",
            intent=intent,
            result=result,
            chain="base",
        )
        assert entry.intent_type == "SUPPLY"
        assert entry.protocol == "aave_v3"
        assert entry.token_in == "USDC"
        assert entry.token_out == ""
        assert entry.amount_in == "500"
        assert entry.amount_out == ""

    def test_withdraw_happy_path(self):
        """WITHDRAW: falls back to intent.supply_token + intent.amount_usd."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[_make_tx_result("0xwd")],
            total_gas_used=160_000,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent(
            "WITHDRAW",
            protocol="aave_v3",
            supply_token="USDC",
            amount_usd=Decimal("250.50"),
        )
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.intent_type == "WITHDRAW"
        assert entry.token_in == "USDC"
        assert entry.amount_in == "250.50"

    def test_borrow_happy_path(self):
        """BORROW: falls back to intent.borrow_token + intent.borrow_amount."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[_make_tx_result("0xbor")],
            total_gas_used=220_000,
            gas_cost_usd=Decimal("1.10"),
            extracted_data={
                "borrow": BorrowData(
                    borrow_amount=1_000_000,
                    borrow_rate=Decimal("0.035"),
                    debt_token="0xdebt",
                    health_factor=Decimal("1.85"),
                )
            },
        )
        intent = _make_intent(
            "BORROW",
            protocol="aave_v3",
            borrow_token="USDC",
            borrow_amount=Decimal("1000"),
        )
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.intent_type == "BORROW"
        assert entry.token_in == "USDC"
        assert entry.amount_in == "1000"

    def test_repay_happy_path(self):
        """REPAY: uses the ``borrow_token``/``amount`` intent-attrs fallback."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[_make_tx_result("0xrep")],
            total_gas_used=190_000,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent(
            "REPAY",
            protocol="aave_v3",
            borrow_token="USDC",
            amount=Decimal("500"),
        )
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.intent_type == "REPAY"
        assert entry.token_in == "USDC"
        assert entry.amount_in == "500"


# ---------------------------------------------------------------------------
# VIB-3450 -- LP_OPEN token/amount extraction (dedicated coverage).
# ---------------------------------------------------------------------------


class TestLPOpenExtraction:
    """LP_OPEN intents carry amounts in ``LPOpenData.amount0/amount1``
    (raw on-chain integers) stored under ``result.extracted_data["lp_open_data"]``.
    ``LPOpenIntent`` has no ``from_token``/``to_token`` in its formal model;
    tokens fall back to ``intent.token0/token1`` then ``from_token/to_token``.

    Before VIB-3450, the LP_OPEN path fell through to
    ``_extract_from_intent_fallback``, which found no matching attrs on the
    intent and left all token/amount fields as empty strings.
    """

    def test_amounts_from_lp_open_data(self):
        """On-chain LPOpenData.amount0/amount1 populate amount_in/amount_out."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[_make_tx_result("0xlpo")],
            total_gas_used=200_000,
            gas_cost_usd=Decimal("1.00"),
            extracted_data={
                "lp_open_data": LPOpenData(
                    position_id=999,
                    liquidity=500_000,
                    amount0=1_000_000,  # raw token0 deposited
                    amount1=2_500_000,  # raw token1 deposited
                )
            },
        )
        intent = _make_intent("LP_OPEN", protocol="uniswap_v3")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.amount_in == "1000000"
        assert entry.amount_out == "2500000"

    def test_tokens_from_intent_token0_token1(self):
        """intent.token0/token1 populate token_in/token_out."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={
                "lp_open_data": LPOpenData(
                    position_id=1,
                    amount0=100,
                    amount1=200,
                )
            },
        )
        intent = _make_intent(
            "LP_OPEN",
            protocol="uniswap_v3",
            token0="0xWETH",
            token1="0xUSDC",
        )
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.token_in == "0xWETH"
        assert entry.token_out == "0xUSDC"

    def test_tokens_fallback_to_from_token_to_token(self):
        """When intent.token0/token1 are absent, from_token/to_token are used."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={
                "lp_open_data": LPOpenData(
                    position_id=2,
                    amount0=50,
                    amount1=75,
                )
            },
        )
        intent = _make_intent(
            "LP_OPEN",
            protocol="uniswap_v3",
            from_token="WETH",
            to_token="USDC",
        )
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.token_in == "WETH"
        assert entry.token_out == "USDC"

    def test_amounts_fall_back_to_intent_amounts_when_no_lp_open_data(self):
        """Without LPOpenData, intent.amount0/amount1 are used as fallback."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent(
            "LP_OPEN",
            protocol="uniswap_v3",
            amount0=Decimal("0.5"),
            amount1=Decimal("1000"),
        )
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.amount_in == "0.5"
        assert entry.amount_out == "1000"

    def test_amounts_empty_when_no_lp_open_data_and_no_intent_amounts(self):
        """No LPOpenData and no intent amounts -> both amount fields are ''."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("LP_OPEN", protocol="uniswap_v3")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.amount_in == ""
        assert entry.amount_out == ""

    def test_lp_open_zero_amounts_record_as_zero_not_empty(self):
        """LPOpenData.amount0 = 0 is a measured zero and must record as '0'."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={
                "lp_open_data": LPOpenData(
                    position_id=3,
                    amount0=0,
                    amount1=500,
                )
            },
        )
        intent = _make_intent("LP_OPEN", protocol="uniswap_v3")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.amount_in == "0"
        assert entry.amount_out == "500"

    def test_lp_open_no_result_falls_through_cleanly(self):
        """result=None: no crash, tokens/amounts from intent if present."""
        intent = _make_intent(
            "LP_OPEN",
            protocol="uniswap_v3",
            token0="WETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
        )
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=None)
        assert entry.token_in == "WETH"
        assert entry.token_out == "USDC"
        assert entry.amount_in == "1"
        assert entry.amount_out == "2000"

    def test_partial_lp_open_data_per_side_fallback(self):
        """LPOpenData with amount0=None falls back per-side to intent.amount0.

        Covers the per-side fallback path: amount0 missing in LPOpenData but
        present on the intent; amount1 present in LPOpenData and takes priority.
        """
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={
                "lp_open_data": LPOpenData(
                    position_id=42,
                    amount0=None,  # missing on-chain actual for token0
                    amount1=250,   # on-chain actual for token1 present
                )
            },
        )
        intent = _make_intent(
            "LP_OPEN",
            protocol="uniswap_v3",
            amount0=Decimal("3.5"),   # fallback for the missing side
            amount1=Decimal("999"),   # should NOT win; lp_open_data.amount1 = 250
        )
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        # token0 side: lp_open_data.amount0 is None → fallback to intent.amount0
        assert entry.amount_in == "3.5"
        # token1 side: lp_open_data.amount1 is 250 → on-chain value wins
        assert entry.amount_out == "250"


# ---------------------------------------------------------------------------
# Phase β — SwapAmounts extraction branch (lines 136-144).
# ---------------------------------------------------------------------------


class TestSwapAmountsExtraction:
    """When ``result.swap_amounts`` is truthy, all downstream token/amount
    fields come from it. The ``or getattr(intent, ...)`` fallbacks only fire
    if the swap_amounts value is falsy (empty string / None).
    """

    def test_swap_amounts_falsy_token_in_falls_back_to_intent_from_token(self):
        """Empty-string token_in falls back to intent.from_token.
        Documents the `or getattr(intent, "from_token", "")` branch on line 138.
        """
        swap = _Attrs(
            token_in="",  # falsy → fallback fires
            token_out="ETH",
            amount_in_decimal=Decimal("1"),
            amount_out_decimal=Decimal("2"),
            effective_price=Decimal("2"),
            slippage_bps=None,
        )
        result = SimpleNamespace(
            swap_amounts=swap,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("SWAP", from_token="FALLBACK_IN", to_token="SHOULDNT_SEE")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.token_in == "FALLBACK_IN"
        # token_out already set on swap_amounts, does NOT fall back.
        assert entry.token_out == "ETH"

    def test_swap_amounts_amount_in_decimal_none_yields_empty_string(self):
        """`str(x) if x else ""` — None/Decimal(0) → "".

        Note: this is an existing behaviour — a measured-zero ``amount_in``
        surfaces as "", which is distinct from the known-measured "0" case
        elsewhere. Pinned as-is; refactor must not change it.
        """
        swap = _Attrs(
            token_in="USDC",
            token_out="ETH",
            amount_in_decimal=None,
            amount_out_decimal=Decimal("0.5"),
            effective_price=None,
            slippage_bps=None,
        )
        result = SimpleNamespace(
            swap_amounts=swap,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.amount_in == ""
        assert entry.amount_out == "0.5"

    def test_swap_amounts_effective_price_none_yields_empty_string(self):
        swap = _Attrs(
            token_in="USDC",
            token_out="ETH",
            amount_in_decimal=Decimal("1"),
            amount_out_decimal=Decimal("2"),
            effective_price=None,  # explicit None → skip branch
            slippage_bps=7,
        )
        result = SimpleNamespace(
            swap_amounts=swap,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.effective_price == ""
        assert entry.slippage_bps == 7

    def test_swap_amounts_slippage_bps_none_kept_as_none(self):
        """``slippage_bps`` is ``float | None`` — None is a first-class value."""
        swap = _Attrs(
            token_in="USDC",
            token_out="ETH",
            amount_in_decimal=Decimal("1"),
            amount_out_decimal=Decimal("2"),
            effective_price=Decimal("2"),
            slippage_bps=None,
        )
        result = SimpleNamespace(
            swap_amounts=swap,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.slippage_bps is None

    def test_swap_amounts_unresolved_decimals_yield_empty_string(self):
        """Regression for issue #1778.

        When a receipt parser cannot resolve ``decimals`` for a token, it
        falls back to ``Decimal(0)`` on ``amount_*_decimal`` AND sets
        ``amount_*_decimal_resolved=False``. The ledger must treat that
        as "unknown" (→ ``""``) even though the Decimal value is 0 — a
        measured-zero amount (``Decimal("0")`` with
        ``amount_*_decimal_resolved=True``) still records as ``"0"``.
        Without this distinction, PnL attribution and portfolio
        accounting silently conflate "no resolvable decimals" with
        "legit zero" (#1778, Codex finding on PR #1774).
        """
        swap = _Attrs(
            token_in="USDC",
            token_out="ETH",
            amount_in_decimal=Decimal(0),  # sentinel, not measured
            amount_out_decimal=Decimal("0.5"),
            effective_price=None,
            slippage_bps=None,
            amount_in_decimal_resolved=False,  # <-- the new signal
            amount_out_decimal_resolved=True,
        )
        result = SimpleNamespace(
            swap_amounts=swap,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        # Unresolved side: suppressed to "" regardless of Decimal value.
        assert entry.amount_in == ""
        # Resolved side: measured value preserved.
        assert entry.amount_out == "0.5"

    def test_swap_amounts_measured_zero_still_records_as_zero(self):
        """A parser that successfully resolves decimals AND sees a measured
        zero (e.g. an LP_CLOSE collecting one side) must still record "0"
        on the ledger — #1768 is still correct. Only the unresolvable
        case is suppressed (#1778)."""
        swap = _Attrs(
            token_in="USDC",
            token_out="ETH",
            amount_in_decimal=Decimal(0),  # legitimately measured zero
            amount_out_decimal=Decimal("0.5"),
            effective_price=None,
            slippage_bps=None,
            amount_in_decimal_resolved=True,  # <-- resolved
            amount_out_decimal_resolved=True,
        )
        result = SimpleNamespace(
            swap_amounts=swap,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.amount_in == "0"
        assert entry.amount_out == "0.5"

    def test_swap_amounts_legacy_parser_missing_resolved_attrs_treated_as_resolved(self):
        """Parsers that predate #1778 do not set ``amount_*_decimal_resolved``
        at all. The ledger's compatibility guard
        (``getattr(swap_amounts, "amount_*_decimal_resolved", True)`` in
        ``almanak/framework/observability/ledger.py``) must treat the
        absence of the flag as "resolved" so those parsers keep recording
        measured zeros as ``"0"``. Without this test, a future cleanup
        could silently blank out old connectors again.
        """
        swap = _Attrs(
            token_in="USDC",
            token_out="ETH",
            amount_in_decimal=Decimal(0),  # legacy parser: legit measured zero
            amount_out_decimal=Decimal("0.5"),
            effective_price=None,
            slippage_bps=None,
            # NOTE: amount_in_decimal_resolved / amount_out_decimal_resolved
            # deliberately OMITTED — simulates a pre-#1778 parser.
        )
        result = SimpleNamespace(
            swap_amounts=swap,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        # getattr fallback => both sides treated as resolved.
        assert entry.amount_in == "0"
        assert entry.amount_out == "0.5"


# ---------------------------------------------------------------------------
# Phase β' — fallback extraction when swap_amounts is absent (lines 146-164).
# ---------------------------------------------------------------------------


class TestFallbackExtraction:
    """When ``result`` has no swap_amounts (or is None), the token/amount
    fields come from the intent itself via a precedence chain:

        token_in: from_token > borrow_token > supply_token > token > ""
        amount_in: amount > borrow_amount > supply_amount > amount_usd > None
    """

    @pytest.mark.parametrize(
        "attr,value",
        [
            ("from_token", "USDC"),
            ("borrow_token", "USDC"),
            ("supply_token", "USDC"),
            ("token", "USDC"),
        ],
    )
    def test_token_in_fallback_chain(self, attr, value):
        """Each link of the token_in fallback chain in isolation."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("ANY_TYPE", **{attr: value})
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.token_in == "USDC"

    def test_token_in_from_token_wins_over_borrow_token(self):
        """Precedence: from_token > borrow_token."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent(
            "ANY",
            from_token="WINS",
            borrow_token="LOSES",
            supply_token="ALSOLOSES",
            token="ALSOLOSES",
        )
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.token_in == "WINS"

    @pytest.mark.parametrize(
        "attr,value,expected",
        [
            ("amount", Decimal("100"), "100"),
            ("borrow_amount", Decimal("200"), "200"),
            ("supply_amount", Decimal("300"), "300"),
            ("amount_usd", Decimal("400.50"), "400.50"),
        ],
    )
    def test_amount_in_fallback_chain(self, attr, value, expected):
        """Each link of the amount_in fallback chain in isolation."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("ANY", token="USDC", **{attr: value})
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.amount_in == expected

    def test_no_tokens_and_no_amounts_leaves_empty_strings(self):
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("HOLD")  # no token attrs, no amount attrs
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.token_in == ""
        assert entry.token_out == ""
        assert entry.amount_in == ""
        assert entry.amount_out == ""

    def test_to_token_copied_from_intent_when_no_swap_amounts(self):
        """The fallback branch reads intent.to_token for token_out."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent(
            "LP_OPEN",
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
        )
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.token_in == "USDC"
        assert entry.token_out == "ETH"


# ---------------------------------------------------------------------------
# Phase γ — tx_hash / gas extraction (lines 171-177).
# ---------------------------------------------------------------------------


class TestTxAndGasExtraction:
    """The first element of ``result.transaction_results`` supplies tx_hash.
    ``gas_used`` comes from ``result.total_gas_used``, ``gas_usd`` from
    ``result.gas_cost_usd``.
    """

    def test_tx_hash_pulled_from_first_transaction_result(self):
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[
                _make_tx_result("0xfirst"),
                _make_tx_result("0xsecond"),
            ],
            total_gas_used=100,
            gas_cost_usd=Decimal("0.10"),
            extracted_data={},
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.tx_hash == "0xfirst"
        assert entry.gas_used == 100
        assert entry.gas_usd == "0.10"

    def test_empty_transaction_results_leaves_tx_hash_empty(self):
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.tx_hash == ""
        assert entry.gas_used == 0
        assert entry.gas_usd == ""

    def test_missing_transaction_results_attr_leaves_tx_hash_empty(self):
        """``hasattr(result, 'transaction_results')`` gating: missing attr → ''"""
        result = SimpleNamespace(
            swap_amounts=None,
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
            # deliberately no transaction_results
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.tx_hash == ""

    def test_none_tx_hash_coalesces_to_empty_string(self):
        """first_tx.tx_hash=None → `first_tx.tx_hash or ""` → ''."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[_make_tx_result(tx_hash=None)],
            total_gas_used=100,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.tx_hash == ""

    def test_total_gas_used_none_coalesces_to_zero(self):
        """``getattr(..., 0) or 0`` maps None → 0."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=None,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.gas_used == 0

    def test_gas_cost_usd_zero_stringified(self):
        """Decimal("0") is not-None → `gas_usd == "0"`."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=Decimal("0"),
            extracted_data={},
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.gas_usd == "0"

    def test_gas_cost_usd_missing_yields_empty_string(self):
        """getattr default of None → stays ''."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            extracted_data={},
            # gas_cost_usd deliberately missing
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.gas_usd == ""


# ---------------------------------------------------------------------------
# Phase δ — error-coalescing (lines 179-180).
# ---------------------------------------------------------------------------


class TestErrorCoalescing:
    """If ``success=False`` AND the caller didn't supply an ``error``, fall
    back to ``result.error``. Otherwise, the caller-supplied value wins.
    """

    def test_failure_without_explicit_error_reads_result_error(self):
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
            error="reverted on-chain",
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(
            strategy_id="s",
            cycle_id="c",
            intent=intent,
            result=result,
            success=False,
            error="",
        )
        assert entry.success is False
        assert entry.error == "reverted on-chain"

    def test_failure_with_explicit_error_preserves_caller_value(self):
        """Caller-supplied `error` takes precedence — no coalescing."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
            error="INNER",
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(
            strategy_id="s",
            cycle_id="c",
            intent=intent,
            result=result,
            success=False,
            error="OUTER",
        )
        assert entry.error == "OUTER"

    def test_success_true_does_not_pull_error_from_result(self):
        """On success=True the coalescing branch is skipped entirely."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
            error="ghost-error",  # should be ignored
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(
            strategy_id="s",
            cycle_id="c",
            intent=intent,
            result=result,
            success=True,
        )
        assert entry.success is True
        assert entry.error == ""

    def test_failure_with_no_result_preserves_error(self):
        """result=None short-circuits the `and result` branch."""
        intent = _make_intent("HOLD")
        entry = build_ledger_entry(
            strategy_id="s",
            cycle_id="c",
            intent=intent,
            result=None,
            success=False,
            error="no execution",
        )
        assert entry.success is False
        assert entry.error == "no execution"

    def test_failure_with_empty_error_and_result_error_none_stays_empty(self):
        """result.error=None → `... or ""` → "" preserved."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
            error=None,
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(
            strategy_id="s",
            cycle_id="c",
            intent=intent,
            result=result,
            success=False,
            error="",
        )
        assert entry.error == ""


# ---------------------------------------------------------------------------
# Phase ε — extracted_data serialization (lines 183-185).
# ---------------------------------------------------------------------------


class TestExtractedDataSerialization:
    """``result.extracted_data`` (when truthy) is routed through
    ``serialize_extracted_data``; round-trippable via ``deserialize_extracted_data``.
    """

    def test_extracted_data_present_yields_json_string(self):
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={"supply": SupplyData(supply_amount=100, a_token_received=99)},
        )
        intent = _make_intent("SUPPLY")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.extracted_data_json
        restored = deserialize_extracted_data(entry.extracted_data_json)
        assert isinstance(restored["supply"], SupplyData)
        assert restored["supply"].supply_amount == 100

    def test_empty_extracted_data_dict_yields_empty_string(self):
        """Falsy extracted_data ({} is falsy) skips serialization entirely."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.extracted_data_json == ""

    def test_missing_extracted_data_attr_yields_empty_string(self):
        """hasattr gating: no extracted_data attr on result → '' preserved."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            # no extracted_data attribute
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.extracted_data_json == ""


# ---------------------------------------------------------------------------
# Phase ζ — multi-tx bundle capture (lines 188-207).
# ---------------------------------------------------------------------------


class TestMultiTxBundle:
    """Multi-action intents (approve+swap, approve+supply) produce >1 entries
    in ``result.transaction_results``. The builder augments the
    ``extracted_data_json`` payload with an ``all_tx_results`` array listing
    every leg's hash/gas/success.
    """

    def test_multi_tx_bundle_captured_in_extracted_data_json(self):
        tx1 = _make_tx_result("0xapprove", gas_used=50_000, success=True)
        tx2 = _make_tx_result("0xswap", gas_used=200_000, success=True)
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[tx1, tx2],
            total_gas_used=250_000,
            gas_cost_usd=Decimal("3.75"),
            extracted_data={"supply": SupplyData(supply_amount=100, a_token_received=99)},
        )
        intent = _make_intent(
            "SUPPLY",
            protocol="aave_v3",
            supply_token="USDC",
            amount=Decimal("100"),
        )
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        parsed = json.loads(entry.extracted_data_json)
        assert "all_tx_results" in parsed
        assert len(parsed["all_tx_results"]) == 2
        assert parsed["all_tx_results"][0]["tx_hash"] == "0xapprove"
        assert parsed["all_tx_results"][0]["gas_used"] == 50_000
        assert parsed["all_tx_results"][0]["success"] is True
        assert parsed["all_tx_results"][1]["tx_hash"] == "0xswap"

    def test_single_tx_bundle_does_not_add_all_tx_results(self):
        """len == 1 → branch is not taken; no `all_tx_results` key added."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[_make_tx_result("0xsolo")],
            total_gas_used=100,
            gas_cost_usd=None,
            extracted_data={"supply": SupplyData(supply_amount=1, a_token_received=1)},
        )
        intent = _make_intent("SUPPLY")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        parsed = json.loads(entry.extracted_data_json)
        assert "all_tx_results" not in parsed

    def test_multi_tx_without_extracted_data_still_emits_sub_transactions(self):
        """VIB-4087 / CodeRabbit follow-up — when ``extracted_data`` is empty
        but ``transaction_results`` exist, the row must still carry the
        ``sub_transactions`` array so the APPROVAL / ACTION / INCIDENTAL
        leg breakdown survives for connectors that don't emit a typed
        payload. Pre-fix this returned ``""`` and the audit trail for
        such intents was lost.

        Operators previously distinguished "single tx" from "missing
        data" by an empty ``extracted_data_json``; that distinction now
        moves to ``json_extract(extracted_data_json, '$.sub_transactions')``
        — present when tx_results exist, absent otherwise.
        """
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[
                _make_tx_result("0xa"),
                _make_tx_result("0xb"),
            ],
            total_gas_used=200,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.extracted_data_json != ""
        parsed = json.loads(entry.extracted_data_json)
        assert "sub_transactions" in parsed
        assert len(parsed["sub_transactions"]) == 2
        assert parsed["sub_transactions"][0]["tx_hash"] == "0xa"
        assert parsed["sub_transactions"][1]["tx_hash"] == "0xb"

    def test_no_tx_results_and_no_extracted_data_returns_empty(self):
        """The empty-empty case stays empty — neither receipts nor a parsed
        payload exist, so there's no audit content to record."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.extracted_data_json == ""

    def test_multi_tx_missing_attrs_default_to_empty_string_zero_true(self):
        """``getattr(tr, 'tx_hash', '') or ''`` + int default 0 + bool default True.

        The multi-tx augmentation uses ``getattr(..., default)`` for every
        field, so a bare secondary tx surfaces the defaults. The FIRST tx
        must still have ``.tx_hash`` because line 173 reads it directly
        (pre-fallback); a bare first tx would AttributeError before reaching
        the multi-tx loop.
        """

        class BareTx:
            pass  # no tx_hash, no gas_used, no success

        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[_make_tx_result("0xfirst"), BareTx()],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={"supply": SupplyData(supply_amount=1, a_token_received=1)},
        )
        intent = _make_intent("SUPPLY")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        parsed = json.loads(entry.extracted_data_json)
        # Second (bare) tx surfaces the three getattr defaults.
        assert parsed["all_tx_results"][1] == {
            "tx_hash": "",
            "gas_used": 0,
            "success": True,
        }

    def test_multi_tx_json_decode_failure_preserves_serialization(self, monkeypatch):
        """If the existing extracted_data_json somehow can't be json-decoded,
        the try/except keeps the original serialization intact verbatim
        (``_build_extracted_data_json`` except branch).

        Forces the failure by monkey-patching the
        ``serialize_extracted_data`` name **as looked up from
        ``_build_extracted_data_json``** to emit a deliberately
        unparseable payload. The augmentation branch (len(tx_results) > 1)
        runs, ``json.loads`` raises ``JSONDecodeError``, and the helper
        returns the original, unaugmented bytes unchanged.
        """
        from almanak.framework.observability import ledger as _ledger_mod

        sentinel = "this-is-not-json {[,"
        monkeypatch.setattr(_ledger_mod, "serialize_extracted_data", lambda _d: sentinel)

        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[
                _make_tx_result("0xa"),
                _make_tx_result("0xb"),
            ],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={"supply": SupplyData(supply_amount=1, a_token_received=1)},
        )
        intent = _make_intent("SUPPLY")
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)

        # Except branch hit: original (unparseable) serialization preserved
        # byte-for-byte -- no "all_tx_results" key injected, no reserialization.
        assert entry.extracted_data_json == sentinel
        with pytest.raises(json.JSONDecodeError):
            json.loads(entry.extracted_data_json)


# ---------------------------------------------------------------------------
# Edge cases — None result, missing intent attrs.
# ---------------------------------------------------------------------------


class TestResultNoneEdgeCases:
    """result=None short-circuits the entire extraction pipeline."""

    def test_none_result_produces_minimal_entry(self):
        intent = _make_intent("HOLD", protocol="")
        entry = build_ledger_entry(
            strategy_id="s",
            cycle_id="c",
            intent=intent,
            result=None,
            success=False,
            error="no execution",
        )
        assert entry.intent_type == "HOLD"
        assert entry.tx_hash == ""
        assert entry.gas_used == 0
        assert entry.gas_usd == ""
        assert entry.extracted_data_json == ""
        assert entry.token_in == ""
        assert entry.amount_in == ""
        assert entry.error == "no execution"

    def test_none_result_with_intent_fallback_still_captures_tokens(self):
        """Even with result=None, the intent.* fallback chain populates tokens."""
        intent = _make_intent(
            "SUPPLY",
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("50"),
        )
        entry = build_ledger_entry(
            strategy_id="s",
            cycle_id="c",
            intent=intent,
            result=None,
        )
        assert entry.token_in == "USDC"
        assert entry.amount_in == "50"
        assert entry.protocol == "aave_v3"


class TestProtocolFallback:
    """Intent.protocol coalesced through `or ''`."""

    def test_protocol_none_coalesces_to_empty(self):
        class NoProto:
            intent_type = SimpleNamespace(value="SWAP")
            protocol = None

        entry = build_ledger_entry(
            strategy_id="s",
            cycle_id="c",
            intent=NoProto(),
            result=None,
        )
        assert entry.protocol == ""

    def test_protocol_missing_attr_coalesces_to_empty(self):
        class NoProto:
            intent_type = SimpleNamespace(value="SWAP")
            # no .protocol attr at all

        entry = build_ledger_entry(
            strategy_id="s",
            cycle_id="c",
            intent=NoProto(),
            result=None,
        )
        assert entry.protocol == ""


# ---------------------------------------------------------------------------
# Wiring — strategy_id / cycle_id / chain threaded into the entry verbatim.
# ---------------------------------------------------------------------------


class TestWiringFields:
    """Strategy/cycle/chain are pass-through, not extracted."""

    def test_strategy_id_and_cycle_id_wired_through(self):
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(
            strategy_id="strat:abc",
            cycle_id="cycle-42",
            intent=intent,
            result=None,
        )
        assert entry.strategy_id == "strat:abc"
        assert entry.cycle_id == "cycle-42"

    def test_chain_wired_through(self):
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(
            strategy_id="s",
            cycle_id="c",
            intent=intent,
            result=None,
            chain="base",
        )
        assert entry.chain == "base"

    def test_success_defaults_to_true(self):
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(
            strategy_id="s",
            cycle_id="c",
            intent=intent,
            result=None,
        )
        assert entry.success is True


# ---------------------------------------------------------------------------
# Phase 5k-golden — LedgerEntry serialization contract lock.
#
# The SQLite INSERT at backends/sqlite.py:2291-2322 is a NAMED column list,
# but each column maps to a specific ``entry.<attribute>`` read. Silent field
# renames here would break the write path at runtime. Pin the dataclass
# keyset + field count so any intentional change has to also touch this test
# (and, by extension, the INSERT column list) in the SAME PR.
# ---------------------------------------------------------------------------


# Order-insensitive golden keyset — post-Phase 4 (deployment_id + execution_mode).
_LEDGER_ENTRY_GOLDEN_KEYS: frozenset[str] = frozenset(
    {
        "id",
        "cycle_id",
        "strategy_id",
        "deployment_id",
        "execution_mode",
        "timestamp",
        "intent_type",
        "token_in",
        "amount_in",
        "token_out",
        "amount_out",
        "effective_price",
        "slippage_bps",
        "gas_used",
        "gas_usd",
        "tx_hash",
        "chain",
        "protocol",
        "success",
        "error",
        "extracted_data_json",
        # VIB-3480: audit-grade replay columns (added to DDL, INSERT, and proto in same PR)
        "price_inputs_json",
        "pre_state_json",
        "post_state_json",
    }
)


class TestLedgerEntryGoldenKeyset:
    """Pin the LedgerEntry dataclass contract at Phase 5k.

    The SQLite INSERT at backends/sqlite.py names 24 columns and pairs each
    with a specific attribute read. Any rename here must also update the
    INSERT column list + value tuple in the SAME PR; this golden set forces
    that coupling.
    """

    def test_dataclass_fields_match_golden(self):
        actual = frozenset(f.name for f in fields(LedgerEntry))
        missing = _LEDGER_ENTRY_GOLDEN_KEYS - actual
        extra = actual - _LEDGER_ENTRY_GOLDEN_KEYS
        assert not missing, f"missing golden fields: {sorted(missing)}"
        assert not extra, (
            f"unexpected fields: {sorted(extra)}. "
            "If intentional, update _LEDGER_ENTRY_GOLDEN_KEYS AND the SQLite "
            "INSERT column list in backends/sqlite.py in the same PR."
        )
        assert actual == _LEDGER_ENTRY_GOLDEN_KEYS

    def test_dataclass_field_count_is_24(self):
        assert len(fields(LedgerEntry)) == 24

    def test_to_dict_and_asdict_keys_match(self):
        e = LedgerEntry()
        assert set(e.to_dict().keys()) == set(asdict(e).keys())

    def test_to_dict_timestamp_is_iso_string(self):
        ts = datetime(2026, 4, 1, 12, 30, 45, tzinfo=UTC)
        e = LedgerEntry(timestamp=ts)
        d = e.to_dict()
        assert isinstance(d["timestamp"], str)
        assert d["timestamp"] == ts.isoformat()


class TestPerpOpenExtraction:
    """VIB-3587: PERP_OPEN collateral fields extracted into token_in/amount_in."""

    def test_perp_open_collateral_token_extracted(self):
        """PERP_OPEN: token_in comes from collateral_token, amount_in from collateral_amount."""
        intent = _Attrs(intent_type=_Attrs(value="PERP_OPEN"), protocol="gmx_v2")
        intent.collateral_token = "WETH"
        intent.collateral_amount = "0.005"
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.intent_type == "PERP_OPEN"
        assert entry.token_in == "WETH"
        assert entry.amount_in == "0.005"

    def test_perp_open_empty_collateral_falls_back_to_empty(self):
        """PERP_OPEN without collateral fields records empty strings (not crash)."""
        intent = _Attrs(intent_type=_Attrs(value="PERP_OPEN"), protocol="gmx_v2")
        # No collateral_token / collateral_amount attributes
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        entry = build_ledger_entry(strategy_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.intent_type == "PERP_OPEN"
        assert entry.token_in == ""
        assert entry.amount_in == ""
