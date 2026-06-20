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
    _stamp_lp_close_discriminator,
    _stamp_lp_close_native_amounts,
    _stamp_lp_open_native_amounts,
    _stamp_v4_lp_close_fees,
    _stamp_v4_lp_close_native_principal,
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=None)
        assert entry.intent_type == "SWAP"

    def test_raw_string_intent_type_falls_through_to_str(self):
        """A plain-string intent_type (no .value) is stringified directly."""

        class StrTypeIntent:
            intent_type = "LP_OPEN"  # not an enum
            protocol = "uniswap_v3"

        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=StrTypeIntent(), result=None)
        assert entry.intent_type == "LP_OPEN"

    def test_missing_intent_type_attr_yields_empty_string(self):
        class NoType:
            protocol = ""

        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=NoType(), result=None)
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
            deployment_id="s",
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
                    amount0=1_000_000_000_000_000_000,  # raw 1 WETH (18 dp)
                    amount1=20_000_000,  # raw 20 USDC (6 dp)
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
            deployment_id="s",
            cycle_id="c",
            intent=intent,
            result=result,
            chain="arbitrum",
        )
        assert entry.intent_type == "LP_OPEN"
        assert entry.protocol == "uniswap_v3"
        assert entry.token_in == "WETH"
        assert entry.token_out == "USDC"
        # VIB-3450: amount_in/amount_out come from LPOpenData on-chain actuals.
        # VIB-5036: scaled to human units via token decimals (WETH 18, USDC 6).
        assert entry.amount_in == "1"
        assert entry.amount_out == "20"
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
            deployment_id="s",
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

    def test_withdraw_usd_only_sizing_leaves_amount_unmeasured(self):
        """WITHDRAW sized only by ``amount_usd``: token amount stays ``""``.

        This test previously asserted ``amount_in == "250.50"`` — it ENCODED
        the VIB-5060 units bug. ``transaction_ledger.amount_in`` is human
        units of ``token_in`` (VIB-5036 contract); ``amount_usd`` only
        coincides with that for stables (USDC here), and the same chain wrote
        a $2 clip as ``amount_in="2.00" / token_in="WBTC"`` (~$126k notional)
        on failed swaps. USD-only sizing means the token amount is
        unmeasured: Empty != Zero.
        """
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.intent_type == "WITHDRAW"
        assert entry.token_in == "USDC"
        assert entry.amount_in == ""

    def test_failed_swap_usd_clip_not_written_as_token_amount(self):
        """VIB-5060: a failed USD-sized swap must not stamp the USD clip as
        ``amount_in`` under the input token's symbol.

        Live repro (stage deployment 33a657c4): a failed $2 WBTC→USDC sell
        rendered as ``2.00 WBTC → — USDC`` (~$126k notional) because the
        intent-attr fallback chained ``amount_usd`` into the token-units
        column. The amounts are unmeasured for a never-executed swap.
        """
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent(
            "SWAP",
            protocol="uniswap_v3",
            from_token="WBTC",
            to_token="USDC",
            amount_usd=Decimal("2.00"),
        )
        entry = build_ledger_entry(
            deployment_id="s", cycle_id="c", intent=intent, result=result, success=False
        )
        assert entry.token_in == "WBTC"
        assert entry.token_out == "USDC"
        assert entry.amount_in == ""
        assert entry.amount_out == ""

    def test_token_sized_swap_fallback_still_writes_amount(self):
        """The ``intent.amount`` link (token units by contract) is preserved."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent(
            "SWAP",
            protocol="uniswap_v3",
            from_token="WBTC",
            to_token="USDC",
            amount=Decimal("0.0001"),
        )
        entry = build_ledger_entry(
            deployment_id="s", cycle_id="c", intent=intent, result=result, success=False
        )
        assert entry.amount_in == "0.0001"

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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        """On-chain LPOpenData.amount0/amount1 populate amount_in/amount_out,
        SCALED to human units via the token decimals (VIB-5036)."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[_make_tx_result("0xlpo")],
            total_gas_used=200_000,
            gas_cost_usd=Decimal("1.00"),
            extracted_data={
                "lp_open_data": LPOpenData(
                    position_id=999,
                    liquidity=500_000,
                    amount0=1_000_000_000_000_000_000,  # raw 1 WETH (18 dp)
                    amount1=2_500_000,  # raw 2.5 USDC (6 dp)
                )
            },
        )
        intent = _make_intent("LP_OPEN", protocol="uniswap_v3", from_token="WETH", to_token="USDC")
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result, chain="arbitrum")
        # VIB-5036: raw on-chain integers are scaled to human units at write.
        assert entry.amount_in == "1"
        assert entry.amount_out == "2.5"

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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.amount_in == ""
        assert entry.amount_out == ""

    def test_lp_open_zero_amounts_record_as_zero_not_empty(self):
        """LPOpenData.amount0 = 0 is a measured zero and must record as '0'
        (Empty != Zero), even without resolvable decimals (0 scaled is 0).
        The non-zero side is scaled to human units (VIB-5036)."""
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={
                "lp_open_data": LPOpenData(
                    position_id=3,
                    amount0=0,
                    amount1=500_000_000,  # raw 500 USDC (6 dp)
                )
            },
        )
        intent = _make_intent("LP_OPEN", protocol="uniswap_v3", from_token="WETH", to_token="USDC")
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result, chain="arbitrum")
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=None)
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
                    amount1=250_000_000,  # raw 250 USDC (6 dp) on-chain actual
                )
            },
        )
        intent = _make_intent(
            "LP_OPEN",
            protocol="uniswap_v3",
            from_token="WETH",
            to_token="USDC",
            amount0=Decimal("3.5"),  # fallback for the missing side (already human)
            amount1=Decimal("999"),  # should NOT win; lp_open_data.amount1 = 250 USDC
        )
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result, chain="arbitrum")
        # token0 side: lp_open_data.amount0 is None → fallback to intent.amount0 (human)
        assert entry.amount_in == "3.5"
        # token1 side: lp_open_data.amount1 is 250 USDC raw → scaled, on-chain wins
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.token_in == "WINS"

    @pytest.mark.parametrize(
        "attr,value,expected",
        [
            ("amount", Decimal("100"), "100"),
            ("borrow_amount", Decimal("200"), "200"),
            ("supply_amount", Decimal("300"), "300"),
            # amount_usd is deliberately NOT a link (VIB-5060): USD is the
            # wrong unit for the token-units amount_in column; USD-only
            # sizing means the token amount is unmeasured (Empty != Zero).
            ("amount_usd", Decimal("400.50"), ""),
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
            deployment_id="s",
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
            deployment_id="s",
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
            deployment_id="s",
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
            deployment_id="s",
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
            deployment_id="s",
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.extracted_data_json == ""

    def test_primitive_money_legs_round_trip_as_typed_object(self):
        """The connector-DECLARED ``PrimitiveMoneyLegs`` survives serialize →
        deserialize as a TYPED object (not a raw dict), preserving Empty≠Zero.

        Without the ``PrimitiveMoneyLegs`` reconstruction branch in
        ``deserialize_extracted_data`` the legs come back as a plain dict, so the
        Pendle redeem accounting handler (which reads the PERSISTED blob via
        ``_pt_context``) silently misses the PT-count INPUT leg and mis-sources
        the basis from the SY-asset amount (VIB-4988 PEN6).
        """
        from decimal import Decimal as _D

        from almanak.connectors._strategy_base.primitive_money_leg import (
            MoneyLegRole,
            PrimitiveMoneyLeg,
            PrimitiveMoneyLegs,
        )
        from almanak.framework.accounting.measured import MeasuredMoney

        legs = PrimitiveMoneyLegs.of(
            PrimitiveMoneyLeg.input("PT-wstETH-25JUN2026", MeasuredMoney.measured(_D("0.012378"))),
            PrimitiveMoneyLeg.output("WSTETH", MeasuredMoney.unmeasured()),
        )
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={"primitive_money_legs": legs},
        )
        intent = _make_intent("WITHDRAW")
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
        restored = deserialize_extracted_data(entry.extracted_data_json)["primitive_money_legs"]
        assert isinstance(restored, PrimitiveMoneyLegs)
        assert restored == legs
        pt_leg = restored.by_role(MoneyLegRole.INPUT)[0]
        assert pt_leg.token == "PT-wstETH-25JUN2026"
        assert pt_leg.amount.value == _D("0.012378")
        # The unmeasured OUTPUT leg stays unmeasured (never a fabricated zero).
        assert restored.by_role(MoneyLegRole.OUTPUT)[0].amount.is_unmeasured

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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)

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
            deployment_id="s",
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
            deployment_id="s",
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
            deployment_id="s",
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
            deployment_id="s",
            cycle_id="c",
            intent=NoProto(),
            result=None,
        )
        assert entry.protocol == ""


# ---------------------------------------------------------------------------
# Wiring — deployment_id / cycle_id / chain threaded into the entry verbatim.
# ---------------------------------------------------------------------------


class TestWiringFields:
    """Strategy/cycle/chain are pass-through, not extracted."""

    def test_deployment_id_and_cycle_id_wired_through(self):
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(
            deployment_id="strat:abc",
            cycle_id="cycle-42",
            intent=intent,
            result=None,
        )
        assert entry.deployment_id == "strat:abc"
        assert entry.cycle_id == "cycle-42"

    def test_chain_wired_through(self):
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(
            deployment_id="s",
            cycle_id="c",
            intent=intent,
            result=None,
            chain="base",
        )
        assert entry.chain == "base"

    def test_success_defaults_to_true(self):
        intent = _make_intent("SWAP")
        entry = build_ledger_entry(
            deployment_id="s",
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

    def test_dataclass_field_count_is_23(self):
        assert len(fields(LedgerEntry)) == 23

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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
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
        entry = build_ledger_entry(deployment_id="s", cycle_id="c", intent=intent, result=result)
        assert entry.intent_type == "PERP_OPEN"
        assert entry.token_in == ""
        assert entry.amount_in == ""


# ---------------------------------------------------------------------------
# VIB-4275 — _stamp_lp_close_discriminator branch coverage.
#
# The close RECEIPT cannot carry the closing NFT's token id; the close INTENT
# can. This helper stamps intent.position_id onto the result's frozen
# LPCloseData just before serialization so the close-side accounting resolver
# can attribute a co-pool close to its OWN prior open. Every guard below is a
# fail-closed / no-op branch — exercise them all (the function is otherwise
# only indirectly covered, which the CRAP gate flags).
# ---------------------------------------------------------------------------


def _lp_close_result(position_id=None, *, extracted="dict"):
    """Build a result whose extracted_data carries an LPCloseData (or a stub)."""
    if extracted == "dict":
        close = LPCloseData(
            amount0_collected=480_000,
            amount1_collected=170_000,
            fees0=5_000,
            fees1=2_000,
            liquidity_removed=1_000_000,
            position_id=position_id,
        )
        return SimpleNamespace(extracted_data={"lp_close_data": close})
    return SimpleNamespace(extracted_data=extracted)


class TestStampLpCloseDiscriminator:
    """Direct branch coverage for ``_stamp_lp_close_discriminator`` (VIB-4275)."""

    @pytest.mark.parametrize("intent_type", ["LP_CLOSE", "LP_COLLECT_FEES"])
    def test_stamps_intent_position_id_onto_close_data(self, intent_type):
        intent = _make_intent(intent_type, position_id="5467895")
        result = _lp_close_result(position_id=None)
        _stamp_lp_close_discriminator(intent, result, intent_type)
        assert result.extracted_data["lp_close_data"].position_id == "5467895"

    def test_non_lp_close_intent_type_is_noop(self):
        intent = _make_intent("SWAP", position_id="5467895")
        result = _lp_close_result(position_id=None)
        _stamp_lp_close_discriminator(intent, result, "SWAP")
        assert result.extracted_data["lp_close_data"].position_id is None

    @pytest.mark.parametrize("intent_type", ["LP_CLOSE", "LP_COLLECT_FEES"])
    def test_fungible_lp_protocol_skips_stamp(self, intent_type):
        # VIB-4968: on Curve the close intent's position_id is the LP-token
        # *amount* to burn (a human-decimal string), NOT an NFT id. It must NOT
        # be stamped as a per-position discriminator — fungible LP has no co-leg
        # to disambiguate, so the close event must carry position_id=None.
        intent = _make_intent(intent_type, position_id="99.0")
        result = _lp_close_result(position_id=None)
        _stamp_lp_close_discriminator(intent, result, intent_type, protocol="curve")
        assert result.extracted_data["lp_close_data"].position_id is None

    def test_nft_lp_protocol_still_stamps(self):
        # Non-fungible venues (e.g. uniswap_v3) keep the VIB-4275 discriminator.
        intent = _make_intent("LP_CLOSE", position_id="5467895")
        result = _lp_close_result(position_id=None)
        _stamp_lp_close_discriminator(intent, result, "LP_CLOSE", protocol="uniswap_v3")
        assert result.extracted_data["lp_close_data"].position_id == "5467895"

    @pytest.mark.parametrize("degenerate", [None, "", 0, "0", "  0  "])
    def test_degenerate_intent_position_id_is_noop(self, degenerate):
        # None/""/0/"0" (and whitespace-padded "0") are never stamped — they are
        # exactly what the resolver would discard (gemini review on #2459).
        intent = _make_intent("LP_CLOSE", position_id=degenerate)
        result = _lp_close_result(position_id=None)
        _stamp_lp_close_discriminator(intent, result, "LP_CLOSE")
        assert result.extracted_data["lp_close_data"].position_id is None

    def test_missing_intent_position_id_attr_is_noop(self):
        intent = _make_intent("LP_CLOSE")  # no position_id attr at all
        result = _lp_close_result(position_id=None)
        _stamp_lp_close_discriminator(intent, result, "LP_CLOSE")
        assert result.extracted_data["lp_close_data"].position_id is None

    def test_none_result_is_noop(self):
        intent = _make_intent("LP_CLOSE", position_id="5467895")
        # Must not raise when there is no result to stamp onto.
        _stamp_lp_close_discriminator(intent, None, "LP_CLOSE")

    def test_non_dict_extracted_data_is_noop(self):
        intent = _make_intent("LP_CLOSE", position_id="5467895")
        result = _lp_close_result(extracted=["not", "a", "dict"])
        _stamp_lp_close_discriminator(intent, result, "LP_CLOSE")
        assert result.extracted_data == ["not", "a", "dict"]

    def test_absent_lp_close_data_is_noop(self):
        intent = _make_intent("LP_CLOSE", position_id="5467895")
        result = _lp_close_result(extracted={})
        _stamp_lp_close_discriminator(intent, result, "LP_CLOSE")
        assert result.extracted_data == {}

    def test_close_data_without_position_id_attr_is_noop(self):
        intent = _make_intent("LP_CLOSE", position_id="5467895")
        stub = SimpleNamespace(amount0_collected=1)  # no position_id attribute
        result = SimpleNamespace(extracted_data={"lp_close_data": stub})
        _stamp_lp_close_discriminator(intent, result, "LP_CLOSE")
        assert result.extracted_data["lp_close_data"] is stub

    def test_existing_discriminator_is_preserved_not_clobbered(self):
        # Empty ≠ Zero: a real parser-emitted id must not be overwritten by the
        # intent's. Idempotent stamp.
        intent = _make_intent("LP_CLOSE", position_id="111")
        result = _lp_close_result(position_id="999")
        _stamp_lp_close_discriminator(intent, result, "LP_CLOSE")
        assert result.extracted_data["lp_close_data"].position_id == "999"

    def test_non_dataclass_close_data_replace_failure_is_noop(self):
        # A duck-typed stub that HAS a falsy position_id attr (so it passes the
        # guards) but is not a dataclass — dataclasses.replace raises TypeError,
        # which is swallowed rather than crashing the ledger-write path.
        intent = _make_intent("LP_CLOSE", position_id="5467895")
        stub = SimpleNamespace(position_id=None)
        result = SimpleNamespace(extracted_data={"lp_close_data": stub})
        _stamp_lp_close_discriminator(intent, result, "LP_CLOSE")
        assert result.extracted_data["lp_close_data"] is stub
        assert result.extracted_data["lp_close_data"].position_id is None


# ---------------------------------------------------------------------------
# VIB-4482 (P-V1-A) — _stamp_v4_lp_close_fees branch coverage.
#
# Uniswap V4's ModifyLiquidity burn carries no amounts and bundles fees into the
# single withdrawal Transfer, so the receipt parser emits fees0=fees1=None
# ("BUNDLED" taxonomy). The runner reads tokens_owed0/1 on-chain BEFORE the burn
# (a post-burn read returns zero liquidity) and threads the pair here; the helper
# stamps it onto the frozen LPCloseData so the LP accounting handler emits
# MEASURED fees. Empty != Zero throughout: a failed/absent read leaves None.
# ---------------------------------------------------------------------------


def _v4_close_result(*, fees0=None, fees1=None, extracted="dict"):
    """Build a result whose extracted_data carries a V4-shaped LPCloseData."""
    if extracted == "dict":
        close = LPCloseData(
            amount0_collected=480_000,
            amount1_collected=170_000,
            fees0=fees0,
            fees1=fees1,
            liquidity_removed=1_000_000,
            pool_address="0x" + "a" * 64,  # 32-byte V4 PoolId shape
            source="modify_liquidity",
            currency0="0x" + "1" * 40,
            currency1="0x" + "2" * 40,
        )
        return SimpleNamespace(extracted_data={"lp_close_data": close})
    return SimpleNamespace(extracted_data=extracted)


class TestStampV4LpCloseFees:
    """Direct branch coverage for ``_stamp_v4_lp_close_fees`` (VIB-4482)."""

    @pytest.mark.parametrize("intent_type", ["LP_CLOSE", "LP_COLLECT_FEES"])
    def test_measured_fees_stamped_and_taxonomy_flips(self, intent_type):
        result = _v4_close_result(fees0=None, fees1=None)
        _stamp_v4_lp_close_fees(result, intent_type, (1234, 5678))
        close = result.extracted_data["lp_close_data"]
        assert close.fees0 == 1234
        assert close.fees1 == 5678
        # Re-derived from the now-measured fees — an on-chain tokens_owed read
        # IS a separated, exact fee value (not the parser's BUNDLED default).
        assert close.fee_separation_method == "SEPARATE"
        assert close.fee_confidence == "EXACT"

    def test_currency_ordering_is_positional(self):
        # fees0 <- tokens_owed0 (currency0 leg); fees1 <- tokens_owed1. Guards a
        # future accidental transpose; both gateway read and parser derive order
        # from the same canonical PoolKey, so positional mapping is correct.
        result = _v4_close_result()
        _stamp_v4_lp_close_fees(result, "LP_CLOSE", (7, 9))
        close = result.extracted_data["lp_close_data"]
        assert (close.fees0, close.fees1) == (7, 9)

    def test_measured_zero_is_honored(self):
        # 0 = the gateway MEASURED zero owed fees — distinct from None
        # (unmeasured). Empty != Zero.
        result = _v4_close_result(fees0=None, fees1=None)
        _stamp_v4_lp_close_fees(result, "LP_CLOSE", (0, 0))
        close = result.extracted_data["lp_close_data"]
        assert close.fees0 == 0
        assert close.fees1 == 0
        assert close.fee_separation_method == "SEPARATE"

    def test_none_read_preserves_none(self):
        # A failed / unavailable on-chain read => fees stay None (unmeasured) and
        # the parser's honest BUNDLED taxonomy is preserved. Never fabricate 0.
        result = _v4_close_result(fees0=None, fees1=None)
        _stamp_v4_lp_close_fees(result, "LP_CLOSE", None)
        close = result.extracted_data["lp_close_data"]
        assert close.fees0 is None
        assert close.fees1 is None
        assert close.fee_separation_method == "BUNDLED"

    def test_parser_measured_fees_not_clobbered(self):
        # If a parser somehow already measured fees, the gateway read must NOT
        # overwrite them (idempotent, Empty != Zero).
        result = _v4_close_result(fees0=99, fees1=88)
        _stamp_v4_lp_close_fees(result, "LP_CLOSE", (1, 2))
        close = result.extracted_data["lp_close_data"]
        assert close.fees0 == 99
        assert close.fees1 == 88

    def test_partial_parser_fee_not_clobbered(self):
        # Even a single measured leg (fees0 set, fees1 None) blocks the stamp —
        # we never half-overwrite a parser that emitted something.
        result = _v4_close_result(fees0=99, fees1=None)
        _stamp_v4_lp_close_fees(result, "LP_CLOSE", (1, 2))
        close = result.extracted_data["lp_close_data"]
        assert close.fees0 == 99
        assert close.fees1 is None

    def test_non_v4_shaped_close_is_noop(self):
        # A non-V4 close (e.g. V3, which separates fees from its Collect log) leaves
        # ``currency0`` unset on LPCloseData — the capability signal this stamp gates
        # on. No protocol string involved; the data shape alone discriminates.
        close = LPCloseData(
            amount0_collected=480_000,
            amount1_collected=170_000,
            fees0=None,
            fees1=None,
            liquidity_removed=1_000_000,
            pool_address="0x" + "a" * 40,
            source="decrease_liquidity",
            # currency0 / currency1 left at their None default → not V4-shaped.
        )
        result = SimpleNamespace(extracted_data={"lp_close_data": close})
        _stamp_v4_lp_close_fees(result, "LP_CLOSE", (1, 2))
        assert result.extracted_data["lp_close_data"].fees0 is None

    @pytest.mark.parametrize("intent_type", ["LP_OPEN", "SWAP", "SUPPLY"])
    def test_non_close_intent_type_is_noop(self, intent_type):
        result = _v4_close_result(fees0=None, fees1=None)
        _stamp_v4_lp_close_fees(result, intent_type, (1, 2))
        assert result.extracted_data["lp_close_data"].fees0 is None

    def test_none_fees_pair_is_noop_short_circuit(self):
        # Earliest guard: fees is None => return immediately.
        result = _v4_close_result(fees0=None, fees1=None)
        _stamp_v4_lp_close_fees(result, "LP_CLOSE", None)
        assert result.extracted_data["lp_close_data"].fees0 is None

    def test_none_result_is_noop(self):
        # Must not raise when there is no result to stamp onto.
        _stamp_v4_lp_close_fees(None, "LP_CLOSE", (1, 2))

    def test_non_dict_extracted_data_is_noop(self):
        result = _v4_close_result(extracted=["not", "a", "dict"])
        _stamp_v4_lp_close_fees(result, "LP_CLOSE", (1, 2))
        assert result.extracted_data == ["not", "a", "dict"]

    def test_absent_lp_close_data_is_noop(self):
        result = _v4_close_result(extracted={})
        _stamp_v4_lp_close_fees(result, "LP_CLOSE", (1, 2))
        assert result.extracted_data == {}

    def test_close_data_without_fees0_attr_is_noop(self):
        stub = SimpleNamespace(amount0_collected=1)  # no fees0 attribute
        result = SimpleNamespace(extracted_data={"lp_close_data": stub})
        _stamp_v4_lp_close_fees(result, "LP_CLOSE", (1, 2))
        assert result.extracted_data["lp_close_data"] is stub

    def test_non_dataclass_close_data_replace_failure_is_noop(self):
        # A duck-typed stub with fees0/fees1 = None and a V4-shaped currency0
        # (passes guards) but is not a dataclass — dataclasses.replace raises
        # TypeError, swallowed rather than crashing the ledger-write path.
        stub = SimpleNamespace(fees0=None, fees1=None, currency0="0x" + "1" * 40)
        result = SimpleNamespace(extracted_data={"lp_close_data": stub})
        _stamp_v4_lp_close_fees(result, "LP_CLOSE", (1, 2))
        assert result.extracted_data["lp_close_data"] is stub
        assert result.extracted_data["lp_close_data"].fees0 is None

    def test_build_ledger_entry_threads_v4_fees_end_to_end(self):
        # Integration through the public entrypoint: a V4 LP_CLOSE with a
        # gateway-read fee pair lands measured fees on the serialized row.
        intent = _make_intent("LP_CLOSE", protocol="uniswap_v4", position_id="5467895")
        result = _v4_close_result(fees0=None, fees1=None)
        entry = build_ledger_entry(
            deployment_id="d",
            cycle_id="c",
            intent=intent,
            result=result,
            chain="base",
            success=True,
            v4_lp_close_fees=(4242, 2424),
        )
        extracted = json.loads(entry.extracted_data_json)
        close = extracted["lp_close_data"]
        assert close["fees0"] == "4242"
        assert close["fees1"] == "2424"


class TestFailedSwapRowRoundTrip:
    """VIB-5060 end-to-end at the PERSISTED row, not just the builder.

    Phase 1 spec critique (Codex): the ticket contract is on
    ``transaction_ledger.amount_in`` — a regression in the writer, DB adapter,
    or serialization layer that stores "0" or the USD clip would pass the
    builder-level tests while still violating the contract. Pin the round
    trip: build → save through the real SQLite store → read the raw row back.
    """

    def test_failed_swap_row_roundtrip_persists_empty_amount(self, tmp_path):
        import asyncio
        import sqlite3

        from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent(
            "SWAP",
            protocol="uniswap_v3",
            from_token="WBTC",
            to_token="USDC",
            amount_usd=Decimal("2.00"),
        )
        entry = build_ledger_entry(
            deployment_id="dep-roundtrip", cycle_id="c1", intent=intent, result=result, success=False
        )

        db_path = str(tmp_path / "roundtrip.db")

        async def _persist() -> None:
            store = SQLiteStore(SQLiteConfig(db_path=db_path))
            await store.initialize()
            try:
                await store.save_ledger_entry(entry)
            finally:
                await store.close()

        asyncio.run(_persist())

        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(
                "SELECT token_in, amount_in, token_out, amount_out, success "
                "FROM transaction_ledger WHERE deployment_id = ?",
                ("dep-roundtrip",),
            ).fetchone()
        finally:
            conn.close()

        assert row is not None, "the failed intent must still produce a ledger row"
        token_in, amount_in, token_out, amount_out, success = row
        assert token_in == "WBTC"
        assert token_out == "USDC"
        assert amount_in == "", f"persisted amount_in must be unmeasured-empty, got {amount_in!r}"
        assert amount_out == ""
        assert not success


class TestMeasuredZeroPreservation:
    """Empty != Zero in the intent-attr fallback (Phase 1 spec critique, round 2).

    The chain used ``or``-truthiness, which collapsed a measured
    ``Decimal("0")`` token amount into the unmeasured ``""`` sentinel — the
    same masking issue #1768 fixed on the swap_amounts path. The first
    NON-None link must win and a measured zero must persist as ``"0"``.
    """

    def test_measured_zero_amount_is_preserved_not_emptied(self):
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent(
            "SWAP",
            protocol="uniswap_v3",
            from_token="WBTC",
            to_token="USDC",
            amount=Decimal("0"),
        )
        entry = build_ledger_entry(
            deployment_id="s", cycle_id="c", intent=intent, result=result, success=False
        )
        assert entry.amount_in == "0", "measured zero must persist as '0', never collapse to ''"

    def test_unmeasured_amount_still_empty(self):
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd=None,
            extracted_data={},
        )
        intent = _make_intent("SWAP", protocol="uniswap_v3", from_token="WBTC", to_token="USDC")
        entry = build_ledger_entry(
            deployment_id="s", cycle_id="c", intent=intent, result=result, success=False
        )
        assert entry.amount_in == ""


# ---------------------------------------------------------------------------
# VIB-4483 (P-V1-B) — _stamp_lp_open_native_amounts branch coverage.
#
# A native-ETH V4 pool deposits its ETH leg via msg.value (no ERC-20 Transfer),
# so the receipt parser leaves that leg None. The runner reads the freshly-minted
# position state on-chain and derives (amount0, amount1) via the framework's
# concentrated-liquidity math; this stamp fills ONLY the unmeasured (None) native
# leg onto the frozen LPOpenData, never clobbering a measured ERC-20 leg. Empty !=
# Zero throughout: a failed/absent read leaves the leg None.
# ---------------------------------------------------------------------------

_NATIVE_ADDR = "0x0000000000000000000000000000000000000000"


def _v4_open_result_for_stamp(*, amount0=None, amount1=1_000_000_000, currency0=_NATIVE_ADDR, extracted="dict"):
    """Build a result whose extracted_data carries a V4-shaped (native) LPOpenData."""
    if extracted == "dict":
        open_data = LPOpenData(
            position_id=4242,
            tick_lower=-887220,
            tick_upper=887220,
            liquidity=10**15,
            amount0=amount0,
            amount1=amount1,
            current_tick=0,
            pool_address="0x" + "a" * 64,  # 32-byte V4 PoolId shape
            position_hash="0x" + "b" * 64,
            currency0=currency0,
            currency1="0x" + "2" * 40,
        )
        return SimpleNamespace(extracted_data={"lp_open_data": open_data})
    return SimpleNamespace(extracted_data=extracted)


class TestStampV4LpOpenNativeAmounts:
    """Direct branch coverage for ``_stamp_lp_open_native_amounts`` (VIB-4483)."""

    def test_native_leg_filled_when_unmeasured(self):
        # currency0 native, amount0 None (parser couldn't see the ETH Transfer),
        # amount1 measured. The stamp fills amount0 from the gateway-derived pair
        # and preserves the measured amount1.
        result = _v4_open_result_for_stamp(amount0=None, amount1=1_000_000_000)
        _stamp_lp_open_native_amounts(result, "LP_OPEN", (777_000, 9_999))
        open_data = result.extracted_data["lp_open_data"]
        assert open_data.amount0 == 777_000  # native leg filled
        assert open_data.amount1 == 1_000_000_000  # measured ERC-20 leg preserved (not clobbered)

    def test_measured_erc20_leg_not_clobbered(self):
        # The measured leg (amount1) must survive even though the derived pair
        # carries a different value for it.
        result = _v4_open_result_for_stamp(amount0=None, amount1=1_000_000_000)
        _stamp_lp_open_native_amounts(result, "LP_OPEN", (777_000, 42))
        open_data = result.extracted_data["lp_open_data"]
        assert open_data.amount1 == 1_000_000_000

    def test_none_amounts_pair_is_noop_short_circuit(self):
        # A failed / unavailable read => leave the native leg None (unmeasured),
        # never fabricate a zero.
        result = _v4_open_result_for_stamp(amount0=None, amount1=1_000_000_000)
        _stamp_lp_open_native_amounts(result, "LP_OPEN", None)
        assert result.extracted_data["lp_open_data"].amount0 is None

    def test_measured_zero_derived_is_honored(self):
        # A derived 0 (e.g. an out-of-range single-sided mint that put nothing on
        # the native leg) is a MEASURED zero and fills the None leg.
        result = _v4_open_result_for_stamp(amount0=None, amount1=1_000_000_000)
        _stamp_lp_open_native_amounts(result, "LP_OPEN", (0, 5))
        assert result.extracted_data["lp_open_data"].amount0 == 0

    def test_non_v4_shaped_open_is_noop(self):
        # currency0 None → not a V4-shaped LPOpenData (the capability signal).
        open_data = LPOpenData(
            position_id=4242,
            tick_lower=-887220,
            tick_upper=887220,
            liquidity=10**15,
            amount0=None,
            amount1=1_000_000_000,
            pool_address="0x" + "a" * 40,
            # currency0 / currency1 left None → not V4-shaped.
        )
        result = SimpleNamespace(extracted_data={"lp_open_data": open_data})
        _stamp_lp_open_native_amounts(result, "LP_OPEN", (1, 2))
        assert result.extracted_data["lp_open_data"].amount0 is None

    @pytest.mark.parametrize("intent_type", ["LP_CLOSE", "SWAP", "SUPPLY"])
    def test_non_open_intent_type_is_noop(self, intent_type):
        result = _v4_open_result_for_stamp(amount0=None, amount1=1_000_000_000)
        _stamp_lp_open_native_amounts(result, intent_type, (1, 2))
        assert result.extracted_data["lp_open_data"].amount0 is None

    def test_none_result_is_noop(self):
        _stamp_lp_open_native_amounts(None, "LP_OPEN", (1, 2))

    def test_non_dict_extracted_data_is_noop(self):
        result = _v4_open_result_for_stamp(extracted=["not", "a", "dict"])
        _stamp_lp_open_native_amounts(result, "LP_OPEN", (1, 2))
        assert result.extracted_data == ["not", "a", "dict"]


# ---------------------------------------------------------------------------
# VIB-5121 — _stamp_lp_close_native_amounts branch coverage. A FLUID/fungible
# native-ETH leg returned on close emits no ERC-20 Transfer, so the parser left
# amountN_collected None; the runner measures it from a balance bracket and this
# stamp fills ONLY the None leg whose currency is the non-V4 (ERC-7528) native
# sentinel, never clobbering a measured ERC-20 leg or a V4 leg.
# ---------------------------------------------------------------------------

_EEEE_ADDR = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"


def _close_result_for_stamp(*, a0=0, a1=None, currency0="0x" + "1" * 40, currency1=_EEEE_ADDR, extracted="dict"):
    if extracted == "dict":
        close_data = LPCloseData(
            amount0_collected=a0,
            amount1_collected=a1,
            pool_address="0x" + "a" * 40,
            currency0=currency0,
            currency1=currency1,
        )
        return SimpleNamespace(extracted_data={"lp_close_data": close_data})
    return SimpleNamespace(extracted_data=extracted)


class TestStampLpCloseNativeAmounts:
    """Direct branch coverage for ``_stamp_lp_close_native_amounts`` (VIB-5121)."""

    def test_native_leg_filled_when_unmeasured(self):
        result = _close_result_for_stamp(a0=683_000_000_000_000_000_000, a1=None)
        _stamp_lp_close_native_amounts(result, "LP_CLOSE", (None, 480_000_000_000_000_000))
        close = result.extracted_data["lp_close_data"]
        assert close.amount1_collected == 480_000_000_000_000_000  # native leg filled
        assert close.amount0_collected == 683_000_000_000_000_000_000  # measured ERC-20 preserved

    def test_measured_erc20_leg_not_clobbered(self):
        result = _close_result_for_stamp(a0=100, a1=None)
        _stamp_lp_close_native_amounts(result, "LP_CLOSE", (999, 5))
        # amount0 was measured (100) → preserved; amount1 was None → filled with 5.
        assert result.extracted_data["lp_close_data"].amount0_collected == 100
        assert result.extracted_data["lp_close_data"].amount1_collected == 5

    def test_none_amounts_pair_is_noop(self):
        result = _close_result_for_stamp(a0=100, a1=None)
        _stamp_lp_close_native_amounts(result, "LP_CLOSE", None)
        assert result.extracted_data["lp_close_data"].amount1_collected is None

    def test_measured_zero_derived_is_honored(self):
        result = _close_result_for_stamp(a0=100, a1=None)
        _stamp_lp_close_native_amounts(result, "LP_CLOSE", (None, 0))
        assert result.extracted_data["lp_close_data"].amount1_collected == 0

    def test_non_by_address_close_is_noop(self):
        close_data = LPCloseData(amount0_collected=100, amount1_collected=None, pool_address="0x" + "a" * 40)
        result = SimpleNamespace(extracted_data={"lp_close_data": close_data})
        _stamp_lp_close_native_amounts(result, "LP_CLOSE", (1, 2))
        assert result.extracted_data["lp_close_data"].amount1_collected is None

    @pytest.mark.parametrize("intent_type", ["LP_OPEN", "SWAP", "BORROW"])
    def test_non_close_intent_type_is_noop(self, intent_type):
        result = _close_result_for_stamp(a0=100, a1=None)
        _stamp_lp_close_native_amounts(result, intent_type, (1, 2))
        assert result.extracted_data["lp_close_data"].amount1_collected is None

    def test_lp_collect_fees_stamps_native_leg(self):
        # VIB-5121 (Codex P2) — LP_COLLECT_FEES carries LPCloseData and is a
        # close-like intent (parity with _stamp_lp_close_discriminator /
        # _stamp_v4_lp_close_fees). A native fee-collect leg the parser left
        # None must be filled by the runner-measured bracket, not discarded.
        result = _close_result_for_stamp(a0=100, a1=None)
        _stamp_lp_close_native_amounts(result, "LP_COLLECT_FEES", (None, 7))
        assert result.extracted_data["lp_close_data"].amount0_collected == 100  # ERC-20 preserved
        assert result.extracted_data["lp_close_data"].amount1_collected == 7  # native leg filled

    def test_none_result_is_noop(self):
        _stamp_lp_close_native_amounts(None, "LP_CLOSE", (1, 2))

    def test_non_dict_extracted_data_is_noop(self):
        result = _close_result_for_stamp(extracted=["x"])
        _stamp_lp_close_native_amounts(result, "LP_CLOSE", (1, 2))
        assert result.extracted_data == ["x"]


# ---------------------------------------------------------------------------
# VIB-5117 — _stamp_v4_lp_close_native_principal branch coverage.
#
# A native-ETH V4 pool returns its ETH leg to the wallet as raw ETH (TAKE_PAIR,
# no ERC-20 Transfer) on close, so the burn-receipt parser leaves that leg's
# amount{0,1}_collected None. The runner reads the PRE-burn position state on-
# chain and derives (amount0, amount1) via the framework's concentrated-liquidity
# math; this stamp fills ONLY the unmeasured (None) native leg onto the frozen
# LPCloseData, never clobbering a measured ERC-20 leg. Empty != Zero throughout:
# a failed/absent read leaves the leg None (never a fabricated zero). The exact
# close-side mirror of TestStampV4LpOpenNativeAmounts.
# ---------------------------------------------------------------------------


def _v4_close_result_for_principal(
    *, amount0_collected=None, amount1_collected=170_000, currency0=_NATIVE_ADDR, extracted="dict"
):
    """Build a result whose extracted_data carries a V4-shaped (native) LPCloseData."""
    if extracted == "dict":
        close = LPCloseData(
            amount0_collected=amount0_collected,
            amount1_collected=amount1_collected,
            fees0=None,
            fees1=None,
            liquidity_removed=1_000_000,
            pool_address="0x" + "a" * 64,  # 32-byte V4 PoolId shape
            source="modify_liquidity",
            currency0=currency0,
            currency1="0x" + "2" * 40,
        )
        return SimpleNamespace(extracted_data={"lp_close_data": close})
    return SimpleNamespace(extracted_data=extracted)


class TestStampV4LpCloseNativePrincipal:
    """Direct branch coverage for ``_stamp_v4_lp_close_native_principal`` (VIB-5117)."""

    def test_native_leg_filled_when_unmeasured(self):
        # currency0 native, amount0_collected None (parser couldn't see the ETH
        # withdrawal). The stamp fills amount0 from the gateway-derived pair and
        # preserves the measured ERC-20 amount1.
        result = _v4_close_result_for_principal(amount0_collected=None, amount1_collected=170_000)
        _stamp_v4_lp_close_native_principal(result, "LP_CLOSE", (888_000, 9_999))
        close = result.extracted_data["lp_close_data"]
        assert close.amount0_collected == 888_000  # native principal filled
        assert close.amount1_collected == 170_000  # measured ERC-20 leg preserved

    def test_measured_erc20_leg_not_clobbered(self):
        # The measured leg (amount1_collected) must survive even though the derived
        # pair carries a different value for it (Empty != Zero idempotence).
        result = _v4_close_result_for_principal(amount0_collected=None, amount1_collected=170_000)
        _stamp_v4_lp_close_native_principal(result, "LP_CLOSE", (888_000, 42))
        assert result.extracted_data["lp_close_data"].amount1_collected == 170_000

    def test_none_amounts_pair_is_noop_short_circuit(self):
        # A failed / unavailable read => leave the native leg None (unmeasured),
        # never fabricate a zero — the whole point of VIB-5117.
        result = _v4_close_result_for_principal(amount0_collected=None, amount1_collected=170_000)
        _stamp_v4_lp_close_native_principal(result, "LP_CLOSE", None)
        assert result.extracted_data["lp_close_data"].amount0_collected is None

    def test_none_read_leg_preserves_none(self):
        # A read that returns a None native leg (partial/failed derivation) leaves
        # the unmeasured leg None — never a fabricated zero.
        result = _v4_close_result_for_principal(amount0_collected=None, amount1_collected=170_000)
        _stamp_v4_lp_close_native_principal(result, "LP_CLOSE", (None, None))
        assert result.extracted_data["lp_close_data"].amount0_collected is None

    def test_measured_zero_derived_is_honored(self):
        # A derived 0 (e.g. an out-of-range single-sided close that withdrew
        # nothing on the native leg) is a MEASURED zero and fills the None leg.
        result = _v4_close_result_for_principal(amount0_collected=None, amount1_collected=170_000)
        _stamp_v4_lp_close_native_principal(result, "LP_CLOSE", (0, 5))
        assert result.extracted_data["lp_close_data"].amount0_collected == 0

    def test_measured_zero_erc20_leg_not_clobbered(self):
        # A genuinely-measured ERC-20 zero (amount1_collected=0, e.g. an out-of-
        # range ERC-20 leg) must be preserved — it is measured, not unmeasured.
        result = _v4_close_result_for_principal(amount0_collected=None, amount1_collected=0)
        _stamp_v4_lp_close_native_principal(result, "LP_CLOSE", (888_000, 9_999))
        close = result.extracted_data["lp_close_data"]
        assert close.amount0_collected == 888_000
        assert close.amount1_collected == 0  # measured zero never clobbered

    def test_non_native_currency_none_leg_not_filled(self):
        # Defense-in-depth (Codex/pr-auditor hardening, VIB-5117): a None leg whose
        # PoolKey currency is NOT the native sentinel must NOT receive a derived
        # value — an ERC-20 leg's true amount comes from its Transfer, never from
        # the pre-burn liquidity math. The parser never emits this shape today (it
        # leaves None only for the native leg); this guards a future regression.
        result = _v4_close_result_for_principal(
            amount0_collected=None, amount1_collected=170_000, currency0="0x" + "1" * 40
        )
        _stamp_v4_lp_close_native_principal(result, "LP_CLOSE", (888_000, 9_999))
        close = result.extracted_data["lp_close_data"]
        assert close.amount0_collected is None  # non-native None leg left unmeasured
        assert close.amount1_collected == 170_000

    def test_non_v4_shaped_close_is_noop(self):
        # currency0 None → not a V4-shaped LPCloseData (the capability signal).
        close = LPCloseData(
            amount0_collected=None,
            amount1_collected=170_000,
            fees0=None,
            fees1=None,
            liquidity_removed=1_000_000,
            pool_address="0x" + "a" * 40,
            # currency0 / currency1 left None → not V4-shaped.
        )
        result = SimpleNamespace(extracted_data={"lp_close_data": close})
        _stamp_v4_lp_close_native_principal(result, "LP_CLOSE", (1, 2))
        assert result.extracted_data["lp_close_data"].amount0_collected is None

    @pytest.mark.parametrize("intent_type", ["LP_OPEN", "SWAP", "SUPPLY", "LP_COLLECT_FEES"])
    def test_non_close_intent_type_is_noop(self, intent_type):
        # The principal stamp is scoped to LP_CLOSE only (a fees-only
        # LP_COLLECT_FEES withdraws no principal — measured zero, not a native fill).
        result = _v4_close_result_for_principal(amount0_collected=None, amount1_collected=170_000)
        _stamp_v4_lp_close_native_principal(result, intent_type, (1, 2))
        assert result.extracted_data["lp_close_data"].amount0_collected is None

    def test_none_result_is_noop(self):
        _stamp_v4_lp_close_native_principal(None, "LP_CLOSE", (1, 2))

    def test_non_dict_extracted_data_is_noop(self):
        result = _v4_close_result_for_principal(extracted=["not", "a", "dict"])
        _stamp_v4_lp_close_native_principal(result, "LP_CLOSE", (1, 2))
        assert result.extracted_data == ["not", "a", "dict"]
