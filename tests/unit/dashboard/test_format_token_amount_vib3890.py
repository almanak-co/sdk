"""VIB-3890 — Quant-readable token amount formatter for the trade tape.

Pre-VIB-3890 the trade-tape headline rendered:
- LP_OPEN: ``891556839636852 WETH → 2294332 USDC`` (raw 18-dec / 6-dec
  integers — unreadable).
- SWAP:    ``2 USDC → 0.000868768309352546 WETH`` (mixed precision —
  hard to scan).

The formatter normalises both to a Quant-readable form while
preserving raw integers in the receipt-parsed expander (audit trail).
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.dashboard.utils import format_token_amount


# ──────────────────────────────────────────────────────────────────────────
# Numerical formatting: ≥1 → 2dp, <1 → 4 sig figs
# ──────────────────────────────────────────────────────────────────────────


def test_format_renders_two_decimals_for_values_ge_1():
    assert format_token_amount("2.0") == "2.00"
    assert format_token_amount("1234.567") == "1,234.57"


def test_format_renders_four_sigfigs_for_sub_1_values():
    assert format_token_amount("0.000868768309352546") == "0.0008688"
    assert format_token_amount("0.5") == "0.5"


def test_format_zero():
    assert format_token_amount("0") == "0"


def test_format_handles_decimal_input():
    assert format_token_amount(Decimal("1.5")) == "1.50"


def test_format_handles_float_input():
    assert format_token_amount(2.5) == "2.50"


def test_format_handles_int_input():
    """A plain integer with no symbol/chain context is rendered as-is."""
    assert format_token_amount(42) == "42.00"


def test_format_passes_through_empty():
    assert format_token_amount("") == "—"
    assert format_token_amount(None) == "—"


def test_format_passes_through_unparseable():
    assert format_token_amount("not-a-number") == "not-a-number"


def test_format_thousands_separator_for_large_human_amounts():
    assert format_token_amount("12345.678") == "12,345.68"


# ──────────────────────────────────────────────────────────────────────────
# Raw-integer scaling — heuristic only fires when symbol+chain provided
# ──────────────────────────────────────────────────────────────────────────


def test_raw_integer_without_symbol_does_not_scale():
    """Without symbol/chain context the formatter cannot know decimals;
    it must NOT guess. Renders the raw integer as-is."""
    out = format_token_amount("891556839636852")
    # Big integer, formatted with thousands sep + .00 (legitimate
    # human-readable for ≥1 values).
    assert "," in out  # thousand separators
    assert out.startswith("891")


def test_format_safely_falls_back_when_resolver_unavailable():
    """If the token resolver can't find decimals, the function still
    returns *something* — never raises."""
    # Garbage symbol/chain → resolver returns None → no scaling.
    out = format_token_amount("891556839636852", symbol="XYZ_UNKNOWN", chain="madeup")
    assert out  # non-empty
    assert "891" in out  # value appears verbatim (modulo separators)


def test_sub_decimal_value_with_unknown_token_kept_intact():
    """A small decimal value still rounds-correctly even without a
    decimals lookup — the heuristic only scales LARGE integers."""
    out = format_token_amount("0.000868768309352546", symbol="WETH", chain="arbitrum")
    # Should NOT have scaled — stays sub-1.
    assert "0.000" in out  # somewhere


def test_format_handles_negative_numbers():
    """Negative balance deltas (post-LP-CLOSE, e.g.) render as expected."""
    assert format_token_amount("-12.5") == "-12.50"


def test_format_handles_scientific_notation_input():
    """Decimal accepts scientific notation; output still readable."""
    assert format_token_amount("1.5e2") == "150.00"


def test_format_very_small_uses_scientific():
    """Sub-1e-4 → scientific to avoid 0.0000000869 mess."""
    out = format_token_amount("0.0000000868")
    assert "e-" in out.lower()


# ──────────────────────────────────────────────────────────────────────────
# VIB-3890 acceptance criteria — end-to-end render tests (D1.S2..S4)
#
# The helper-level boundary matrix lives in
# tests/unit/dashboard/test_should_scale_raw_amount.py. These tests verify
# the rendered string at the format_token_amount boundary (= what the
# Trade Tape headline actually shows).
# ──────────────────────────────────────────────────────────────────────────


def test_wbtc_dust_renders_as_human_in_default_context():
    """Post-audit (PR #2371): in default (SWAP/SUPPLY/etc.) context, WBTC raw
    integers in [1000, 999999] render as human integers — NOT scaled.

    SWAP rows store human Decimals via ``SwapAmounts.amount_*_decimal``; firing
    the new 8-dec branch on them would mis-scale `5000 CRO` to `0.00005 CRO`.
    The new branch is reserved for ``_format_lp_ledger_amount``
    (lp_fallback_context=True); coverage there lives in
    ``test_should_scale_raw_amount.py`` and ``test_trade_tape_lp_ledger.py``.
    """
    # Raw 1346 as a SWAP amount = "1,346.00" human integer.
    assert format_token_amount("1346", "WBTC", "arbitrum") == "1,346.00"
    assert format_token_amount("1790", "WBTC", "arbitrum") == "1,790.00"


def test_wbtc_human_integers_preserved():
    """AC #2 / D1.S3 — human integer WBTC sizes pass through unchanged in
    default (SWAP) context. The audit-found case `1000 WBTC` ≈ $60M whale
    trade must render as `1,000.00`, never mis-scaled to dust.
    """
    assert format_token_amount("100", "WBTC", "arbitrum") == "100.00"
    assert format_token_amount("999", "WBTC", "arbitrum") == "999.00"
    assert format_token_amount("1000", "WBTC", "arbitrum") == "1,000.00"
    assert format_token_amount("5000", "WBTC", "arbitrum") == "5,000.00"


def test_legacy_18dec_weth_still_scales():
    """AC #5 / D1.S4 — the legacy `>= 10⁶` branch still works on 18-dec WETH.

    Raw 891556839636852 = 0.000891556839636852 WETH; .4g render → 0.0008916.
    """
    assert format_token_amount("891556839636852", "WETH", "arbitrum") == "0.0008916"


def test_six_dec_usdc_raw_scales():
    """AC #7 — 6-dec USDC raw integers >= 10⁶ continue to scale (legacy branch)."""
    assert format_token_amount("5000000", "USDC", "arbitrum") == "5.00"


def test_six_dec_usdc_below_legacy_threshold_unchanged():
    """AC #7 — 6-dec USDC raw integers below 10⁶ pass through unchanged (no new branch for dec=6)."""
    # 999_999 is below 10⁶; the new 8-dec branch is decimals==8 only, so USDC stays human.
    assert format_token_amount("999999", "USDC", "arbitrum") == "999,999.00"


def test_eight_dec_bracket_in_default_context_not_scaled():
    """Bracket edges (1000, 999999) in default (SWAP-row) context: human integer.

    These are exercised via the LP-fallback path in
    ``test_trade_tape_lp_ledger.py``; here we verify the default context
    intentionally does NOT scale (audit-found risk guard).
    """
    assert format_token_amount("1000", "WBTC", "arbitrum") == "1,000.00"
    assert format_token_amount("999999", "WBTC", "arbitrum") == "999,999.00"


def test_tbtc_dust_falls_to_human_integer():
    """tBTC is 18-dec → new 8-dec branch wouldn't apply anyway, AND default
    context wouldn't enable it. Dust renders as human integer.

    The legacy branch only fires above 10⁶.
    """
    assert format_token_amount("1346", "tBTC", "ethereum") == "1,346.00"


def test_tbtc_large_raw_scales_via_legacy_branch():
    """tBTC 18-dec raw of 1 whole token (10**18) → legacy branch scales to 1.00.

    The legacy branch fires regardless of context — preserves PR #2290.
    """
    one_tbtc_raw = str(10**18)  # = "1000000000000000000"
    assert format_token_amount(one_tbtc_raw, "tBTC", "ethereum") == "1.00"


def test_cbbtc_swap_context_renders_human():
    """cbBTC dust on a SWAP row renders as the human integer (default context
    has no new branch). LP-fallback path coverage is in
    ``test_trade_tape_lp_ledger.py``.
    """
    assert format_token_amount("1346", "cbBTC", "base") == "1,346.00"
    assert format_token_amount("1346", "cbBTC", "ethereum") == "1,346.00"
    assert format_token_amount("1346", "cbBTC", "arbitrum") == "1,346.00"


def test_lbtc_swap_context_renders_human():
    """LBTC dust on a SWAP row renders as the human integer."""
    assert format_token_amount("1346", "LBTC", "ethereum") == "1,346.00"
    assert format_token_amount("1346", "LBTC", "base") == "1,346.00"
