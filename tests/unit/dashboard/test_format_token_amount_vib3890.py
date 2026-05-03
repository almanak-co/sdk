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
