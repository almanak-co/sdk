"""VIB-3890 — LP ledger formatter tests for the Trade Tape.

Three classes of invariant enforced here:

1. **LP-fallback dust scaling** — ``_format_lp_ledger_amount`` enables the
   new 8-dec branch via ``lp_fallback_context=True``. Raw integers in
   ``[1000, 999999]`` for 8-dec tokens (WBTC, cbBTC, LBTC) scale correctly.

2. **Legacy-branch symmetry** — for ``>=10**6`` raws and below-1000 inputs,
   ``_format_lp_ledger_amount`` and ``format_token_amount`` produce
   bit-identical output. (The legacy branch fires regardless of context.)

3. **Payload bypass (AC #4 / D3.F6 — Codex anchor)** — payload-sourced
   Decimals (`payload.amount0/amount1/fees0/fees1`) are already human and
   must NEVER pass through the helper. A `Decimal("2000000")` USDC payload
   must render as `"2,000,000.00"`, NOT mis-scaled to `"2.00"`. Verified
   both by integration through ``_format_human_amount`` AND by a static
   source-string check that ``_format_human_amount``'s body does not call
   ``_should_scale_raw_amount``.

Direct helper boundary tests live in
``tests/unit/dashboard/test_should_scale_raw_amount.py``; end-to-end render
tests live in ``tests/unit/dashboard/test_format_token_amount_vib3890.py``.
"""

from __future__ import annotations

import inspect
from decimal import Decimal

import pytest

from almanak.framework.dashboard.pages.trade_tape import (
    _format_human_amount,
    _format_lp_ledger_amount,
)
from almanak.framework.dashboard.utils import format_token_amount


# ──────────────────────────────────────────────────────────────────────────
# LP-fallback dust scaling — the new 8-dec branch fires here, NOT in
# format_token_amount.
# ──────────────────────────────────────────────────────────────────────────


def test_lp_ledger_wbtc_dust_scales():
    """The documented bug case: LP_OPEN row with raw 1346 WBTC.

    Renders as `0.00001346` via the lp_fallback_context=True path.
    """
    assert _format_lp_ledger_amount("1346", "WBTC", "arbitrum") == "0.00001346"
    assert _format_lp_ledger_amount("1790", "WBTC", "arbitrum") == "0.0000179"


def test_lp_ledger_cbbtc_dust_scales():
    assert _format_lp_ledger_amount("1346", "cbBTC", "base") == "0.00001346"
    assert _format_lp_ledger_amount("1346", "cbBTC", "ethereum") == "0.00001346"


def test_lp_ledger_lbtc_dust_scales():
    assert _format_lp_ledger_amount("1346", "LBTC", "ethereum") == "0.00001346"
    assert _format_lp_ledger_amount("1346", "LBTC", "base") == "0.00001346"


def test_lp_ledger_below_bracket_passes_through():
    """Below 1000: not scaled by either branch."""
    assert _format_lp_ledger_amount("100", "WBTC", "arbitrum") == "100.00"
    assert _format_lp_ledger_amount("999", "WBTC", "arbitrum") == "999.00"


def test_lp_ledger_legacy_branch_still_scales():
    """`>=10**6` continues to scale (legacy branch fires in both contexts).

    Note: Decimal preserves the input's exact representation; ``.4g``
    strips trailing zeros on clean decimals (1_000_000 / 1e8 = 0.01 exact).
    """
    assert _format_lp_ledger_amount("1000000", "WBTC", "arbitrum") == "0.01"
    # 18-dec WETH large raw — same legacy path as format_token_amount.
    assert _format_lp_ledger_amount("891556839636852", "WETH", "arbitrum") == "0.0008916"


# ──────────────────────────────────────────────────────────────────────────
# Legacy-branch symmetry between the two formatters
# ──────────────────────────────────────────────────────────────────────────


# Cases where both formatters MUST produce bit-identical output. The new
# 8-dec dust branch is asymmetric by design (only fires in
# _format_lp_ledger_amount), so 1000-999999 8-dec raws are deliberately
# NOT in this symmetry table.
_LEGACY_SYMMETRY_ROWS = [
    pytest.param("1000000", "WBTC", "arbitrum", id="wbtc-legacy-threshold"),
    pytest.param("10000000", "WBTC", "arbitrum", id="wbtc-10M-raw"),
    pytest.param("100", "WBTC", "arbitrum", id="wbtc-below-bracket-100"),
    pytest.param("999", "WBTC", "arbitrum", id="wbtc-below-bracket-999"),
    pytest.param("891556839636852", "WETH", "arbitrum", id="weth-large-raw-18dec"),
    pytest.param("5000000", "USDC", "arbitrum", id="usdc-5-raw-6dec"),
    pytest.param("0", "WBTC", "arbitrum", id="zero"),
    pytest.param("0.0001", "WETH", "arbitrum", id="weth-sub-1"),
    pytest.param("1234.567", "USDC", "arbitrum", id="usdc-human-decimal"),
    pytest.param("5000", "WETH", "arbitrum", id="weth-below-legacy"),
]


@pytest.mark.parametrize("amount,symbol,chain", _LEGACY_SYMMETRY_ROWS)
def test_legacy_branch_symmetry(amount, symbol, chain):
    """For inputs that do NOT trip the new 8-dec dust branch, both formatters
    produce bit-identical output (legacy branch fires in both, or neither).

    The new branch is asymmetric by design — only the LP-fallback caller
    enables it. See ``test_should_scale_raw_amount.py`` for that path.
    """
    a = format_token_amount(amount, symbol, chain)
    b = _format_lp_ledger_amount(amount, symbol, chain)
    assert a == b, (
        f"legacy-branch symmetry break on ({amount!r}, {symbol!r}, {chain!r}): "
        f"format_token_amount = {a!r}, _format_lp_ledger_amount = {b!r}"
    )


def test_new_branch_asymmetry_is_intentional():
    """8-dec dust 1346 WBTC: deliberately asymmetric across the two formatters.

    SWAP rows store human Decimals (5000 = 5,000.00); LP-fallback rows can
    store raw integers (5000 = 0.00005 if scaling fires). The new branch
    fires only in the latter context to prevent mis-scaling integer-valued
    SWAP amounts (Codex + Claude pr-auditor convergent finding on PR #2371).
    """
    swap_render = format_token_amount("1346", "WBTC", "arbitrum")
    lp_render = _format_lp_ledger_amount("1346", "WBTC", "arbitrum")
    assert swap_render == "1,346.00", swap_render
    assert lp_render == "0.00001346", lp_render
    assert swap_render != lp_render  # by design


def test_symmetry_on_empty_input():
    """Both formatters return ``"—"`` on empty/None input."""
    for val in (None, "", "—"):
        assert format_token_amount(val, "WBTC", "arbitrum") == "—"
        assert _format_lp_ledger_amount(val, "WBTC", "arbitrum") == "—"


# ──────────────────────────────────────────────────────────────────────────
# AC #4 / D3.F6 — payload-amount Decimals MUST bypass the helper
# ──────────────────────────────────────────────────────────────────────────


def test_payload_amount_never_scaled():
    """AC #4 / D3.F6 (Codex anchor) — already-human payload Decimals are not mis-scaled.

    Accounting payload fields like ``payload.amount0/amount1/fees0/fees1`` are
    stamped as already-decoded human Decimals at execution block. Routing one
    through the raw-integer heuristic would understate the headline by
    ``10**decimals``. The trade-tape renders these via ``_format_human_amount``
    (which never calls the helper), so a million-USDC LP leg renders as
    ``"2,000,000.00"``, not ``"2.00"``.
    """
    rendered = _format_human_amount(Decimal("2000000"))
    assert rendered == "2,000,000.00", rendered


def test_payload_one_million_usdc_via_human_amount():
    """Companion to AC #4 — payload-sourced 1 USDC renders as 1.00 (not 1e-6)."""
    assert _format_human_amount(Decimal("1")) == "1.00"
    # Whole-million payload value — must render verbatim.
    assert _format_human_amount(Decimal("1000000")) == "1,000,000.00"
    # Fractional payload value (sub-1 → 4 sig figs).
    assert _format_human_amount(Decimal("0.000868768309352546")) == "0.0008688"


def test_format_human_amount_does_not_call_helper_static_check():
    """AC #8 — static-source check that ``_format_human_amount`` does NOT
    reference ``_should_scale_raw_amount``.

    Routing a payload Decimal through the helper would be a silent-error bug
    (the payload is already human; helper would understate by 10**decimals).
    The two functions must stay structurally isolated. This test reads the
    source body and asserts the helper is not referenced — catches a
    well-intentioned future refactor that "de-duplicates" the formatters.
    """
    src = inspect.getsource(_format_human_amount)
    assert "_should_scale_raw_amount" not in src, (
        "_format_human_amount must NOT call _should_scale_raw_amount — payload "
        "Decimals are already human values. Routing them through the helper "
        "would understate the headline by 10**decimals."
    )
    assert "_try_token_decimals" not in src, (
        "_format_human_amount must NOT look up token decimals — payload values "
        "are already decoded."
    )


def test_payload_zero_renders_as_zero():
    """Payload-sourced zero must render as '0', not '—' (Empty ≠ Zero, per CLAUDE.md)."""
    assert _format_human_amount(Decimal("0")) == "0"


def test_payload_empty_renders_as_dash():
    """Empty/None payload renders as '—' (unmeasured)."""
    assert _format_human_amount(None) == "—"
    assert _format_human_amount("") == "—"
