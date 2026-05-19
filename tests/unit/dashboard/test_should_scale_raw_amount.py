"""VIB-3890 — Direct boundary tests for the ``_should_scale_raw_amount`` helper.

The helper is the single chokepoint shared by ``format_token_amount``
(utils.py) and ``_format_lp_ledger_amount`` (trade_tape.py). It returns
the token's decimals when a Decimal should be interpreted as a raw on-chain
integer and scaled down by ``10**decimals``; otherwise ``None`` (degrade
safe). Two branches:

1. Legacy ``abs(d) >= 10**6`` for ANY decimals (preserves PR #2290 behaviour
   on 18-dec WETH, 6-dec USDC raw integers, etc.).
2. New 8-dec dust bracket ``decimals == 8 AND 1000 <= abs(d) < 10**6``
   (catches the WBTC residual: raw 1346 = 0.00001346 WBTC).

End-to-end format/render tests live in
``tests/unit/dashboard/test_format_token_amount_vib3890.py``; symmetry +
payload-bypass tests live in
``tests/unit/dashboard/test_trade_tape_lp_ledger.py``.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.dashboard.utils import (
    _should_scale_raw_amount,
    format_token_amount,
)


# ──────────────────────────────────────────────────────────────────────────
# D1 happy path: WBTC dust + AC #1
# ──────────────────────────────────────────────────────────────────────────


def test_helper_returns_8_for_wbtc_1346():
    """D1.S1 — the documented WBTC dust bug case. Raw 1346 = 0.00001346 WBTC.

    The new 8-dec dust branch requires ``lp_fallback_context=True`` (per PR
    #2371 audit — SWAP / SUPPLY / etc. rows store human Decimals and must
    not fire this branch). LP-fallback rows are the legitimate caller.
    """
    assert _should_scale_raw_amount(Decimal("1346"), "WBTC", "arbitrum", lp_fallback_context=True) == 8


def test_helper_returns_8_for_wbtc_1790():
    """D1.S1 — second documented bug case (LP-fallback context)."""
    assert _should_scale_raw_amount(Decimal("1790"), "WBTC", "arbitrum", lp_fallback_context=True) == 8


def test_helper_default_does_not_fire_new_branch_on_wbtc_dust():
    """SWAP / SUPPLY / etc. row context (default, no lp_fallback flag) must
    NOT fire the new 8-dec branch on WBTC dust — those rows store human
    Decimals via SwapAmounts.amount_*_decimal; firing the new branch would
    mis-scale integer-valued amounts. The legacy >=10**6 branch still fires
    on raws in that bracket.
    """
    assert _should_scale_raw_amount(Decimal("1346"), "WBTC", "arbitrum") is None
    assert _should_scale_raw_amount(Decimal("1790"), "WBTC", "arbitrum") is None
    # Legacy branch is unchanged — still fires for >=10**6.
    assert _should_scale_raw_amount(Decimal("10000000"), "WBTC", "arbitrum") == 8


# ──────────────────────────────────────────────────────────────────────────
# D2 matrix (parametrized)
# ──────────────────────────────────────────────────────────────────────────


# (input Decimal, symbol, chain, expected helper return)
# Mirrors the D2 matrix in docs/internal/uat-cards/VIB-3890.md.
_D2_MATRIX = [
    # M1: dust 1346 → fires new 8-dec branch
    pytest.param(Decimal("1346"), "WBTC", "arbitrum", 8, id="M1-wbtc-dust-1346"),
    # M2: bracket upper edge (999999) → fires new branch
    pytest.param(Decimal("999999"), "WBTC", "arbitrum", 8, id="M2-wbtc-bracket-upper"),
    # M3: crosses to legacy threshold (1_000_000) → fires legacy branch
    pytest.param(Decimal("1000000"), "WBTC", "arbitrum", 8, id="M3-wbtc-legacy-crossing"),
    # M4: 100 below bracket → human integer, NOT scaled
    pytest.param(Decimal("100"), "WBTC", "arbitrum", None, id="M4-wbtc-human-integer"),
    # M5-M7: cbBTC inventory across three chains
    pytest.param(Decimal("1346"), "cbBTC", "base", 8, id="M5-cbbtc-base"),
    pytest.param(Decimal("1346"), "cbBTC", "ethereum", 8, id="M6-cbbtc-ethereum"),
    pytest.param(Decimal("1346"), "cbBTC", "arbitrum", 8, id="M7-cbbtc-arbitrum"),
    # M8: LBTC inventory
    pytest.param(Decimal("1346"), "LBTC", "ethereum", 8, id="M8-lbtc-ethereum"),
    # M9: tBTC is 18-dec — new branch is 8-dec-only, helper returns None
    pytest.param(Decimal("1346"), "tBTC", "ethereum", None, id="M9-tbtc-dust-no-branch"),
    # M10: USDC raw 5_000_000 (= 5 USDC) → legacy branch fires for any dec >= 1
    pytest.param(Decimal("5000000"), "USDC", "arbitrum", 6, id="M10-usdc-legacy"),
    # M11: 18-dec WETH large raw → legacy branch
    pytest.param(Decimal("891556839636852"), "WETH", "arbitrum", 18, id="M11-weth-legacy"),
    # M12: 18-dec below 10⁶ → human integer, NOT scaled
    pytest.param(Decimal("5000"), "WETH", "arbitrum", None, id="M12-weth-below-legacy"),
    # Edge: bracket lower edge inclusive (1000) → new branch fires
    pytest.param(Decimal("1000"), "WBTC", "arbitrum", 8, id="edge-wbtc-bracket-lower"),
    # Edge: just below bracket (999) → human integer, NOT scaled
    pytest.param(Decimal("999"), "WBTC", "arbitrum", None, id="edge-wbtc-below-bracket"),
    # Edge: large tBTC raw (10**18, well above legacy threshold) → legacy fires with dec=18
    pytest.param(Decimal("10") ** 18, "tBTC", "ethereum", 18, id="edge-tbtc-large-legacy"),
]


@pytest.mark.parametrize("d,symbol,chain,expected", _D2_MATRIX)
def test_should_scale_raw_amount_matrix_lp_fallback(d, symbol, chain, expected):
    """D2.M1..M12 + edge cases — parametrized helper boundary matrix.

    Runs with ``lp_fallback_context=True`` to exercise the new 8-dec dust
    branch. SWAP-row callers (without the flag) only ever see the legacy
    branch; that path is covered by
    ``test_should_scale_raw_amount_matrix_swap_context`` below.
    """
    assert _should_scale_raw_amount(d, symbol, chain, lp_fallback_context=True) == expected


# Same input matrix, but exercised from SWAP / SUPPLY / etc. context
# (lp_fallback_context=False). Below-10**6 8-dec rows MUST return None to
# prevent mis-scaling of integer-valued human Decimals stored via
# SwapAmounts.amount_*_decimal (audit finding: 5000 CRO must NOT render
# as 0.00005 CRO).
_D2_MATRIX_SWAP = [
    # 8-dec dust IS NOT scaled in default context (the audit-found risk).
    pytest.param(Decimal("1346"), "WBTC", "arbitrum", None, id="swap-wbtc-dust-not-scaled"),
    pytest.param(Decimal("1790"), "WBTC", "arbitrum", None, id="swap-wbtc-1790-not-scaled"),
    pytest.param(Decimal("999999"), "WBTC", "arbitrum", None, id="swap-wbtc-bracket-upper-not-scaled"),
    pytest.param(Decimal("5000"), "CRO", "ethereum", None, id="swap-cro-5000-not-scaled"),
    pytest.param(Decimal("1234"), "GALA", "ethereum", None, id="swap-gala-not-scaled"),
    # 8-dec dust below 1000 — never fires either branch.
    pytest.param(Decimal("100"), "WBTC", "arbitrum", None, id="swap-wbtc-below-bracket"),
    # Legacy branch still fires for >= 10**6 regardless of context.
    pytest.param(Decimal("1000000"), "WBTC", "arbitrum", 8, id="swap-wbtc-legacy-crossing"),
    pytest.param(Decimal("5000000"), "USDC", "arbitrum", 6, id="swap-usdc-legacy"),
    pytest.param(Decimal("891556839636852"), "WETH", "arbitrum", 18, id="swap-weth-legacy"),
    pytest.param(Decimal("5000"), "WETH", "arbitrum", None, id="swap-weth-below-legacy"),
]


@pytest.mark.parametrize("d,symbol,chain,expected", _D2_MATRIX_SWAP)
def test_should_scale_raw_amount_matrix_swap_context(d, symbol, chain, expected):
    """Default context (lp_fallback_context=False): only legacy branch fires."""
    assert _should_scale_raw_amount(d, symbol, chain) == expected


# ──────────────────────────────────────────────────────────────────────────
# D3 — silent-failure guards
# ──────────────────────────────────────────────────────────────────────────


def test_unknown_symbol_returns_none():
    """D3.F1 — resolver returns None for an unknown symbol → helper returns None.

    The format function must render the input as a human integer
    (`1,346.00`), NOT mis-scale to `0.00001346` — operator trust depends on
    never silently mis-scaling on uncertain input. Asserted under both
    contexts (the resolver-miss short-circuit fires before context check).
    """
    assert _should_scale_raw_amount(Decimal("1346"), "FAKE_UNKNOWN", "arbitrum") is None
    assert _should_scale_raw_amount(Decimal("1346"), "FAKE_UNKNOWN", "arbitrum", lp_fallback_context=True) is None
    rendered = format_token_amount("1346", "FAKE_UNKNOWN", "arbitrum")
    assert "1,346" in rendered, rendered
    assert "0.0000" not in rendered, rendered  # no mis-scaling


def test_resolver_exception_returns_none():
    """D3.F2 — resolver raises → helper still returns None (does NOT propagate).

    The contract is "degrade safe" — never fail the dashboard render because
    the resolver hiccups. Asserted under both contexts.
    """

    def _raise(*_args, **_kwargs):
        raise RuntimeError("simulated resolver outage")

    with patch(
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        side_effect=_raise,
    ):
        # The helper should swallow via the _try_token_decimals try/except.
        assert _should_scale_raw_amount(Decimal("1346"), "WBTC", "arbitrum") is None
        assert _should_scale_raw_amount(Decimal("1346"), "WBTC", "arbitrum", lp_fallback_context=True) is None
        # format_token_amount must also degrade-safe (no exception, no scaling).
        rendered = format_token_amount("1346", "WBTC", "arbitrum")
        assert "1,346" in rendered


def test_non_integral_rejected():
    """D3.F3 — non-integral input never enters either branch.

    A human Decimal like `0.5` must NEVER be mis-scaled by 10**decimals.
    """
    assert _should_scale_raw_amount(Decimal("0.5"), "WETH", "arbitrum") is None
    assert _should_scale_raw_amount(Decimal("0.5"), "WETH", "arbitrum", lp_fallback_context=True) is None
    rendered = format_token_amount(Decimal("0.5"), "WETH", "arbitrum")
    assert rendered == "0.5"


def test_missing_symbol_or_chain_returns_none():
    """D3.F4 — empty symbol or chain → helper returns None (no decimals lookup attempted)."""
    for kwargs in ({}, {"lp_fallback_context": True}):
        assert _should_scale_raw_amount(Decimal("1346"), "", "arbitrum", **kwargs) is None
        assert _should_scale_raw_amount(Decimal("1346"), "WBTC", "", **kwargs) is None
        assert _should_scale_raw_amount(Decimal("1346"), "", "", **kwargs) is None


def test_non_finite_rejected():
    """Helper rejects NaN / Infinity (degrade safe), under both contexts."""
    for kwargs in ({}, {"lp_fallback_context": True}):
        assert _should_scale_raw_amount(Decimal("NaN"), "WBTC", "arbitrum", **kwargs) is None
        assert _should_scale_raw_amount(Decimal("Infinity"), "WBTC", "arbitrum", **kwargs) is None
        assert _should_scale_raw_amount(Decimal("-Infinity"), "WBTC", "arbitrum", **kwargs) is None


def test_zero_decimals_rejected():
    """If a token resolves with decimals <= 0 (corrupt registry), no scaling.

    This guards against accidental zero-decimals registry entries; the helper
    must not divide by 10**0 = 1 (a no-op that still claims "scaled").
    """
    with patch(
        "almanak.framework.dashboard.utils._try_token_decimals",
        return_value=0,
    ):
        assert _should_scale_raw_amount(Decimal("1346"), "WBTC", "arbitrum") is None
        assert _should_scale_raw_amount(Decimal("1346"), "WBTC", "arbitrum", lp_fallback_context=True) is None


def test_swap_row_eight_dec_human_integer_not_misscaled():
    """Audit-found regression guard (PR #2371 — Codex + Claude pr-auditor
    convergent finding): SWAP / SUPPLY / etc. rows store human Decimals via
    SwapAmounts.amount_*_decimal. An integer-valued amount like `5000 CRO`
    on a SWAP row must render verbatim (5,000.00), NOT be mis-scaled to
    0.00005 by the new 8-dec branch.

    The new branch is gated on lp_fallback_context=True; format_token_amount
    (the SWAP-lane caller) does NOT pass this flag.
    """
    # Audit-cited examples: 70 8-dec tokens in the registry; only 4 are BTC
    # wrappers. Non-BTC 8-dec swap amounts must render as the human integer.
    for sym in ("CRO", "GALA", "ICP"):
        rendered = format_token_amount("5000", sym, "ethereum")
        assert rendered == "5,000.00", f"{sym} 5000 mis-scaled: {rendered}"
    # WBTC on SWAP context: also human integer (1000 WBTC = $60M whale trade
    # — must render as `1,000.00`, NOT `0.00001 WBTC`).
    assert format_token_amount("1000", "WBTC", "arbitrum") == "1,000.00"
    assert format_token_amount("1346", "WBTC", "arbitrum") == "1,346.00"
    # Legacy branch unchanged: >=10**6 still scales on SWAP context (catches
    # raw integers that DID leak through SwapAmounts somehow — preserves
    # PR #2290 behaviour). Decimal preserves exact representation; .4g
    # strips trailing zeros for clean decimals (10000000 / 1e8 = 0.1 exact).
    assert format_token_amount("10000000", "WBTC", "arbitrum") == "0.1"


# ──────────────────────────────────────────────────────────────────────────
# D3.F7 — BTC wrapper inventory merge gate
# ──────────────────────────────────────────────────────────────────────────


# (symbol, chain, expected_decimals, new_branch_eligible)
# Missing registry entries cause the resolver to return None, the helper to
# return None, and the assertion to fail. tBTC is added at 18-dec for general
# resolver coverage but the new branch is 8-dec-only.
_BTC_WRAPPER_INVENTORY = [
    pytest.param("WBTC", "arbitrum", 8, True, id="wbtc-arbitrum"),
    pytest.param("WBTC", "ethereum", 8, True, id="wbtc-ethereum"),
    pytest.param("cbBTC", "ethereum", 8, True, id="cbbtc-ethereum"),
    pytest.param("cbBTC", "base", 8, True, id="cbbtc-base"),
    pytest.param("cbBTC", "arbitrum", 8, True, id="cbbtc-arbitrum"),
    pytest.param("LBTC", "ethereum", 8, True, id="lbtc-ethereum"),
    pytest.param("LBTC", "base", 8, True, id="lbtc-base"),
    pytest.param("tBTC", "ethereum", 18, False, id="tbtc-ethereum"),
    pytest.param("tBTC", "arbitrum", 18, False, id="tbtc-arbitrum"),
    pytest.param("tBTC", "base", 18, False, id="tbtc-base"),
]


@pytest.mark.parametrize("symbol,chain,expected_dec,new_branch", _BTC_WRAPPER_INVENTORY)
def test_btc_wrapper_inventory(symbol, chain, expected_dec, new_branch):
    """D3.F7 — registry must know every in-scope BTC wrapper.

    Merge gate: cbBTC and LBTC must be registered at decimals=8 on the
    listed chains; tBTC must be registered at decimals=18 on its listed
    chains. Missing entries silently demote the helper to a no-op for that
    token — verified by injecting a dust amount and asserting branch
    eligibility.
    """
    from almanak.framework.data.tokens.resolver import get_token_resolver

    resolver = get_token_resolver()
    info = resolver.resolve(symbol, chain=chain)
    assert info is not None, f"registry missing {symbol} on {chain}"
    assert info.decimals == expected_dec, (
        f"{symbol}@{chain}: expected decimals={expected_dec}, got {info.decimals}"
    )

    # Dust amount in the new 8-dec branch range — requires lp_fallback_context=True.
    helper_result = _should_scale_raw_amount(
        Decimal("1346"), symbol, chain, lp_fallback_context=True
    )
    if new_branch:
        assert helper_result == 8, (
            f"new-branch-eligible {symbol}@{chain} should return 8 on dust 1346 "
            f"with lp_fallback_context=True, got {helper_result}"
        )
    else:
        assert helper_result is None, (
            f"non-new-branch {symbol}@{chain} should return None on dust 1346 "
            f"(new branch is 8-dec-only; this token is {expected_dec}-dec), got {helper_result}"
        )

    # In default (SWAP-row) context the new branch never fires regardless
    # of the symbol — this is the audit-found risk guard.
    default_result = _should_scale_raw_amount(Decimal("1346"), symbol, chain)
    assert default_result is None, (
        f"default context {symbol}@{chain} should return None on dust 1346 "
        f"(only lp_fallback_context enables the new 8-dec branch), got {default_result}"
    )

    # Large raw amount in the legacy branch range (always fires for any decimals,
    # regardless of lp_fallback_context).
    legacy_amount = Decimal("10") ** expected_dec  # 1 whole token raw
    legacy_result = _should_scale_raw_amount(legacy_amount, symbol, chain)
    assert legacy_result == expected_dec, (
        f"{symbol}@{chain} legacy branch should return {expected_dec} on raw 10**{expected_dec}, "
        f"got {legacy_result}"
    )


# ──────────────────────────────────────────────────────────────────────────
# Negative / sign symmetry
# ──────────────────────────────────────────────────────────────────────────


def test_negative_raw_integer_scales():
    """Negative balance deltas (post-LP-CLOSE etc.) follow the same rules.

    abs() is the predicate, so the bracket holds for both signs.
    """
    # New 8-dec branch requires lp_fallback_context.
    assert _should_scale_raw_amount(
        Decimal("-1346"), "WBTC", "arbitrum", lp_fallback_context=True
    ) == 8
    # Legacy branch fires regardless of context.
    assert _should_scale_raw_amount(Decimal("-891556839636852"), "WETH", "arbitrum") == 18
