"""Tests for ``_amount_in_usd`` decimals-aware fix (VIB-4778 / W1-3).

History:

- The original W1-3 interim shipped a SWAP-only scoping: ``_amount_in_usd``
  returned ``None`` for any non-SWAP row to avoid the raw-wei inflation bug
  (``$1,585,552`` instead of ``$1.585552`` for LP_OPEN's USDC leg).
- This file covers the **scalable** follow-up: ``_amount_in_usd`` resolves
  decimals via ``get_token_resolver()`` and converts raw-wei → human → USD
  for LP_OPEN / LP_CLOSE / LP_COLLECT_FEES rows as well as SWAP rows.

Coverage:

- SWAP / LP_OPEN / LP_CLOSE all resolve correctly across multiple connectors
  and chains (Uniswap V3 Arbitrum, Aerodrome Slipstream Base, PancakeSwap V3
  Arbitrum).
- Raw-wei LP_OPEN rows do NOT inflate ``avg_trade_size_usd``.
- Empty != Zero discipline: ``None`` / ``""`` → ``None``; ``"0"`` →
  ``Decimal("0")``.
- WETH/WBTC (both-sides-volatile) → ``None`` (no price oracle in this CLI).
- Non-trade intents (SUPPLY, BORROW, ...) → ``None``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.cli.strat_pnl import _amount_in_usd, compute_pnl_breakdown
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.portfolio.models import PortfolioMetrics


# ---------------------------------------------------------------------------
# Minimal stub for a LedgerEntry-shaped object without a full DB round-trip.
# ---------------------------------------------------------------------------


def _make_entry(
    intent_type: str,
    *,
    token_in: str,
    amount_in: str,
    token_out: str,
    amount_out: str,
    chain: str = "arbitrum",
) -> LedgerEntry:
    return LedgerEntry(
        deployment_id="test:abc123",
        timestamp=datetime.now(UTC),
        intent_type=intent_type,
        token_in=token_in,
        amount_in=amount_in,
        token_out=token_out,
        amount_out=amount_out,
        chain=chain,
        success=True,
    )


# ---------------------------------------------------------------------------
# Unit tests — SWAP path (human-form amounts on the ledger)
# ---------------------------------------------------------------------------


def test_swap_with_stable_token_out_returns_amount() -> None:
    """SWAP row with USDC token_out → amount_out returned as Decimal."""
    entry = _make_entry(
        "SWAP",
        token_in="WETH",
        amount_in="0.002125",
        token_out="USDC",
        amount_out="4.50",
    )
    result = _amount_in_usd(entry)
    assert result is not None
    assert result == Decimal("4.50")


def test_swap_with_stable_token_in_returns_amount() -> None:
    """SWAP row with USDC token_in → amount_in returned as Decimal."""
    entry = _make_entry(
        "SWAP",
        token_in="USDC",
        amount_in="100.00",
        token_out="WETH",
        amount_out="0.047",
    )
    result = _amount_in_usd(entry)
    assert result is not None
    assert result == Decimal("100.00")


def test_swap_integer_form_stable_amount_treated_as_human() -> None:
    """SWAP with ``amount_in="100"`` (no decimal point) is human, not wei.

    SWAP rows always come from ``SwapAmounts.amount_in_decimal`` which is a
    ``Decimal`` — ``str(Decimal("100"))`` is ``"100"`` (no trailing zeros).
    The reader MUST recognise this as $100, not 0.0001 USDC raw-wei.
    """
    entry = _make_entry(
        "SWAP",
        token_in="USDC",
        amount_in="100",  # integer-form, but human (Decimal-string)
        token_out="WETH",
        amount_out="0.047",
    )
    result = _amount_in_usd(entry)
    assert result == Decimal("100")


def test_swap_with_no_stable_leg_returns_none() -> None:
    """SWAP with no stable leg (e.g. WETH → ARB) → None (no price oracle)."""
    entry = _make_entry(
        "SWAP",
        token_in="WETH",
        amount_in="0.1",
        token_out="ARB",
        amount_out="450",
    )
    result = _amount_in_usd(entry)
    assert result is None


def test_swap_weth_wbtc_both_volatile_returns_none() -> None:
    """WETH/WBTC SWAP — neither leg is a stablecoin, no oracle → None.

    Per the task brief: the stable-side heuristic is intentionally not
    needed for both-sides-volatile pools because a real fix would route
    through a price oracle. ``strat pnl`` is a read-only local CLI with
    no network egress, so the cleanest behavior is to return ``None`` and
    let the caller skip the row.
    """
    entry = _make_entry(
        "SWAP",
        token_in="WETH",
        amount_in="0.5",
        token_out="WBTC",
        amount_out="0.013",
    )
    result = _amount_in_usd(entry)
    assert result is None


# ---------------------------------------------------------------------------
# Unit tests — LP_OPEN path (raw-wei amounts on the ledger). Multi-protocol.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "chain,stable_token,weth_wei,stable_raw,expected_usd",
    [
        # The canonical W1-3 fixture from the May-22 audit doc:
        # Uniswap V3 Arbitrum WETH/USDC.  amount_in=701279299182337 (WETH wei,
        # ~$1.49 @ $2117) + amount_out=1585552 (USDC 6dp, $1.585552).
        ("arbitrum", "USDC", "701279299182337", "1585552", Decimal("1.585552")),
        # Aerodrome Slipstream Base WETH/USDC.
        ("base", "USDC", "750000000000000", "1700000", Decimal("1.700000")),
        # PancakeSwap V3 Arbitrum WETH/USDC.
        ("arbitrum", "USDC", "800000000000000", "1810369", Decimal("1.810369")),
        # USDbC on Base (different stablecoin symbol; 6 dp).
        ("base", "USDBC", "650000000000000", "1450000", Decimal("1.450000")),
    ],
    ids=["uniswap_v3_arbitrum", "aerodrome_base", "pancakeswap_v3_arbitrum", "usdbc_base"],
)
def test_lp_open_raw_wei_scales_via_decimals(
    chain: str, stable_token: str, weth_wei: str, stable_raw: str, expected_usd: Decimal
) -> None:
    """LP_OPEN raw-wei amounts scale through token decimals to USD.

    This is the core W1-3 regression: pre-fix, ``amount_out=1585552`` was
    read as ``$1,585,552`` (USDC stable heuristic). Post-fix, the decimals
    resolver gives USDC=6dp, so 1585552 / 10^6 = $1.585552.

    Same logic applies across protocols: ``_amount_in_usd`` is connector-
    agnostic. The decimals come from the static token registry, not the
    protocol, so Uniswap V3 / Aerodrome / PancakeSwap V3 all behave
    identically on read.
    """
    entry = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in=weth_wei,
        token_out=stable_token,
        amount_out=stable_raw,
        chain=chain,
    )
    result = _amount_in_usd(entry)
    assert result is not None
    assert result == expected_usd
    # Sanity: the result is firmly sub-$10 — definitely not $1.5M.
    assert result < Decimal("10")


def test_lp_open_stable_amount_in_side() -> None:
    """LP_OPEN where the stable leg is on the ``amount_in`` side.

    Some LP_OPEN intents declare USDC as token0 (the ``amount_in`` slot in
    the ledger row). The decimals-aware reader must scale that side too.
    """
    entry = _make_entry(
        "LP_OPEN",
        token_in="USDC",
        amount_in="2500000",  # 2.5 USDC raw 6dp
        token_out="WETH",
        amount_out="1180000000000000",  # ~0.00118 WETH wei
        chain="arbitrum",
    )
    result = _amount_in_usd(entry)
    assert result == Decimal("2.500000")


def test_lp_open_weth_wbtc_both_volatile_returns_none() -> None:
    """LP_OPEN on WETH/WBTC (no stable leg) → None (same as SWAP)."""
    entry = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="500000000000000000",  # 0.5 WETH
        token_out="WBTC",
        amount_out="1300000",  # 0.013 WBTC (8dp)
        chain="arbitrum",
    )
    result = _amount_in_usd(entry)
    assert result is None


# ---------------------------------------------------------------------------
# Unit tests — LP_CLOSE path. Audit doc names LP_CLOSE in the bug class
# alongside LP_OPEN. In current code LP_CLOSE flows through SwapAmounts
# (human-form) but the read side must be defensive about it.
# ---------------------------------------------------------------------------


def test_lp_close_human_form_arbitrum() -> None:
    """LP_CLOSE today writes human-form via SwapAmounts. Reader handles it."""
    entry = _make_entry(
        "LP_CLOSE",
        token_in="WETH",
        amount_in="0.001434",
        token_out="USDC",
        amount_out="1.810369",
        chain="arbitrum",
    )
    result = _amount_in_usd(entry)
    assert result == Decimal("1.810369")


def test_lp_close_human_form_base_aerodrome() -> None:
    """Aerodrome Slipstream LP_CLOSE on Base (different chain, same path)."""
    entry = _make_entry(
        "LP_CLOSE",
        token_in="WETH",
        amount_in="0.0007",
        token_out="USDC",
        amount_out="1.490200",
        chain="base",
    )
    result = _amount_in_usd(entry)
    assert result == Decimal("1.490200")


def test_lp_close_weth_wbtc_returns_none() -> None:
    """LP_CLOSE on WETH/WBTC → None (no stable side)."""
    entry = _make_entry(
        "LP_CLOSE",
        token_in="WETH",
        amount_in="0.5",
        token_out="WBTC",
        amount_out="0.013",
        chain="arbitrum",
    )
    result = _amount_in_usd(entry)
    assert result is None


def test_lp_collect_fees_human_form() -> None:
    """LP_COLLECT_FEES rows also resolve correctly (same path as LP_CLOSE)."""
    entry = _make_entry(
        "LP_COLLECT_FEES",
        token_in="WETH",
        amount_in="0.00000008",
        token_out="USDC",
        amount_out="0.000148",
        chain="arbitrum",
    )
    result = _amount_in_usd(entry)
    # 0.000148 USDC = $0.000148. Tiny but real.
    assert result == Decimal("0.000148")


# ---------------------------------------------------------------------------
# Empty != Zero discipline (AGENTS.md §Accounting)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("amount_in", "amount_out"),
    [("", ""), (None, None)],
    ids=["empty-string", "none"],
)
def test_lp_open_amount_in_none_returns_none(
    amount_in: str | None, amount_out: str | None
) -> None:
    """Unmeasured ``amount_in = None`` (or "") propagates as None, never $0.

    Empty != Zero: an unmeasured amount is NOT the same as a measured zero.
    The stable-side heuristic must not substitute $0 when a leg is missing.

    Parametrised on both ``""`` and ``None`` to lock the contract that the
    two unmeasured shapes are treated identically (CodeRabbit follow-up:
    prevent silent regression of the ``None`` branch).
    """
    entry = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in=amount_in,  # type: ignore[arg-type]
        token_out="USDC",
        amount_out=amount_out,  # type: ignore[arg-type]
        chain="arbitrum",
    )
    result = _amount_in_usd(entry)
    assert result is None


def test_lp_open_measured_zero_stable_side_falls_through() -> None:
    """``amount_out="0"`` on the stable side is measured zero, not $X.

    The stable-side heuristic only fires on a positive amount; a measured
    zero (Decimal("0")) falls through to the next leg / None so a single-
    sided LP deposit that happened to put 0 on the stable side doesn't
    misreport as $0 trade size.
    """
    entry = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="701279299182337",
        token_out="USDC",
        amount_out="0",  # measured zero on stable side
        chain="arbitrum",
    )
    result = _amount_in_usd(entry)
    # WETH leg can't be valued (no oracle), USDC leg is 0 → None.
    assert result is None


def test_unresolvable_token_with_raw_wei_returns_none() -> None:
    """Unresolvable token in a raw-wei intent → None (no 18-decimal default)."""
    entry = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="701279299182337",
        token_out="UNKNOWN_TOKEN_XYZ",
        amount_out="1585552",
        chain="arbitrum",
    )
    result = _amount_in_usd(entry)
    # Stable heuristic fails (UNKNOWN_TOKEN_XYZ not in _STABLE_SYMBOLS) and
    # WETH is not stable either; both legs eventually yield None.
    assert result is None


# ---------------------------------------------------------------------------
# Non-trade intent types — should always return None
# ---------------------------------------------------------------------------


def test_supply_returns_none() -> None:
    """SUPPLY (lending) rows → None."""
    entry = _make_entry(
        "SUPPLY",
        token_in="USDC",
        amount_in="10.00",
        token_out="",
        amount_out="",
    )
    result = _amount_in_usd(entry)
    assert result is None


def test_borrow_returns_none() -> None:
    """BORROW rows → None."""
    entry = _make_entry(
        "BORROW",
        token_in="",
        amount_in="",
        token_out="USDT",
        amount_out="3.11",
    )
    result = _amount_in_usd(entry)
    assert result is None


def test_repay_returns_none() -> None:
    entry = _make_entry(
        "REPAY",
        token_in="USDT",
        amount_in="3.11",
        token_out="",
        amount_out="",
    )
    assert _amount_in_usd(entry) is None


def test_withdraw_returns_none() -> None:
    entry = _make_entry(
        "WITHDRAW",
        token_in="USDC",
        amount_in="10.00",
        token_out="",
        amount_out="",
    )
    assert _amount_in_usd(entry) is None


def test_perp_open_returns_none() -> None:
    entry = _make_entry(
        "PERP_OPEN",
        token_in="USDC",
        amount_in="100.00",
        token_out="",
        amount_out="",
    )
    assert _amount_in_usd(entry) is None


# ---------------------------------------------------------------------------
# Codex audit follow-ups (PR #2484 round 2)
# ---------------------------------------------------------------------------


def test_lp_open_intent_amount_fallback_whole_number_not_scaled() -> None:
    """LP_OPEN intent-fallback path stores ``str(Decimal("100"))`` = ``"100"``.

    The writer (``observability/ledger.py:_extract_from_lp_open``) takes a
    user-supplied ``intent.amount0/amount1`` when ``lp_open_data`` is absent
    from the receipt. Those intent amounts are human-form Decimals; a whole
    number like ``Decimal("100")`` stringifies WITHOUT a decimal point.

    Without the magnitude-based disambiguation, the read side would treat
    ``"100"`` as raw-wei and scale by 10^6 (USDC decimals) → $0.0001 for a
    real $100 deposit. The magnitude check (``< 10^6 → human``) catches this.
    """
    entry = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="0.05",  # human-form whole-Decimal stringifies with no `.`
        token_out="USDC",
        amount_out="100",  # the bug case: Decimal("100") -> "100", no `.`
        chain="arbitrum",
    )
    result = _amount_in_usd(entry)
    # Should report $100 (human form preserved), NOT $0.0001 (scaled by 10^6).
    assert result is not None
    assert result == Decimal("100")


def test_lp_open_intent_amount_fallback_large_whole_human_skipped() -> None:
    """Edge case: a human-form integer >= 10^6 USDC is ambiguous from raw-wei.

    A $1,000,000 USDC LP intent would stringify as ``"1000000"`` — identical
    to a raw 1 USDC. We accept the limitation: at the 10^6 threshold the
    magnitude heuristic prefers the raw-wei interpretation (the common case
    on the canonical fixture). A genuine $1M+ LP intent fallback would
    under-report as $1.00, but this is unmeasured-with-fallback territory;
    the proper fix is VIB-3204 (explicit ``amount_in_usd`` column on the
    writer side).
    """
    entry = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="500000000000000000",  # 0.5 WETH raw-wei
        token_out="USDC",
        amount_out="1000000",  # could be $1M human OR 1 USDC raw — we pick raw
        chain="arbitrum",
    )
    result = _amount_in_usd(entry)
    # Raw-wei interpretation: 1_000_000 / 10^6 = 1.0 USDC = $1.00
    assert result is not None
    assert result == Decimal("1")


def test_lp_open_raw_wei_with_chain_prefixed_address() -> None:
    """Tokens persisted as ``chain:address`` must resolve decimals correctly.

    The writer historically may serialise tokens as ``"arbitrum:0xaf88..."``
    (VIB-3206 comment). ``_resolve_symbol`` strips the prefix before resolver
    lookup; ``_human_amount`` must do the same, otherwise the LP_OPEN row
    fails to scale and gets skipped from ``avg_trade_size_usd`` despite
    having a perfectly resolvable USDC address underneath.
    """
    # USDC on Arbitrum canonical address.
    usdc_arb = "arbitrum:0xaf88d065e77c8cc2239327c5edb3a432268e5831"
    entry = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="701279299182337",
        token_out=usdc_arb,
        amount_out="1585552",  # raw 6-dp USDC = $1.585552
        chain="arbitrum",
    )
    result = _amount_in_usd(entry)
    assert result is not None
    # Within rounding: $1.585552 (raw 1_585_552 / 10^6).
    assert abs(result - Decimal("1.585552")) < Decimal("0.000001")


# ---------------------------------------------------------------------------
# End-to-end: avg_trade_size_usd regression (the May-22 audit's canonical case)
# ---------------------------------------------------------------------------


def _make_metrics(initial: str = "9.00", total: str = "9.00") -> PortfolioMetrics:
    return PortfolioMetrics(
        deployment_id="test:abc123",
        timestamp=datetime.now(UTC),
        total_value_usd=Decimal(total),
        initial_value_usd=Decimal(initial),
        gas_spent_usd=Decimal("0"),
    )


def test_avg_trade_size_usd_not_inflated_by_lp_rows() -> None:
    """May-22 audit doc §B.4 LP-3: SWAP + 3 LP_OPEN raw-wei rows.

    Before the W1-3 fix: the 3 LP_OPEN rows contributed $1,585,552 /
    $1,654,257 / $1,810,369 (raw 6-dp USDC misread as human USD), and the
    initial SWAP contributed $4.50.  Average = $1,262,545.56 — the famous
    phantom "Avg trade size: $1,262,545.62" from the audit doc.

    After the fix: the 3 LP_OPEN rows scale via USDC decimals (6) to
    $1.585552 / $1.654257 / $1.810369, and the SWAP contributes $4.50.
    Average = ($4.50 + $1.585552 + $1.654257 + $1.810369) / 4 ≈ $2.39.

    Acceptance criteria from the audit doc + task brief: avg < $100 (audit)
    and < $5 (tighter — these are sub-$2 trades).
    """
    swap = _make_entry(
        "SWAP",
        token_in="USDC",
        amount_in="4.50",
        token_out="WETH",
        amount_out="0.002125",
    )
    lp1 = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="701279299182337",
        token_out="USDC",
        amount_out="1585552",
    )
    lp2 = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="731000000000000",
        token_out="USDC",
        amount_out="1654257",
    )
    lp3 = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="800000000000000",
        token_out="USDC",
        amount_out="1810369",
    )

    breakdown = compute_pnl_breakdown(
        deployment_id="test:abc123",
        metrics=_make_metrics(),
        ledger_entries=[swap, lp1, lp2, lp3],
        position_events=[],
        snapshot=None,
    )

    assert breakdown.avg_trade_size_usd is not None
    # Audit acceptance: < $100. Tight check: < $5 (the trades are sub-$2).
    assert breakdown.avg_trade_size_usd < Decimal("100")
    assert breakdown.avg_trade_size_usd < Decimal("5"), (
        f"avg_trade_size_usd={breakdown.avg_trade_size_usd} should be ~$2.39 "
        f"(mean of $4.50 + $1.585552 + $1.654257 + $1.810369 across 4 rows)"
    )

    # Exact expected average:
    expected = (
        Decimal("4.50") + Decimal("1.585552") + Decimal("1.654257") + Decimal("1.810369")
    ) / Decimal(4)
    assert breakdown.avg_trade_size_usd == expected


def test_avg_trade_size_pure_lp_strategy() -> None:
    """Pure-LP strategy (no SWAP rows) now produces a meaningful average.

    Before the scalable W1-3 fix, ``_amount_in_usd`` returned ``None`` for
    every LP_OPEN row, so a pure-LP strategy showed ``avg_trade_size: —``
    (missing). The decimals-aware fix means a pure-LP triple strategy now
    surfaces a real avg trade size — the stable-side dollar amount per LP.
    """
    lp1 = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="701279299182337",
        token_out="USDC",
        amount_out="1585552",
        chain="arbitrum",
    )
    lp2 = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="731000000000000",
        token_out="USDC",
        amount_out="1654257",
        chain="arbitrum",
    )

    breakdown = compute_pnl_breakdown(
        deployment_id="test:abc123",
        metrics=_make_metrics(),
        ledger_entries=[lp1, lp2],
        position_events=[],
        snapshot=None,
    )
    assert breakdown.avg_trade_size_usd is not None
    assert breakdown.avg_trade_size_usd < Decimal("5")
    expected = (Decimal("1.585552") + Decimal("1.654257")) / Decimal(2)
    assert breakdown.avg_trade_size_usd == expected


def test_avg_trade_size_skips_unmeasured_lp_rows() -> None:
    """Empty != Zero: rows with ``amount_*=""`` are skipped from the average.

    The denominator counts only rows that contributed a measured positive
    notional; an unmeasured row does not drag the mean toward zero.
    """
    swap = _make_entry(
        "SWAP",
        token_in="USDC",
        amount_in="10.00",
        token_out="WETH",
        amount_out="0.005",
    )
    lp_unmeasured = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="",  # unmeasured
        token_out="USDC",
        amount_out="",  # unmeasured
    )
    lp_measured = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="701279299182337",
        token_out="USDC",
        amount_out="1585552",
    )

    breakdown = compute_pnl_breakdown(
        deployment_id="test:abc123",
        metrics=_make_metrics(),
        ledger_entries=[swap, lp_unmeasured, lp_measured],
        position_events=[],
        snapshot=None,
    )

    # Only 2 measured rows: SWAP $10 + LP_OPEN $1.585552 → avg = $5.792776.
    assert breakdown.avg_trade_size_usd == (Decimal("10.00") + Decimal("1.585552")) / Decimal(2)
    # trade_count counts ALL ledger rows (including the unmeasured one).
    assert breakdown.trade_count == 3
