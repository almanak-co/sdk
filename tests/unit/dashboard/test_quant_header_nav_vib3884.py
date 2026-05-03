"""VIB-3884 — Senior-Quant header "NAV now" tile reads wallet NAV, not deployed.

Codex F1 corrected the v1 framing: ``portfolio_snapshots.total_value_usd``
is *deployed positions only* per VIB-3614 (``portfolio_valuer.py:241-247``).
A pre-deployment / fresh-iter / post-close snapshot legitimately has
``total=0, available_cash>0, value_confidence=HIGH`` — that is NOT a
snapshot-writer bug. The actual bug was dashboard-side: the "NAV now"
header tile rendered ``total_value_usd`` (deployed-only) when the
Senior-Quant audience reads it as wallet net asset value
(``total + available_cash``).

These tests fence the column-mapping fix in ``build_quant_header``:
the May 2 reproducer (Deployed $19.26, NAV $4.35, Cash 343% of NAV)
must produce a sane wallet NAV that exceeds Deployed.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from almanak.framework.dashboard.quant_aggregations import build_quant_header


def _metrics(initial: str = "20.00", deposits: str = "0", withdrawals: str = "0"):
    return SimpleNamespace(
        initial_value_usd=initial,
        deposits_usd=deposits,
        withdrawals_usd=withdrawals,
        total_value_usd=initial,  # legacy field; ignored post-VIB-3884
        initial_timestamp=datetime.now(tz=UTC).isoformat(),
        gas_spent_usd="0",
    )


def _snapshot(*, total: str, cash: str, deployed_capital: str = "0", confidence: str = "HIGH"):
    return SimpleNamespace(
        total_value_usd=total,
        available_cash_usd=cash,
        value_confidence=confidence,
        deployed_capital_usd=deployed_capital,
        positions_json="[]",
    )


# ──────────────────────────────────────────────────────────────────────────
# Direct property: wallet NAV = total + cash
# ──────────────────────────────────────────────────────────────────────────


def test_nav_now_equals_total_plus_cash_post_lp_open():
    """The May 2 reproducer in §0 of AccountingPost1977.md.

    Pre-VIB-3884 the dashboard rendered:
        Deployed $19.26 / NAV $4.35 / Cash $14.91 (343% of NAV)
    The 343% ratio was the smoke. Post-VIB-3884:
        Deployed $19.26 / NAV $19.26 / Cash $14.91 (~77% of NAV).
    """
    h = build_quant_header(
        portfolio_metrics=_metrics(initial="19.26"),
        snapshots=[_snapshot(total="4.35", cash="14.91", deployed_capital="4.35")],
        ledger_entries=[],
        accounting_events=[],
    )
    # NAV now is the wallet NAV (positions + cash), not deployed-only.
    assert h.nav_usd == Decimal("19.26")
    # Lifetime PnL is essentially flat — the strategy redeployed cash
    # into LP, no organic loss apart from gas (which the test omits).
    assert h.lifetime_pnl_usd == Decimal("0.00")
    # Cash buffer ratio = cash / wallet_nav < 100% (sanity check).
    cash_pct = (h.available_cash_usd / h.nav_usd) * Decimal("100")
    assert cash_pct < Decimal("100"), (
        f"cash_pct={cash_pct}; pre-VIB-3884 this rendered as 343% absurd"
    )


def test_nav_now_pre_deployment_snapshot_is_cash_only():
    """Pre-deployment snapshot: total=0, cash=100 → wallet NAV is 100."""
    h = build_quant_header(
        portfolio_metrics=_metrics(initial="100"),
        snapshots=[_snapshot(total="0", cash="100")],
        ledger_entries=[],
        accounting_events=[],
    )
    assert h.nav_usd == Decimal("100")
    assert h.lifetime_pnl_usd == Decimal("0")


def test_nav_now_post_close_snapshot_is_cash_only():
    """Post-LP-close snapshot: positions returned to cash; total=0, cash≈100."""
    h = build_quant_header(
        portfolio_metrics=_metrics(initial="100"),
        snapshots=[
            _snapshot(total="0", cash="100"),
            _snapshot(total="50", cash="50", deployed_capital="50"),
            _snapshot(total="0", cash="103"),  # LP closed, $3 of fees realised
        ],
        ledger_entries=[],
        accounting_events=[],
    )
    assert h.nav_usd == Decimal("103")
    assert h.lifetime_pnl_usd == Decimal("3")


def test_nav_now_uses_latest_snapshot_when_multiple():
    """Build uses ``snapshots[-1]`` for wallet NAV — older entries don't bleed."""
    h = build_quant_header(
        portfolio_metrics=_metrics(initial="50"),
        snapshots=[
            _snapshot(total="50", cash="0", deployed_capital="50"),
            _snapshot(total="40", cash="20", deployed_capital="40"),
        ],
        ledger_entries=[],
        accounting_events=[],
    )
    # Latest: 40 + 20 = 60 wallet NAV; PnL = 60 - 50 = 10.
    assert h.nav_usd == Decimal("60")
    assert h.lifetime_pnl_usd == Decimal("10")


# ──────────────────────────────────────────────────────────────────────────
# Drawdown — must be measured on wallet NAV, not deployed-only
# ──────────────────────────────────────────────────────────────────────────


def test_drawdown_uses_wallet_nav_not_deployed_only():
    """A strategy that fully un-deploys then re-deploys has NO drawdown
    at the wallet level — total=0 transitions are not 100% drawdowns.

    Pre-VIB-3884 the drawdown helper read ``total_value_usd`` only and
    reported max_drawdown=100% on every undeployed snapshot. Post-fix
    drawdown is computed against ``total + cash`` (wallet NAV), which
    stays steady through deploy/redeploy cycles."""
    h = build_quant_header(
        portfolio_metrics=_metrics(initial="100"),
        snapshots=[
            _snapshot(total="0", cash="100"),
            _snapshot(total="100", cash="0", deployed_capital="100"),  # deployed
            _snapshot(total="0", cash="100"),  # un-deployed
            _snapshot(total="100", cash="0", deployed_capital="100"),  # re-deployed
        ],
        ledger_entries=[],
        accounting_events=[],
    )
    # Wallet NAV is constant at $100 across all four snapshots.
    assert h.max_drawdown_pct == Decimal("0")
    assert h.current_drawdown_pct == Decimal("0")


# ──────────────────────────────────────────────────────────────────────────
# Defensive: empty / missing snapshot inputs collapse to zero, not error
# ──────────────────────────────────────────────────────────────────────────


def test_no_snapshots_zero_nav_no_crash():
    h = build_quant_header(
        portfolio_metrics=_metrics(initial="100"),
        snapshots=[],
        ledger_entries=[],
        accounting_events=[],
    )
    assert h.nav_usd == Decimal("0")
    # Lifetime PnL = 0 - 100 = -100 (no live data → cannot compute true PnL).
    # The dashboard footer surfaces this via value_confidence=UNAVAILABLE.
    assert h.value_confidence == "UNAVAILABLE"


def test_snapshot_with_missing_cash_field_treats_as_zero():
    """Older fixtures may omit ``available_cash_usd``; helper must not raise."""
    snap = SimpleNamespace(
        total_value_usd="50",
        # available_cash_usd intentionally missing
        value_confidence="HIGH",
        deployed_capital_usd="50",
        positions_json="[]",
    )
    h = build_quant_header(
        portfolio_metrics=_metrics(initial="50"),
        snapshots=[snap],
        ledger_entries=[],
        accounting_events=[],
    )
    # cash defaults to 0 → wallet NAV = 50.
    assert h.nav_usd == Decimal("50")
