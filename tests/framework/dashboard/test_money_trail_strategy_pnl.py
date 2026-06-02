"""Unit tests for the strategy-scoped PnL / APR helpers behind the Money Trail.

These replace the old wallet-level ``nav − deployed`` PnL / APR (which double-
counted idle wallet balances and moved whenever gas was spent or another
strategy traded the same wallet — ``deployment_id`` is wallet+chain-scoped).

Strategy PnL = net realized (from accounting: realized close/swap PnL + LP fees
earned + funding net + interest net − gas) + unrealized (open position NAV −
open cost basis). The headline property under test is **wallet-independence**:
idle cash / wallet-deployed swings must not move Strategy PnL.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.dashboard.gateway_client import CostStackInfo, PnLSummary
from almanak.framework.dashboard.pages._detail_header import (
    _net_realized_pnl_usd,
    _strategy_apr_pct,
    _strategy_pnl_usd,
)

D = Decimal


def _pnl(**overrides: object) -> PnLSummary:
    base: dict[str, object] = {
        "deployed_usd": D("100"),
        "nav_usd": D("110"),
        "lifetime_pnl_usd": D("10"),
        "lifetime_pnl_pct": D("10"),
        "net_apr_pct": D("0"),
        "max_drawdown_pct": D("0"),
        "current_drawdown_pct": D("0"),
        "value_confidence": "HIGH",
        "age_days": 10,
        "deployed_capital_usd": D("40"),
        "available_cash_usd": D("70"),
        "open_position_count": 1,
        "primary_risk_kind": "lp",
        "primary_risk_label": "Range",
        "primary_risk_value": "in-range",
        "primary_risk_color": "green",
    }
    base.update(overrides)
    return PnLSummary(**base)  # type: ignore[arg-type]


def _cost(**overrides: object) -> CostStackInfo:
    base: dict[str, object] = {
        "cost_gas_usd": D("0"),
        "cost_protocol_fees_usd": D("0"),
        "cost_slippage_usd": D("0"),
        "fees_earned_usd": D("0"),
        "interest_paid_usd": D("0"),
        "interest_earned_usd": D("0"),
        "funding_paid_usd": D("0"),
        "funding_earned_usd": D("0"),
        "realized_pnl_usd": D("0"),
        "il_usd": D("0"),
    }
    base.update(overrides)
    return CostStackInfo(**base)  # type: ignore[arg-type]


# ── _net_realized_pnl_usd ────────────────────────────────────────────────────


def test_net_realized_mirrors_g6_component_decomposition() -> None:
    """realized + fees + funding_net + interest_net − gas."""
    cost = _cost(
        realized_pnl_usd=D("5"),
        fees_earned_usd=D("2"),
        funding_earned_usd=D("3"),
        funding_paid_usd=D("1"),
        interest_earned_usd=D("4"),
        interest_paid_usd=D("1.5"),
        cost_gas_usd=D("0.75"),
    )
    # 5 + 2 + (3-1) + (4-1.5) - 0.75 = 10.75
    assert _net_realized_pnl_usd(cost) == D("10.75")


def test_net_realized_excludes_protocol_fees_and_slippage_and_il() -> None:
    """Those are already embedded in realized prices / are diagnostic — adding
    them would double-count (matches ``compute_reconciliation``)."""
    cost = _cost(
        realized_pnl_usd=D("5"),
        cost_protocol_fees_usd=D("9"),
        cost_slippage_usd=D("9"),
        il_usd=D("9"),
    )
    assert _net_realized_pnl_usd(cost) == D("5")


# ── _strategy_pnl_usd ────────────────────────────────────────────────────────


def test_strategy_pnl_is_realized_plus_unrealized() -> None:
    p = _pnl(nav_usd=D("110"), available_cash_usd=D("70"), deployed_capital_usd=D("40"))
    # open_position_nav = 110 - 70 = 40 ; unrealized = 40 - 40 = 0
    cost = _cost(realized_pnl_usd=D("3"))
    open_nav = p.nav_usd - p.available_cash_usd
    assert _strategy_pnl_usd(p, cost, open_nav) == D("3")  # realized only


def test_strategy_pnl_includes_unrealized_markup() -> None:
    p = _pnl(nav_usd=D("120"), available_cash_usd=D("70"), deployed_capital_usd=D("40"))
    # open_position_nav = 50 ; unrealized = 50 - 40 = 10
    cost = _cost(realized_pnl_usd=D("3"))
    open_nav = p.nav_usd - p.available_cash_usd
    assert _strategy_pnl_usd(p, cost, open_nav) == D("13")


def test_strategy_pnl_is_none_when_cost_unavailable() -> None:
    p = _pnl()
    open_nav = p.nav_usd - p.available_cash_usd
    assert _strategy_pnl_usd(p, None, open_nav) is None


def test_strategy_pnl_is_none_when_cost_basis_unmeasured_with_open_positions() -> None:
    """Empty ≠ Zero: the intermittent intra-run NAV double-count snapshot
    (deployed_capital_usd == 0 while positions are live, VIB-3932 cluster).
    Treating the 0 as real would make unrealized = NAV − 0 = the entire
    position value — the bogus "+$13.01 / +100%" the operator screenshotted.
    Must degrade to None ("—") instead.
    """
    # Reproduces the reported snapshot: NAV 26.21, cash 13.11 → open nav 13.10,
    # cost basis 0.
    p = _pnl(nav_usd=D("26.21"), available_cash_usd=D("13.11"), deployed_capital_usd=D("0"))
    open_nav = p.nav_usd - p.available_cash_usd
    assert open_nav > D("0.01")
    assert _strategy_pnl_usd(p, _cost(realized_pnl_usd=D("0")), open_nav) is None


def test_strategy_pnl_is_none_when_cost_basis_is_sub_dust_with_open_positions() -> None:
    """A sub-dust cost basis (e.g. $0.005) with live positions behaves like an
    unmeasured 0 — the guard must still fire, else unrealized = NAV − ~0 inflates
    PnL. Gemini review hardening on PR #2576."""
    p = _pnl(nav_usd=D("26.21"), available_cash_usd=D("13.11"), deployed_capital_usd=D("0.005"))
    open_nav = p.nav_usd - p.available_cash_usd
    assert _strategy_pnl_usd(p, _cost(realized_pnl_usd=D("0")), open_nav) is None


def test_strategy_apr_none_when_cost_basis_is_sub_dust() -> None:
    """A dust denominator would yield astronomical APR — must be None."""
    assert _strategy_apr_pct(D("10"), D("0.005"), 10) is None


def test_strategy_pnl_computes_when_flat_and_cost_basis_zero() -> None:
    """A genuinely flat strategy (everything closed → open nav ~ 0, cost basis
    0) is NOT caught by the unmeasured-cost-basis guard — it returns realized
    PnL, not "—"."""
    p = _pnl(nav_usd=D("100"), available_cash_usd=D("100"), deployed_capital_usd=D("0"))
    open_nav = p.nav_usd - p.available_cash_usd
    assert open_nav == D("0")
    assert _strategy_pnl_usd(p, _cost(realized_pnl_usd=D("2.5")), open_nav) == D("2.5")


def test_strategy_pnl_is_wallet_independent() -> None:
    """The whole point: idle cash / wallet-deployed swings (e.g. a co-tenant
    strategy spending the shared wallet, or gas burn) must NOT move Strategy
    PnL, as long as the strategy's own positions + realized accounting are
    unchanged. The OLD wallet PnL (nav − deployed) would move here."""
    cost = _cost(realized_pnl_usd=D("3"))

    # Position economics fixed: open_position_nav = 40, cost basis = 40.
    clean = _pnl(nav_usd=D("110"), available_cash_usd=D("70"), deployed_capital_usd=D("40"))
    # Co-tenant drains $25 of idle wallet cash → nav & cash both drop $25,
    # open_position_nav (nav − cash) is unchanged at 40.
    contaminated = _pnl(nav_usd=D("85"), available_cash_usd=D("45"), deployed_capital_usd=D("40"))

    open_clean = clean.nav_usd - clean.available_cash_usd
    open_dirty = contaminated.nav_usd - contaminated.available_cash_usd
    assert open_clean == open_dirty == D("40")

    pnl_clean = _strategy_pnl_usd(clean, cost, open_clean)
    pnl_dirty = _strategy_pnl_usd(contaminated, cost, open_dirty)
    assert pnl_clean == pnl_dirty == D("3")

    # ... whereas the old wallet method diverges by the full $25 contamination.
    assert (clean.nav_usd - clean.deployed_usd) - (contaminated.nav_usd - contaminated.deployed_usd) == D("25")


# ── _strategy_apr_pct ────────────────────────────────────────────────────────


def test_strategy_apr_annualises_over_cost_basis() -> None:
    # pnl 10 / cost basis 40 = 25% over 10 days → ×365/10 = 912.5%
    apr = _strategy_apr_pct(D("10"), D("40"), 10)
    assert apr == D("10") / D("40") * D("365") / D("10") * D("100")


def test_strategy_apr_none_when_pnl_none() -> None:
    assert _strategy_apr_pct(None, D("40"), 10) is None


def test_strategy_apr_none_when_zero_cost_basis() -> None:
    """All positions closed → nothing deployed → APR undefined, not div-by-0."""
    assert _strategy_apr_pct(D("10"), D("0"), 10) is None


def test_strategy_apr_none_when_zero_age() -> None:
    assert _strategy_apr_pct(D("10"), D("40"), 0) is None
