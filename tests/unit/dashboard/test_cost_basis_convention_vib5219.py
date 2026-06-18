"""VIB-5219 [US-013]: canonical cost-basis convention per consuming tile.

US-012 (``test_netting_parity.py``) pinned the gross-vs-net cost-basis divergence
at the **aggregation-helper** level (``compute_net_debt_projection.net_cost`` vs the
valuer's ``Σ abs(cost_basis_usd)`` mirror). This file pins the convention one
layer up — at each **consuming surface** — so a tile can never silently read the
wrong quantity. It is the executable form of the per-consumer contract documented
in blueprint 27 §7.11 ("Canonical cost-basis convention per consumer").

The two consumers and their DECLARED conventions
-------------------------------------------------
* **Consumer A — snapshot projection (PortfolioValuer).**
  ``PortfolioSnapshot.deployed_capital_usd`` = GROSS ``Σ abs(cost_basis_usd)``
  (``portfolio_valuer.py:707-710``). The ``abs()`` counts the BORROW leg's cost
  as a *positive* deployed amount, so it is NOT net equity. This is the raw,
  un-netted yardstick the read paths consume — it is never rendered gross.
* **Consumer B — dashboard PnL tiles.**
  ``PnLSummary.deployed_capital_usd`` = NET-equity cost (collateral cost −
  borrow cost), set by ``compute_pnl_summary`` (``quant_aggregations.py:1546-1547``)
  whenever a debt leg exists. EVERY dashboard tile that displays cost basis reads
  this single NET projection:
    - "Open cost basis"  (``_detail_header.py:443``)
    - "Open exposure"    (``_detail_header.py:547``)
    - "Strategy PnL"     (``_detail_header._strategy_pnl_usd:322`` — open NAV − cost)
    - "Strategy APR"     (``_detail_header._strategy_apr_pct``  — PnL ÷ cost)

OQ-3 resolution (traced, not assumed): there is NO tile reading the wrong
convention. Consumer A is an internal projection (re-netted by ``net_debt_from_snapshot``
/ ``compute_pnl_summary`` before display); Consumer B is uniformly NET across all
four tiles. The gateway emits the netted ``pnl.deployed_capital_usd`` to the proto
(``dashboard_service.py:2578``), so the wire value the dashboard renders is NET.

Canonical leveraged fixture (blueprint 27 §7.11 / VIB-5201) — supply 10 wstETH @
$4000 = $40,000 collateral (cost $39,000), borrow 8 WETH @ $4000 = $32,000 debt
(cost $31,800):

  * gross deployed capital (Consumer A) = 39000 + 31800 = $70,800
  * net-equity cost       (Consumer B) =  39000 − 31800 =  $7,200
  * Δ = 2 × borrow cost = $63,600  (divergence BY DESIGN)
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.dashboard.gateway_client import CostStackInfo
from almanak.framework.dashboard.pages._detail_header import (
    _strategy_apr_pct,
    _strategy_pnl_usd,
)
from almanak.framework.dashboard.quant_aggregations import compute_pnl_summary
from almanak.framework.portfolio.models import PortfolioSnapshot
from almanak.framework.valuation.net_debt import compute_net_debt_projection

# --- canonical fixture economics (ground truth) ----------------------------
GROSS_DEPLOYED_CAPITAL = Decimal("70800")  # Consumer A: Σ abs(cost) = 39000 + 31800
NET_EQUITY_COST = Decimal("7200")  # Consumer B: 39000 − 31800
DIVERGENCE = Decimal("63600")  # 2 × borrow cost
TOTAL_VALUE_USD = Decimal("40000")  # positive legs only (VIB-3614 drops debt)
DEBT_MARK = Decimal("32000")
NET_EQUITY_NAV = Decimal("8000")  # collateral 40000 − debt 32000


def _canonical_leverage_snapshot() -> PortfolioSnapshot:
    """The production typed shape (``StateManager.get_recent_snapshots``): typed
    ``positions`` list, NO ``positions_json`` attribute, and a GROSS
    ``deployed_capital_usd`` stamp exactly as the valuer writes it.

    available_cash_usd = 0 so wallet NAV equals the net-equity NAV — keeps the
    tile arithmetic free of idle-cash terms and isolates the cost-basis read.
    """

    def _p(ptype: str, value: str, cost: str) -> dict:
        return {
            "position_type": ptype,
            "protocol": "aave_v3",
            "chain": "arbitrum",
            "value_usd": value,
            "cost_basis_usd": cost,
            "label": ptype,
            "tokens": [],
            "details": {},
        }

    return PortfolioSnapshot.from_dict(
        {
            "timestamp": datetime.now(tz=UTC).isoformat(),
            "deployment_id": "vib5219",
            "total_value_usd": str(TOTAL_VALUE_USD),
            "available_cash_usd": "0",
            # Consumer A: the valuer stamps GROSS Σ abs(cost) onto the column.
            "deployed_capital_usd": str(GROSS_DEPLOYED_CAPITAL),
            "wallet_total_value_usd": "8000",
            "value_confidence": "HIGH",
            "positions": [
                _p("SUPPLY", "40000", "39000"),
                _p("BORROW", "-32000", "-31800"),
            ],
        }
    )


def _zero_cost_stack() -> CostStackInfo:
    """A CostStackInfo with every realized/earn component measured-zero and
    inventory MTM unmeasured — so ``_strategy_pnl_usd`` reduces to its unrealized
    leg (``open_position_nav − deployed_capital_usd``), isolating the cost-basis
    read the tile performs.
    """
    z = Decimal("0")
    return CostStackInfo(
        cost_gas_usd=z,
        cost_protocol_fees_usd=z,
        cost_slippage_usd=z,
        fees_earned_usd=z,
        interest_paid_usd=z,
        interest_earned_usd=z,
        funding_paid_usd=z,
        funding_earned_usd=z,
        realized_pnl_usd=z,
        il_usd=z,
        inventory_unrealized_usd=None,
    )


# ---------------------------------------------------------------------------
# Consumer A — snapshot projection (valuer) reads GROSS
# ---------------------------------------------------------------------------


def test_consumer_a_snapshot_projection_is_gross():
    """The valuer's stamped ``deployed_capital_usd`` is the GROSS convention:
    Σ abs(cost_basis_usd), counting the borrow cost as a positive deployed amount.
    """
    snap = _canonical_leverage_snapshot()
    # Mirror portfolio_valuer.py:707-710 directly off the typed legs.
    gross = sum(
        (abs(p.cost_basis_usd) for p in snap.positions if p.cost_basis_usd != Decimal("0")),
        Decimal("0"),
    )
    assert gross == GROSS_DEPLOYED_CAPITAL
    # And the stamp the valuer persisted carries that same GROSS number.
    assert snap.deployed_capital_usd == GROSS_DEPLOYED_CAPITAL


# ---------------------------------------------------------------------------
# Consumer B — dashboard PnL projection reads NET-equity cost
# ---------------------------------------------------------------------------


def test_consumer_b_pnl_projection_is_net_equity_cost():
    """``compute_pnl_summary`` re-nets the GROSS snapshot stamp to NET-equity cost
    for a leverage loop — the single projection every dashboard tile reads.

    This is the convention boundary: the consumer differs from the snapshot stamp
    by exactly 2 × borrow cost ($63,600). A regression that let the gross stamp
    flow through unchanged would surface a phantom −debt loss downstream.
    """
    snap = _canonical_leverage_snapshot()
    pnl = compute_pnl_summary(
        portfolio_metrics=None,
        snapshots=[snap],
        ledger_entries=[],
        accounting_events=[],
    )
    assert pnl.deployed_capital_usd == NET_EQUITY_COST
    # NAV nets the debt mark exactly once (collateral − debt + cash).
    assert pnl.nav_usd == NET_EQUITY_NAV
    # The consumer is NET, not the GROSS stamp it was handed.
    assert pnl.deployed_capital_usd != snap.deployed_capital_usd
    assert snap.deployed_capital_usd - pnl.deployed_capital_usd == DIVERGENCE


def test_aggregation_helper_pins_both_conventions():
    """The aggregation helper exposes both quantities side-by-side so the
    divergence is explicit, not inferred (US-012 contract, re-pinned here on the
    same fixture the tile tests use).
    """
    snap = _canonical_leverage_snapshot()
    count, debt_mark, debt_cost, net_cost = compute_net_debt_projection(snap.positions)
    assert count == 2
    assert debt_mark == DEBT_MARK
    assert debt_cost == Decimal("31800")
    assert net_cost == NET_EQUITY_COST  # Consumer B basis
    # Consumer A basis (gross) − Consumer B basis (net) == 2 × borrow cost.
    assert (debt_cost + net_cost) + debt_cost == GROSS_DEPLOYED_CAPITAL


# ---------------------------------------------------------------------------
# Tile-level: every cost-basis tile reads the DECLARED (NET) convention
# ---------------------------------------------------------------------------


def test_strategy_pnl_tile_reads_net_cost_not_gross():
    """The Strategy-PnL tile differences ``open_position_nav − deployed_capital_usd``.
    Reading NET ($7,200) yields a small real unrealized PnL ($800 = net NAV $8,000 −
    net cost $7,200). Reading GROSS ($70,800) would manufacture a −$62,800 phantom
    loss on a flat loop — the exact failure mode VIB-4983/VIB-5170 fixed.
    """
    snap = _canonical_leverage_snapshot()
    pnl = compute_pnl_summary(
        portfolio_metrics=None,
        snapshots=[snap],
        ledger_entries=[],
        accounting_events=[],
    )
    open_position_nav = pnl.nav_usd - pnl.available_cash_usd  # 8000 − 0
    strategy_pnl = _strategy_pnl_usd(pnl, _zero_cost_stack(), open_position_nav)
    # unrealized = open NAV − NET cost = 8000 − 7200 = 800 (realized/inventory all 0).
    assert strategy_pnl == Decimal("800")
    # Had the tile read the GROSS stamp it would be 8000 − 70800 = −62800.
    assert strategy_pnl != open_position_nav - GROSS_DEPLOYED_CAPITAL


def test_strategy_apr_tile_denominator_is_net_cost():
    """The Strategy-APR tile annualises ``strategy_pnl ÷ deployed_capital_usd``.
    The denominator is the NET-equity cost; a gross denominator would understate
    APR by ~10× on this fixture (70800 vs 7200).
    """
    snap = _canonical_leverage_snapshot()
    pnl = compute_pnl_summary(
        portfolio_metrics=None,
        snapshots=[snap],
        ledger_entries=[],
        accounting_events=[],
    )
    strategy_pnl = Decimal("800")
    apr_net = _strategy_apr_pct(strategy_pnl, pnl.deployed_capital_usd, age_days=365)
    apr_gross = _strategy_apr_pct(strategy_pnl, GROSS_DEPLOYED_CAPITAL, age_days=365)
    assert apr_net is not None
    # 800 / 7200 × 100 (age 365d ⇒ annualisation factor 1) ≈ 11.11%.
    assert apr_net == (strategy_pnl / NET_EQUITY_COST) * Decimal("100")
    # The net denominator yields a materially higher (correct) APR than gross.
    assert apr_net > apr_gross


def test_no_debt_leg_gross_equals_net_both_conventions_agree():
    """Control: with no debt leg the two conventions COINCIDE — the snapshot stamp
    is left byte-identical (no spurious netting) and every tile reads the same
    number. Confirms the per-consumer split only bites debt-bearing primitives.
    """
    snap = PortfolioSnapshot.from_dict(
        {
            "timestamp": datetime.now(tz=UTC).isoformat(),
            "deployment_id": "vib5219-nodebt",
            "total_value_usd": "12.00",
            "available_cash_usd": "0",
            "deployed_capital_usd": "11.50",
            "wallet_total_value_usd": "12.00",
            "value_confidence": "HIGH",
            "positions": [
                {
                    "position_type": "LP",
                    "protocol": "uniswap_v3",
                    "chain": "arbitrum",
                    "value_usd": "12.00",
                    "cost_basis_usd": "11.50",
                    "label": "LP",
                    "tokens": [],
                    "details": {},
                }
            ],
        }
    )
    pnl = compute_pnl_summary(
        portfolio_metrics=None,
        snapshots=[snap],
        ledger_entries=[],
        accounting_events=[],
    )
    # No debt leg → the override at quant_aggregations.py:1546 does not fire; the
    # NET consumer equals the GROSS stamp.
    assert pnl.deployed_capital_usd == snap.deployed_capital_usd == Decimal("11.50")
