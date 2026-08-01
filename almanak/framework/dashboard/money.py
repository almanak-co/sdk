"""Accounting-backed money for dashboard templates (VIB-6283).

> **A money value must never be *sourced* from ``session_state``.**

This module is the seam that makes that rule enforceable. It is the display-side
mirror of the write-side rule the accounting layer already enforces (AGENTS.md
§Accounting: *"``AccountingWriter`` is the only path that may call
``save_accounting_event()``"*). Without it, a strategy author can make the
dashboard report any PnL they like by putting a key in a dict.

Layers (blueprint 22 §"Money ownership contract"):

* **L1 Value** — framework, primitive-agnostic: NAV, cost basis, realized /
  unrealized, gas. Sourced from ``GetPnLSummary`` + ``GetCostStack``.
* **L2 Attribution** — framework, **per-primitive**: LP fees + IL, perp funding,
  lending interest. Also carried on the cost stack, folded per primitive.
* **L3 Presentation** — strategy: labels, custom charts, strategy KPIs. Never
  money.

Why the framework can own this at all: the dashboard templates
(``templates/{lp,perp,lending,prediction,ta}_dashboard.py``) are framework-owned
and **per-primitive**, not per-strategy. The "custom lane" that produced the
original bug was never strategy-specific — it was a second *framework*
computation that happened to source from a strategy-facing dict. Attribution
varies by primitive (~6), not by strategy (~50+), which is what makes single
ownership tractable.

**Empty ≠ Zero.** Every money field here is ``Decimal | None``. ``None`` means
UNMEASURED and must render ``—``; ``Decimal("0")`` is a measured zero and
renders ``$0.00``. Conflating them is the original defect: an LP panel rendered
``$0.00`` fees against a position with ~$88 of fees provably owed on-chain.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal

logger = logging.getLogger(__name__)


# Money-shaped keys a caller must never be able to inject into a money tile.
# Sourced from the census of what the five primitive templates actually read
# out of ``session_state`` today (VIB-6283); extend as primitives migrate onto
# this seam.
FORBIDDEN_MONEY_KEYS: frozenset[str] = frozenset(
    {
        # lp_dashboard
        "total_fees_usd",
        "impermanent_loss_pct",
        "net_pnl_usd",
        # perp_dashboard (VIB-5954)
        "size_usd",
        "collateral_usd",
        "unrealized_pnl",
        "realized_pnl",
        "total_pnl",
        # lending_dashboard
        "collateral_value_usd",
        "borrowed_value_usd",
        "interest_earned_usd",
        "interest_paid_usd",
        "net_interest_usd",
        "total_pnl_usd",
        # prediction_dashboard
        "cost_basis",
        "current_value",
        "total_fees_paid",
    }
)
"""Keys that are MONEY and therefore may not be sourced from ``session_state``.

Deliberately a deny-list of *known* money keys rather than a heuristic on the
name: it fails LOUD and specific (we name the key in the log) rather than
silently reclassifying an unrelated key as money. A key missing from this set
is a gap to close by adding it here, discovered by the census test.
"""


@dataclass(frozen=True)
class StrategyMoney:
    """L1 value + L2 attribution for one deployment, accounting-backed.

    Every field is presence-aware. ``None`` = unmeasured, never a fabricated
    zero.
    """

    # --- L1: primitive-agnostic value ---
    nav_usd: Decimal | None = None
    open_position_nav_usd: Decimal | None = None
    cost_basis_usd: Decimal | None = None
    gas_usd: Decimal | None = None
    strategy_pnl_usd: Decimal | None = None
    """The headline number. The SAME computation the Strategy PnL tile renders
    (``pages/_detail_header._strategy_pnl_usd``) — imported, not re-derived, so
    a per-primitive panel and the headline can never disagree. Two
    presentations, one computation."""

    # --- L2: per-primitive attribution ---
    lp_fees_earned_usd: Decimal | None = None
    lp_il_usd: Decimal | None = None
    # VIB-6283: coverage for the pair above. None value = inapplicable ("—");
    # ``partial`` True = a REAL sum that must be shown (dropping it understated
    # Strategy PnL) but must not read as a complete total. False = nothing to
    # flag, which is also the right answer when coverage is simply unknown.
    lp_fees_earned_partial: bool = False
    lp_il_partial: bool = False
    lending_interest_earned_usd: Decimal | None = None
    lending_interest_paid_usd: Decimal | None = None
    perp_funding_earned_usd: Decimal | None = None
    perp_funding_paid_usd: Decimal | None = None


def reject_caller_money_keys(session_state: dict | None, *, surface: str) -> dict:
    """Strip money-shaped keys from a caller-supplied ``session_state``.

    Ignore-and-log, never raise: a strategy that passes these today keeps
    working, it simply stops influencing what the dashboard reports as money.
    Raising would break existing strategies for a defect that is ours, not
    theirs.

    The log is WARNING and names the keys, because the failure this replaces
    was silent — the panel rendered a confident wrong number and nothing
    anywhere said so.
    """
    if not session_state:
        return {}
    present = sorted(FORBIDDEN_MONEY_KEYS.intersection(session_state))
    if present:
        logger.warning(
            "%s: ignoring caller-supplied money key(s) %s in session_state. Money on the "
            "dashboard is sourced from the accounting tables via the gateway, never from "
            "session_state (blueprint 22 §Money ownership contract). Use session_state "
            "for labels, chart data and strategy KPIs only.",
            surface,
            ", ".join(present),
        )
    return {k: v for k, v in session_state.items() if k not in FORBIDDEN_MONEY_KEYS}


def load_strategy_money(deployment_id: str | None) -> StrategyMoney | None:
    """Build :class:`StrategyMoney` from the gateway's accounting-backed RPCs.

    Returns ``None`` when the deployment is unknown or both RPCs are
    unavailable — the caller then renders ``—`` rather than zeros. A partial
    read (one RPC up, one down) still returns an object: the fields it could
    measure carry values and the rest stay ``None``, which is exactly the
    distinction the tiles render.
    """
    if not deployment_id:
        return None

    # Imported lazily: this module is pulled in by templates, and the section
    # helpers drag in Streamlit + the gRPC stack.
    from almanak.framework.dashboard.pages._detail_header import _strategy_pnl_usd
    from almanak.framework.dashboard.sections import get_cost_stack, get_pnl_summary

    try:
        pnl = get_pnl_summary(deployment_id)
    except Exception:
        logger.exception("load_strategy_money: GetPnLSummary failed for %s", deployment_id)
        pnl = None
    try:
        cost = get_cost_stack(deployment_id)
    except Exception:
        logger.exception("load_strategy_money: GetCostStack failed for %s", deployment_id)
        cost = None

    if pnl is None and cost is None:
        return None

    open_position_nav: Decimal | None = None
    if pnl is not None:
        open_position_nav = pnl.nav_usd - pnl.available_cash_usd
        if open_position_nav < Decimal("0"):
            open_position_nav = Decimal("0")

    strategy_pnl: Decimal | None = None
    if pnl is not None and open_position_nav is not None:
        # Reuse the headline's own function rather than reimplementing it. A
        # second computation that agrees today is precisely what drifted before
        # (VIB-5738 closed this symptom once; it recurred four times).
        strategy_pnl = _strategy_pnl_usd(pnl, cost, open_position_nav)

    return StrategyMoney(
        nav_usd=pnl.nav_usd if pnl is not None else None,
        open_position_nav_usd=open_position_nav,
        cost_basis_usd=pnl.deployed_capital_usd if pnl is not None else None,
        gas_usd=cost.cost_gas_usd if cost is not None else None,
        strategy_pnl_usd=strategy_pnl,
        # L2 — already presence-aware on CostStackInfo (VIB-6283 made the LP
        # pair Optional; they are None until a close / collect lands).
        lp_fees_earned_usd=cost.fees_earned_usd if cost is not None else None,
        lp_il_usd=cost.il_usd if cost is not None else None,
        lp_fees_earned_partial=cost.fees_earned_partial if cost is not None else False,
        lp_il_partial=cost.il_partial if cost is not None else False,
        lending_interest_earned_usd=cost.interest_earned_usd if cost is not None else None,
        lending_interest_paid_usd=cost.interest_paid_usd if cost is not None else None,
        perp_funding_earned_usd=cost.funding_earned_usd if cost is not None else None,
        perp_funding_paid_usd=cost.funding_paid_usd if cost is not None else None,
    )
