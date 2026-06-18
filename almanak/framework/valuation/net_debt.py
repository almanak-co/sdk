"""Canonical debt-netting projection for the PortfolioValuer contract (VIB-5222).

``almanak/framework/valuation/portfolio_valuer.py::PortfolioValuer`` is the single
source of truth for portfolio valuation at runtime, and blueprint 27 §7.11
("PortfolioValuer projection contract", VIB-5206) pins what each projection means.
The valuer stamps ``total_value_usd`` (Σ positive ``value_usd``, debt dropped per
VIB-3614) and ``deployed_capital_usd`` (Σ ``abs(cost_basis_usd)``, GROSS) onto the
snapshot, but the **debt_mark / NAV / net-equity-cost** re-netting is, by contract,
"computed in the read path … not stamped on the snapshot" (§7.11 table). This module
is that canonical read-path computation.

Historically the netting math lived inside the dashboard
(``framework/dashboard/quant_aggregations.py::_net_from_position_items``) and was
duplicated by the CLI/report lane (``accounting/lending_nav.py::compute_lending_nav``)
— the VIB-5202 bypass inventory (`docs/internal/bypass-inventory-vib-5202.md` §2.1 / §4
Tier 1) and the VIB-5217 shadow-parity report flagged lending as the *only* primitive
where the representation choice changes the answer. VIB-5222 (US-015) lifts the netting
math out of the dashboard into the valuation layer — its canonical home alongside the
valuer whose contract it implements — so the lending NAV / cost / PnL / drawdown read
paths all route through ONE implementation. The dashboard helpers remain as thin
delegating shims (kept, not deleted, for the gateway import + test surfaces) until
US-016 collapses them once cross-primitive parity is proven.

**The canonical money representation is the signed leg** (§7.11): a position's
``value_usd`` carries its economic sign — collateral / supply / long exposure positive,
debt negative. Every term below is a deterministic function of those signed legs:

* ``debt_mark`` = Σ|negative ``value_usd``| — the amount a NAV consumer subtracts from
  the (debt-dropped) ``total_value_usd`` *once* to recover net equity
  (``NAV = total_value_usd − debt_mark``; the VIB-5201 baseline: 40000 − 32000 = 8000).
* ``debt_cost`` = Σ|``cost_basis_usd``| over those negative legs (debt magnitude only).
* ``net_cost`` = Σ signed cost basis — asset legs add ``+|cost|``, debt legs add
  ``−|cost|`` (the **net-equity** cost basis the Strategy-PnL tile differences against
  the debt-netted open NAV, §7.11.1 Consumer B). Computing it directly from the legs
  avoids the writer's GROSS ``Σ abs(cost_basis_usd)`` convention.

Empty≠Zero (CLAUDE.md §Accounting): a leg with absent/unparsable ``value_usd`` is
skipped entirely (unmeasured — never coerced to a measured zero). A leg with a measured
value but absent/unparsable ``cost_basis_usd`` still nets its ``debt_mark`` (the
liability is real) but contributes to neither ``debt_cost`` nor ``net_cost``. The
accumulators are seeded as ``MeasuredMoney.measured(Decimal("0"))`` (VIB-5205) and only
ever add measured legs, so they stay measured throughout and the projection is a true
measured value, not a sentinel.

Pure value module — no gateway calls, no I/O, no heavy connector imports — so the
dashboard read path can import it without an import-cost or circular-import penalty.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.framework.accounting.measured import MeasuredMoney

__all__ = [
    "compute_net_debt_projection",
    "read_position_decimal",
]


def read_position_decimal(pos: Any, key: str) -> Decimal | None:
    """Read ``key`` off a position that is EITHER a typed ``PositionValue``
    dataclass (production ``PortfolioSnapshot.positions``) OR a dict (the
    ``positions_json`` text / envelope path).

    Returns ``None`` for an absent / empty / unparsable value — Empty≠Zero: an
    unmeasured field is never coerced to a measured ``Decimal("0")``. A non
    dict / non-object item (e.g. a stray string in a malformed payload) also
    yields ``None``.
    """
    raw = pos.get(key) if isinstance(pos, dict) else getattr(pos, key, None)
    if raw is None or raw == "":
        return None
    try:
        return Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError):
        return None


def compute_net_debt_projection(items: Any) -> tuple[int, Decimal, Decimal, Decimal]:
    """Core debt-netting over a position sequence → ``(count, debt_mark,
    debt_cost, net_cost)``.

    The canonical read-path computation of the PortfolioValuer projection
    contract's ``debt_mark`` / net-equity-cost terms (blueprint 27 §7.11). See the
    module docstring for the full term definitions, sign convention, and Empty≠Zero
    discipline.

    ``items`` is an iterable of typed ``PositionValue`` dataclasses
    (``PortfolioSnapshot.positions``, the PRODUCTION dashboard path) and/or dicts
    (the ``positions_json`` text / envelope path used by the lifetime-drawdown reader
    and legacy callers). ``count`` is the total item count (matching the legacy
    ``len(positions)`` contract), including malformed entries.
    """
    items_list = list(items)
    _ZERO = MeasuredMoney.measured(Decimal("0"))
    debt_mark = _ZERO
    debt_cost = _ZERO
    net_cost = _ZERO
    for pos in items_list:
        value = read_position_decimal(pos, "value_usd")
        if value is None:
            continue
        cost = read_position_decimal(pos, "cost_basis_usd")
        if value < 0:
            # value < 0, so -value is the positive debt magnitude.
            debt_mark = debt_mark + MeasuredMoney.measured(-value)
            if cost is not None:
                cost_abs = MeasuredMoney.measured(abs(cost))
                debt_cost = debt_cost + cost_abs
                net_cost = net_cost - cost_abs
        elif cost is not None:
            net_cost = net_cost + MeasuredMoney.measured(abs(cost))
    return len(items_list), debt_mark.value, debt_cost.value, net_cost.value
