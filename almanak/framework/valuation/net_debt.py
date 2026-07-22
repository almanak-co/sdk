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
where the representation choice changes the answer. VIB-5222 (US-015) lifted the netting
math out of the dashboard into the valuation layer — its canonical home alongside the
valuer whose contract it implements — and VIB-5225 (US-016) then deleted the dashboard
delegating shims entirely and moved the typed position-sourcing accessors here too, so
this module is the **single** home for both the netting math AND its accessors. Every
lending NAV / cost / PnL / drawdown read path (dashboard read path + gateway
``dashboard_service`` history builders) routes through ONE implementation; there is no
duplicate netting math anywhere else.

Two thin typed accessors over :func:`compute_net_debt_projection` live here so callers
never re-derive the position-sourcing convention:

* :func:`net_debt_from_snapshot` — resolves a snapshot's typed ``PortfolioSnapshot.positions``
  list (production shape), falling back to a ``positions_json`` attribute/key (dict /
  ``SimpleNamespace`` / legacy callers), then nets.
* :func:`net_debt_from_positions_json` — the raw ``positions_json`` text/dict/list entry
  point (the lifetime-drawdown + windowed-PnL readers, which only have raw text), via
  :func:`parse_positions_payload`.

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

import json
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.framework.accounting.measured import MeasuredMoney

__all__ = [
    "compute_net_debt_projection",
    "net_debt_from_positions_json",
    "net_debt_from_snapshot",
    "parse_positions_payload",
    "read_position_decimal",
    "wallet_nav_usd",
]


def wallet_nav_usd(
    total_value_usd: Decimal,
    debt_mark: Decimal,
    available_cash_usd: Decimal,
) -> Decimal:
    """Wallet NAV (VIB-3884) = net position equity + idle wallet cash.

    ``net position equity = total_value_usd − debt_mark`` (the VIB-5201 baseline
    netting; ``total_value_usd`` is already positive-position-scoped per VIB-3614,
    so the BORROW debt leg is re-subtracted once here). Idle wallet cash lives in a
    separate column (``available_cash_usd``) and must be ADDED back to recover the
    net asset value an operator marks to market.

    This is the **single definition** shared by (all routed through THIS helper):

    * the "NAV now" tile (``quant_aggregations.compute_pnl_summary`` calls
      ``wallet_nav_usd(total_value_usd, debt_mark, available_cash_usd)`` — the
      former inline ``deployed_value_usd + available_cash_usd`` and its
      ``deployed_value_usd`` local were removed in VIB-5942),
    * the recent-window and lifetime drawdown series
      (``quant_aggregations._drawdowns`` / ``_wallet_navs_from_nav_text``), and
    * the NAV-history chart series (``dashboard_service`` recent + windowed PnL
      history builders, VIB-5942).

    Routing all of them through one function is why they agree by construction — the
    VIB-5942 defect was the chart builders computing net equity ALONE (cash
    dropped), so a post-close snapshot (position value 0, funds returned to cash)
    plotted NAV 0 and collapsed the chart. Empty≠Zero is the caller's job: pass a
    measured ``Decimal`` for each term, or drop the sample upstream — never coerce
    an unmeasured column to ``Decimal("0")`` here.
    """
    return total_value_usd - debt_mark + available_cash_usd


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


def parse_positions_payload(positions_json: Any) -> list:
    """Unwrap a ``positions_json`` payload to its bare position list.

    Accepts the legacy bare list, the VIB-3923 envelope
    ``{schema_version, positions, metadata, reconciliation}``, a JSON string of
    either, OR an already-deserialized list/dict (hosted Postgres JSON/JSONB
    columns and some test mocks hand back parsed objects — calling ``json.loads``
    on those would ``TypeError`` and silently drop the debt). Returns ``[]`` for an
    empty / malformed / non-list-non-dict payload.
    """
    if not positions_json:
        return []
    if isinstance(positions_json, list | dict):
        parsed = positions_json
    else:
        try:
            parsed = json.loads(positions_json)
        except (json.JSONDecodeError, TypeError):
            return []
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict) and isinstance(parsed.get("positions"), list):
        return parsed["positions"]
    return []


def net_debt_from_positions_json(positions_json: Any) -> tuple[int, Decimal, Decimal]:
    """``positions_json`` text/dict/list → ``(open_count, debt_mark, debt_cost)``.

    The raw-text entry point — used by the lifetime-drawdown reader
    (``quant_aggregations._wallet_navs_from_nav_text``, which only has raw
    ``positions_json`` text from ``get_nav_series``) and the gateway's windowed-PnL
    history builder. Unwraps the payload (:func:`parse_positions_payload`) and nets
    via :func:`compute_net_debt_projection`, dropping the signed ``net_cost`` term
    those callers do not consume.
    """
    count, debt_mark, debt_cost, _net_cost = compute_net_debt_projection(parse_positions_payload(positions_json))
    return count, debt_mark, debt_cost


def net_debt_from_snapshot(snap: Any) -> tuple[int, Decimal, Decimal, Decimal]:
    """``(count, debt_mark, debt_cost, net_cost)`` for one snapshot.

    Prefers the typed ``PortfolioSnapshot.positions`` list (the production shape
    returned by ``StateManager.get_recent_snapshots`` / ``get_latest_snapshot``) —
    these dataclasses carry ``value_usd`` / ``cost_basis_usd`` directly. Falls back
    to a ``positions_json`` attribute/key (dicts, ``SimpleNamespace`` test mocks,
    legacy callers); a bare dict carrying a ``positions`` list is handled too. A
    real ``PortfolioSnapshot`` has NO ``positions_json`` attribute — reading it
    returned ``None`` and silently no-op'd the netting; that was the inert-feature
    bug (VIB-5170) behind the persisted leverage phantom, fixed by preferring
    ``.positions`` here.
    """
    positions = getattr(snap, "positions", None)
    if positions is not None and not isinstance(snap, dict):
        return compute_net_debt_projection(positions)
    if isinstance(snap, dict):
        payload = snap.get("positions_json")
        if payload is None:
            payload = snap.get("positions")
    else:
        payload = getattr(snap, "positions_json", None)
    return compute_net_debt_projection(parse_positions_payload(payload))
