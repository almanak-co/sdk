"""VIB-5217 [US-014]: read-only shadow-parity harness — PortfolioValuer
projections vs the AccountantTest DB-derived / dashboard-aggregation values.

This module is a **shadow / observation** layer. It swaps NO read path and
mutates nothing: it imports the REAL dashboard netting helper
(``quant_aggregations._net_from_position_items``) and the REAL valuer
wallet-exclusion predicates (``portfolio_valuer._is_swap_inventory_row`` /
``_token_overlaps_wallet_index`` / ``_build_wallet_match_index``), then diffs the
two projection conventions over a snapshot's typed positions. The output is an
enumerated discrepancy list — the input to US-015 (migrate one primitive's
netting behind the canonical contract).

It accepts positions from either source the AccountantTest itself consumes:

  * A live fixture SQLite DB's ``portfolio_snapshots.positions_json`` (the exact
    payload the AccountantTest scores) — :func:`positions_from_sqlite`.
  * Hand-constructed typed ``PositionValue`` legs grounded in the fixture
    economics (used when no round-trip DB is checked in) — see
    ``test_shadow_parity_portfoliovaluer.py``.

The two projection conventions under shadow (verified against HEAD; blueprint 27
§7.11, do NOT re-derive):

  * Valuer ``total_value_usd`` = Σ ``value_usd`` over positions with
    ``value_usd > 0``, excluding wallet pseudo-positions, INCLUDING swap-inventory
    lots, dropping debt legs (VIB-3614) — ``portfolio_valuer.py:751-762``.
  * Valuer ``deployed_capital_usd`` = Σ ``abs(cost_basis_usd)`` (GROSS) —
    ``portfolio_valuer.py:707-710``.
  * Valuer NAV (derived, not stamped) = ``total_value_usd - debt_mark`` where
    ``debt_mark`` = Σ |negative ``value_usd``| computed by the READ path
    ``_net_from_position_items`` — blueprint 27 §7.11, VIB-4983 / VIB-5201.
  * Dashboard / DB-derived aggregation = ``_net_from_position_items`` →
    ``(count, debt_mark, debt_cost, net_cost)`` where ``net_cost`` is signed
    net-equity cost (collateral cost − borrow cost), the dashboard's cost basis.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from almanak.framework.dashboard.quant_aggregations import (
    _net_from_position_items,
    _parse_positions_payload,
)
from almanak.framework.teardown.models import PositionType
from almanak.framework.valuation.portfolio_valuer import (
    _build_wallet_match_index,
    _is_swap_inventory_row,
    _token_overlaps_wallet_index,
)

ZERO = Decimal("0")


def _read_decimal(pos: Any, key: str) -> Decimal | None:
    """Read ``key`` off a typed ``PositionValue`` OR a ``positions_json`` dict as a
    Decimal, honoring Empty≠Zero (absent/empty/unparsable → ``None``).

    Mirrors ``quant_aggregations._read_position_decimal`` so the shadow side reads
    positions exactly as the production dashboard path does.
    """
    raw = pos.get(key) if isinstance(pos, dict) else getattr(pos, key, None)
    if raw is None or raw == "":
        return None
    try:
        return Decimal(str(raw))
    except Exception:
        return None


def _position_type(pos: Any) -> Any:
    return pos.get("position_type") if isinstance(pos, dict) else getattr(pos, "position_type", None)


def _is_wallet_pseudo_token(pos: Any, wallet_index: Any) -> bool:
    """Replicate the valuer's ``total_value_usd`` exclusion predicate
    (``portfolio_valuer.py:751-762``) using the REAL valuer helpers: a
    ``PositionType.TOKEN`` row that overlaps the wallet is excluded UNLESS it is a
    swap-inventory lot (those are deployed capital and count in).

    The valuer helpers are attribute-based (``position.position_type`` /
    ``position.details``); ``positions_from_sqlite`` yields ``dict`` rows, so
    normalize a dict into an attribute carrier first to avoid an
    ``AttributeError`` on the advertised ``positions_json`` path. ``PositionType``
    is a ``StrEnum``, so the serialized string compares equal to the enum member.
    """
    helper_pos = (
        SimpleNamespace(
            position_type=pos.get("position_type"),
            details=pos.get("details") or {},
        )
        if isinstance(pos, dict)
        else pos
    )
    if _is_swap_inventory_row(helper_pos):
        return False
    ptype = _position_type(pos)
    is_token = ptype == PositionType.TOKEN or ptype == "TOKEN"
    return bool(is_token and _token_overlaps_wallet_index(helper_pos, wallet_index))


@dataclass(frozen=True)
class Discrepancy:
    """One enumerated parity gap between the valuer projection and the
    DB-derived / aggregation value."""

    name: str
    valuer_value: Decimal
    db_derived_value: Decimal
    description: str

    @property
    def delta(self) -> Decimal:
        return self.valuer_value - self.db_derived_value

    @property
    def magnitude(self) -> Decimal:
        return abs(self.delta)


@dataclass
class ShadowParityResult:
    """The shadow comparison for one snapshot / scenario."""

    label: str
    primitive: str
    position_count: int
    # Valuer projections (portfolio_valuer.py).
    total_value_usd: Decimal
    deployed_capital_usd: Decimal
    debt_mark: Decimal
    nav: Decimal
    # DB-derived / dashboard aggregation (_net_from_position_items).
    agg_debt_mark: Decimal
    agg_debt_cost: Decimal
    agg_net_cost: Decimal
    # Optional independent ground-truth net equity (from the fixture economics).
    ground_truth_nav: Decimal | None = None
    discrepancies: list[Discrepancy] = field(default_factory=list)

    @property
    def max_magnitude(self) -> Decimal:
        return max((d.magnitude for d in self.discrepancies), default=ZERO)

    @property
    def has_discrepancy(self) -> bool:
        return any(d.magnitude != ZERO for d in self.discrepancies)


def _valuer_total_value_usd(positions: list[Any], wallet_index: Any) -> Decimal:
    """Σ positive ``value_usd`` excluding wallet pseudo-positions, including
    swap-inventory lots — ``portfolio_valuer.py:751-762`` (VIB-3614)."""
    total = ZERO
    for pos in positions:
        value = _read_decimal(pos, "value_usd")
        if value is None or value <= 0:
            continue
        if _is_wallet_pseudo_token(pos, wallet_index):
            continue
        total += value
    return total


def _valuer_deployed_capital_usd(positions: list[Any]) -> Decimal:
    """Σ ``abs(cost_basis_usd)`` over non-zero-basis legs — GROSS;
    ``portfolio_valuer.py:707-710``."""
    total = ZERO
    for pos in positions:
        cost = _read_decimal(pos, "cost_basis_usd")
        if cost is None or cost == 0:
            continue
        total += abs(cost)
    return total


def compute_shadow_parity(
    label: str,
    primitive: str,
    positions: list[Any],
    *,
    wallet_balances: list[Any] | None = None,
    ground_truth_nav: Decimal | None = None,
) -> ShadowParityResult:
    """Diff the valuer projections against the DB-derived aggregation over one
    snapshot's typed positions. Pure / read-only — touches no read path.

    ``positions`` may be typed ``PositionValue`` dataclasses (the production
    ``PortfolioSnapshot.positions``) and/or ``positions_json`` dicts (the DB
    payload the AccountantTest scores) — both are accepted, mirroring
    ``_net_from_position_items``.
    """
    wallet_index = _build_wallet_match_index(wallet_balances or [])

    total_value_usd = _valuer_total_value_usd(positions, wallet_index)
    deployed_capital_usd = _valuer_deployed_capital_usd(positions)

    # REAL dashboard read-path aggregation for the debt-netting terms.
    count, agg_debt_mark, agg_debt_cost, agg_net_cost = _net_from_position_items(positions)

    # Valuer NAV is derived, never stamped: total_value_usd − debt_mark
    # (blueprint 27 §7.11). The debt_mark term comes from the read path above.
    nav = total_value_usd - agg_debt_mark

    discrepancies: list[Discrepancy] = [
        Discrepancy(
            name="cost_basis_gross_vs_net",
            valuer_value=deployed_capital_usd,
            db_derived_value=agg_net_cost,
            description=(
                "Valuer deployed_capital_usd is GROSS (Σ|cost|, borrow cost counted "
                "positive); the dashboard aggregation net_cost is net-equity cost "
                "(collateral cost − borrow cost). Diverge by 2×debt_cost whenever a "
                "debt leg exists."
            ),
        ),
        Discrepancy(
            name="gross_total_vs_nav",
            valuer_value=total_value_usd,
            db_derived_value=nav,
            description=(
                "total_value_usd drops debt legs (VIB-3614) but does NOT subtract "
                "debt_mark; a consumer reading the stamped total_value_usd as NAV "
                "overstates net equity by debt_mark (the read-path subtraction is the "
                "only thing that re-nets the loop)."
            ),
        ),
    ]
    if ground_truth_nav is not None:
        discrepancies.append(
            Discrepancy(
                name="nav_vs_ground_truth",
                valuer_value=nav,
                db_derived_value=ground_truth_nav,
                description=(
                    "Derived NAV (total_value_usd − debt_mark) vs the fixture's true "
                    "net equity. Zero for the canonical separate-reserve shape and for "
                    "no-debt primitives; catastrophic for the net-leg landmine "
                    "(double-subtracted debt)."
                ),
            )
        )

    return ShadowParityResult(
        label=label,
        primitive=primitive,
        position_count=count,
        total_value_usd=total_value_usd,
        deployed_capital_usd=deployed_capital_usd,
        debt_mark=agg_debt_mark,
        nav=nav,
        agg_debt_mark=agg_debt_mark,
        agg_debt_cost=agg_debt_cost,
        agg_net_cost=agg_net_cost,
        ground_truth_nav=ground_truth_nav,
        discrepancies=discrepancies,
    )


def positions_from_sqlite(
    db_path: str | Path,
    *,
    deployment_id: str | None = None,
) -> list[dict[str, Any]]:
    """Read the latest ``portfolio_snapshots.positions_json`` payload from a real
    round-trip fixture DB and return its bare position list (dicts).

    Read-only: opens the SQLite file, SELECTs the most recent snapshot, and
    unwraps the payload with the REAL ``_parse_positions_payload``. Used when a
    fixture DB is present (live Anvil/mainnet round-trip); returns ``[]`` when the
    table/column/payload is absent. This is the production-realistic shadow path —
    the same payload the AccountantTest scores.
    """
    path = Path(db_path)
    if not path.exists():
        return []
    # ``as_uri()`` URL-encodes spaces / special chars so paths like
    # ``/tmp/a b?c.db`` open read-only instead of corrupting the URI.
    conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        conn.row_factory = sqlite3.Row
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(portfolio_snapshots)")}
        if "positions_json" not in cols:
            return []
        where = ""
        params: tuple[Any, ...] = ()
        if deployment_id and "deployment_id" in cols:
            where = "WHERE deployment_id = ?"
            params = (deployment_id,)
        order = "timestamp DESC" if "timestamp" in cols else "rowid DESC"
        row = conn.execute(
            f"SELECT positions_json FROM portfolio_snapshots {where} ORDER BY {order} LIMIT 1",  # noqa: S608
            params,
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return []
    # ``_parse_positions_payload`` natively unwraps a JSON string OR an
    # already-deserialized list/dict, so pass the raw column through directly.
    try:
        return _parse_positions_payload(row["positions_json"])
    except Exception:
        return []
