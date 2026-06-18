"""VIB-5222 (US-015) + VIB-5225 (US-016): canonical net-debt projection + parity.

The lending primitive's NAV / cost / PnL / drawdown netting was lifted out of the
dashboard (``quant_aggregations._net_from_position_items``) into the canonical
valuation layer (``valuation/net_debt.py::compute_net_debt_projection``) — the home
that owns the PortfolioValuer projection contract (blueprint 27 §7.11). US-015 made the
dashboard helpers delegating shims; US-016 then DELETED those shims and moved the typed
position-sourcing accessors (``net_debt_from_snapshot`` / ``net_debt_from_positions_json``)
into the canonical module, so there is exactly ONE netting implementation. This test
proves:

  1. The dashboard module no longer defines duplicate netting helpers, and the canonical
     accessors route through the single ``compute_net_debt_projection`` (one copy of the
     math, not a second).
  2. The canonical projection holds the VIB-5201 leveraged baseline (collateral +$40k /
     debt −$32k → NAV $8k, net-equity cost $7,200), the net-leg landmine, and the LP/perp
     zero-discrepancy controls.
  3. Empty≠Zero discipline (unmeasured legs skipped; debt with absent cost still nets
     ``debt_mark``) and the MeasuredMoney-seeded zero aggregate survive the move.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.dashboard import quant_aggregations
from almanak.framework.portfolio.models import PositionValue
from almanak.framework.teardown.models import PositionType
from almanak.framework.valuation.net_debt import (
    compute_net_debt_projection,
    net_debt_from_snapshot,
)

# VIB-5201 leveraged baseline economics.
_LEVERAGED = [
    PositionValue(
        position_type=PositionType.SUPPLY,
        protocol="aave_v3",
        chain="arbitrum",
        value_usd=Decimal("40000"),
        label="aave wstETH supply",
        cost_basis_usd=Decimal("39000"),
    ),
    PositionValue(
        position_type=PositionType.BORROW,
        protocol="aave_v3",
        chain="arbitrum",
        value_usd=Decimal("-32000"),
        label="aave WETH borrow",
        cost_basis_usd=Decimal("-31800"),
    ),
]


def _total_value_usd(positions: list[PositionValue]) -> Decimal:
    """Mirror portfolio_valuer.py (VIB-3614): Σ positive value_usd (debt dropped)."""
    return sum((p.value_usd for p in positions if p.value_usd > 0), Decimal("0"))


def test_dashboard_has_no_duplicate_netting_helpers():
    """US-016: the dashboard's duplicate netting wrappers are DELETED — the netting
    math + accessors live only in ``valuation/net_debt``. The typed accessors route
    through the single ``compute_net_debt_projection`` (not a second copy)."""
    for deleted in (
        "_net_from_position_items",
        "_snapshot_net_debt",
        "_open_positions_and_net_debt",
        "_parse_positions_payload",
        "_read_position_decimal",
    ):
        assert not hasattr(quant_aggregations, deleted), f"{deleted} should be deleted (US-016)"
    # The snapshot accessor delegates to the canonical math: a typed-positions snapshot
    # nets identically to calling the projection on its positions directly.
    snap = type("Snap", (), {"positions": _LEVERAGED})()
    assert net_debt_from_snapshot(snap) == compute_net_debt_projection(_LEVERAGED)


def test_canonical_matches_dashboard_on_leveraged_baseline():
    """Canonical projection + NAV $8k / net-cost $7,200 on the leveraged loop."""
    canonical = compute_net_debt_projection(_LEVERAGED)

    count, debt_mark, debt_cost, net_cost = canonical
    assert count == 2
    assert debt_mark == Decimal("32000")
    assert debt_cost == Decimal("31800")
    # Net-equity cost (collateral cost − borrow cost), not the gross writer convention.
    assert net_cost == Decimal("7200")
    # NAV contract: total_value_usd − debt_mark ties to true net equity.
    assert _total_value_usd(_LEVERAGED) - debt_mark == Decimal("8000")


def test_canonical_matches_dashboard_on_net_leg_landmine():
    """The net-SUPPLY + separate-BORROW landmine still double-subtracts (parity
    preserved): the move must not silently 'fix' the unsafe shape."""
    positions = [
        PositionValue(
            position_type=PositionType.SUPPLY,
            protocol="aave_v3",
            chain="arbitrum",
            value_usd=Decimal("8000"),
            label="aave loop (net)",
            cost_basis_usd=Decimal("7200"),
        ),
        PositionValue(
            position_type=PositionType.BORROW,
            protocol="aave_v3",
            chain="arbitrum",
            value_usd=Decimal("-32000"),
            label="aave WETH borrow",
            cost_basis_usd=Decimal("-31800"),
        ),
    ]
    canonical = compute_net_debt_projection(positions)
    snap = type("Snap", (), {"positions": positions})()
    assert canonical == net_debt_from_snapshot(snap)
    _count, debt_mark, _debt_cost, _net_cost = canonical
    # total_value_usd is the already-net 8000; subtracting debt again = -24000.
    assert _total_value_usd(positions) - debt_mark == Decimal("-24000")


def test_lp_perp_zero_discrepancy_controls():
    """LP / perp carry no negative leg → debt_mark 0, projection byte-identical."""
    for value, cost in ((Decimal("4"), Decimal("4")), (Decimal("5"), Decimal("5"))):
        positions = [
            PositionValue(
                position_type=PositionType.LP,
                protocol="uniswap_v3",
                chain="arbitrum",
                value_usd=value,
                label="single positive leg",
                cost_basis_usd=cost,
            )
        ]
        count, debt_mark, debt_cost, net_cost = compute_net_debt_projection(positions)
        assert (count, debt_mark, debt_cost, net_cost) == (1, Decimal("0"), Decimal("0"), cost)
        snap = type("Snap", (), {"positions": positions})()
        assert net_debt_from_snapshot(snap) == compute_net_debt_projection(positions)


def test_empty_aggregate_is_measured_zero():
    """An empty position set yields a measured-zero aggregate (MeasuredMoney seed)."""
    assert compute_net_debt_projection([]) == (0, Decimal("0"), Decimal("0"), Decimal("0"))


def test_empty_not_zero_skips_unmeasured_value_leg():
    """A leg with absent/unparsable value_usd is skipped (unmeasured ≠ measured zero)."""
    positions = [{"value_usd": "", "cost_basis_usd": "100"}]
    assert compute_net_debt_projection(positions) == (1, Decimal("0"), Decimal("0"), Decimal("0"))


def test_debt_leg_with_absent_cost_still_nets_debt_mark():
    """A measured debt value with absent cost nets debt_mark but contributes no cost."""
    positions = [{"value_usd": "-500", "cost_basis_usd": ""}]
    count, debt_mark, debt_cost, net_cost = compute_net_debt_projection(positions)
    assert count == 1
    assert debt_mark == Decimal("500")
    assert debt_cost == Decimal("0")
    assert net_cost == Decimal("0")
