"""VIB-5278 (closed by VIB-5857) — the degraded fallback snapshot must stamp the
POSITIVE-scoped ``total_value_usd``, never the signed (debt-netted) summary total.

``IntentStrategy.get_portfolio_snapshot`` is the runner's substitution when the
canonical ``PortfolioValuer`` path is unavailable. It used to stamp
``position_summary.total_value_usd`` — a SIGNED sum that already nets negative
BORROW legs — into a column whose contract is Σ POSITIVE ``value_usd`` with the
debt dropped (VIB-3614, blueprint 27 §7.11). While every consumer read the
column gross-of-debt that divergence was latent; the moment a consumer applies
the canonical ``NAV = total_value_usd − debt_mark`` netting (VIB-5857,
``accountant_test._snapshot_equity``), a net-shaped fallback row has its debt
subtracted TWICE. This is the highest-risk interaction the VIB-5857 consumer
sweep found, so it is pinned here with the double-subtract arithmetic explicit.

Harness pattern follows ``test_perp_net_equity_vib5252.py``'s Site D class
(the same fallback entry point, driven through a MagicMock strategy).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

from almanak.framework.teardown.models import PositionType
from almanak.framework.valuation.net_debt import net_debt_from_snapshot, wallet_nav_usd


def _leg(position_type: PositionType, value_usd: Decimal, protocol: str = "aave_v3") -> MagicMock:
    leg = MagicMock()
    leg.position_type = position_type
    leg.protocol = protocol
    leg.chain = "arbitrum"
    leg.value_usd = value_usd
    leg.details = {}
    return leg


def _make_strategy(positions, signed_total: Decimal) -> MagicMock:
    from almanak.framework.strategies.intent_strategy import IntentStrategy

    strat = MagicMock(spec=IntentStrategy)
    strat.deployment_id = "test-strat"
    strat._chain = "arbitrum"
    strat.chain = "arbitrum"
    summary = MagicMock()
    summary.positions = positions
    summary.total_value_usd = signed_total
    strat.get_open_positions.return_value = summary
    strat._get_tracked_tokens.return_value = []
    strat._append_native_gas_to_wallet.return_value = ("unknown_chain", Decimal("0"))
    return strat


def _snapshot(strat):
    from almanak.framework.strategies.intent_strategy import IntentStrategy

    market = MagicMock()
    market.chains = ()
    return IntentStrategy.get_portfolio_snapshot(strat, market)


def test_debt_leg_dropped_from_total_but_kept_in_positions():
    """Supply +500, borrow −300: the column is 500 (positive-scoped), while the
    signed −300 leg survives in ``positions`` so ``debt_mark`` still finds the
    debt in its one canonical place."""
    strat = _make_strategy(
        [_leg(PositionType.SUPPLY, Decimal("500")), _leg(PositionType.BORROW, Decimal("-300"))],
        signed_total=Decimal("200"),
    )
    snapshot = _snapshot(strat)

    assert snapshot.total_value_usd == Decimal("500")
    assert sorted(p.value_usd for p in snapshot.positions) == [Decimal("-300"), Decimal("500")]


def test_canonical_netting_over_the_fallback_row_does_not_double_subtract():
    """The end-to-end arithmetic this file exists for. Pre-fix the row carried
    total=200 (already net) beside a −300 leg, so the canonical netting read
    200 − 300 = −100 — the double-subtract. Post-fix: 500 − 300 = 200, the true
    net equity."""
    strat = _make_strategy(
        [_leg(PositionType.SUPPLY, Decimal("500")), _leg(PositionType.BORROW, Decimal("-300"))],
        signed_total=Decimal("200"),
    )
    snapshot = _snapshot(strat)

    _count, debt_mark, _debt_cost, _net_cost = net_debt_from_snapshot(snapshot)
    assert debt_mark == Decimal("300")
    nav = wallet_nav_usd(snapshot.total_value_usd, debt_mark, snapshot.available_cash_usd)
    assert nav == Decimal("200")
    assert nav != Decimal("-100"), "double-subtract shape — the fallback stamped a net total again"


def test_debt_free_summary_is_byte_identical():
    """No negative leg: positive-scoped sum == signed total, so ordinary
    strategies are unchanged by the VIB-5278 closure."""
    strat = _make_strategy(
        [_leg(PositionType.SUPPLY, Decimal("500")), _leg(PositionType.LP, Decimal("250"), protocol="uniswap_v3")],
        signed_total=Decimal("750"),
    )
    snapshot = _snapshot(strat)
    assert snapshot.total_value_usd == Decimal("750")


def test_positive_magnitude_borrow_leg_is_sign_normalised():
    """THE SHAPE PRODUCTION ACTUALLY EMITS. Strategies report debt as a positive
    magnitude — ``benqi_looping`` appends its BORROW leg under
    ``if self._debt_usdc > _DUST_USD``, ``morpho_looping`` under
    ``if live.debt_value_usd > dust_usd`` — so the fallback receives +300, not
    −300. The canonical valuer negates it ("framework negates"); this fallback
    must too, or the leg is persisted positive, ``debt_mark`` (Σ|negative|) finds
    nothing, and the row reports ``supply + debt`` as NAV.

    Every other test in this file feeds an already-negative leg, which is the one
    shape this path does NOT receive from a looping strategy.
    """
    strat = _make_strategy(
        [_leg(PositionType.SUPPLY, Decimal("500")), _leg(PositionType.BORROW, Decimal("300"))],
        signed_total=Decimal("800"),
    )
    snapshot = _snapshot(strat)

    assert sorted(p.value_usd for p in snapshot.positions) == [Decimal("-300"), Decimal("500")]
    assert snapshot.total_value_usd == Decimal("500")
    assert snapshot.total_value_usd != Decimal("800"), "debt booked as an asset — the sign was copied through"

    _count, debt_mark, _debt_cost, _net_cost = net_debt_from_snapshot(snapshot)
    assert debt_mark == Decimal("300"), "a positive debt leg nets nothing — NAV would read gross-of-debt"
    nav = wallet_nav_usd(snapshot.total_value_usd, debt_mark, snapshot.available_cash_usd)
    assert nav == Decimal("200")
    assert nav != Decimal("800"), "pre-fix shape: supply + debt reported as equity"


def test_zero_and_absent_borrow_values_stay_unmeasured():
    """Empty ≠ Zero. A BORROW leg with no measurement must not be turned into a
    measured ``-0`` by the normalisation, and must not invent debt."""
    strat = _make_strategy(
        [
            _leg(PositionType.SUPPLY, Decimal("500")),
            _leg(PositionType.BORROW, Decimal("0")),
            _leg(PositionType.BORROW, None, protocol="spark"),
        ],
        signed_total=Decimal("500"),
    )
    snapshot = _snapshot(strat)

    borrow_values = [p.value_usd for p in snapshot.positions if p.position_type == PositionType.BORROW]
    assert borrow_values == [Decimal("0"), None]
    assert snapshot.total_value_usd == Decimal("500")
    _count, debt_mark, _debt_cost, _net_cost = net_debt_from_snapshot(snapshot)
    assert debt_mark == Decimal("0")


def test_perp_exclusion_still_composes_with_debt_drop():
    """VIB-5252's perp-notional exclusion and VIB-5278's debt drop act on the
    same accumulator: a perp leg is skipped entirely (and degrades confidence),
    a debt leg is kept as a signed position but dropped from the total."""
    from almanak.framework.portfolio.models import ValueConfidence

    strat = _make_strategy(
        [
            _leg(PositionType.PERP, Decimal("10000"), protocol="gmx_v2"),  # notional — excluded
            _leg(PositionType.SUPPLY, Decimal("500")),
            _leg(PositionType.BORROW, Decimal("-300")),
        ],
        signed_total=Decimal("10200"),
    )
    snapshot = _snapshot(strat)

    assert snapshot.total_value_usd == Decimal("500")
    assert snapshot.value_confidence == ValueConfidence.ESTIMATED
    assert all(p.position_type != PositionType.PERP for p in snapshot.positions)
