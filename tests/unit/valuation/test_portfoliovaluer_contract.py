"""VIB-5206 [US-012]: PortfolioValuer projection contract invariant tests.

``almanak/framework/valuation/portfolio_valuer.py::PortfolioValuer`` is the documented
**single source of truth for portfolio valuation at runtime** (``portfolio_valuer.py:8``).
This module pins — without touching any read path — the projection conventions ratified in
blueprint 27 §7.11 and the VIB-5202 bypass inventory (``docs/internal/bypass-inventory-vib-5202.md``
§1 / §5): the signed-leg representation is the ONE canonical money representation, and every
projection (``total_value_usd``, ``deployed_capital_usd``, ``debt_mark``, NAV) is a
deterministic function of it.

Where the full ``PortfolioValuer`` needs gateway context to run, we assert against the
documented projection conventions over ``PositionValue`` legs constructed by hand (same
approach as ``tests/unit/dashboard/test_netting_parity.py``) AND drive the REAL dashboard
netting helper ``compute_net_debt_projection`` for the ``debt_mark`` term — so the NAV
invariant is anchored to production code, not a re-implementation.

Source conventions under test (verified against HEAD; do NOT re-derive):
  * ``total_value_usd`` = Σ ``value_usd`` over positions with ``value_usd > 0``, excluding
    wallet pseudo-positions, dropping debt legs (VIB-3614) — ``portfolio_valuer.py:751-762``.
  * ``deployed_capital_usd`` = Σ ``abs(cost_basis_usd)`` (GROSS) — ``portfolio_valuer.py:707-710``.
  * lending sign convention: BORROW ``value_usd = -debt_value_usd``; SUPPLY
    ``value_usd = net_value_usd`` — ``portfolio_valuer.py:2571-2574``.
  * NAV = ``total_value_usd - debt_mark`` where ``debt_mark`` = Σ |negative value_usd|
    (``compute_net_debt_projection``); ties to true net equity for the canonical
    separate-reserve shape (VIB-4983 / VIB-5201).

Economics (the VIB-5201 baseline): supply 10 wstETH @ $4000 = $40,000 collateral
(cost $39,000); borrow 8 WETH @ $4000 = $32,000 debt (cost $31,800).
  * true net-equity NAV  = $40,000 − $32,000 = $8,000
  * true net-equity cost = $39,000 − $31,800 = $7,200
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.portfolio.models import PositionValue
from almanak.framework.teardown.models import PositionType
from almanak.framework.valuation.net_debt import compute_net_debt_projection

# --- true economics (ground truth) -----------------------------------------
TRUE_NET_EQUITY_NAV = Decimal("8000")
TRUE_NET_EQUITY_COST = Decimal("7200")


def _valuer_total_value_usd(positions: list[PositionValue]) -> Decimal:
    """Mirror ``portfolio_valuer.py:751-762`` (VIB-3614): Σ positive ``value_usd`` only —
    negative debt legs are dropped. None of these fixtures are wallet pseudo-positions, so
    the valuer's ``PositionType.TOKEN``/wallet-overlap filter is a no-op here.
    """
    return sum((p.value_usd for p in positions if p.value_usd > 0), Decimal("0"))


def _valuer_deployed_capital_usd(positions: list[PositionValue]) -> Decimal:
    """Mirror ``portfolio_valuer.py:707-710``: Σ ``abs(cost_basis_usd)`` — GROSS; the
    ``abs()`` counts the borrow cost as a positive deployed amount.
    """
    return sum(
        (abs(p.cost_basis_usd) for p in positions if p.cost_basis_usd != Decimal("0")),
        Decimal("0"),
    )


def _canonical_separate_reserves() -> list[PositionValue]:
    """Canonical representation: separate SUPPLY (+collateral) and BORROW (-debt) legs,
    matching the valuer's lending sign convention (``portfolio_valuer.py:2571-2574``).
    """
    return [
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


def _net_supply_plus_borrow() -> list[PositionValue]:
    """The net-leg landmine: SUPPLY leg already net of debt (+8000) AND a separate BORROW
    leg (-32000). Emitting this shape silently breaks NAV (double-subtracts debt).
    """
    return [
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


def _nav_via_contract(positions: list[PositionValue]) -> Decimal:
    """The documented NAV contract (blueprint 27 §7.11, VIB-4983), using the REAL dashboard
    aggregation fn for the ``debt_mark`` term: ``nav = total_value_usd - debt_mark``.
    """
    _count, debt_mark, _debt_cost, _net_cost = compute_net_debt_projection(positions)
    return _valuer_total_value_usd(positions) - debt_mark


def test_total_value_usd_excludes_debt_legs():
    """Contract: ``total_value_usd`` drops negative (debt) legs (VIB-3614,
    ``portfolio_valuer.py:751-762``). A BORROW leg with negative ``value_usd`` must NOT
    raise the total — adding the loop's BORROW leg leaves ``total_value_usd`` at the
    collateral value alone.
    """
    positions = _canonical_separate_reserves()
    # Only the +40000 SUPPLY leg contributes; the -32000 BORROW leg is dropped.
    assert _valuer_total_value_usd(positions) == Decimal("40000")

    supply_only = [p for p in positions if p.position_type == PositionType.SUPPLY]
    # The debt leg added nothing: total is identical with or without it.
    assert _valuer_total_value_usd(positions) == _valuer_total_value_usd(supply_only)


def test_lending_sign_convention():
    """Contract: BORROW ``value_usd`` is negative; SUPPLY ``value_usd`` is the (positive)
    net value (``portfolio_valuer.py:2571-2574``). This is the canonical signed-leg
    representation every projection is derived from.
    """
    positions = _canonical_separate_reserves()
    borrow = next(p for p in positions if p.position_type == PositionType.BORROW)
    supply = next(p for p in positions if p.position_type == PositionType.SUPPLY)

    assert borrow.value_usd < 0
    assert supply.value_usd > 0


def test_nav_ties_to_net_equity_on_canonical_shape():
    """Load-bearing: NAV = ``total_value_usd - debt_mark`` ties to true net equity for the
    canonical separate-reserve shape (collateral +$40k, debt -$32k → NAV $8k).

    ``debt_mark`` (= Σ |negative value_usd|) is subtracted exactly once. Guards against any
    regression that breaks the NAV contract for the representation connectors actually
    persist.
    """
    positions = _canonical_separate_reserves()
    count, debt_mark, _debt_cost, _net_cost = compute_net_debt_projection(positions)

    assert count == 2
    assert debt_mark == Decimal("32000")  # Σ |negative value_usd|, subtracted once
    assert _nav_via_contract(positions) == TRUE_NET_EQUITY_NAV


def test_net_leg_shape_double_subtracts_debt():
    """Landmine guard (VIB-5201): the net-SUPPLY + separate-BORROW shape must NOT silently
    produce the correct NAV.

    The SUPPLY leg is already net (+8000) and the contract still subtracts the full
    ``debt_mark`` again → NAV double-subtracts to -24000. Pinning this exact wrong value
    means any future change that makes the unsafe shape "work" (produce $8,000) flips this
    test red, forcing a deliberate decision rather than silent convention drift.
    """
    positions = _net_supply_plus_borrow()
    _count, debt_mark, _debt_cost, _net_cost = compute_net_debt_projection(positions)

    assert debt_mark == Decimal("32000")
    assert _valuer_total_value_usd(positions) == Decimal("8000")  # already-net positive leg
    nav = _nav_via_contract(positions)
    assert nav == Decimal("-24000")
    assert nav != TRUE_NET_EQUITY_NAV  # the unsafe shape does NOT tie to net equity


def test_deployed_capital_usd_is_gross():
    """Contract: ``deployed_capital_usd`` = Σ ``abs(cost_basis_usd)`` is GROSS — it counts
    the borrow cost as a positive deployed amount (``portfolio_valuer.py:707-710``).

    For the canonical loop this is 39000 + 31800 = 70800, NOT the net-equity cost 7200.
    The divergence is by design (gross deployed capital vs net-equity cost basis); pinning
    it means a convention change to either is noticed.
    """
    positions = _canonical_separate_reserves()
    _count, _debt_mark, debt_cost, net_cost = compute_net_debt_projection(positions)

    # GROSS: borrow cost counted positive.
    assert _valuer_deployed_capital_usd(positions) == Decimal("70800")
    # The aggregation's net-equity cost is a DIFFERENT quantity.
    assert net_cost == TRUE_NET_EQUITY_COST
    assert debt_cost == Decimal("31800")
    # The two are not expected to agree — gross − net = 63600.
    assert _valuer_deployed_capital_usd(positions) - net_cost == Decimal("63600")
