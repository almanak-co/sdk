"""VIB-5201: net-debt parity contract test (graduated from the Deliverable-#1
parity harness, discussion 2026-06-17).

This pins the debt-netting convention in the accounting/dashboard layer by
driving the REAL aggregation function
``almanak.framework.dashboard.quant_aggregations._net_from_position_items`` over
an Aave-style leveraged loop expressed in the two representations a connector
could persist, and diffing NAV against the documented contract.

Economics under test — supply 10 wstETH @ $4000 = $40,000 collateral (cost
$39,000), borrow 8 WETH @ $4000 = $32,000 debt (cost $31,800):

  * true net-equity NAV  = collateral value − debt value = $40,000 − $32,000 = $8,000
  * true net-equity cost = collateral cost  − debt cost  = $39,000 − $31,800 = $7,200

Two representations:

  * Representation 1 (CANONICAL, separate reserves): SUPPLY value_usd=+40000,
    BORROW value_usd=-32000. The NAV contract (VIB-4983)
    ``nav = total_value_usd - debt_mark`` reads $8,000 — it AGREES. This is the
    load-bearing parity assertion.
  * Representation 2 (net SUPPLY leg + ALSO a separate BORROW leg): SUPPLY
    value_usd=+8000 (already net of debt) + BORROW value_usd=-32000. The same
    contract double-subtracts debt → NAV = -24000. This is the landmine: a
    regression that makes this shape silently produce the correct $8,000 (or one
    that breaks canonical parity) must fail this test.

NAV contract facts (do NOT re-derive — verified against source):
  * ``_net_from_position_items(positions)`` → ``(count, debt_mark, debt_cost, net_cost)``.
    ``debt_mark`` = Σ|negative value_usd|; ``net_cost`` = signed net-equity cost;
    legs with absent value_usd are skipped (Empty≠Zero).
  * ``nav = total_value_usd - debt_mark`` where ``total_value_usd`` = Σ positive
    value_usd (portfolio_valuer.py:746-757, VIB-3614 drops debt legs).
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.dashboard.quant_aggregations import _net_from_position_items
from almanak.framework.portfolio.models import PositionValue
from almanak.framework.teardown.models import PositionType

# --- true economics (ground truth) -----------------------------------------
TRUE_NET_EQUITY_NAV = Decimal("8000")
TRUE_NET_EQUITY_COST = Decimal("7200")


def _valuer_total_value_usd(positions: list[PositionValue]) -> Decimal:
    """Mirror portfolio_valuer.py:746-757 (VIB-3614): Σ positive value_usd only
    (negative debt legs are dropped). None of these legs are wallet
    pseudo-positions, so the valuer's wallet/inventory filter is a no-op here.
    """
    return sum((p.value_usd for p in positions if p.value_usd > 0), Decimal("0"))


def _valuer_deployed_capital_usd(positions: list[PositionValue]) -> Decimal:
    """Mirror portfolio_valuer.py:702-705: Σ abs(cost_basis_usd) — GROSS: the
    abs() counts the borrow cost as a positive deployed amount.
    """
    return sum(
        (abs(p.cost_basis_usd) for p in positions if p.cost_basis_usd != Decimal("0")),
        Decimal("0"),
    )


def _canonical_separate_reserves() -> list[PositionValue]:
    """Representation 1: separate SUPPLY (+collateral) and BORROW (-debt) legs."""
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
    """Representation 2: SUPPLY leg already net of debt (+8000) AND a separate
    BORROW leg (-32000) — the double-representation landmine.
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
    """The documented NAV contract (VIB-4983), using the REAL aggregation fn for
    the debt_mark term: ``nav = total_value_usd - debt_mark``.
    """
    _count, debt_mark, _debt_cost, _net_cost = _net_from_position_items(positions)
    return _valuer_total_value_usd(positions) - debt_mark


def test_canonical_separate_reserves_nav_parity():
    """Load-bearing: the canonical separate-reserve shape ties to true net equity.

    Guards against any regression that breaks the ``total_value_usd - debt_mark``
    NAV contract for the shape connectors actually persist.
    """
    positions = _canonical_separate_reserves()
    count, debt_mark, _debt_cost, _net_cost = _net_from_position_items(positions)

    assert count == 2
    # debt_mark = Σ|negative value_usd| = $32,000, subtracted exactly once.
    assert debt_mark == Decimal("32000")
    assert _valuer_total_value_usd(positions) == Decimal("40000")
    assert _nav_via_contract(positions) == TRUE_NET_EQUITY_NAV


def test_net_supply_plus_borrow_double_subtracts_debt():
    """Landmine guard: the net-SUPPLY + separate-BORROW shape must NOT silently
    produce the correct NAV.

    Because the SUPPLY leg is already net of debt (+8000) and the contract still
    subtracts the full debt_mark again, NAV double-subtracts to -24000. Pinning
    this exact wrong value means a future change that makes this shape "work"
    silently (i.e. produce $8,000) flips this test red, forcing a deliberate
    decision rather than a silent convention drift.
    """
    positions = _net_supply_plus_borrow()
    _count, debt_mark, _debt_cost, _net_cost = _net_from_position_items(positions)

    assert debt_mark == Decimal("32000")
    # Σ positive value_usd is the already-net 8000; subtracting debt again = -24000.
    assert _valuer_total_value_usd(positions) == Decimal("8000")
    nav = _nav_via_contract(positions)
    assert nav == Decimal("-24000")
    # The unsafe shape does NOT equal true net equity — that's the whole point.
    assert nav != TRUE_NET_EQUITY_NAV


def test_cost_basis_representations_pinned():
    """Document & pin the known cost-basis divergence (lightweight, non-blocking
    in intent but enforced as a contract): for the canonical loop the two paths
    report DIFFERENT cost bases on purpose —

      * aggregation ``net_cost`` = signed net-equity cost  = $7,200
      * valuer ``deployed_capital_usd`` = gross Σ|cost|     = $70,800

    These represent different quantities (net equity cost vs gross deployed
    capital); this test exists so a future change to either convention is
    noticed rather than silently merged.
    """
    positions = _canonical_separate_reserves()
    _count, _debt_mark, debt_cost, net_cost = _net_from_position_items(positions)

    # aggregation: signed net-equity cost = 39000 - 31800
    assert net_cost == TRUE_NET_EQUITY_COST
    # aggregation also exposes the debt magnitude separately.
    assert debt_cost == Decimal("31800")
    # valuer: gross deployed capital counts borrow cost as positive = 39000 + 31800
    assert _valuer_deployed_capital_usd(positions) == Decimal("70800")
    # the divergence the parity harness surfaced (gross − net).
    assert _valuer_deployed_capital_usd(positions) - net_cost == Decimal("63600")
