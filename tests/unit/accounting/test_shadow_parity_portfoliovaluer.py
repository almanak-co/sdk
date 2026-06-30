"""VIB-5217 [US-014]: shadow-parity of PortfolioValuer projections vs the
AccountantTest round-trip fixtures' DB-derived / dashboard-aggregation values.

READ-ONLY shadow. This test swaps NO read path and changes no behavior: it drives
:mod:`tests.unit.accounting.shadow_parity_harness`, which diffs the two projection
conventions (valuer vs ``net_debt.compute_net_debt_projection``) over each primitive's typed
positions, and PINS the enumerated discrepancies so a convention drift on either
side flips red. The enumerated list is the input to US-015 (migrate one
primitive's netting behind the canonical contract).

Why hand-constructed legs (no live DB). The Accountant fixtures
(``strategies/accounting/{lp,looping,perp}/``) produce a SQLite DB only after a
real round-trip (Anvil/mainnet); none is checked into the repo. The harness DOES
support a live DB via ``positions_from_sqlite`` (the production-realistic path —
the exact ``portfolio_snapshots.positions_json`` the AccountantTest scores), but
for a deterministic, no-fork CI check we construct each primitive's end-state as
typed ``PositionValue`` legs grounded in the fixture ``config.json`` economics:

  * LP        — ``total_value_usd=$4.0``  (lp/config.json)               → one positive LP leg, no debt.
  * Looping   — ``starting_collateral_usd=$4.0``, ``target_ltv=0.30``    → SUPPLY +$5.20 / BORROW −$1.20.
  * Perp      — ``collateral_amount=$5.0``, ``leverage=2.0``             → one positive PERP leg, no debt.

Plus the two ratified reference shapes from blueprint 27 §7.11 / VIB-5201:

  * Canonical loop — supply 10 wstETH @ $4000 / borrow 8 WETH @ $4000 (the
    separate-reserve shape connectors persist; NAV ties to $8,000).
  * Net-leg landmine — net SUPPLY +$8000 AND a separate BORROW −$32000 (the unsafe
    shape that silently double-subtracts debt → NAV −$24,000 vs true $8,000).

The three enumerated discrepancy classes (see report
``docs/internal/accounting/shadow-parity-vib-5217.md``):

  1. ``cost_basis_gross_vs_net``  — valuer ``deployed_capital_usd`` (GROSS) minus
     aggregation ``net_cost`` (net equity) = 2×debt_cost when a debt leg exists.
  2. ``gross_total_vs_nav``       — valuer ``total_value_usd`` (debt dropped, NOT
     debt-subtracted) minus derived NAV = ``debt_mark``.
  3. ``nav_vs_ground_truth``      — derived NAV minus the fixture's true net equity;
     zero for canonical / no-debt, catastrophic for the net-leg landmine.

LP and perp carry ZERO discrepancy (no debt leg). Looping/lending is the ONLY
primitive with parity gaps — so US-015 should migrate the lending netting first.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from almanak.framework.portfolio.models import PositionValue
from almanak.framework.teardown.models import PositionType
from tests.unit.accounting.shadow_parity_harness import (
    ShadowParityResult,
    compute_shadow_parity,
)

ZERO = Decimal("0")


# --------------------------------------------------------------------------- #
# Fixture-grounded primitive end-states (typed positions a snapshot carries).
# --------------------------------------------------------------------------- #
def _lp_fixture_positions() -> list[PositionValue]:
    """lp/config.json: total_value_usd=$4.0. Held end-state = one Uniswap V3 LP
    position holding both tokens; cost basis = capital deployed. No debt leg."""
    return [
        PositionValue(
            position_type=PositionType.LP,
            protocol="uniswap_v3",
            chain="arbitrum",
            value_usd=Decimal("4.00"),
            label="USDC/WETH LP",
            cost_basis_usd=Decimal("4.00"),
        ),
    ]


def _looping_fixture_positions() -> list[PositionValue]:
    """looping/config.json: starting_collateral_usd=$4.0, target_ltv=0.30, borrow
    USDT. Loop = supply $4.0 USDC, borrow $1.20 USDT, swap→USDC, re-supply $1.20 →
    aggregated SUPPLY USDC +$5.20 / BORROW USDT −$1.20. True net equity = $4.00."""
    return [
        PositionValue(
            position_type=PositionType.SUPPLY,
            protocol="aave_v3",
            chain="arbitrum",
            value_usd=Decimal("5.20"),
            label="aave USDC supply",
            cost_basis_usd=Decimal("5.20"),
        ),
        PositionValue(
            position_type=PositionType.BORROW,
            protocol="aave_v3",
            chain="arbitrum",
            value_usd=Decimal("-1.20"),
            label="aave USDT borrow",
            cost_basis_usd=Decimal("-1.20"),
        ),
    ]


def _perp_fixture_positions() -> list[PositionValue]:
    """perp/config.json: collateral_amount=$5.0, leverage=2.0 (notional $10). True
    net equity = $5.00 (collateral + uPnL − fees, §7.4); this fixture pins that
    POST-fix target shape — ONE positive PERP leg at net equity, notional off the
    balance sheet.

    Note (VIB-5254/VIB-5252): production did NOT emit this shape before VIB-5252.
    The strategy reported ``value_usd = collateral × leverage`` (notional, $10),
    and the merge discarded the only repriceable leg, so the original
    "zero-discrepancy control" was VOID — it passed only because this hand-built
    fixture already carried net equity. VIB-5252 makes the on-chain discovery
    path emit net equity in production, so this fixture is now a faithful target,
    not a fiction. Do NOT raise value_usd to the notional to "match" the old
    production bug."""
    return [
        PositionValue(
            position_type=PositionType.PERP,
            protocol="gmx_v2",
            chain="arbitrum",
            value_usd=Decimal("5.00"),
            label="GMX ETH/USD long",
            cost_basis_usd=Decimal("5.00"),
        ),
    ]


def _canonical_loop_positions() -> list[PositionValue]:
    """Blueprint 27 §7.11 / VIB-5201 ratified yardstick: supply 10 wstETH @ $4000 =
    $40,000 (cost $39,000); borrow 8 WETH @ $4000 = $32,000 (cost $31,800).
    Separate-reserve shape — NAV ties to true net equity $8,000."""
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


def _net_leg_landmine_positions() -> list[PositionValue]:
    """The unsafe shape (VIB-5201): SUPPLY leg already net of debt (+$8000) AND a
    separate BORROW leg (−$32000). True net equity is still $8,000, but the
    contract double-subtracts debt → NAV −$24,000."""
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


def _all_results() -> list[ShadowParityResult]:
    return [
        compute_shadow_parity("LP (uniswap_v3, $4.0)", "lp", _lp_fixture_positions(), ground_truth_nav=Decimal("4.00")),
        compute_shadow_parity(
            "Looping (aave_v3, $4.0 @ 0.30 LTV)",
            "looping",
            _looping_fixture_positions(),
            ground_truth_nav=Decimal("4.00"),
        ),
        compute_shadow_parity(
            "Perp (gmx_v2, $5.0 @ 2x)", "perp", _perp_fixture_positions(), ground_truth_nav=Decimal("5.00")
        ),
        compute_shadow_parity(
            "Looping canonical (blueprint §7.11)",
            "looping",
            _canonical_loop_positions(),
            ground_truth_nav=Decimal("8000"),
        ),
        compute_shadow_parity(
            "Looping net-leg LANDMINE",
            "looping",
            _net_leg_landmine_positions(),
            ground_truth_nav=Decimal("8000"),
        ),
    ]


# --------------------------------------------------------------------------- #
# Per-primitive pinned discrepancies.
# --------------------------------------------------------------------------- #
def _disc(result: ShadowParityResult, name: str) -> Decimal:
    return next(d.delta for d in result.discrepancies if d.name == name)


def test_lp_has_zero_discrepancy():
    """LP: single positive leg, no debt → valuer and aggregation agree exactly."""
    r = compute_shadow_parity("lp", "lp", _lp_fixture_positions(), ground_truth_nav=Decimal("4.00"))
    assert r.total_value_usd == Decimal("4.00")
    assert r.debt_mark == ZERO
    assert r.nav == Decimal("4.00")
    assert _disc(r, "cost_basis_gross_vs_net") == ZERO
    assert _disc(r, "gross_total_vs_nav") == ZERO
    assert _disc(r, "nav_vs_ground_truth") == ZERO
    assert not r.has_discrepancy


def test_perp_has_zero_discrepancy():
    """Perp: leverage/notional is off-balance-sheet (value_usd = collateral+uPnL,
    not notional); single positive leg, no debt → parity holds exactly."""
    r = compute_shadow_parity("perp", "perp", _perp_fixture_positions(), ground_truth_nav=Decimal("5.00"))
    assert r.total_value_usd == Decimal("5.00")
    assert r.debt_mark == ZERO
    assert r.nav == Decimal("5.00")
    assert not r.has_discrepancy


def test_looping_fixture_discrepancies_pinned():
    """Looping ($4.0 @ 0.30 LTV): the ONLY primitive with parity gaps.

    * cost_basis_gross_vs_net = deployed(gross $6.40) − net_cost($4.00) = $2.40 = 2×debt_cost
    * gross_total_vs_nav      = total($5.20) − NAV($4.00) = $1.20 = debt_mark
    * nav_vs_ground_truth     = $0 (canonical separate-reserve shape is correct)
    """
    r = compute_shadow_parity("looping", "looping", _looping_fixture_positions(), ground_truth_nav=Decimal("4.00"))
    assert r.total_value_usd == Decimal("5.20")
    assert r.deployed_capital_usd == Decimal("6.40")  # GROSS: 5.20 + 1.20
    assert r.debt_mark == Decimal("1.20")
    assert r.agg_net_cost == Decimal("4.00")  # net equity: 5.20 − 1.20
    assert r.agg_debt_cost == Decimal("1.20")
    assert r.nav == Decimal("4.00")

    assert _disc(r, "cost_basis_gross_vs_net") == Decimal("2.40")  # 6.40 − 4.00
    assert _disc(r, "gross_total_vs_nav") == Decimal("1.20")  # 5.20 − 4.00 = debt_mark
    assert _disc(r, "nav_vs_ground_truth") == ZERO
    assert r.has_discrepancy
    # 2×debt_cost identity holds.
    assert _disc(r, "cost_basis_gross_vs_net") == 2 * r.agg_debt_cost


def test_canonical_loop_discrepancies_pinned():
    """Blueprint §7.11 canonical loop: NAV ties to $8,000 (no NAV gap) but the
    cost-basis and gross-total gaps are large ($63,600 and $32,000)."""
    r = compute_shadow_parity("canonical", "looping", _canonical_loop_positions(), ground_truth_nav=Decimal("8000"))
    assert r.total_value_usd == Decimal("40000")
    assert r.deployed_capital_usd == Decimal("70800")  # 39000 + 31800 GROSS
    assert r.debt_mark == Decimal("32000")
    assert r.agg_net_cost == Decimal("7200")  # 39000 − 31800
    assert r.nav == Decimal("8000")

    assert _disc(r, "cost_basis_gross_vs_net") == Decimal("63600")  # 70800 − 7200
    assert _disc(r, "gross_total_vs_nav") == Decimal("32000")  # 40000 − 8000 = debt_mark
    assert _disc(r, "nav_vs_ground_truth") == ZERO  # canonical shape correct


def test_net_leg_landmine_catastrophic_nav_gap():
    """Net-leg landmine: NAV double-subtracts debt → −$24,000 vs true $8,000, a
    −$32,000 NAV gap. Pinned so any change that silently makes the unsafe shape
    'work' flips this red."""
    r = compute_shadow_parity("landmine", "looping", _net_leg_landmine_positions(), ground_truth_nav=Decimal("8000"))
    assert r.total_value_usd == Decimal("8000")  # already-net positive leg
    assert r.debt_mark == Decimal("32000")
    assert r.nav == Decimal("-24000")  # 8000 − 32000 double-subtract

    assert _disc(r, "nav_vs_ground_truth") == Decimal("-32000")  # −24000 − 8000
    assert r.nav != r.ground_truth_nav


def test_dict_positions_json_path_handles_wallet_exclusion():
    """Regression (VIB-5217): the advertised ``positions_from_sqlite`` path yields
    ``dict`` rows, which must flow through the attribute-based valuer
    wallet-exclusion predicates without an ``AttributeError``. A dict ``TOKEN`` row
    overlapping the wallet is excluded from ``total_value_usd``; a swap-inventory
    ``TOKEN`` lot (deployed capital) is retained — ``portfolio_valuer.py:751-762``.
    """
    wallet_token = {
        "position_type": "TOKEN",
        "value_usd": "100",
        "cost_basis_usd": "100",
        "details": {"asset": "USDC"},
    }
    swap_inventory_lot = {
        "position_type": "TOKEN",
        "value_usd": "50",
        "cost_basis_usd": "50",
        "details": {"asset": "USDC", "source": "swap_inventory_lots"},
    }
    r = compute_shadow_parity(
        "dict-wallet-path",
        "lp",
        [wallet_token, swap_inventory_lot],
        wallet_balances=[SimpleNamespace(symbol="USDC", address=None)],
    )
    # $100 wallet pseudo-token excluded; $50 swap-inventory lot counts in.
    assert r.total_value_usd == Decimal("50")


def test_no_debt_primitives_have_no_gaps_but_debt_primitives_do():
    """Ranking invariant: across all scenarios, the primitives WITHOUT a debt leg
    (lp, perp) carry zero discrepancy; every scenario WITH a debt leg carries a
    nonzero gap. This is the US-015 prioritization signal."""
    results = _all_results()
    for r in results:
        has_debt = r.debt_mark != ZERO
        assert r.has_discrepancy == has_debt, f"{r.label}: discrepancy/debt mismatch"


# --------------------------------------------------------------------------- #
# Report rendering (used to (re)generate docs/internal/accounting/shadow-parity-vib-5217.md).
# --------------------------------------------------------------------------- #
def render_markdown_report() -> str:
    """Render the enumerated discrepancy table. Used to (re)generate the
    checked-in report doc (``docs/internal/accounting/shadow-parity-vib-5217.md``);
    :func:`test_report_renders` exercises the renderer."""
    results = _all_results()
    lines = [
        "| Scenario | Primitive | total_value_usd | deployed (gross) | debt_mark | NAV | net_cost | max |Δ| |",
        "|---|---|--:|--:|--:|--:|--:|--:|",
    ]
    for r in results:
        lines.append(
            f"| {r.label} | {r.primitive} | {r.total_value_usd} | {r.deployed_capital_usd} | "
            f"{r.debt_mark} | {r.nav} | {r.agg_net_cost} | {r.max_magnitude} |"
        )
    return "\n".join(lines)


def test_report_renders():
    """The report renderer produces a stable table for every scenario."""
    report = render_markdown_report()
    assert "net-leg LANDMINE" in report
    assert report.count("|") > 0
    for primitive in ("lp", "looping", "perp"):
        assert primitive in report


if __name__ == "__main__":  # pragma: no cover - manual report (re)generation
    print(render_markdown_report())
    for res in _all_results():
        print(f"\n## {res.label} ({res.primitive})  max|delta|={res.max_magnitude}")
        for d in res.discrepancies:
            print(f"  - {d.name}: valuer={d.valuer_value} db={d.db_derived_value} delta={d.delta}")
