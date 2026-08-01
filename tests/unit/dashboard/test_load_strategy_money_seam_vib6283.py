"""VIB-6283 — ``load_strategy_money`` is the seam the whole fix routes through.

``almanak/framework/dashboard/money.py::load_strategy_money`` is the single
function every per-primitive money tile now reads from. Its two dependencies
(``GetPnLSummary`` / ``GetCostStack``) fail independently, and the four
combinations have four DIFFERENT correct answers:

===================  ================================================
PnL RPC / Cost RPC   contract
===================  ================================================
up / up              fully measured object
up / down            object; L1 value measured, L2 attribution ``None``
down / up            object; L2 attribution measured, L1 value ``None``
down / down          ``None`` — the caller renders "—", NOT zeros
===================  ================================================

The bottom row is the one that matters most: a zero-filled ``StrategyMoney``
returned on a total outage would render ``$0.00 / $0.00 / $0.00`` — the exact
"confident wrong number" this ticket exists to remove, reintroduced one layer
lower.

Every number here comes from ``tests/fixtures/lp_dashboard_vib6283/
anvil_fee_induction_run.json`` — the real Anvil Arbitrum fork run whose fees
were measured two independent ways on-chain (``collect()`` staticcall and a
hand-computed ``feeGrowthInside`` delta, agreeing to the wei) and then recorded
by the accounting layer as ``fees_total_usd = 88.50577177198135678775255617``.

Every test in this file fails on the pre-VIB-6283 tree: ``money.py`` does not
exist there.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from almanak.framework.dashboard.gateway_client import CostStackInfo, PnLSummary
from almanak.framework.dashboard.money import StrategyMoney, load_strategy_money
from almanak.framework.dashboard.pages._detail_header import _strategy_pnl_usd

# ── The real run ─────────────────────────────────────────────────────────

_FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "lp_dashboard_vib6283" / "anvil_fee_induction_run.json"

DEPLOYMENT_ID = "deployment:b3816ff5ddb8"


def _fixture_lp_close_payload() -> dict[str, Any]:
    """The LP_CLOSE accounting payload from the fee-induction fork run."""
    rows = json.loads(_FIXTURE.read_text())["accounting_events"]
    close = next(r for r in rows if r["event_type"] == "LP_CLOSE")
    return json.loads(close["payload_json"])


_CLOSE = _fixture_lp_close_payload()

# Fees provably owed on-chain (~$88.48) and recorded by accounting to the cent.
FEES_EARNED_USD = Decimal(_CLOSE["fees_total_usd"])
IL_USD = Decimal(_CLOSE["il_usd"])
REALIZED_PNL_USD = Decimal(_CLOSE["realized_pnl_usd"])
COST_BASIS_USD = Decimal(_CLOSE["cost_basis_usd"])
# portfolio_metrics.gas_spent_usd for the same run.
GAS_USD = Decimal("2.3755149327301")


def _pnl(
    *,
    nav: Decimal = COST_BASIS_USD,
    cash: Decimal = Decimal("0"),
    deployed_capital: Decimal = COST_BASIS_USD,
) -> PnLSummary:
    return PnLSummary(
        deployed_usd=Decimal("1261191.665331584048588828107"),  # initial_value_usd
        nav_usd=nav,
        lifetime_pnl_usd=None,
        lifetime_pnl_pct=None,
        net_apr_pct=None,
        max_drawdown_pct=Decimal("0"),
        current_drawdown_pct=Decimal("0"),
        value_confidence="HIGH",
        age_days=0,
        age_days_exact=Decimal("0.0139"),  # ~20 min run
        deployed_capital_usd=deployed_capital,
        available_cash_usd=cash,
        open_position_count=1,
        primary_risk_kind="lp",
        primary_risk_label="Range",
        primary_risk_value="in-range",
        primary_risk_color="green",
    )


def _cost(
    *,
    fees: Decimal | None = FEES_EARNED_USD,
    il: Decimal | None = IL_USD,
    realized: Decimal = REALIZED_PNL_USD,
    gas: Decimal = GAS_USD,
) -> CostStackInfo:
    return CostStackInfo(
        cost_gas_usd=gas,
        cost_protocol_fees_usd=Decimal("0"),
        cost_slippage_usd=Decimal("0"),
        fees_earned_usd=fees,
        interest_paid_usd=Decimal("0"),
        interest_earned_usd=Decimal("0"),
        funding_paid_usd=Decimal("0"),
        funding_earned_usd=Decimal("0"),
        realized_pnl_usd=realized,
        il_usd=il,
    )


@pytest.fixture
def stub_rpcs(monkeypatch: pytest.MonkeyPatch):
    """Replace the two gateway RPCs ``load_strategy_money`` reads.

    ``load_strategy_money`` imports them lazily from
    ``almanak.framework.dashboard.sections`` (Streamlit + gRPC are heavy), so
    the patch target is that module's attributes — resolved at call time.
    """

    def _install(pnl: Any, cost: Any) -> None:
        def _get_pnl(_deployment_id: str) -> Any:
            if isinstance(pnl, Exception):
                raise pnl
            return pnl

        def _get_cost(_deployment_id: str) -> Any:
            if isinstance(cost, Exception):
                raise cost
            return cost

        monkeypatch.setattr("almanak.framework.dashboard.sections.get_pnl_summary", _get_pnl)
        monkeypatch.setattr("almanak.framework.dashboard.sections.get_cost_stack", _get_cost)

    return _install


# ── Both RPCs healthy ────────────────────────────────────────────────────


def test_both_rpcs_healthy_surface_the_measured_fork_numbers(stub_rpcs) -> None:
    """The motivating run, end to end through the seam.

    ~$88.48 of fees were owed on-chain and the accounting layer recorded
    ``88.5057…``. The panel rendered ``$0.00`` because it never asked this
    question. It asks now, and the answer must be the accounting number — not
    a zero, and not a rounded copy.
    """
    stub_rpcs(_pnl(), _cost())

    money = load_strategy_money(DEPLOYMENT_ID)

    assert money is not None
    assert isinstance(money, StrategyMoney)
    # Full Decimal precision preserved — no float round-trip on the seam.
    assert money.lp_fees_earned_usd == FEES_EARNED_USD
    assert money.lp_fees_earned_usd != Decimal("0")
    assert money.lp_il_usd == IL_USD
    assert money.nav_usd == COST_BASIS_USD
    assert money.cost_basis_usd == COST_BASIS_USD
    assert money.gas_usd == GAS_USD


def test_strategy_pnl_is_the_headline_computation_not_a_second_one(stub_rpcs) -> None:
    """THE cross-surface invariant: two presentations, ONE computation.

    The original defect was two framework computations on the same page
    disagreeing — the headline Strategy PnL tile showed a real number while the
    LP panel's Net PnL tile showed ``$+0.00``. ``load_strategy_money`` must
    return the headline's OWN function's output, byte for byte, so the two
    surfaces cannot drift again.

    Asserted twice: against ``_strategy_pnl_usd`` (the identity that makes
    drift impossible) and against an independent re-derivation of the formula
    (which catches ``_strategy_pnl_usd`` itself being wired to the wrong
    inputs — an identity to the wrong argument set is still wrong).
    """
    pnl = _pnl(nav=Decimal("1261434.306192221506426206997"), cash=Decimal("100000"))
    cost = _cost()
    stub_rpcs(pnl, cost)

    money = load_strategy_money(DEPLOYMENT_ID)

    assert money is not None
    open_nav = pnl.nav_usd - pnl.available_cash_usd
    assert money.open_position_nav_usd == open_nav
    assert money.strategy_pnl_usd == _strategy_pnl_usd(pnl, cost, open_nav)

    # Independent re-derivation: net realized (accounting) + unrealized.
    net_realized = (
        cost.realized_pnl_usd
        + FEES_EARNED_USD
        + (cost.funding_earned_usd - cost.funding_paid_usd)
        + (cost.interest_earned_usd - cost.interest_paid_usd)
        - cost.cost_gas_usd
    )
    unrealized = open_nav - pnl.deployed_capital_usd
    assert money.strategy_pnl_usd == net_realized + unrealized


def test_open_position_nav_floors_at_zero_never_negative(stub_rpcs) -> None:
    """Cash above NAV (a snapshot-ordering artefact) must not produce a
    negative position value — it would render as a phantom short."""
    stub_rpcs(_pnl(nav=Decimal("100"), cash=Decimal("250")), _cost())

    money = load_strategy_money(DEPLOYMENT_ID)

    assert money is not None
    assert money.open_position_nav_usd == Decimal("0")


# ── Partial outage: one RPC up, one down ─────────────────────────────────


def test_pnl_up_cost_down_keeps_value_and_leaves_attribution_unmeasured(stub_rpcs) -> None:
    """L1 value is measurable without the cost stack; L2 attribution is not.

    A partial read must still return an object. Returning ``None`` here would
    blank tiles that ARE measured, and returning zeros would fabricate them.
    """
    stub_rpcs(_pnl(), None)

    money = load_strategy_money(DEPLOYMENT_ID)

    assert money is not None
    assert money.nav_usd == COST_BASIS_USD
    assert money.cost_basis_usd == COST_BASIS_USD
    assert money.open_position_nav_usd is not None
    # Everything sourced from the cost stack is UNMEASURED, not zero.
    assert money.gas_usd is None
    assert money.lp_fees_earned_usd is None
    assert money.lp_il_usd is None
    assert money.lending_interest_earned_usd is None
    assert money.perp_funding_paid_usd is None
    # Strategy PnL needs the realized leg; without it the honest answer is "—"
    # (dropping the leg silently would understate PnL).
    assert money.strategy_pnl_usd is None


def test_cost_up_pnl_down_keeps_attribution_and_leaves_value_unmeasured(stub_rpcs) -> None:
    """The mirror case: LP fees are known, NAV is not."""
    stub_rpcs(None, _cost())

    money = load_strategy_money(DEPLOYMENT_ID)

    assert money is not None
    assert money.lp_fees_earned_usd == FEES_EARNED_USD
    assert money.lp_il_usd == IL_USD
    assert money.gas_usd == GAS_USD
    assert money.nav_usd is None
    assert money.cost_basis_usd is None
    assert money.open_position_nav_usd is None
    assert money.strategy_pnl_usd is None


# ── Total outage ─────────────────────────────────────────────────────────


def test_both_rpcs_down_returns_none_not_a_zero_filled_object(stub_rpcs) -> None:
    """The failure mode that would reintroduce the bug one layer lower.

    A zero-filled ``StrategyMoney`` renders ``$0.00 / $0.00 / $0.00`` — the
    original symptom, now with an accounting-shaped provenance. ``None`` makes
    the caller render "—".
    """
    stub_rpcs(None, None)

    money = load_strategy_money(DEPLOYMENT_ID)

    assert money is None


def test_no_deployment_id_returns_none(stub_rpcs) -> None:
    """No deployment ⇒ nothing to measure. Must not fall through to a
    zero-valued object (and must not call the RPCs at all)."""
    stub_rpcs(RuntimeError("must not be called"), RuntimeError("must not be called"))

    assert load_strategy_money(None) is None
    assert load_strategy_money("") is None


# ── RPC raising ──────────────────────────────────────────────────────────


def test_a_raising_rpc_degrades_instead_of_taking_down_the_page(stub_rpcs) -> None:
    """``get_pnl_summary`` returns ``None`` on outage by convention, but a
    transport-level surprise (a gRPC error escaping the client) must not
    propagate out of a render path — a money tile that cannot load is "—", not
    a stack trace where the dashboard used to be."""
    stub_rpcs(RuntimeError("gateway channel closed"), _cost())

    money = load_strategy_money(DEPLOYMENT_ID)

    assert money is not None
    assert money.nav_usd is None
    assert money.strategy_pnl_usd is None
    # The half that answered still reports.
    assert money.lp_fees_earned_usd == FEES_EARNED_USD


def test_both_rpcs_raising_returns_none(stub_rpcs) -> None:
    """Two exceptions are still a total outage — ``None``, never zeros."""
    stub_rpcs(RuntimeError("boom"), RuntimeError("boom"))

    assert load_strategy_money(DEPLOYMENT_ID) is None


# ── Empty ≠ Zero through the seam ────────────────────────────────────────


def test_unmeasured_lp_buckets_stay_none_through_the_seam(stub_rpcs) -> None:
    """An OPEN LP position has no close/collect row yet: fees and IL are
    UNMEASURED. The seam must carry that through untouched — a ``Decimal("0")``
    here renders "$0.00" against a position with fees genuinely accruing."""
    stub_rpcs(_pnl(), _cost(fees=None, il=None))

    money = load_strategy_money(DEPLOYMENT_ID)

    assert money is not None
    assert money.lp_fees_earned_usd is None
    assert money.lp_il_usd is None
    # ...while Strategy PnL still computes (unmeasured contributes 0 to a sum
    # that cannot express "unmeasured"). Both facts must hold at once.
    assert money.strategy_pnl_usd is not None


def test_measured_zero_lp_buckets_stay_zero_through_the_seam(stub_rpcs) -> None:
    """The inverse conflation. A close that genuinely earned nothing is a
    MEASURED zero and must not be laundered into "unmeasured" — the fix must
    not trade one wrong render for the other."""
    stub_rpcs(_pnl(), _cost(fees=Decimal("0"), il=Decimal("0")))

    money = load_strategy_money(DEPLOYMENT_ID)

    assert money is not None
    assert money.lp_fees_earned_usd == Decimal("0")
    assert money.lp_fees_earned_usd is not None
    assert money.lp_il_usd == Decimal("0")
    assert money.lp_il_usd is not None
