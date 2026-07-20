"""VIB-5866 leg B (PR-C2) — ``strat pnl`` is honest about UNMEASURED capital
flows.

Two seams:

1. ``_populate_gross_net_pnl`` already leaves gross/net ``None`` when
   ``pnl_before_gas`` is ``None`` (VIB-2475), and PR-C1 made an unmeasured
   ``deposits_usd`` / ``withdrawals_usd`` propagate into that ``None``. What was
   wrong is the WARNING: it blamed ``total_value_usd`` unconditionally, sending
   the operator to diagnose the wrong column. It now names the inputs that are
   actually unmeasured. The rendered lines stay honest ("—", never "$0.00").
2. ``_apply_open_leveraged_headline`` re-derives the headline as
   ``net_lending_nav − initial − deposits + withdrawals``. It read the flows
   with ``_dec(getattr(...))``, whose ``None → Decimal("0")`` fallback books
   external capital as profit. It now skips the derivation when either flow is
   unmeasured (Empty≠Zero) — the same rule the ``initial_value_usd`` guard
   already applied.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from almanak.framework.accounting.reporting.leveraged_lending import LeveragedLendingVerdict
from almanak.framework.cli.strat_pnl import (
    PnLBreakdown,
    _apply_open_leveraged_headline,
    _populate_gross_net_pnl,
    render_text,
)
from almanak.framework.portfolio.models import PortfolioMetrics

_DEPLOYMENT_ID = "deployment:vib5866c2cl"
_TS = datetime(2026, 7, 19, 12, 0, 0, tzinfo=UTC)


def _metrics(*, deposits: Decimal | None, withdrawals: Decimal | None) -> PortfolioMetrics:
    return PortfolioMetrics(
        deployment_id=_DEPLOYMENT_ID,
        timestamp=_TS,
        initial_value_usd=Decimal("1000"),
        total_value_usd=Decimal("1200"),
        deposits_usd=deposits,
        withdrawals_usd=withdrawals,
        gas_spent_usd=Decimal("0.50"),
    )


# ─── Seam 1: the verbatim headline + its warning ───────────────────────────


def test_unmeasured_deposit_leaves_headline_unavailable_and_names_the_column() -> None:
    breakdown = PnLBreakdown(deployment_id=_DEPLOYMENT_ID)
    _populate_gross_net_pnl(breakdown, _metrics(deposits=None, withdrawals=Decimal("0")))

    assert breakdown.gross_pnl_usd is None
    assert breakdown.net_pnl_usd is None
    assert len(breakdown.warnings) == 1
    warning = breakdown.warnings[0]
    assert "deposits_usd" in warning
    assert "total_value_usd" not in warning  # it IS measured — don't misdirect
    assert "VIB-5866" in warning


def test_unmeasured_flows_render_as_placeholder_never_zero() -> None:
    """Rendered output must never print a fabricated ``$0.00`` headline."""
    breakdown = PnLBreakdown(deployment_id=_DEPLOYMENT_ID)
    _populate_gross_net_pnl(breakdown, _metrics(deposits=None, withdrawals=None))

    text = render_text(breakdown)

    gross_line = next(line for line in text.splitlines() if line.startswith("Gross PnL:"))
    net_line = next(line for line in text.splitlines() if line.startswith("Net PnL:"))
    assert "—" in gross_line and "$" not in gross_line
    assert "—" in net_line and "$" not in net_line


def test_measured_flows_headline_unchanged() -> None:
    """Measured (including measured-zero) flows keep the verbatim headline."""
    breakdown = PnLBreakdown(deployment_id=_DEPLOYMENT_ID)
    _populate_gross_net_pnl(breakdown, _metrics(deposits=Decimal("0"), withdrawals=Decimal("0")))

    assert breakdown.gross_pnl_usd == Decimal("200")  # 1200 − 1000
    assert breakdown.net_pnl_usd == Decimal("199.50")  # − gas
    assert breakdown.warnings == []


def test_measured_deposit_is_not_booked_as_profit() -> None:
    """$200 deposited, NAV up $200 ⇒ zero PnL. The Case-B contract."""
    breakdown = PnLBreakdown(deployment_id=_DEPLOYMENT_ID)
    _populate_gross_net_pnl(breakdown, _metrics(deposits=Decimal("200"), withdrawals=Decimal("0")))

    assert breakdown.gross_pnl_usd == Decimal("0")


# ─── Seam 2: the VIB-4975 leveraged-lending B-open re-derivation ───────────


def _open_verdict(nav: str = "900") -> LeveragedLendingVerdict:
    return LeveragedLendingVerdict(
        is_leveraged_lending=True,
        state="open",
        net_lending_nav_usd=Decimal(nav),
        reason="",
    )


def test_leveraged_open_derivation_skipped_when_a_flow_is_unmeasured() -> None:
    breakdown = PnLBreakdown(deployment_id=_DEPLOYMENT_ID)
    _apply_open_leveraged_headline(
        breakdown,
        _metrics(deposits=None, withdrawals=Decimal("0")),
        _open_verdict(),
    )

    # No fabricated-zero derivation: the headline stays as the (unavailable)
    # verbatim one, and the leverage-adjusted stamp is NOT applied.
    assert breakdown.gross_pnl_usd is None
    assert breakdown.net_pnl_usd is None
    assert breakdown.headline_leverage_adjusted is False


def test_leveraged_open_derivation_runs_when_flows_are_measured() -> None:
    breakdown = PnLBreakdown(deployment_id=_DEPLOYMENT_ID)
    _apply_open_leveraged_headline(
        breakdown,
        _metrics(deposits=Decimal("0"), withdrawals=Decimal("0")),
        _open_verdict(),
    )

    assert breakdown.gross_pnl_usd == Decimal("-100")  # 900 − 1000
    assert breakdown.net_pnl_usd == Decimal("-100.50")
    assert breakdown.headline_leverage_adjusted is True


def test_leveraged_open_derivation_runs_for_legacy_shape_without_flow_attrs() -> None:
    """A metrics shape that never carried the flow attributes at all keeps the
    legacy measured-zero behaviour — absence of the attribute predates the
    field and is not an ``unmeasured`` claim."""
    legacy = SimpleNamespace(initial_value_usd=Decimal("1000"), gas_spent_usd=Decimal("0.50"))
    breakdown = PnLBreakdown(deployment_id=_DEPLOYMENT_ID)
    _apply_open_leveraged_headline(breakdown, legacy, _open_verdict())

    assert breakdown.gross_pnl_usd == Decimal("-100")
    assert breakdown.headline_leverage_adjusted is True
