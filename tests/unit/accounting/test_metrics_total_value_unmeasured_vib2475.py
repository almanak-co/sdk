"""VIB-2475 — ``total_value_usd`` Empty≠Zero contract on ``PortfolioMetrics``.

The gRPC metrics-reconstruction path historically hardcoded
``total_value_usd=Decimal("0")`` because the ``PortfolioMetricsData`` proto does
not carry it (it is sourced from the latest snapshot). That fabricated zero fed
``pnl_before_gas = 0 − initial − deposits + withdrawals`` ≈ −initial — a
confident-wrong −100% loss across the authoritative strategy-PnL read
boundaries.

These tests lock the Empty≠Zero contract on the model and on the ``strat pnl``
consumer:

* ``total_value_usd`` is ``Decimal | None``; ``None`` (unmeasured) propagates
  through ``pnl_before_gas`` / ``pnl_after_gas`` / ``roi_percent`` as ``None``,
  never a fabricated ``Decimal("0")``.
* ``to_dict`` / ``from_dict`` round-trip preserves ``None`` (no ``"None"`` /
  ``"0"`` coercion).
* ``strat_pnl._populate_gross_net_pnl`` distinguishes three states (no row /
  unmeasured / measured) and never lets ``_dec`` swallow ``None`` into 0.

The gRPC-reconstruction half of the contract (snapshot sourcing + None on miss)
is covered in ``tests/gateway/test_portfolio_metrics_rpc.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.cli.strat_pnl import PnLBreakdown, _populate_gross_net_pnl
from almanak.framework.portfolio.models import PortfolioMetrics


def _metrics(total_value_usd: Decimal | None) -> PortfolioMetrics:
    """Build a REAL PortfolioMetrics (not a SimpleNamespace) per CLAUDE.md."""
    return PortfolioMetrics(
        deployment_id="deploy-1",
        timestamp=datetime(2026, 6, 22, 12, 0, tzinfo=UTC),
        total_value_usd=total_value_usd,
        initial_value_usd=Decimal("4.0000169954955"),
        deposits_usd=Decimal("0"),
        withdrawals_usd=Decimal("0"),
        gas_spent_usd=Decimal("0.50"),
    )


def test_unmeasured_total_value_propagates_none_through_pnl() -> None:
    """None total_value_usd → None pnl_before/after_gas + roi (Empty≠Zero)."""
    m = _metrics(None)
    assert m.total_value_usd is None
    assert m.pnl_before_gas is None
    assert m.pnl_after_gas is None
    assert m.roi_percent is None


def test_measured_zero_is_not_unmeasured() -> None:
    """Decimal("0") is a MEASURED zero NAV — pnl is computed, not None.

    Empty≠Zero: measured-zero NAV legitimately yields pnl ≈ −initial; that is a
    real (if grim) figure, distinct from the unmeasured None case.
    """
    m = _metrics(Decimal("0"))
    assert m.total_value_usd == Decimal("0")
    assert m.pnl_before_gas == Decimal("-4.0000169954955")
    assert m.pnl_after_gas == Decimal("-4.5000169954955")


def test_measured_value_computes_pnl() -> None:
    """A measured NAV equal to initial → ~0 pnl (the looping-fixture shape)."""
    m = _metrics(Decimal("4.0000169954955"))
    assert m.pnl_before_gas == Decimal("0")
    assert m.pnl_after_gas == Decimal("-0.50")


def test_none_survives_to_dict_from_dict_round_trip() -> None:
    """to_dict/from_dict preserves None (no "None"/"0" coercion)."""
    m = _metrics(None)
    d = m.to_dict()
    assert d["total_value_usd"] is None
    restored = PortfolioMetrics.from_dict(d)
    assert restored.total_value_usd is None
    assert restored.pnl_before_gas is None


def test_measured_survives_to_dict_from_dict_round_trip() -> None:
    """to_dict/from_dict preserves a measured Decimal value."""
    m = _metrics(Decimal("12345.67"))
    restored = PortfolioMetrics.from_dict(m.to_dict())
    assert restored.total_value_usd == Decimal("12345.67")


def test_populate_gross_net_pnl_no_metrics_row() -> None:
    """State 1: no metrics row → gross/net stay None, 'no row' warning."""
    breakdown = PnLBreakdown(deployment_id="deploy-1")
    _populate_gross_net_pnl(breakdown, None)
    assert breakdown.gross_pnl_usd is None
    assert breakdown.net_pnl_usd is None
    assert any("No PortfolioMetrics row found" in w for w in breakdown.warnings)


def test_populate_gross_net_pnl_unmeasured_value() -> None:
    """State 2: row present but total_value_usd unmeasured → gross/net None.

    _dec(None) would coerce to Decimal("0") — the consumer must NOT call it on
    the unmeasured path, or the poison this fix removes is re-introduced.
    """
    breakdown = PnLBreakdown(deployment_id="deploy-1")
    _populate_gross_net_pnl(breakdown, _metrics(None))
    assert breakdown.gross_pnl_usd is None
    assert breakdown.net_pnl_usd is None
    assert any("total_value_usd is unmeasured" in w for w in breakdown.warnings)
    # The unmeasured warning must be distinct from the no-row warning.
    assert not any("No PortfolioMetrics row found" in w for w in breakdown.warnings)


def test_populate_gross_net_pnl_measured_value() -> None:
    """State 3: measured value → gross/net populated verbatim."""
    breakdown = PnLBreakdown(deployment_id="deploy-1")
    _populate_gross_net_pnl(breakdown, _metrics(Decimal("4.0000169954955")))
    assert breakdown.gross_pnl_usd == Decimal("0")
    assert breakdown.net_pnl_usd == Decimal("-0.50")
    assert breakdown.warnings == []
