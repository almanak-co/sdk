"""VIB-5866 leg B (PR-C2) — the lifetime-PnL headline suppresses when the
capital flows behind its baseline are UNMEASURED.

``compute_pnl_summary`` used to read ``deposits_usd`` / ``withdrawals_usd``
with ``_to_decimal``, whose ``None → Decimal("0")`` coercion is a read-side
zero fabrication: with the flows unknown, ``deployed_usd`` silently omits an
external deposit and ``lifetime_pnl_usd = nav − deployed`` books that deposit
as PROFIT (Empty≠Zero, blueprint 27 §10.10).

Contract shipped here:

* Either flow unmeasured ⇒ NO flow adjustment to ``deployed_usd``,
  ``capital_flows_unmeasured=True``, and ``lifetime_pnl_usd`` /
  ``lifetime_pnl_pct`` / ``net_apr_pct`` are ``None`` (suppressed).
  ``deployed_usd`` / ``nav_usd`` / cash / positions stay VISIBLE — they are
  measured, and hiding them would degrade a working card.
* Both flows measured (including the legacy ``'0'`` measured zero) ⇒ output is
  byte-identical to the pre-change behaviour.
* A measured deposit still moves ``deployed_usd`` and must NOT move lifetime
  PnL (the Case-B contract: capital in is not profit).
* Suppression survives the ``GetPnLSummary`` wire: ``None`` serialises to the
  empty string (the presence-aware encoding VIB-4984 established for
  ``CostStackInfo.inventory_unrealized_usd``) and decodes back to ``None``.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.framework.dashboard.gateway_client import _convert_pnl_summary
from almanak.framework.dashboard.quant_aggregations import PnLSummary, compute_pnl_summary
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import DashboardServiceServicer


def _snapshot(*, total_value_usd: str = "0", available_cash_usd: str = "1000") -> SimpleNamespace:
    return SimpleNamespace(
        total_value_usd=total_value_usd,
        available_cash_usd=available_cash_usd,
        value_confidence="HIGH",
        deployed_capital_usd="0",
        positions_json="[]",
        timestamp=datetime.now(tz=UTC),
    )


def _metrics(*, deposits: Any, withdrawals: Any, initial: str = "1000") -> SimpleNamespace:
    """PortfolioMetrics stand-in. ``deposits`` / ``withdrawals`` accept the
    real shapes the reader sees: ``Decimal`` / text / ``None`` (unmeasured) /
    ``''`` (the PR-C1 storage sentinel for unmeasured)."""
    return SimpleNamespace(
        deposits_usd=deposits,
        withdrawals_usd=withdrawals,
        initial_value_usd=initial,
        timestamp=datetime.now(tz=UTC),
    )


def _summary(metrics: Any, snapshot: Any | None = None) -> PnLSummary:
    return compute_pnl_summary(
        portfolio_metrics=metrics,
        snapshots=[snapshot if snapshot is not None else _snapshot()],
        ledger_entries=[],
        accounting_events=[],
    )


# ─── Suppression ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("deposits", "withdrawals"),
    [
        (None, Decimal("0")),  # deposits unmeasured (explicit None)
        (Decimal("0"), None),  # withdrawals unmeasured
        (None, None),  # both unmeasured
        ("", "0"),  # PR-C1 storage sentinel arriving as raw text
        ("0", ""),
    ],
)
def test_either_flow_unmeasured_suppresses_headline(deposits: Any, withdrawals: Any) -> None:
    """Either flow unmeasured ⇒ the three flow-derived metrics are None and the
    diagnostic flag is set — never a confident-wrong number off a fabricated
    zero flow."""
    pnl = _summary(_metrics(deposits=deposits, withdrawals=withdrawals))

    assert pnl.capital_flows_unmeasured is True
    assert pnl.lifetime_pnl_usd is None
    assert pnl.lifetime_pnl_pct is None
    assert pnl.net_apr_pct is None


def test_suppressed_headline_keeps_measured_tiles_visible() -> None:
    """Suppression is scoped to the flow-derived metrics: the measured wallet
    tiles must still render. A blanket blank-out would break a working card."""
    snap = _snapshot(total_value_usd="250", available_cash_usd="750")
    pnl = _summary(_metrics(deposits=None, withdrawals=Decimal("0")), snapshot=snap)

    # No flow adjustment applied — deployed is the un-adjusted baseline.
    assert pnl.deployed_usd == Decimal("1000")
    assert pnl.nav_usd == Decimal("1000")  # 250 positions + 750 cash
    assert pnl.available_cash_usd == Decimal("750")
    assert pnl.value_confidence == "HIGH"


def test_unparseable_flow_text_is_unmeasured_not_zero() -> None:
    """Text that cannot be parsed to a finite Decimal (including the literal
    ``"None"`` an older write seam could persist) is UNMEASURED. The previous
    ``_to_decimal`` silently turned it into a measured zero."""
    for junk in ("None", "abc", "NaN"):
        pnl = _summary(_metrics(deposits=junk, withdrawals="0"))
        assert pnl.capital_flows_unmeasured is True, junk
        assert pnl.lifetime_pnl_usd is None, junk


def test_absent_metrics_row_is_not_suppression() -> None:
    """No ``portfolio_metrics`` at all is the pre-existing degraded path (flows
    default to a measured zero), NOT an unmeasured-flow claim — byte-identical
    to before."""
    pnl = compute_pnl_summary(
        portfolio_metrics=None,
        snapshots=[_snapshot()],
        ledger_entries=[],
        accounting_events=[],
    )

    assert pnl.capital_flows_unmeasured is False
    assert pnl.lifetime_pnl_usd == Decimal("1000")


# ─── Measured path: byte-identical ─────────────────────────────────────────


def test_measured_zero_flows_are_byte_identical() -> None:
    """Snapshot assertion over the FULL summary for the legacy shape every
    existing DB has (``'0'`` / ``'0'``). Any drift on this fixture is a
    regression for every strategy on disk."""
    pnl = _summary(_metrics(deposits="0", withdrawals="0"))

    assert dataclasses.asdict(pnl) == {
        "deployed_usd": Decimal("1000"),
        "nav_usd": Decimal("1000"),
        "lifetime_pnl_usd": Decimal("0"),
        "lifetime_pnl_pct": Decimal("0"),
        "net_apr_pct": Decimal("0"),
        "max_drawdown_pct": Decimal("0"),
        "current_drawdown_pct": Decimal("0"),
        "value_confidence": "HIGH",
        "age_days": 0,
        "capital_flows_unmeasured": False,
        "deployed_capital_usd": Decimal("0"),
        "available_cash_usd": Decimal("1000"),
        "open_position_count": 0,
        "primary_risk_label": "No active positions",
        "primary_risk_value": "",
        "primary_risk_color": "neutral",
        "primary_risk_kind": "none",
    }


def test_measured_flows_still_adjust_deployed() -> None:
    """A measured withdrawal reduces the deployed baseline exactly as before."""
    pnl = _summary(_metrics(deposits="0", withdrawals="200"))

    assert pnl.capital_flows_unmeasured is False
    assert pnl.deployed_usd == Decimal("800")
    assert pnl.lifetime_pnl_usd == Decimal("200")  # nav 1000 − deployed 800


# ─── Case-B contract: a deposit is capital, not profit ─────────────────────


def test_measured_deposit_is_capital_not_profit() -> None:
    """$500 deposited and still held: ``deployed_usd`` absorbs it and lifetime
    PnL is UNCHANGED vs the no-deposit baseline. This is the defect VIB-5866
    exists for — fabricating a zero deposit would print +$500 of profit."""
    baseline = _summary(
        _metrics(deposits="0", withdrawals="0"),
        snapshot=_snapshot(available_cash_usd="1000"),
    )
    with_deposit = _summary(
        _metrics(deposits="500", withdrawals="0"),
        snapshot=_snapshot(available_cash_usd="1500"),  # the deposit landed in cash
    )

    assert with_deposit.deployed_usd == Decimal("1500")
    assert with_deposit.nav_usd == Decimal("1500")
    assert with_deposit.lifetime_pnl_usd == baseline.lifetime_pnl_usd == Decimal("0")


# ─── Serialization seam: gateway wire + client decode ──────────────────────


def _servicer_returning(metrics: Any, snapshot: Any) -> DashboardServiceServicer:
    """A ``DashboardServiceServicer`` whose quant-input load is stubbed, so the
    test exercises the real ``GetPnLSummary`` serialization expressions."""
    svc = DashboardServiceServicer.__new__(DashboardServiceServicer)
    svc.settings = SimpleNamespace()
    svc._state_manager = MagicMock()
    svc._initialized = True
    svc._strategies_root = None
    svc._cached_positions = {}

    async def _inputs(_deployment_id: str) -> tuple[Any, list[Any], list[Any], list[Any], Any]:
        return (metrics, [snapshot], [], [], None)

    async def _drawdown(_deployment_id: str) -> None:
        return None

    async def _ensure() -> None:
        return None

    svc._get_quant_inputs = _inputs  # type: ignore[method-assign]
    svc._get_lifetime_drawdown = _drawdown  # type: ignore[method-assign]
    svc._ensure_initialized = _ensure  # type: ignore[method-assign]
    return svc


@pytest.mark.asyncio
async def test_rpc_serialises_suppressed_metrics_as_empty_string() -> None:
    """The RPC must not crash (``f"{None:.2f}"`` raises) and must not emit the
    literal ``"None"``. Empty string == unmeasured, the same presence-aware
    encoding as ``CostStackInfo.inventory_unrealized_usd`` (VIB-4984) — no
    proto change needed."""
    svc = _servicer_returning(_metrics(deposits=None, withdrawals=Decimal("0")), _snapshot())

    proto = await svc.GetPnLSummary(
        gateway_pb2.GetPnLSummaryRequest(deployment_id="deployment:vib5866c2"),
        MagicMock(),
    )

    assert proto.lifetime_pnl_usd == ""
    assert proto.lifetime_pnl_pct == ""
    assert proto.net_apr_pct == ""
    # Measured tiles still travel.
    assert proto.deployed_usd == "1000"
    assert proto.nav_usd == "1000"
    assert proto.value_confidence == "HIGH"


@pytest.mark.asyncio
async def test_rpc_measured_path_wire_unchanged() -> None:
    """Measured flows serialise exactly as before (2dp percents, plain str)."""
    svc = _servicer_returning(_metrics(deposits="0", withdrawals="200"), _snapshot())

    proto = await svc.GetPnLSummary(
        gateway_pb2.GetPnLSummaryRequest(deployment_id="deployment:vib5866c2"),
        MagicMock(),
    )

    assert proto.deployed_usd == "800"
    assert proto.lifetime_pnl_usd == "200"
    assert proto.lifetime_pnl_pct == "25.00"
    assert proto.net_apr_pct == "0.00"


def test_client_decodes_empty_string_as_unmeasured() -> None:
    """The dashboard client must surface ``None``, never ``Decimal("0")`` — a
    zero here renders a confident-wrong "$0.00 lifetime PnL"."""
    suppressed = _convert_pnl_summary(
        gateway_pb2.PnLSummary(
            deployed_usd="1000",
            nav_usd="1000",
            lifetime_pnl_usd="",
            lifetime_pnl_pct="",
            net_apr_pct="",
            value_confidence="HIGH",
        )
    )

    assert suppressed.lifetime_pnl_usd is None
    assert suppressed.lifetime_pnl_pct is None
    assert suppressed.net_apr_pct is None
    assert suppressed.deployed_usd == Decimal("1000")

    measured = _convert_pnl_summary(
        gateway_pb2.PnLSummary(
            deployed_usd="800",
            nav_usd="1000",
            lifetime_pnl_usd="200",
            lifetime_pnl_pct="25.00",
            net_apr_pct="0.00",
            value_confidence="HIGH",
        )
    )

    assert measured.lifetime_pnl_usd == Decimal("200")
    assert measured.lifetime_pnl_pct == Decimal("25.00")
    assert measured.net_apr_pct == Decimal("0.00")
