"""VIB-5942 — Cost Stack tile honours Empty≠Zero (blueprint 27 §8.4).

An UNMEASURED fee / slippage bucket (None — no contributing event carried the
term, e.g. a GMX perp whose receipt parser is pending, the VIB-5941 payload gap)
must render "— unmeasured", NEVER a fabricated "−$0.00". A MEASURED zero
(Decimal("0")) renders "$0.00". Gas is always measured and uses the precise-small
formatter so a real sub-cent gas cost no longer collapses to "−$0.00".
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

from almanak.framework.dashboard.gateway_client import CostStackInfo
from almanak.framework.dashboard.pages import _detail_header as dh


def _render(cost: CostStackInfo) -> str:
    captured: list[str] = []
    with patch.object(dh.st, "markdown", side_effect=lambda html, *a, **k: captured.append(html)):
        dh.render_cost_stack(cost)
    return "\n".join(captured)


def _cost(**overrides) -> CostStackInfo:
    base = dict(
        cost_gas_usd=Decimal("0.0008"),
        cost_protocol_fees_usd=None,
        cost_slippage_usd=None,
        fees_earned_usd=Decimal("0"),
        interest_paid_usd=Decimal("0"),
        interest_earned_usd=Decimal("0"),
        funding_paid_usd=Decimal("0"),
        funding_earned_usd=Decimal("0"),
        realized_pnl_usd=Decimal("0"),
        il_usd=Decimal("0"),
    )
    base.update(overrides)
    return CostStackInfo(**base)  # type: ignore[arg-type]


def test_unmeasured_fees_and_slippage_render_dash_not_zero() -> None:
    html = _render(_cost(cost_protocol_fees_usd=None, cost_slippage_usd=None))
    assert "Fees — unmeasured" in html
    assert "Slip — unmeasured" in html
    # The fabricated measured-zero must NOT appear for these unmeasured buckets.
    assert "Fees −$0.00" not in html
    assert "Slip −$0.00" not in html


def test_measured_zero_fee_renders_dollar_zero() -> None:
    html = _render(_cost(cost_protocol_fees_usd=Decimal("0"), cost_slippage_usd=Decimal("0")))
    assert "Fees −$0.00" in html
    assert "Slip −$0.00" in html
    assert "unmeasured" not in html


def test_measured_subcent_gas_is_not_rounded_to_zero() -> None:
    """Gas is always MEASURED; a real $0.0008 gas cost must show precisely, not
    round to "−$0.00" (the pre-VIB-5942 plain-2dp formatting)."""
    html = _render(_cost(cost_gas_usd=Decimal("0.0008")))
    assert "Gas −$0.0008" in html
    assert "Gas −$0.00<" not in html


def test_measured_fee_value_renders() -> None:
    html = _render(_cost(cost_protocol_fees_usd=Decimal("0.45"), cost_slippage_usd=Decimal("0.12")))
    assert "Fees −$0.45" in html
    assert "Slip −$0.12" in html
