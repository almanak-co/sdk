"""Unit tests for ``compute_position_pnl`` realized-interest sign convention.

VIB-4974: realized PnL for leveraged-lending positions must sign borrow
interest as a COST and supply yield as a GAIN.  DELEVERAGE is structurally a
repay (it routes through ``basis_store.match_repay`` and carries borrow-side
interest), so it belongs on the debt (subtract) side alongside REPAY.

These tests pin the convention at the canonical computation site so the CLI
report (``lending_report.py``) and the dashboard stay in agreement.
"""

from __future__ import annotations

import json
from decimal import Decimal

from almanak.framework.accounting.position_pnl import compute_position_pnl


def _event(event_type: str, *, principal: str | None, interest: str | None, ts: str) -> dict:
    payload: dict = {}
    if principal is not None:
        payload["principal_delta_usd"] = principal
    if interest is not None:
        payload["interest_delta_usd"] = interest
    return {
        "event_type": event_type,
        "timestamp": ts,
        "payload_json": json.dumps(payload),
        "ledger_entry_id": f"led-{event_type}-{ts}",
    }


class TestRealizedInterestSign:
    # NB: ``principal_delta_usd`` is stored as a POSITIVE magnitude for the
    # debt/supply closes (REPAY / DELEVERAGE / WITHDRAW) — the lending writer
    # sets it from ``_amount_to_usd(match_result.repaid_principal, ...)``
    # (lending_accounting.py:862-865,949-958), and ``compute_position_pnl``
    # reduces cost basis with ``cost_basis -= principal``. The tests use that
    # production shape so cost_basis assertions are physically meaningful.

    def test_repay_interest_is_a_cost(self):
        events = [
            _event("BORROW", principal="3.0", interest=None, ts="2026-06-01T00:00:00"),
            _event("REPAY", principal="3.0", interest="0.000237", ts="2026-06-01T01:00:00"),
        ]
        summary = compute_position_pnl(events)
        assert summary is not None
        # Borrow interest PAID is a realized cost → negative.
        assert summary.realized_pnl_usd < 0
        assert str(summary.realized_pnl_usd) == "-0.000237"
        # Borrowed 3, repaid 3 → principal fully returned, cost basis 0.
        assert summary.cost_basis_usd == Decimal("0.0")

    def test_deleverage_interest_is_a_cost(self):
        # VIB-4974: DELEVERAGE was omitted from the (REPAY, WITHDRAW) gate and
        # contributed nothing.  It is a debt-side close → interest is a cost.
        events = [
            _event("BORROW", principal="5.0", interest=None, ts="2026-06-01T00:00:00"),
            _event("DELEVERAGE", principal="5.0", interest="0.001500", ts="2026-06-01T02:00:00"),
        ]
        summary = compute_position_pnl(events)
        assert summary is not None
        assert summary.realized_pnl_usd < 0
        assert str(summary.realized_pnl_usd) == "-0.001500"
        assert summary.cost_basis_usd == Decimal("0.0")

    def test_withdraw_interest_is_a_gain(self):
        events = [
            _event("SUPPLY", principal="10.0", interest=None, ts="2026-06-01T00:00:00"),
            _event("WITHDRAW", principal="10.0", interest="0.000634", ts="2026-06-01T01:00:00"),
        ]
        summary = compute_position_pnl(events)
        assert summary is not None
        # Supply yield received is a realized gain → positive.
        assert summary.realized_pnl_usd > 0
        assert str(summary.realized_pnl_usd) == "0.000634"
        assert summary.cost_basis_usd == Decimal("0.0")

    def test_deleverage_participates_in_repay_principal_branch(self):
        # VIB-4974: DELEVERAGE joins the (WITHDRAW, REPAY) cost-basis branch
        # (``cost_basis -= principal``) — identical handling to REPAY.  A
        # DELEVERAGE with principal X must move cost_basis the same direction
        # and magnitude a REPAY with principal X does.
        delev = [
            _event("BORROW", principal="5.0", interest=None, ts="2026-06-01T00:00:00"),
            _event("DELEVERAGE", principal="2.0", interest=None, ts="2026-06-01T02:00:00"),
        ]
        repay = [
            _event("BORROW", principal="5.0", interest=None, ts="2026-06-01T00:00:00"),
            _event("REPAY", principal="2.0", interest=None, ts="2026-06-01T02:00:00"),
        ]
        s_delev = compute_position_pnl(delev)
        s_repay = compute_position_pnl(repay)
        assert s_delev is not None and s_repay is not None
        assert s_delev.cost_basis_usd == s_repay.cost_basis_usd
        # Borrowed 5, repaid 2 → 3 of principal still outstanding.
        assert s_delev.cost_basis_usd == Decimal("3.0")

    def test_repay_and_deleverage_both_subtract_interest(self):
        events = [
            _event("BORROW", principal="8.0", interest=None, ts="2026-06-01T00:00:00"),
            _event("REPAY", principal="3.0", interest="0.001", ts="2026-06-01T01:00:00"),
            _event("DELEVERAGE", principal="5.0", interest="0.002", ts="2026-06-01T02:00:00"),
        ]
        summary = compute_position_pnl(events)
        assert summary is not None
        # Two debt-side closes: -(0.001) + -(0.002) = -0.003.
        assert str(summary.realized_pnl_usd) == "-0.003"
        # Borrowed 8, repaid 3 + 5 = 8 → principal fully returned, cost basis 0.
        assert summary.cost_basis_usd == Decimal("0.0")
