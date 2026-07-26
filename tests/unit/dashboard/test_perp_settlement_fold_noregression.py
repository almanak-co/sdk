"""VIB-3872 WI-3 — dashboard fold non-regression for PERP_SETTLEMENT rows.

WI-3 CONTRACT: a ``PERP_SETTLEMENT`` accounting row must be SILENTLY IGNORED by the
``quant_aggregations`` cost-basis / cost-stack / reconciliation folds — consuming
its economics is WI-4. This proves the three folds produce byte-identical output on
a DB with the new rows vs the same DB without them (forward-compatibility, no crash,
no double-count).
"""

from __future__ import annotations

import json
from decimal import Decimal

from almanak.connectors._strategy_base.runner_hook_registry import PerpSettlementState, PerpSettlementVerdict
from almanak.connectors.gmx_v2.receipt_parser import PerpFillData
from almanak.framework.accounting.perp_settlement_accounting import build_perp_settlement_event
from almanak.framework.dashboard.quant_aggregations import (
    _open_position_cost_basis,
    compute_cost_stack,
    compute_reconciliation,
)


def _row(payload: dict) -> dict:
    return {"event_type": payload["event_type"], "payload_json": json.dumps(payload), "ledger_entry_id": "l"}


def _base_events() -> list[dict]:
    return [
        _row(
            {
                "event_type": "PERP_OPEN",
                "position_key": "perp:arbitrum:gmx_v2:w:ETH",
                "protocol": "gmx_v2",
                "size": "2000",
                "cost_basis_usd": "1000",
                "confidence": "HIGH",
            }
        ),
        _row(
            {
                "event_type": "PERP_CLOSE",
                "position_key": "perp:arbitrum:gmx_v2:w:ETH",
                "protocol": "gmx_v2",
                "size": "2000",
                "realized_pnl_usd": "-30.96",
                "confidence": "HIGH",
            }
        ),
    ]


def _settlement_row() -> dict:
    fill = PerpFillData(
        is_open=False,
        is_long=False,
        position_key="perp:arbitrum:gmx_v2:w:ETH",
        order_key="0x" + "ab" * 32,
        exit_price=Decimal("1.44e-19"),
        size_delta_usd=Decimal("7183.75"),
        realized_pnl_usd=Decimal("-30.96"),
        funding_fee_usd=Decimal("2.40"),
        position_fee_usd=Decimal("2.87"),
        keeper_tx_hash="0xkeeper",
    )
    verdict = PerpSettlementVerdict(
        order_key="0x" + "ab" * 32,
        state=PerpSettlementState.EXECUTED,
        terminal=True,
        fill_data=fill,
        keeper_tx_hash="0xkeeper",
    )
    ev = build_perp_settlement_event(
        verdict=verdict,
        submission_ledger_entry_id="l",
        deployment_id="d",
        cycle_id="c",
        execution_mode="paper",
        chain="arbitrum",
        protocol="gmx_v2",
        wallet_address="0xw",
        is_open=False,
    )
    return {"event_type": ev.event_type, "payload_json": ev.to_payload_json(), "ledger_entry_id": "l"}


def test_cost_basis_fold_ignores_perp_settlement() -> None:
    base = _base_events()
    with_settlement = [*base, _settlement_row()]
    assert _open_position_cost_basis(base) == _open_position_cost_basis(with_settlement)


def test_cost_stack_fold_ignores_perp_settlement() -> None:
    base = _base_events()
    with_settlement = [*base, _settlement_row()]
    assert repr(compute_cost_stack([], base)) == repr(compute_cost_stack([], with_settlement))


def test_reconciliation_fold_ignores_perp_settlement() -> None:
    base = _base_events()
    with_settlement = [*base, _settlement_row()]
    cs_base = compute_cost_stack([], base)
    cs_with = compute_cost_stack([], with_settlement)
    r_base = compute_reconciliation(Decimal("1000"), Decimal("970"), cs_base, base)
    r_with = compute_reconciliation(Decimal("1000"), Decimal("970"), cs_with, with_settlement)
    assert repr(r_base) == repr(r_with)
