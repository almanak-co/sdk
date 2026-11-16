"""Prepared integer GM calculations match actual same-clock executor transfers and supply."""

import hashlib
import json
from pathlib import Path

import pytest

from almanak.connectors.gmx_v2.economic_deposit_execution import execute_deposit
from almanak.connectors.gmx_v2.economic_liquidity import LiquidityState, quote_withdrawal
from almanak.connectors.gmx_v2.economic_liquidity_risk import (
    LiquidityRiskState,
    validate_deposit,
    validate_withdrawal,
)
from almanak.connectors.gmx_v2.economic_valuation import pool_value
from almanak.connectors.gmx_v2.economic_valuation_types import ValuationState

ROOT = Path("docs/internal/reports/evidence/2026-09-09-gmx-finish/arbitrum-gm-executor")


def test_native_gm_executor_packet_is_sealed_and_full_lifecycle_passed():
    manifest = json.loads((ROOT / "manifest.json").read_text())
    for name, digest in manifest["files"].items():
        assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest
    result = json.loads((ROOT / "result.json").read_text())
    assert result["status"] == "passed" and result["comparisons"] == 4
    assert result["phases"][-1] == "closed"
    assert "gateway_replaced_and_resumed" in result["phases"]


@pytest.mark.parametrize("index", range(4))
def test_native_gm_executor_exact_transfer_and_share_supply(index):
    row = json.loads((ROOT / "comparisons.json").read_text())[index]
    state = ValuationState.model_validate_json(json.dumps(row["state"]))
    risk = LiquidityRiskState.model_validate_json(json.dumps(row["risk"]))
    amounts = [value["amount"] for value in row["request"]["escrow"]]
    if row["kind"] == "deposit":
        calculation = execute_deposit(state, *amounts)
        validate_deposit(state, risk, calculation)
        expected = [0, 0, calculation.shares]
        shares = calculation.shares
    else:
        value = pool_value(state, maximize=False).pool_usd_e30
        calculation = quote_withdrawal(
            LiquidityState(
                action="WITHDRAWAL",
                spot=state.spot,
                pool_value_usd_e30=value,
                share_supply=state.share_supply,
            ),
            amounts[0],
        )
        validate_withdrawal(state, risk, calculation)
        expected = [calculation.long_amount, calculation.short_amount, 0]
        shares = -calculation.shares
    assert calculation.model_dump(mode="json") == row["calculation"]
    assert expected == row["actual"] == row["expected"]
    assert shares == row["share_supply_delta"] == row["expected_share_supply_delta"]
    assert row["settled_block"]["timestamp"] == state.spot.block.timestamp
    assert row["receipt"]["status"] == "0x1"
