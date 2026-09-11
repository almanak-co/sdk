"""FIFO mark gains and exchange value changes together explain wallet PnL."""

import json
from decimal import Decimal

import pytest

from almanak.framework.accounting.accountant_test import _cell_g6_reconciliation


def evaluate(received="7"):
    payloads = {
        "buy": {
            "token_in": "USDC",
            "token_out": "WETH",
            "amount_in": "10",
            "amount_out": "0.009",
            "amount_in_usd": "10",
            "amount_out_usd": "9",
            "realized_pnl_usd": None,
        },
        "sell": {
            "token_in": "WETH",
            "token_out": "USDC",
            "amount_in": "0.009",
            "amount_out": "7",
            "amount_in_usd": "8",
            "amount_out_usd": received,
            "realized_pnl_usd_matched": "-1",
        },
    }
    events = [
        {
            "id": key,
            "event_type": "SWAP",
            "deployment_id": "dep",
            "chain": "base",
            "position_key": "swap:base:0x0000000000000000000000000000000000000001",
            "timestamp": f"2026-09-09T00:0{i + 1}:00+00:00",
            "payload_json": json.dumps(payload),
        }
        for i, (key, payload) in enumerate(payloads.items())
    ]
    snapshots = [
        {
            "id": i,
            "deployment_id": "dep",
            "timestamp": f"2026-09-09T00:0{i * 3}:00+00:00",
            "total_value_usd": "0",
            "available_cash_usd": value,
            "value_confidence": "HIGH",
            "positions_json": "[]",
            "wallet_balances_json": json.dumps(
                [
                    {
                        "symbol": "USDC",
                        "balance": value,
                        "price_usd": "1",
                        "chain": "base",
                        "wallet_address": "0x0000000000000000000000000000000000000001",
                    }
                ]
            ),
        }
        for i, value in enumerate(("10", "7"))
    ]
    return _cell_g6_reconciliation(snapshots, [], [], events, "spot", payloads, {})


def test_round_trip_accounts_for_exchange_losses_on_both_legs():
    cell, decomposition = evaluate()
    assert cell.status == "PASS", cell.diagnostic
    assert Decimal(decomposition["Σ_swaps_usd"]) == -1
    assert Decimal(decomposition["Σ_swap_exchange_usd"]) == -2
    assert Decimal(decomposition["wallet_pnl_usd"]) == -3
    assert Decimal(decomposition["gap_usd"]) == 0


@pytest.mark.parametrize("received", [None, "", "NaN", "-1"])
def test_unmeasured_or_invalid_exchange_value_cannot_be_zero(received):
    cell, decomposition = evaluate(received)
    assert cell.status == "FAIL"
    assert decomposition["Σ_swap_exchange_usd_null_count"] == "1"
