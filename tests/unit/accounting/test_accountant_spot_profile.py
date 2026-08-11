"""Load-bearing S1–S4 controls for the dedicated spot/TA scorecard."""

from __future__ import annotations

import json

from almanak.framework.accounting.accountant_test import SCORECARD_PROFILES, _cells_spot
from almanak.framework.primitives.types import Primitive


def _rows() -> tuple[list[dict], dict[str, dict], list[dict]]:
    events = [
        {"id": "buy", "event_type": "SWAP", "timestamp": "2026-08-11T00:01:00+00:00"},
        {"id": "sell", "event_type": "SWAP", "timestamp": "2026-08-11T00:03:00+00:00"},
    ]
    payloads = {
        "buy": {
            "event_type": "SWAP",
            "token_in": "USDC",
            "token_out": "WETH",
            "amount_in": "4",
            "amount_out": "0.002",
            "amount_in_usd": "4",
            "amount_out_usd": "3.99",
            "unmatched_amount_in": "4",
            "unmatched_proceeds_usd": "4",
            "cost_basis_recorded": True,
        },
        "sell": {
            "event_type": "SWAP",
            "token_in": "WETH",
            "token_out": "USDC",
            "amount_in": "0.002",
            "amount_out": "4.01",
            "amount_in_usd": "4.02",
            "amount_out_usd": "4.01",
            "realized_pnl_usd": "0.03",
            "realized_pnl_usd_matched": "0.03",
            "unmatched_amount_in": "0",
            "unmatched_proceeds_usd": "0",
            "cost_basis_recorded": True,
        },
    }
    positions = {
        "schema_version": 1,
        "positions": [],
        "metadata": {
            "swap_inventory": {
                "cost_usd": "3.99",
                "tokens": {
                    "weth": {
                        "quantity": "0.002",
                        "cost_usd": "3.99",
                        "value_usd": "4.00",
                    }
                },
            }
        },
    }
    snapshots = [
        {
            "id": 1,
            "timestamp": "2026-08-11T00:02:00+00:00",
            "positions_json": json.dumps(positions),
            "wallet_balances_json": json.dumps(
                [{"symbol": "WETH", "balance": "0.002", "value_usd": "4.00", "price_usd": "2000"}]
            ),
        }
    ]
    return events, payloads, snapshots


def _by_id(events: list[dict], payloads: dict[str, dict], snapshots: list[dict]):
    return {cell.cell_id: cell for cell in _cells_spot(events, snapshots, payloads, {})}


def test_spot_profile_is_registered_as_atomic_swap() -> None:
    profile = SCORECARD_PROFILES["spot"]
    assert profile.canonical_primitive is Primitive.SWAP
    assert profile.required_lifecycle == ()


def test_complete_round_trip_passes_all_spot_cells() -> None:
    cells = _by_id(*_rows())
    assert {cell_id: cell.status for cell_id, cell in cells.items()} == {
        "S1": "PASS",
        "S2": "PASS",
        "S3": "PASS",
        "S4": "PASS",
    }


def test_s1_rejects_non_closing_pair() -> None:
    events, payloads, snapshots = _rows()
    payloads["sell"]["token_out"] = "DAI"
    assert _by_id(events, payloads, snapshots)["S1"].status == "FAIL"


def test_s2_rejects_realized_pnl_that_disagrees_with_fifo_replay() -> None:
    events, payloads, snapshots = _rows()
    payloads["sell"]["realized_pnl_usd_matched"] = "0"
    assert _by_id(events, payloads, snapshots)["S2"].status == "FAIL"


def test_s3_rejects_inventory_mark_that_disagrees_with_wallet() -> None:
    events, payloads, snapshots = _rows()
    wallet = json.loads(snapshots[0]["wallet_balances_json"])
    wallet[0]["value_usd"] = "3.50"
    snapshots[0]["wallet_balances_json"] = json.dumps(wallet)
    assert _by_id(events, payloads, snapshots)["S3"].status == "FAIL"


def test_s3_rejects_self_consistent_inventory_and_wallet_marks_with_wrong_arithmetic() -> None:
    events, payloads, snapshots = _rows()
    wallet = json.loads(snapshots[0]["wallet_balances_json"])
    wallet[0]["value_usd"] = "3.50"
    snapshots[0]["wallet_balances_json"] = json.dumps(wallet)
    positions = json.loads(snapshots[0]["positions_json"])
    positions["metadata"]["swap_inventory"]["tokens"]["weth"]["value_usd"] = "3.50"
    snapshots[0]["positions_json"] = json.dumps(positions)
    assert _by_id(events, payloads, snapshots)["S3"].status == "FAIL"


def test_s4_rejects_inventory_basis_that_disagrees_with_acquisition_replay() -> None:
    events, payloads, snapshots = _rows()
    positions = json.loads(snapshots[0]["positions_json"])
    positions["metadata"]["swap_inventory"]["tokens"]["weth"]["cost_usd"] = "3.50"
    snapshots[0]["positions_json"] = json.dumps(positions)
    assert _by_id(events, payloads, snapshots)["S4"].status == "FAIL"
