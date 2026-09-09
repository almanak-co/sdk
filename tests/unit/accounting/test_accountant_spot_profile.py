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
    for row in events + snapshots:
        row["chain"] = "base"
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


def _prefunded_wallet(snapshots):
    wallet = json.loads(snapshots[0]["wallet_balances_json"])
    wallet[0].update(balance="100.002", value_usd="200004.00")
    snapshots[0]["wallet_balances_json"] = json.dumps(wallet)


def test_s3_preexisting_wallet_inventory_does_not_become_acquired_inventory():
    events, payloads, snapshots = _rows()
    _prefunded_wallet(snapshots)
    assert _by_id(events, payloads, snapshots)["S3"].status == "PASS"


def test_s3_wrong_quantity_below_wallet_coverage_still_fails_fifo_replay():
    events, payloads, snapshots = _rows()
    _prefunded_wallet(snapshots)
    positions = json.loads(snapshots[0]["positions_json"])
    positions["metadata"]["swap_inventory"]["tokens"]["weth"].update(quantity="0.001", value_usd="2.00")
    snapshots[0]["positions_json"] = json.dumps(positions)
    cell = _by_id(events, payloads, snapshots)["S3"]
    assert cell.status == "FAIL"
    assert "independent FIFO replay" in cell.diagnostic


def test_s3_correct_fifo_quantity_without_wallet_coverage_fails():
    events, payloads, snapshots = _rows()
    wallet = json.loads(snapshots[0]["wallet_balances_json"])
    wallet[0].update(balance="0.001", value_usd="2.00")
    snapshots[0]["wallet_balances_json"] = json.dumps(wallet)
    assert _by_id(events, payloads, snapshots)["S3"].status == "FAIL"


def test_s3_prefix_orders_parsed_instants_not_lexical_offsets_or_input_order():
    events, payloads, snapshots = _rows()
    events[0]["timestamp"] = "2026-08-10T20:01:00-04:00"
    events[1]["timestamp"] = "2026-08-10T20:03:00-04:00"
    assert _by_id(list(reversed(events)), payloads, snapshots)["S3"].status == "PASS"


def test_s3_ambiguous_same_second_snapshot_cannot_prove_event_inclusion():
    events, payloads, snapshots = _rows()
    snapshots[0]["timestamp"] = events[0]["timestamp"]
    assert _by_id(events, payloads, snapshots)["S3"].status == "FAIL"


def test_s3_non_swap_inventory_movements_need_independent_replay_provenance():
    events, payloads, snapshots = _rows()
    events.append(
        {"id": "transfer", "event_type": "TRANSFER", "chain": "base", "timestamp": "2026-08-11T00:01:30+00:00"}
    )
    cell = _by_id(events, payloads, snapshots)["S3"]
    assert cell.status == "FAIL"
    assert "non-SWAP" in cell.diagnostic


def test_native_anvil_prefunded_snapshot_replay_preserves_measured_acquisition():
    from pathlib import Path

    fixture = Path(__file__).parents[2] / "fixtures/accounting/spot_native_prefunded_snapshot.json"
    data = json.loads(fixture.read_text())
    cells = _by_id(data["events"], data["payloads"], data["snapshots"])
    assert {key: cell.status for key, cell in cells.items()} == {"S1": "PASS", "S2": "PASS", "S3": "PASS", "S4": "PASS"}


def test_s3_same_symbol_and_prefunded_wallet_cannot_mix_chains():
    events, payloads, snapshots = _rows()
    _prefunded_wallet(snapshots)
    for row in events:
        row["chain"] = "arbitrum"
    cell = _by_id(events, payloads, snapshots)["S3"]
    assert cell.status == "FAIL"
    assert "chain scope" in cell.diagnostic


def test_s3_missing_chain_evidence_cannot_establish_inventory_ownership():
    events, payloads, snapshots = _rows()
    events[0].pop("chain")
    assert _by_id(events, payloads, snapshots)["S3"].status == "FAIL"


def test_s3_canonical_chain_alias_preserves_same_scope():
    events, payloads, snapshots = _rows()
    events[0]["chain"] = "BASE"
    assert _by_id(events, payloads, snapshots)["S3"].status == "PASS"


def _same_second_native_rows():
    from pathlib import Path

    fixture = Path(__file__).parents[2] / "fixtures/accounting/spot_native_same_second_snapshot.json"
    return json.loads(fixture.read_text())


def _same_second_s3(data):
    return next(
        cell
        for cell in _cells_spot(data["events"], data["snapshots"], data["payloads"], {}, data["ledger"])
        if cell.cell_id == "S3"
    )


def test_real_native_same_second_post_iteration_snapshot_uses_ledger_cycle_binding():
    data = _same_second_native_rows()
    assert data["events"][0]["timestamp"] == data["snapshots"][1]["timestamp"]
    assert _same_second_s3(data).status == "PASS"


def test_same_second_snapshot_without_ledger_evidence_remains_ambiguous():
    data = _same_second_native_rows()
    data["ledger"] = []
    assert _same_second_s3(data).status == "FAIL"


def test_same_second_teardown_pre_snapshot_cannot_use_shared_cycle_as_order():
    data = _same_second_native_rows()
    for row in (data["events"][0], data["snapshots"][1], data["ledger"][0]):
        row["cycle_id"] = "teardown-td_example"
    assert _same_second_s3(data).status == "FAIL"


def test_same_second_ledger_binding_rejects_wrong_identity_and_multiple_members():
    import copy

    original = _same_second_native_rows()
    for field in ("deployment_id", "cycle_id", "chain", "id", "tx_hash", "intent_type", "timestamp"):
        data = copy.deepcopy(original)
        data["ledger"][0][field] = "unrelated"
        assert _same_second_s3(data).status == "FAIL", field
    data = copy.deepcopy(original)
    data["ledger"][0]["success"] = False
    assert _same_second_s3(data).status == "FAIL"
    data = copy.deepcopy(original)
    data["ledger"].append(copy.deepcopy(data["ledger"][0]))
    assert _same_second_s3(data).status == "FAIL"


def test_same_second_cycle_binding_does_not_bypass_wrong_inventory_quantity():
    data = _same_second_native_rows()
    snapshot = data["snapshots"][1]
    positions = json.loads(snapshot["positions_json"])
    positions["metadata"]["swap_inventory"]["tokens"]["eth"]["quantity"] = "0.001"
    snapshot["positions_json"] = json.dumps(positions)
    assert _same_second_s3(data).status == "FAIL"
