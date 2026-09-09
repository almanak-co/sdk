"""Load-bearing S1–S4 controls for the dedicated spot/TA scorecard."""

from __future__ import annotations

import json

import pytest

from almanak.framework.accounting.accountant_test import SCORECARD_PROFILES, _cells_spot
from almanak.framework.primitives.types import Primitive


def _rows() -> tuple[list[dict], dict[str, dict], list[dict]]:
    events = [
        {"id": "buy", "event_type": "SWAP", "timestamp": "2026-08-11T00:01:00+00:00"},
        {"id": "sell", "event_type": "SWAP", "timestamp": "2026-08-11T00:03:00+00:00"},
    ]
    for event in events:
        event.update(deployment_id="deployment:test", chain="base", position_key="swap:base:0x1234")
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
            "deployment_id": "deployment:test",
            "chain": "base",
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


def _append_consolidation(events, payloads):
    events.append({**events[-1], "id": "consolidate", "timestamp": "2026-08-11T00:04:00+00:00"})
    payloads["consolidate"] = {
        **payloads["sell"],
        "token_in": "USDC",
        "token_out": "DAI",
        "amount_in": "4.01",
        "amount_out": "4",
        "amount_in_usd": "4.01",
        "amount_out_usd": "4",
        "realized_pnl_usd": "0",
        "realized_pnl_usd_matched": "0",
    }


def test_s1_closed_lot_survives_terminal_consolidation():
    events, payloads, snapshots = _rows()
    _append_consolidation(events, payloads)
    cells = _by_id(events, payloads, snapshots)
    assert all(cell.status == "PASS" for cell in cells.values())
    assert "buy -> sell" in cells["S1"].diagnostic
    assert "buy -> consolidate" not in cells["S1"].diagnostic


def test_s1_requires_the_actual_close_even_with_consolidation():
    events, payloads, snapshots = _rows()
    _append_consolidation(events, payloads)
    events.pop(1)
    assert _by_id(events, payloads, snapshots)["S1"].status == "FAIL"


def test_s1_accumulates_partial_disposals_until_the_lot_is_closed():
    events, payloads, snapshots = _rows()
    payloads["sell"].update(
        amount_in="0.001",
        amount_out="2.005",
        amount_in_usd="2.01",
        amount_out_usd="2.005",
        realized_pnl_usd="0.015",
        realized_pnl_usd_matched="0.015",
    )
    assert _by_id(events, payloads, snapshots)["S1"].status == "FAIL"
    events.append({**events[-1], "id": "sell-rest", "timestamp": "2026-08-11T00:04:00+00:00"})
    payloads["sell-rest"] = dict(payloads["sell"])
    cells = _by_id(events, payloads, snapshots)
    assert cells["S1"].status == "PASS"
    assert cells["S2"].status == "PASS"
    assert "buy -> sell,sell-rest" in cells["S1"].diagnostic


@pytest.mark.parametrize(
    "field,value",
    [
        ("cost_basis_recorded", False),
        ("amount_out_usd", None),
        ("amount_out_usd", ""),
        ("amount_out_usd", "0"),
        ("amount_out_usd", "NaN"),
        ("amount_out_usd", "Infinity"),
        ("amount_in", "0"),
        ("amount_out", "-1"),
    ],
)
def test_s1_later_good_pair_cannot_hide_bad_acquisition_basis(field, value):
    events, payloads, snapshots = _rows()
    payloads["buy"][field] = value
    more, other, _ = _rows()
    for event in more:
        old_id = event["id"]
        event["id"] = "later-" + old_id
        event["position_key"] = "swap:base:0x5678"
        events.append(event)
        payloads[event["id"]] = other[old_id]
    assert _by_id(events, payloads, snapshots)["S1"].status == "FAIL"


@pytest.mark.parametrize(
    "field,value",
    [
        ("deployment_id", "deployment:other"),
        ("chain", "ethereum"),
        ("position_key", "swap:base:0x5678"),
        ("position_key", ""),
        ("chain", ""),
        ("deployment_id", ""),
    ],
)
def test_s1_never_matches_a_different_or_unmeasured_accounting_scope(field, value):
    events, payloads, snapshots = _rows()
    events[1][field] = value
    assert _by_id(events, payloads, snapshots)["S1"].status == "FAIL"


def test_s1_uses_payload_swap_key_when_row_position_key_is_empty():
    events, payloads, snapshots = _rows()
    for event in events:
        payloads[event["id"]]["swap_position_key"] = event.pop("position_key")
    assert _by_id(events, payloads, snapshots)["S1"].status == "PASS"
    payloads["sell"]["swap_position_key"] = "swap:base:0x5678"
    assert _by_id(events, payloads, snapshots)["S1"].status == "FAIL"


@pytest.mark.parametrize("unmatched", [None, "", "0.001", "NaN"])
def test_s1_requires_measured_fully_matched_disposal(unmatched):
    events, payloads, snapshots = _rows()
    payloads["sell"]["unmatched_amount_in"] = unmatched
    assert _by_id(events, payloads, snapshots)["S1"].status == "FAIL"


def test_s1_rejects_persisted_zero_when_disposal_exceeds_acquired_quantity():
    events, payloads, snapshots = _rows()
    payloads["sell"]["amount_in"] = "0.003"
    cells = _by_id(events, payloads, snapshots)
    assert cells["S1"].status == "FAIL"
    assert cells["S2"].status == "FAIL"
    assert "FIFO replay" in cells["S2"].diagnostic


def test_s1_does_not_match_a_sell_before_its_acquisition():
    events, payloads, snapshots = _rows()
    events.reverse()
    assert _by_id(events, payloads, snapshots)["S1"].status == "FAIL"


def test_s1_respects_fifo_provenance_instead_of_reversed_symbol_presence():
    events, payloads, snapshots = _rows()
    older = {**events[0], "id": "older", "timestamp": "2026-08-11T00:00:00+00:00"}
    events.insert(0, older)
    payloads["older"] = {**payloads["buy"], "token_in": "DAI"}
    # The only sale consumes the older DAI-funded lot, not the USDC-funded one.
    assert _by_id(events, payloads, snapshots)["S1"].status == "FAIL"


def test_s1_explicit_different_token_addresses_do_not_match():
    events, payloads, snapshots = _rows()
    payloads["buy"]["token_out"] = "0x" + "1" * 40
    payloads["sell"]["token_in"] = "0x" + "2" * 40
    assert _by_id(events, payloads, snapshots)["S1"].status == "FAIL"


def test_s1_preserves_a_closed_lot_with_interleaved_other_wallet_activity():
    events, payloads, snapshots = _rows()
    events.insert(1, {**events[0], "id": "other-wallet", "position_key": "swap:base:0x5678"})
    payloads["other-wallet"] = dict(payloads["buy"])
    cells = _by_id(events, payloads, snapshots)
    assert cells["S1"].status == "PASS"
    assert cells["S2"].status == "PASS"


def test_s1_measured_zero_realized_pnl_still_closes_the_lot():
    events, payloads, snapshots = _rows()
    payloads["sell"].update(amount_in_usd="3.99", realized_pnl_usd="0", realized_pnl_usd_matched="0")
    cells = _by_id(events, payloads, snapshots)
    assert cells["S1"].status == "PASS"
    assert cells["S2"].status == "PASS"


def test_s1_solana_mint_case_is_identity_not_a_symbol_alias():
    events, payloads, snapshots = _rows()
    for event in events:
        event.update(chain="solana", position_key="swap:solana:WalletIdentity")
    mint = "A" * 32
    payloads["buy"]["token_out"] = mint
    payloads["sell"]["token_in"] = mint.lower()
    assert _by_id(events, payloads, snapshots)["S1"].status == "FAIL"
    payloads["sell"]["token_in"] = mint
    assert _by_id(events, payloads, snapshots)["S1"].status == "PASS"


def test_explicit_conflicting_row_and_payload_wallet_keys_fail_closed():
    events, payloads, snapshots = _rows()
    payloads["buy"]["swap_position_key"] = "swap:base:0x5678"
    cells = _by_id(events, payloads, snapshots)
    assert cells["S1"].status == "FAIL"
    assert cells["S2"].status == "FAIL"
    assert cells["S4"].status == "FAIL"


def test_matching_explicit_keys_preserve_evm_wallet_case_normalization():
    events, payloads, snapshots = _rows()
    for row in events:
        row["position_key"] = "swap:base:0xabCd"
        payloads[row["id"]]["swap_position_key"] = "swap:base:0xABcD"
    assert _by_id(events, payloads, snapshots)["S1"].status == "PASS"


@pytest.mark.parametrize("different_scope", ["chain", "deployment"])
def test_s4_snapshot_excludes_lots_from_another_proven_scope(different_scope):
    events, payloads, snapshots = _rows()
    other = {**events[0], "id": "other-scope"}
    if different_scope == "chain":
        other.update(chain="ethereum", position_key="swap:ethereum:0x1234")
    else:
        other["deployment_id"] = "deployment:other"
    events.insert(1, other)
    payloads["other-scope"] = dict(payloads["buy"])
    assert _by_id(events, payloads, snapshots)["S4"].status == "PASS"
    positions = json.loads(snapshots[0]["positions_json"])
    positions["metadata"]["swap_inventory"]["tokens"]["weth"]["cost_usd"] = "7.98"
    snapshots[0]["positions_json"] = json.dumps(positions)
    assert _by_id(events, payloads, snapshots)["S4"].status == "FAIL"


def test_s4_same_deployment_chain_with_two_wallets_is_ambiguous():
    events, payloads, snapshots = _rows()
    events.insert(1, {**events[0], "id": "other-wallet", "position_key": "swap:base:0x5678"})
    payloads["other-wallet"] = dict(payloads["buy"])
    positions = json.loads(snapshots[0]["positions_json"])
    positions["metadata"]["swap_inventory"]["tokens"]["weth"]["cost_usd"] = "7.98"
    snapshots[0]["positions_json"] = json.dumps(positions)
    cell = _by_id(events, payloads, snapshots)["S4"]
    assert cell.status == "FAIL"
    assert "ambiguous" in cell.diagnostic


@pytest.mark.parametrize("field", ["deployment_id", "chain"])
def test_s4_does_not_infer_missing_snapshot_scope(field):
    events, payloads, snapshots = _rows()
    snapshots[0].pop(field)
    assert _by_id(events, payloads, snapshots)["S4"].status == "FAIL"


def test_s1_closure_evidence_remains_independent_of_s2_pnl_mismatch():
    events, payloads, snapshots = _rows()
    payloads["sell"]["realized_pnl_usd_matched"] = "999"
    cells = _by_id(events, payloads, snapshots)
    assert cells["S1"].status == "PASS"
    assert cells["S2"].status == "FAIL"
    assert "realized_pnl_usd_matched" in cells["S2"].diagnostic


def _thirds_rows():
    events, payloads, snapshots = _rows()
    payloads["buy"].update(
        amount_in="2",
        amount_out="3",
        amount_in_usd="2",
        amount_out_usd="2",
        unmatched_amount_in="2",
        unmatched_proceeds_usd="2",
    )
    payloads["sell"].update(
        amount_in="1",
        amount_out="1",
        amount_in_usd="1",
        amount_out_usd="1",
        realized_pnl_usd="0.3333333333333333333333333334",
        realized_pnl_usd_matched="0.3333333333333333333333333334",
    )
    for index in (2, 3):
        row_id = f"sell-{index}"
        events.append({**events[1], "id": row_id, "timestamp": f"2026-08-11T00:0{index + 2}:00+00:00"})
        payloads[row_id] = dict(payloads["sell"])
    # The held one-third basis is independently fixed at the 28-digit Decimal oracle.
    positions = json.loads(snapshots[0]["positions_json"])
    positions["metadata"]["swap_inventory"]["tokens"]["weth"].update(
        quantity="1", cost_usd="0.6666666666666666666666666666", value_usd="1"
    )
    snapshots[0].update(
        timestamp="2026-08-11T00:04:30+00:00",
        positions_json=json.dumps(positions),
        wallet_balances_json=json.dumps([{"symbol": "WETH", "balance": "1", "value_usd": "1", "price_usd": "1"}]),
    )
    return events, payloads, snapshots


def test_sequential_thirds_preserve_original_acquisition_basis():
    cells = _by_id(*_thirds_rows())
    assert {key: cell.status for key, cell in cells.items()} == {"S1": "PASS", "S2": "PASS", "S3": "PASS", "S4": "PASS"}


def test_partial_thirds_do_not_claim_full_lot_closure():
    events, payloads, snapshots = _thirds_rows()
    cells = _by_id(events[:-1], payloads, snapshots)
    assert cells["S1"].status == "FAIL"
    assert cells["S2"].status == "PASS"
    assert cells["S4"].status == "PASS"


def test_thirds_replay_still_rejects_small_pnl_discrepancy():
    events, payloads, snapshots = _thirds_rows()
    payloads["sell-2"]["realized_pnl_usd_matched"] = "0.3333333333333333333333333335"
    cells = _by_id(events, payloads, snapshots)
    assert cells["S1"].status == "PASS"
    assert cells["S2"].status == "FAIL"
    assert "realized_pnl_usd_matched" in cells["S2"].diagnostic


def test_thirds_replay_still_rejects_small_open_basis_discrepancy():
    events, payloads, snapshots = _thirds_rows()
    positions = json.loads(snapshots[0]["positions_json"])
    positions["metadata"]["swap_inventory"]["tokens"]["weth"]["cost_usd"] = "0.6666666666666666666666666665"
    snapshots[0]["positions_json"] = json.dumps(positions)
    cells = _by_id(events, payloads, snapshots)
    assert cells["S2"].status == "PASS"
    assert cells["S4"].status == "FAIL"
    assert "cost_usd" in cells["S4"].diagnostic


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
