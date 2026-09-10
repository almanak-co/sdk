"""Captured NFT closure plus synthetic replacement, cycle rows and price records."""

import json
import sqlite3
from copy import deepcopy
from types import SimpleNamespace

import pytest

from qa_lab.e2e_card import canonical
from qa_lab.e2e_generation_verdict import _replay, generation_predicates
from qa_lab.e2e_rebalance_input import POLICY, validate_rebalance_input
from tests.unit.scripts import test_e2e_generation_verdict as generation_tests

captured = generation_tests.captured
rebalanced = generation_tests.rebalanced


@pytest.fixture
def evidence(tmp_path, rebalanced):
    opened, managed = rebalanced
    generations = generation_predicates(opened, managed, None)
    narrow = generations["rebalance"]["narrow_original"]
    wide = generations["rebalance"]["wide_held"]
    order = _replay(managed)[narrow]["close_order"]
    close = next(event for event in managed["logs"] if (event["block_number"], event["log_index"]) == order)
    for name, value in (("positions-open.json", opened), ("positions-managed.json", managed)):
        (tmp_path / name).write_bytes(canonical(value))
    state = {"path": "almanak_state.db", "deployment_id": "deployment:123456789abc"}
    cycle = "11111111-1111-4111-8111-111111111111"
    with sqlite3.connect(tmp_path / state["path"]) as db:
        db.execute(
            "CREATE TABLE transaction_ledger (id INTEGER, deployment_id TEXT, cycle_id TEXT, timestamp TEXT, intent_type TEXT, success INTEGER, extracted_data_json TEXT)"
        )
        db.execute(
            "INSERT INTO transaction_ledger VALUES (1, ?, ?, ?, 'LP_CLOSE', 1, ?)",
            (
                state["deployment_id"],
                cycle,
                "2026-09-07T10:00:01+00:00",
                json.dumps({"all_tx_results": [{"tx_hash": close["transaction_hash"], "success": True}]}),
            ),
        )
        db.execute(
            "INSERT INTO transaction_ledger VALUES (2, ?, ?, ?, 'SWAP', 1, '{}')",
            (
                state["deployment_id"],
                "teardown-td_123456789abc",
                "2026-09-07T11:00:01+00:00",
            ),
        )
        db.execute(
            "CREATE TABLE teardown_requests (deployment_id TEXT, status TEXT, positions_failed INTEGER, started_at TEXT, completed_at TEXT)"
        )
        db.execute(
            "INSERT INTO teardown_requests VALUES (?, 'completed', 0, ?, ?)",
            (
                state["deployment_id"],
                "2026-09-07T11:00:00+00:00",
                "2026-09-07T11:00:02+00:00",
            ),
        )
    prices = {
        "status": "PASS",
        "deployment_id": state["deployment_id"],
        "source_artifacts": ["run.log"],
        "records": [
            {
                "cycle_id": cycle,
                "phase": "both_open",
                "position_ids": [narrow, wide],
                "line": 10,
                "logged_at": "2026-09-07T10:00:00+00:00",
                "token0_price": "1000000",
                "token1_price": "1",
                "inputs": [{"block": opened["end_block"]}, {"block": opened["end_block"]}],
            }
        ],
    }
    return SimpleNamespace(bundle=tmp_path, state=state, prices=prices, generations=generations, close=close)


def check(evidence):
    return validate_rebalance_input(
        evidence.bundle,
        {"rebalance_price_cycle": POLICY},
        prices=evidence.prices,
        generations=evidence.generations,
        state=evidence.state,
    )


def test_close_is_tied_to_exact_cycle_and_original_position_pair(evidence):
    result = check(evidence)
    assert result["status"] == "PASS"
    assert result["close_transaction"] == evidence.close["transaction_hash"]
    assert result["price_record_line"] == 10
    assert result["stimulus_causality"] == "UNMEASURED"
    assert evidence.state["path"] in result["source_artifacts"]


@pytest.mark.parametrize(
    "fault", ["other_cycle", "missing", "duplicate", "phase", "position", "late_log", "after_close", "in_range"]
)
def test_recent_or_unrelated_price_records_cannot_certify_the_close(evidence, fault):
    record = evidence.prices["records"][0]
    if fault == "other_cycle":
        record["cycle_id"] = "22222222-2222-4222-8222-222222222222"
    elif fault == "missing":
        evidence.prices["records"] = []
    elif fault == "duplicate":
        evidence.prices["records"].append(deepcopy(record))
    elif fault == "phase":
        record["phase"] = "init"
    elif fault == "position":
        record["position_ids"].reverse()
    elif fault == "late_log":
        record["logged_at"] = "2026-09-07T10:00:02+00:00"
    elif fault == "after_close":
        record["inputs"][0]["block"] = evidence.close["block_number"]
    else:
        from decimal import Decimal, localcontext

        opened = json.loads((evidence.bundle / "positions-open.json").read_text())
        ticks = _replay(opened)[evidence.generations["rebalance"]["narrow_original"]]["ticks"]
        with localcontext() as arithmetic:
            arithmetic.prec = 78
            record["token0_price"] = str(Decimal("1.0001") ** ((ticks[0] + ticks[1]) // 2) * Decimal(10**12))
    with pytest.raises(ValueError, match="close|Close|Narrow"):
        check(evidence)


def test_unrelated_runtime_close_cannot_borrow_generation_transaction(evidence):
    with sqlite3.connect(evidence.bundle / evidence.state["path"]) as db:
        db.execute(
            "UPDATE transaction_ledger SET extracted_data_json=? WHERE id=1",
            (json.dumps({"all_tx_results": [{"tx_hash": "0x" + "aa" * 32, "success": True}]}),),
        )
    with pytest.raises(ValueError, match="one successful runtime SDK close"):
        check(evidence)


def test_legacy_contract_does_not_invent_a_cycle_claim(tmp_path):
    assert validate_rebalance_input(tmp_path, {}, prices=None, generations=None, state=None) is None
