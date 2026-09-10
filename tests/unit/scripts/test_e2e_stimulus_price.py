"""Captured swap receipts with a synthetic matching gateway observation and cycle."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from eth_abi import decode, encode
from web3 import Web3

from qa_lab.e2e_actor_quantities import validate_actor_quantity_contract
from qa_lab.e2e_card import canonical, digest, load_json
from qa_lab.e2e_lifecycle import _rows
from qa_lab.e2e_stimulus_price import POLICY, SWAP_TOPIC, validate_stimulus_price_link
from tests.unit.scripts import test_e2e_actor_quantities as actor_tests

actor_bundle = actor_tests.actor_bundle


@pytest.fixture
def evidence(actor_bundle):
    terminal_path = actor_bundle / "stimulus-terminal.json"
    terminal = load_json(terminal_path)
    terminal["fork_identity"]["chain_id"] = 42161
    terminal_path.write_bytes(canonical(terminal))
    actor = validate_actor_quantity_contract(actor_bundle, actor_tests.POLICY, terminal={"status": "PASS"})
    runtime = next(
        row for row in _rows(actor_bundle / "stimulus-quantities/almanak_state.db") if row["phase"] == "runtime"
    )
    receipt_path = actor_bundle / f"stimulus-quantities/chain/receipt-{runtime['tx_hash']}.json"
    receipt = load_json(receipt_path)
    swap = next(log for log in receipt["logs"] if log["topics"][0] == SWAP_TOPIC)
    _, _, sqrt_price, _, tick = decode(
        ["int256", "int256", "uint160", "uint128", "int24"], Web3.to_bytes(hexstr=swap["data"])
    )
    observation = {
        "pool": swap["address"],
        "block_number": int(receipt["blockNumber"], 16),
        "block_hash": receipt["blockHash"],
        "fork_identity": terminal["fork_identity"],
        "raw_reads": {
            "slot0": Web3.to_hex(
                encode(
                    ["uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool"],
                    [sqrt_price, tick, 0, 1, 1, 0, True],
                )
            )
        },
    }
    (actor_bundle / "price-observations").mkdir()
    prices = {
        "status": "PASS",
        "source_artifacts": [],
        "records": [{"line": 10, "cycle_id": "subject-close", "inputs": [{"id": "pending"}]}],
    }
    cycle = {
        "status": "PASS",
        "price_record_line": 10,
        "cycle_id": "subject-close",
        "close_transaction": "0x" + "ff" * 32,
        "source_artifacts": [],
    }
    value = SimpleNamespace(
        bundle=actor_bundle,
        actor=actor,
        prices=prices,
        cycle=cycle,
        observation=observation,
        receipt_path=receipt_path,
        receipt=receipt,
        swap=swap,
    )
    retain(value)
    return value


def retain(evidence):
    raw = canonical(evidence.observation)
    name = f"price-observations/{digest(raw)}.json"
    (evidence.bundle / name).write_bytes(raw)
    evidence.prices["records"][0]["inputs"][0]["id"] = digest(raw)
    evidence.prices["source_artifacts"] = [name]


def check(evidence):
    return validate_stimulus_price_link(
        evidence.bundle,
        {"stimulus_price_link": POLICY},
        prices=evidence.prices,
        close_cycle=evidence.cycle,
        actor_quantities=evidence.actor,
    )


def test_consumed_close_input_matches_the_actors_settled_pool_event(evidence):
    result = check(evidence)
    assert result["status"] == "PASS"
    assert result["stimulus_transaction"] == evidence.receipt["transactionHash"]
    assert result["close_transaction"] == evidence.cycle["close_transaction"]
    assert result["observation_id"] == evidence.prices["records"][0]["inputs"][0]["id"]
    assert evidence.receipt_path.relative_to(evidence.bundle).as_posix() in result["source_artifacts"]


@pytest.mark.parametrize("fault", ["price", "tick", "block", "block_hash", "fork", "wrong_pool", "short_slot0"])
def test_rehashed_gateway_observation_cannot_describe_another_settled_price(evidence, fault):
    observed = evidence.observation
    if fault in {"price", "tick"}:
        types = ["uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool"]
        values = list(decode(types, Web3.to_bytes(hexstr=observed["raw_reads"]["slot0"])))
        values[0 if fault == "price" else 1] += 1
        observed["raw_reads"]["slot0"] = Web3.to_hex(encode(types, values))
    elif fault == "block":
        observed["block_number"] += 1
    elif fault == "block_hash":
        observed["block_hash"] = "0x" + "cc" * 32
    elif fault == "fork":
        observed["fork_identity"] = {**observed["fork_identity"], "instance_id": "other-fork"}
    elif fault == "wrong_pool":
        observed["pool"] = "0x" + "cc" * 20
    else:
        observed["raw_reads"]["slot0"] = "0x"
    retain(evidence)
    with pytest.raises(ValueError):
        check(evidence)


@pytest.mark.parametrize("fault", ["missing", "duplicate", "block", "direction"])
def test_stimulus_receipt_must_have_one_consistent_price_moving_pool_event(evidence, fault):
    receipt, swap = evidence.receipt, evidence.swap
    if fault == "missing":
        receipt["logs"].remove(swap)
    elif fault == "duplicate":
        receipt["logs"].append(deepcopy(swap))
    elif fault == "block":
        swap["blockNumber"] = "0x1"
    else:
        types = ["int256", "int256", "uint160", "uint128", "int24"]
        values = list(decode(types, Web3.to_bytes(hexstr=swap["data"])))
        values[0] = abs(values[0])
        swap["data"] = Web3.to_hex(encode(types, values))
    evidence.receipt_path.write_bytes(canonical(receipt))
    with pytest.raises(ValueError):
        check(evidence)


def test_another_close_cycle_cannot_borrow_the_price_link(evidence):
    evidence.cycle["cycle_id"] = "unrelated-close"
    with pytest.raises(ValueError, match="exact consumed close-cycle"):
        check(evidence)


def test_unknown_quantities_cannot_be_replaced_by_a_matching_price(evidence):
    evidence.actor["status"] = "UNMEASURED"
    with pytest.raises(ValueError, match="admitted quantities"):
        check(evidence)
