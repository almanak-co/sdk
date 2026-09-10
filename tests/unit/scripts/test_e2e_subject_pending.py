from copy import deepcopy

import pytest

from qa_lab.e2e_actor_terminal import capture_actor_terminal
from qa_lab.e2e_card import canonical
from qa_lab.e2e_residual_policy import assess_residual_policy
from qa_lab.e2e_subject_pending import subject_pending_predicate
from tests.unit.scripts import test_e2e_actor_terminal as actor_tests
from tests.unit.scripts.test_e2e_residual_policy import POLICY

fork = actor_tests.fork
WALLET = "0x" + "ab" * 20


@pytest.fixture
def witnesses(fork):
    raw = capture_actor_terminal(fork.context)
    subject = {
        "wallet": WALLET,
        "fork_identity": raw["fork_identity"],
        "end_block": raw["block_number"],
        "end_block_hash": raw["block_hash"],
    }
    return raw, subject


@pytest.mark.parametrize("kind", ["pending", "queued"])
def test_subject_queue_cannot_hide_behind_empty_actor_queue(witnesses, kind):
    raw, subject = witnesses
    raw["txpool"][kind][WALLET] = {"0x1": {"hash": "0x" + "11" * 32}}
    result = subject_pending_predicate(raw, subject)
    assert result["status"] == "FAIL"
    assert result["pending_transactions"] == 1


def test_empty_observation_does_not_prove_producer_quiescence(witnesses):
    result = subject_pending_predicate(*witnesses)
    assert result["status"] == "PASS"
    assert result["pending_transactions"] == 0
    assert result["producer_quiescence"] == "UNMEASURED"


@pytest.mark.parametrize("mutation", ["fork", "head", "missing_queue", "ambiguous", "malformed"])
def test_incomplete_or_cross_bound_txpool_never_passes(witnesses, mutation):
    raw, subject = deepcopy(witnesses)
    if mutation == "fork":
        subject["fork_identity"] = {**subject["fork_identity"], "fork_block": 99}
    elif mutation == "head":
        subject["end_block"] += 1
    elif mutation == "missing_queue":
        del raw["txpool"]["queued"]
    elif mutation == "ambiguous":
        raw["txpool"]["pending"] = {WALLET: {}, "0x" + "AB" * 20: {}}
    else:
        raw["txpool"]["pending"][WALLET] = None
    assert subject_pending_predicate(raw, subject)["status"] != "PASS"


@pytest.mark.parametrize("pending", [False, True])
def test_residual_admission_replays_raw_subject_pending_work(witnesses, tmp_path, pending):
    raw, subject = witnesses
    if pending:
        raw["txpool"]["queued"][WALLET] = {"0x3": {}}
    for name, value in [("stimulus-terminal.json", raw), ("positions-terminal.json", subject)]:
        (tmp_path / name).write_bytes(canonical(value))
    result = assess_residual_policy(
        {"residual_policy": POLICY}, generations=None, quantities=None, actor=None, bundle=tmp_path
    )
    assert result["status"] == ("FAIL" if pending else "UNMEASURED")
    assert result["subject_txpool_observation"]["pending_transactions"] == int(pending)
    assert result["subject_pending_transactions"] == ("FAIL" if pending else "UNMEASURED")
    assert result["all_token_wallet_inventory"] == "UNMEASURED"
    assert result["source_artifacts"] == ["stimulus-terminal.json", "positions-terminal.json"]
