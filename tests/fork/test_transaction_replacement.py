"""Real reference strategies on an identified fork with controlled pending candidates."""

import json
import sqlite3
import time
from pathlib import Path
from uuid import uuid4

import pytest

from almanak.gateway.fork_fixture import Fork
from almanak.gateway.production_config import ReplacementPolicy
from almanak.runtime import Runtime
from almanak.state import Journal
from almanak.strategy import StrategyDefinition
from almanak.transaction_family import family_report
from tests.fork.production_setup import rehearsal_config
from tests.fork.support import processes
from tests.fork.test_production import (
    STRATEGY,
    assert_closed,
    gateway_arguments,
    prepare,
)
from tests.strategy_fork.scenarios.aave_v4_weth_supply.assertions import entered

pytestmark = pytest.mark.fork
ROOT = Path(__file__).parents[2]
BOUNDS = dict(interval_seconds=1, minimum_bump_bps=1000, max_candidates=3, max_elapsed_seconds=120)


@pytest.fixture(scope="module")
def node(request):
    author = rehearsal_config("eoa_supply")
    env = request.config.getoption("--fork-env")
    original = author.resolved(author.single_target().resolve(env))
    with Fork(env, original.origin, target=original.single_target()) as fork:
        yield fork


def setup(node, directory, profile, bounds=None):
    node.rpc("anvil_setChainId", [31337])
    node.rpc("evm_setAutomine", [True])
    original = rehearsal_config(profile).resolved(node.origin)
    config = prepare(node, original, directory)
    gas = tuple(
        p.model_copy(update={"replacement": ReplacementPolicy(**(bounds or BOUNDS))})
        for p in config.production.gas
    )
    config = config.model_copy(
        update={"production": config.production.model_copy(update={"gas": gas})}
    )
    (directory / "production.json").write_text(config.model_dump_json(indent=2))
    return config


def drive(runtime, predicate, timeout=100):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = runtime.journal.load()
        if predicate(state):
            return state
        runtime.tick()
        time.sleep(0.05)
    raise AssertionError(
        "reference did not reach expected family state: " + runtime.journal.load().model_dump_json()
    )


def prepare_pending(runtime, node, operation="supply"):
    def control(stage, pending):
        if stage == "prepared" and pending.operation.kind == operation:
            node.rpc("evm_setAutomine", [False])

    runtime.fault = control
    state = drive(
        runtime, lambda s: s.pending is not None and s.pending.operation.kind == operation
    )
    state = drive(runtime, lambda s: s.pending.family.candidates[0].submission is not None)
    runtime.tick()
    pending = state.pending
    assert node.rpc("eth_getTransactionReceipt", [pending.tx_hash]) is None
    return pending


def escalate(runtime, node, pending):
    node.rpc("anvil_setNextBlockBaseFeePerGas", [hex(pending.gas_price * 2)])
    node.rpc("evm_mine")
    state = drive(
        runtime, lambda s: s.pending is not None and len(s.pending.family.candidates) == 2
    )
    assert state.pending.nonce == pending.nonce
    assert state.pending.gas == pending.gas
    return state.pending


def mine_winner(runtime, node, pending, winner):
    latest = pending.family.candidates[-1]
    drive(runtime, lambda s: s.pending.family.candidates[-1].submission is not None)
    runtime.tick()
    if winner == 0:
        node.rpc("anvil_dropTransaction", [latest.tx_hash])
    chosen = pending.family.candidates[winner]
    node.rpc("anvil_setNextBlockBaseFeePerGas", ["0x1"])
    if node.rpc("eth_getTransactionByHash", [chosen.tx_hash]) is None:
        # Restore a retained candidate to model a peer retaining the original after replacement.
        node.rpc("eth_sendRawTransaction", [chosen.raw_transaction])
    node.rpc("evm_mine")
    receipt = node.rpc("eth_getTransactionReceipt", [chosen.tx_hash])
    assert receipt is not None and receipt["status"] == "0x1"
    return chosen, receipt


def close(runtime, node):
    runtime.fault = lambda *args: None
    node.rpc("evm_setAutomine", [True])
    drive(runtime, entered)
    runtime.journal.teardown()
    state = drive(runtime, lambda s: s.mode == "closed")
    assert_closed(state)
    return state


def retain(node, journal, label, extra=None):
    directory = ROOT / "artifacts/transaction-replacement" / (label + "-" + uuid4().hex)
    directory.mkdir(parents=True)
    with journal.connect() as source, sqlite3.connect(directory / "state.sqlite") as target:
        source.backup(target)
    state = journal.load()
    payload = {
        "label": label,
        "status": "passed",
        "client": node.rpc("web3_clientVersion"),
        "fork": node.rpc("anvil_metadata"),
        "gas_policies": [p.model_dump(mode="json") for p in state.config.production.gas],
        "families": family_report(state),
        "evidence": extra or {},
    }
    (directory / "verification.json").write_text(json.dumps(payload, indent=2) + "\n")
    print("Replacement fork evidence:", directory)


def assert_winner(state, first, receipt):
    outcome = only_family_outcome(state, first)
    assert outcome.nonce == first.nonce
    assert outcome.gas_paid.amount == int(receipt["gasUsed"], 16) * int(
        receipt["effectiveGasPrice"], 16
    )


def only_family_outcome(state, first):
    matched = [o for o in state.outcomes if o.family.identity == first.family.identity]
    assert len(matched) == 1
    return matched[0]


def complete_aave(runtime, node, first, chosen, receipt):
    state = drive(runtime, lambda s: any(o.tx_hash == chosen.tx_hash for o in s.outcomes))
    assert_winner(state, first, receipt)
    closed = close(runtime, node)
    assert all(o.family is not None for o in closed.outcomes)


@pytest.mark.parametrize("profile", ["eoa_supply", "safe_borrow"])
@pytest.mark.parametrize("winner", [0, 1])
def test_aave_either_candidate_wins(node, tmp_path, profile, winner):
    directory = tmp_path / "deployment"
    config = setup(node, directory, profile)
    gateway, control = gateway_arguments(node, directory)
    with processes(tmp_path) as launch:
        launch(gateway, ready="gateway_ready")
        with_runtime = Runtime(
            control[3],
            directory / "gateway-auth.token",
            create=config,
            strategy=StrategyDefinition.read(STRATEGY),
        )
        try:
            first = prepare_pending(with_runtime, node)
            pending = escalate(with_runtime, node, first)
            chosen, receipt = mine_winner(with_runtime, node, pending, winner)
            complete_aave(with_runtime, node, first, chosen, receipt)
            retain(node, Journal(directory / "state.sqlite"), f"{profile}-winner-{winner}")
        finally:
            with_runtime.close()
