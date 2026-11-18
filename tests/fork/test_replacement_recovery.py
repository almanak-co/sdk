"""Real runtime and gateway SIGKILL recovery with pending Aave fee families."""

import signal
import sys
import time
from pathlib import Path

import pytest

from almanak.state import Journal
from tests.fork.support import processes, request_teardown, wait_state
from tests.fork.test_production import STRATEGY, assert_closed, gateway_arguments, wait_expired
from tests.fork.test_transaction_replacement import node as node
from tests.fork.test_transaction_replacement import retain, setup
from tests.strategy_fork.scenarios.aave_v4_weth_supply.assertions import entered

pytestmark = pytest.mark.fork
RUNTIME_STAGES = (
    "replacement_reserve",
    "replacement_sign",
    "replacement_attempt",
    "broadcast",
    "receipt",
    "commit",
    "outcome",
    "transition",
)
GATEWAY_STAGES = (
    "reserve",
    "sign-before-record",
    "sign",
    "attempt",
    "broadcast-before-record",
    "broadcast",
    "reconcile",
    "commit-before-record",
    "update",
)
MINED_STAGES = {
    "receipt",
    "reconcile",
    "commit",
    "outcome",
    "transition",
    "commit-before-record",
    "update",
}


def wait_for(predicate, process):
    deadline = time.monotonic() + 100
    while time.monotonic() < deadline:
        if predicate():
            return
        assert process.poll() is None
        time.sleep(0.05)
    raise AssertionError("controlled process did not reach its interruption point")


def pending_second(state):
    return state.pending is not None and len(state.pending.family.candidates) == 2


def mine_second(node, journal, runtime):
    state = wait_state(
        journal,
        lambda s: pending_second(s) and s.pending.family.candidates[1].submission is not None,
        runtime,
    )
    candidate = state.pending.family.candidates[1]
    wait_for(lambda: node.rpc("eth_getTransactionByHash", [candidate.tx_hash]) is not None, runtime)
    node.rpc("anvil_setNextBlockBaseFeePerGas", ["0x1"])
    node.rpc("evm_mine")
    assert node.rpc("eth_getTransactionReceipt", [candidate.tx_hash])["status"] == "0x1"


def release_original(node, journal, runtime, pause):
    wait_for(pause.exists, runtime)
    node.rpc("evm_setAutomine", [False])
    first = journal.load().pending
    pause.unlink()
    wait_for(lambda: node.rpc("eth_getTransactionByHash", [first.tx_hash]) is not None, runtime)
    node.rpc("anvil_setNextBlockBaseFeePerGas", [hex(first.gas_price * 2)])
    node.rpc("evm_mine")
    return first


def restart(launch, gateway, journal, runtime, service, owner):
    if owner == "gateway":
        runtime.wait(timeout=70)
    else:
        service.kill()
        service.wait(timeout=10)
    launch(gateway, ready="gateway_ready")
    wait_expired(journal)


def assert_recovered(holding, first, interrupted):
    winner = next(o for o in holding.outcomes if o.family.identity == first.family.identity)
    assert len(winner.family.candidates) == 2
    assert winner.nonce == first.nonce
    assert_retained(interrupted, winner)


def finish(node, journal, control, resumed, first, interrupted, label):
    node.rpc("evm_setAutomine", [True])
    holding = wait_state(journal, entered, resumed)
    assert_recovered(holding, first, interrupted)
    request_teardown(control)
    closed = wait_state(journal, lambda s: s.mode == "closed", resumed)
    assert resumed.wait(timeout=30) == 0
    assert_closed(closed)
    retain(node, journal, label, {"interrupted": interrupted.model_dump(mode="json")})


def await_crash(node, journal, runtime, service, owner, stage):
    if stage in MINED_STAGES:
        mine_second(node, journal, runtime)
    victim = runtime if owner == "runtime" else service
    assert victim.wait(timeout=100) == -signal.SIGKILL
    return journal.load()


@pytest.mark.parametrize(
    "owner,stage",
    [("runtime", s) for s in RUNTIME_STAGES] + [("gateway", s) for s in GATEWAY_STAGES],
)
@pytest.mark.parametrize("profile", ["eoa_supply", "safe_borrow"])
def test_process_death_preserves_one_family(node, tmp_path, owner, stage, profile):
    directory = tmp_path / "deployment"
    setup(node, directory, profile)
    gateway, control = gateway_arguments(node, directory)
    gateway.extend(("--lease-seconds", "5"))
    driver = [
        sys.executable,
        str(Path(__file__).with_name("replacement_driver.py")),
        "--strategy",
        str(STRATEGY),
        *control,
    ]
    pause = tmp_path / "pause-supply"
    with processes(tmp_path) as launch:
        fault_args = ["--fault-stage", stage] if owner == "gateway" else []
        service = launch([*gateway, *fault_args], ready="gateway_ready")
        crash_args = ["--crash", stage] if owner == "runtime" else []
        runtime = launch([*driver, "--pause-file", str(pause), *crash_args])
        journal = Journal(directory / "state.sqlite")
        first = release_original(node, journal, runtime, pause)
        interrupted = await_crash(node, journal, runtime, service, owner, stage)
        restart(launch, gateway, journal, runtime, service, owner)
        resumed = launch([*driver, "--resume"])
        if stage not in MINED_STAGES:
            mine_second(node, journal, resumed)
        finish(node, journal, control, resumed, first, interrupted, f"{profile}-{owner}-{stage}")


def assert_retained(interrupted, winner):
    if interrupted.pending is None:
        return
    for candidate in interrupted.pending.family.candidates:
        retained = winner.family.candidates[candidate.index]
        assert_candidate_retained(candidate, retained)


def assert_candidate_retained(candidate, retained):
    assert (retained.tx_hash, retained.raw_transaction) == (
        candidate.tx_hash,
        candidate.raw_transaction,
    )
    assert retained.configuration_revision == candidate.configuration_revision
    if candidate.submission is not None:
        assert retained.submission == candidate.submission
