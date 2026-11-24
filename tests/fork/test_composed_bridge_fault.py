"""A runtime killed around the arrival commit of the composed bridge reference.

The actual runtime process is killed at the runtime's own fault points: after the
gateway returned the arrival page and before it is committed (`external_evidence`), and
after the commit and before anything else (`external_commit`). A new runtime then
records the arrival once, credits the destination inventory once and supplies exactly
that amount.

The quote the deposit signed is the fork gateway's constructed one and the fill is the
fixture's own `fillRelay`. Nothing here is promotion or Fork Qualification evidence.
"""

import signal
import sys
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import pytest

from almanak.connectors.across.fork_settler import Settler
from tests.fork.support import CLI, processes, request_teardown, resume, wait_state
from tests.fork.test_gmx_liquidation_runtime import wait_expired_lease
from tests.strategy_fork.harness import (
    bind_workspace,
    case_fork_config,
    discover,
    load_assertions,
    resolve_config,
    restart_gateway,
    retain,
    start_gateway,
)

ROOT = Path(__file__).parents[2]
NAME = "arbitrum_base_bridge_and_lend"
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


class Nodes:
    """The in-process fork nodes as the settler addresses them: by the chain they fork."""

    def __init__(self, nodes):
        self.nodes = {origin.chain_id: node for origin, node in nodes.nodes.items()}

    def rpc(self, chain_id):
        return self.nodes[chain_id].rpc

    def origin(self, chain_id):
        return self.nodes[chain_id].origin


def arrivals(state) -> list:
    return [o for o in state.outcomes if getattr(o, "kind", None) == "across_arrival"]


def assert_interrupted(checks, state, held, boundary):
    """What the killed runtime left: nothing of the arrival, or all of it, never a part."""
    assert state.memory == held.memory and state.delivered == held.delivered
    if boundary == "external_evidence":
        # Empty pages may have moved the cursor before the arrival's page was read, so
        # the scan history extends the held one; the outcomes are exactly the held ones.
        assert state.outcomes == held.outcomes
        assert state.external_scans[: len(held.external_scans)] == held.external_scans
        assert all(page.outcome is None for page in state.external_scans)
        assert state.transfers == held.transfers and checks.inventory(state) == (0, 0)
        return
    assert len(arrivals(state)) == 1 and state.outcomes[:-1] == held.outcomes
    assert state.transfers[0].resolution.received.amount == checks.OFFERED
    assert checks.inventory(state) == (0, checks.OFFERED)


@pytest.mark.parametrize("boundary", ["external_evidence", "external_commit"])
def test_a_runtime_killed_around_the_arrival_commit_records_the_arrival_once(
    request, tmp_path, boundary
):
    case = next(
        c
        for c in discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if c.name == NAME
    )
    checks = load_assertions(case)
    env = request.config.getoption("--fork-env").resolve()
    config = resolve_config(case_fork_config(case, env, tmp_path / "resolution"), env, {})
    artifact = ROOT / "artifacts/strategy-fork" / f"{NAME}-arrival-{boundary}-{uuid4().hex}"
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": NAME,
        "boundary": boundary,
        "status": "failed",
        "scope": "actual reference runtime killed around the arrival commit",
        "phases": [],
    }
    try:
        with ExitStack() as stack:
            nodes, _ = bind_workspace(config, env, stack, record)
            launch = stack.enter_context(processes(temporary))
            directory = temporary / "deployment"
            nodes.prepare(config, directory)
            journal, arguments = start_gateway(nodes, directory, launch, lease_seconds=30)
            start = [CLI, "service", "start", "--strategy", str(case.strategy), *arguments]
            running = launch(start)
            held = wait_state(
                journal, lambda s: checks.in_flight(s) and checks.delivered(s), running
            )
            checks.assert_in_flight(nodes, held)
            running.kill()
            assert running.wait(timeout=20) == -signal.SIGKILL
            (transfer,) = held.transfers
            Settler(Nodes(nodes), directory / "keepers").fill(held, transfer)
            record["phases"].append("deposit_mined_runtime_killed_and_transfer_filled")
            restart_gateway(nodes, launch)
            wait_expired_lease(journal)
            driver = str(ROOT / "tests/fork/settlement_driver.py")
            crashing = launch(
                [sys.executable, driver, *arguments, "--crash", boundary], ready=False
            )
            assert crashing.wait(timeout=240) == -signal.SIGKILL
            interrupted = journal.load()
            assert_interrupted(checks, interrupted, held, boundary)
            (artifact / "interrupted-checkpoint.json").write_text(
                interrupted.model_dump_json(indent=2)
            )
            record["phases"].append(f"runtime_killed_at_{boundary}")
            restart_gateway(nodes, launch)
            wait_expired_lease(journal)
            running = resume(launch, journal, arguments)
            holding = wait_state(journal, checks.entered, running, timeout=300)
            assert len(arrivals(holding)) == 1, "the arrival was recorded more than once"
            checks.assert_holding(nodes, holding)
            record["phases"].append("arrival_recorded_once_and_exactly_that_amount_supplied")
            request_teardown(arguments)
            closed = wait_state(journal, lambda s: s.mode == "closed", running, timeout=300)
            assert running.wait(timeout=30) == 0
            checks.assert_closed(nodes, closed)
            assert len(arrivals(closed)) == 1
            record["phases"].append("closed")
            record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("Composed bridge arrival fault evidence:", artifact)
