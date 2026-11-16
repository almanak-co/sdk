"""Invalidated external financial history halts recovery without erasing retained facts."""

import signal
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tests import gmx_fork
from tests.fork.support import CLI, processes, wait_state
from tests.fork.test_gmx_liquidation_capture import capture_liquidation, source_after
from tests.fork.test_gmx_liquidation_pending import commit_liquidation
from tests.fork.test_gmx_liquidation_runtime import wait_expired_lease
from tests.fork.test_gmx_requests import case, configuration
from tests.strategy_fork.harness import (
    bind_workspace,
    install_permissions,
    resolve_config,
    restart_gateway,
    retain,
    start_gateway,
    start_runtime,
)

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("profile", ["eoa", "safe_roles"])
def test_liquidation_reorganization_halts_recovery(request, tmp_path, chain, profile):
    selected = case(f"gmx_v2_{chain}_position")
    env = request.config.getoption("--fork-env").resolve()
    config = resolve_config(configuration(selected, profile, env, tmp_path / "resolution"), env, {})
    artifact = (
        ROOT
        / "artifacts/strategy-fork"
        / (selected.name + "-liquidation-reorg-" + profile + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": selected.name,
        "profile": profile,
        "status": "failed",
        "phases": [],
        "scope": "actual reference liquidation canonicality invalidation",
    }
    try:
        with ExitStack() as stack:
            nodes, node = bind_workspace(config, env, stack, record)
            launch = stack.enter_context(processes(temporary))
            directory = temporary / "deployment"
            deployment = nodes.prepare(config, directory)
            journal, arguments = start_gateway(node, directory, launch, lease_seconds=30)
            if profile == "safe_roles":
                install_permissions(selected, node, directory, deployment, arguments)
            node.rpc("anvil_setBlockTimestampInterval", [1])
            running, _ = start_runtime(
                selected, node, SimpleNamespace(), journal, arguments, launch, record["phases"]
            )
            gmx_fork.drive_external(
                node,
                journal,
                running,
                lambda r: source_after(chain, r.events[0].identity.block.timestamp),
            )
            held = wait_state(journal, gmx_fork.entered, running)
            running.kill()
            assert running.wait(timeout=20) == -signal.SIGKILL
            snapshot = node.rpc("evm_snapshot", [])
            capture_liquidation(node, held, directory, chain)
            committed = commit_liquidation(node, journal, arguments, launch)
            (artifact / "external-checkpoint.json").write_text(committed.model_dump_json(indent=2))
            replace_liquidation_history(node, snapshot, committed)
            restart_gateway(node, launch)
            wait_expired_lease(journal)
            running = launch([CLI, "service", "resume", *arguments], ready=False)
            assert running.wait(timeout=180) == 1
            invalidated = journal.load()
            assert_invalidated(committed, invalidated)
            record["phases"].append(
                "same_height_replacement_gateway_restart_halts_and_retains_external_history"
            )
            record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        captured = temporary / "deployment/external-liquidation.json"
        if captured.exists():
            (artifact / "external-liquidation.json").write_bytes(captured.read_bytes())
        print("GMX liquidation reorganization evidence:", artifact)


def replace_liquidation_history(node, snapshot, committed):
    expected = committed.outcomes[-1].after.block
    assert node.rpc("evm_revert", [snapshot]) is True
    current = int(node.rpc("eth_blockNumber", []), 16)
    assert current < expected.number
    node.rpc("anvil_mine", [hex(expected.number - current)])
    replacement = node.rpc("eth_getBlockByNumber", [hex(expected.number), False])
    assert replacement["hash"] != expected.hash
    assert node.rpc("eth_getTransactionReceipt", [committed.outcomes[-1].tx_hash]) is None


def assert_invalidated(committed, invalidated):
    assert invalidated.integrity_error is not None
    assert "canonical" in invalidated.integrity_error
    assert invalidated.model_dump(exclude={"integrity_error"}) == committed.model_dump(
        exclude={"integrity_error"}
    )
