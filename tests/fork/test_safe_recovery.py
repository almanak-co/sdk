"""Actual strategy process death through generated Safe authority and debt closure."""

import signal
import sys
import time
from pathlib import Path

import pytest

from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.gateway.fork_fixture import Fork
from tests.author_evidence import retained_author
from tests.fork.support import processes, start_gateway, wait_state
from tests.strategy_fork.harness import Case, install_permissions, retain
from tests.strategy_fork.scenarios.aave_v4_weth_supply.assertions import assert_closed

ROOT = Path(__file__).parents[2]
STRATEGY = ROOT / "strategies/aave_v4_weth_supply/strategy.py"
DRIVER = ROOT / "tests/fork/driver.py"
STAGES = (
    "reserved",
    "signed",
    "prepared",
    "broadcast",
    "receipt",
    "commit",
    "outcome",
    "transition",
)
pytestmark = pytest.mark.fork


def one_safe_config():
    config = ForkConfig.model_validate_json(retained_author(STRATEGY, "config.json").read_text())
    account = config.funds[0].account
    return ForkConfig.model_validate(
        {
            **config.model_dump(),
            "funds": config.funds[:1],
            "allocations": tuple(a for a in config.allocations if a.account == account),
            "debts": tuple(d for d in config.debts if d.account == account),
        }
    )


def await_expired_lease(journal):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        with journal.connect() as connection:
            (expires,) = connection.execute("SELECT expires FROM coordinator").fetchone()
        if expires <= time.time():
            return
        time.sleep(0.05)
    raise AssertionError("crashed process retained its coordinator lease")


@pytest.mark.parametrize("stage", STAGES)
@pytest.mark.parametrize("operation", ("approve", "collateral", "borrow", "repay"))
def test_safe_process_death_preserves_entry_and_full_debt_closure(
    request, tmp_path, stage, operation
):
    config = one_safe_config()
    env = request.config.getoption("--fork-env")
    with (
        Fork(env, config.origin, target=config.single_target()) as node,
        processes(tmp_path) as launch,
    ):
        directory = tmp_path / "deployment"
        deployment = node.prepare(config, directory)
        journal, arguments = start_gateway(node, directory, launch)
        case = Case(
            "safe-recovery", STRATEGY, retained_author(STRATEGY, "config.json"), Path(__file__)
        )
        install_permissions(case, node, directory, deployment, arguments)
        driver = [sys.executable, str(DRIVER), "--strategy", str(STRATEGY), *arguments]
        crashed_args = [*driver, "--crash", stage, "--operation", operation]
        if operation == "repay":
            crashed_args.append("--teardown-after-entry")
        crashed = launch(crashed_args)
        assert crashed.wait(timeout=80) == -signal.SIGKILL
        interrupted = journal.load()
        await_expired_lease(journal)
        replace_gateway(node, launch)
        resumed = launch([*driver, "--resume", "--teardown-after-entry"])
        closed = wait_state(journal, lambda s: s.mode == "closed", resumed)
        assert resumed.wait(timeout=20) == 0
        assert_closed(node, closed)
        assert_exact_entry_counts(closed)
        artifact = ROOT / "artifacts/safe-recovery" / f"{operation}-{stage}-{closed.deployment_id}"
        artifact.mkdir(parents=True)
        (artifact / "interrupted.json").write_text(interrupted.model_dump_json(indent=2) + "\n")
        retain(tmp_path, artifact, {"status": "passed", "operation": operation, "stage": stage})


def replace_gateway(node, launch):
    node.gateway_process.kill()
    node.gateway_process.wait(timeout=10)
    node.gateway_process = launch(node.gateway_arguments, ready="gateway_ready")


def assert_exact_entry_counts(state):
    kinds = [o.operation.kind for o in state.outcomes]
    assert (kinds.count("supply"), kinds.count("borrow"), kinds.count("repay")) == (2, 1, 1)
    nonces = [o.nonce for o in state.outcomes]
    assert nonces == list(range(nonces[0], nonces[0] + len(nonces)))
