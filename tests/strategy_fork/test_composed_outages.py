"""Actual mixed protocol positions remain independently recoverable across chains."""

from pathlib import Path
from uuid import uuid4

import pytest

from almanak.gateway.fork_workspace import ForkSet
from tests.fork.support import processes
from tests.strategy_fork.adapter import fixture_plan_for, prepare_admitted_fork, resolve_reference
from tests.strategy_fork.harness import discover, retain
from tests.strategy_fork.test_multichain_outages import independent_recovery

ROOT = Path(__file__).parents[2]
CASE = next(
    c
    for c in discover(ROOT / "strategies", Path(__file__).parent / "scenarios")
    if c.name == "ethereum_base_borrow_and_trade"
)
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.mark.parametrize("failed_index", [0, 1], ids=["ethereum_outage", "base_outage"])
def test_composed_funds_recover_independently(failed_index, request, tmp_path):
    env = request.config.getoption("--fork-env").resolve()
    operator = ROOT / "examples" / "operators" / f"{CASE.name}.fork.yaml"
    progress = resolve_reference(CASE.config, operator, env, tmp_path / "resolution")
    plan = fixture_plan_for(CASE.config, operator, progress)
    artifact = (
        ROOT
        / "artifacts/strategy-fork"
        / (CASE.name + "-outage-" + str(failed_index) + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    record = {
        "strategy": CASE.name,
        "failed_chain_index": failed_index,
        "status": "failed",
        "phases": [],
    }
    try:
        with (
            ForkSet(plan, env) as nodes,
            processes(tmp_path) as launch,
        ):
            directory = tmp_path / "deployment"
            deployment = prepare_admitted_fork(
                CASE.config, CASE.strategy, operator, progress, plan, nodes, directory
            )
            assert deployment.fork_gas is not None
            assert deployment.fork_gas.gas == deployment.gas
            assert deployment.production is None
            independent_recovery(CASE, nodes, directory, failed_index, deployment, launch, record)
        record["status"] = "passed"
    finally:
        retain(tmp_path, artifact, record)
