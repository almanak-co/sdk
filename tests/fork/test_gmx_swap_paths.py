"""Swap references retain asynchronous requests across process recovery in both account profiles."""

from pathlib import Path
from uuid import uuid4

import pytest

from almanak.state import Journal
from tests.fork.test_gmx_collateral_paths import run_path
from tests.fork.test_gmx_requests import case, configuration
from tests.strategy_fork.harness import resolve_config, retain

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("profile", ["safe_roles", "eoa"])
def test_gmx_swap_path(request, tmp_path, chain, profile):
    run_swap_case(request, tmp_path, chain, profile)


def test_gmx_swap_completion_after_admission_deadline(request, tmp_path):
    run_swap_case(request, tmp_path, "avalanche", "safe_roles", deadline_seconds=60)


def run_swap_case(request, tmp_path, chain, profile, *, deadline_seconds=None):
    selected = case(f"gmx_v2_{chain}_swap")
    env = request.config.getoption("--fork-env").resolve()
    config = resolve_config(configuration(selected, profile, env, tmp_path / "resolution"), env, {})
    if deadline_seconds is not None:
        config = config.model_copy(
            update={
                "allocations": tuple(
                    allocation.model_copy(update={"deadline_seconds": deadline_seconds})
                    for allocation in config.allocations
                )
            }
        )
        config = type(config).model_validate_json(config.model_dump_json())
    artifact = (
        ROOT / "artifacts/strategy-fork" / (selected.name + "-" + profile + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {"strategy": selected.name, "profile": profile, "status": "failed", "phases": []}
    try:
        run_path(selected, config, env, temporary, record)
        if deadline_seconds is not None:
            assert_late_completion(Journal(temporary / "deployment/state.sqlite").load())
            record["phases"].append("accepted_before_admission_deadline_executed_after_deadline")
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("GMX swap path evidence:", artifact)


def assert_late_completion(state):
    assert state.mode == "closed"
    assert all(
        request.events[0].identity.block.timestamp < request.admission_deadline
        for request in state.requests
    )
    assert any(
        request.events[-1].identity.block.timestamp > request.admission_deadline
        for request in state.requests
    )
    assert all(
        request.status == "executed" and len(request.events) == 2 for request in state.requests
    )
