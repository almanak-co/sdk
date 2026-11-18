"""Long and short collateral paths use the reusable strategy and generated or EOA authority."""

from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import pytest

from tests.fork.support import processes
from tests.fork.test_gmx_requests import configuration, settle_eoa
from tests.strategy_fork.harness import (
    Case,
    bind_workspace,
    install_permissions,
    lifecycle,
    load_assertions,
    resolve_config,
    retain,
    start_gateway,
)

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


def case(chain):
    name = f"gmx_v2_{chain}_collateral_path"
    root = ROOT / "strategies" / name
    return Case(
        name,
        root / "strategy.py",
        root / "config.yaml",
        ROOT / "tests/strategy_fork/scenarios" / name / "assertions.py",
    )


def path_configuration(selected, profile, side, env, directory):
    config = configuration(selected, profile, env, directory)
    if side == "short":
        allocations = tuple(
            allocation.model_copy(
                update={
                    "side": "short",
                    # A short mirrors the long reference: any entry price, and an exit
                    # price no higher than the long reference's entry price.
                    "increase_acceptable_price": 0,
                    "decrease_acceptable_price": allocation.increase_acceptable_price,
                }
            )
            for allocation in config.allocations
        )
        config = config.model_copy(update={"allocations": allocations})
    return type(config).model_validate_json(config.model_dump_json())


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("profile", ["safe_roles", "eoa"])
def test_gmx_collateral_path(request, tmp_path, chain, side, profile):
    selected = case(chain)
    env = request.config.getoption("--fork-env").resolve()
    config = path_configuration(selected, profile, side, env, tmp_path / "resolution")
    config = resolve_config(config, env, {})
    artifact = (
        ROOT
        / "artifacts/strategy-fork"
        / (selected.name + "-" + side + "-" + profile + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": selected.name,
        "side": side,
        "profile": profile,
        "status": "failed",
        "phases": [],
    }
    try:
        run_path(selected, config, env, temporary, record)
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("GMX collateral path evidence:", artifact)


def run_path(selected, config, env, temporary, record):
    assertions = load_assertions(selected)
    with ExitStack() as stack:
        nodes, node = bind_workspace(config, env, stack, record)
        launch = stack.enter_context(processes(temporary))
        directory = temporary / "deployment"
        deployment = nodes.prepare(config, directory)
        journal, arguments = start_gateway(node, directory, launch, lease_seconds=30)
        if record["profile"] == "safe_roles":
            install_permissions(selected, node, directory, deployment, arguments)
            record["phases"].append("generated_permissions_installed_and_verified")
            lifecycle(selected, node, assertions, journal, arguments, launch, record["phases"])
        else:
            settle_eoa(selected, node, assertions, journal, arguments, launch, record["phases"])
