"""Uniswap V4 EOA references use identified forks and shared service recovery."""

from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import pytest
import yaml

from almanak.gateway.fork_workspace import ForkSet
from almanak.gateway.operator_config import read_operator_config
from tests.fork.support import processes
from tests.fork.test_aave_v3_base_eoa import eoa_lifecycle
from tests.strategy_fork.adapter import (
    fixture_plan_for,
    prepare_admitted_fork,
    resolve_reference,
)
from tests.strategy_fork.harness import (
    discover,
    load_assertions,
    retain,
    start_gateway,
)
from tests.strategy_fork.supervised import eoa_inputs, revalidated

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.mark.parametrize("style", ["trading", "lp"])
@pytest.mark.parametrize("chain", ["ethereum", "avalanche"])
def test_uniswap_v4_eoa_fork_lifecycle(request, tmp_path, style, chain):
    case = eoa_case(style, chain)
    env = request.config.getoption("--fork-env").resolve()
    operator_path = tmp_path / "operator.yaml"
    source_operator = read_operator_config(
        ROOT / "examples" / "operators" / f"{case.name}.fork.yaml"
    )
    operator = revalidated(source_operator, fork=eoa_inputs(source_operator.fork))
    operator_path.write_text(yaml.safe_dump(operator.model_dump(mode="json"), sort_keys=False))
    progress = resolve_reference(case.config, operator_path, env, tmp_path / "resolution")
    plan = fixture_plan_for(case.config, operator_path, progress)
    artifact = ROOT / "artifacts/strategy-fork" / (case.name + "-eoa-" + uuid4().hex)
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": case.name,
        "account_profile": "eoa",
        "authority": "identified Anvil fixture only; no production authority",
        "onchain_roles_quotas": "not_applicable_to_explicit_eoa_profile",
        "status": "failed",
        "phases": [],
    }
    try:
        execute_eoa_case(case, operator_path, progress, plan, env, temporary, record)
    finally:
        retain(temporary, artifact, record)


def eoa_case(style, chain):
    name = f"uniswap_v4_{'avalanche_' if chain == 'avalanche' else ''}{style}"
    return next(
        case
        for case in discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if case.name == name
    )


def execute_eoa_case(case, operator_path, progress, plan, env, temporary, record):
    with ExitStack() as stack:
        nodes = stack.enter_context(ForkSet(plan, env))
        node = next(iter(nodes.nodes.values()))
        launch = stack.enter_context(processes(temporary))
        directory = temporary / "deployment"
        deployment = prepare_admitted_fork(
            case.config, case.strategy, operator_path, progress, plan, nodes, directory
        )
        assert all(binding.profile.kind == "eoa" for binding in deployment.accounts.bindings)
        assert_eoa_gas(deployment)
        journal, arguments = start_gateway(node, directory, launch, lease_seconds=30)
        eoa_lifecycle(
            case, node, load_assertions(case), journal, arguments, launch, record["phases"]
        )
        record["status"] = "passed"


def assert_eoa_gas(deployment):
    if deployment.gas:
        assert deployment.fork_gas is not None
        assert deployment.fork_gas.gas == deployment.gas
        assert deployment.production is None


def qualified_fund_indices(config, *, lp):
    start = {
        "eip155:1": 1094400,
        "eip155:43114": 431149400,
        "eip155:8453": 84539400,
        "eip155:42161": 42161400,
        "eip155:10": 101400,
        "eip155:56": 569400,
        "eip155:137": 1379400,
        "eip155:4663": 46639400,
    }[str(config.forks[0].chain.chain_id)] + int(lp)
    return config.model_copy(
        update={
            "funds": tuple(
                f.model_copy(update={"foundry_account_index": start + i})
                for i, f in enumerate(config.funds)
            )
        }
    )


def lp_fund_indices(config):
    return qualified_fund_indices(config, lp=True)
