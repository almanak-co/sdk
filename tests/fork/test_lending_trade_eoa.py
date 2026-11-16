"""Shared lending and trading retain EOA fund ownership through process recovery."""

from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import pytest

from tests.fork.support import processes
from tests.fork.test_aave_v3_base_eoa import eoa_configuration, eoa_lifecycle
from tests.fork.test_morpho_blue_eoa import retain_transactions
from tests.strategy_fork.harness import (
    bind_workspace,
    case_fork_config,
    discover,
    load_assertions,
    resolve_config,
    retain,
    start_gateway,
)

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.mark.parametrize(
    "reference",
    [
        "base_morpho_borrow_and_trade",
        "base_compound_borrow_and_trade",
        "ethereum_morpho_curve_borrow_and_trade",
    ],
)
def test_lending_trade_eoa_fork_lifecycle(request, tmp_path, reference):
    case = next(
        c
        for c in discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if c.name == reference
    )
    env = request.config.getoption("--fork-env").resolve()
    config = eoa_configuration(case_fork_config(case, env, tmp_path / "resolution"))
    config = resolve_config(config, env, {})
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
        with ExitStack() as stack:
            nodes, node = bind_workspace(config, env, stack, record)
            launch = stack.enter_context(processes(temporary))
            directory = temporary / "deployment"
            deployment = nodes.prepare(config, directory)
            assert all(b.profile.kind == "eoa" for b in deployment.accounts.bindings)
            journal, arguments = start_gateway(node, directory, launch, lease_seconds=30)
            try:
                eoa_lifecycle(
                    case, node, load_assertions(case), journal, arguments, launch, record["phases"]
                )
            finally:
                retain_transactions(node, journal, artifact)
            record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
