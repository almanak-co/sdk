"""Curve EOA references use identified forks and shared service recovery."""

import json
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import pytest

from tests.fork.support import processes
from tests.fork.test_aave_v3_base_eoa import eoa_configuration, eoa_lifecycle
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
        "curve_ethereum_stable_trading",
        "curve_ethereum_stable_lp",
        "curve_ethereum_twocrypto_lp",
        "curve_ethereum_tricrypto_lp",
        "curve_base_stable_lp",
        "curve_base_twocrypto_lp",
        "curve_base_tricrypto_lp",
        "curve_arbitrum_stable_lp",
        "curve_arbitrum_twocrypto_lp",
        "curve_arbitrum_tricrypto_lp",
        "curve_polygon_stable_lp",
        "curve_polygon_twocrypto_lp",
        "curve_polygon_tricrypto_lp",
        "curve_bnb_stable_lp",
        "curve_bnb_twocrypto_lp",
        "curve_avalanche_stable_lp",
        "curve_optimism_stable_lp",
        "curve_optimism_twocrypto_lp",
        "curve_optimism_tricrypto_lp",
        "curve_robinhood_stable_lp",
        "curve_robinhood_twocrypto_lp",
        "curve_robinhood_tricrypto_lp",
    ],
)
def test_curve_eoa_fork_lifecycle(request, tmp_path, reference):
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
        exercise_eoa(case, config, env, temporary, artifact, record)
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)


def exercise_eoa(case, config, env, temporary, artifact, record):
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


def retain_transactions(node, journal, artifact):
    state = journal.load()
    if state is None:
        return
    records = []
    for outcome in state.outcomes:
        tx = node.rpc("eth_getTransactionByHash", [outcome.tx_hash])
        record = {"transaction": tx, "receipt": outcome.receipt}
        if not outcome.success:
            record["trace"] = node.rpc(
                "debug_traceTransaction", [outcome.tx_hash, {"tracer": "callTracer"}]
            )
        records.append(record)
    (artifact / "transactions.json").write_text(json.dumps(records, indent=2) + "\n")
