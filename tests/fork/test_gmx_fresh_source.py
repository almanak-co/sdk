"""Unchanged fresh source signatures execute through the actual reference target Oracle."""

import signal
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from almanak.connectors.gmx_v2.fresh_reports import FreshReports, Limits
from almanak.connectors.gmx_v2.report_source import selections_for
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.providers import provider_endpoint
from tests import gmx_fork
from tests.fork.support import processes, request_teardown, wait_state
from tests.fork.test_gmx_requests import case, configuration
from tests.strategy_fork.harness import (
    bind_workspace,
    resolve_config,
    restart_gateway,
    resume,
    retain,
    start_gateway,
    start_runtime,
)

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
def test_fresh_authentic_source_executes_actual_reference(request, tmp_path, chain):
    selected = case(f"gmx_v2_{chain}_position")
    env = request.config.getoption("--fork-env").resolve()
    config = resolve_config(configuration(selected, "eoa", env, tmp_path / "resolution"), env, {})
    artifact = ROOT / "artifacts/strategy-fork" / (selected.name + "-fresh-source-" + uuid4().hex)
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {"strategy": selected.name, "profile": "eoa", "status": "failed", "phases": []}
    try:
        with ExitStack() as stack:
            fork = config.forks[0]
            reader = stack.enter_context(
                EthereumReader(fork.chain, provider_endpoint(fork.provider, env))
            )
            origin = reader.pin_block("latest")
            config = config.model_copy(
                update={"forks": (fork.model_copy(update={"block": origin}),)}
            )
            source = fresh_source(reader, config.allocations[0].market, origin, artifact)
            record["fresh_origin"] = origin.model_dump(mode="json")
            nodes, node = bind_workspace(config, env, stack, record)
            launch = stack.enter_context(processes(temporary))
            directory = temporary / "deployment"
            nodes.prepare(config, directory)
            journal, arguments = start_gateway(node, directory, launch, lease_seconds=30)
            node.rpc("anvil_setBlockTimestampInterval", [1])
            running, _ = start_runtime(
                selected, node, SimpleNamespace(), journal, arguments, launch, record["phases"]
            )
            drive(node, journal, running, source)
            held = wait_state(journal, gmx_fork.entered, running)
            running.send_signal(signal.SIGINT)
            assert running.wait(timeout=30) == 130
            restart_gateway(node, launch)
            running = resume(launch, journal, arguments)
            request_teardown(arguments)
            drive(node, journal, running, source)
            closed = wait_state(journal, lambda state: state.mode == "closed", running)
            assert running.wait(timeout=30) == 0
            assert_fresh_completion(source, origin, held, closed)
            record["phases"].append(
                "fresh_open_gateway_restart_close_with_actual_target_oracle_verification"
            )
            record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("GMX fresh-source evidence:", artifact)


def fresh_source(reader, market, origin, artifact):
    selection = selections_for(market)
    limits = Limits(
        page_blocks=1000,
        maximum_blocks=10000,
        maximum_transactions=100,
        maximum_logs=1000,
        wait_seconds=300,
        poll_seconds=2,
    )
    return FreshReports(
        reader, market, origin, selection, limits, artifact / "fresh-report-source.json"
    )


def drive(node, journal, running, source):
    def selected(request):
        now = int(node.rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)
        return source.acquire(request, not_before=now)

    gmx_fork.drive_external(node, journal, running, selected)


def assert_fresh_completion(source, origin, held, closed):
    assert len(source.selected) == 2
    assert held.owned[0].position is not None and closed.owned[0].position is None
    assert closed.delivered == len(closed.outcomes)
    assert all(not outcome.accounting_issues for outcome in closed.outcomes)
    assert_payloads(source, origin)


def assert_payloads(source, origin):
    for row in source.selected:
        assert row["publication_block"]["number"] > origin.number
        assert [report["signed_payload"] for report in row["reports"]] == [
            report["signed_payload"] for report in row["source_reports"]
        ]
