"""Production EOA semantics using only identified loopback forks and public test keys."""

import json
import signal
import socket
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from almanak.gateway.credentials import write_private
from almanak.gateway.evm import chain_number
from almanak.gateway.fork_config import ForkEoaFund
from almanak.gateway.fork_workspace import ForkSet
from almanak.gateway.provisioning import production_configuration
from almanak.models import Account, AccountBinding, Eoa
from almanak.state import Journal
from tests.fork.support import CLI, processes, request_teardown, resume, wait_state
from tests.fork.test_production import encrypt_test_keys
from tests.strategy_fork.adapter import production_rehearsal_config_for, resolve_reference
from tests.strategy_fork.harness import (
    discover,
    drive_external,
    load_assertions,
    retain,
    stop_runtime,
    verify_hold,
)
from tests.strategy_fork.rehearsal import (
    production_references,
    qualifying_references,
    rehearsal_author,
    rehearsal_fork_operator,
)

ROOT = Path(__file__).parents[2]


PRODUCTION_REFERENCES = frozenset(production_references(qualifying_references()))
CONVERTED_CASES = tuple(
    case
    for case in discover(ROOT / "strategies", Path(__file__).parent / "scenarios")
    if case.name in PRODUCTION_REFERENCES
)
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


def eoa_funding(author):
    index = int(uuid4().hex[:7], 16)
    funds = []
    for offset, fund in enumerate(author.funds):
        operator = next(o for o in author.operators if o.name == fund.operator)
        funds.append(
            ForkEoaFund(
                account=fund.account,
                native_balance=operator.native_balance,
                assets=fund.assets,
                foundry_account_index=index + offset,
            )
        )
    author = rehearsal_policy(author)
    return type(author).model_validate(
        {**author.model_dump(), "funds": tuple(funds), "operators": ()}
    )


def rehearsal_policy(author):
    if author.production is not None:
        return author
    policies = json.loads((Path(__file__).with_name("rehearsal-policy.json")).read_text())
    networks, gas = [], []
    for target in author.forks:
        policy = policies[str(target.chain.chain_id)]
        accounts = tuple(
            dict.fromkeys(a.account for a in author.allocations if a.chain == target.chain)
        )
        networks.append(
            {
                "chain": target.chain.model_dump(mode="json"),
                "provider": target.provider.model_dump(mode="json"),
                "max_observation_age_seconds": 180,
            }
        )
        gas.append({**policy, "accounts": accounts})
    return type(author).model_validate_json(
        json.dumps(
            {**author.model_dump(mode="json"), "production": {"networks": networks, "gas": gas}}
        )
    )


def prepare(nodes, author, directory):
    fork = nodes.prepare(author, directory)
    accounts = []
    for binding in fork.accounts.bindings:
        origin = nodes.for_chain(binding.profile.fund.chain).origin.chain
        accounts.append(
            AccountBinding(
                name=binding.name,
                profile=Eoa(
                    kind="eoa", fund=Account(chain=origin, address=binding.profile.fund.address)
                ),
            )
        )
    production = production_configuration(author, accounts)
    write_private(directory / "production.json", production.model_dump_json(indent=2))
    encrypt_test_keys(directory)
    routes = []
    for origin, node in nodes.nodes.items():
        node.rpc("anvil_setChainId", [chain_number(origin.chain_id)])
        node.rpc.signing_chain = origin.chain_id
        # Fee history must include a locally mined block, independent of the
        # archive provider's fee-history support at the fork origin.
        node.rpc("evm_mine", [])
        routes.append(
            {
                "chain": origin.model_dump(mode="json"),
                "endpoint": node.rpc.endpoint,
                "identity": node.rpc("anvil_metadata"),
            }
        )
    write_private(directory / "rehearsal-routes.json", json.dumps(routes, indent=2))
    return production


def launch_gateway(directory, launch):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    arguments = [
        sys.executable,
        str(ROOT / "tests/fork/production_gateway_driver.py"),
        "--port",
        str(port),
        "--routes",
        str(directory / "rehearsal-routes.json"),
    ]
    for option, filename in (
        ("config", "production.json"),
        ("key-file", "keystores.json"),
        ("password-file", "password"),
        ("database", "state.sqlite"),
        ("auth-token-file", "gateway-auth.token"),
    ):
        arguments.extend(("--" + option, str(directory / filename)))
    process = launch(arguments, ready="gateway_ready")
    control = [
        "--config",
        str(directory / "production.json"),
        "--gateway-url",
        f"http://127.0.0.1:{port}",
        "--gateway-auth-file",
        str(directory / "gateway-auth.token"),
    ]
    return process, arguments, control


def exercise(case, author, env_file, temporary, phases):
    assertions = load_assertions(case)
    # Process stops wait for an in-flight evaluation, as the fork lifecycle does.
    process_timeout = getattr(assertions, "process_timeout", 20)
    with ForkSet(author, env_file) as nodes, processes(temporary) as launch:
        directory = temporary / "deployment"
        prepare(nodes, author, directory)
        node = next(iter(nodes.nodes.values())) if len(nodes.nodes) == 1 else nodes
        gateway, gateway_args, control = launch_gateway(directory, launch)
        journal = Journal(directory / "state.sqlite")
        runtime = launch([CLI, "service", "start", "--strategy", str(case.strategy), *control])
        # Connector-owned requests (GMX orders and deposits) settle only through a
        # keeper, which the supervised fork harness drives on the identified fork.
        drive_external(assertions, node, journal, runtime)
        holding = wait_state(
            journal, assertions.entered, runtime, timeout=getattr(assertions, "entry_timeout", 180)
        )
        verify_hold(node, assertions, journal, runtime, holding)
        phases.append("production_eoa_entered")
        stop_runtime(runtime, timeout=process_timeout)
        runtime = resume(launch, journal, control)
        verify_hold(node, assertions, journal, runtime, holding)
        phases.append("runtime_resumed")
        stop_runtime(runtime, timeout=process_timeout)
        gateway.send_signal(signal.SIGINT)
        assert gateway.wait(timeout=process_timeout) == 0
        launch(gateway_args, ready="gateway_ready")
        runtime = resume(launch, journal, control)
        verify_hold(node, assertions, journal, runtime, holding)
        phases.append("gateway_replaced")
        if exercise_hold := getattr(assertions, "exercise_hold", None):
            exercise_hold(node, journal, runtime)
        request_teardown(control)
        drive_external(assertions, node, journal, runtime)
        closed = wait_state(
            journal,
            lambda s: s.mode == "closed",
            runtime,
            timeout=getattr(assertions, "teardown_timeout", 180),
        )
        assert runtime.wait(timeout=process_timeout) == 0
        assertions.assert_closed(node, closed)
        assert_production_payers(closed)
        phases.append("canonical_receipts_and_zero_obligations")


def assert_production_payers(closed):
    """Every submitted transaction is paid under its account's production EOA policy.

    A keeper settlement (a GMX request outcome) has no submitter charge. It must
    settle a retained request in a transaction no bound account sent.
    """
    submitted = [o for o in closed.outcomes if "gas_paid" in type(o).model_fields]
    settled = [o for o in closed.outcomes if "gas_paid" not in type(o).model_fields]
    assert submitted
    assert all(
        o.gas_paid.asset == closed.config.production.policy(o.operation.account).asset
        for o in submitted
    )
    assert all(o.execution.binding.profile.kind == "eoa" for o in submitted)
    accounts = {b.profile.fund.address.lower() for b in closed.config.accounts.bindings}
    hashes = {o.tx_hash for o in submitted}
    assert all(
        getattr(o, "request", None) in closed.requests
        and o.receipt["from"].lower() not in accounts
        and o.tx_hash not in hashes
        for o in settled
    )


@pytest.mark.parametrize("case", CONVERTED_CASES, ids=lambda case: case.name)
def test_production_reference_path_on_identified_fork(case, request, tmp_path):
    """Run each production-capable reference with explicit rehearsal-only authority."""
    temporary = tmp_path / case.name
    temporary.mkdir()
    author_path = rehearsal_author(case.config.parent, temporary)
    operator_path = rehearsal_fork_operator(case.name)
    env_file = request.config.getoption("--fork-env").resolve()
    progress = resolve_reference(author_path, operator_path, env_file, temporary / "resolution")
    author = eoa_funding(production_rehearsal_config_for(author_path, operator_path, progress))
    artifact = ROOT / "artifacts/strategy-fork" / (case.name + "-production-eoa-" + uuid4().hex)
    artifact.mkdir(parents=True)
    record = {
        "strategy": case.name,
        "profile": "production_eoa_rehearsal",
        "status": "failed",
        "phases": [],
    }
    try:
        exercise(case, author, env_file, temporary, record["phases"])
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
