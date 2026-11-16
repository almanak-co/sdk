import json
import signal
import socket
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.connectors.aave_v4.models import LendingConfig
from almanak.gateway.fork_fixture import Fork, Rpc
from almanak.runtime import report
from almanak.state import Journal
from tests.fork.aave_assertions import assert_closed, outcomes_of_kind
from tests.fork.support import (
    assert_teardown_response,
    control_arguments,
    request_teardown,
    resume,
    start_gateway,
    wait_state,
)
from tests.fork.support import launch as launch

pytestmark = pytest.mark.fork
ROOT = Path(__file__).resolve().parents[2]
CLI = str(Path(sys.executable).with_name("almanak"))
STRATEGY = ROOT / "strategies/aave_v4_weth_supply/strategy.py"
FORK_CONFIG = ROOT / "tests/fixtures/aave-v4-interval-config.json"
AUTHOR_CONFIG = ROOT / "tests/fixtures/author/aave_v4_weth_supply/config.json"


@pytest.fixture(scope="module")
def fork(request):
    config = ForkConfig.model_validate_json(FORK_CONFIG.read_text())
    with Fork(request.config.getoption("--fork-env").resolve(), config.origin) as running:
        yield running


def setup_run(fork, directory, launch, count=1, positions=False, decision_interval=None):
    from tests.fork.eoa import configuration

    selected = ForkConfig.model_validate_json(json.dumps(configuration())).resolved(fork.origin)
    raw = selected.model_dump()
    if decision_interval is not None:
        raw["decision_trigger"]["seconds"] = decision_interval
    raw = select_allocations(raw, getattr(fork, "account_index", 0), count, positions)
    fork.account_index = getattr(fork, "account_index", 0) + count
    directory = directory / "deployment"
    config = fork.prepare(ForkConfig.model_validate(raw), directory)
    journal, arguments = start_gateway(fork, directory, launch)
    return config, journal, arguments


def select_allocations(raw, start_index, count, positions):
    raw["funds"] = tuple(
        {
            **raw["funds"][0],
            "foundry_account_index": start_index + index,
            "account": f"lending_{index}",
        }
        for index in range(count)
    )
    selected_allocations = raw["allocations"] if positions else raw["allocations"][:1]
    raw["allocations"] = tuple(
        clone_allocation(allocation, index, positions)
        for index in range(count)
        for allocation in selected_allocations
    )
    return raw


def clone_allocation(allocation, index, positions):
    return {
        **allocation,
        "account": f"lending_{index}",
        "name": f"{allocation['name']}_{index}",
        "hold_seconds": allocation["hold_seconds"] if positions else None,
    }


def evidence(journal, name):
    directory = ROOT / "artifacts/fork" / (name + "-" + uuid4().hex[:8])
    directory.mkdir(parents=True)
    with journal.connect() as source, sqlite3.connect(directory / "state.sqlite") as target:
        source.backup(target)
    (directory / "report.json").write_text(json.dumps(report(journal.load()), indent=2) + "\n")
    print("Evidence:", directory)
    return directory


@pytest.mark.parametrize(
    "stage",
    ["reserved", "signed", "prepared", "broadcast", "receipt", "commit", "outcome", "transition"],
)
def test_process_death_reconciles_without_duplicate_supply(fork, tmp_path, launch, stage):
    config, journal, arguments = setup_run(fork, tmp_path, launch)
    crashed = launch(
        [
            sys.executable,
            str(ROOT / "tests/fork/driver.py"),
            "--strategy",
            str(STRATEGY),
            *arguments,
            "--crash",
            stage,
        ]
    )
    assert_exit(crashed, -signal.SIGKILL, timeout=80)
    interrupted = journal.load()
    expected_hash = interrupted_supply_hash(interrupted, stage)
    receipt = fork.rpc("eth_getTransactionReceipt", [expected_hash]) if expected_hash else None
    assert (receipt is None) == (stage in ("reserved", "signed", "prepared"))
    resumed = resume(launch, journal, arguments)
    holding = wait_holding(journal, resumed)
    supplied = outcomes_of_kind(holding.outcomes, "supply")[0]
    assert_recovered_supply(supplied, interrupted, expected_hash)
    competitor = launch([CLI, "service", "resume", *arguments], ready=False)
    assert competitor.wait(timeout=20) != 0
    resumed.kill()
    resumed.wait(timeout=10)
    resumed_again = resume(launch, journal, arguments)
    control = subprocess.run(
        [CLI, "service", "teardown", *control_arguments(arguments)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert_teardown_response(control)
    closed = wait_closed(journal, resumed_again)
    assert_exit(resumed_again, 0, timeout=20)
    assert_closed(fork, closed)
    assert len(config.allocations) == 1
    evidence(journal, "crash-" + stage)


def test_two_wallets_complete_the_same_lifecycle(fork, tmp_path, launch):
    _, journal, arguments = setup_run(fork, tmp_path, launch, count=2)
    running = launch([CLI, "service", "start", "--strategy", str(STRATEGY), *arguments])
    wait_state(
        journal,
        lambda s: len(s.memory.values.get("supplied_at", {})) == 2 and s.pending is None,
        running,
    )
    request_teardown(arguments)
    closed = wait_closed(journal, running)
    assert_exit(running, 0, timeout=20)
    assert_closed(fork, closed)
    assert closed.baseline[0].position.account != closed.baseline[1].position.account
    evidence(journal, "two-wallets")


def test_teardown_after_approval_revokes_without_opening_a_position(fork, tmp_path, launch):
    config, journal, arguments = setup_run(fork, tmp_path, launch)
    crashed = launch(
        [
            sys.executable,
            str(ROOT / "tests/fork/driver.py"),
            "--strategy",
            str(STRATEGY),
            *arguments,
            "--crash",
            "commit",
            "--operation",
            "approve",
        ]
    )
    assert_exit(crashed, -signal.SIGKILL, timeout=80)
    assert not journal.load().memory.values.get("supplied_at")
    request_teardown(arguments)
    resumed = resume(launch, journal, arguments)
    closed = wait_closed(journal, resumed)
    assert_exit(resumed, 0, timeout=20)
    assert [item.operation.kind for item in closed.outcomes] == ["approve", "revoke"]
    assert closed.closure[0].wallet_balance == 4 * config.allocations[0].amount
    assert closed.closure[0].allowance == closed.closure[0].position.supplied_shares == 0
    evidence(journal, "approval-cleanup")


def test_backtest_share_math_matches_isolated_contract_actions(fork, tmp_path, launch):
    config, journal, arguments = setup_run(fork, tmp_path, launch, positions=True)
    crashed = launch(
        [
            sys.executable,
            str(ROOT / "tests/fork/driver.py"),
            "--strategy",
            str(STRATEGY),
            *arguments,
            "--crash",
            "commit",
            "--operation",
            "approve",
        ]
    )
    assert_exit(crashed, -signal.SIGKILL, timeout=80)
    probe_model_action(fork, config, config.allocations[0], supply=True)
    running = resume(launch, journal, arguments)
    wait_state(
        journal,
        lambda s: len(s.memory.values.get("supplied_at", {})) == 2 and s.pending is None,
        running,
    )
    running.kill()
    running.wait(timeout=10)
    fork.rpc("evm_increaseTime", [3600])
    fork.rpc("evm_mine", [])
    for allocation in config.allocations:
        probe_model_action(fork, config, allocation, supply=False)
    request_teardown(arguments)
    resumed = resume(launch, journal, arguments)
    closed = wait_closed(journal, resumed)
    assert_exit(resumed, 0, timeout=20)
    assert_closed(fork, closed)
    evidence(journal, "backtest-contract-math")


def probe_model_action(fork, config, allocation, *, supply):
    from eth_abi import decode, encode
    from eth_utils import keccak

    from almanak.connectors.aave_v4.history import read_market
    from almanak.connectors.aave_v4.reader import read_position
    from almanak.gateway.ethereum import EthereumReader

    source = allocation.reserve
    fund = config.target(allocation.name).fund
    with EthereumReader(source.chain, fork.rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        market = read_market(reader, source, block)
        before = read_position(reader, source, fund, block)
        amount = allocation.capital if supply else 2**256 - 1
        signature = (
            "supply(uint256,uint256,address)" if supply else "withdraw(uint256,uint256,address)"
        )
        data = keccak(text=signature)[:4] + encode(
            ("uint256", "uint256", "address"), (source.reserve_id, amount, fund.address)
        )
        # eth_call executes the actual protocol action against this exact fork state,
        # including its checks and rounding, without persisting the probe's effects.
        raw = fork.rpc(
            "eth_call",
            [
                {"from": fund.address, "to": source.spoke, "data": "0x" + data.hex()},
                {"blockHash": block.hash, "requireCanonical": True},
            ],
        )
        shares, assets = decode(("uint256", "uint256"), bytes.fromhex(raw[2:]))
        assert_model_movement(market, before, amount, (shares, assets), supply=supply)
        assert read_position(reader, source, fund, block) == before


def assert_model_movement(market, before, amount, actual, *, supply):
    from almanak.connectors.aave_v4.history import SUPPORTED_REVISIONS

    assert set(market.revisions) <= set(SUPPORTED_REVISIONS)
    if supply:
        assert actual == (market.supplied_shares(amount), amount)
    else:
        expected_assets = market.supplied_assets(before.supplied_shares)
        assert actual == (market.burned_shares(expected_assets), expected_assets)


def test_changed_config_and_new_database_cannot_take_over_open_supply(fork, tmp_path, launch):
    config, journal, arguments = setup_run(fork, tmp_path, launch)
    running = launch([CLI, "service", "start", "--strategy", str(STRATEGY), *arguments])
    holding = wait_holding(journal, running)
    running.kill()
    running.wait(timeout=10)
    config_path = Path(arguments[1])
    changed = config.model_dump()
    changed["allocations"][0]["amount"] *= 2
    config_path.write_text(LendingConfig.model_validate(changed).model_dump_json())
    rejected = subprocess.run(
        [CLI, "service", "resume", *arguments], capture_output=True, text=True, timeout=20, cwd=ROOT
    )
    assert_refused(rejected, "configuration changed")
    assert journal.load() == holding
    config_path.write_text(config.model_dump_json())
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        competing_port = listener.getsockname()[1]
    gateway_arguments = fork.gateway_arguments.copy()
    gateway_arguments[gateway_arguments.index("--database") + 1] = str(
        tmp_path / "different.sqlite"
    )
    gateway_arguments[gateway_arguments.index("--port") + 1] = str(competing_port)
    launch(gateway_arguments, ready="gateway_ready")
    competing = arguments.copy()
    competing[competing.index("--gateway-url") + 1] = f"http://127.0.0.1:{competing_port}"
    rejected = subprocess.run(
        [CLI, "service", "start", "--strategy", str(STRATEGY), *competing],
        capture_output=True,
        text=True,
        timeout=20,
        cwd=ROOT,
    )
    assert_refused(rejected, "already claimed")
    assert journal.load() == holding
    config_path.unlink()
    request_teardown(arguments)
    resumed = resume(launch, journal, arguments[2:])
    closed = wait_closed(journal, resumed)
    assert_exit(resumed, 0, timeout=20)
    assert_closed(fork, closed)
    evidence(journal, "config-and-claim-refusal")


def test_teardown_reconciles_broadcast_supply_before_withdrawal(fork, tmp_path, launch):
    _, journal, arguments = setup_run(fork, tmp_path, launch)
    crashed = launch(
        [
            sys.executable,
            str(ROOT / "tests/fork/driver.py"),
            "--strategy",
            str(STRATEGY),
            *arguments,
            "--crash",
            "broadcast",
        ]
    )
    assert_exit(crashed, -signal.SIGKILL, timeout=80)
    pending = journal.load().pending
    request_teardown(arguments)
    resumed = resume(launch, journal, arguments)
    closed = wait_closed(journal, resumed)
    assert_exit(resumed, 0, timeout=20)
    assert next(item for item in closed.outcomes if item.operation.kind == "supply").tx_hash == (
        pending.tx_hash
    )
    assert_closed(fork, closed)
    evidence(journal, "pending-teardown")


def test_rewound_fork_cannot_resume_recorded_history(fork, tmp_path, launch):
    _, journal, arguments = setup_run(fork, tmp_path, launch)
    snapshot = fork.rpc("evm_snapshot", [])
    running = launch([CLI, "service", "start", "--strategy", str(STRATEGY), *arguments])
    wait_holding(journal, running)
    request_teardown(arguments)
    closed = wait_closed(journal, running)
    assert_exit(running, 0, timeout=20)
    assert_closed(fork, closed)
    assert fork.rpc("evm_revert", [snapshot]) is True
    block = fork.rpc("eth_blockNumber", [])
    rejected = subprocess.run(
        [CLI, "service", "resume", *arguments], capture_output=True, text=True, timeout=20, cwd=ROOT
    )
    assert_refused(rejected, "absent from this fork history")
    assert journal.load() == closed
    assert fork.rpc("eth_blockNumber", []) == block
    evidence(journal, "rewind-refusal")


def test_documented_cli_runs_recorded_strategy_after_source_is_deleted(request, tmp_path, launch):
    directory = tmp_path / "workspace"
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    setup = launch(
        [
            CLI,
            "service",
            "fork",
            "--config",
            str(FORK_CONFIG),
            "--env-file",
            str(request.config.getoption("--fork-env").resolve()),
            "--output",
            str(directory),
            "--port",
            str(port),
        ],
        ready="fork_ready",
    )
    try:
        endpoint = f"http://127.0.0.1:{port}"
        journal = Journal(directory / "state.sqlite")
        source = tmp_path / "strategy.py"
        source.write_text(STRATEGY.read_text())
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            gateway_port = listener.getsockname()[1]
        gateway_arguments = [
            CLI,
            "service",
            "gateway",
            "--config",
            str(directory / "deployment.json"),
            "--database",
            str(journal.path),
            "--fork-url",
            endpoint,
            "--fork-key-file",
            str(directory / "fork-keys.json"),
            "--auth-token-file",
            str(directory / "gateway-auth.token"),
            "--port",
            str(gateway_port),
            "--lease-seconds",
            "2",
        ]
        gateway = launch(gateway_arguments, ready="gateway_ready")
        arguments = [
            "--gateway-url",
            f"http://127.0.0.1:{gateway_port}",
            "--gateway-auth-file",
            str(directory / "gateway-auth.token"),
        ]
        running = launch(
            [
                CLI,
                "service",
                "start",
                "--strategy",
                str(source),
                "--config",
                str(directory / "deployment.json"),
                *arguments,
            ]
        )
        holding = wait_holding(journal, running)
        running.send_signal(signal.SIGINT)
        assert_exit(running, 130, timeout=20)
        assert setup.poll() is None
        source.write_text('raise RuntimeError("changed source must not execute")')
        source.unlink()
        (directory / "deployment.json").unlink()
        resumed = resume(launch, journal, arguments)
        assert journal.load().strategy == holding.strategy
        control = subprocess.run(
            [CLI, "service", "teardown", *arguments],
            capture_output=True,
            text=True,
            timeout=20,
        )
        assert control.returncode == 0, control.stderr
        closed = wait_closed(journal, resumed)
        assert_exit(resumed, 0, timeout=20)
        assert_closed(SimpleNamespace(rpc=Rpc(endpoint)), closed)
        assert_private_credentials(directory)
        assert (
            closed.config.accounts.bindings[0].profile.fund.address
            == "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
        )
        evidence(journal, "folder-strategy-cli")
        gateway.send_signal(signal.SIGINT)
        assert_exit(gateway, 0, timeout=20)
    finally:
        setup.send_signal(signal.SIGINT)
        assert_exit(setup, 0, timeout=20)


@pytest.mark.parametrize("stage", ["sign", "broadcast"])
def test_gateway_death_preserves_admitted_attempt(fork, tmp_path, launch, stage):
    _, journal, arguments = setup_run(fork, tmp_path, launch)
    fork.gateway_process.kill()
    fork.gateway_process.wait(timeout=10)
    killed_gateway = launch(
        [
            sys.executable,
            str(ROOT / "tests/fork/gateway_driver.py"),
            *fork.gateway_arguments[2:],
            "--crash",
            stage,
        ],
        ready="gateway_ready",
    )
    runtime = launch([CLI, "service", "start", "--strategy", str(STRATEGY), *arguments])
    assert_exit(killed_gateway, -signal.SIGKILL, timeout=80)
    assert runtime.wait(timeout=20) != 0
    interrupted = journal.load()
    expected_hash = interrupted_gateway_hash(fork, interrupted, stage)
    launch(fork.gateway_arguments, ready="gateway_ready")
    assert journal.load() == interrupted
    running = resume(launch, journal, arguments)
    holding = wait_holding(journal, running)
    supplied = outcomes_of_kind(holding.outcomes, "supply")[0]
    assert_recovered_supply(supplied, interrupted, expected_hash)
    request_teardown(arguments)
    closed = wait_closed(journal, running)
    assert_exit(running, 0, timeout=20)
    assert_closed(fork, closed)
    evidence(journal, "gateway-crash-" + stage)


def interrupted_supply_hash(interrupted, stage):
    if stage in ("commit", "outcome", "transition"):
        assert interrupted.pending is None
        return interrupted.outcomes[-1].tx_hash
    elif stage in ("reserved", "signed"):
        assert_unsigned_supply(interrupted)
        return None
    else:
        assert interrupted.pending.operation.kind == "supply"
        return interrupted.pending.tx_hash


def wait_holding(journal, process):
    return wait_state(
        journal,
        lambda s: (
            len(s.memory.values.get("supplied_at", {})) == len(s.config.allocations)
            and s.pending is None
        ),
        process,
    )


def wait_closed(journal, process):
    return wait_state(journal, lambda s: s.mode == "closed", process)


def assert_exit(process, code=0, *, timeout=20):
    assert process.wait(timeout=timeout) == code


def assert_refused(result, message):
    assert result.returncode != 0
    assert message in result.stderr


def assert_recovered_supply(supplied, interrupted, expected_hash):
    if expected_hash is not None:
        assert supplied.tx_hash == expected_hash
    else:
        assert supplied.nonce == interrupted.plan.nonce


def interrupted_gateway_hash(fork, interrupted, stage):
    if stage == "broadcast":
        assert interrupted.pending is not None
        expected_hash = interrupted.pending.tx_hash
        assert fork.rpc("eth_getTransactionReceipt", [expected_hash]) is not None
        return expected_hash
    else:
        assert_unsigned_supply(interrupted)
        return None


def assert_private_credentials(directory):
    assert (directory / "fork-keys.json").stat().st_mode & 0o777 == 0o600
    assert (directory / "gateway-auth.token").stat().st_mode & 0o777 == 0o600


def assert_unsigned_supply(interrupted):
    assert interrupted.pending is None
    assert interrupted.plan.operation.kind == "supply"


@pytest.mark.parametrize("stage", ["prepared", "broadcast", "outcome", "transition"])
def test_two_positions_share_wallet_and_close_independently(fork, tmp_path, launch, stage):
    config, journal, arguments = setup_run(fork, tmp_path, launch, positions=True)
    first, second = config.allocations
    crashed = launch(
        [
            sys.executable,
            str(ROOT / "tests/fork/driver.py"),
            "--strategy",
            str(STRATEGY),
            *arguments,
            "--crash",
            stage,
            "--allocation",
            second.name,
        ]
    )
    assert_exit(crashed, -signal.SIGKILL, timeout=80)
    interrupted = journal.load()
    assert interrupted.owned[0].shares > 0
    running = resume(launch, journal, arguments)
    holding = wait_holding(journal, running)
    assert_two_owned_positions(holding)
    running.send_signal(signal.SIGINT)
    assert_exit(running, 130)
    fork.gateway_process.kill()
    fork.gateway_process.wait(timeout=10)
    fork.gateway_process = launch(fork.gateway_arguments, ready="gateway_ready")
    running = resume(launch, journal, arguments)
    assert journal.load().memory == holding.memory
    fork.rpc("evm_increaseTime", [first.hold_seconds + 1])
    fork.rpc("evm_mine")
    reduced = wait_state(journal, only_second_position_remains, running)
    assert reduced.mode == "running"
    assert reduced.owned[1] == holding.owned[1]
    assert_remaining_chain_position(fork, config, second, reduced.owned[1].shares)
    request_teardown(arguments)
    closed = wait_closed(journal, running)
    assert_exit(running)
    assert_closed(fork, closed)
    assert_wallet_nonce_sequence(closed)
    artifact = evidence(journal, "two-positions-" + stage)
    (artifact / "holding.json").write_text(holding.model_dump_json(indent=2) + "\n")
    (artifact / "one-position-remaining.json").write_text(reduced.model_dump_json(indent=2) + "\n")
    dashboard_lifecycle_evidence(artifact, interrupted, holding, reduced, closed)


def dashboard_lifecycle_evidence(artifact, interrupted, holding, reduced, closed):
    from almanak.dashboard.evaluate import evaluate
    from almanak.dashboard.preview import render_html, write_private

    for phase, state in (
        ("interrupted", interrupted),
        ("holding", holding),
        ("reduced", reduced),
        ("closed", closed),
    ):
        response = evaluate(state)
        assert response.status == "ready", response.diagnostic
        for owned in state.owned:
            assert response.snapshot.values[owned.target.allocation + "-shares"].amount == str(
                owned.shares
            )
        write_private(artifact / f"dashboard-{phase}.json", response.model_dump_json(indent=2))
        write_private(artifact / f"dashboard-{phase}.html", render_html(response))


def assert_two_owned_positions(state):
    assert all(item.shares > 0 for item in state.owned)
    assert len(state.config.accounts.bindings) == 1


def only_second_position_remains(state):
    return (
        state.owned[0].shares == 0
        and state.owned[1].shares > 0
        and state.pending is None
        and state.delivered == len(state.outcomes)
    )


def assert_remaining_chain_position(fork, config, allocation, shares):
    from almanak.connectors.aave_v4.reader import read_position
    from almanak.gateway.ethereum import EthereumReader

    fund = config.accounts.binding(allocation.account).profile.fund
    with EthereumReader(fund.chain, fork.rpc.endpoint) as reader:
        block = reader.pin_block(int(fork.rpc("eth_blockNumber"), 16))
        observed = read_position(reader, allocation.reserve, fund, block)
    assert observed.supplied_shares == shares
    assert observed.total_debt == 0


def assert_wallet_nonce_sequence(state):
    nonces = [item.nonce for item in state.outcomes]
    assert nonces == list(range(nonces[0], nonces[0] + len(nonces)))
    assert len(state.outcomes) == 6
    assert state.delivered == len(state.outcomes)
    assert_reported_wallet_gas(state)


def assert_reported_wallet_gas(state):
    assert report(state)["funds"][0]["gas_paid"]["amount"] == str(
        sum(item.gas_paid.amount for item in state.outcomes)
    )
