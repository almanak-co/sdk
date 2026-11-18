"""Process/fork proof of production semantics; the upstream transport is read-only."""

import json
import signal
import socket
import sqlite3
import sys
import time
from pathlib import Path
from uuid import uuid4

import pytest
from eth_account import Account as Signer

from almanak.connectors.aave_v4.models import LendingConfig
from almanak.gateway.credentials import write_private
from almanak.gateway.fork_fixture import Fork
from almanak.reporting import report
from almanak.state import Journal
from tests.author_evidence import retained_author
from tests.fork.production_setup import provision_authority, rehearsal_config
from tests.fork.support import CLI, processes, request_teardown, resume, wait_state
from tests.strategy_fork.scenarios.aave_v4_weth_supply.assertions import (
    assert_capital,
    assert_nonce_scopes,
    assert_payer_gas,
    assert_repayment_cost,
    assert_zero_obligations,
    entered,
)

ROOT = Path(__file__).parents[2]
STRATEGY = ROOT / "strategies/aave_v4_weth_supply/strategy.py"
pytestmark = pytest.mark.fork


def prepare(node, original, directory):
    fork_config = node.prepare(original, directory)
    config = production_config(original, fork_config)
    write_private(directory / "production.json", config.model_dump_json(indent=2))
    encrypt_test_keys(directory)
    provision_authority(node, config, directory)
    node.rpc("anvil_setChainId", [1])
    node.rpc("evm_setNextBlockTimestamp", [int(time.time())])
    node.rpc("evm_mine")
    write_private(directory / "identity.json", json.dumps(node.rpc("anvil_metadata")))
    return config


def production_config(original, fork_config):
    example = json.loads((retained_author(STRATEGY, "production.example.json")).read_text())
    names = [binding.name for binding in fork_config.accounts.bindings]
    example["production"]["gas"][0]["accounts"] = names
    raw = fork_config.model_dump(mode="json")
    raw.pop("fork_instance")
    raw.pop("fork_origin")
    raw["production"] = example["production"]
    raw["allocations"] = [a.model_dump(mode="json") for a in original.allocations]
    raw["debts"] = [d.model_dump(mode="json") for d in original.debts]
    for allocation in (*raw["allocations"], *raw["debts"]):
        allocation["hold_seconds"] = None
    bind_mainnet_accounts(raw, original.origin.chain)
    return LendingConfig.model_validate_json(json.dumps(raw))


def bind_mainnet_accounts(raw, chain):
    for binding in raw["accounts"]["bindings"]:
        profile = binding["profile"]
        profile["fund"]["chain"] = chain.model_dump(mode="json")
        if "operator" in profile:
            profile["operator"]["chain"] = chain.model_dump(mode="json")


def encrypt_test_keys(directory):
    keys = json.loads((directory / "fork-keys.json").read_text())
    stores = {
        name: Signer.encrypt(key, "public-test-password", kdf="pbkdf2", iterations=1000)
        for name, key in keys.items()
    }
    write_private(directory / "keystores.json", json.dumps(stores))
    write_private(directory / "password", "public-test-password")


def gateway_arguments(node, directory):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    args = [
        sys.executable,
        str(Path(__file__).with_name("production_gateway_driver.py")),
        "--fork-url",
        node.rpc.endpoint,
        "--port",
        str(port),
    ]
    for name, filename in (
        ("config", "production.json"),
        ("identity", "identity.json"),
        ("key-file", "keystores.json"),
        ("password-file", "password"),
        ("auth-token-file", "gateway-auth.token"),
        ("database", "state.sqlite"),
        ("receipt-gate", "hold-supply-receipt"),
    ):
        args.extend(("--" + name, str(directory / filename)))
    control = [
        "--config",
        str(directory / "production.json"),
        "--gateway-url",
        f"http://127.0.0.1:{port}",
        "--gateway-auth-file",
        str(directory / "gateway-auth.token"),
    ]
    return args, control


def run_lifecycle(launch, node, directory):
    receipt_gate = directory / "hold-supply-receipt"
    receipt_gate.touch()
    gateway_args, control = gateway_arguments(node, directory)
    gateway = launch(gateway_args, ready="gateway_ready")
    journal = Journal(directory / "state.sqlite")
    runtime = launch([CLI, "service", "start", "--strategy", str(STRATEGY), *control])
    interrupted = wait_state(journal, lambda s: pending_supply(node, s), runtime)
    pending_hash = interrupted.pending.tx_hash
    runtime.kill()
    runtime.wait(timeout=20)
    gateway.kill()
    gateway.wait(timeout=20)
    gateway = launch(gateway_args, ready="gateway_ready")
    wait_expired(journal)
    runtime = resume(launch, journal, control)
    assert journal.load().pending.tx_hash == pending_hash
    receipt_gate.unlink()
    holding = wait_state(
        journal,
        entered,
        runtime,
    )
    assert_holding(holding)
    stop_cleanly(runtime, 130)
    stop_cleanly(gateway, 0)
    launch(gateway_args, ready="gateway_ready")
    runtime = launch([CLI, "service", "resume", *control])
    request_teardown(control)
    closed = wait_state(journal, lambda s: s.mode == "closed", runtime)
    assert runtime.wait(timeout=20) == 0
    assert_closed(closed)
    assert_closed_accounting(closed)
    finalized = int(node.rpc("eth_getBlockByNumber", ["finalized", False])["number"], 16)
    assert closed.outcomes[-1].after.position.reserve.block.number > finalized
    return journal


def stop_cleanly(process, expected):
    process.send_signal(signal.SIGINT)
    assert process.wait(timeout=20) == expected


def wait_expired(journal):
    deadline = time.monotonic() + 35
    while time.monotonic() < deadline:
        with journal.connect() as db:
            expires = db.execute("SELECT expires FROM coordinator WHERE id=1").fetchone()[0]
        if expires <= time.time():
            return
        time.sleep(0.1)
    raise AssertionError("dead coordinator retained an unexpired lease")


def pending_supply(node, state):
    if state.pending is not None and state.pending.operation.kind == "supply":
        return node.rpc("eth_getTransactionReceipt", [state.pending.tx_hash]) is not None
    return False


def assert_holding(state):
    assert entered(state)
    assert all(state.shares(state.config.target(a.name)) for a in state.config.allocations)
    assert all(
        o.drawn_shares
        for o in state.owned
        if o.target.allocation in {d.name for d in state.config.debts}
    )
    assert not any(o.accounting_issues for o in state.outcomes)


def assert_closed(state):
    assert (state.pending, state.plan, state.decision) == (None, None, None)
    assert state.capabilities is not None
    assert len({o.tx_hash for o in state.outcomes}) == len(state.outcomes)
    assert_nonce_scopes(state)
    assert_obligations_and_capital(state)


def assert_obligations_and_capital(state):
    assert all(o.success for o in state.outcomes)
    for closed in state.closure:
        assert_zero_obligations(closed)
        assert_capital(state, closed)
        assert_payer_gas(state, closed)
    for debt in state.config.debts:
        assert_repayment_cost(state, debt)


def assert_closed_accounting(state):
    assert not any(item.accounting_issues for item in state.outcomes)


def retain(journal):
    artifact = ROOT / "artifacts/production-rehearsal" / uuid4().hex
    artifact.mkdir(parents=True)
    with journal.connect() as source, sqlite3.connect(artifact / "state.sqlite") as target:
        source.backup(target)
    write_private(artifact / "report.json", json.dumps(report(journal.load()), indent=2))
    write_private(
        artifact / "verification.json",
        json.dumps(
            {
                "evidence": "isolated_anvil_production_path_rehearsal",
                "live_funds": False,
                "phases": [
                    "gateway_and_runtime_killed_with_mined_supply_receipt_withheld",
                    "same_pending_hash_recovered",
                    "supply_all_positions",
                    "runtime_and_gateway_restart",
                    "withdraw_and_close",
                    "closed_before_last_receipt_finalized",
                ],
                "limits": "Production transport guard replaced with an identified-fork guard; "
                "no live execution proof.",
            },
            indent=2,
        ),
    )
    print("Production rehearsal evidence:", artifact)


@pytest.mark.parametrize("profile", ["eoa_supply", "eoa_borrow", "safe_borrow"])
def test_production_lifecycle_on_identified_fork(tmp_path, request, profile):
    author = rehearsal_config(profile)
    env_file = request.config.getoption("--fork-env").resolve()
    original = author.resolved(author.single_target().resolve(env_file))
    with Fork(env_file, original.origin, target=original.single_target()) as node:
        directory = tmp_path / "deployment"
        prepare(node, original, directory)
        with processes(tmp_path) as launch:
            journal = run_lifecycle(launch, node, directory)
            retain(journal)
