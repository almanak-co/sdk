"""Faults that cross the runtime for the HyperCore perp reference (blueprint 30, 13.4).

The strategy fork harness runs the reference's lifecycle through the public CLI
(`tests/strategy_fork/scenarios/hyperliquid_hyperevm_perp`), but it has no hook that
stops the coordinator between a venue post and the post's recovery, and none that lands
an outside transaction in the block of the coordinator's own. This module drives the
same reference, fork workspace (the HyperEVM node and the `hypercore-model` venue
process) and gateway service, with the coordinator run as the crash driver
(`tests/fork/driver.py`) and then resumed as `almanak service resume`:

- a withheld order answer, recovered after a coordinator restart: the venue process
  applies the entry and withholds its answer, the coordinator is killed right after the
  post, and the resumed coordinator resolves the attempt from its own evidence
  (`orderStatus` by cloid) without posting it again;
- a custody action dropped beside a matching credit, which halts: in teardown the venue
  process drops the withdrawal's move of perp USDC to spot while an outside `depositFor`
  credits that amount to the Safe's spot balance in the same block, so the balance moves
  as if the move applied. The request stays unresolved, and once its window passes the
  coordinator halts with the request and the evidence that identified no effect of it.

The HyperEVM side is fork evidence; every HyperCore effect is the synthesized model's
(section 9), so nothing here is evidence of HyperCore's behaviour.
"""

import json
import signal
import sys
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_utils import to_checksum_address

from almanak.caip import AssetType
from almanak.connectors.hyperliquid import corewriter as cw
from almanak.connectors.hyperliquid.assets import CORE_DEPOSIT_WALLET
from almanak.connectors.hyperliquid.fork_model import REGISTER
from almanak.connectors.hyperliquid.fork_venue import ModelKeeper
from almanak.connectors.hyperliquid.state import Pending, VenuePending
from almanak.gateway.fork_funding import fund_erc20
from almanak.gateway.fork_workspace import ForkSet
from almanak.gateway.safe_fixture import calldata
from tests.fork.support import processes, request_teardown, resume, start_gateway, wait_state
from tests.strategy_fork.adapter import fork_config_for, resolve_reference
from tests.strategy_fork.harness import Case, install_permissions, load_assertions

pytestmark = pytest.mark.fork

ROOT = Path(__file__).resolve().parents[2]
NAME = "hyperliquid_hyperevm_perp"
CASE = Case(
    NAME,
    ROOT / "strategies" / NAME / "strategy.py",
    ROOT / "strategies" / NAME / "config.yaml",
    ROOT / "tests/strategy_fork/scenarios" / NAME / "assertions.py",
)
OPERATOR = ROOT / "examples/operators" / (NAME + ".fork.yaml")
DRIVER = Path(__file__).with_name("driver.py")
USDC = AssetType("eip155:999/erc20:" + cw.USDC_CONTRACT)
# The custody request window (`CustodyPolicy.window`), passed by a second.
WINDOW_SECONDS = 121
LIFECYCLE_SECONDS = 900
ASSERTIONS = load_assertions(CASE)


@pytest.fixture(scope="module")
def world(request, tmp_path_factory):
    env_file = request.config.getoption("--fork-env").resolve()
    root = tmp_path_factory.mktemp("hyperliquid-faults")
    progress = resolve_reference(CASE.config, OPERATOR, env_file, root / "resolution")
    config = fork_config_for(CASE.config, OPERATOR, progress)
    with ForkSet(config, env_file) as nodes, processes(root) as launch:
        directory = root / "deployment"
        deployment = nodes.prepare(config, directory)
        journal, arguments = start_gateway(nodes, directory, launch, lease_seconds=5)
        install_permissions(CASE, nodes, directory, deployment, arguments)
        (route,) = json.loads((directory / "fork-routes.json").read_text())
        yield SimpleNamespace(
            root=root,
            node=next(iter(nodes.nodes.values())),
            launch=launch,
            journal=journal,
            arguments=arguments,
            model=ModelKeeper(route["venues"]["hypercore"]["model"]),
            report={"register": REGISTER, "origin": route["chain"]},
        )


def model(world) -> dict:
    return world.model.control({"op": "state"})["model"]


# A withheld order answer, recovered after a coordinator restart.


def crashed_after_the_entry_post(world):
    """The coordinator posts the entry, which the venue applies without answering, and is
    killed before it reads anything else."""
    world.model.control({"op": "withhold", "action": "order"})
    driver = world.launch(
        [
            sys.executable,
            str(DRIVER),
            "--strategy",
            str(CASE.strategy),
            *world.arguments,
            "--crash",
            "broadcast",
            "--operation",
            "hyperliquid_venue_action",
            "--purpose",
            "entry",
        ]
    )
    assert driver.wait(timeout=LIFECYCLE_SECONDS) == -signal.SIGKILL
    pending = world.journal.load().pending
    assert isinstance(pending, VenuePending) and pending.operation.attempt.purpose == "entry"
    assert not pending.unsent and pending.answer is None and pending.resolved is None
    venue = applied_unanswered(world, pending)
    world.report["withheld"] = {
        "attempt": pending.operation.attempt.model_dump(mode="json"),
        "injected": venue["injected"][-1],
    }
    return pending


def applied_unanswered(world, pending) -> dict:
    """The venue applied the order it never answered."""
    venue = model(world)
    assert {"kind": "withhold", "action": "order"}.items() <= venue["injected"][-1].items()
    order = pending.operation.attempt.request.action.order
    position = venue["positions"][pending.operation.target.fund.address]["BTC"]
    assert Decimal(position["szi"]) == order.size
    return venue


def recovered_after_restart(world, crashed):
    """The resumed coordinator resolves the maybe-sent entry from its own evidence."""
    running = resume(world.launch, world.journal, world.arguments)
    state = wait_state(world.journal, ASSERTIONS.entered, running, timeout=LIFECYCLE_SECONDS)
    attempt = state.trading.attempt(crashed.operation.attempt.id)
    assert attempt.status == "applied" and attempt.answer is None and attempt.evidence
    assert [a.purpose for a in state.trading.attempts] == ["leverage", "entry"]
    ASSERTIONS.assert_holding(world.node, state)
    # A lost answer never posts again: the venue received the entry's bytes once.
    entry = {"nonce": attempt.request.nonce, "type": "order"}
    assert model(world)["posts"].count(entry) == 1
    world.report["recovered"] = {
        "attempt": attempt.model_dump(mode="json"),
        "owned": state.owned[0].model_dump(mode="json"),
    }
    return running, state


# A custody action dropped beside a matching credit, which halts.


def outside_funder(world):
    """An address outside the Safe's authority, holding USDC it has approved for
    CoreDepositWallet."""
    node = world.node
    funder = node.funded_signer("", 0, 10**18, 85)
    fund_erc20(node, funder.address, USDC, 100_000_000)
    approve = calldata(
        "approve(address,uint256)", ("address", "uint256"), (CORE_DEPOSIT_WALLET, 100_000_000)
    )
    node.rpc.send(funder, to_checksum_address(cw.USDC_CONTRACT), approve)
    return funder


def moving_to_spot(state) -> bool:
    pending = state.pending
    return (
        isinstance(pending, Pending)
        and pending.operation.kind == "hyperliquid_class_transfer"
        and not pending.operation.to_perp
        and not pending.unsent
    )


def dropped_beside_a_credit(world, running, safe):
    """Teardown closes the position; the withdrawal's move to spot is dropped while the
    outside funder's `depositFor` of the same amount lands in the same block."""
    node, funder = world.node, outside_funder(world)
    world.model.control({"op": "drop", "action": cw.USD_CLASS_TRANSFER, "user": safe})
    node.rpc("evm_setAutomine", [False])
    try:
        request_teardown(world.arguments)
        pending = wait_state(world.journal, moving_to_spot, running, timeout=LIFECYCLE_SECONDS)
        pending = pending.pending
        broadcast(node, pending.tx_hash)
        deposit = depositing(node, funder, safe, pending.operation.amount.amount)
        node.rpc("evm_mine")
    finally:
        node.rpc("evm_setAutomine", [True])
    own, outside = node.rpc.wait_receipt(pending.tx_hash), node.rpc.wait_receipt(deposit)
    assert own["blockNumber"] == outside["blockNumber"]
    assert own["status"] == outside["status"] == "0x1"
    world.report["same_block"] = {"own": own["transactionHash"], "outside": deposit}
    return pending


def broadcast(node, tx_hash) -> None:
    """Wait for the coordinator's own transaction to reach the unmined pool."""
    deadline = time.monotonic() + 60
    while node.rpc("eth_getTransactionByHash", [tx_hash]) is None:
        assert time.monotonic() < deadline, "the move to spot was never broadcast"
        time.sleep(0.1)


def depositing(node, funder, safe, amount) -> str:
    data = calldata(
        "depositFor(address,uint256,uint32)",
        ("address", "uint256", "uint32"),
        (safe, amount, cw.SPOT_DEX),
    )
    transaction = {
        "chainId": int(node.rpc("eth_chainId"), 16),
        "nonce": int(node.rpc("eth_getTransactionCount", [funder.address, "pending"]), 16),
        "to": to_checksum_address(CORE_DEPOSIT_WALLET),
        "data": data,
        "value": 0,
        "gas": 300_000,
        "gasPrice": int(node.rpc("eth_gasPrice"), 16),
    }
    raw = "0x" + funder.sign_transaction(transaction).raw_transaction.hex()
    return node.rpc("eth_sendRawTransaction", [raw])


def unresolved_move(state) -> bool:
    last = state.custody.requests[-1]
    return (last.kind, last.to_perp, last.status) == ("class_transfer", False, "unresolved")


def halted_with_its_evidence(world, running, pending, safe):
    """The move stays unresolved while the Safe's spot USDC grew by its amount; past the
    window the coordinator halts and names the request and what it observed."""
    state = wait_state(world.journal, unresolved_move, running, timeout=LIFECYCLE_SECONDS)
    assert model(world)["injected"][-1]["kind"] == "drop"
    world.node.rpc("evm_increaseTime", [WINDOW_SECONDS])
    world.node.rpc("evm_mine")
    assert running.wait(timeout=300) != 0
    halted = halted_in_teardown(world, state)
    refusal = logged_refusal(world.root)
    assert_names_the_request(refusal, halted)
    assert_credit_without_move(refusal, pending.operation.amount.amount, safe)
    world.report["dropped_beside_matching_credit"] = refusal


def halted_in_teardown(world, state):
    halted = world.journal.load()
    assert halted.mode == "teardown" and halted.decision.intent.kind == "withdraw"
    assert halted.custody.requests == state.custody.requests and unresolved_move(halted)
    return halted


def assert_names_the_request(refusal, halted):
    assert refusal["error"] == "custody_unresolved"
    request = halted.custody.requests[-1]
    assert refusal["details"]["request"]["key"] == request.key
    # The Safe emitted the move once; HyperCore wrote no entry for it.
    assert refusal["details"]["evidence"]["actions"].count(request.payload) == 1


def assert_credit_without_move(refusal, amount, safe):
    entries = ledger_entries(refusal)
    assert not any(e["type"] == "accountClassTransfer" for e in entries)
    credits = [Decimal(e["amount"]) for e in credits_to(entries, safe)]
    assert credits == [Decimal(amount) / 10**6]


def ledger_entries(refusal) -> list[dict]:
    return [e["delta"] for e in refusal["details"]["evidence"]["ledger"]["result"]]


def credits_to(entries, safe) -> list[dict]:
    return [e for e in entries if e["type"] == "spotTransfer" and e["destination"] == safe]


def logged_refusal(root) -> dict:
    """The structured error the halted coordinator printed, with its details."""
    for log in sorted(root.glob("process-*.log"), reverse=True):
        for line in reversed(log.read_text().splitlines()):
            if line.startswith('{"error": "custody_unresolved"'):
                return json.loads(line)
    raise AssertionError("no coordinator log names the unresolved custody request")


def test_a_withheld_order_recovers_across_a_restart_and_a_dropped_move_halts(world):
    crashed = crashed_after_the_entry_post(world)
    running, held = recovered_after_restart(world, crashed)
    safe = held.owned[0].target.fund.address
    pending = dropped_beside_a_credit(world, running, safe)
    halted_with_its_evidence(world, running, pending, safe)
    world.report["model"] = model(world)
    path = ROOT / "artifacts/hyperliquid-runtime-faults-fork/report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(world.report, indent=1, default=str) + "\n")
