"""Capture real unsolicited liquidation semantics before enabling runtime attribution."""

import json
import signal
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from eth_abi import decode, encode
from eth_utils import keccak

from almanak.connectors.gmx_v2.deployments import records
from almanak.connectors.gmx_v2.fork_keeper import fees, routed
from almanak.connectors.gmx_v2.oracle import StreamReport
from almanak.connectors.gmx_v2.receipts import one_protocol_event
from almanak.connectors.gmx_v2.report_source import oracle_parameters
from tests import gmx_fork
from tests.fork.gmx_calls import call, receipt_for
from tests.fork.support import processes, wait_state
from tests.fork.test_gmx_liquidity_recovery import reports
from tests.fork.test_gmx_requests import case, configuration
from tests.strategy_fork.harness import (
    bind_workspace,
    install_permissions,
    resolve_config,
    retain,
    start_gateway,
    start_runtime,
)

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("profile", ["eoa", "safe_roles"])
def test_native_liquidation_capture(request, tmp_path, chain, profile):
    selected = case(f"gmx_v2_{chain}_position")
    env = request.config.getoption("--fork-env").resolve()
    config = resolve_config(configuration(selected, profile, env, tmp_path / "resolution"), env, {})
    artifact = (
        ROOT
        / "artifacts/strategy-fork"
        / (selected.name + "-liquidation-capture-" + profile + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": selected.name,
        "profile": profile,
        "status": "failed",
        "scope": "direct native liquidation capture",
        "phases": [],
    }
    try:
        with ExitStack() as stack:
            nodes, node = bind_workspace(config, env, stack, record)
            launch = stack.enter_context(processes(temporary))
            directory = temporary / "deployment"
            deployment = nodes.prepare(config, directory)
            journal, arguments = start_gateway(node, directory, launch, lease_seconds=30)
            if profile == "safe_roles":
                install_permissions(selected, node, directory, deployment, arguments)
            node.rpc("anvil_setBlockTimestampInterval", [1])
            running, _ = start_runtime(
                selected, node, SimpleNamespace(), journal, arguments, launch, record["phases"]
            )
            gmx_fork.drive_external(
                node,
                journal,
                running,
                lambda r: source_after(chain, r.events[0].identity.block.timestamp),
            )
            held = wait_state(journal, gmx_fork.entered, running)
            running.kill()
            assert running.wait(timeout=20) == -signal.SIGKILL
            capture_liquidation(node, held, journal.path.parent, chain)
            record["phases"].append("actual_reference_position_liquidated_with_authentic_oracle")
            record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("GMX liquidation capture evidence:", artifact)


def source_after(chain, timestamp):
    proof, rows = reports(chain)
    adjustments = {row["asset"]: row["timestamp_adjustment"] for row in proof["bindings"]}
    return next(
        row
        for row in rows
        if min(r["observed_at"] - adjustments[r["asset"]] for r in row["reports"]) > timestamp
    )


def view(node, address, contract, method, args, outputs):
    payload = {"to": address, "data": "0x" + call(contract, method, args).hex()}
    return decode(outputs, bytes.fromhex(node.rpc("eth_call", [payload, "latest"])[2:]))


def send(node, sender, recipient, payload):
    node.rpc("anvil_impersonateAccount", [sender])
    node.rpc("anvil_setBalance", [sender, hex(10**20)])
    transaction = {
        "from": sender,
        "to": recipient,
        "data": "0x" + payload.hex(),
        "gas": hex(20_000_000),
        # Anvil's default tip can exceed its default fee cap on a low base fee fork.
        **fees(node.rpc),
    }
    receipt = receipt_for(node.rpc, node.rpc("eth_sendTransaction", [transaction]))
    return transaction, receipt


def role_address(target, name):
    matches = [
        address for address, value in records(target.market).items() if value["contract"] == name
    ]
    assert len(matches) == 1
    return matches[0]


def risk_intervention(node, target):
    controller = target.market.contracts.order_handler
    role_store = role_address(target, "RoleStore")
    role = keccak(encode(("string",), ("CONTROLLER",)))
    authority = view(node, role_store, "RoleStore", "hasRole", [controller, role], ("bool",))[0]
    assert authority
    store = target.market.contracts.data_store
    key = keccak(encode(("string",), ("MIN_COLLATERAL_USD",)))
    before = view(node, store, "DataStore", "getUint", [key], ("uint256",))[0]
    transaction, receipt = send(
        node, controller, store, call("DataStore", "setUint", [key, 100_000 * 10**30])
    )
    after = view(node, store, "DataStore", "getUint", [key], ("uint256",))[0]
    assert receipt["status"] == "0x1" and after == 100_000 * 10**30
    return {
        "fixture_only": True,
        "description": "Existing controller impersonation raises MIN_COLLATERAL_USD "
        "on an identified fork",
        "role_store": role_store,
        "role": "0x" + role.hex(),
        "controller_authorized": authority,
        "key": "0x" + key.hex(),
        "before": before,
        "after": after,
        "transaction": transaction,
        "receipt": receipt,
    }


def liquidation_keeper(node, target):
    role = keccak(encode(("string",), ("LIQUIDATION_KEEPER",)))
    store = role_address(target, "RoleStore")
    members = view(node, store, "RoleStore", "getRoleMembers", [role, 0, 100], ("address[]",))[0]
    assert members
    keeper = members[0]
    assert view(node, store, "RoleStore", "hasRole", [keeper, role], ("bool",))[0]
    return keeper, {"role_store": store, "role": "0x" + role.hex(), "existing_members": members}


def capture_liquidation(node, held, directory, chain):
    target = held.owned[0].target
    intervention = risk_intervention(node, target)
    timestamp = int(node.rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)
    source = source_after(chain, timestamp)
    # Authentic payloads, submitted through the provider the target Oracle is configured for.
    selected = tuple(
        routed(node.rpc, target.market.contracts, StreamReport.model_validate_json(json.dumps(r)))
        for r in source["reports"]
    )
    keeper, authority = liquidation_keeper(node, target)
    node.rpc("evm_setNextBlockTimestamp", [max(r.observed_at for r in selected) + 1])
    payload = call(
        "LiquidationHandler",
        "executeLiquidation",
        [
            target.fund.address,
            target.market.share_asset.reference,
            target.collateral_asset.reference,
            target.side == "long",
            oracle_parameters(selected),
        ],
    )
    transaction, receipt = send(node, keeper, role_address(target, "LiquidationHandler"), payload)
    trace = node.rpc(
        "debug_traceTransaction", [receipt["transactionHash"], {"tracer": "callTracer"}]
    )
    result = {
        "intervention": intervention,
        "keeper_authority": authority,
        "source": source,
        "transaction": transaction,
        "receipt": receipt,
        "trace": trace,
    }
    (directory / "external-liquidation.json").write_text(json.dumps(result, indent=2) + "\n")
    assert_full_liquidation(receipt, target)


def assert_full_liquidation(receipt, target):
    assert receipt["status"] == "0x1"
    event = one_protocol_event(
        receipt, target, "PositionDecrease", target.market.contracts.decrease_order_executor
    )
    assert event.value("uint256", "orderType") == 7
    assert event.value("uint256", "sizeInUsd") == 0
