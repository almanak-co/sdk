"""Actual Safe absorption and credit withdrawal under one generated permission install."""

import json
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from eth_abi import encode

from almanak.connectors.compound_v3.lifecycle import validate_market_ownership
from almanak.connectors.compound_v3.reader import read_account, read_market
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.safe_fixture import calldata
from almanak.models import SafeRoles
from tests.fork.support import CLI, processes, start_gateway, wait_state
from tests.strategy_fork.compound_assertions import assert_holding, entered
from tests.strategy_fork.harness import (
    Case,
    bind_workspace,
    case_fork_config,
    install_permissions,
    require_safe_accounts,
    resolve_config,
    retain,
    stop_runtime,
)
from tests.strategy_fork.pool_lending_authority import wrapped

ROOT = Path(__file__).parents[2]
pytestmark = pytest.mark.fork


def transaction(node, call):
    node._validate_origin()
    node.rpc("anvil_impersonateAccount", [call["from"]])
    try:
        gas = int(node.rpc("eth_estimateGas", [call]), 16) + 100_000
        assert gas < 2_000_000
        receipt = node.rpc.wait_receipt(
            node.rpc("eth_sendTransaction", [{**call, "gas": hex(gas)}])
        )
        assert receipt["status"] == "0x1"
        return receipt
    finally:
        node.rpc("anvil_stopImpersonatingAccount", [call["from"]])


def assert_credit(state, credit):
    assert credit.principal > 0 and credit.borrowed_base == 0
    assert all(c.balance == 0 for c in credit.collaterals)
    # An on-chain permission proof cannot adopt an unrecorded external liquidation.
    with pytest.raises(DataError, match="not owned"):
        validate_market_ownership(state, SimpleNamespace(account=credit))


def assert_credit_withdrawn(reader, profile, final, before_cash, after_cash):
    assert final.principal == 0 and after_cash > before_cash
    assert read_account(reader, final.market, profile.operator).principal == 0


def credit_exit(node, state):
    target = state.config.target("loan")
    profile = state.config.accounts.binding(target.account).profile
    assert isinstance(profile, SafeRoles)
    source = target.reserve.market
    with EthereumReader(source.chain, node.rpc.endpoint, local_transport=True) as reader:
        block = reader.pin_block("latest")
        market = read_market(reader, source, block)
        before = read_account(reader, market, target.fund)
        assert before.principal < 0
        collateral = next(
            c for c in market.collaterals if c.asset_type == state.config.target("collateral").asset
        )
        response = encode(
            ("uint80", "int256", "uint256", "uint256", "uint80"),
            (1, 220 * 10**8, block.timestamp, block.timestamp, 1),
        )
        node._validate_origin()
        node.rpc(
            "anvil_setCode",
            [collateral.price_feed, "0x6100a0600e6000396100a06000f3" + response.hex()],
        )
        absorbed = transaction(
            node,
            {
                "from": profile.operator.address,
                "to": source.comet,
                "data": calldata(
                    "absorb(address,address[])",
                    ("address", "address[]"),
                    (profile.operator.address, (target.fund.address,)),
                ),
            },
        )
        block = reader.pin_block("latest")
        market = read_market(reader, source, block)
        credit = read_account(reader, market, target.fund)
        assert_credit(state, credit)
        before_cash = reader.call(
            target.asset.reference,
            "balanceOf(address)",
            block,
            inputs=("address",),
            args=(target.fund.address,),
        )[0]
        call = wrapped(
            profile,
            source.comet,
            calldata(
                "withdraw(address,uint256)",
                ("address", "uint256"),
                (target.asset.reference, 2**256 - 1),
            ),
        )
        node.rpc("eth_call", [call, "latest"])
        receipt = transaction(node, call)
        block = reader.pin_block("latest")
        final = read_account(reader, read_market(reader, source, block), target.fund)
        after_cash = reader.call(
            target.asset.reference,
            "balanceOf(address)",
            block,
            inputs=("address",),
            args=(target.fund.address,),
        )[0]
        assert_credit_withdrawn(reader, profile, final, before_cash, after_cash)
        return {
            "before": before.model_dump(mode="json"),
            "credit": credit.model_dump(mode="json"),
            "final": final.model_dump(mode="json"),
            "absorb_receipt": absorbed,
            "withdraw_call": call,
            "withdraw_receipt": receipt,
            "credit_received": str(after_cash - before_cash),
            "fixture_price": str(220 * 10**8),
        }


@pytest.mark.parametrize("network", ["ethereum", "base"])
def test_generated_safe_authority_withdraws_actual_absorption_credit(network, request, tmp_path):
    name = "compound_v3_" + network + "_lending"
    reference = ROOT / "strategies" / name
    case = Case(
        name,
        reference / "strategy.py",
        reference / "config.yaml",
        ROOT / "tests/strategy_fork/scenarios" / name / "assertions.py",
    )
    env_file = request.config.getoption("--fork-env").resolve()
    config = resolve_config(case_fork_config(case, env_file, tmp_path / "resolution"), env_file, {})
    artifact = ROOT / "artifacts/compound-credit-authority" / (network + "-" + uuid4().hex)
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": name,
        "status": "failed",
        "phases": [],
        "scope": "Identified fork oracle stress and on-chain Roles credit exit. "
        "Native ownership refuses the external absorption; this is not automatic "
        "native liquidation recovery.",
    }
    try:
        with ExitStack() as stack:
            nodes, node = bind_workspace(config, env_file, stack, record)
            launch = stack.enter_context(processes(temporary))
            directory = temporary / "deployment"
            deployment = nodes.prepare(config, directory)
            require_safe_accounts(deployment)
            journal, arguments = start_gateway(node, directory, launch, lease_seconds=30)
            install_permissions(case, nodes, directory, deployment, arguments)
            record["phases"].append("generated_permissions_installed_once")
            running = launch(
                [CLI, "service", "start", "--strategy", str(case.strategy), *arguments]
            )
            state = wait_state(journal, entered, running)
            assert_holding(node, state)
            stop_runtime(running)
            proof = credit_exit(node, state)
            (artifact / "credit-proof.json").write_text(json.dumps(proof, indent=2) + "\n")
            record["phases"].extend(
                (
                    "actual_absorption_created_safe_credit",
                    "unrecorded_native_ownership_refused",
                    "operator_withdrew_credit_through_existing_roles_permissions",
                )
            )
            record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
