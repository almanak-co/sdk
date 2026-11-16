"""Endpoint acceptance, late inclusion and complete Aave operation coverage."""

import json
from uuid import uuid4

import pytest

from almanak.runtime import Runtime
from almanak.state import Journal
from almanak.strategy import StrategyDefinition
from tests.fork.replacement_evidence import gas_price_traces
from tests.fork.support import processes
from tests.fork.test_production import STRATEGY, assert_closed, gateway_arguments
from tests.fork.test_transaction_replacement import (
    BOUNDS,
    ROOT,
    assert_winner,
    close,
    drive,
    escalate,
    mine_winner,
    prepare_pending,
    retain,
    setup,
)
from tests.fork.test_transaction_replacement import node as node
from tests.fork.test_uniswap_replacement import run_until
from tests.strategy_fork.scenarios.aave_v4_weth_supply.assertions import entered

pytestmark = pytest.mark.fork


def test_endpoint_accepts_one_wei_increase_but_rejects_equal_price(node):
    node.rpc("anvil_setChainId", [31337])
    signer = node.funded_signer("", 0, 10**18, int(uuid4().hex[:7], 16))
    node.rpc("evm_setAutomine", [False])
    price = int(node.rpc("eth_gasPrice"), 16) * 2
    base = dict(chainId=31337, nonce=0, to=signer.address, gas=21000, gasPrice=price, value=0)
    first = signer.sign_transaction(base)
    original = node.rpc("eth_sendRawTransaction", ["0x" + first.raw_transaction.hex()])
    # A different value distinguishes equal-price replacement from identical-byte rebroadcast.
    equal = signer.sign_transaction({**base, "value": 1})
    with pytest.raises(RuntimeError, match="replacement transaction underpriced"):
        node.rpc("eth_sendRawTransaction", ["0x" + equal.raw_transaction.hex()])
    higher = signer.sign_transaction({**base, "value": 1, "gasPrice": price + 1})
    accepted = node.rpc("eth_sendRawTransaction", ["0x" + higher.raw_transaction.hex()])
    node.rpc("evm_mine")
    assert node.rpc("eth_getTransactionReceipt", [original]) is None
    assert node.rpc("eth_getTransactionReceipt", [accepted])["status"] == "0x1"
    evidence = ROOT / "artifacts/transaction-replacement/endpoint.json"
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text(
        json.dumps(
            {
                "client": node.rpc("web3_clientVersion"),
                "fork": node.rpc("anvil_metadata"),
                "original_price": price,
                "rejected_price": price,
                "accepted_price": price + 1,
                "hash": accepted,
            },
            indent=2,
        )
    )


def stop_escalation(runtime, node, first, condition):
    if condition == "candidate_limit":
        escalate(runtime, node, first)
        drive(runtime, lambda s: s.pending.family.candidates[-1].submission is not None)
        runtime.tick()
    else:
        node.rpc("anvil_setNextBlockBaseFeePerGas", [hex(10**12)])
        node.rpc("evm_mine")
    return drive(runtime, lambda s: s.pending.family.blocked == condition).pending


@pytest.mark.parametrize("condition", ["candidate_limit", "elapsed_limit", "gas_price"])
def test_limits_preserve_late_inclusion(node, tmp_path, condition):
    directory = tmp_path / "deployment"
    bounds = {**BOUNDS, "max_candidates": 2}
    if condition == "elapsed_limit":
        bounds["max_elapsed_seconds"] = 3
    config = setup(node, directory, "eoa_supply", bounds)
    gateway, control = gateway_arguments(node, directory)
    with processes(tmp_path) as launch:
        launch(gateway, ready="gateway_ready")
        runtime = Runtime(
            control[3],
            directory / "gateway-auth.token",
            create=config,
            strategy=StrategyDefinition.read(STRATEGY),
        )
        try:
            first = prepare_pending(runtime, node)
            pending = stop_escalation(runtime, node, first, condition)
            chosen, receipt = mine_winner(runtime, node, pending, 0)
            state = drive(runtime, lambda s: any(o.tx_hash == chosen.tx_hash for o in s.outcomes))
            assert_winner(state, first, receipt)
            close(runtime, node)
            retain(node, Journal(directory / "state.sqlite"), "late-" + condition)
        finally:
            runtime.close()


@pytest.mark.parametrize("profile", ["eoa_borrow", "safe_borrow"])
def test_every_aave_operation_replaced(node, tmp_path, profile):
    directory = tmp_path / "deployment"
    config = setup(node, directory, profile)
    gateway, control = gateway_arguments(node, directory)
    with processes(tmp_path) as launch:
        launch(gateway, ready="gateway_ready")
        runtime = Runtime(
            control[3],
            directory / "gateway-auth.token",
            create=config,
            strategy=StrategyDefinition.read(STRATEGY),
        )
        try:
            runtime.fault = lambda stage, _: (
                node.rpc("evm_setAutomine", [False]) if stage == "prepared" else None
            )
            run_until(runtime, node, entered, 1)
            runtime.journal.teardown()
            state = run_until(runtime, node, lambda s: s.mode == "closed", 0)
            assert_closed(state)
            retain(
                node,
                Journal(directory / "state.sqlite"),
                profile + "-all-operations",
                {"traces": gas_price_traces(node, state)},
            )
        finally:
            runtime.close()
