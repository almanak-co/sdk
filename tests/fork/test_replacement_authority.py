"""Revoked Safe submission remains reconcilable, including a previously relayed revert."""

import pytest

from almanak.gateway.ethereum import DataError
from almanak.gateway.safe_fixture import calldata, owner_transaction
from almanak.runtime import Runtime
from almanak.state import Journal
from almanak.strategy import StrategyDefinition
from tests.fork.support import processes
from tests.fork.test_production import STRATEGY, gateway_arguments
from tests.fork.test_transaction_replacement import (
    assert_winner,
    drive,
    escalate,
    prepare_pending,
    retain,
    setup,
)
from tests.fork.test_transaction_replacement import node as node

pytestmark = pytest.mark.fork


def role(node, config, pending, enabled):
    binding = config.accounts.binding(pending.operation.account)
    profile = binding.profile
    node.rpc("anvil_setChainId", [31337])
    node.rpc("anvil_setNextBlockBaseFeePerGas", ["0x1"])
    node.rpc("evm_setAutomine", [True])
    receipt = owner_transaction(
        node.rpc,
        node.fixture_owners[binding.name],
        profile.fund.address,
        profile.roles_module,
        calldata(
            "assignRoles(address,bytes32[],bool[])",
            ("address", "bytes32[]", "bool[]"),
            (profile.operator.address, [bytes.fromhex(profile.role_key[2:])], [enabled]),
        ),
    )
    node.rpc("anvil_setChainId", [1])
    node.rpc("evm_setAutomine", [False])
    return receipt


def revoke_pending(runtime, node, config, pending):
    state = drive(runtime, lambda s: s.pending.family.candidates[-1].submission is not None)
    runtime.tick()
    candidate = state.pending.family.candidates[-1]
    node.rpc("anvil_dropTransaction", [candidate.tx_hash])
    revocation = role(node, config, pending, False)
    state = drive(runtime, lambda s: s.pending.family.blocked == "authority")
    assert len(state.pending.family.candidates) == 2
    assert runtime.gateway.receipt(state.pending) is None
    return candidate, revocation


def relay_revert(runtime, node, candidate, first):
    # Model a peer retaining bytes that the authorized gateway submitted before revocation.
    node.rpc("eth_sendRawTransaction", [candidate.raw_transaction])
    node.rpc("evm_mine")
    receipt = node.rpc("eth_getTransactionReceipt", [candidate.tx_hash])
    assert receipt["status"] == "0x0"
    state = drive(runtime, lambda s: any(o.tx_hash == candidate.tx_hash for o in s.outcomes))
    assert_winner(state, first, receipt)
    assert not state.outcomes[-1].success
    assert state.pending is None
    return state


def refuse_unauthorized_cleanup(runtime):
    with pytest.raises(RuntimeError, match="transaction reverted"):
        runtime.tick()
    runtime.fault = lambda *args: None
    runtime.journal.teardown()
    with pytest.raises(DataError) as refused:
        drive(runtime, lambda s: s.mode == "closed")
    assert refused.value.kind == "authority"
    retained = runtime.journal.load()
    assert retained.mode == "teardown" and retained.pending is None
    assert retained.delivered == len(retained.outcomes)


def test_revoked_family_recovers_revert_and_refuses_unauthorized_cleanup(node, tmp_path):
    directory = tmp_path / "deployment"
    config = setup(node, directory, "safe_borrow")
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
            pending = escalate(runtime, node, first)
            candidate, revocation = revoke_pending(runtime, node, config, pending)
            relay_revert(runtime, node, candidate, first)
            refuse_unauthorized_cleanup(runtime)
            retain(
                node,
                Journal(directory / "state.sqlite"),
                "safe-revoked-revert",
                {"revocation": revocation},
            )
        finally:
            runtime.close()
