"""A partial receipt outage cannot release a consumed family nonce or reservation."""

import pytest

from almanak.runtime import Runtime
from almanak.state import Journal
from almanak.strategy import StrategyDefinition
from tests.fork.support import processes
from tests.fork.test_production import STRATEGY, gateway_arguments
from tests.fork.test_transaction_replacement import (
    complete_aave,
    drive,
    escalate,
    mine_winner,
    prepare_pending,
    retain,
    setup,
)
from tests.fork.test_transaction_replacement import node as node

pytestmark = pytest.mark.fork


def hide_and_include(runtime, node, directory, pending):
    gate = directory / "hold-supply-receipt"
    gate.touch()
    chosen, receipt = mine_winner(runtime, node, pending, 1)
    blocked = drive(runtime, lambda s: s.pending.family.blocked == "nonce_conflict")
    assert len(blocked.pending.family.candidates) == 2
    assert blocked.pending.family.maximum_price == pending.family.maximum_price
    assert (
        blocked.pending.family.candidates[0].submission == pending.family.candidates[0].submission
    )
    gate.unlink()
    return chosen, receipt


def test_missing_receipt_visibility_retains_family_until_recovery(node, tmp_path):
    directory = tmp_path / "deployment"
    config = setup(node, directory, "eoa_supply")
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
            chosen, receipt = hide_and_include(runtime, node, directory, pending)
            complete_aave(runtime, node, first, chosen, receipt)
            retain(node, Journal(directory / "state.sqlite"), "receipt-visibility-recovered")
        finally:
            runtime.close()
