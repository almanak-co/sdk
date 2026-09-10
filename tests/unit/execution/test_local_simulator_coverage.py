"""A compiler gas allowance is not evidence that an EVM call succeeded."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.simulator.local import LocalSimulator
from tests.unit.execution.test_local_simulator_approve import _make_approve_tx, _make_tx


@pytest.mark.asyncio
async def test_remote_approval_bundle_cannot_claim_the_skipped_swap_was_simulated():
    simulator = LocalSimulator(rpc_url="https://rpc.example.invalid")
    web3 = MagicMock()
    web3.eth.estimate_gas = AsyncMock(return_value=150_000)
    web3.eth.send_transaction = AsyncMock()
    simulator._web3 = web3
    transactions = [_make_approve_tx(), _make_tx(data="0x87517c45"), _make_tx(data="0x3593564c")]

    result = await simulator.simulate(transactions, chain="robinhood")

    web3.eth.estimate_gas.assert_not_called()
    web3.eth.send_transaction.assert_not_called()
    assert result.simulated is False
    assert result.success is False
    assert "dependent" in result.revert_reason.lower()
