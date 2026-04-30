"""Tests for LocalSimulator evm_snapshot/evm_revert hard timeout (VIB-3740).

A stalled Anvil RPC socket on the snapshot/revert calls used to hang the entire
simulation pipeline for ~2.5 minutes (the default web3 provider timeout). With
asyncio.wait_for(timeout=10s) the simulator now degrades cleanly: it logs the
timeout, marks the snapshot as unavailable, and continues with gas estimation.

Mirrors the VIB-3295 fix on _estimate_gas, just on a different RPC method.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.interfaces import (
    TransactionType,
    UnsignedTransaction,
)
from almanak.framework.execution.simulator.local import (
    _EVM_SNAPSHOT_TIMEOUT,
    LocalSimulator,
)

APPROVE_SELECTOR = "0x095ea7b3"  # Routes through the approve fast-path.
SWAP_SELECTOR = "0x12345678"


def _make_tx(selector: str, gas_limit: int = 200_000) -> UnsignedTransaction:
    return UnsignedTransaction(
        to="0x" + "c" * 40,
        value=0,
        data=selector + "0" * 56,
        chain_id=42161,
        gas_limit=gas_limit,
        gas_price=1_000_000_000,
        tx_type=TransactionType.LEGACY,
        from_address="0x" + "b" * 40,
    )


class TestEvmSnapshotTimeout:
    """evm_snapshot must be wrapped in asyncio.wait_for so it cannot hang."""

    @pytest.mark.asyncio
    async def test_snapshot_timeout_falls_through_to_no_snapshot(self, monkeypatch):
        """A hanging evm_snapshot does not block the simulation.

        Without the wait_for guard, a stalled provider would block here for the
        full transport-layer timeout (~150s on AsyncHTTPProvider). With the
        guard, the simulator marks the snapshot unavailable and continues —
        gas estimation proceeds and the call returns within the test budget.
        """
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        monkeypatch.setattr(
            "almanak.framework.execution.simulator.local._EVM_SNAPSHOT_TIMEOUT",
            0.1,
        )

        hung = asyncio.Event()

        async def _hang(*_args, **_kwargs):
            hung.set()
            await asyncio.Event().wait()

        mock_provider = MagicMock()
        mock_provider.make_request = AsyncMock(side_effect=_hang)

        mock_web3 = MagicMock()
        mock_web3.provider = mock_provider
        mock_web3.eth.estimate_gas = AsyncMock(return_value=100_000)
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        # Multi-tx bundle triggers the snapshot path. First tx is approve so it
        # uses the compiler gas_limit (no estimate_gas dependency we need to mock
        # for state-setup execution).
        txs = [_make_tx(APPROVE_SELECTOR), _make_tx(SWAP_SELECTOR)]

        result = await asyncio.wait_for(sim.simulate(txs, chain="arbitrum"), timeout=5.0)

        assert hung.is_set()
        # Snapshot timed out → warnings populated → simulation still succeeds.
        assert result.success
        assert any("Snapshot unavailable" in w for w in result.warnings)

    @pytest.mark.asyncio
    async def test_snapshot_timeout_constant_is_bounded(self):
        """Sanity check: the timeout must be bounded and non-zero."""
        assert _EVM_SNAPSHOT_TIMEOUT > 0
        # 30s ceiling matches the eth_estimateGas envelope; snapshot/revert are
        # cheap RPC ops on Anvil and should never need more.
        assert _EVM_SNAPSHOT_TIMEOUT <= 30

    @pytest.mark.asyncio
    async def test_normal_snapshot_still_works(self):
        """Happy path: fast snapshot returns successfully and gates state setup."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        snapshot_calls: list[str] = []

        async def _make_request(method, _params):
            snapshot_calls.append(str(method))
            if str(method) == "evm_snapshot":
                return {"result": "0x1"}
            if str(method) == "evm_revert":
                return {"result": True}
            return {"result": None}

        mock_provider = MagicMock()
        mock_provider.make_request = AsyncMock(side_effect=_make_request)

        mock_web3 = MagicMock()
        mock_web3.provider = mock_provider
        mock_web3.eth.estimate_gas = AsyncMock(return_value=150_000)
        mock_web3.eth.send_transaction = AsyncMock(return_value=b"\x00" * 32)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(
            return_value={"status": 1, "transactionHash": "0x" + "a" * 64}
        )
        mock_web3.eth.get_block = AsyncMock(return_value={"baseFeePerGas": 1_000_000_000})
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        txs = [_make_tx(APPROVE_SELECTOR), _make_tx(SWAP_SELECTOR)]
        result = await sim.simulate(txs, chain="arbitrum")

        assert result.success
        assert "evm_snapshot" in snapshot_calls
        assert "evm_revert" in snapshot_calls
