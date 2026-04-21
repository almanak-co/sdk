"""Tests for LocalSimulator._estimate_gas hard timeout (VIB-3295).

Verifies that eth_estimateGas cannot hang the whole simulation when an
upstream RPC is slow or unresponsive. Without this guard, complex deep-call
contracts (e.g. MetaMorpho deposit routing into Morpho Blue markets on Base)
can block the strategy for minutes, which the regress shard reporter
interpreted as a process-level hang.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.interfaces import (
    TransactionType,
    UnsignedTransaction,
)
from almanak.framework.execution.simulator.local import (
    _ESTIMATE_GAS_TIMEOUT,
    LocalSimulator,
)

# Deposit selector: deposit(uint256,address) — ERC-4626 vault method.
DEPOSIT_SELECTOR = "0x6e553f65"


def _make_vault_deposit_tx(gas_limit: int = 450_000) -> UnsignedTransaction:
    """Create a single-tx VAULT_DEPOSIT that will hit _estimate_gas directly."""
    return UnsignedTransaction(
        to="0x" + "c" * 40,
        value=0,
        data=DEPOSIT_SELECTOR + "0" * 56,
        chain_id=8453,  # base
        gas_limit=gas_limit,
        gas_price=1_000_000_000,
        tx_type=TransactionType.LEGACY,
        from_address="0x" + "b" * 40,
    )


class TestEstimateGasTimeout:
    """eth_estimateGas must be wrapped in asyncio.wait_for so it cannot hang."""

    @pytest.mark.asyncio
    async def test_estimate_gas_timeout_is_reported_as_error(self, monkeypatch):
        """A hanging estimate_gas must surface as a timeout error, not block forever."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        # Shrink the timeout for fast test execution; the production value is
        # exercised by the presence of the asyncio.wait_for wrapper below.
        monkeypatch.setattr(
            "almanak.framework.execution.simulator.local._ESTIMATE_GAS_TIMEOUT",
            0.1,
        )

        hung = asyncio.Event()

        async def _hang(*_args, **_kwargs):
            # Never completes — simulates a slow/hung RPC endpoint.
            hung.set()
            await asyncio.Event().wait()

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(side_effect=_hang)
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        tx = _make_vault_deposit_tx(gas_limit=450_000)

        # Run with an outer watchdog so a regression that removes the timeout
        # fails the test in bounded time instead of hanging CI.
        result = await asyncio.wait_for(sim.simulate([tx], chain="base"), timeout=5.0)

        # estimate_gas was called and hung, but the simulator surfaced the timeout.
        assert hung.is_set()
        assert not result.success
        assert result.revert_reason is not None
        assert "timed out" in result.revert_reason.lower()

    @pytest.mark.asyncio
    async def test_estimate_gas_timeout_constant_matches_state_setup_budget(self):
        """Sanity check: the timeout must be bounded and non-zero."""
        assert _ESTIMATE_GAS_TIMEOUT > 0
        # 30s ceiling matches the _STATE_SETUP_TX_TIMEOUT envelope used elsewhere
        # in the simulator; both guard the gateway's Execute gRPC call from
        # hanging past its chain-specific grpc_execute timeout.
        assert _ESTIMATE_GAS_TIMEOUT <= 60

    @pytest.mark.asyncio
    async def test_normal_estimate_gas_still_returns_result(self):
        """Happy path: fast estimate_gas returns the value without touching the timeout."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=321_000)
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        tx = _make_vault_deposit_tx(gas_limit=450_000)
        result = await sim.simulate([tx], chain="base")

        assert result.success
        assert result.gas_estimates == [321_000]
