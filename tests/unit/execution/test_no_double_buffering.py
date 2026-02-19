"""Tests to verify that gas estimation buffer is applied exactly once.

The orchestrator._update_gas_estimate() is the SINGLE point of buffer
application. Simulators (Tenderly, Alchemy) must return raw gas_used values
without any buffer.

Regression test for the double-buffering bug where:
- Simulator applied ~1.5x buffer (simulation buffer)
- Orchestrator applied ~1.5x buffer (gas buffer multiplier)
- Result: 2.25x inflation instead of 1.5x
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.interfaces import (
    SimulationResult,
    TransactionType,
    UnsignedTransaction,
)
from almanak.framework.execution.simulator.alchemy import AlchemySimulator
from almanak.framework.execution.simulator.tenderly import TenderlySimulator


# =============================================================================
# Tenderly: returns raw gas_used
# =============================================================================


class TestTenderlyReturnsRawGas:
    """Verify TenderlySimulator returns raw gas_used without buffer."""

    def _make_tenderly_response(self, gas_used: int) -> dict:
        """Build a mock Tenderly API response."""
        return {
            "simulation_results": [
                {
                    "simulation": {"status": True, "id": "sim-123"},
                    "transaction": {"gas_used": gas_used},
                }
            ]
        }

    def test_parse_response_returns_raw_gas(self):
        """Gas estimates from Tenderly should be raw gas_used, not buffered."""
        sim = TenderlySimulator(
            account_slug="test",
            project_slug="test",
            access_key="test-key",
        )

        raw_gas = 200_000
        response = self._make_tenderly_response(raw_gas)

        result = sim._parse_response(response, chain="arbitrum", expected_count=1)

        assert result.success is True
        assert result.simulated is True
        assert len(result.gas_estimates) == 1
        # Must be exactly the raw gas_used, NOT buffered
        assert result.gas_estimates[0] == raw_gas

    def test_parse_response_multiple_txs_raw_gas(self):
        """Multiple transactions should all return raw gas_used."""
        sim = TenderlySimulator(
            account_slug="test",
            project_slug="test",
            access_key="test-key",
        )

        response = {
            "simulation_results": [
                {
                    "simulation": {"status": True, "id": "sim-1"},
                    "transaction": {"gas_used": 100_000},
                },
                {
                    "simulation": {"status": True, "id": "sim-2"},
                    "transaction": {"gas_used": 300_000},
                },
            ]
        }

        result = sim._parse_response(response, chain="arbitrum", expected_count=2)

        assert result.success is True
        assert result.gas_estimates == [100_000, 300_000]

    def test_parse_response_hex_gas_used(self):
        """Hex-encoded gas_used should be parsed and returned raw."""
        sim = TenderlySimulator(
            account_slug="test",
            project_slug="test",
            access_key="test-key",
        )

        response = {
            "simulation_results": [
                {
                    "simulation": {"status": True, "id": "sim-1"},
                    "transaction": {"gas_used": "0x30D40"},  # 200,000 in hex
                },
            ]
        }

        result = sim._parse_response(response, chain="ethereum", expected_count=1)

        assert result.success is True
        assert result.gas_estimates[0] == 200_000


# =============================================================================
# Alchemy: returns raw gas_used
# =============================================================================


class TestAlchemyReturnsRawGas:
    """Verify AlchemySimulator returns raw gas_used without buffer."""

    def _make_alchemy_response(self, gas_used_hex: str) -> dict:
        """Build a mock Alchemy RPC response."""
        return {
            "jsonrpc": "2.0",
            "id": 1,
            "result": [
                {
                    "calls": [
                        {
                            "gasUsed": gas_used_hex,
                            "status": "0x1",
                        }
                    ]
                }
            ],
        }

    def test_parse_response_returns_raw_gas(self):
        """Gas estimates from Alchemy should be raw gas_used, not buffered."""
        sim = AlchemySimulator(api_key="test-key")

        raw_gas = 200_000
        gas_hex = hex(raw_gas)
        response = self._make_alchemy_response(gas_hex)

        result = sim._parse_response(response, chain="arbitrum", expected_count=1)

        assert result.success is True
        assert result.simulated is True
        assert len(result.gas_estimates) == 1
        # Must be exactly the raw gas_used, NOT buffered
        assert result.gas_estimates[0] == raw_gas

    def test_parse_response_multiple_txs_raw_gas(self):
        """Multiple transactions should all return raw gas_used."""
        sim = AlchemySimulator(api_key="test-key")

        response = {
            "jsonrpc": "2.0",
            "id": 1,
            "result": [
                {"calls": [{"gasUsed": hex(100_000), "status": "0x1"}]},
                {"calls": [{"gasUsed": hex(300_000), "status": "0x1"}]},
            ],
        }

        result = sim._parse_response(response, chain="base", expected_count=2)

        assert result.success is True
        assert result.gas_estimates == [100_000, 300_000]


# =============================================================================
# Orchestrator: buffer applied exactly once
# =============================================================================


class TestOrchestratorSingleBufferApplication:
    """Verify orchestrator applies gas buffer exactly once to raw simulator output."""

    def _make_orchestrator(self, chain: str = "arbitrum"):
        """Create an ExecutionOrchestrator with mocked dependencies."""
        from almanak.framework.execution.orchestrator import ExecutionOrchestrator

        signer = MagicMock()
        signer.address = "0x1234567890abcdef1234567890abcdef12345678"
        submitter = MagicMock()
        simulator = MagicMock()

        return ExecutionOrchestrator(
            signer=signer,
            submitter=submitter,
            simulator=simulator,
            chain=chain,
        )

    def _make_tx(self, chain_id: int = 42161, gas_limit: int = 100_000) -> UnsignedTransaction:
        """Create a test transaction (legacy type to skip EIP-1559 validation)."""
        return UnsignedTransaction(
            to="0xdeadbeef",
            value=0,
            data="0x",
            chain_id=chain_id,
            gas_limit=gas_limit,
            tx_type=TransactionType.LEGACY,
            gas_price=1_000_000_000,
        )

    def test_update_gas_estimate_applies_buffer_once(self):
        """_update_gas_estimate should apply gas_buffer_multiplier exactly once."""
        orchestrator = self._make_orchestrator(chain="arbitrum")
        # Arbitrum gas buffer = 1.5 (from CHAIN_GAS_BUFFERS)
        assert orchestrator.gas_buffer_multiplier == 1.5

        raw_gas = 200_000
        tx = self._make_tx(chain_id=42161)

        updated_tx = orchestrator._update_gas_estimate(tx, raw_gas)

        # Buffer applied once: 200_000 * 1.5 = 300_000
        expected = int(raw_gas * 1.5)
        assert updated_tx.gas_limit == expected

    def test_no_double_buffering_l2(self):
        """End-to-end: raw simulator gas on L2 should only get 1.5x, not 2.25x."""
        orchestrator = self._make_orchestrator(chain="arbitrum")

        raw_gas = 200_000
        tx = self._make_tx(chain_id=42161)

        updated_tx = orchestrator._update_gas_estimate(tx, raw_gas)

        # Should be 300,000 (1.5x), NOT 450,000 (2.25x from double-buffering)
        assert updated_tx.gas_limit == 300_000
        assert updated_tx.gas_limit != 450_000  # Explicit check against old bug

    def test_no_double_buffering_base(self):
        """Base chain (L2) should also get single 1.5x buffer."""
        orchestrator = self._make_orchestrator(chain="base")
        assert orchestrator.gas_buffer_multiplier == 1.5

        raw_gas = 150_000
        tx = self._make_tx(chain_id=8453)

        updated_tx = orchestrator._update_gas_estimate(tx, raw_gas)

        expected = int(150_000 * 1.5)
        assert updated_tx.gas_limit == expected

    def test_no_double_buffering_ethereum(self):
        """Ethereum mainnet should get single 1.1x buffer."""
        orchestrator = self._make_orchestrator(chain="ethereum")
        assert orchestrator.gas_buffer_multiplier == 1.1

        raw_gas = 200_000
        tx = self._make_tx(chain_id=1)

        updated_tx = orchestrator._update_gas_estimate(tx, raw_gas)

        expected = int(200_000 * 1.1)
        assert updated_tx.gas_limit == expected
