"""Tests for the simulator fallback cascade.

Verifies that create_simulator() wires the cascade correctly:
    Tenderly -> Alchemy -> LocalSimulator -> DirectSimulator

And that FallbackSimulator tries each simulator in order on recoverable errors,
but stops immediately on transaction reverts.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.interfaces import (
    SimulationError,
    SimulationResult,
    Simulator,
)
from almanak.framework.execution.simulator import (
    create_simulator,
    SimulationConfig,
)
from almanak.framework.execution.simulator.direct import DirectSimulator
from almanak.framework.execution.simulator.fallback import FallbackSimulator
from almanak.framework.execution.simulator.local import LocalSimulator
from almanak.framework.execution.simulator.alchemy import AlchemySimulator
from almanak.framework.execution.simulator.tenderly import TenderlySimulator


# =============================================================================
# create_simulator() factory tests
# =============================================================================


class TestCreateSimulatorCascade:
    """Verify create_simulator() wires LocalSimulator into the cascade."""

    def test_local_rpc_returns_local_simulator(self):
        """Local RPC (Anvil) should return LocalSimulator, not DirectSimulator."""
        sim = create_simulator(rpc_url="http://localhost:8545")
        assert isinstance(sim, LocalSimulator)
        assert sim.name == "local_fork"

    def test_local_rpc_127(self):
        """127.0.0.1 should also return LocalSimulator."""
        sim = create_simulator(rpc_url="http://127.0.0.1:8545")
        assert isinstance(sim, LocalSimulator)

    def test_disabled_returns_direct(self):
        """Disabled simulation should return DirectSimulator."""
        config = SimulationConfig.disabled()
        sim = create_simulator(config=config)
        assert isinstance(sim, DirectSimulator)

    def test_no_credentials_no_rpc_returns_direct(self):
        """No credentials and no RPC URL should return DirectSimulator."""
        config = SimulationConfig(enabled=True)
        sim = create_simulator(config=config, rpc_url=None)
        assert isinstance(sim, DirectSimulator)

    def test_no_credentials_with_rpc_returns_local(self):
        """No credentials but with RPC URL should return LocalSimulator."""
        config = SimulationConfig(enabled=True)
        sim = create_simulator(config=config, rpc_url="https://arb-mainnet.g.alchemy.com/v2/key")
        assert isinstance(sim, LocalSimulator)

    def test_tenderly_only_with_rpc_returns_fallback_with_local(self):
        """Tenderly + RPC URL should create cascade with LocalSimulator fallback."""
        config = SimulationConfig(
            enabled=True,
            tenderly_account="test",
            tenderly_project="test",
            tenderly_access_key="key",
        )
        sim = create_simulator(config=config, rpc_url="https://arb-mainnet.g.alchemy.com/v2/key")

        assert isinstance(sim, FallbackSimulator)
        # Primary should be Tenderly, fallback should include LocalSimulator
        assert isinstance(sim._primary, TenderlySimulator)
        assert len(sim._fallbacks) == 1
        assert isinstance(sim._fallbacks[0], LocalSimulator)

    def test_tenderly_only_no_rpc_returns_tenderly_alone(self):
        """Tenderly without RPC URL should return TenderlySimulator alone (no fallback)."""
        config = SimulationConfig(
            enabled=True,
            tenderly_account="test",
            tenderly_project="test",
            tenderly_access_key="key",
        )
        sim = create_simulator(config=config, rpc_url=None)

        assert isinstance(sim, TenderlySimulator)

    def test_tenderly_alchemy_rpc_returns_full_cascade(self):
        """Tenderly + Alchemy + RPC URL should create full cascade."""
        config = SimulationConfig(
            enabled=True,
            tenderly_account="test",
            tenderly_project="test",
            tenderly_access_key="key",
            alchemy_api_key="alchemy-key",
        )
        sim = create_simulator(config=config, rpc_url="https://arb-mainnet.g.alchemy.com/v2/key")

        assert isinstance(sim, FallbackSimulator)
        assert isinstance(sim._primary, TenderlySimulator)
        assert len(sim._fallbacks) == 2
        assert isinstance(sim._fallbacks[0], AlchemySimulator)
        assert isinstance(sim._fallbacks[1], LocalSimulator)

    def test_tenderly_alchemy_no_rpc_returns_two_sim_cascade(self):
        """Tenderly + Alchemy without RPC should create cascade without LocalSimulator."""
        config = SimulationConfig(
            enabled=True,
            tenderly_account="test",
            tenderly_project="test",
            tenderly_access_key="key",
            alchemy_api_key="alchemy-key",
        )
        sim = create_simulator(config=config, rpc_url=None)

        assert isinstance(sim, FallbackSimulator)
        assert isinstance(sim._primary, TenderlySimulator)
        assert len(sim._fallbacks) == 1
        assert isinstance(sim._fallbacks[0], AlchemySimulator)


# =============================================================================
# FallbackSimulator cascade behavior tests
# =============================================================================


class TestFallbackSimulatorCascade:
    """Verify FallbackSimulator cascades through simulators on recoverable errors."""

    def _make_mock_simulator(self, name: str = "mock") -> MagicMock:
        """Create a mock simulator that supports all chains."""
        sim = MagicMock(spec=Simulator)
        sim.name = name
        sim.supports_chain.return_value = True
        return sim

    @pytest.mark.asyncio
    async def test_cascade_tenderly_timeout_alchemy_timeout_local_succeeds(self):
        """When Tenderly and Alchemy both timeout, LocalSimulator should be tried."""
        tenderly = self._make_mock_simulator("tenderly")
        alchemy = self._make_mock_simulator("alchemy")
        local = self._make_mock_simulator("local")

        tenderly.simulate = AsyncMock(
            side_effect=SimulationError("Tenderly timeout", recoverable=True)
        )
        alchemy.simulate = AsyncMock(
            side_effect=SimulationError("Alchemy timeout", recoverable=True)
        )
        local.simulate = AsyncMock(
            return_value=SimulationResult(
                success=True, simulated=True, gas_estimates=[200_000]
            )
        )

        fallback = FallbackSimulator(
            primary=tenderly,
            fallbacks=[alchemy, local],
        )

        result = await fallback.simulate([], chain="arbitrum")
        # Empty txs returns early
        assert result.success is True

        # Now test with actual txs
        mock_tx = MagicMock()
        result = await fallback.simulate([mock_tx], chain="arbitrum")

        assert result.success is True
        assert result.gas_estimates == [200_000]

        # Verify cascade order
        tenderly.simulate.assert_called_once()
        alchemy.simulate.assert_called_once()
        local.simulate.assert_called_once()

    @pytest.mark.asyncio
    async def test_revert_stops_cascade(self):
        """Transaction revert should NOT trigger fallback."""
        tenderly = self._make_mock_simulator("tenderly")
        alchemy = self._make_mock_simulator("alchemy")

        tenderly.simulate = AsyncMock(
            return_value=SimulationResult(
                success=False,
                simulated=True,
                revert_reason="execution reverted: insufficient balance",
            )
        )
        alchemy.simulate = AsyncMock()

        fallback = FallbackSimulator(
            primary=tenderly,
            fallbacks=[alchemy],
        )

        mock_tx = MagicMock()
        result = await fallback.simulate([mock_tx], chain="arbitrum")

        assert result.success is False
        assert result.revert_reason == "execution reverted: insufficient balance"
        # Alchemy should NOT have been called
        alchemy.simulate.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_recoverable_error_stops_cascade(self):
        """Non-recoverable SimulationError should NOT trigger fallback."""
        tenderly = self._make_mock_simulator("tenderly")
        local = self._make_mock_simulator("local")

        tenderly.simulate = AsyncMock(
            side_effect=SimulationError("Invalid chain config", recoverable=False)
        )
        local.simulate = AsyncMock()

        fallback = FallbackSimulator(
            primary=tenderly,
            fallbacks=[local],
        )

        mock_tx = MagicMock()
        with pytest.raises(SimulationError) as exc_info:
            await fallback.simulate([mock_tx], chain="arbitrum")

        assert "Invalid chain config" in str(exc_info.value)
        local.simulate.assert_not_called()

    @pytest.mark.asyncio
    async def test_all_fail_raises_simulation_error(self):
        """When all simulators fail, should raise SimulationError."""
        tenderly = self._make_mock_simulator("tenderly")
        alchemy = self._make_mock_simulator("alchemy")
        local = self._make_mock_simulator("local")

        tenderly.simulate = AsyncMock(
            side_effect=SimulationError("Tenderly 503", recoverable=True)
        )
        alchemy.simulate = AsyncMock(
            side_effect=SimulationError("Alchemy rate limit", recoverable=True)
        )
        local.simulate = AsyncMock(
            side_effect=SimulationError("RPC connection refused", recoverable=True)
        )

        fallback = FallbackSimulator(
            primary=tenderly,
            fallbacks=[alchemy, local],
        )

        mock_tx = MagicMock()
        with pytest.raises(SimulationError) as exc_info:
            await fallback.simulate([mock_tx], chain="arbitrum")

        assert "All simulators failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_primary_succeeds_no_fallback(self):
        """When primary succeeds, fallbacks should not be tried."""
        tenderly = self._make_mock_simulator("tenderly")
        alchemy = self._make_mock_simulator("alchemy")

        tenderly.simulate = AsyncMock(
            return_value=SimulationResult(
                success=True, simulated=True, gas_estimates=[150_000]
            )
        )
        alchemy.simulate = AsyncMock()

        fallback = FallbackSimulator(
            primary=tenderly,
            fallbacks=[alchemy],
        )

        mock_tx = MagicMock()
        result = await fallback.simulate([mock_tx], chain="arbitrum")

        assert result.success is True
        assert result.gas_estimates == [150_000]
        alchemy.simulate.assert_not_called()

    @pytest.mark.asyncio
    async def test_chain_not_supported_by_any(self):
        """When no simulator supports the chain, should raise SimulationError."""
        tenderly = self._make_mock_simulator("tenderly")
        tenderly.supports_chain.return_value = False

        local = self._make_mock_simulator("local")
        local.supports_chain.return_value = False

        fallback = FallbackSimulator(
            primary=tenderly,
            fallbacks=[local],
        )

        mock_tx = MagicMock()
        with pytest.raises(SimulationError) as exc_info:
            await fallback.simulate([mock_tx], chain="unsupported")

        assert "No simulator supports chain" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_skip_unsupported_chain_simulator(self):
        """Simulators that don't support the chain should be skipped."""
        tenderly = self._make_mock_simulator("tenderly")
        tenderly.supports_chain.return_value = False

        local = self._make_mock_simulator("local")
        local.supports_chain.return_value = True
        local.simulate = AsyncMock(
            return_value=SimulationResult(
                success=True, simulated=True, gas_estimates=[100_000]
            )
        )

        fallback = FallbackSimulator(
            primary=tenderly,
            fallbacks=[local],
        )

        mock_tx = MagicMock()
        result = await fallback.simulate([mock_tx], chain="sonic")

        assert result.success is True
        tenderly.simulate.assert_not_called()  # Skipped (doesn't support chain)
        local.simulate.assert_called_once()

    def test_backward_compat_secondary_param(self):
        """The 'secondary' parameter should still work for backward compatibility."""
        primary = self._make_mock_simulator("primary")
        secondary = self._make_mock_simulator("secondary")

        fallback = FallbackSimulator(primary=primary, secondary=secondary)

        assert len(fallback._fallbacks) == 1
        assert fallback._fallbacks[0] is secondary

    def test_secondary_and_fallbacks_combined(self):
        """When both secondary and fallbacks are provided, secondary comes first."""
        primary = self._make_mock_simulator("primary")
        secondary = self._make_mock_simulator("secondary")
        extra = self._make_mock_simulator("extra")

        fallback = FallbackSimulator(
            primary=primary,
            secondary=secondary,
            fallbacks=[extra],
        )

        assert len(fallback._fallbacks) == 2
        assert fallback._fallbacks[0] is secondary
        assert fallback._fallbacks[1] is extra


# =============================================================================
# Simulator payload tests — gas field omission (PR #817)
# =============================================================================


class TestSimulatorPayloadNoGasCap:
    """Verify Tenderly/Alchemy simulators do NOT send gas_limit in their payloads.

    Simulators should let the API estimate gas freely rather than capping at
    our hardcoded values, which fail on chains with non-standard gas models
    (e.g. Mantle where approve costs 203M gas).
    """

    @pytest.mark.asyncio
    async def test_tenderly_payload_omits_gas_field(self):
        """Tenderly simulation payload should not include 'gas' key.

        Patches aiohttp to capture the actual JSON payload sent to the
        Tenderly API and asserts no 'gas' field is present in any
        simulation entry — even when the UnsignedTransaction has a
        non-zero gas_limit.
        """
        import json as json_mod

        from almanak.framework.execution.interfaces import TransactionType, UnsignedTransaction

        sim = TenderlySimulator(
            account_slug="test",
            project_slug="test",
            access_key="fake-key",
        )

        tx = UnsignedTransaction(
            to="0x" + "ab" * 20,
            value=0,
            data="0x095ea7b3" + "00" * 64,
            chain_id=42161,
            gas_limit=75_000_000,  # Must NOT appear in the API payload
            tx_type=TransactionType.EIP_1559,
            from_address="0x" + "cd" * 20,
            max_fee_per_gas=1000000000,
            max_priority_fee_per_gas=0,
        )

        captured_payload = {}

        # Mock the aiohttp session to capture the outgoing JSON
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(
            return_value=json_mod.dumps(
                {
                    "simulation_results": [
                        {
                            "simulation": {"status": True},
                            "transaction": {"gas_used": 119925407},
                        }
                    ]
                }
            )
        )

        mock_post_cm = AsyncMock()
        mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_post_cm)

        mock_session_cm = AsyncMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_cm.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await sim.simulate([tx], "arbitrum")

        # Extract the JSON payload that was sent to Tenderly
        call_args = mock_session.post.call_args
        assert call_args is not None, "aiohttp post was never called"
        captured_payload = call_args.kwargs.get("json") or call_args[1].get("json", {})

        # Assert no simulation entry contains a 'gas' field
        for i, sim_entry in enumerate(captured_payload.get("simulations", [])):
            assert "gas" not in sim_entry, (
                f"Tenderly simulation[{i}] should NOT include 'gas' field — "
                f"let the API estimate gas freely (PR #817). Found: gas={sim_entry.get('gas')}"
            )

        assert result.success is True
        assert result.gas_estimates == [119925407]

    @pytest.mark.asyncio
    async def test_alchemy_payload_omits_gas_field(self):
        """Alchemy simulation payload should not include 'gas' key.

        Patches aiohttp to capture the actual JSON payload sent to the
        Alchemy API and asserts no 'gas' field is present in any
        transaction entry.
        """
        import json as json_mod

        from almanak.framework.execution.interfaces import TransactionType, UnsignedTransaction

        sim = AlchemySimulator(api_key="fake-key")

        tx = UnsignedTransaction(
            to="0x" + "ab" * 20,
            value=0,
            data="0x095ea7b3" + "00" * 64,
            chain_id=42161,
            gas_limit=75_000_000,  # Must NOT appear in the API payload
            tx_type=TransactionType.EIP_1559,
            from_address="0x" + "cd" * 20,
            max_fee_per_gas=1000000000,
            max_priority_fee_per_gas=0,
        )

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(
            return_value=json_mod.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": [
                        {
                            "calls": [
                                {
                                    "status": "0x1",
                                    "gasUsed": hex(119925407),
                                    "returnValue": "0x",
                                }
                            ]
                        }
                    ],
                }
            )
        )

        mock_post_cm = AsyncMock()
        mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_post_cm)

        mock_session_cm = AsyncMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_cm.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await sim.simulate([tx], "arbitrum")

        # Extract the JSON payload sent to Alchemy
        call_args = mock_session.post.call_args
        assert call_args is not None, "aiohttp post was never called"
        captured_payload = call_args.kwargs.get("json") or call_args[1].get("json", {})

        # Assert no transaction entry contains a 'gas' field
        raw_txs = captured_payload.get("params", [None])[0] or []
        for i, tx_entry in enumerate(raw_txs):
            assert "gas" not in tx_entry, (
                f"Alchemy tx[{i}] should NOT include 'gas' field — "
                f"let the API estimate gas freely (PR #817). Found: gas={tx_entry.get('gas')}"
            )


class TestAlchemyMultiCallGasAggregation:
    """Verify Alchemy simulator sums sub-call gas per transaction, not per call.

    Alchemy's simulateExecutionBundle returns ALL internal sub-calls in a flat
    `calls` array for each transaction. The simulator must sum gas across all
    sub-calls within a transaction to produce one gas estimate per TX.

    Bug fixed: VIB-1861 — framework previously appended each sub-call's gas
    individually, producing N gas estimates (where N = total sub-calls) instead
    of one per transaction. This caused the orchestrator to assign the wrong
    sub-call's gas to transactions.
    """

    @pytest.mark.asyncio
    async def test_multi_call_tx_sums_gas_across_subcalls(self):
        """A 3-TX bundle where TX[1] has 9 internal sub-calls should produce 3 gas estimates."""
        import json as json_mod

        from almanak.framework.execution.interfaces import TransactionType, UnsignedTransaction

        sim = AlchemySimulator(api_key="fake-key")

        txs = [
            UnsignedTransaction(
                to="0x" + "a1" * 20,
                value=0,
                data="0x095ea7b3" + "00" * 64,
                chain_id=42161,
                tx_type=TransactionType.EIP_1559,
                from_address="0x" + "cd" * 20,
                gas_limit=75_000_000,
                max_fee_per_gas=1000000000,
                max_priority_fee_per_gas=0,
            ),
            UnsignedTransaction(
                to="0x" + "a2" * 20,
                value=0,
                data="0x12345678" + "00" * 64,
                chain_id=42161,
                tx_type=TransactionType.EIP_1559,
                from_address="0x" + "cd" * 20,
                gas_limit=75_000_000,
                max_fee_per_gas=1000000000,
                max_priority_fee_per_gas=0,
            ),
            UnsignedTransaction(
                to="0x" + "a3" * 20,
                value=0,
                data="0xaabbccdd" + "00" * 64,
                chain_id=42161,
                tx_type=TransactionType.EIP_1559,
                from_address="0x" + "cd" * 20,
                gas_limit=75_000_000,
                max_fee_per_gas=1000000000,
                max_priority_fee_per_gas=0,
            ),
        ]

        # Simulate Alchemy response: TX[0] has 1 call, TX[1] has 9 sub-calls, TX[2] has 3 sub-calls
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(
            return_value=json_mod.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": [
                        {
                            "calls": [
                                {"status": "0x1", "gasUsed": hex(50000), "returnValue": "0x"},
                            ]
                        },
                        {
                            "calls": [
                                {"status": "0x1", "gasUsed": hex(10000), "returnValue": "0x"},
                                {"status": "0x1", "gasUsed": hex(20000), "returnValue": "0x"},
                                {"status": "0x1", "gasUsed": hex(30000), "returnValue": "0x"},
                                {"status": "0x1", "gasUsed": hex(40000), "returnValue": "0x"},
                                {"status": "0x1", "gasUsed": hex(50000), "returnValue": "0x"},
                                {"status": "0x1", "gasUsed": hex(60000), "returnValue": "0x"},
                                {"status": "0x1", "gasUsed": hex(70000), "returnValue": "0x"},
                                {"status": "0x1", "gasUsed": hex(80000), "returnValue": "0x"},
                                {"status": "0x1", "gasUsed": hex(90000), "returnValue": "0x"},
                            ]
                        },
                        {
                            "calls": [
                                {"status": "0x1", "gasUsed": hex(100000), "returnValue": "0x"},
                                {"status": "0x1", "gasUsed": hex(200000), "returnValue": "0x"},
                                {"status": "0x1", "gasUsed": hex(46642), "returnValue": "0x"},
                            ]
                        },
                    ],
                }
            )
        )

        mock_post_cm = AsyncMock()
        mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_post_cm)

        mock_session_cm = AsyncMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_cm.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await sim.simulate(txs, "arbitrum")

        assert result.success is True
        # Must produce exactly 3 gas estimates (one per TX), not 13 (one per sub-call)
        assert len(result.gas_estimates) == 3, (
            f"Expected 3 gas estimates (one per TX), got {len(result.gas_estimates)}. "
            f"Sub-call gas must be summed per transaction, not flattened."
        )
        # TX[0]: 1 call with 50000 gas
        assert result.gas_estimates[0] == 50000
        # TX[1]: 9 sub-calls summed: 10k+20k+30k+40k+50k+60k+70k+80k+90k = 450000
        assert result.gas_estimates[1] == 450000
        # TX[2]: 3 sub-calls summed: 100k+200k+46642 = 346642
        assert result.gas_estimates[2] == 346642

    @pytest.mark.asyncio
    async def test_single_call_tx_unchanged(self):
        """A bundle where every TX has exactly 1 call should behave the same as before."""
        import json as json_mod

        from almanak.framework.execution.interfaces import TransactionType, UnsignedTransaction

        sim = AlchemySimulator(api_key="fake-key")

        txs = [
            UnsignedTransaction(
                to="0x" + "a1" * 20,
                value=0,
                data="0x095ea7b3" + "00" * 64,
                chain_id=42161,
                gas_limit=75_000_000,
                tx_type=TransactionType.EIP_1559,
                from_address="0x" + "cd" * 20,
                max_fee_per_gas=1000000000,
                max_priority_fee_per_gas=0,
            ),
            UnsignedTransaction(
                to="0x" + "a2" * 20,
                value=0,
                data="0x12345678" + "00" * 64,
                chain_id=42161,
                gas_limit=75_000_000,
                tx_type=TransactionType.EIP_1559,
                from_address="0x" + "cd" * 20,
                max_fee_per_gas=1000000000,
                max_priority_fee_per_gas=0,
            ),
        ]

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(
            return_value=json_mod.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": [
                        {
                            "calls": [
                                {"status": "0x1", "gasUsed": hex(46000), "returnValue": "0x"},
                            ]
                        },
                        {
                            "calls": [
                                {"status": "0x1", "gasUsed": hex(917902), "returnValue": "0x"},
                            ]
                        },
                    ],
                }
            )
        )

        mock_post_cm = AsyncMock()
        mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_post_cm)

        mock_session_cm = AsyncMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_cm.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await sim.simulate(txs, "arbitrum")

        assert result.success is True
        assert len(result.gas_estimates) == 2
        assert result.gas_estimates[0] == 46000
        assert result.gas_estimates[1] == 917902

    @pytest.mark.asyncio
    async def test_zero_gas_subcall_uses_default(self):
        """If all sub-calls in a TX return 0 gas, the TX should get a conservative default."""
        import json as json_mod

        from almanak.framework.execution.interfaces import TransactionType, UnsignedTransaction

        sim = AlchemySimulator(api_key="fake-key")

        tx = UnsignedTransaction(
            to="0x" + "a1" * 20,
            value=0,
            data="0x095ea7b3" + "00" * 64,
            chain_id=42161,
            gas_limit=75_000_000,
            tx_type=TransactionType.EIP_1559,
            from_address="0x" + "cd" * 20,
            max_fee_per_gas=1000000000,
            max_priority_fee_per_gas=0,
        )

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(
            return_value=json_mod.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": [
                        {
                            "calls": [
                                {"status": "0x1", "gasUsed": "0x0", "returnValue": "0x"},
                                {"status": "0x1", "gasUsed": "0x0", "returnValue": "0x"},
                            ]
                        },
                    ],
                }
            )
        )

        mock_post_cm = AsyncMock()
        mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_post_cm)

        mock_session_cm = AsyncMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_cm.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await sim.simulate([tx], "arbitrum")

        assert result.success is True
        assert len(result.gas_estimates) == 1
        assert result.gas_estimates[0] == 100000  # Conservative default


class TestMantleFallbackGasEstimates:
    """Verify Mantle fallback gas estimates are calibrated to measured on-chain values.

    These constants are only used when Tenderly/Alchemy simulation is unavailable.
    """

    def test_mantle_approve_gas_covers_usdc_proxy(self):
        """Mantle approve estimate must cover USDC proxy (~203M measured)."""
        from almanak.framework.intents.compiler import get_gas_estimate

        gas = get_gas_estimate("mantle", "approve")
        assert gas >= 203_000_000, (
            f"Mantle approve fallback ({gas}) too low for USDC proxy (~203M). "
            "This will cause 'out of gas' when simulation is unavailable."
        )

    def test_mantle_wrap_gas_covers_wmnt_deposit(self):
        """Mantle wrap estimate must cover WMNT deposit() (~118M measured)."""
        from almanak.framework.intents.compiler import get_gas_estimate

        gas = get_gas_estimate("mantle", "wrap_eth")
        assert gas >= 118_000_000, (
            f"Mantle wrap fallback ({gas}) too low for WMNT deposit (~118M)."
        )

    def test_mantle_unwrap_gas_covers_wmnt_withdraw(self):
        """Mantle unwrap estimate must cover WMNT withdraw() (~146M measured)."""
        from almanak.framework.intents.compiler import get_gas_estimate

        gas = get_gas_estimate("mantle", "unwrap_eth")
        assert gas >= 146_000_000, (
            f"Mantle unwrap fallback ({gas}) too low for WMNT withdraw (~146M)."
        )
