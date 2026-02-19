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
