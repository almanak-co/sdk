"""Simulator implementations for transaction simulation.

This module provides implementations of the Simulator ABC for various
simulation backends including Tenderly (primary) and Alchemy (fallback).

Available Simulators:
    - DirectSimulator: Pass-through simulator that skips actual simulation
    - TenderlySimulator: Full simulation via Tenderly REST API (primary)
    - AlchemySimulator: Simulation via Alchemy RPC API (fallback)
    - FallbackSimulator: Composite simulator with primary/fallback strategy

Configuration:
    - SimulationConfig: Configuration class for simulation settings

Factory Function:
    - create_simulator: Create the appropriate simulator based on configuration

Example:
    # Simple pass-through (no simulation)
    from almanak.framework.execution.simulator import DirectSimulator
    simulator = DirectSimulator()
    result = await simulator.simulate([unsigned_tx], chain="arbitrum")

    # Full simulation with Tenderly (primary) and Alchemy (fallback)
    from almanak.framework.execution.simulator import create_simulator, SimulationConfig
    config = SimulationConfig.from_env()
    simulator = create_simulator(config)
    result = await simulator.simulate([unsigned_tx], chain="arbitrum")

    # SAFE wallet simulation with state overrides
    state_overrides = {
        "0xSafeAddress": {"balance": hex(10 * 10**18)}  # 10 ETH
    }
    result = await simulator.simulate(
        [unsigned_tx],
        chain="arbitrum",
        state_overrides=state_overrides,
    )
"""

import logging
from typing import Optional

from almanak.framework.execution.interfaces import Simulator
from almanak.framework.execution.simulator.alchemy import AlchemySimulator
from almanak.framework.execution.simulator.config import (
    ALCHEMY_MAX_BUNDLE_SIZE,
    ALCHEMY_NETWORKS,
    ALCHEMY_SUPPORTED_CHAINS,
    LOCAL_RPC_PATTERNS,
    LOCAL_RPC_PORTS,
    SIMULATION_GAS_BUFFERS,
    TENDERLY_NETWORK_IDS,
    TENDERLY_SUPPORTED_CHAINS,
    SimulationConfig,
    is_local_rpc,
)
from almanak.framework.execution.simulator.direct import DirectSimulator
from almanak.framework.execution.simulator.fallback import FallbackSimulator
from almanak.framework.execution.simulator.local import LocalSimulator
from almanak.framework.execution.simulator.tenderly import TenderlySimulator

logger = logging.getLogger(__name__)


def create_simulator(
    config: SimulationConfig | None = None,
    rpc_url: str | None = None,
) -> Simulator:
    """Create a simulator based on the provided configuration.

    This factory function creates the appropriate simulator based on what
    credentials are available. The cascade order is:

    For remote RPC (production):
        Tenderly -> Alchemy -> LocalSimulator (eth_estimateGas) -> DirectSimulator

    For local RPC (Anvil/Hardhat):
        LocalSimulator (preferred - simulates against fork state)

    Transaction reverts at any stage cause immediate failure (no fallback).
    Only infrastructure errors (timeout, 5xx, rate limit) trigger fallback.

    Args:
        config: SimulationConfig with credentials. If None, loads from environment.
        rpc_url: Optional RPC URL. If local RPC detected, LocalSimulator is preferred.
                 Also used as the RPC endpoint for LocalSimulator fallback.

    Returns:
        Appropriate Simulator implementation

    Example:
        # Load from environment
        simulator = create_simulator()

        # With explicit config
        config = SimulationConfig(
            enabled=True,
            tenderly_account="my-account",
            tenderly_project="my-project",
            tenderly_access_key="xxx",
        )
        simulator = create_simulator(config)

        # Explicitly disabled
        simulator = create_simulator(SimulationConfig.disabled())

        # Auto-detect Anvil - uses LocalSimulator (fast, on-node estimation)
        simulator = create_simulator(rpc_url="http://localhost:8545")
    """
    # Load config from environment if not provided
    if config is None:
        config = SimulationConfig.from_env()

    # If simulation disabled, return pass-through
    if not config.enabled:
        logger.info("Simulation disabled, using DirectSimulator (pass-through)")
        return DirectSimulator(name="direct_disabled")

    # Check for local RPC - Tenderly/Alchemy simulate against mainnet, not fork state.
    # Use LocalSimulator which simulates against the actual fork state.
    if rpc_url and is_local_rpc(rpc_url):
        logger.info(
            f"Local RPC detected ({rpc_url}), using LocalSimulator. "
            "Tenderly/Alchemy simulate against mainnet, not your fork's state."
        )
        return LocalSimulator(rpc_url=rpc_url, name="local_fork")

    has_tenderly = config.has_tenderly()
    has_alchemy = config.has_alchemy()

    # No simulators configured - use LocalSimulator if we have an RPC URL,
    # otherwise DirectSimulator
    if not has_tenderly and not has_alchemy:
        if rpc_url:
            logger.warning(
                "No simulation credentials configured (TENDERLY_*, ALCHEMY_API_KEY). "
                "Using LocalSimulator (eth_estimateGas) as fallback."
            )
            return LocalSimulator(rpc_url=rpc_url, name="local_no_credentials")

        logger.warning(
            "No simulation credentials configured and no RPC URL provided. "
            "Using DirectSimulator (pass-through). Configure Tenderly credentials "
            "for production gas estimation."
        )
        return DirectSimulator(name="direct_no_credentials")

    # Build primary and fallbacks list
    primary: Simulator | None = None
    fallbacks: list[Simulator] = []

    if has_tenderly:
        primary = TenderlySimulator(
            account_slug=config.tenderly_account,  # type: ignore
            project_slug=config.tenderly_project,  # type: ignore
            access_key=config.tenderly_access_key,  # type: ignore
            timeout_seconds=config.timeout_seconds,
        )
        logger.info("Created TenderlySimulator as primary")

    if has_alchemy:
        alchemy_sim = AlchemySimulator(
            api_key=config.alchemy_api_key,  # type: ignore
            timeout_seconds=config.timeout_seconds,
        )

        if primary is None or config.prefer_alchemy:
            # Alchemy is primary (no Tenderly or prefer_alchemy=True)
            if config.prefer_alchemy and primary is not None:
                fallbacks.append(primary)
                primary = alchemy_sim
                logger.info("Created AlchemySimulator as primary (prefer_alchemy=True)")
            else:
                primary = alchemy_sim
                logger.info("Created AlchemySimulator as primary (no Tenderly)")
        else:
            # Alchemy is fallback
            fallbacks.append(alchemy_sim)
            logger.info("Created AlchemySimulator as fallback")

    # Add LocalSimulator as final fallback before DirectSimulator
    # (only if we have an RPC URL for eth_estimateGas)
    if rpc_url:
        fallbacks.append(LocalSimulator(rpc_url=rpc_url, name="local_fallback"))
        logger.info("Created LocalSimulator as fallback (eth_estimateGas)")

    # Build the cascade
    if primary is not None and fallbacks:
        fallback_names = [getattr(fb, "name", fb.__class__.__name__) for fb in fallbacks]
        logger.info(f"Using FallbackSimulator: primary={primary.name}, fallbacks={fallback_names}")
        return FallbackSimulator(primary=primary, fallbacks=fallbacks)

    # Single simulator
    if primary is not None:
        return primary

    # Should not reach here, but return pass-through as safety
    logger.warning("Unexpected simulator configuration, using DirectSimulator")
    return DirectSimulator(name="direct_fallback")


__all__ = [
    # Simulators
    "DirectSimulator",
    "LocalSimulator",
    "TenderlySimulator",
    "AlchemySimulator",
    "FallbackSimulator",
    # Configuration
    "SimulationConfig",
    # Factory
    "create_simulator",
    # Local RPC detection
    "is_local_rpc",
    "LOCAL_RPC_PATTERNS",
    "LOCAL_RPC_PORTS",
    # Constants
    "TENDERLY_NETWORK_IDS",
    "TENDERLY_SUPPORTED_CHAINS",
    "ALCHEMY_NETWORKS",
    "ALCHEMY_SUPPORTED_CHAINS",
    "ALCHEMY_MAX_BUNDLE_SIZE",
    "SIMULATION_GAS_BUFFERS",
]
