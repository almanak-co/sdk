"""Simulation Configuration for Gas Estimation and Pre-flight Validation.

This module provides configuration classes for transaction simulation using
Tenderly (primary) and Alchemy (fallback) APIs.

Design Principles:
    - Tenderly First: Tenderly is the primary simulator (unlimited bundles, state overrides)
    - Alchemy Fallback: Alchemy is fallback for supported chains (≤3 tx, 4 chains)
    - Configurable Bypass: Simulation can be disabled for trusted environments (Anvil)
    - UX First: Clear error messages, simulation URLs for debugging

Example:
    # From environment variables
    config = SimulationConfig.from_env()

    # Direct configuration
    config = SimulationConfig(
        enabled=True,
        tenderly_account="my-account",
        tenderly_project="my-project",
        tenderly_access_key="xxx",
    )

    # Check if simulation should run
    if config.should_simulate():
        simulator = create_simulator(config)
        result = await simulator.simulate(txs, chain)
"""

import logging
from dataclasses import dataclass, field
from types import MappingProxyType

from almanak.config import load_config
from almanak.config.simulation import simulation_config_from_env
from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.framework.execution.gas.constants import DEFAULT_SIMULATION_BUFFER

logger = logging.getLogger(__name__)


def _simulation_buffer_for(chain: str) -> float:
    """Return per-chain simulation buffer (VIB-4801)."""
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None or descriptor.gas.simulation_buffer is None:
        return DEFAULT_SIMULATION_BUFFER
    return descriptor.gas.simulation_buffer


# =============================================================================
# Local RPC Detection
# =============================================================================


# Patterns that indicate a local/fork RPC (where simulation should be skipped)
LOCAL_RPC_PATTERNS: list[str] = [
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    "::1",
    "anvil",
    "hardhat",
    "ganache",
]

# Common local RPC ports
LOCAL_RPC_PORTS: set[int] = {8545, 8546, 8547, 8548, 8549, 8550}


def is_local_rpc(rpc_url: str | None) -> bool:
    """Check if an RPC URL appears to be a local/fork environment.

    This function detects local RPC URLs where Tenderly/Alchemy simulation
    would be WRONG because they simulate against mainnet state, not fork state.

    When running on Anvil/Hardhat/Ganache:
    - The fork has its own state (modified by your tests)
    - Tenderly/Alchemy don't know about your fork's state
    - Simulation results would be incorrect

    Args:
        rpc_url: The RPC URL to check

    Returns:
        True if this appears to be a local RPC (simulation should be skipped)

    Example:
        >>> is_local_rpc("http://localhost:8545")
        True
        >>> is_local_rpc("http://127.0.0.1:8545")
        True
        >>> is_local_rpc("https://arb-mainnet.g.alchemy.com/v2/xxx")
        False
    """
    if not rpc_url:
        return False

    rpc_lower = rpc_url.lower()

    # Check for local hostname patterns
    for pattern in LOCAL_RPC_PATTERNS:
        if pattern in rpc_lower:
            return True

    # Check for local ports (e.g., http://192.168.1.100:8545)
    # Extract port if present
    import re

    port_match = re.search(r":(\d+)", rpc_url)
    if port_match:
        try:
            port = int(port_match.group(1))
            if port in LOCAL_RPC_PORTS:
                return True
        except ValueError:
            pass

    return False


# =============================================================================
# Chain Mappings
# =============================================================================


# Per-chain Tenderly / Alchemy SIMULATION-API support is owned by
# ``ChainDescriptor.simulation`` (``SimulationProfile``) under
# ``almanak/core/chains/`` (VIB-4851). The maps below derive from the
# chain registry instead of hardcoding chain-name literals, mirroring the
# derive-from-registry pattern in
# ``almanak/framework/execution/gas/constants.py``.
#
# The Tenderly network-id VALUE is always ``str(descriptor.chain_id)`` — it
# is NOT stored on the descriptor, which kills the historical chain-id drift
# (1648-vs-9745). Solana is excluded: simulation is EVM-only.
_SIM_DESCRIPTORS = [d for d in ChainRegistry.all() if d.family is ChainFamily.EVM]


def _alchemy_networks() -> dict[str, str]:
    """Build the chain→Alchemy-network map with a clean ``str`` value type.

    An explicit loop (rather than a dict comprehension) lets mypy narrow
    ``alchemy_network`` from ``str | None`` to ``str`` via the local guard.
    """
    networks: dict[str, str] = {}
    for d in _SIM_DESCRIPTORS:
        network = d.simulation.alchemy_network
        if network is not None:
            networks[d.name] = network
    return networks


# Tenderly network IDs for supported chains
# https://docs.tenderly.co/simulations-and-forks/simulation-api
TENDERLY_NETWORK_IDS: MappingProxyType[str, str] = MappingProxyType(
    {d.name: str(d.chain_id) for d in _SIM_DESCRIPTORS if d.simulation.tenderly_supported}
)

# Alchemy network names for supported chains
# https://docs.alchemy.com/reference/simulateexecutionbundle
ALCHEMY_NETWORKS: MappingProxyType[str, str] = MappingProxyType(_alchemy_networks())

# Chains that support Alchemy simulation
ALCHEMY_SUPPORTED_CHAINS: frozenset[str] = frozenset(ALCHEMY_NETWORKS)

# Chains that support Tenderly simulation
TENDERLY_SUPPORTED_CHAINS: frozenset[str] = frozenset(TENDERLY_NETWORK_IDS)

# Maximum transactions per Alchemy bundle
ALCHEMY_MAX_BUNDLE_SIZE: int = 3


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class SimulationConfig:
    """Configuration for transaction simulation.

    This configuration controls whether and how transactions are simulated
    before submission. Tenderly is the primary simulator with Alchemy as
    fallback for supported scenarios.

    Attributes:
        enabled: Master switch for simulation (default True)
        tenderly_account: Tenderly account slug
        tenderly_project: Tenderly project slug
        tenderly_access_key: Tenderly API access key
        alchemy_api_key: Alchemy API key (optional, for fallback)
        timeout_seconds: Timeout per simulation attempt (default 10)
        prefer_alchemy: Use Alchemy first when available (default False)

    Environment Variables:
        ALMANAK_SIMULATION_ENABLED: "true"/"false" (default true)
        TENDERLY_ACCOUNT_SLUG: Tenderly account slug
        TENDERLY_PROJECT_SLUG: Tenderly project slug
        TENDERLY_ACCESS_KEY: Tenderly API access key
        ALCHEMY_API_KEY: Alchemy API key (optional)
        ALMANAK_SIMULATION_TIMEOUT: Timeout in seconds (default 10)

    Example:
        # Load from environment
        config = SimulationConfig.from_env()

        # Check if Tenderly is configured
        if config.has_tenderly():
            print("Tenderly simulation available")

        # Check if simulation should run
        if config.should_simulate():
            # Create and use simulator
            ...
    """

    enabled: bool = True
    tenderly_account: str | None = None
    tenderly_project: str | None = None
    tenderly_access_key: str | None = field(default=None, repr=False)
    alchemy_api_key: str | None = field(default=None, repr=False)
    timeout_seconds: float = 10.0
    prefer_alchemy: bool = False

    def has_tenderly(self) -> bool:
        """Check if Tenderly is configured.

        Returns:
            True if all Tenderly credentials are present
        """
        return all(
            [
                self.tenderly_account,
                self.tenderly_project,
                self.tenderly_access_key,
            ]
        )

    def has_alchemy(self) -> bool:
        """Check if Alchemy is configured.

        Returns:
            True if Alchemy API key is present
        """
        return bool(self.alchemy_api_key)

    def should_simulate(self) -> bool:
        """Check if simulation should run.

        Simulation runs if enabled AND at least one simulator is configured.

        Returns:
            True if simulation should be performed
        """
        if not self.enabled:
            return False

        return self.has_tenderly() or self.has_alchemy()

    def can_simulate_chain(self, chain: str) -> bool:
        """Check if a chain can be simulated.

        Args:
            chain: Chain name (lowercase)

        Returns:
            True if simulation is available for this chain
        """
        # Normalize chain alias (e.g., "bnb" -> "bsc")
        try:
            from almanak.core.constants import resolve_chain_name

            chain_lower = resolve_chain_name(chain)
        except (ValueError, ImportError):
            chain_lower = chain.lower()

        # Check Tenderly support
        if self.has_tenderly() and chain_lower in TENDERLY_SUPPORTED_CHAINS:
            return True

        # Check Alchemy support
        if self.has_alchemy() and chain_lower in ALCHEMY_SUPPORTED_CHAINS:
            return True

        return False

    def get_gas_buffer(self, chain: str) -> float:
        """Get the gas buffer multiplier for a chain.

        Args:
            chain: Chain name

        Returns:
            Gas buffer as decimal (e.g., 0.1 for 10%)
        """
        return _simulation_buffer_for(chain)

    @classmethod
    def from_env(
        cls,
        prefix: str = "ALMANAK_",
        dotenv_path: str | None = None,
    ) -> "SimulationConfig":
        """Create configuration from environment variables.

        Args:
            prefix: Environment variable prefix for Almanak settings
            dotenv_path: Optional path to .env file

        Returns:
            SimulationConfig instance

        Example:
            # Standard usage
            config = SimulationConfig.from_env()

            # Custom prefix
            config = SimulationConfig.from_env(prefix="MY_APP_")
        """
        if prefix == "ALMANAK_" and dotenv_path is None:
            cfg = load_config().simulation
        else:
            # Compatibility path for non-standard prefixes / explicit dotenv
            # sources that are outside the normal framework boot surface.
            cfg = simulation_config_from_env(prefix=prefix, dotenv_path=dotenv_path)
        return cls(
            enabled=cfg.enabled,
            tenderly_account=cfg.tenderly_account,
            tenderly_project=cfg.tenderly_project,
            tenderly_access_key=cfg.tenderly_access_key,
            alchemy_api_key=cfg.alchemy_api_key,
            timeout_seconds=cfg.timeout_seconds,
            prefer_alchemy=cfg.prefer_alchemy,
        )

    @classmethod
    def disabled(cls) -> "SimulationConfig":
        """Create a disabled simulation config.

        Convenience factory for creating a config that skips all simulation.
        Useful for testing or trusted environments like Anvil forks.

        Returns:
            SimulationConfig with enabled=False
        """
        return cls(enabled=False)

    def to_dict(self) -> dict:
        """Convert to dictionary (without secrets).

        Returns:
            Dictionary safe for logging/serialization
        """
        return {
            "enabled": self.enabled,
            "tenderly_configured": self.has_tenderly(),
            "alchemy_configured": self.has_alchemy(),
            "timeout_seconds": self.timeout_seconds,
            "prefer_alchemy": self.prefer_alchemy,
        }

    def __repr__(self) -> str:
        """String representation without exposing secrets."""
        return (
            f"SimulationConfig("
            f"enabled={self.enabled}, "
            f"tenderly={'configured' if self.has_tenderly() else 'not configured'}, "
            f"alchemy={'configured' if self.has_alchemy() else 'not configured'})"
        )


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "SimulationConfig",
    "TENDERLY_NETWORK_IDS",
    "ALCHEMY_NETWORKS",
    "ALCHEMY_SUPPORTED_CHAINS",
    "TENDERLY_SUPPORTED_CHAINS",
    "ALCHEMY_MAX_BUNDLE_SIZE",
    "LOCAL_RPC_PATTERNS",
    "LOCAL_RPC_PORTS",
    "is_local_rpc",
]
