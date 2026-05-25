"""Framework-wide gas / timeout defaults and read-only derived views.

The framework-default knobs (``DEFAULT_GAS_BUFFER``, ``DEFAULT_TX_TIMEOUT_SECONDS``,
``ANVIL_GAS_PRICE_CAP_GWEI``, …) are literal here because they are *not* per-chain.

Per-chain values are owned by :class:`ChainDescriptor` under
``almanak/core/chains/`` (VIB-4801). Production code reads them via
``ChainRegistry.try_resolve(chain).gas.<field>``. The ``MappingProxyType``
views exposed below (``CHAIN_GAS_BUFFERS`` etc.) are kept solely as a
back-compat surface for regression tests that snapshot the historical
dicts — they have **zero production callers** and must not be re-introduced.
"""

from types import MappingProxyType

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily

# Walk every EVM descriptor once. Solana is intentionally excluded — the
# legacy dicts only covered EVM chains.
_EVM_DESCRIPTORS = [d for d in ChainRegistry.all() if d.family is ChainFamily.EVM]


# =============================================================================
# Gas Limit Buffers (applied to raw gas estimates)
# =============================================================================

# Default gas buffer multiplier for chains not explicitly listed
DEFAULT_GAS_BUFFER: float = 1.2

# Chain-specific gas buffer multipliers for gas limit estimation.
# Applied to raw gas estimates (from simulation or eth_estimateGas) to
# account for L1 data costs on L2s and estimation variance.
CHAIN_GAS_BUFFERS: MappingProxyType[str, float] = MappingProxyType(
    {d.name: d.gas.buffer for d in _EVM_DESCRIPTORS if d.gas.buffer is not None}
)


# =============================================================================
# Simulation Gas Buffers (applied after simulation to account for variance)
# =============================================================================

# Default simulation buffer for chains not explicitly listed
DEFAULT_SIMULATION_BUFFER: float = 0.1

# Chain-specific post-simulation gas buffers (decimal fraction; 0.1 == 10%).
CHAIN_SIMULATION_BUFFERS: MappingProxyType[str, float] = MappingProxyType(
    {d.name: d.gas.simulation_buffer for d in _EVM_DESCRIPTORS if d.gas.simulation_buffer is not None}
)


# =============================================================================
# Recommended Gas Price Caps (gwei)
# =============================================================================

# Default gas price cap for chains not explicitly listed (default 500 GWEI; 0 = no limit)
DEFAULT_GAS_PRICE_CAP_GWEI: int = 500

# Gas price cap used in Anvil mode. Gas costs no real money on Anvil forks,
# so the cap is set very high to avoid blocking development and test workflows.
ANVIL_GAS_PRICE_CAP_GWEI: int = 9999

# Recommended maximum gas prices per chain (gwei).
# Operators can override via MAX_GAS_PRICE_GWEI env var or config.
CHAIN_GAS_PRICE_CAPS_GWEI: MappingProxyType[str, int] = MappingProxyType(
    {d.name: d.gas.price_cap_gwei for d in _EVM_DESCRIPTORS if d.gas.price_cap_gwei is not None}
)


# =============================================================================
# Recommended Native Gas Cost Caps (in native token units)
# =============================================================================

# Recommended maximum gas cost per transaction in native token units.
# Operators can override via MAX_GAS_COST_NATIVE.
CHAIN_GAS_COST_CAPS_NATIVE: MappingProxyType[str, float] = MappingProxyType(
    {d.name: d.gas.cost_cap_native for d in _EVM_DESCRIPTORS if d.gas.cost_cap_native is not None}
)


# =============================================================================
# Transaction Confirmation Timeouts (seconds)
# =============================================================================

# Default transaction confirmation timeout for chains not explicitly listed
DEFAULT_TX_TIMEOUT_SECONDS: int = 120

# Chain-specific transaction confirmation timeouts.
# Ethereum L1 has 12s block times and multi-tx strategies may need 3+ blocks,
# plus gas price volatility can delay inclusion. L2s are much faster (~2s blocks).
CHAIN_TX_TIMEOUTS: MappingProxyType[str, int] = MappingProxyType(
    {d.name: d.timeouts.tx_confirmation for d in _EVM_DESCRIPTORS if d.timeouts.tx_confirmation is not None}
)


# =============================================================================
# gRPC Execute Call Timeouts (seconds)
# =============================================================================

# Default gRPC timeout for the Execute call (covers gas estimation + submission + TX confirmation).
# Must be larger than CHAIN_TX_TIMEOUTS to account for overhead before TX is submitted.
# Overhead components: gas estimation (LocalSimulator snapshot+execute), signing, submission.
DEFAULT_GRPC_EXECUTE_TIMEOUT_SECONDS: int = 300

# Chain-specific gRPC Execute timeouts.
# Rule of thumb: CHAIN_TX_TIMEOUTS[chain] + 180s overhead for gas estimation + processing.
CHAIN_GRPC_EXECUTE_TIMEOUTS: MappingProxyType[str, int] = MappingProxyType(
    {d.name: d.timeouts.grpc_execute for d in _EVM_DESCRIPTORS if d.timeouts.grpc_execute is not None}
)
