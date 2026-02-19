"""Single source of truth for all gas-related constants.

This module consolidates gas buffer multipliers, simulation buffers, and
default gas prices that were previously scattered across orchestrator.py,
chain_executor.py, simulator/config.py, and gateway/simulation_service.py.

All gas-related constant lookups should import from this module.
"""

# =============================================================================
# Gas Limit Buffers (applied to raw gas estimates)
# =============================================================================

# Default gas buffer multiplier for chains not explicitly listed
DEFAULT_GAS_BUFFER: float = 1.2

# Chain-specific gas buffer multipliers for gas limit estimation.
# Applied to raw gas estimates (from simulation or eth_estimateGas) to
# account for L1 data costs on L2s and estimation variance.
#
# Superset of all chains from orchestrator.py (6 chains) and
# chain_executor.py (12 chains). chain_executor.py had already drifted
# ahead with additional chain support.
CHAIN_GAS_BUFFERS: dict[str, float] = {
    "ethereum": 1.1,  # 10% buffer for mainnet
    "arbitrum": 1.5,  # 50% buffer for L1 data cost
    "optimism": 1.5,  # 50% buffer for L1 data cost
    "polygon": 1.2,  # 20% buffer
    "base": 1.5,  # 50% buffer for L1 data cost
    "avalanche": 1.1,  # 10% buffer
    "bnb": 1.2,  # 20% buffer for BSC
    "linea": 1.5,  # 50% buffer for zkEVM L1 data cost
    "plasma": 1.1,  # 10% buffer for L1
    "blast": 1.5,  # 50% buffer for L2 data cost
    "mantle": 1.5,  # 50% buffer for L2 data cost
    "berachain": 1.2,  # 20% buffer for L1
}

# =============================================================================
# Simulation Gas Buffers (applied after simulation to account for variance)
# =============================================================================

# Default simulation buffer for chains not explicitly listed
DEFAULT_SIMULATION_BUFFER: float = 0.1

# Chain-specific simulation gas buffers (decimal fraction, e.g. 0.1 = 10%).
# Applied after Tenderly/Alchemy simulation to account for estimation variance
# and L1 data cost variability on L2s.
#
# Consolidated from simulator/config.py (framework, 12 chains) and
# gateway/services/simulation_service.py (gateway, 9 chains).
# Framework values are used as source of truth since they have higher L2
# buffers which are more appropriate for L1 data cost variability.
# sonic (0.10) was added from gateway values since it was missing from framework.
# =============================================================================
# Recommended Gas Price Caps (gwei)
# =============================================================================

# Default gas price cap for chains not explicitly listed (default 500 GWEI; 0 = no limit)
DEFAULT_GAS_PRICE_CAP_GWEI: int = 500

# Recommended maximum gas prices per chain.
# These reflect typical upper bounds for normal operation. Prices above
# these thresholds usually indicate network congestion or spike conditions
# where execution should be deferred.
#
# Values are conservative - most transactions should be well below these.
# Operators can override via MAX_GAS_PRICE_GWEI env var or config.
CHAIN_GAS_PRICE_CAPS_GWEI: dict[str, int] = {
    "ethereum": 300,  # L1 mainnet: 300 gwei max (base fee spikes)
    "arbitrum": 10,  # L2: normally <1 gwei, 10 covers spikes
    "optimism": 10,  # L2: normally <1 gwei
    "polygon": 500,  # Can spike during congestion
    "base": 10,  # L2: normally <1 gwei
    "avalanche": 100,  # C-Chain: normally 25-50 gwei
    "bnb": 20,  # BSC: normally 3-5 gwei
    "linea": 10,  # zkEVM L2
    "plasma": 50,  # L1
    "blast": 10,  # L2
    "mantle": 10,  # L2
    "berachain": 50,  # L1
    "sonic": 100,  # L1
}

# =============================================================================
# Recommended Native Gas Cost Caps (in native token units)
# =============================================================================

# Recommended maximum gas cost per transaction in native token units.
# These prevent unexpectedly expensive transactions during gas spikes.
# Set to 0.0 to use no limit. Operators can override via MAX_GAS_COST_NATIVE.
CHAIN_GAS_COST_CAPS_NATIVE: dict[str, float] = {
    "ethereum": 0.1,  # 0.1 ETH (~$300 at $3000/ETH)
    "arbitrum": 0.01,  # 0.01 ETH (~$30)
    "optimism": 0.01,  # 0.01 ETH (~$30)
    "polygon": 50.0,  # 50 MATIC (~$50 at $1/MATIC)
    "base": 0.01,  # 0.01 ETH (~$30)
    "avalanche": 1.0,  # 1 AVAX (~$30)
    "bnb": 0.05,  # 0.05 BNB (~$30)
}

# =============================================================================
# Transaction Confirmation Timeouts (seconds)
# =============================================================================

# Default transaction confirmation timeout for chains not explicitly listed
DEFAULT_TX_TIMEOUT_SECONDS: int = 120

# Chain-specific transaction confirmation timeouts.
# Ethereum L1 has 12s block times and multi-tx strategies may need 3+ blocks,
# plus gas price volatility can delay inclusion. L2s are much faster (~2s blocks).
CHAIN_TX_TIMEOUTS: dict[str, int] = {
    "ethereum": 300,  # 300s - L1 has 12s blocks; 3+ sequential txs need more time
    "arbitrum": 120,  # 120s - L2 fast blocks (~0.25s)
    "optimism": 120,  # 120s - L2 fast blocks (~2s)
    "polygon": 180,  # 180s - 2s blocks but can have reorgs
    "base": 120,  # 120s - L2 fast blocks (~2s)
    "avalanche": 120,  # 120s - 2s blocks
    "plasma": 120,  # 120s - fast finality
}

CHAIN_SIMULATION_BUFFERS: dict[str, float] = {
    "ethereum": 0.1,  # 10% buffer
    "arbitrum": 0.5,  # 50% buffer for L1 data cost
    "optimism": 0.5,  # 50% buffer for L1 data cost
    "polygon": 0.2,  # 20% buffer
    "base": 0.5,  # 50% buffer for L1 data cost
    "avalanche": 0.1,  # 10% buffer
    "bnb": 0.1,  # 10% buffer
    "linea": 0.3,  # 30% buffer
    "plasma": 0.1,  # 10% buffer
    "blast": 0.5,  # 50% buffer for L2 data cost
    "mantle": 0.5,  # 50% buffer for L2 data cost
    "berachain": 0.2,  # 20% buffer for L1
    "sonic": 0.1,  # 10% buffer (from gateway)
}
