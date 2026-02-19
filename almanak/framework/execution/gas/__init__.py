"""Gas estimation constants and utilities.

This package is the single source of truth for all gas-related constants
used across the execution framework and gateway.
"""

from almanak.framework.execution.gas.constants import (
    CHAIN_GAS_BUFFERS,
    CHAIN_SIMULATION_BUFFERS,
    DEFAULT_GAS_BUFFER,
    DEFAULT_SIMULATION_BUFFER,
)

# Backward-compatible aliases
GAS_BUFFER_MULTIPLIERS = CHAIN_GAS_BUFFERS
SIMULATION_GAS_BUFFERS = CHAIN_SIMULATION_BUFFERS

__all__ = [
    "CHAIN_GAS_BUFFERS",
    "CHAIN_SIMULATION_BUFFERS",
    "DEFAULT_GAS_BUFFER",
    "DEFAULT_SIMULATION_BUFFER",
    "GAS_BUFFER_MULTIPLIERS",
    "SIMULATION_GAS_BUFFERS",
]
