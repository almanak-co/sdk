"""Direct Simulator - Pass-through implementation for trusted environments.

This module provides a simulator implementation that skips actual simulation,
returning a successful result to allow execution to proceed. This is a
legitimate production implementation for scenarios where simulation is
not needed or not desired, such as:

- Local fork execution (Anvil, Hardhat) where transactions are tested directly
- Trusted environments where transactions are pre-validated
- High-frequency execution where simulation latency is unacceptable

The interface allows future implementations (e.g., TenderlySimulator) to be
swapped in without changes to calling code.

Example:
    from almanak.framework.execution.simulator import DirectSimulator
    from almanak.framework.execution.interfaces import UnsignedTransaction

    simulator = DirectSimulator()

    unsigned_tx = UnsignedTransaction(
        to="0x1234...",
        value=0,
        data="0x",
        chain_id=42161,
        gas_limit=21000,
        max_fee_per_gas=100000000,
        max_priority_fee_per_gas=1000000,
    )

    result = await simulator.simulate([unsigned_tx], chain="arbitrum")
    assert result.success is True
    assert result.simulated is False  # No actual simulation performed
"""

import logging
from typing import TYPE_CHECKING, Any

from almanak.framework.execution.interfaces import (
    SimulationResult,
    Simulator,
    UnsignedTransaction,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class DirectSimulator(Simulator):
    """Pass-through simulator that skips actual simulation.

    This simulator returns a successful SimulationResult without performing
    any actual simulation. It is designed for environments where simulation
    is not needed, such as local fork testing or trusted execution contexts.

    The key differentiator from other simulators:
    - Returns `simulated=False` to indicate no simulation was performed
    - Always returns `success=True` (assumes transactions are valid)
    - Logs that simulation was skipped for observability

    This is NOT a stub or shortcut - it is a legitimate implementation for
    production use cases where pre-execution simulation adds no value or
    where the latency cost is unacceptable.

    Future Alternatives:
    - TenderlySimulator: Full simulation via Tenderly API
    - LocalSimulator: Simulation via local node eth_call
    - FlashbotsSimulator: Simulation via Flashbots bundle simulation

    Attributes:
        name: Identifier for this simulator (for logging and metrics)

    Example:
        simulator = DirectSimulator()

        # Simulate a single transaction
        result = await simulator.simulate([tx], chain="arbitrum")

        if result.success:
            # Proceed to signing and submission
            ...
        else:
            # Handle simulation failure (won't happen with DirectSimulator)
            ...
    """

    def __init__(self, name: str = "direct") -> None:
        """Initialize the DirectSimulator.

        Args:
            name: Identifier for this simulator instance (default: "direct")
        """
        self._name = name
        logger.info(
            "DirectSimulator initialized",
            extra={
                "simulator_name": self._name,
                "simulation_enabled": False,
            },
        )

    @property
    def name(self) -> str:
        """Return the simulator name."""
        return self._name

    async def simulate(
        self,
        txs: list[UnsignedTransaction],
        chain: str,
        state_overrides: dict[str, Any] | None = None,
    ) -> SimulationResult:
        """Return a pass-through simulation result without actual simulation.

        This method logs that simulation was skipped and returns a successful
        SimulationResult with `simulated=False` to indicate no actual simulation
        was performed.

        Args:
            txs: List of unsigned transactions to "simulate"
            chain: Chain name (logged but not used for simulation)
            state_overrides: Ignored - DirectSimulator doesn't perform simulation

        Returns:
            SimulationResult with:
            - success=True (assumes transactions are valid)
            - simulated=False (indicates no simulation was performed)
            - Empty gas_estimates, warnings, state_changes, logs

        Note:
            This method never raises exceptions for valid input.
            Infrastructure failures are not possible since no external
            calls are made.
        """
        tx_count = len(txs)

        logger.info(
            "Simulation skipped (DirectSimulator)",
            extra={
                "simulator_name": self._name,
                "chain": chain,
                "transaction_count": tx_count,
                "reason": "direct_passthrough",
            },
        )

        # Log individual transaction details at debug level
        for i, tx in enumerate(txs):
            logger.debug(
                f"Transaction {i + 1}/{tx_count} passed through without simulation",
                extra={
                    "tx_index": i,
                    "to": tx.to,
                    "value": tx.value,
                    "gas_limit": tx.gas_limit,
                    "chain_id": tx.chain_id,
                },
            )

        return SimulationResult(
            success=True,
            simulated=False,
        )


__all__ = [
    "DirectSimulator",
]
