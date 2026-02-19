"""Fallback Simulator - Cascade simulation strategy.

This module provides a composite simulator that tries simulators in order,
falling back to the next one if the current fails with a recoverable error.

Cascade order (when fully configured):
    Tenderly -> Alchemy -> LocalSimulator -> DirectSimulator (last resort)

Key Features:
    - Tenderly as primary (better SAFE support, more chains, unlimited bundles)
    - Alchemy as first fallback (when Tenderly is down)
    - LocalSimulator as second fallback (eth_estimateGas against RPC)
    - Automatic fallback on recoverable errors (timeouts, 5xx errors)
    - No fallback for transaction reverts (that's a real failure)

Example:
    fallback_simulator = FallbackSimulator(
        primary=TenderlySimulator(...),
        fallbacks=[AlchemySimulator(...), LocalSimulator(...)],
    )

    # Will try Tenderly first, then Alchemy, then LocalSimulator
    result = await fallback_simulator.simulate(txs, chain="arbitrum")
"""

import logging
from typing import Any

from almanak.framework.execution.interfaces import (
    SimulationError,
    SimulationResult,
    Simulator,
    UnsignedTransaction,
)

logger = logging.getLogger(__name__)


class FallbackSimulator(Simulator):
    """Composite simulator with cascade fallback strategy.

    This simulator wraps multiple underlying simulators and implements a
    cascade strategy: try the primary first, and if it fails with a
    recoverable error (timeout, connection error, 5xx status), try each
    fallback in order.

    Transaction reverts are NOT recoverable - if any simulator says
    a transaction would revert, the cascade stops immediately.

    Fallback Conditions:
        - SimulationError with recoverable=True (timeouts, connection errors)
        - HTTP 5xx errors from the API
        - Current simulator doesn't support the chain but a later one does

    NO Fallback:
        - Transaction simulation shows revert (that's a real failure)
        - Invalid input (bad chain, too many transactions for any)
        - SimulationError with recoverable=False

    Example:
        fallback = FallbackSimulator(
            primary=TenderlySimulator(...),
            fallbacks=[AlchemySimulator(...), LocalSimulator(rpc_url=...)],
        )

        # Tries Tenderly -> Alchemy -> LocalSimulator
        result = await fallback.simulate(txs, chain="arbitrum")
    """

    def __init__(
        self,
        primary: Simulator,
        secondary: Simulator | None = None,
        fallbacks: list[Simulator] | None = None,
        name: str = "fallback",
    ) -> None:
        """Initialize the FallbackSimulator.

        Args:
            primary: Primary simulator to try first
            secondary: Optional single fallback simulator (backward-compatible).
                       If both secondary and fallbacks are provided, secondary
                       is prepended to fallbacks.
            fallbacks: Optional ordered list of fallback simulators
            name: Simulator name for logging (default "fallback")
        """
        self._primary = primary
        self._name = name

        # Build fallback list: secondary (if given) + fallbacks (if given)
        self._fallbacks: list[Simulator] = []
        if secondary is not None:
            self._fallbacks.append(secondary)
        if fallbacks:
            for fb in fallbacks:
                if fb not in self._fallbacks:
                    self._fallbacks.append(fb)

        primary_name = getattr(primary, "name", primary.__class__.__name__)
        fallback_names = [getattr(fb, "name", fb.__class__.__name__) for fb in self._fallbacks]

        logger.info(
            f"FallbackSimulator initialized: primary={primary_name}, "
            f"fallbacks={fallback_names if fallback_names else 'none'}"
        )

    @property
    def name(self) -> str:
        """Return the simulator name."""
        return self._name

    def supports_chain(self, chain: str) -> bool:
        """Check if any simulator supports the given chain.

        Args:
            chain: Chain name (lowercase)

        Returns:
            True if primary or any fallback supports this chain
        """
        if self._primary.supports_chain(chain):
            return True

        return any(fb.supports_chain(chain) for fb in self._fallbacks)

    async def simulate(
        self,
        txs: list[UnsignedTransaction],
        chain: str,
        state_overrides: dict[str, Any] | None = None,
    ) -> SimulationResult:
        """Simulate transactions with cascade fallback strategy.

        Tries the primary simulator first. If it fails with a recoverable
        error, tries each fallback in order. Transaction reverts cause
        immediate failure (no fallback).

        Args:
            txs: List of unsigned transactions to simulate
            chain: Chain name
            state_overrides: Optional state overrides (may not be supported by all simulators)

        Returns:
            SimulationResult from whichever simulator succeeded

        Raises:
            SimulationError: If all simulators fail or none supports the chain
        """
        if not txs:
            logger.warning("No transactions to simulate")
            return SimulationResult(success=True, simulated=False)

        chain_lower = chain.lower()

        # Build ordered list of simulators to try
        all_simulators = [self._primary, *self._fallbacks]

        # Find simulators that support this chain
        supporting = [
            (sim, getattr(sim, "name", sim.__class__.__name__))
            for sim in all_simulators
            if sim.supports_chain(chain_lower)
        ]

        if not supporting:
            raise SimulationError(
                reason=f"No simulator supports chain: {chain}",
                recoverable=False,
            )

        last_error: SimulationError | None = None

        for i, (sim, sim_name) in enumerate(supporting):
            is_last = i == len(supporting) - 1
            logger.info(f"Trying simulator ({sim_name}) for {chain} [{i + 1}/{len(supporting)}]")

            try:
                result = await sim.simulate(txs, chain, state_overrides)

                # Tag the result with which simulator produced it
                result.simulator_name = sim_name

                # If simulation shows transaction would revert, that's authoritative
                # Don't try fallback - the tx will still revert
                if not result.success and result.simulated:
                    logger.info(f"Simulator {sim_name} shows transaction would revert")
                    return result

                return result

            except SimulationError as e:
                logger.warning(f"Simulator {sim_name} failed: {e}")
                last_error = e

                # Only cascade on recoverable errors
                if not e.recoverable:
                    logger.info(f"Error from {sim_name} is not recoverable, stopping cascade")
                    raise

                if is_last:
                    break

                # Continue to next fallback
                logger.info("Falling back to next simulator")

        # All simulators failed
        sim_names = [name for _, name in supporting]
        raise SimulationError(
            reason=f"All simulators failed: {sim_names}. Last error: {last_error}",
            recoverable=False,
        )


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "FallbackSimulator",
]
