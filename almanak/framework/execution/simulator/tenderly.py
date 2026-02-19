"""Tenderly Simulator - Production transaction simulation via Tenderly REST API.

This module provides transaction simulation using the Tenderly simulation API,
which is the primary simulation backend for production use.

Key Features:
    - Unlimited bundle size (no 3-transaction limit like Alchemy)
    - State overrides for SAFE wallet ETH balance simulation
    - Support for all v2 chains (9+ chains)
    - Detailed revert reasons and simulation URLs
    - Gas estimation with chain-specific buffers

API Documentation:
    https://docs.tenderly.co/simulations-and-forks/simulation-api

Example:
    simulator = TenderlySimulator(
        account_slug="my-account",
        project_slug="my-project",
        access_key="xxx",
    )

    result = await simulator.simulate(
        txs=[unsigned_tx],
        chain="arbitrum",
        state_overrides={"0x...": {"balance": hex(10 * 10**18)}},
    )

    if result.success:
        print(f"Gas estimates: {result.gas_estimates}")
    else:
        print(f"Would revert: {result.revert_reason}")
"""

import json
import logging
from typing import Any

import aiohttp

from almanak.framework.execution.interfaces import (
    SimulationError,
    SimulationResult,
    Simulator,
    UnsignedTransaction,
)

from .config import (
    TENDERLY_NETWORK_IDS,
    TENDERLY_SUPPORTED_CHAINS,
)

logger = logging.getLogger(__name__)


class TenderlySimulator(Simulator):
    """Transaction simulation via Tenderly REST API.

    This simulator uses Tenderly's simulate-bundle endpoint to simulate
    transactions before submission. It provides accurate gas estimates,
    pre-flight validation, and supports SAFE wallet simulations via
    state overrides.

    Key Advantages over Alchemy:
        - No transaction bundle limit (Alchemy limited to 3)
        - State override support for SAFE wallet ETH balance
        - Support for more chains (9+ vs 4 for Alchemy)
        - Detailed simulation dashboard URLs

    Attributes:
        account_slug: Tenderly account identifier
        project_slug: Tenderly project identifier
        timeout_seconds: Request timeout

    Example:
        simulator = TenderlySimulator(
            account_slug="my-account",
            project_slug="my-project",
            access_key="xxx",
        )

        # Basic simulation
        result = await simulator.simulate([tx], chain="arbitrum")

        # SAFE wallet simulation with ETH balance override
        result = await simulator.simulate(
            [tx],
            chain="arbitrum",
            state_overrides={"0xSafeAddress": {"balance": hex(10 * 10**18)}},
        )
    """

    def __init__(
        self,
        account_slug: str,
        project_slug: str,
        access_key: str,
        timeout_seconds: float = 10.0,
        name: str = "tenderly",
    ) -> None:
        """Initialize the TenderlySimulator.

        Args:
            account_slug: Tenderly account slug
            project_slug: Tenderly project slug
            access_key: Tenderly API access key
            timeout_seconds: Request timeout (default 10s)
            name: Simulator name for logging (default "tenderly")

        Raises:
            ValueError: If any required parameter is missing
        """
        if not account_slug:
            raise ValueError("Tenderly account_slug is required")
        if not project_slug:
            raise ValueError("Tenderly project_slug is required")
        if not access_key:
            raise ValueError("Tenderly access_key is required")

        self._account_slug = account_slug
        self._project_slug = project_slug
        self._access_key = access_key
        self._timeout = timeout_seconds
        self._name = name

        self._base_url = f"https://api.tenderly.co/api/v1/account/{account_slug}/project/{project_slug}"

        logger.info(
            f"TenderlySimulator initialized: account={account_slug}, project={project_slug}, timeout={timeout_seconds}s"
        )

    @property
    def name(self) -> str:
        """Return the simulator name."""
        return self._name

    def supports_chain(self, chain: str) -> bool:
        """Check if this simulator supports a given chain.

        Args:
            chain: Chain name (lowercase)

        Returns:
            True if Tenderly supports this chain
        """
        return chain.lower() in TENDERLY_SUPPORTED_CHAINS

    async def simulate(
        self,
        txs: list[UnsignedTransaction],
        chain: str,
        state_overrides: dict[str, Any] | None = None,
    ) -> SimulationResult:
        """Simulate transactions via Tenderly API.

        This method simulates the execution of one or more transactions
        using Tenderly's bundle simulation endpoint. It returns gas estimates
        and validates that transactions will succeed.

        Args:
            txs: List of unsigned transactions to simulate
            chain: Chain name (e.g., "arbitrum")
            state_overrides: Optional state overrides for SAFE wallets
                Format: {"0xAddress": {"balance": "0xHexWei"}}

        Returns:
            SimulationResult with gas estimates and success status

        Raises:
            SimulationError: For infrastructure failures (not tx failures)
        """
        if not txs:
            logger.warning("No transactions to simulate")
            return SimulationResult(success=True, simulated=False)

        chain_lower = chain.lower()

        # Validate chain is supported
        if not self.supports_chain(chain_lower):
            raise SimulationError(
                reason=f"Tenderly does not support chain: {chain}. Supported: {sorted(TENDERLY_SUPPORTED_CHAINS)}",
                recoverable=False,
            )

        network_id = TENDERLY_NETWORK_IDS[chain_lower]

        logger.info(
            f"Simulating {len(txs)} transaction(s) on {chain} via Tenderly",
            extra={
                "chain": chain,
                "network_id": network_id,
                "tx_count": len(txs),
                "has_state_overrides": state_overrides is not None,
            },
        )

        try:
            return await self._simulate_bundle(
                txs=txs,
                network_id=network_id,
                chain=chain_lower,
                state_overrides=state_overrides,
            )
        except aiohttp.ClientError as e:
            logger.error(f"Tenderly API connection error: {e}")
            raise SimulationError(
                reason=f"Tenderly API connection failed: {e}",
                recoverable=True,
            ) from e
        except TimeoutError as e:
            logger.error(f"Tenderly API timeout after {self._timeout}s")
            raise SimulationError(
                reason=f"Tenderly API timeout after {self._timeout} seconds",
                recoverable=True,
            ) from e

    async def _simulate_bundle(
        self,
        txs: list[UnsignedTransaction],
        network_id: str,
        chain: str,
        state_overrides: dict[str, Any] | None = None,
    ) -> SimulationResult:
        """Execute bundle simulation via Tenderly REST API.

        Args:
            txs: Transactions to simulate
            network_id: Tenderly network ID
            chain: Chain name for gas buffer lookup
            state_overrides: Optional state overrides

        Returns:
            SimulationResult with gas estimates
        """
        url = f"{self._base_url}/simulate-bundle"

        # Build simulations array
        simulations = []
        for i, tx in enumerate(txs):
            simulation = {
                "network_id": network_id,
                "from": tx.from_address or "0x0000000000000000000000000000000000000000",
                "to": tx.to or "0x0000000000000000000000000000000000000000",
                "input": tx.data or "0x",
                "save": False,  # Don't save to dashboard (faster)
            }

            # Add value if non-zero
            if tx.value and tx.value > 0:
                simulation["value"] = hex(tx.value)

            # Add gas limit if set (for validation, not estimation)
            if tx.gas_limit and tx.gas_limit > 0:
                simulation["gas"] = tx.gas_limit

            # Add state_objects only to first simulation (applies to entire bundle)
            if i == 0 and state_overrides:
                simulation["state_objects"] = state_overrides
                logger.debug(f"Using state overrides for simulation: {list(state_overrides.keys())}")

            simulations.append(simulation)

        payload = {
            "simulations": simulations,
        }

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Access-Key": self._access_key,
        }

        logger.debug(f"Tenderly request URL: {url}")
        logger.debug(f"Tenderly payload: {json.dumps(payload, indent=2)}")

        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=self._timeout),
            ) as response:
                response_text = await response.text()

                # Log response for debugging
                logger.debug(f"Tenderly response status: {response.status}")

                if response.status != 200:
                    logger.error(f"Tenderly API error: status={response.status}, body={response_text[:500]}")
                    raise SimulationError(
                        reason=f"Tenderly API error: HTTP {response.status}",
                        recoverable=response.status >= 500,  # Retry 5xx errors
                    )

                try:
                    response_json = json.loads(response_text)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse Tenderly response: {e}")
                    raise SimulationError(
                        reason="Invalid JSON response from Tenderly",
                        recoverable=False,
                    ) from e

        return self._parse_response(response_json, chain, len(txs))

    def _parse_response(
        self,
        response: dict[str, Any],
        chain: str,
        expected_count: int,
    ) -> SimulationResult:
        """Parse Tenderly simulation response.

        Args:
            response: Raw API response
            chain: Chain name for gas buffer
            expected_count: Expected number of simulation results

        Returns:
            SimulationResult with parsed data
        """
        # Check for API-level error
        if "error" in response:
            error_msg = response.get("error", {})
            if isinstance(error_msg, dict):
                error_msg = error_msg.get("message", str(error_msg))
            logger.error(f"Tenderly API returned error: {error_msg}")
            return SimulationResult(
                success=False,
                simulated=True,
                revert_reason=str(error_msg),
            )

        # Get simulation results
        simulation_results = response.get("simulation_results")
        if simulation_results is None:
            logger.error(f"Missing simulation_results in response: {list(response.keys())}")
            raise SimulationError(
                reason="Tenderly response missing simulation_results",
                recoverable=False,
            )

        # Verify count matches
        if len(simulation_results) != expected_count:
            logger.warning(f"Result count mismatch: expected {expected_count}, got {len(simulation_results)}")

        # Parse each simulation result
        gas_estimates: list[int] = []
        warnings: list[str] = []
        logs: list[dict[str, Any]] = []
        simulation_url: str | None = None

        for i, result in enumerate(simulation_results):
            simulation_data = result.get("simulation", {})
            transaction_data = result.get("transaction", {})

            # Check simulation status
            if not simulation_data.get("status", False):
                # Simulation failed - transaction would revert
                error_message = simulation_data.get("error_message", "Unknown error")
                revert_reason = simulation_data.get("error_info", {}).get("error_message", error_message)

                logger.warning(f"Transaction {i} simulation failed: {revert_reason}")

                # Get simulation URL for debugging
                sim_url = self._build_simulation_url(result)

                return SimulationResult(
                    success=False,
                    simulated=True,
                    revert_reason=revert_reason,
                    simulation_url=sim_url,
                    gas_estimates=gas_estimates,  # Partial estimates
                )

            # Extract gas used - return raw value without buffer.
            # The orchestrator applies the gas buffer exactly once in _update_gas_estimate().
            gas_used = transaction_data.get("gas_used", 0)
            if isinstance(gas_used, str):
                gas_used = int(gas_used, 16) if gas_used.startswith("0x") else int(gas_used)

            if gas_used == 0:
                logger.warning(f"Transaction {i} returned 0 gas_used")
                # Use a conservative default
                gas_used = 100000

            gas_estimates.append(gas_used)

            logger.debug(f"Transaction {i}: gas_used={gas_used}")

            # Collect logs if present
            tx_logs = transaction_data.get("logs", [])
            if tx_logs:
                logs.extend(tx_logs)

            # Get simulation URL from first result
            if i == 0:
                simulation_url = self._build_simulation_url(result)

        logger.info(f"Simulation successful: {len(gas_estimates)} tx(s), gas_estimates={gas_estimates}")

        return SimulationResult(
            success=True,
            simulated=True,
            gas_estimates=gas_estimates,
            warnings=warnings,
            logs=logs,
            simulation_url=simulation_url,
        )

    def _build_simulation_url(self, result: dict[str, Any]) -> str | None:
        """Build Tenderly dashboard URL for a simulation.

        Args:
            result: Single simulation result

        Returns:
            Dashboard URL or None if not available
        """
        simulation = result.get("simulation", {})
        sim_id = simulation.get("id")

        if sim_id:
            return f"https://dashboard.tenderly.co/{self._account_slug}/{self._project_slug}/simulator/{sim_id}"

        return None


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "TenderlySimulator",
]
