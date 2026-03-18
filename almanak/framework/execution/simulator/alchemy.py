"""Alchemy Simulator - Fallback transaction simulation via Alchemy RPC API.

This module provides transaction simulation using the Alchemy simulateExecutionBundle
RPC method as a fallback when Tenderly is unavailable.

Key Limitations:
    - Maximum 3 transactions per bundle
    - Only 4 chains supported (ethereum, arbitrum, optimism, base)
    - No state override support (cannot properly simulate SAFE wallets)

Use Tenderly as primary for production. Alchemy is best for:
    - Development/testing when Tenderly is not configured
    - Fallback when Tenderly API is down
    - Simple EOA transactions (not SAFE wallet)

API Documentation:
    https://docs.alchemy.com/reference/alchemy-simulateexecutionbundle

Example:
    simulator = AlchemySimulator(api_key="xxx")

    result = await simulator.simulate(
        txs=[unsigned_tx],
        chain="arbitrum",
    )

    if result.success:
        print(f"Gas estimates: {result.gas_estimates}")
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
    ALCHEMY_MAX_BUNDLE_SIZE,
    ALCHEMY_NETWORKS,
    ALCHEMY_SUPPORTED_CHAINS,
)

logger = logging.getLogger(__name__)


class AlchemySimulator(Simulator):
    """Transaction simulation via Alchemy simulateExecutionBundle API.

    This simulator uses Alchemy's bundle simulation RPC method as a fallback
    when Tenderly is unavailable. It has several limitations compared to
    Tenderly but provides good coverage for simple use cases.

    Limitations:
        - Maximum 3 transactions per bundle
        - Only 4 chains: ethereum, arbitrum, optimism, base
        - No state override support (SAFE wallets may fail simulation)

    When to use Alchemy vs Tenderly:
        - Use Tenderly for: SAFE wallets, >3 tx bundles, more chains
        - Use Alchemy for: Simple EOA transactions, Tenderly outage

    Attributes:
        api_key: Alchemy API key
        timeout_seconds: Request timeout

    Example:
        simulator = AlchemySimulator(api_key="xxx")

        # Simple simulation
        result = await simulator.simulate([tx], chain="arbitrum")

        # Check limits before simulating
        if len(txs) <= 3 and chain in ALCHEMY_SUPPORTED_CHAINS:
            result = await simulator.simulate(txs, chain)
    """

    def __init__(
        self,
        api_key: str,
        timeout_seconds: float = 10.0,
        name: str = "alchemy",
    ) -> None:
        """Initialize the AlchemySimulator.

        Args:
            api_key: Alchemy API key
            timeout_seconds: Request timeout (default 10s)
            name: Simulator name for logging (default "alchemy")

        Raises:
            ValueError: If api_key is missing
        """
        if not api_key:
            raise ValueError("Alchemy api_key is required")

        self._api_key = api_key
        self._timeout = timeout_seconds
        self._name = name

        logger.info(
            f"AlchemySimulator initialized: timeout={timeout_seconds}s, "
            f"supported_chains={sorted(ALCHEMY_SUPPORTED_CHAINS)}"
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
            True if Alchemy supports this chain
        """
        return chain.lower() in ALCHEMY_SUPPORTED_CHAINS

    def supports_bundle_size(self, tx_count: int) -> bool:
        """Check if the bundle size is within Alchemy's limit.

        Args:
            tx_count: Number of transactions

        Returns:
            True if within the 3-transaction limit
        """
        return tx_count <= ALCHEMY_MAX_BUNDLE_SIZE

    async def simulate(
        self,
        txs: list[UnsignedTransaction],
        chain: str,
        state_overrides: dict[str, Any] | None = None,
    ) -> SimulationResult:
        """Simulate transactions via Alchemy API.

        This method simulates the execution of transactions using Alchemy's
        simulateExecutionBundle RPC method.

        Args:
            txs: List of unsigned transactions to simulate (max 3)
            chain: Chain name (must be supported by Alchemy)
            state_overrides: NOT SUPPORTED - will log warning if provided

        Returns:
            SimulationResult with gas estimates and success status

        Raises:
            SimulationError: For infrastructure failures or unsupported params
        """
        if not txs:
            logger.warning("No transactions to simulate")
            return SimulationResult(success=True, simulated=False)

        chain_lower = chain.lower()

        # Validate chain is supported
        if not self.supports_chain(chain_lower):
            raise SimulationError(
                reason=f"Alchemy does not support chain: {chain}. Supported: {sorted(ALCHEMY_SUPPORTED_CHAINS)}",
                recoverable=False,
            )

        # Validate bundle size
        if not self.supports_bundle_size(len(txs)):
            raise SimulationError(
                reason=f"Alchemy supports max {ALCHEMY_MAX_BUNDLE_SIZE} transactions per bundle, got {len(txs)}",
                recoverable=False,
            )

        # Warn if state_overrides provided (not supported)
        if state_overrides:
            logger.warning(
                "Alchemy does not support state overrides. SAFE wallet "
                "simulations may fail with balance errors. Consider using Tenderly."
            )

        network = ALCHEMY_NETWORKS[chain_lower]

        logger.info(
            f"Simulating {len(txs)} transaction(s) on {chain} via Alchemy",
            extra={
                "chain": chain,
                "network": network,
                "tx_count": len(txs),
            },
        )

        try:
            return await self._simulate_bundle(
                txs=txs,
                network=network,
                chain=chain_lower,
            )
        except aiohttp.ClientError as e:
            logger.error(f"Alchemy API connection error: {e}")
            raise SimulationError(
                reason=f"Alchemy API connection failed: {e}",
                recoverable=True,
            ) from e
        except TimeoutError as e:
            logger.error(f"Alchemy API timeout after {self._timeout}s")
            raise SimulationError(
                reason=f"Alchemy API timeout after {self._timeout} seconds",
                recoverable=True,
            ) from e

    async def _simulate_bundle(
        self,
        txs: list[UnsignedTransaction],
        network: str,
        chain: str,
    ) -> SimulationResult:
        """Execute bundle simulation via Alchemy RPC API.

        Args:
            txs: Transactions to simulate
            network: Alchemy network identifier
            chain: Chain name for gas buffer lookup

        Returns:
            SimulationResult with gas estimates
        """
        url = f"https://{network}.g.alchemy.com/v2/{self._api_key}"

        # Build transaction array for Alchemy
        raw_transactions = []
        for tx in txs:
            tx_dict = {
                "from": tx.from_address or "0x0000000000000000000000000000000000000000",
                "to": tx.to or "0x0000000000000000000000000000000000000000",
                "data": tx.data or "0x",
            }

            # Add value if non-zero
            if tx.value and tx.value > 0:
                tx_dict["value"] = hex(tx.value)

            # Do NOT send gas_limit to Alchemy. Let it estimate the actual
            # gas used. The orchestrator applies the buffer afterward.
            # Sending hardcoded estimates as caps causes false "out of gas"
            # on chains with unusual gas models (e.g. Mantle).

            raw_transactions.append(tx_dict)

        payload = {
            "jsonrpc": "2.0",
            "method": "alchemy_simulateExecutionBundle",
            "params": [raw_transactions, "latest"],
            "id": 1,
        }

        headers = {"Content-Type": "application/json"}

        logger.debug(f"Alchemy request URL: {url[:50]}...")
        logger.debug(f"Alchemy payload: {json.dumps(payload, indent=2)}")

        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=self._timeout),
            ) as response:
                response_text = await response.text()

                logger.debug(f"Alchemy response status: {response.status}")

                if response.status != 200:
                    logger.error(f"Alchemy API error: status={response.status}, body={response_text[:500]}")
                    raise SimulationError(
                        reason=f"Alchemy API error: HTTP {response.status}",
                        recoverable=response.status >= 500,
                    )

                try:
                    response_json = json.loads(response_text)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse Alchemy response: {e}")
                    raise SimulationError(
                        reason="Invalid JSON response from Alchemy",
                        recoverable=False,
                    ) from e

        return self._parse_response(response_json, chain, len(txs))

    def _parse_response(
        self,
        response: dict[str, Any],
        chain: str,
        expected_count: int,
    ) -> SimulationResult:
        """Parse Alchemy simulation response.

        Args:
            response: Raw RPC response
            chain: Chain name for gas buffer
            expected_count: Expected number of results

        Returns:
            SimulationResult with parsed data
        """
        # Check for RPC-level error
        if "error" in response:
            error = response["error"]
            error_msg = error.get("message", str(error)) if isinstance(error, dict) else str(error)
            logger.error(f"Alchemy RPC error: {error_msg}")
            return SimulationResult(
                success=False,
                simulated=True,
                revert_reason=error_msg,
            )

        # Get result array
        results = response.get("result")
        if results is None:
            logger.error(f"Missing result in Alchemy response: {list(response.keys())}")
            raise SimulationError(
                reason="Alchemy response missing result",
                recoverable=False,
            )

        if not isinstance(results, list):
            logger.error(f"Unexpected result type: {type(results)}")
            raise SimulationError(
                reason="Alchemy response result is not an array",
                recoverable=False,
            )

        # Check for top-level simulation error
        if len(results) > 0 and "error" in results[0]:
            error_msg = results[0].get("error", "Unknown error")
            logger.error(f"Alchemy simulation error: {error_msg}")
            return SimulationResult(
                success=False,
                simulated=True,
                revert_reason=str(error_msg),
            )

        # Parse each transaction result
        gas_estimates: list[int] = []
        warnings: list[str] = []
        logs: list[dict[str, Any]] = []

        for i, result in enumerate(results):
            # Check for transaction-level error
            calls = result.get("calls", [])
            if not calls:
                logger.warning(f"Transaction {i}: missing calls array")
                # Alchemy sometimes returns results without calls on error
                if "error" in result:
                    return SimulationResult(
                        success=False,
                        simulated=True,
                        revert_reason=str(result["error"]),
                    )
                continue

            # Check each call for errors
            for call in calls:
                if "error" in call:
                    error_msg = call.get("error", "Unknown error")
                    revert_reason = call.get("revertReason", error_msg)

                    # Check if this is a token balance error (common with SAFE wallets)
                    is_balance_error = self._is_token_balance_error(error_msg, revert_reason)

                    if is_balance_error:
                        warnings.append(f"Transaction {i}: Token balance error (may be SAFE wallet issue)")
                        logger.warning(
                            f"Transaction {i}: Token balance error - this may be "
                            "expected for SAFE wallet simulations. Use Tenderly "
                            "with state_overrides for accurate SAFE simulation."
                        )
                    else:
                        # Real error - transaction would revert
                        logger.warning(f"Transaction {i} would revert: {revert_reason}")
                        return SimulationResult(
                            success=False,
                            simulated=True,
                            revert_reason=revert_reason,
                        )

                # Extract gas used - return raw value without buffer.
                # The orchestrator applies the gas buffer exactly once in _update_gas_estimate().
                gas_used_hex = call.get("gasUsed", "0x0")
                gas_used = int(gas_used_hex, 16) if gas_used_hex.startswith("0x") else int(gas_used_hex)

                if gas_used == 0:
                    logger.warning(f"Transaction {i}: returned 0 gas_used")
                    gas_used = 100000  # Conservative default

                gas_estimates.append(gas_used)

                logger.debug(f"Transaction {i}: gas_used={gas_used}")

                # Collect logs
                call_logs = call.get("logs", [])
                if call_logs:
                    logs.extend(call_logs)

        # Verify we got estimates for all transactions
        if len(gas_estimates) != expected_count:
            logger.warning(f"Gas estimate count mismatch: expected {expected_count}, got {len(gas_estimates)}")

        logger.info(f"Simulation successful: {len(gas_estimates)} tx(s), gas_estimates={gas_estimates}")

        return SimulationResult(
            success=True,
            simulated=True,
            gas_estimates=gas_estimates,
            warnings=warnings,
            logs=logs,
        )

    def _is_token_balance_error(self, error_msg: str, revert_reason: str) -> bool:
        """Check if an error is a token balance error.

        Token balance errors are common when simulating SAFE wallet transactions
        via Alchemy (which doesn't support state overrides). The tokens are in
        the SAFE wallet, but simulation runs from EOA.

        Args:
            error_msg: Error message from call
            revert_reason: Revert reason if available

        Returns:
            True if this appears to be a token balance error
        """
        balance_patterns = [
            "transfer amount exceeds balance",
            "ERC20: transfer amount exceeds balance",
            "STF",  # Safe Transfer Failed
            "insufficient balance",
        ]

        error_lower = error_msg.lower()
        revert_lower = revert_reason.lower()

        for pattern in balance_patterns:
            pattern_lower = pattern.lower()
            if pattern_lower in error_lower or pattern_lower in revert_lower:
                return True

        return False


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "AlchemySimulator",
]
