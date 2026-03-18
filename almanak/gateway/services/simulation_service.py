"""SimulationService implementation - transaction simulation via Alchemy/Tenderly.

This service provides transaction simulation through external APIs:
- Tenderly: Primary for production (supports state overrides, more chains)
- Alchemy: Fallback (limited to 3 txs, 4 chains, no state overrides)

All API keys are held in the gateway, keeping credentials secure.
"""

import json
import logging
import os
import time

import aiohttp
import grpc

from almanak.framework.execution.gas.constants import (
    CHAIN_SIMULATION_BUFFERS as SIMULATION_GAS_BUFFERS,
)
from almanak.framework.execution.gas.constants import (
    DEFAULT_SIMULATION_BUFFER,
)
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Tenderly network IDs
TENDERLY_NETWORK_IDS = {
    "ethereum": "1",
    "arbitrum": "42161",
    "optimism": "10",
    "base": "8453",
    "polygon": "137",
    "avalanche": "43114",
    "bsc": "56",
    "sonic": "146",
    "plasma": "1648",
}

TENDERLY_SUPPORTED_CHAINS = set(TENDERLY_NETWORK_IDS.keys())

# Alchemy network prefixes
ALCHEMY_NETWORKS = {
    "ethereum": "eth-mainnet",
    "arbitrum": "arb-mainnet",
    "optimism": "opt-mainnet",
    "base": "base-mainnet",
}

ALCHEMY_SUPPORTED_CHAINS = set(ALCHEMY_NETWORKS.keys())
ALCHEMY_MAX_BUNDLE_SIZE = 3


class SimulationServiceServicer(gateway_pb2_grpc.SimulationServiceServicer):
    """Implements SimulationService gRPC interface.

    Provides transaction simulation via Tenderly (primary) or Alchemy (fallback).
    API credentials are managed by the gateway.
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize SimulationService.

        Args:
            settings: Gateway settings (may contain Tenderly/Alchemy credentials)
        """
        self.settings = settings
        self._http_session: aiohttp.ClientSession | None = None

        # Load credentials from settings or environment
        self._tenderly_account = getattr(settings, "tenderly_account_slug", None) or os.environ.get(
            "TENDERLY_ACCOUNT_SLUG"
        )
        self._tenderly_project = getattr(settings, "tenderly_project_slug", None) or os.environ.get(
            "TENDERLY_PROJECT_SLUG"
        )
        self._tenderly_key = getattr(settings, "tenderly_access_key", None) or os.environ.get("TENDERLY_ACCESS_KEY")
        self._alchemy_key = getattr(settings, "alchemy_api_key", None) or os.environ.get("ALCHEMY_API_KEY")

        # Determine available simulators
        self._tenderly_available = bool(self._tenderly_account and self._tenderly_project and self._tenderly_key)
        self._alchemy_available = bool(self._alchemy_key)

        logger.debug(
            "SimulationService initialized: tenderly=%s, alchemy=%s",
            self._tenderly_available,
            self._alchemy_available,
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30.0))
        return self._http_session

    async def close(self) -> None:
        """Close HTTP session."""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None

    def _select_simulator(
        self,
        chain: str,
        tx_count: int,
        has_state_overrides: bool,
        preferred: str,
    ) -> str:
        """Select the best simulator for the request.

        Args:
            chain: Chain name
            tx_count: Number of transactions
            has_state_overrides: Whether state overrides are needed
            preferred: User-preferred simulator (or empty for auto)

        Returns:
            "tenderly" or "alchemy"

        Raises:
            ValueError: If no suitable simulator is available
        """
        # If user specified, try to use it
        if preferred == "tenderly":
            if not self._tenderly_available:
                err_msg = "Tenderly not configured"
                raise ValueError(err_msg)
            if chain not in TENDERLY_SUPPORTED_CHAINS:
                err_msg = f"Tenderly does not support chain: {chain}"
                raise ValueError(err_msg)
            return "tenderly"

        if preferred == "alchemy":
            if not self._alchemy_available:
                err_msg = "Alchemy not configured"
                raise ValueError(err_msg)
            if chain not in ALCHEMY_SUPPORTED_CHAINS:
                err_msg = f"Alchemy does not support chain: {chain}"
                raise ValueError(err_msg)
            if tx_count > ALCHEMY_MAX_BUNDLE_SIZE:
                err_msg = f"Alchemy supports max {ALCHEMY_MAX_BUNDLE_SIZE} transactions"
                raise ValueError(err_msg)
            if has_state_overrides:
                err_msg = "Alchemy does not support state overrides"
                raise ValueError(err_msg)
            return "alchemy"

        # Auto-select: prefer Tenderly, fall back to Alchemy
        if self._tenderly_available and chain in TENDERLY_SUPPORTED_CHAINS:
            return "tenderly"

        if self._alchemy_available and chain in ALCHEMY_SUPPORTED_CHAINS:
            if tx_count <= ALCHEMY_MAX_BUNDLE_SIZE and not has_state_overrides:
                return "alchemy"

        # Check if any simulator could work
        if self._tenderly_available:
            if chain not in TENDERLY_SUPPORTED_CHAINS:
                err_msg = f"Chain {chain} not supported. Tenderly supports: {sorted(TENDERLY_SUPPORTED_CHAINS)}"
                raise ValueError(err_msg)
            return "tenderly"  # Even if constraints don't match, try Tenderly

        if self._alchemy_available:
            err_msg = (
                f"Chain {chain} or request constraints not supported by Alchemy. "
                f"Configure Tenderly for broader support."
            )
            raise ValueError(err_msg)

        err_msg = "No simulation backend configured. Set TENDERLY_* or ALCHEMY_API_KEY environment variables."
        raise ValueError(err_msg)

    async def _simulate_tenderly(
        self,
        chain: str,
        transactions: list[gateway_pb2.SimulateTransaction],
        state_overrides: list[gateway_pb2.SimulateStateOverride],
    ) -> gateway_pb2.SimulateBundleResponse:
        """Simulate via Tenderly API."""
        network_id = TENDERLY_NETWORK_IDS[chain]
        url = f"https://api.tenderly.co/api/v1/account/{self._tenderly_account}/project/{self._tenderly_project}/simulate-bundle"

        # Build simulations array
        simulations = []
        for tx in transactions:
            sim = {
                "network_id": network_id,
                "from": tx.from_address or "0x0000000000000000000000000000000000000000",
                "input": tx.data or "0x",
                "save": False,
            }
            # Only include "to" for non-contract-creation transactions
            # Contract creation has empty to_address and should omit the field
            if tx.to_address:
                sim["to"] = tx.to_address

            if tx.value:
                # Convert to int if needed
                value = tx.value
                try:
                    if value.startswith("0x"):
                        sim["value"] = str(int(value, 16))
                    else:
                        sim["value"] = value
                except ValueError:
                    logger.warning("Invalid transaction value format: %s, using default 0", value)
                    sim["value"] = "0"

            if tx.gas_limit > 0:
                sim["gas"] = tx.gas_limit

            # Add state overrides if provided
            if state_overrides:
                overrides = {}
                for override in state_overrides:
                    overrides[override.address] = {"balance": override.balance}
                sim["state_objects"] = overrides

            simulations.append(sim)

        payload = {"simulations": simulations}
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "X-Access-Key": self._tenderly_key or "",
        }

        session = await self._get_session()
        async with session.post(url, headers=headers, json=payload) as response:
            response_text = await response.text()

            if response.status != 200:
                logger.error("Tenderly API error: status=%d, body=%s", response.status, response_text[:500])
                return gateway_pb2.SimulateBundleResponse(
                    success=False,
                    simulated=True,
                    simulator_used="tenderly",
                    error=f"Tenderly API error: HTTP {response.status}",
                )

            try:
                data = json.loads(response_text)
            except json.JSONDecodeError as e:
                return gateway_pb2.SimulateBundleResponse(
                    success=False,
                    simulated=True,
                    simulator_used="tenderly",
                    error=f"Invalid JSON response: {e}",
                )

        # Parse results
        results = data.get("simulation_results", [])
        gas_estimates: list[int] = []
        warnings: list[str] = []
        gas_buffer = SIMULATION_GAS_BUFFERS.get(chain, DEFAULT_SIMULATION_BUFFER)

        for result in results:
            # Check for transaction failure
            tx_info = result.get("transaction", {})
            if tx_info.get("status") is False:
                error_info = tx_info.get("error_info", {})
                error_message = error_info.get("error_message", "Transaction would revert")
                return gateway_pb2.SimulateBundleResponse(
                    success=False,
                    simulated=True,
                    simulator_used="tenderly",
                    revert_reason=error_message,
                )

            # Extract gas used
            gas_used = tx_info.get("gas_used", 0)
            if gas_used == 0:
                gas_used = 100000  # Conservative default

            buffered_gas = int(gas_used * (1 + gas_buffer))
            gas_estimates.append(buffered_gas)

        # Get simulation URL if available
        simulation_url = ""
        if results and "simulation" in results[0]:
            sim_id = results[0]["simulation"].get("id", "")
            if sim_id:
                simulation_url = f"https://dashboard.tenderly.co/{self._tenderly_account}/{self._tenderly_project}/simulator/{sim_id}"

        return gateway_pb2.SimulateBundleResponse(
            success=True,
            simulated=True,
            gas_estimates=gas_estimates,
            warnings=warnings,
            simulation_url=simulation_url,
            simulator_used="tenderly",
        )

    async def _simulate_alchemy(
        self,
        chain: str,
        transactions: list[gateway_pb2.SimulateTransaction],
    ) -> gateway_pb2.SimulateBundleResponse:
        """Simulate via Alchemy API."""
        network = ALCHEMY_NETWORKS[chain]
        url = f"https://{network}.g.alchemy.com/v2/{self._alchemy_key}"

        # Build transactions array
        raw_transactions = []
        for tx in transactions:
            tx_dict = {
                "from": tx.from_address or "0x0000000000000000000000000000000000000000",
                "data": tx.data or "0x",
            }
            # Only include "to" for non-contract-creation transactions
            # Contract creation has empty to_address and should omit the field
            if tx.to_address:
                tx_dict["to"] = tx.to_address

            if tx.value:
                value = tx.value
                try:
                    if not value.startswith("0x"):
                        value = hex(int(value))
                    tx_dict["value"] = value
                except ValueError:
                    logger.warning("Invalid transaction value format: %s, using default 0x0", value)
                    tx_dict["value"] = "0x0"

            if tx.gas_limit > 0:
                tx_dict["gas"] = hex(tx.gas_limit)

            raw_transactions.append(tx_dict)

        payload = {
            "jsonrpc": "2.0",
            "method": "alchemy_simulateExecutionBundle",
            "params": [raw_transactions, "latest"],
            "id": 1,
        }

        session = await self._get_session()
        async with session.post(url, json=payload, headers={"Content-Type": "application/json"}) as response:
            response_text = await response.text()

            if response.status != 200:
                return gateway_pb2.SimulateBundleResponse(
                    success=False,
                    simulated=True,
                    simulator_used="alchemy",
                    error=f"Alchemy API error: HTTP {response.status}",
                )

            try:
                data = json.loads(response_text)
            except json.JSONDecodeError as e:
                return gateway_pb2.SimulateBundleResponse(
                    success=False,
                    simulated=True,
                    simulator_used="alchemy",
                    error=f"Invalid JSON response: {e}",
                )

        # Check for RPC-level error
        if "error" in data:
            error = data["error"]
            error_msg = error.get("message", str(error)) if isinstance(error, dict) else str(error)
            return gateway_pb2.SimulateBundleResponse(
                success=False,
                simulated=True,
                simulator_used="alchemy",
                revert_reason=error_msg,
            )

        # Parse results - one gas estimate per result (transaction)
        results = data.get("result", [])
        gas_estimates: list[int] = []
        warnings: list[str] = []
        gas_buffer = SIMULATION_GAS_BUFFERS.get(chain, DEFAULT_SIMULATION_BUFFER)

        for result in results:
            calls = result.get("calls", [])

            # Check all calls for errors first
            for call in calls:
                if "error" in call:
                    error_msg = call.get("error", "Unknown error")
                    revert_reason = call.get("revertReason", error_msg)
                    return gateway_pb2.SimulateBundleResponse(
                        success=False,
                        simulated=True,
                        simulator_used="alchemy",
                        revert_reason=revert_reason,
                    )

            # Use result-level gas or first call's gas (top-level transaction)
            # Only one gas estimate per result, not per call
            gas_used = 0
            if "gasUsed" in result:
                gas_used_hex = result.get("gasUsed", "0x0")
                gas_used = int(gas_used_hex, 16) if gas_used_hex.startswith("0x") else int(gas_used_hex)
            elif calls:
                # Use only the first call (top-level transaction gas)
                gas_used_hex = calls[0].get("gasUsed", "0x0")
                gas_used = int(gas_used_hex, 16) if gas_used_hex.startswith("0x") else int(gas_used_hex)

            if gas_used == 0:
                gas_used = 100000  # Conservative default

            buffered_gas = int(gas_used * (1 + gas_buffer))
            gas_estimates.append(buffered_gas)

        return gateway_pb2.SimulateBundleResponse(
            success=True,
            simulated=True,
            gas_estimates=gas_estimates,
            warnings=warnings,
            simulator_used="alchemy",
        )

    async def SimulateBundle(
        self,
        request: gateway_pb2.SimulateBundleRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SimulateBundleResponse:
        """Simulate a bundle of transactions.

        Args:
            request: Simulation request with transactions and chain
            context: gRPC context

        Returns:
            SimulateBundleResponse with gas estimates and success status
        """
        chain = request.chain.lower()
        transactions = list(request.transactions)
        state_overrides = list(request.state_overrides)
        preferred_simulator = request.simulator.lower() if request.simulator else ""

        if not transactions:
            return gateway_pb2.SimulateBundleResponse(
                success=True,
                simulated=False,
                simulator_used="none",
            )

        start_time = time.time()

        try:
            # Select simulator
            simulator = self._select_simulator(
                chain=chain,
                tx_count=len(transactions),
                has_state_overrides=bool(state_overrides),
                preferred=preferred_simulator,
            )

            logger.info(
                "Simulating %d transaction(s) on %s via %s",
                len(transactions),
                chain,
                simulator,
            )

            # Run simulation
            if simulator == "tenderly":
                result = await self._simulate_tenderly(chain, transactions, state_overrides)
            else:
                result = await self._simulate_alchemy(chain, transactions)

            latency = time.time() - start_time
            logger.info(
                "Simulation completed in %.2fms: success=%s, simulator=%s",
                latency * 1000,
                result.success,
                simulator,
            )

            return result

        except ValueError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SimulateBundleResponse(
                success=False,
                simulated=False,
                error=str(e),
            )
        except Exception as e:
            logger.exception("Simulation failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.SimulateBundleResponse(
                success=False,
                simulated=False,
                error=str(e),
            )
