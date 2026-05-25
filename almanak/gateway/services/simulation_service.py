"""SimulationService implementation - transaction simulation via Alchemy/Tenderly.

This service provides transaction simulation through external APIs:
- Tenderly: Primary for production (supports state overrides, more chains)
- Alchemy: Fallback (limited to 3 txs, 4 chains, no state overrides)

All API keys are held in the gateway, keeping credentials secure.
"""

import json
import logging
import time

import aiohttp
import grpc

from almanak.core.chains import ChainRegistry
from almanak.framework.execution.gas.constants import DEFAULT_SIMULATION_BUFFER
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)


def _simulation_gas_buffer_for(chain: str) -> float:
    """Return the per-chain simulation gas buffer, or ``DEFAULT_SIMULATION_BUFFER``.

    Thin wrapper around :class:`ChainRegistry.try_resolve` extracted so the
    descriptor lookup does not add branches inside the already-complex
    ``_parse_alchemy_results`` / ``_simulate_tenderly`` call sites (VIB-4801:
    replaces the legacy ``SIMULATION_GAS_BUFFERS.get(chain, DEFAULT)`` lookup
    without a cc bump).
    """
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None or descriptor.gas.simulation_buffer is None:
        return DEFAULT_SIMULATION_BUFFER
    return descriptor.gas.simulation_buffer


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

_ALCHEMY_DEFAULT_GAS_USED = 100_000
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


# =============================================================================
# Alchemy helpers (module-private; pure unless noted)
# =============================================================================


def _normalize_alchemy_value(value: str) -> str:
    """Normalize a tx value string to 0x-prefixed hex; default '0x0' on parse failure."""
    try:
        return value if value.startswith("0x") else hex(int(value))
    except ValueError:
        logger.warning("Invalid transaction value format: %s, using default 0x0", value)
        return "0x0"


def _build_alchemy_tx_dict(tx: gateway_pb2.SimulateTransaction) -> dict:
    """Convert a single SimulateTransaction proto into Alchemy's tx dict."""
    tx_dict: dict = {
        "from": tx.from_address or _ZERO_ADDRESS,
        "data": tx.data or "0x",
    }
    # Only include "to" for non-contract-creation transactions
    # Contract creation has empty to_address and should omit the field
    if tx.to_address:
        tx_dict["to"] = tx.to_address
    if tx.value:
        tx_dict["value"] = _normalize_alchemy_value(tx.value)
    if tx.gas_limit > 0:
        tx_dict["gas"] = hex(tx.gas_limit)
    return tx_dict


def _build_alchemy_payload(transactions: list[gateway_pb2.SimulateTransaction]) -> dict:
    """Build the alchemy_simulateExecutionBundle JSON-RPC payload."""
    return {
        "jsonrpc": "2.0",
        "method": "alchemy_simulateExecutionBundle",
        "params": [[_build_alchemy_tx_dict(tx) for tx in transactions], "latest"],
        "id": 1,
    }


def _parse_hex_or_int(value: object) -> int:
    """Parse a string that is either 0x-prefixed hex or decimal; returns 0 on bad input.

    Defensive against malformed Alchemy responses (e.g. ``"gasUsed": "bogus"`` or a
    non-string value) — these would otherwise raise ``ValueError`` / ``TypeError`` and
    bubble out of ``_parse_alchemy_results`` into ``SimulateBundle``'s generic
    ``except Exception`` handler, which maps to gRPC ``INTERNAL`` and obscures the
    real shape problem.
    """
    if not isinstance(value, str):
        return 0
    try:
        return int(value, 16) if value.startswith("0x") else int(value)
    except ValueError:
        return 0


def _extract_alchemy_gas_used(result: dict) -> int:
    """Extract gas used from one Alchemy result; returns 0 if absent or malformed."""
    if "gasUsed" in result:
        return _parse_hex_or_int(result.get("gasUsed", "0x0"))
    calls = result.get("calls")
    if isinstance(calls, list) and calls and isinstance(calls[0], dict):
        # Use only the first call (top-level transaction gas).
        return _parse_hex_or_int(calls[0].get("gasUsed", "0x0"))
    return 0


def _find_alchemy_call_error(result: dict) -> str | None:
    """Return the revert reason from the first failing call, or None on malformed shape.

    Important subtlety: ``call.get("revertReason", default)`` returns ``None`` when
    the field is *present and explicitly null* (only returns ``default`` when the key
    is *absent*). Alchemy can and does emit ``"revertReason": null`` alongside an
    ``"error"`` field. Without explicit-null handling, the caller's
    ``revert_reason is not None`` check would skip the error branch and report the
    failed tx as ``success=True``.
    """
    calls = result.get("calls")
    if not isinstance(calls, list):
        return None
    for call in calls:
        if not isinstance(call, dict):
            continue
        if "error" in call:
            # Prefer revertReason if it's a non-empty string. Fall back to error.
            revert_reason = call.get("revertReason")
            if isinstance(revert_reason, str) and revert_reason:
                return revert_reason
            error_msg = call.get("error", "Unknown error")
            # Coerce non-string error payloads (dict / int / None) so the gRPC
            # proto field stays a clean string, not a leaked Python repr.
            return error_msg if isinstance(error_msg, str) else str(error_msg)
    return None


def _alchemy_rpc_error_response(data: dict) -> gateway_pb2.SimulateBundleResponse:
    """Build an error response for a JSON-RPC ``error`` payload."""
    error = data["error"]
    error_msg = error.get("message", str(error)) if isinstance(error, dict) else str(error)
    return gateway_pb2.SimulateBundleResponse(
        success=False,
        simulated=True,
        simulator_used="alchemy",
        revert_reason=error_msg,
    )


def _parse_alchemy_results(data: dict, chain: str) -> gateway_pb2.SimulateBundleResponse:
    """Convert a parsed Alchemy response into a SimulateBundleResponse.

    HTTP 200 with neither a JSON-RPC ``error`` nor a non-empty ``result`` list is treated
    as a malformed upstream response, not a green simulation — otherwise an Alchemy
    protocol failure would be reported back to callers as ``success=True`` with empty
    ``gas_estimates``.
    """
    if "error" in data:
        return _alchemy_rpc_error_response(data)

    results = data.get("result")
    if not isinstance(results, list) or not results:
        return gateway_pb2.SimulateBundleResponse(
            success=False,
            simulated=True,
            simulator_used="alchemy",
            error="Malformed Alchemy response: missing or empty result list",
        )

    gas_buffer = _simulation_gas_buffer_for(chain)
    gas_estimates: list[int] = []

    for result in results:
        if not isinstance(result, dict):
            return gateway_pb2.SimulateBundleResponse(
                success=False,
                simulated=True,
                simulator_used="alchemy",
                error="Malformed Alchemy response: invalid result item",
            )
        revert_reason = _find_alchemy_call_error(result)
        if revert_reason is not None:
            return gateway_pb2.SimulateBundleResponse(
                success=False,
                simulated=True,
                simulator_used="alchemy",
                revert_reason=revert_reason,
            )
        gas_used = _extract_alchemy_gas_used(result) or _ALCHEMY_DEFAULT_GAS_USED
        gas_estimates.append(int(gas_used * (1 + gas_buffer)))

    return gateway_pb2.SimulateBundleResponse(
        success=True,
        simulated=True,
        gas_estimates=gas_estimates,
        simulator_used="alchemy",
    )


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

        # Boot-time settings are resolved through the config service; the
        # servicer consumes that typed slice rather than reparsing env.
        self._tenderly_account = getattr(settings, "tenderly_account_slug", None)
        self._tenderly_project = getattr(settings, "tenderly_project_slug", None)
        self._tenderly_key = getattr(settings, "tenderly_access_key", None)
        self._alchemy_key = getattr(settings, "alchemy_api_key", None)

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
            connector = aiohttp.TCPConnector(ssl=build_ssl_context())
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30.0),
                connector=connector,
            )
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

    # crap-allowlist: VIB-4801 mechanical SIMULATION_GAS_BUFFERS -> ChainRegistry cutover in pre-existing high-CRAP function (cc preserved at 20 by extracting _simulation_gas_buffer_for). Function was already over threshold on main (CRAP~396 at cc=20 / cov=2%); coverage of the broader function is a separate hosted-only gateway test gap (Tenderly API mocking) tracked in VIB-4079.
    async def _simulate_tenderly(  # noqa: C901
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
        gas_buffer = _simulation_gas_buffer_for(chain)

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
        url = f"https://{ALCHEMY_NETWORKS[chain]}.g.alchemy.com/v2/{self._alchemy_key}"
        payload = _build_alchemy_payload(transactions)

        data_or_response = await self._post_alchemy_simulate(url, payload)
        if isinstance(data_or_response, gateway_pb2.SimulateBundleResponse):
            return data_or_response

        return _parse_alchemy_results(data_or_response, chain)

    async def _post_alchemy_simulate(
        self,
        url: str,
        payload: dict,
    ) -> dict | gateway_pb2.SimulateBundleResponse:
        """POST to Alchemy and decode JSON.

        Returns the parsed dict on success, or an error SimulateBundleResponse
        on HTTP non-200 / JSON decode failure.
        """
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
                return json.loads(response_text)
            except json.JSONDecodeError as e:
                return gateway_pb2.SimulateBundleResponse(
                    success=False,
                    simulated=True,
                    simulator_used="alchemy",
                    error=f"Invalid JSON response: {e}",
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
