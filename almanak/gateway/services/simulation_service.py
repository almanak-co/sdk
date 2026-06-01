"""SimulationService implementation - transaction simulation via Alchemy/Tenderly.

This service exposes the gRPC ``SimulateBundle`` RPC. Since VIB-4851 it does NOT
reimplement Tenderly/Alchemy HTTP egress: it **delegates** to the single
framework simulator hierarchy
(``almanak.framework.execution.simulator.{tenderly,alchemy}``). That hierarchy
owns the one and only egress implementation and the one and only
chain -> network-id map (``almanak.framework.execution.simulator.config``).

The servicer keeps three gateway-local responsibilities:

* ``_select_simulator`` — the tenderly-vs-alchemy decision logic and the exact
  ``ValueError`` strings that map to gRPC ``INVALID_ARGUMENT``. Its membership
  sets are now sourced from the framework config (14 EVM chains), not a stale
  local copy.
* ``_simulation_gas_buffer_for`` — the per-chain post-simulation gas buffer. The
  framework simulators return RAW gas; the gateway re-applies the buffer here
  because there is no execution orchestrator on this path.
* TLS — the gateway threads its certifi-backed ``ssl.SSLContext`` into the
  framework simulators so hosted egress keeps consistent certificate
  verification. The framework does not import gateway code; the context is
  passed in.

All API keys are held in the gateway, keeping credentials secure.
"""

import logging
import time

import grpc

from almanak.core.chains import ChainRegistry
from almanak.framework.execution.gas.constants import DEFAULT_SIMULATION_BUFFER
from almanak.framework.execution.interfaces import (
    SimulationResult,
    TransactionType,
    UnsignedTransaction,
)
from almanak.framework.execution.simulator.alchemy import AlchemySimulator
from almanak.framework.execution.simulator.config import (
    ALCHEMY_MAX_BUNDLE_SIZE,
    ALCHEMY_SUPPORTED_CHAINS,
    TENDERLY_SUPPORTED_CHAINS,
)
from almanak.framework.execution.simulator.tenderly import TenderlySimulator
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)


def _simulation_gas_buffer_for(chain: str) -> float:
    """Return the per-chain simulation gas buffer, or ``DEFAULT_SIMULATION_BUFFER``.

    Thin wrapper around :class:`ChainRegistry.try_resolve` (VIB-4801). The
    framework simulators return RAW gas; the gateway re-applies this buffer in
    ``_result_to_response`` because no execution orchestrator runs on the gRPC
    simulation path. Uses ``descriptor.gas.simulation_buffer`` (NOT
    ``gas.buffer`` — that multiplier belongs to the execution orchestrator).
    """
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None or descriptor.gas.simulation_buffer is None:
        return DEFAULT_SIMULATION_BUFFER
    return descriptor.gas.simulation_buffer


def _parse_value(value: str) -> int:
    """Parse a proto tx value (hex ``0x…`` or decimal) into wei; 0 on empty/invalid.

    Mirrors the historical gateway value handling so an unparseable value never
    bubbles a ``ValueError`` out of ``SimulateBundle``.
    """
    if not value:
        return 0
    try:
        return int(value, 16) if value.startswith("0x") else int(value)
    except ValueError:
        logger.warning("Invalid transaction value format: %s, using default 0", value)
        return 0


class SimulationServiceServicer(gateway_pb2_grpc.SimulationServiceServicer):
    """Implements SimulationService gRPC interface.

    Provides transaction simulation via Tenderly (primary) or Alchemy (fallback)
    by delegating to the framework simulator hierarchy. API credentials are
    managed by the gateway.
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize SimulationService.

        Args:
            settings: Gateway settings (may contain Tenderly/Alchemy credentials)
        """
        self.settings = settings

        # Boot-time settings are resolved through the config service; the
        # servicer consumes that typed slice rather than reparsing env.
        self._tenderly_account = getattr(settings, "tenderly_account_slug", None)
        self._tenderly_project = getattr(settings, "tenderly_project_slug", None)
        self._tenderly_key = getattr(settings, "tenderly_access_key", None)
        self._alchemy_key = getattr(settings, "alchemy_api_key", None)

        # Determine available simulators
        self._tenderly_available = bool(self._tenderly_account and self._tenderly_project and self._tenderly_key)
        self._alchemy_available = bool(self._alchemy_key)

        # Cached framework simulators (lazily built, reused across calls). The
        # gateway threads its certifi SSL context in; the framework simulators
        # own their own aiohttp sessions, so the servicer holds no session.
        self._tenderly_sim: TenderlySimulator | None = None
        self._alchemy_sim: AlchemySimulator | None = None

        logger.debug(
            "SimulationService initialized: tenderly=%s, alchemy=%s",
            self._tenderly_available,
            self._alchemy_available,
        )

    async def close(self) -> None:
        """No-op close for shutdown symmetry.

        The framework simulators own (and close) their own aiohttp sessions per
        request, so the servicer holds no long-lived HTTP resource. Kept because
        ``server.py`` shutdown calls ``close()`` on every gateway-owned servicer.
        """
        return None

    def _framework_simulator_for(self, chosen: str) -> TenderlySimulator | AlchemySimulator:
        """Build (and cache) the framework simulator for the chosen backend.

        The gateway's certifi-backed ``ssl.SSLContext`` is threaded in so hosted
        egress keeps consistent certificate verification. ``build_ssl_context()``
        is only invoked when a simulator is actually constructed (it is itself
        ``lru_cache``-backed, but the cached simulators make even that lookup
        unnecessary on the hot path).
        """
        if chosen == "tenderly":
            if self._tenderly_sim is None:
                self._tenderly_sim = TenderlySimulator(
                    account_slug=self._tenderly_account or "",
                    project_slug=self._tenderly_project or "",
                    access_key=self._tenderly_key or "",
                    ssl_context=build_ssl_context(),
                )
            return self._tenderly_sim

        if self._alchemy_sim is None:
            self._alchemy_sim = AlchemySimulator(
                api_key=self._alchemy_key or "",
                ssl_context=build_ssl_context(),
            )
        return self._alchemy_sim

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

    def _proto_to_unsigned(
        self,
        tx: gateway_pb2.SimulateTransaction,
        chain: str,
    ) -> UnsignedTransaction:
        """Convert a ``SimulateTransaction`` proto into a framework ``UnsignedTransaction``.

        Only ``to`` / ``from`` / ``value`` / ``data`` reach the outbound
        Tenderly/Alchemy payload — the framework simulators deliberately omit
        gas and fee fields (PR #817: let the API estimate gas freely). A legacy
        tx_type with ``gas_price=0`` and a positive sentinel ``gas_limit`` is
        therefore used purely to satisfy ``UnsignedTransaction`` validation;
        neither value leaves the process.
        """
        descriptor = ChainRegistry.resolve(chain)
        return UnsignedTransaction(
            to=tx.to_address or None,
            value=_parse_value(tx.value),
            data=tx.data or "0x",
            chain_id=descriptor.chain_id,
            gas_limit=tx.gas_limit if tx.gas_limit > 0 else 1,
            from_address=tx.from_address or None,
            tx_type=TransactionType.LEGACY,
            gas_price=0,
        )

    def _result_to_response(
        self,
        result: SimulationResult,
        chain: str,
        simulator_used: str,
    ) -> gateway_pb2.SimulateBundleResponse:
        """Map a framework ``SimulationResult`` onto a ``SimulateBundleResponse``.

        Re-applies the per-chain simulation gas buffer because the framework
        returns RAW gas and there is no execution orchestrator on this path.
        """
        if not result.simulated:
            return gateway_pb2.SimulateBundleResponse(
                success=True,
                simulated=False,
                simulator_used="none",
            )

        if not result.success:
            return gateway_pb2.SimulateBundleResponse(
                success=False,
                simulated=True,
                simulator_used=simulator_used,
                revert_reason=result.revert_reason or "",
            )

        gas_buffer = _simulation_gas_buffer_for(chain)
        gas_estimates = [int(g * (1 + gas_buffer)) for g in result.gas_estimates]
        return gateway_pb2.SimulateBundleResponse(
            success=True,
            simulated=True,
            simulator_used=simulator_used,
            gas_estimates=gas_estimates,
            warnings=list(result.warnings or []),
            simulation_url=result.simulation_url or "",
        )

    async def SimulateBundle(
        self,
        request: gateway_pb2.SimulateBundleRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SimulateBundleResponse:
        """Simulate a bundle of transactions by delegating to the framework simulators.

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
            chosen = self._select_simulator(
                chain=chain,
                tx_count=len(transactions),
                has_state_overrides=bool(state_overrides),
                preferred=preferred_simulator,
            )

            logger.info(
                "Simulating %d transaction(s) on %s via %s",
                len(transactions),
                chain,
                chosen,
            )

            simulator = self._framework_simulator_for(chosen)
            txs = [self._proto_to_unsigned(tx, chain) for tx in transactions]
            overrides = {o.address: {"balance": o.balance} for o in state_overrides}
            result = await simulator.simulate(txs, chain, state_overrides=overrides or None)
            response = self._result_to_response(result, chain, chosen)

            latency = time.time() - start_time
            logger.info(
                "Simulation completed in %.2fms: success=%s, simulator=%s",
                latency * 1000,
                response.success,
                chosen,
            )

            return response

        except ValueError as e:
            # Selector / chain-resolution failures -> caller error.
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SimulateBundleResponse(
                success=False,
                simulated=False,
                error=str(e),
            )
        except Exception as e:
            # Infrastructure failures (incl. framework SimulationError raised on
            # HTTP non-200 / bad JSON / Alchemy JSON-RPC errors) -> INTERNAL.
            logger.exception("Simulation failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.SimulateBundleResponse(
                success=False,
                simulated=False,
                error=str(e),
            )
