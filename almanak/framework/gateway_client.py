"""Client for communicating with the gateway sidecar.

This module provides the GatewayClient class that strategy containers use
to communicate with the gateway. All external access (market data, state,
execution) goes through this client.
"""

import logging
from dataclasses import dataclass
from typing import Any

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc

from almanak.connectors._strategy_base.gateway_stub_registry import GatewayStubRegistry
from almanak.gateway.proto import gateway_pb2_grpc

logger = logging.getLogger(__name__)

# Metadata key for authentication token (matches server-side auth.py)
AUTH_METADATA_KEY = "authorization"


def _encode_block_tag(block: int | str | None) -> str:
    """Encode an optional block reference for a typed-query proto ``block`` field.

    VIB-5140 (+ VIB-5148 Layer-2): the typed RPC queries (QueryAllowance /
    QueryBalance / QueryPositionLiquidity / QueryPositionTokensOwed /
    QueryV4PositionState) carry block as a ``string`` (proto3 default
    ``""``). This helper mirrors
    :meth:`GatewayClient.eth_call`'s JSON-RPC block encoding so the gateway
    handler receives a wire-ready tag:

    - ``None`` → ``""`` (proto default; the handler maps ``""`` → ``"latest"``,
      preserving exactly the pre-VIB-5140 behaviour for every caller that
      omits ``block``).
    - ``int`` → ``hex(N)`` (the standard JSON-RPC block-number form). ``bool``
      is rejected (it is an ``int`` subclass but never a valid block), and a
      negative int is rejected — both surface a caller bug loudly rather than
      silently degrading to ``"latest"`` and re-opening the stale-read race.
    - ``str`` → passed through unchanged (covers ``"latest"`` / ``"pending"`` /
      ``"safe"`` / ``"finalized"`` and pre-encoded ``"0x..."`` hex).
    - any other type → ``TypeError`` (fail loudly here rather than letting a
      bad value reach proto serialization, which raises a cryptic gRPC error
      far from the offending call site).

    Post-transaction reads (the teardown closure verifier) MUST pass the
    confirmed receipt's ``block_number`` so the call cannot race a read
    replica that trails the writer by a block and return PRE-tx state.
    """
    if block is None:
        return ""
    if isinstance(block, bool):
        raise ValueError(f"block must not be bool, got {block!r}")
    if isinstance(block, int):
        if block < 0:
            raise ValueError(f"block must be non-negative, got {block}")
        return hex(block)
    if isinstance(block, str):
        return block
    raise TypeError(f"block must be int, str, or None, got {type(block).__name__}")


@dataclass(frozen=True)
class V4PositionState:
    """Live on-chain state of a Uniswap V4 LP position (VIB-5024).

    Read via the gateway ``QueryV4PositionState`` RPC. All amounts are integers
    (wei / raw tick units) — exactly what ``value_lp_position`` consumes for an
    exact, HIGH-confidence concentrated-liquidity valuation.
    """

    liquidity: int
    tick_lower: int
    tick_upper: int
    current_tick: int
    sqrt_price_x96: int
    pool_id: str
    tokens_owed0: int = 0
    tokens_owed1: int = 0


class _AuthClientInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """Client interceptor that adds authentication token to all RPC calls.

    This interceptor adds the auth token to the metadata of every outgoing
    request, allowing the gateway server to authenticate the client.
    """

    def __init__(self, token: str):
        """Initialize the auth client interceptor.

        Args:
            token: The authentication token to add to requests.
        """
        self._token = token

    def _add_auth_metadata(self, metadata: tuple | None) -> tuple:
        """Add auth token to metadata.

        Args:
            metadata: Existing metadata tuple (may be None)

        Returns:
            Updated metadata tuple with auth token
        """
        metadata = metadata or ()
        return metadata + ((AUTH_METADATA_KEY, self._token),)

    def intercept_unary_unary(self, continuation, client_call_details, request):
        """Intercept unary-unary calls to add auth metadata."""
        new_details = _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            self._add_auth_metadata(client_call_details.metadata),
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )
        return continuation(new_details, request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        """Intercept unary-stream calls to add auth metadata."""
        new_details = _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            self._add_auth_metadata(client_call_details.metadata),
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )
        return continuation(new_details, request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        """Intercept stream-unary calls to add auth metadata."""
        new_details = _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            self._add_auth_metadata(client_call_details.metadata),
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )
        return continuation(new_details, request_iterator)

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        """Intercept stream-stream calls to add auth metadata."""
        new_details = _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            self._add_auth_metadata(client_call_details.metadata),
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )
        return continuation(new_details, request_iterator)


class _CycleIdInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """Client interceptor that propagates cycle_id as gRPC metadata.

    Reads the current cycle_id from the ContextVar and attaches it to
    every outgoing RPC call, so the gateway can correlate all calls
    within a single decide->execute cycle.
    """

    METADATA_KEY = "x-cycle-id"

    def _add_cycle_metadata(self, metadata: tuple | None) -> tuple:
        from almanak.framework.observability.context import get_cycle_id

        metadata = metadata or ()
        cycle_id = get_cycle_id()
        if cycle_id:
            return metadata + ((self.METADATA_KEY, cycle_id),)
        return metadata

    def intercept_unary_unary(self, continuation, client_call_details, request):
        new_details = _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            self._add_cycle_metadata(client_call_details.metadata),
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )
        return continuation(new_details, request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        new_details = _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            self._add_cycle_metadata(client_call_details.metadata),
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )
        return continuation(new_details, request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        new_details = _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            self._add_cycle_metadata(client_call_details.metadata),
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )
        return continuation(new_details, request_iterator)

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        new_details = _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            self._add_cycle_metadata(client_call_details.metadata),
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )
        return continuation(new_details, request_iterator)


class _ClientCallDetails(
    grpc.ClientCallDetails,
):
    """Implementation of grpc.ClientCallDetails for use in interceptors."""

    def __init__(self, method, timeout, metadata, credentials, wait_for_ready, compression):
        self.method = method
        self.timeout = timeout
        self.metadata = metadata
        self.credentials = credentials
        self.wait_for_ready = wait_for_ready
        self.compression = compression


@dataclass
class GatewayClientConfig:
    """Configuration for gateway client.

    Attributes:
        host: Gateway hostname (default: localhost for local dev, 'gateway' in Docker)
        port: Gateway gRPC port (default: 50051)
        timeout: Default timeout for RPC calls in seconds
        auth_token: Authentication token for gateway access (optional)
    """

    host: str = "localhost"
    port: int = 50051
    timeout: float = 30.0
    auth_token: str | None = None

    @classmethod
    def from_env(cls) -> "GatewayClientConfig":
        """Load configuration from the typed config service.

        The precedence ladder ``ALMANAK_GATEWAY_*`` →
        ``GATEWAY_*`` → hardcoded default is encoded once in
        :func:`almanak.config.cli_runtime.cli_runtime_config_from_env`
        and exposed here as the ``gateway_client_*_resolved`` fields:

        * ``ALMANAK_GATEWAY_HOST`` > ``GATEWAY_HOST`` > ``"localhost"``
        * ``ALMANAK_GATEWAY_PORT`` > ``GATEWAY_PORT`` > ``50051``
        * ``ALMANAK_GATEWAY_TIMEOUT`` > ``GATEWAY_TIMEOUT`` > ``30.0``
        * ``ALMANAK_GATEWAY_AUTH_TOKEN`` > ``GATEWAY_AUTH_TOKEN``
          > ``None``

        The legacy unprefixed forms remain operator-supported for one
        release; ``warn_legacy_gateway_envvars`` (called at the Click
        main group) emits a deprecation warning when they are set.
        """
        # Imported lazily so this dataclass module stays cheap to import
        # in test contexts that never hit the env path.
        from almanak.config.cli_runtime import cli_runtime_config_from_env

        cli = cli_runtime_config_from_env()
        return cls(
            host=cli.gateway_client_host_resolved,
            port=cli.gateway_client_port_resolved,
            timeout=cli.gateway_client_timeout_resolved,
            auth_token=cli.gateway_client_auth_token_resolved,
        )


class GatewayClient:
    """Client for the gateway sidecar service.

    The gateway client provides a type-safe interface for strategy containers
    to access platform services. All calls go through gRPC to the gateway
    sidecar, which holds the actual secrets and external connections.

    Usage:
        # As context manager (recommended)
        with GatewayClient() as client:
            if client.health_check():
                # Use client...
                pass

        # Manual connection management
        client = GatewayClient()
        client.connect()
        try:
            if client.health_check():
                # Use client...
                pass
        finally:
            client.disconnect()
    """

    def __init__(self, config: GatewayClientConfig | None = None):
        """Initialize gateway client.

        Args:
            config: Client configuration. If None, loads from environment.
        """
        self.config = config or GatewayClientConfig.from_env()
        self._channel: grpc.Channel | None = None
        self._health_stub: health_pb2_grpc.HealthStub | None = None
        self._market_stub: gateway_pb2_grpc.MarketServiceStub | None = None
        self._state_stub: gateway_pb2_grpc.StateServiceStub | None = None
        self._execution_stub: gateway_pb2_grpc.ExecutionServiceStub | None = None
        self._observe_stub: gateway_pb2_grpc.ObserveServiceStub | None = None
        self._rpc_stub: gateway_pb2_grpc.RpcServiceStub | None = None
        self._integration_stub: gateway_pb2_grpc.IntegrationServiceStub | None = None
        self._dashboard_stub: gateway_pb2_grpc.DashboardServiceStub | None = None
        self._funding_rate_stub: gateway_pb2_grpc.FundingRateServiceStub | None = None
        # VIB-5595: PerpFillService stub for per-fill economics + funding deltas
        # on async-settlement perp venues (Hyperliquid). Backs the accounting
        # ``PerpFillReader``; the userFills/userFunding HTTP egress is gateway-side.
        self._perp_fill_stub: gateway_pb2_grpc.PerpFillServiceStub | None = None
        # VIB-4727: PoolAnalyticsService stub for market.pool_analytics(...).
        self._pool_analytics_stub: gateway_pb2_grpc.PoolAnalyticsServiceStub | None = None
        # VIB-4728 / POOL-7 (VIB-4755): PoolHistoryService stub. Backs
        # ``PoolHistoryReader`` / ``MarketSnapshot.pool_history(...)``.
        # All HTTP / GraphQL egress to The Graph / DefiLlama /
        # CoinGecko Onchain happens on the gateway side.
        self._pool_history_stub: gateway_pb2_grpc.PoolHistoryServiceStub | None = None
        # VIB-4859 / W7: RateHistoryService — lending APY / perp funding
        # / DEX TWAP / DEX volume. Backs ``RateMonitor`` /
        # ``RateHistoryReader`` / backtesting rate providers after W7.
        self._rate_history_stub: gateway_pb2_grpc.RateHistoryServiceStub | None = None
        self._simulation_stub: gateway_pb2_grpc.SimulationServiceStub | None = None
        self._connector_stubs: dict[str, Any] = {}
        self._enso_stub: gateway_pb2_grpc.EnsoServiceStub | None = None
        self._lifecycle_stub: gateway_pb2_grpc.LifecycleServiceStub | None = None
        self._teardown_stub: gateway_pb2_grpc.TeardownServiceStub | None = None
        # T24 / VIB-4210: PositionService stub for `ax positions reconcile`.
        self._position_stub: gateway_pb2_grpc.PositionServiceStub | None = None
        self._gateway_health_stub: gateway_pb2_grpc.HealthStub | None = None
        self.registered_chains: list[str] | None = None
        self.registered_with_wallet_registry: bool = False

    @property
    def target(self) -> str:
        """Get the gRPC target address."""
        return f"{self.config.host}:{self.config.port}"

    @property
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self._channel is not None

    @property
    def channel(self) -> grpc.Channel | None:
        """Get the underlying gRPC channel, or None if not connected."""
        return self._channel

    @property
    def market(self) -> gateway_pb2_grpc.MarketServiceStub:
        """Get MarketService stub. Raises if not connected."""
        if self._market_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._market_stub

    @property
    def state(self) -> gateway_pb2_grpc.StateServiceStub:
        """Get StateService stub. Raises if not connected."""
        if self._state_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._state_stub

    @property
    def execution(self) -> gateway_pb2_grpc.ExecutionServiceStub:
        """Get ExecutionService stub. Raises if not connected."""
        if self._execution_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._execution_stub

    @property
    def observe(self) -> gateway_pb2_grpc.ObserveServiceStub:
        """Get ObserveService stub. Raises if not connected."""
        if self._observe_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._observe_stub

    @property
    def rpc(self) -> gateway_pb2_grpc.RpcServiceStub:
        """Get RpcService stub. Raises if not connected."""
        if self._rpc_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._rpc_stub

    @property
    def integration(self) -> gateway_pb2_grpc.IntegrationServiceStub:
        """Get IntegrationService stub. Raises if not connected."""
        if self._integration_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._integration_stub

    @property
    def dashboard(self) -> gateway_pb2_grpc.DashboardServiceStub:
        """Get DashboardService stub. Raises if not connected."""
        if self._dashboard_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._dashboard_stub

    @property
    def funding_rate(self) -> gateway_pb2_grpc.FundingRateServiceStub:
        """Get FundingRateService stub. Raises if not connected."""
        if self._funding_rate_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._funding_rate_stub

    @property
    def perp_fill(self) -> gateway_pb2_grpc.PerpFillServiceStub:
        """Get PerpFillService stub (VIB-5595). Raises if not connected.

        Backs the accounting ``PerpFillReader``: per-fill economics (fee,
        realized PnL, price, size) + funding deltas for async-settlement perp
        venues. All HTTP egress to the venue Info API (userFills / userFunding)
        happens gateway-side; the strategy container only speaks gRPC.
        """
        if self._perp_fill_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._perp_fill_stub

    @property
    def pool_analytics(self) -> gateway_pb2_grpc.PoolAnalyticsServiceStub:
        """Get PoolAnalyticsService stub (VIB-4727). Raises if not connected.

        The strategy-container ``PoolAnalyticsReader`` calls this stub to fetch
        TVL / volume / fee-APR. All HTTP egress to DefiLlama / CoinGecko Onchain
        happens on the gateway side; the strategy container only speaks gRPC.
        """
        if self._pool_analytics_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._pool_analytics_stub

    @property
    def pool_history(self) -> gateway_pb2_grpc.PoolHistoryServiceStub:
        """Get PoolHistoryService stub (VIB-4728 / POOL-7 VIB-4755). Raises if not connected.

        The strategy-container ``PoolHistoryReader`` calls this stub to fetch
        historical pool snapshots (TVL / volume / fee revenue / reserves over
        time). All HTTP / GraphQL egress to The Graph / DefiLlama /
        CoinGecko Onchain happens on the gateway side. The framework reader is a
        thin gRPC client per `docs/internal/uat-cards/VIB-4755.md`.
        """
        if self._pool_history_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._pool_history_stub

    @property
    def rate_history(self) -> gateway_pb2_grpc.RateHistoryServiceStub:
        """Get RateHistoryService stub (VIB-4859 / W7). Raises if not connected.

        The strategy-container ``RateMonitor`` / ``RateHistoryReader`` /
        backtesting rate providers call this stub. All HTTP / GraphQL /
        Web3 egress for lending APY / perp funding / DEX TWAP / DEX volume
        happens on the gateway side.
        """
        if self._rate_history_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._rate_history_stub

    @property
    def simulation(self) -> gateway_pb2_grpc.SimulationServiceStub:
        """Get SimulationService stub. Raises if not connected."""
        if self._simulation_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._simulation_stub

    def connector_stub(self, name: str) -> Any:
        """Get a connector-published gRPC service stub by name. Raises if not connected.

        VIB-4989: replaces per-connector stub properties (e.g. ``.polymarket``). A
        connector ships its stub via ``GatewayStubRegistry`` (built at connect time).
        """
        stub = self._connector_stubs.get(name)
        if stub is None:
            raise RuntimeError(f"Gateway client not connected (or no {name!r} connector stub)")
        return stub

    @property
    def enso(self) -> gateway_pb2_grpc.EnsoServiceStub:
        """Get EnsoService stub. Raises if not connected."""
        if self._enso_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._enso_stub

    @property
    def lifecycle(self) -> gateway_pb2_grpc.LifecycleServiceStub:
        """Get LifecycleService stub. Raises if not connected."""
        if self._lifecycle_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._lifecycle_stub

    @property
    def teardown(self) -> gateway_pb2_grpc.TeardownServiceStub:
        """Get TeardownService stub. Raises if not connected."""
        if self._teardown_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._teardown_stub

    @property
    def position(self) -> gateway_pb2_grpc.PositionServiceStub:
        """Get PositionService stub (T24 / VIB-4210). Raises if not connected.

        Reconcile = control-plane RPC. Used exclusively from operator surfaces
        (the ``ax positions reconcile`` CLI in v1; dashboard / hosted-boot in
        T24+1). Strategy code MUST NOT import this property — the static
        guard in ``tests/unit/gateway/test_position_service_strategy_call_ban.py``
        enforces the ban per ADR §6.
        """
        if self._position_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._position_stub

    def connect(self) -> None:
        """Establish connection to gateway.

        Creates a gRPC channel and initializes service stubs.
        If auth_token is configured, adds authentication metadata to all calls.
        """
        if self._channel is not None:
            logger.warning("Already connected to gateway")
            return

        base_channel = grpc.insecure_channel(self.target)

        # Wrap channel with interceptors
        interceptors: list[grpc.UnaryUnaryClientInterceptor] = [_CycleIdInterceptor()]
        if self.config.auth_token:
            interceptors.append(_AuthClientInterceptor(self.config.auth_token))
            logger.debug("Auth token configured for gateway connection")

        self._channel = grpc.intercept_channel(base_channel, *interceptors)

        self._health_stub = health_pb2_grpc.HealthStub(self._channel)
        self._gateway_health_stub = gateway_pb2_grpc.HealthStub(self._channel)

        # Initialize Phase 2 service stubs
        self._market_stub = gateway_pb2_grpc.MarketServiceStub(self._channel)
        self._state_stub = gateway_pb2_grpc.StateServiceStub(self._channel)
        self._execution_stub = gateway_pb2_grpc.ExecutionServiceStub(self._channel)
        self._observe_stub = gateway_pb2_grpc.ObserveServiceStub(self._channel)

        # Initialize Phase 3 service stubs
        self._rpc_stub = gateway_pb2_grpc.RpcServiceStub(self._channel)
        self._integration_stub = gateway_pb2_grpc.IntegrationServiceStub(self._channel)

        # Initialize Dashboard service stub
        self._dashboard_stub = gateway_pb2_grpc.DashboardServiceStub(self._channel)

        # Initialize FundingRate service stub
        self._funding_rate_stub = gateway_pb2_grpc.FundingRateServiceStub(self._channel)

        # Initialize PerpFill service stub (VIB-5595)
        self._perp_fill_stub = gateway_pb2_grpc.PerpFillServiceStub(self._channel)

        # Initialize PoolAnalytics service stub (VIB-4727)
        self._pool_analytics_stub = gateway_pb2_grpc.PoolAnalyticsServiceStub(self._channel)

        # Initialize PoolHistory service stub (VIB-4728 / POOL-7 VIB-4755)
        self._pool_history_stub = gateway_pb2_grpc.PoolHistoryServiceStub(self._channel)

        # Initialize RateHistory service stub (VIB-4859 / W7)
        self._rate_history_stub = gateway_pb2_grpc.RateHistoryServiceStub(self._channel)

        # Initialize Simulation service stub
        self._simulation_stub = gateway_pb2_grpc.SimulationServiceStub(self._channel)

        # Initialize connector-published gRPC service stubs (VIB-4989: each
        # connector ships its stub via GatewayStubRegistry; no per-connector import).
        # Roll back the half-open connection if stub construction raises (e.g. a
        # service_name collision) so connect() never leaves an open channel +
        # populated core stubs behind.
        try:
            self._connector_stubs = GatewayStubRegistry.build_stubs(self._channel)
        except Exception:
            self.disconnect()
            raise

        # Initialize Enso service stub
        self._enso_stub = gateway_pb2_grpc.EnsoServiceStub(self._channel)

        # Initialize Lifecycle service stub
        self._lifecycle_stub = gateway_pb2_grpc.LifecycleServiceStub(self._channel)

        # Initialize Teardown service stub
        self._teardown_stub = gateway_pb2_grpc.TeardownServiceStub(self._channel)

        # Initialize Position service stub (T24 / VIB-4210)
        self._position_stub = gateway_pb2_grpc.PositionServiceStub(self._channel)

        logger.debug(f"Channel opened to gateway at {self.target}")

    def disconnect(self) -> None:
        """Close connection to gateway.

        Closes the gRPC channel and clears service stubs.
        """
        if self._channel is not None:
            self._channel.close()
            self._channel = None
            self._health_stub = None
            self._gateway_health_stub = None
            self.registered_chains = None
            self.registered_with_wallet_registry = False
            self._market_stub = None
            self._state_stub = None
            self._execution_stub = None
            self._observe_stub = None
            self._rpc_stub = None
            self._integration_stub = None
            self._dashboard_stub = None
            self._funding_rate_stub = None
            self._perp_fill_stub = None
            self._pool_analytics_stub = None
            self._pool_history_stub = None
            self._rate_history_stub = None
            self._simulation_stub = None
            self._connector_stubs = {}
            self._enso_stub = None
            self._lifecycle_stub = None
            self._teardown_stub = None
            self._position_stub = None
            logger.info("Disconnected from gateway")

    def health_check(self, service: str = "") -> bool:
        """Check if gateway is healthy.

        Args:
            service: Specific service to check. Empty string checks overall health.

        Returns:
            True if gateway is serving, False otherwise.
        """
        if self._health_stub is None:
            logger.warning("Gateway client not connected")
            return False

        try:
            response = self._health_stub.Check(
                health_pb2.HealthCheckRequest(service=service),
                timeout=self.config.timeout,
            )
            is_healthy = response.status == health_pb2.HealthCheckResponse.SERVING
            if is_healthy:
                logger.debug(f"Gateway health check passed (service={service or 'overall'})")
            else:
                logger.warning(f"Gateway health check failed: status={response.status}")
            return is_healthy
        except grpc.RpcError as e:
            logger.warning(f"Gateway health check failed: {e}")
            return False

    def wait_for_ready(self, timeout: float = 30.0, interval: float = 1.0) -> bool:
        """Wait for gateway to become ready.

        Suppresses per-attempt logs and only logs an error if the gateway
        never becomes ready within the timeout.

        Args:
            timeout: Maximum time to wait in seconds
            interval: Time between health check attempts in seconds

        Returns:
            True if gateway became ready within timeout, False otherwise.
        """
        import time

        start = time.monotonic()
        last_error = None
        while True:
            remaining = timeout - (time.monotonic() - start)
            if remaining <= 0:
                break
            try:
                if self._health_stub is None:
                    last_error = "Gateway client not connected"
                else:
                    response = self._health_stub.Check(
                        health_pb2.HealthCheckRequest(service=""),
                        timeout=min(self.config.timeout, remaining),
                    )
                    if response.status == health_pb2.HealthCheckResponse.SERVING:
                        return True
                    last_error = f"status={response.status}"
            except grpc.RpcError as e:
                last_error = str(e)
            time.sleep(min(interval, max(0, timeout - (time.monotonic() - start))))

        elapsed = time.monotonic() - start
        logger.error(f"Gateway not ready after {elapsed:.0f}s timeout. Last error: {last_error}")
        return False

    def __enter__(self) -> "GatewayClient":
        """Context manager entry - connect to gateway."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - disconnect from gateway."""
        self.disconnect()

    def register_chains(self, chains: list[str]) -> dict[str, str]:
        """Register chains with the gateway and discover per-chain wallets.

        Calls the gateway's RegisterChains RPC to pre-warm orchestrators and
        compilers. If the gateway has a wallet registry plugin, per-chain wallet
        addresses are returned in the response.

        Args:
            chains: List of chain names to register (e.g., ["arbitrum", "base"])

        Returns:
            Dict mapping chain name to wallet address (from wallet registry).
            Empty dict if no wallet registry is configured.
        """
        from almanak.gateway.proto import gateway_pb2

        if self._gateway_health_stub is None:
            raise RuntimeError("Gateway client not connected")

        try:
            response = self._gateway_health_stub.RegisterChains(
                gateway_pb2.RegisterChainsRequest(chains=chains),
                timeout=self.config.timeout,
            )
            chain_wallets = dict(response.chain_wallets) if response.chain_wallets else {}
            self.registered_chains = list(response.initialized_chains)
            self.registered_with_wallet_registry = bool(chain_wallets)
            if chain_wallets:
                logger.info(
                    "Registered chains with wallet registry: %s",
                    {k: v[:10] + "..." for k, v in chain_wallets.items()},
                )
            return chain_wallets
        except grpc.RpcError as e:
            # Legacy fallback: gateway may not support RegisterChains yet
            if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                logger.debug("Gateway does not support RegisterChains, using legacy flow")
                self.registered_chains = list(chains)
                return {}
            raise

    # =========================================================================
    # Convenience methods for typed RPC queries
    # =========================================================================

    def query_allowance(
        self,
        chain: str,
        token_address: str,
        owner_address: str,
        spender_address: str,
        block: int | str | None = None,
    ) -> int | None:
        """Query ERC-20 allowance via gateway.

        Args:
            chain: Chain identifier (e.g., "arbitrum", "base")
            token_address: ERC-20 token contract address
            owner_address: Token owner address
            spender_address: Spender address
            block: Optional block reference (VIB-5140). ``None`` (default) →
                ``"latest"`` (legacy behaviour). Post-transaction reads should
                pin to the confirmed receipt's ``block_number`` so the call
                cannot race a read replica that trails the writer.

        Returns:
            Allowance in wei, or None if query fails
        """
        from almanak.gateway.proto import gateway_pb2

        if self._rpc_stub is None:
            logger.warning("Gateway client not connected")
            return None

        try:
            response = self._rpc_stub.QueryAllowance(
                gateway_pb2.AllowanceRequest(
                    chain=chain,
                    token_address=token_address,
                    owner_address=owner_address,
                    spender_address=spender_address,
                    block=_encode_block_tag(block),
                ),
                timeout=self.config.timeout,
            )
            if response.success:
                return int(response.allowance)
            logger.warning(f"QueryAllowance failed: {response.error}")
            return None
        except grpc.RpcError as e:
            logger.warning(f"QueryAllowance RPC error: {e}")
            return None

    def query_erc20_balance(
        self,
        chain: str,
        token_address: str,
        wallet_address: str,
        block: int | str | None = None,
    ) -> int | None:
        """Query ERC-20 balance via gateway.

        Args:
            chain: Chain identifier (e.g., "arbitrum", "base")
            token_address: ERC-20 token contract address
            wallet_address: Wallet address to query balance for
            block: Optional block reference (VIB-5140). ``None`` (default) →
                ``"latest"`` (legacy behaviour). Post-transaction reads should
                pin to the confirmed receipt's ``block_number`` so the call
                cannot race a read replica that trails the writer.

        Returns:
            Balance in wei, or None if query fails
        """
        from almanak.gateway.proto import gateway_pb2

        if self._rpc_stub is None:
            logger.warning("Gateway client not connected")
            return None

        try:
            response = self._rpc_stub.QueryBalance(
                gateway_pb2.BalanceQueryRequest(
                    chain=chain,
                    token_address=token_address,
                    wallet_address=wallet_address,
                    block=_encode_block_tag(block),
                ),
                timeout=self.config.timeout,
            )
            if response.success:
                return int(response.balance)
            logger.warning(f"QueryBalance failed: {response.error}")
            return None
        except grpc.RpcError as e:
            logger.warning(f"QueryBalance RPC error: {e}")
            return None

    def query_native_balance(
        self,
        chain: str,
        wallet_address: str,
        block: int | str | None = None,
    ) -> int | None:
        """Query native token balance (ETH, MATIC, AVAX, etc.) via gateway RPC.

        Args:
            chain: Chain identifier (e.g., "arbitrum", "base")
            wallet_address: Wallet address to query balance for
            block: Optional block reference for ``eth_getBalance``. ``None``
                (default) uses the ``"latest"`` block tag — backwards-compatible
                behaviour. An ``int`` is encoded as the standard JSON-RPC hex
                string (``hex(N)``); a ``str`` passes through unchanged
                (``"latest"`` / ``"pending"`` / ``"safe"`` / a pre-encoded hex).

                VIB-5121 — native-leg LP accounting reads a BLOCK-PINNED
                pre/post wallet native-balance bracket around a deposit/withdraw
                tx (the native msg.value leg emits no ERC-20 Transfer). Both
                anchors MUST pin to the receipt's exact block: reading the PRE
                anchor at ``"latest"`` after the tx landed would return the POST
                balance and fabricate a near-zero deposit (Empty ≠ Zero). Block
                anchoring eliminates that race by construction — mirrors the
                VIB-4589 / F7 discipline already used by :meth:`eth_call`.

        Returns:
            Native balance in wei, or None if query fails
        """
        from almanak.gateway.proto import gateway_pb2

        if self._rpc_stub is None:
            logger.warning("Gateway client not connected")
            return None

        # Encode the block reference per JSON-RPC eth_getBalance semantics —
        # same rules as :meth:`eth_call`. Reject bool (int subclass) and negative
        # ints locally so a caller bug surfaces here instead of silently
        # degrading to ``"latest"`` (which would re-open the bracket race).
        if block is None:
            block_param: str = "latest"
        elif isinstance(block, bool):
            raise ValueError(f"query_native_balance block must not be bool, got {block!r}")
        elif isinstance(block, int):
            if block < 0:
                raise ValueError(f"query_native_balance block must be non-negative, got {block}")
            block_param = hex(block)
        elif isinstance(block, str):
            block_param = block
        else:
            # Reject any other type (float / list / dict / bytes) locally rather
            # than letting json.dumps serialize a malformed block tag into the
            # eth_getBalance RPC param.
            raise ValueError(f"query_native_balance block must be int | str | None, got {type(block).__name__}")

        try:
            import json

            response = self._rpc_stub.Call(
                gateway_pb2.RpcRequest(
                    chain=chain,
                    method="eth_getBalance",
                    params=json.dumps([wallet_address, block_param]),
                ),
                timeout=self.config.timeout,
            )
            if not response.success:
                logger.warning(f"Native balance query failed: {response.error}")
                return None
            if response.result:
                hex_balance = json.loads(response.result)
                return int(hex_balance, 16)
            return None
        except grpc.RpcError as e:
            logger.warning(f"Native balance query RPC error: {e}")
            return None

    def eth_call(
        self,
        chain: str,
        to: str,
        data: str,
        block: int | str | None = None,
    ) -> str | None:
        """Perform a raw eth_call via the gateway's RPC proxy.

        Args:
            chain: Chain identifier (e.g., "base", "arbitrum")
            to: Contract address to call
            data: Hex-encoded calldata (with 0x prefix)
            block: Optional block reference. ``None`` (default) uses the
                ``"latest"`` block tag — backwards-compatible behaviour.
                An ``int`` is encoded as the standard JSON-RPC hex string
                (``hex(N)``). A ``str`` is passed through unchanged (so
                callers can supply ``"latest"``, ``"pending"``, ``"safe"``,
                or a pre-encoded ``"0x..."``).

                VIB-4589 / F7 — receipt-time post-state reads (Aave V3
                ``getUserAccountData`` etc.) MUST pin to ``receipt.block_number``
                to avoid racing the upstream RPC's receipt indexer. Reading
                at ``"latest"`` on mainnet has surfaced as a stale-collateral
                bug where a confirmed WITHDRAW receipt is not yet reflected
                in the next ``"latest"`` view, so the post-state read returns
                a near-full collateral balance instead of the expected
                near-zero. Block-anchoring eliminates the race by construction.

        Returns:
            Hex-encoded result string, or None on failure
        """
        import json

        from almanak.gateway.proto import gateway_pb2

        if self._rpc_stub is None:
            logger.warning("Gateway client not connected")
            return None

        # Encode the block reference per JSON-RPC eth_call semantics.
        # Integers → 0x-prefixed hex (no zero-pad — Geth / Erigon / Alchemy
        # all accept the minimal form). Strings pass through (covers
        # "latest" / "pending" / "safe" / "finalized" / pre-encoded hex).
        # Reject bool (subclass of int) and negative ints locally — failing
        # here surfaces a caller bug instead of (a) silently degrading to
        # ``"latest"`` (which would re-open the VIB-4589 race) or
        # (b) producing a confusing downstream RPC error. The ValueError
        # propagates past the inner try/except (which only catches
        # ``grpc.RpcError``) and is converted to ``None`` at the next layer
        # up by ``lending_accounting._gateway_eth_call``'s outer
        # ``except Exception`` clause, so production paths still fail
        # gracefully while the caller's traceback identifies the bad value.
        if block is None:
            block_param: str = "latest"
        elif isinstance(block, bool):
            raise ValueError(f"eth_call block must not be bool, got {block!r}")
        elif isinstance(block, int):
            if block < 0:
                raise ValueError(f"eth_call block must be non-negative, got {block}")
            block_param = hex(block)
        else:
            block_param = block

        try:
            params = json.dumps([{"to": to, "data": data}, block_param])
            response = self._rpc_stub.Call(
                gateway_pb2.RpcRequest(
                    chain=chain,
                    method="eth_call",
                    params=params,
                ),
                timeout=self.config.timeout,
            )
            if not response.success:
                logger.warning(f"eth_call failed: {response.error}")
                return None
            if response.result:
                return json.loads(response.result)
            return None
        except grpc.RpcError as e:
            logger.warning(f"eth_call RPC error: {e}")
            return None

    def estimate_gas(
        self,
        chain: str,
        to: str,
        data: str,
        *,
        from_address: str | None = None,
        value: int = 0,
    ) -> int | None:
        """Perform ``eth_estimateGas`` via the gateway's RPC proxy.

        ``eth_estimateGas`` is already on the gateway's ``ALLOWED_RPC_METHODS``
        allowlist (``almanak/gateway/validation.py``), so this rides the SAME
        gateway RPC channel as :meth:`eth_call` / ``eth_getBalance`` — the
        strategy container never opens a socket. Connectors use it to seed a
        live, pool-shape-aware gas floor instead of a hardcoded per-op constant
        that under-sizes 4-coin / native-ETH / aave-type pools (VIB-5440).

        Args:
            chain: Chain identifier (e.g. "ethereum", "base", "arbitrum").
            to: Target contract address.
            data: Hex-encoded calldata (``0x``-prefixed).
            from_address: Caller address. Gas depends on caller state (token
                balances / allowances), so pass the execution wallet for an
                accurate estimate; omit only when the caller is unknown.
            value: Native value (wei) attached to the call — required for
                native-ETH pool paths so the estimate reflects the msg.value leg.

        Returns:
            Estimated gas units, or ``None`` on any failure (RPC error, revert
            on estimate, gateway not connected) so callers fall back to their
            conservative static floor. ``None`` means "unmeasured", never 0
            (Empty≠Zero).
        """
        import json

        from almanak.gateway.proto import gateway_pb2

        if self._rpc_stub is None:
            logger.warning("Gateway client not connected; cannot estimate gas")
            return None

        tx_obj: dict[str, str] = {"to": to, "data": data}
        if from_address:
            tx_obj["from"] = from_address
        if value:
            tx_obj["value"] = hex(value)

        try:
            response = self._rpc_stub.Call(
                gateway_pb2.RpcRequest(
                    chain=chain,
                    method="eth_estimateGas",
                    params=json.dumps([tx_obj]),
                ),
                timeout=self.config.timeout,
            )
            if not response.success:
                logger.debug(f"eth_estimateGas failed: {response.error}")
                return None
            if response.result:
                return int(json.loads(response.result), 16)
            return None
        except grpc.RpcError as e:
            logger.debug(f"eth_estimateGas RPC error: {e}")
            return None
        except (ValueError, TypeError) as e:
            # A malformed / unexpected result (e.g. ``'0x'``, non-hex, non-JSON)
            # makes ``json.loads`` / ``int(..., 16)`` raise — which ``except
            # grpc.RpcError`` does not catch. Degrade to None (Empty≠Zero — the
            # caller falls back to the conservative static floor) rather than
            # propagate and crash the strategy container. Mirrors ``block_number``.
            logger.debug(f"eth_estimateGas decode error: {e}")
            return None

    def block_number(self, chain: str, *, timeout: float | None = None) -> int | None:
        """Return the current chain head block number via the gateway RPC proxy.

        VIB-3350 — the confirmation-depth wait before a block-pinned
        reconciliation read polls this to learn how far the chain has advanced
        past the receipt block. Routed through the gateway's ``RpcService``
        (``eth_blockNumber``) so the strategy container holds no RPC URL.

        Args:
            chain: Chain identifier (e.g., "base", "arbitrum").
            timeout: Optional per-call gRPC deadline in seconds. The polling
                caller passes its remaining wait budget so a single stalled
                ``eth_blockNumber`` cannot outlive the caller's deadline; when
                ``None`` the client's configured timeout is used.

        Returns:
            The head block number, or ``None`` on any failure (caller treats a
            missing head as "cannot confirm" rather than blocking forever).
        """
        import json

        from almanak.gateway.proto import gateway_pb2

        if self._rpc_stub is None:
            logger.warning("Gateway client not connected")
            return None

        try:
            response = self._rpc_stub.Call(
                gateway_pb2.RpcRequest(
                    chain=chain,
                    method="eth_blockNumber",
                    params="[]",
                ),
                timeout=timeout if timeout is not None else self.config.timeout,
            )
            if not response.success:
                logger.warning("eth_blockNumber failed: %s", response.error)
                return None
            if not response.result:
                return None
            raw = json.loads(response.result)
            # JSON-RPC returns the head as a 0x-prefixed hex quantity.
            return int(raw, 16) if isinstance(raw, str) else int(raw)
        except grpc.RpcError as e:
            logger.warning("eth_blockNumber RPC error: %s", e)
            return None
        except (ValueError, TypeError) as e:
            logger.warning("eth_blockNumber decode error: %s", e)
            return None

    def query_position_liquidity(
        self,
        chain: str,
        position_manager: str,
        token_id: int,
        block: int | str | None = None,
    ) -> int | None:
        """Query Uniswap V3 position liquidity via gateway.

        Args:
            chain: Chain identifier (e.g., "arbitrum", "base")
            position_manager: NFT Position Manager contract address
            token_id: Position NFT token ID
            block: Optional block reference (VIB-5140). ``None`` (default) →
                ``"latest"`` (legacy behaviour). The teardown closure verifier
                pins this to the close-tx receipt's ``block_number`` so a read
                replica that trails the writer cannot return PRE-close
                liquidity and false-negative the closure check.

        Returns:
            Liquidity value, or None if query fails
        """
        from almanak.gateway.proto import gateway_pb2

        if self._rpc_stub is None:
            logger.warning("Gateway client not connected")
            return None

        try:
            response = self._rpc_stub.QueryPositionLiquidity(
                gateway_pb2.PositionLiquidityRequest(
                    chain=chain,
                    position_manager=position_manager,
                    token_id=token_id,
                    block=_encode_block_tag(block),
                ),
                timeout=self.config.timeout,
            )
            if response.success:
                return int(response.liquidity)
            error_msg = response.error or ""
            if "invalid token id" in error_msg.lower():
                logger.info(
                    "QueryPositionLiquidity indicates invalid token id; treating as closed position",
                    extra={"token_id": token_id, "error": error_msg},
                )
                return 0
            logger.warning(f"QueryPositionLiquidity failed: {error_msg}")
            return None
        except grpc.RpcError as e:
            logger.warning(f"QueryPositionLiquidity RPC error: {e}")
            return None

    def query_position_tokens_owed(
        self,
        chain: str,
        position_manager: str,
        token_id: int,
        block: int | str | None = None,
    ) -> tuple[int, int] | None:
        """Query Uniswap V3 ``positions(tokenId).tokensOwed{0,1}`` via gateway.

        Used by the teardown verifier to confirm that a Uniswap V3 LP position
        has zero residual fees / withdrawn-but-uncollected liquidity. A
        position can be "closed" in two ways: the NFT is burnt (tokenId no
        longer exists, query returns "invalid token id") OR the position is
        still owned with ``liquidity == 0`` AND ``tokensOwed{0,1} == 0``.

        Args:
            chain: Chain identifier (e.g., "arbitrum", "base").
            position_manager: NFT Position Manager contract address.
            token_id: Position NFT token ID.
            block: Optional block reference (VIB-5140). ``None`` (default) →
                ``"latest"`` (legacy behaviour). The teardown closure verifier
                pins this to the close-tx receipt's ``block_number`` so a read
                replica that trails the writer cannot return PRE-close
                tokensOwed and false-negative the closure check.

        Returns:
            ``(tokens_owed0, tokens_owed1)`` on success.
            ``(0, 0)`` when the gateway reports the position is closed
            (invalid token id / position not found — the canonical revert
            from ``positions(tokenId)`` after the NFT is burnt).
            ``None`` when the gateway/RPC call could not be completed —
            callers MUST treat ``None`` as "unknown, fail closed" rather
            than "no fees owed".
        """
        from almanak.gateway.proto import gateway_pb2

        if self._rpc_stub is None:
            logger.warning("Gateway client not connected")
            return None

        try:
            response = self._rpc_stub.QueryPositionTokensOwed(
                gateway_pb2.PositionTokensOwedRequest(
                    chain=chain,
                    position_manager=position_manager,
                    token_id=token_id,
                    block=_encode_block_tag(block),
                ),
                timeout=self.config.timeout,
            )
            if response.success:
                tokens_owed0 = int(response.tokens_owed0) if response.tokens_owed0 else 0
                tokens_owed1 = int(response.tokens_owed1) if response.tokens_owed1 else 0
                return tokens_owed0, tokens_owed1
            error_msg = response.error or ""
            error_lower = error_msg.lower()
            if "invalid token id" in error_lower or "position not found" in error_lower:
                logger.info(
                    "QueryPositionTokensOwed indicates closed position; treating tokens owed as 0",
                    extra={"token_id": token_id, "error": error_msg},
                )
                return 0, 0
            logger.warning(f"QueryPositionTokensOwed failed: {error_msg}")
            return None
        except grpc.RpcError as e:
            logger.warning(f"QueryPositionTokensOwed RPC error: {e}")
            return None

    def query_v4_position_state(
        self,
        chain: str,
        position_manager: str,
        state_view: str,
        token_id: int,
        block: int | str | None = None,
    ) -> "V4PositionState | None":
        """Read live Uniswap V4 LP position state on-chain via the gateway (VIB-5024).

        Args:
            chain: Chain identifier (e.g. "base").
            position_manager: V4 PositionManager address (connector-resolved).
            state_view: V4 StateView address (connector-resolved).
            token_id: Position NFT token ID.
            block: Optional block reference (VIB-5148, Layer-2 follow-up to
                VIB-5140). ``None`` (default) → ``"latest"`` (legacy
                behaviour). A future post-tx V4 read (e.g. a V4 teardown
                closure verifier, not yet implemented) MUST pin this to the
                close-tx receipt's ``block_number`` so a read replica that
                trails the writer cannot return PRE-close state and
                false-negative the closure check — the same latent race
                VIB-5140 fixed for V3.

        Returns:
            A :class:`V4PositionState` on a clean full read, or ``None`` when the
            gateway reports failure / a partial read / the RPC errors. ``None``
            means "no live on-chain truth" — the valuer falls back to the
            ESTIMATED OPEN-amount path rather than ever valuing at HIGH from
            incomplete data (the never-wrong-HIGH guarantee).
        """
        from almanak.gateway.proto import gateway_pb2

        if self._rpc_stub is None:
            logger.warning("Gateway client not connected")
            return None

        try:
            response = self._rpc_stub.QueryV4PositionState(
                gateway_pb2.V4PositionStateRequest(
                    chain=chain,
                    position_manager=position_manager,
                    state_view=state_view,
                    token_id=token_id,
                    block=_encode_block_tag(block),
                ),
                timeout=self.config.timeout,
            )
        except grpc.RpcError as e:
            logger.warning(f"QueryV4PositionState RPC error: {e}")
            return None

        if not response.success:
            logger.info(
                "QueryV4PositionState did not return live state; valuer will fall back to ESTIMATED",
                extra={"token_id": token_id, "chain": chain, "error": response.error or ""},
            )
            return None

        # Empty ≠ Zero: the gateway emits "" only on failure (success=False,
        # handled above). On success the numeric strings are always populated;
        # a measured-zero liquidity ("0") is a valid closed-but-owned position.
        try:
            liquidity = int(response.liquidity) if response.liquidity != "" else None
            sqrt_price_x96 = int(response.sqrt_price_x96) if response.sqrt_price_x96 != "" else None
            # Fees are part of a complete HIGH read (V3 parity); the gateway fails
            # closed if it cannot measure them, so on success they are populated.
            tokens_owed0 = int(response.tokens_owed0) if response.tokens_owed0 != "" else None
            tokens_owed1 = int(response.tokens_owed1) if response.tokens_owed1 != "" else None
        except (ValueError, TypeError):
            logger.warning("QueryV4PositionState returned unparseable numeric fields; treating as no live state")
            return None
        if liquidity is None or sqrt_price_x96 is None or tokens_owed0 is None or tokens_owed1 is None:
            return None

        return V4PositionState(
            liquidity=liquidity,
            tick_lower=int(response.tick_lower),
            tick_upper=int(response.tick_upper),
            current_tick=int(response.current_tick),
            sqrt_price_x96=sqrt_price_x96,
            pool_id=response.pool_id or "",
            tokens_owed0=tokens_owed0,
            tokens_owed1=tokens_owed1,
        )


# =============================================================================
# Singleton accessor for convenience
# =============================================================================

_default_client: GatewayClient | None = None


def get_gateway_client() -> GatewayClient:
    """Get the default gateway client (singleton).

    Returns a shared GatewayClient instance. The client is not connected
    by default; call connect() before use or use as context manager.

    Returns:
        Shared GatewayClient instance.
    """
    global _default_client
    if _default_client is None:
        _default_client = GatewayClient()
    return _default_client


def reset_gateway_client() -> None:
    """Reset the default gateway client.

    Disconnects and clears the singleton client. Useful for testing.
    """
    global _default_client
    if _default_client is not None:
        _default_client.disconnect()
        _default_client = None
