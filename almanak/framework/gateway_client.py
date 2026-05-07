"""Client for communicating with the gateway sidecar.

This module provides the GatewayClient class that strategy containers use
to communicate with the gateway. All external access (market data, state,
execution) goes through this client.
"""

import logging
from dataclasses import dataclass

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc

from almanak.gateway.proto import gateway_pb2_grpc

logger = logging.getLogger(__name__)

# Metadata key for authentication token (matches server-side auth.py)
AUTH_METADATA_KEY = "authorization"


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
        self._simulation_stub: gateway_pb2_grpc.SimulationServiceStub | None = None
        self._polymarket_stub: gateway_pb2_grpc.PolymarketServiceStub | None = None
        self._enso_stub: gateway_pb2_grpc.EnsoServiceStub | None = None
        self._lifecycle_stub: gateway_pb2_grpc.LifecycleServiceStub | None = None
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
    def simulation(self) -> gateway_pb2_grpc.SimulationServiceStub:
        """Get SimulationService stub. Raises if not connected."""
        if self._simulation_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._simulation_stub

    @property
    def polymarket(self) -> gateway_pb2_grpc.PolymarketServiceStub:
        """Get PolymarketService stub. Raises if not connected."""
        if self._polymarket_stub is None:
            raise RuntimeError("Gateway client not connected")
        return self._polymarket_stub

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

        # Initialize Simulation service stub
        self._simulation_stub = gateway_pb2_grpc.SimulationServiceStub(self._channel)

        # Initialize Polymarket service stub
        self._polymarket_stub = gateway_pb2_grpc.PolymarketServiceStub(self._channel)

        # Initialize Enso service stub
        self._enso_stub = gateway_pb2_grpc.EnsoServiceStub(self._channel)

        # Initialize Lifecycle service stub
        self._lifecycle_stub = gateway_pb2_grpc.LifecycleServiceStub(self._channel)

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
            self._simulation_stub = None
            self._polymarket_stub = None
            self._enso_stub = None
            self._lifecycle_stub = None
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
    ) -> int | None:
        """Query ERC-20 allowance via gateway.

        Args:
            chain: Chain identifier (e.g., "arbitrum", "base")
            token_address: ERC-20 token contract address
            owner_address: Token owner address
            spender_address: Spender address

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
    ) -> int | None:
        """Query ERC-20 balance via gateway.

        Args:
            chain: Chain identifier (e.g., "arbitrum", "base")
            token_address: ERC-20 token contract address
            wallet_address: Wallet address to query balance for

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
    ) -> int | None:
        """Query native token balance (ETH, MATIC, AVAX, etc.) via gateway RPC.

        Args:
            chain: Chain identifier (e.g., "arbitrum", "base")
            wallet_address: Wallet address to query balance for

        Returns:
            Native balance in wei, or None if query fails
        """
        from almanak.gateway.proto import gateway_pb2

        if self._rpc_stub is None:
            logger.warning("Gateway client not connected")
            return None

        try:
            import json

            response = self._rpc_stub.Call(
                gateway_pb2.RpcRequest(
                    chain=chain,
                    method="eth_getBalance",
                    params=f'["{wallet_address}", "latest"]',
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
    ) -> str | None:
        """Perform a raw eth_call via the gateway's RPC proxy.

        Args:
            chain: Chain identifier (e.g., "base", "arbitrum")
            to: Contract address to call
            data: Hex-encoded calldata (with 0x prefix)

        Returns:
            Hex-encoded result string, or None on failure
        """
        import json

        from almanak.gateway.proto import gateway_pb2

        if self._rpc_stub is None:
            logger.warning("Gateway client not connected")
            return None

        try:
            params = json.dumps([{"to": to, "data": data}, "latest"])
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

    def query_position_liquidity(
        self,
        chain: str,
        position_manager: str,
        token_id: int,
    ) -> int | None:
        """Query Uniswap V3 position liquidity via gateway.

        Args:
            chain: Chain identifier (e.g., "arbitrum", "base")
            position_manager: NFT Position Manager contract address
            token_id: Position NFT token ID

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
