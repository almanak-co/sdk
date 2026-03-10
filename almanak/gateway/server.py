"""Gateway gRPC server - mediates all external access for strategy containers.

This module provides the gRPC server that strategy containers connect to.
All platform secrets are held here; strategy containers have no direct
access to external services or credentials.
"""

import asyncio
import logging
import signal
from concurrent import futures

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc
from grpc_health.v1.health import aio as health_aio
from grpc_reflection.v1alpha import reflection

from almanak.core.redaction import install_redaction
from almanak.gateway.audit import AuditInterceptor, configure_structlog
from almanak.gateway.auth import AuthInterceptor
from almanak.gateway.core.settings import GatewaySettings, get_settings
from almanak.gateway.lifecycle import get_lifecycle_store, reset_lifecycle_store
from almanak.gateway.metrics import MetricsInterceptor, MetricsServer
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.services import (
    DashboardServiceServicer,
    EnsoServiceServicer,
    ExecutionServiceServicer,
    FundingRateServiceServicer,
    IntegrationServiceServicer,
    LifecycleServiceServicer,
    MarketServiceServicer,
    ObserveServiceServicer,
    PolymarketServiceServicer,
    RpcServiceServicer,
    SimulationServiceServicer,
    StateServiceServicer,
    TokenServiceServicer,
)
from almanak.gateway.timeline import get_timeline_store

logger = logging.getLogger(__name__)


class _RegisterChainsServicer(gateway_pb2_grpc.HealthServicer):
    """Custom Health servicer that adds RegisterChains RPC.

    Delegates Check/Watch to the standard grpc_health servicer and implements
    RegisterChains to pre-warm execution orchestrators and compilers.
    """

    def __init__(
        self,
        health_servicer: health_aio.HealthServicer,
        execution_servicer: "ExecutionServiceServicer",
        settings: "GatewaySettings",
    ):
        self._health = health_servicer
        self._execution = execution_servicer
        self._settings = settings

    async def Check(self, request, context):
        return await self._health.Check(request, context)

    async def Watch(self, request, context):
        async for response in self._health.Watch(request, context):
            yield response

    async def RegisterChains(self, request, context):
        """Pre-initialize orchestrators and compilers for requested chains."""
        chains = list(request.chains)
        wallet_address = request.wallet_address

        # If no wallet_address provided, use Safe address or derive from private key
        if not wallet_address:
            safe_mode_enabled = self._settings.safe_mode in ("direct", "zodiac")
            if self._settings.safe_address and safe_mode_enabled:
                wallet_address = self._settings.safe_address
            elif self._settings.private_key:
                from eth_account import Account

                key = self._settings.private_key
                if not key.startswith("0x"):
                    key = "0x" + key
                wallet_address = Account.from_key(key).address

        if not wallet_address:
            return gateway_pb2.RegisterChainsResponse(
                success=False,
                error="No wallet_address provided and no private key configured in gateway",
            )

        initialized = []
        errors = []
        for chain in chains:
            try:
                await self._execution._get_orchestrator(chain.lower(), wallet_address)
                self._execution._get_compiler(chain.lower(), wallet_address)
                initialized.append(chain.lower())
                logger.info(f"Pre-warmed orchestrator and compiler for {chain}")
            except Exception as e:
                errors.append(f"{chain}: {e}")
                logger.error(f"Failed to pre-warm {chain}: {e}")

        if errors:
            return gateway_pb2.RegisterChainsResponse(
                success=False,
                initialized_chains=initialized,
                wallet_address=wallet_address,
                error="; ".join(errors),
            )

        return gateway_pb2.RegisterChainsResponse(
            success=True,
            initialized_chains=initialized,
            wallet_address=wallet_address,
        )


class GatewayServer:
    """gRPC server that provides controlled access to platform services.

    The gateway server runs as a sidecar container alongside strategy containers.
    It holds all platform secrets and exposes a controlled API for:
    - Market data (prices, balances, indicators)
    - State persistence
    - Intent compilation and execution
    - Observability (logging, alerts, metrics)
    """

    def __init__(self, settings: GatewaySettings | None = None):
        """Initialize the gateway server.

        Args:
            settings: Gateway settings. If None, loads from environment.
        """
        self.settings = settings or get_settings()
        self.server: grpc.aio.Server | None = None
        self._executor: futures.ThreadPoolExecutor | None = None
        self._health_servicer = health_aio.HealthServicer()
        self._metrics_server: MetricsServer | None = None

        # Execution servicer (needs reference for RegisterChains pre-warming)
        self._execution_servicer: ExecutionServiceServicer | None = None

        # Servicers that manage HTTP sessions (need cleanup on shutdown)
        self._market_servicer: MarketServiceServicer | None = None
        self._rpc_servicer: RpcServiceServicer | None = None
        self._integration_servicer: IntegrationServiceServicer | None = None
        self._observe_servicer: ObserveServiceServicer | None = None
        self._funding_rate_servicer: FundingRateServiceServicer | None = None
        self._simulation_servicer: SimulationServiceServicer | None = None
        self._polymarket_servicer: PolymarketServiceServicer | None = None
        self._enso_servicer: EnsoServiceServicer | None = None
        self._token_servicer: TokenServiceServicer | None = None
        self._lifecycle_servicer: LifecycleServiceServicer | None = None

    async def start(self) -> None:
        """Start the gRPC server."""
        # Create interceptors list
        # Auth interceptor runs first to reject unauthenticated requests early
        interceptors = []
        if self.settings.auth_token:
            interceptors.append(AuthInterceptor(self.settings.auth_token))
            logger.info("Auth interceptor enabled - token authentication required")
        elif self.settings.allow_insecure:
            network = self.settings.network
            if network not in ("anvil", "sepolia"):
                logger.warning(
                    "INSECURE MODE on network '%s': Auth interceptor disabled - no auth_token configured. "
                    "Gateway authentication is DISABLED on a production network. "
                    "Set ALMANAK_GATEWAY_AUTH_TOKEN or remove ALMANAK_GATEWAY_ALLOW_INSECURE.",
                    network,
                )
            else:
                logger.warning(
                    "INSECURE MODE: Auth interceptor disabled - no auth_token configured. "
                    "This is acceptable for local development on '%s'.",
                    network,
                )
        else:
            raise RuntimeError(
                "Gateway startup aborted: No auth_token configured. "
                "Set ALMANAK_GATEWAY_AUTH_TOKEN environment variable or "
                "set allow_insecure=True for local development."
            )

        if self.settings.audit_enabled:
            interceptors.append(
                AuditInterceptor(
                    enabled=True,
                    log_level=self.settings.audit_log_level,
                )
            )
            logger.info("Audit interceptor enabled (level=%s)", self.settings.audit_log_level)
        if self.settings.metrics_enabled:
            interceptors.append(MetricsInterceptor())
            logger.info("Metrics interceptor enabled")

        self._executor = futures.ThreadPoolExecutor(max_workers=self.settings.grpc_max_workers)
        self.server = grpc.aio.server(
            self._executor,
            interceptors=interceptors,
        )

        # Determine effective DB path for timeline: explicit override or unified gateway DB
        effective_timeline_db = self.settings.timeline_db_path or self.settings.gateway_db_path

        # Initialize TimelineStore with persistent path
        # This must happen before services are created so they all share the same store
        get_timeline_store(db_path=effective_timeline_db)
        logger.info(f"TimelineStore initialized with persistent storage: {effective_timeline_db}")

        # Initialize InstanceRegistry with the same gateway DB
        from almanak.gateway.registry import get_instance_registry

        get_instance_registry(db_path=self.settings.gateway_db_path)
        logger.info(f"InstanceRegistry initialized with persistent storage: {self.settings.gateway_db_path}")

        # Ensure PostgreSQL schema is up-to-date (idempotent, runs once)
        if self.settings.database_url:
            from almanak.gateway.database import ensure_schema

            await ensure_schema(self.settings.database_url)
            logger.info("PostgreSQL schema initialized")

        # Initialize LifecycleStore (uses same gateway DB or database_url for platform)
        lifecycle_store = get_lifecycle_store(
            database_url=self.settings.database_url,
            sqlite_path=self.settings.gateway_db_path,
        )
        logger.info("LifecycleStore initialized")

        # Log pricing source configuration
        if not self.settings.coingecko_api_key:
            logger.info(
                "No CoinGecko API key -- using on-chain pricing (Chainlink oracles) "
                "with free CoinGecko as fallback. Set ALMANAK_GATEWAY_COINGECKO_API_KEY "
                "for CoinGecko as primary source."
            )

        # Add health service (standard gRPC health protocol)
        health_pb2_grpc.add_HealthServicer_to_server(self._health_servicer, self.server)

        # Add Phase 2 services (execution first, needed for health servicer)
        self._execution_servicer = ExecutionServiceServicer(self.settings)
        gateway_pb2_grpc.add_ExecutionServiceServicer_to_server(self._execution_servicer, self.server)

        # Add custom health servicer with RegisterChains support
        register_chains_servicer = _RegisterChainsServicer(
            self._health_servicer,
            self._execution_servicer,
            self.settings,
        )
        gateway_pb2_grpc.add_HealthServicer_to_server(register_chains_servicer, self.server)

        self._market_servicer = MarketServiceServicer(self.settings)
        gateway_pb2_grpc.add_MarketServiceServicer_to_server(self._market_servicer, self.server)

        state_servicer = StateServiceServicer(self.settings)
        gateway_pb2_grpc.add_StateServiceServicer_to_server(state_servicer, self.server)

        self._observe_servicer = ObserveServiceServicer(self.settings)
        gateway_pb2_grpc.add_ObserveServiceServicer_to_server(self._observe_servicer, self.server)

        # Add Phase 3 services
        self._rpc_servicer = RpcServiceServicer(self.settings)
        gateway_pb2_grpc.add_RpcServiceServicer_to_server(self._rpc_servicer, self.server)

        self._integration_servicer = IntegrationServiceServicer(self.settings)
        gateway_pb2_grpc.add_IntegrationServiceServicer_to_server(self._integration_servicer, self.server)

        # Add Dashboard service
        dashboard_servicer = DashboardServiceServicer(self.settings)
        gateway_pb2_grpc.add_DashboardServiceServicer_to_server(dashboard_servicer, self.server)

        # Add FundingRate service
        self._funding_rate_servicer = FundingRateServiceServicer(self.settings)
        gateway_pb2_grpc.add_FundingRateServiceServicer_to_server(self._funding_rate_servicer, self.server)

        # Add Simulation service
        self._simulation_servicer = SimulationServiceServicer(self.settings)
        gateway_pb2_grpc.add_SimulationServiceServicer_to_server(self._simulation_servicer, self.server)

        # Add Polymarket service
        self._polymarket_servicer = PolymarketServiceServicer(self.settings)
        gateway_pb2_grpc.add_PolymarketServiceServicer_to_server(self._polymarket_servicer, self.server)

        # Add Enso service
        self._enso_servicer = EnsoServiceServicer(self.settings)
        gateway_pb2_grpc.add_EnsoServiceServicer_to_server(self._enso_servicer, self.server)

        # Add Token service
        self._token_servicer = TokenServiceServicer(self.settings)
        gateway_pb2_grpc.add_TokenServiceServicer_to_server(self._token_servicer, self.server)

        # Add Lifecycle service
        self._lifecycle_servicer = LifecycleServiceServicer(store=lifecycle_store)
        gateway_pb2_grpc.add_LifecycleServiceServicer_to_server(self._lifecycle_servicer, self.server)

        logger.info("Registered Phase 2 services: Market, State, Execution, Observe")
        logger.info("Registered Phase 3 services: Rpc, Integration, FundingRate, Simulation, Polymarket, Enso")
        logger.info("Registered Dashboard, Token, and Lifecycle services")

        # Enable reflection for debugging and development
        # Service names must match the proto package (almanak.gateway.proto)
        service_names = (
            health_pb2.DESCRIPTOR.services_by_name["Health"].full_name,
            "almanak.gateway.proto.MarketService",
            "almanak.gateway.proto.StateService",
            "almanak.gateway.proto.ExecutionService",
            "almanak.gateway.proto.ObserveService",
            "almanak.gateway.proto.RpcService",
            "almanak.gateway.proto.IntegrationService",
            "almanak.gateway.proto.DashboardService",
            "almanak.gateway.proto.FundingRateService",
            "almanak.gateway.proto.SimulationService",
            "almanak.gateway.proto.PolymarketService",
            "almanak.gateway.proto.EnsoService",
            "almanak.gateway.proto.TokenService",
            "almanak.gateway.proto.LifecycleService",
            reflection.SERVICE_NAME,
        )
        reflection.enable_server_reflection(service_names, self.server)

        # Mark as serving
        await self._health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)

        # Use grpc_host from settings, default to localhost for security
        listen_addr = f"{self.settings.grpc_host}:{self.settings.grpc_port}"
        self.server.add_insecure_port(listen_addr)

        # Start metrics HTTP server if enabled
        if self.settings.metrics_enabled:
            self._metrics_server = MetricsServer(port=self.settings.metrics_port)
            self._metrics_server.start()

        await self.server.start()
        logger.info(f"Gateway gRPC server started on {listen_addr}")

        # Pre-warm orchestrators if chains are configured
        if self.settings.chains:
            await self._prewarm_chains()

    async def _prewarm_chains(self) -> None:
        """Pre-warm execution orchestrators for configured chains."""
        if not self._execution_servicer or not self.settings.private_key:
            logger.warning("Cannot pre-warm: execution servicer or private key not available")
            return

        from eth_account import Account

        # Use Safe address when configured, otherwise derive from private key
        safe_mode_enabled = self.settings.safe_mode in ("direct", "zodiac")
        if self.settings.safe_address and safe_mode_enabled:
            wallet_address = self.settings.safe_address
        else:
            key = self.settings.private_key
            if not key.startswith("0x"):
                key = "0x" + key
            wallet_address = Account.from_key(key).address

        for chain in self.settings.chains:
            try:
                await self._execution_servicer._get_orchestrator(chain.lower(), wallet_address)
                self._execution_servicer._get_compiler(chain.lower(), wallet_address)
                logger.info(f"Pre-warmed orchestrator for chain={chain}")
            except Exception as e:
                logger.warning(f"Failed to pre-warm chain {chain}: {e}")

    async def stop(self, grace: float = 5.0) -> None:
        """Gracefully stop the server.

        Args:
            grace: Grace period in seconds for in-flight requests.
        """
        if self._metrics_server:
            self._metrics_server.stop()
        if self.server:
            await self._health_servicer.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
            await self.server.stop(grace=grace)
            logger.info("Gateway gRPC server stopped")
        # Shutdown thread pool executor
        if self._executor:
            self._executor.shutdown(wait=True)
        # Close servicer resources (HTTP sessions, etc.)
        # Note: _lifecycle_servicer is excluded -- it delegates to the
        # LifecycleStore singleton whose lifecycle is managed via
        # reset_lifecycle_store() and owns no HTTP sessions.
        for servicer in (
            self._market_servicer,
            self._rpc_servicer,
            self._integration_servicer,
            self._observe_servicer,
            self._funding_rate_servicer,
            self._simulation_servicer,
            self._polymarket_servicer,
            self._enso_servicer,
            self._token_servicer,
        ):
            if servicer:
                await servicer.close()
        # Reset LifecycleStore singleton so a subsequent start() gets a fresh instance
        reset_lifecycle_store()

    async def wait_for_termination(self) -> None:
        """Wait until server is terminated."""
        if self.server:
            await self.server.wait_for_termination()


async def serve(settings: GatewaySettings | None = None) -> None:
    """Run the gateway server with signal handling.

    Args:
        settings: Gateway settings. If None, loads from environment.
    """
    server = GatewayServer(settings)

    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()

    def handle_signal() -> None:
        logger.info("Received shutdown signal")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    await server.start()

    await stop_event.wait()
    await server.stop()


def main() -> None:
    """Entry point for gateway gRPC server."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Install centralized secret redaction on all logging channels
    install_redaction()

    # Initialize structlog for audit logging
    configure_structlog()

    settings = get_settings()
    logger.info(f"Starting gateway with settings: grpc_port={settings.grpc_port}")
    asyncio.run(serve(settings))


if __name__ == "__main__":
    main()
