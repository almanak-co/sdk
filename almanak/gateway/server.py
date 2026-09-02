"""Gateway gRPC server - mediates all external access for strategy containers.

This module provides the gRPC server that strategy containers connect to.
All platform secrets are held here; strategy containers have no direct
access to external services or credentials.
"""

import asyncio
import inspect
import logging
import signal
from concurrent import futures
from typing import Any

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc
from grpc_health.v1.health import aio as health_aio
from grpc_reflection.v1alpha import reflection

from almanak.core.lifecycle import LifecycleState
from almanak.core.redaction import install_redaction
from almanak.framework.utils.deployment_banner import emit_gateway_banner
from almanak.gateway._server_start_helpers import (
    acquire_local_db_flock,
    build_interceptors,
    build_reflection_service_names,
    initialize_instance_registry,
    initialize_lifecycle_store,
    initialize_timeline_store,
    load_wallet_registry,
    log_pricing_source_configuration,
    validate_deployment_invariants,
    validate_state_schema_at_boot,
)
from almanak.gateway.audit import configure_structlog
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.lifecycle import reset_lifecycle_store
from almanak.gateway.metrics import MetricsServer
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.services import (
    DashboardServiceServicer,
    ExecutionServiceServicer,
    FundingRateServiceServicer,
    IntegrationServiceServicer,
    LifecycleServiceServicer,
    MarketServiceServicer,
    ObserveServiceServicer,
    PerpFillServiceServicer,
    PoolAnalyticsServiceServicer,
    PoolHistoryServiceServicer,
    PositionServiceServicer,
    RateHistoryServiceServicer,
    RpcServiceServicer,
    SimulationServiceServicer,
    StateServiceServicer,
    TeardownServiceServicer,
    TokenServiceServicer,
)
from almanak.gateway.timeline import get_timeline_store

logger = logging.getLogger(__name__)

# Grace period (seconds) after closing servicer sessions to let aiohttp's
# underlying TCP connectors finalize cleanup before the event loop exits.
_AIOHTTP_SHUTDOWN_GRACE_SECONDS = 0.25

# Writable lifecycle states are derived from the canonical vocabulary so boot
# validation cannot drift from service validation.
_SDK_OWNED_STATES = frozenset(state for state in LifecycleState if state.is_writable)


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
        wallet_registry: "Any | None" = None,
        market_servicer: "Any | None" = None,
    ):
        self._health = health_servicer
        self._execution = execution_servicer
        self._settings = settings
        self._wallet_registry: Any = wallet_registry
        self._market: Any = market_servicer

    async def Check(self, request, context):
        return await self._health.Check(request, context)

    async def Watch(self, request, context):
        async for response in self._health.Watch(request, context):
            yield response

    async def RegisterChains(self, request, context):
        """Pre-initialize orchestrators and compilers for requested chains.

        Thin orchestrator: delegates each phase of the workflow to the helper
        module so this method stays declarative. Phases (see
        ``_register_chains_helpers.py`` for the contracts):

        1. Derive the default wallet from settings / request.
        2. Guard: wallet missing AND no registry -> error response.
        3. Resolve per-chain wallets from the registry (first pass).
        4. Guard: Solana chain leaked into the resolved map -> error response.
        5. Validate + map each requested chain to an effective wallet.
        6. Merge in non-requested registry chains so cross-chain intents can
           route.
        7. Publish session topology to the execution servicer and invalidate
           the compiler cache.
        8. Pre-warm orchestrator + compiler for every mapped chain.
        9. Record initialized chains, reinit MarketService, build response.
        """
        from almanak.gateway._register_chains_helpers import (
            derive_default_wallet,
            find_solana_chain_in_wallets,
            merge_all_registry_chains,
            prewarm_chains,
            reinitialize_market_service,
            resolve_requested_chain_wallets,
            validate_and_map_chains,
        )

        chains = list(request.chains)

        wallet_address = derive_default_wallet(self._settings, request.wallet_address)

        if not wallet_address and not self._wallet_registry:
            return gateway_pb2.RegisterChainsResponse(
                success=False,
                error="No wallet_address provided and no private key configured in gateway",
            )

        chain_wallets = resolve_requested_chain_wallets(self._wallet_registry, chains)

        solana_chain = find_solana_chain_in_wallets(chains, chain_wallets)
        if solana_chain is not None:
            return gateway_pb2.RegisterChainsResponse(
                success=False,
                error=f"Wallet registry does not support Solana chain: {solana_chain}",
            )

        chain_wallet_map, errors = validate_and_map_chains(chains, chain_wallets, wallet_address)

        full_chain_wallets = merge_all_registry_chains(self._wallet_registry, chain_wallet_map)
        self._execution._registered_chain_wallets = full_chain_wallets if full_chain_wallets else None
        self._execution._compiler_cache.clear()

        initialized, prewarm_errors = await prewarm_chains(self._execution, chain_wallet_map)
        errors.extend(prewarm_errors)
        self._execution._registered_chains = set(initialized)

        await reinitialize_market_service(self._market, initialized)

        legacy_wallet = wallet_address or (full_chain_wallets.get(initialized[0], "") if initialized else "")

        if errors:
            return gateway_pb2.RegisterChainsResponse(
                success=False,
                initialized_chains=initialized,
                wallet_address=legacy_wallet,
                error="; ".join(errors),
                chain_wallets=full_chain_wallets,
            )

        return gateway_pb2.RegisterChainsResponse(
            success=True,
            initialized_chains=initialized,
            wallet_address=legacy_wallet,
            chain_wallets=full_chain_wallets,
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

    def __init__(self, settings: GatewaySettings):
        """Initialize the gateway server.

        Args:
            settings: Gateway settings. Phase 1 (config-service plan): the
                caller is responsible for constructing settings via
                :func:`almanak.config.service.load_config` (or the lower-level
                :func:`almanak.config.env.gateway_config_from_env`). The old
                ``settings or get_settings()`` fallback was removed because it
                bypassed the service boundary.
        """
        self.settings = settings
        self.server: grpc.aio.Server | None = None
        # Actual port the gRPC server bound, set during ``start``. Equals
        # ``settings.grpc_port`` unless that is 0 (ephemeral bind — the OS
        # picks a free port; tests use this to avoid fixed-port collisions).
        self.bound_port: int | None = None
        self._executor: futures.ThreadPoolExecutor | None = None
        self._health_servicer = health_aio.HealthServicer()
        self._metrics_server: MetricsServer | None = None
        self._instance_registry: Any | None = None

        self._execution_servicer: ExecutionServiceServicer | None = None

        # Servicers that manage HTTP sessions (need cleanup on shutdown)
        self._market_servicer: MarketServiceServicer | None = None
        self._rpc_servicer: RpcServiceServicer | None = None
        self._integration_servicer: IntegrationServiceServicer | None = None
        self._observe_servicer: ObserveServiceServicer | None = None
        self._funding_rate_servicer: FundingRateServiceServicer | None = None
        self._perp_fill_servicer: PerpFillServiceServicer | None = None
        self._simulation_servicer: SimulationServiceServicer | None = None
        # Retain connector-owned servicers for shutdown without naming providers.
        self._connector_servicers: list[Any] = []
        self._pool_analytics_servicer: PoolAnalyticsServiceServicer | None = None
        self._pool_history_servicer: PoolHistoryServiceServicer | None = None
        self._rate_history_servicer: RateHistoryServiceServicer | None = None
        self._token_servicer: TokenServiceServicer | None = None
        self._lifecycle_servicer: LifecycleServiceServicer | None = None
        self._teardown_servicer: TeardownServiceServicer | None = None
        self._position_servicer: PositionServiceServicer | None = None
        self._state_servicer: StateServiceServicer | None = None

        self._heartbeat_ttl_task: asyncio.Task | None = None

        # Held for the gateway lifetime to enforce one local DB and one gateway.
        self._local_db_lock: int | None = None

    async def _heartbeat_ttl_loop(self, interval_seconds: int = 60, stale_threshold_seconds: int = 300) -> None:
        """Background task that persistently marks stale RUNNING entries as STALE.

        Runs every ``interval_seconds`` and marks any RUNNING instance whose
        last_heartbeat_at is older than ``stale_threshold_seconds`` as STALE in
        SQLite.  This catches mid-session crashes that startup reconciliation
        cannot see (VIB-1280).
        """
        from almanak.gateway.registry import get_instance_registry

        registry = get_instance_registry()
        while True:
            try:
                await asyncio.sleep(interval_seconds)
                try:
                    await asyncio.to_thread(
                        registry.enforce_heartbeat_ttl,
                        stale_threshold_seconds=stale_threshold_seconds,
                    )
                except Exception:
                    logger.exception("Heartbeat TTL enforcement failed, will retry next cycle")
            except asyncio.CancelledError:
                return

    async def start(self) -> None:
        """Start the gRPC server.

        Bootstrap is decomposed into phases; each phase below is a helper
        (either a method on this class or a pure function in
        ``_server_start_helpers``) so every branch can be unit-tested without
        binding a real port.
        """
        # Fail fast on deployment-shape mismatches before touching storage or ports.
        validate_deployment_invariants(self.settings)

        interceptors = build_interceptors(self.settings)
        self._executor = futures.ThreadPoolExecutor(max_workers=self.settings.grpc_max_workers)
        self.server = grpc.aio.server(self._executor, interceptors=interceptors)

        # Enforce one strategy, one DB, and one gateway in local mode.
        self._local_db_lock = acquire_local_db_flock(self.settings)

        initialize_timeline_store(self.settings, get_timeline_store)
        self._instance_registry = initialize_instance_registry(self.settings)
        lifecycle_store = initialize_lifecycle_store(self.settings)

        # Refuse to boot when the live schema lacks accounting-writer columns.
        await validate_state_schema_at_boot(self.settings)

        log_pricing_source_configuration(self.settings)

        health_pb2_grpc.add_HealthServicer_to_server(self._health_servicer, self.server)

        wallet_registry = load_wallet_registry(self.settings)
        self._wallet_registry = wallet_registry

        self._register_services(wallet_registry, lifecycle_store)

        reflection.enable_server_reflection(build_reflection_service_names(), self.server)
        # Publish NOT_SERVING before exposing the port to prevent warmup races.
        await self._health_servicer.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
        listen_addr = f"{self.settings.grpc_host}:{self.settings.grpc_port}"
        self.bound_port = self.server.add_insecure_port(listen_addr)

        if self.settings.metrics_enabled:
            self._metrics_server = MetricsServer(port=self.settings.metrics_port)
            self._metrics_server.start()

        await self.server.start()
        logger.info(f"Gateway gRPC server started on {self.settings.grpc_host}:{self.bound_port}")
        self._heartbeat_ttl_task = asyncio.create_task(
            self._heartbeat_ttl_loop(interval_seconds=60, stale_threshold_seconds=300),
            name="heartbeat-ttl-enforcer",
        )
        logger.debug("Heartbeat TTL enforcer task started (interval=60s, threshold=300s)")

        await self._warmup_market_service()
        await self._prewarm_if_chains_known()

        await self._health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
        logger.info("Gateway marked SERVING (warmup complete)")

        # Only the hosted strategy-pod gateway configured as lifecycle writer
        # announces INITIALIZING; dashboard gateways never write lifecycle state.
        await self._announce_initializing(lifecycle_store)

    async def _announce_initializing(self, lifecycle_store: Any) -> None:
        """Write ``INITIALIZING`` to ``agent_state`` for this pod's ALMANAK_IS_HOSTED.

        No-op outside hosted mode, and no-op when ``lifecycle_writer`` is
        false — both pods of an agent run this code path, but only the
        strategy-pod gateway is configured to write.

        Skips the write when the row is already in any state the SDK itself
        owns (RUNNING, STOPPING, TEARING_DOWN, TERMINATED, ERROR,
        INITIALIZING). A K8s native-sidecar gateway can restart on its own
        while the strategy container keeps running healthily; without this
        guard such a restart would clobber RUNNING back to INITIALIZING, and
        because the SDK runner only writes RUNNING at strategy-process
        startup (``_run_loop_helpers.py``), the row would stay regressed
        until the platform reconciler escalated to ``V2_DEPLOY_FAILED``.

        Best-effort: read or write failures are logged and swallowed; the
        SDK runner's later ``RUNNING`` write covers the canonical signal.
        """
        from almanak.framework.deployment.mode import deployment_id, is_hosted

        if not is_hosted() or not self.settings.lifecycle_writer:
            return
        aid = deployment_id()
        if aid is None:
            return
        try:
            current = await asyncio.to_thread(lifecycle_store.read_state, aid)
            if current is not None and current.state in _SDK_OWNED_STATES:
                logger.debug(
                    "Skipping INITIALIZING announce for agent %s — state already %s (SDK-owned)",
                    aid,
                    current.state,
                )
                return
            await asyncio.to_thread(lifecycle_store.write_state, aid, LifecycleState.INITIALIZING)
            logger.info("Announced INITIALIZING state for agent %s", aid)
        except Exception:
            logger.exception("Failed to announce INITIALIZING state for agent %s", aid)

    def _register_services(self, wallet_registry: Any | None, lifecycle_store: Any) -> None:
        """Build + register every Phase-2/3 servicer on ``self.server``.

        Order matters only where one servicer captures a reference to
        another: execution needs wallet_registry and market_servicer;
        RegisterChains needs execution + market. Every other registration
        is order-independent.
        """
        self._execution_servicer = ExecutionServiceServicer(self.settings)
        gateway_pb2_grpc.add_ExecutionServiceServicer_to_server(self._execution_servicer, self.server)
        self._execution_servicer.wallet_registry = wallet_registry

        self._market_servicer = MarketServiceServicer(self.settings)
        self._market_servicer.wallet_registry = wallet_registry
        gateway_pb2_grpc.add_MarketServiceServicer_to_server(self._market_servicer, self.server)

        register_chains_servicer = _RegisterChainsServicer(
            self._health_servicer,
            self._execution_servicer,
            self.settings,
            wallet_registry=wallet_registry,
            market_servicer=self._market_servicer,
        )
        gateway_pb2_grpc.add_HealthServicer_to_server(register_chains_servicer, self.server)

        self._execution_servicer.market_servicer = self._market_servicer

        state_servicer = StateServiceServicer(self.settings)
        gateway_pb2_grpc.add_StateServiceServicer_to_server(state_servicer, self.server)
        self._state_servicer = state_servicer

        self._observe_servicer = ObserveServiceServicer(self.settings)
        gateway_pb2_grpc.add_ObserveServiceServicer_to_server(self._observe_servicer, self.server)

        self._rpc_servicer = RpcServiceServicer(self.settings)
        gateway_pb2_grpc.add_RpcServiceServicer_to_server(self._rpc_servicer, self.server)

        self._integration_servicer = IntegrationServiceServicer(self.settings)
        gateway_pb2_grpc.add_IntegrationServiceServicer_to_server(self._integration_servicer, self.server)

        self._dashboard_servicer = DashboardServiceServicer(self.settings)
        gateway_pb2_grpc.add_DashboardServiceServicer_to_server(self._dashboard_servicer, self.server)

        self._funding_rate_servicer = FundingRateServiceServicer(self.settings)
        gateway_pb2_grpc.add_FundingRateServiceServicer_to_server(self._funding_rate_servicer, self.server)

        self._perp_fill_servicer = PerpFillServiceServicer(self.settings)
        gateway_pb2_grpc.add_PerpFillServiceServicer_to_server(self._perp_fill_servicer, self.server)

        self._simulation_servicer = SimulationServiceServicer(self.settings)
        gateway_pb2_grpc.add_SimulationServiceServicer_to_server(self._simulation_servicer, self.server)

        from almanak.connectors._base.gateway_capabilities import (
            GatewayServicerCapability,
        )
        from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

        self._connector_servicers = []
        # ``type-abstract``: passing a runtime-checkable Protocol class is the
        # documented usage of ``capability_providers``, but mypy treats every
        # Protocol as abstract by default. The runtime check is correct.
        for provider in GATEWAY_REGISTRY.capability_providers(GatewayServicerCapability):  # type: ignore[type-abstract]
            provider.register_servicers(self.server, self.settings)
            # ``servicer`` is part of the GatewayServicerCapability contract
            # (declared on the Protocol). ``None`` is legitimate but rare —
            # means the connector intentionally exposed no concrete
            # servicer for shutdown management.
            if provider.servicer is not None:
                self._connector_servicers.append(provider.servicer)

        self._pool_analytics_servicer = PoolAnalyticsServiceServicer(self.settings)
        gateway_pb2_grpc.add_PoolAnalyticsServiceServicer_to_server(self._pool_analytics_servicer, self.server)

        self._pool_history_servicer = PoolHistoryServiceServicer(self.settings)
        gateway_pb2_grpc.add_PoolHistoryServiceServicer_to_server(self._pool_history_servicer, self.server)

        self._rate_history_servicer = RateHistoryServiceServicer(self.settings)
        gateway_pb2_grpc.add_RateHistoryServiceServicer_to_server(self._rate_history_servicer, self.server)

        self._token_servicer = TokenServiceServicer(self.settings)
        gateway_pb2_grpc.add_TokenServiceServicer_to_server(self._token_servicer, self.server)
        self._market_servicer._token_servicer = self._token_servicer

        self._lifecycle_servicer = LifecycleServiceServicer(store=lifecycle_store)
        gateway_pb2_grpc.add_LifecycleServiceServicer_to_server(self._lifecycle_servicer, self.server)

        self._teardown_servicer = TeardownServiceServicer(settings=self.settings)
        gateway_pb2_grpc.add_TeardownServiceServicer_to_server(self._teardown_servicer, self.server)

        self._position_servicer = PositionServiceServicer(self.settings)
        self._position_servicer.rpc_servicer = self._rpc_servicer
        self._position_servicer.state_servicer = self._state_servicer
        self._position_servicer.wallet_registry = wallet_registry
        gateway_pb2_grpc.add_PositionServiceServicer_to_server(self._position_servicer, self.server)

        self._dashboard_servicer.position_servicer = self._position_servicer

        logger.debug("Registered Phase 2 services: Market, State, Execution, Observe")
        logger.debug("Registered Phase 3 services: Rpc, Integration, FundingRate, Simulation")
        if self._connector_servicers:
            logger.debug(
                "Registered %d connector-owned servicer(s) via GATEWAY_REGISTRY",
                len(self._connector_servicers),
            )
        logger.debug("Registered Dashboard, Token, Lifecycle, Teardown, and Position services")

    async def _warmup_market_service(self) -> None:
        """Pre-warm MarketServiceServicer HTTP/RPC caches.

        Only runs when chains are already configured; wallet-registry
        deployments get chains later via ``RegisterChains`` and must
        lazy-init with the correct chain context (otherwise
        ``_ensure_initialized`` locks to CoinGecko-only). VIB-2392.
        """
        if not (self._market_servicer and self.settings.chains):
            return
        wallet_for_warmup = self._resolve_wallet_address()
        try:
            await self._market_servicer.warmup(wallet_address=wallet_for_warmup)
        except Exception as e:
            logger.warning(f"Market service warmup failed (will lazy-init on first call): {e}")

    async def _prewarm_if_chains_known(self) -> None:
        """Pre-warm execution orchestrators when any chain source is known."""
        if self.settings.chains or (self._wallet_registry and self._wallet_registry.all_chains()):
            try:
                await self._prewarm_chains()
            except Exception as e:
                logger.warning(f"Chain pre-warm failed (will lazy-init on first call): {e}")

    def _resolve_wallet_address(self) -> str | None:
        """Resolve the wallet address from registry or legacy config.

        Returns the first available wallet address (for balance provider warmup),
        or None if no wallet is configured.
        """
        if self._wallet_registry is not None:
            for chain in self._wallet_registry.all_chains():
                try:
                    resolved = self._wallet_registry.resolve(chain)
                    return resolved.account_address
                except Exception:
                    continue
            return None

        safe_mode_enabled = self.settings.safe_mode in ("direct", "zodiac")
        if self.settings.safe_address and safe_mode_enabled:
            return self.settings.safe_address
        if not self.settings.private_key:
            return None
        try:
            from eth_account import Account

            key = self.settings.private_key
            if not key.startswith("0x"):
                key = "0x" + key
            return Account.from_key(key).address
        except Exception:
            return None

    async def _prewarm_chains(self) -> None:
        """Pre-warm execution orchestrators for configured chains."""
        if not self._execution_servicer:
            logger.warning("Cannot pre-warm: execution servicer not available")
            return

        # In single-chain Anvil mode, avoid RPC calls to inactive chain forks.
        configured_chains = set(self.settings.chains) if self.settings.chains else set()
        is_anvil_mode = self.settings.network == "anvil"

        if self._wallet_registry is not None:
            for chain in self._wallet_registry.all_chains():
                if self._skip_chain_in_anvil_mode(chain, configured_chains, is_anvil_mode):
                    continue
                await self._prewarm_registry_chain(chain)
            return

        for chain in self.settings.chains:
            await self._prewarm_chain_legacy(chain)

    @staticmethod
    def _skip_chain_in_anvil_mode(chain: str, configured_chains: set[str], is_anvil_mode: bool) -> bool:
        """Return True when ``chain`` should be skipped because Anvil mode is
        configured for a different chain (VIB-2580). Centralized so the
        pre-warm loop body stays a straight-line sequence of helper calls."""
        if is_anvil_mode and configured_chains and chain not in configured_chains:
            logger.debug(f"Skipping non-configured chain {chain} in Anvil mode")
            return True
        return False

    async def _prewarm_registry_chain(self, chain: str) -> None:
        """Pre-warm orchestrator + compiler for a single wallet-registry chain.

        Solana-family wallets are skipped via the shared
        ``_is_solana_resolved`` predicate in ``_register_chains_helpers``
        (see that helper's docstring for why we compare on the wallet's
        *stated* family rather than ``ChainFamily.SOLANA`` — intentionally
        NOT the W3 migration target).
        """
        from almanak.gateway._register_chains_helpers import _is_solana_resolved

        assert self._wallet_registry is not None
        assert self._execution_servicer is not None
        try:
            resolved = self._wallet_registry.resolve(chain)
            if _is_solana_resolved(resolved):
                logger.info(f"Skipping Solana chain {chain} during pre-warm")
                return
            wallet_address = resolved.account_address
            await self._execution_servicer._get_orchestrator(chain, wallet_address)
            self._execution_servicer._get_compiler(chain, wallet_address)
            logger.info(f"Pre-warmed orchestrator for chain={chain} (wallet={wallet_address[:10]}...)")
        except Exception as e:
            logger.warning(f"Failed to pre-warm chain {chain}: {e}")

    async def _prewarm_chain_legacy(self, chain: str) -> None:
        """Pre-warm a single chain using the legacy private-key path."""
        if not self.settings.private_key:
            logger.warning(f"Cannot pre-warm {chain}: no private key configured")
            return

        from eth_account import Account

        safe_mode_enabled = self.settings.safe_mode in ("direct", "zodiac")
        if self.settings.safe_address and safe_mode_enabled:
            wallet_address = self.settings.safe_address
        else:
            key = self.settings.private_key
            if not key.startswith("0x"):
                key = "0x" + key
            wallet_address = Account.from_key(key).address

        try:
            assert self._execution_servicer is not None
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
        if self._heartbeat_ttl_task and not self._heartbeat_ttl_task.done():
            self._heartbeat_ttl_task.cancel()
            try:
                await self._heartbeat_ttl_task
            except asyncio.CancelledError:
                pass

        if self._metrics_server:
            self._metrics_server.stop()
        if self.server:
            await self._health_servicer.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
            await self.server.stop(grace=grace)
            logger.info("Gateway gRPC server stopped")
        if self._executor:
            self._executor.shutdown(wait=True)
        # _lifecycle_servicer is excluded because it delegates to the
        # LifecycleStore singleton whose lifecycle is managed via
        # reset_lifecycle_store() and owns no HTTP sessions.
        gateway_owned_servicers: tuple[Any, ...] = (
            self._market_servicer,
            self._rpc_servicer,
            self._integration_servicer,
            self._observe_servicer,
            self._funding_rate_servicer,
            self._perp_fill_servicer,
            self._simulation_servicer,
            self._pool_analytics_servicer,
            self._pool_history_servicer,
            self._rate_history_servicer,
            self._token_servicer,
        )
        # Connector close is optional and may be synchronous or asynchronous;
        # one close failure must not prevent the remaining shutdown work.
        for servicer in (*gateway_owned_servicers, *self._connector_servicers):
            if not servicer:
                continue
            close_fn = getattr(servicer, "close", None)
            if close_fn is None:
                continue
            try:
                result = close_fn()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception(
                    "Error closing servicer %s during shutdown",
                    type(servicer).__qualname__,
                )
        # Let aiohttp finalize TCP cleanup before the event loop exits.
        await asyncio.sleep(_AIOHTTP_SHUTDOWN_GRACE_SECONDS)
        reset_lifecycle_store()
        if self._local_db_lock is not None:
            from almanak.framework.local_paths import release_local_db_lock

            release_local_db_lock(self._local_db_lock)
            self._local_db_lock = None

    async def wait_for_termination(self) -> None:
        """Wait until server is terminated."""
        if self.server:
            await self.server.wait_for_termination()


async def serve(settings: GatewaySettings) -> None:
    """Run the gateway server with signal handling.

    Args:
        settings: Gateway settings. Caller resolves via almanak.config.load_config().
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

    install_redaction()

    configure_structlog()

    # Banner failures are best-effort, but FatalBootError must fail boot.
    try:
        emit_gateway_banner(logger)
    except Exception as exc:
        from almanak.framework.deployment.mode import FatalBootError

        if isinstance(exc, FatalBootError):
            raise
        logger.warning(f"Failed to emit deployment-start banner: {exc}")

    from almanak.config.service import load_config

    config = load_config()
    settings = config.gateway
    logger.info(f"Starting gateway with settings: grpc_port={settings.grpc_port}")
    asyncio.run(serve(settings))


if __name__ == "__main__":
    main()
