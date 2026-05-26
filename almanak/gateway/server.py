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
    PoolAnalyticsServiceServicer,
    PoolHistoryServiceServicer,
    PositionServiceServicer,
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

# States the SDK / gateway own — written by the strategy runner or by the
# gateway itself once a pod is past the deploy phase. Mirror of
# ``LifecycleServiceServicer._VALID_STATES``; kept local so Phase 14 can
# skip its INITIALIZING write whenever the row is already in one of these
# (prevents a sidecar-only restart from regressing a healthy RUNNING agent).
_SDK_OWNED_STATES = frozenset({"INITIALIZING", "RUNNING", "STOPPING", "TEARING_DOWN", "TERMINATED", "ERROR"})


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

        # Publish session topology BEFORE pre-warm so compilers pick it up, and
        # include ALL registry chains (not just requested) so cross-chain
        # intents can resolve destination wallets.
        full_chain_wallets = merge_all_registry_chains(self._wallet_registry, chain_wallet_map)
        self._execution._registered_chain_wallets = full_chain_wallets if full_chain_wallets else None
        self._execution._compiler_cache.clear()

        initialized, prewarm_errors = await prewarm_chains(self._execution, chain_wallet_map)
        errors.extend(prewarm_errors)
        self._execution._registered_chains = set(initialized)

        await reinitialize_market_service(self._market, initialized)

        # Derive a legacy wallet_address for backward compat.
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
        self._executor: futures.ThreadPoolExecutor | None = None
        self._health_servicer = health_aio.HealthServicer()
        self._metrics_server: MetricsServer | None = None
        # Captured from the singleton factory during ``start`` for
        # observability; not used elsewhere today.
        self._instance_registry: Any | None = None

        # Execution servicer (needs reference for RegisterChains pre-warming)
        self._execution_servicer: ExecutionServiceServicer | None = None

        # Servicers that manage HTTP sessions (need cleanup on shutdown)
        self._market_servicer: MarketServiceServicer | None = None
        self._rpc_servicer: RpcServiceServicer | None = None
        self._integration_servicer: IntegrationServiceServicer | None = None
        self._observe_servicer: ObserveServiceServicer | None = None
        self._funding_rate_servicer: FundingRateServiceServicer | None = None
        self._simulation_servicer: SimulationServiceServicer | None = None
        # VIB-4812 — connector-owned servicers (e.g. Polymarket, Enso) are
        # discovered via ``GATEWAY_REGISTRY.capability_providers(GatewayServicerCapability)``
        # in ``_register_services``. The constructed servicer instances are
        # appended to ``self._connector_servicers`` so the shutdown loop can
        # call ``close()`` on each without naming individual providers.
        self._connector_servicers: list[Any] = []
        # VIB-4727: pool analytics (off-chain DefiLlama / GeckoTerminal egress
        # moved from the framework PoolAnalyticsReader to the gateway).
        self._pool_analytics_servicer: PoolAnalyticsServiceServicer | None = None
        # VIB-4728 / POOL-2 (VIB-4750): pool history skeleton. Default-disabled
        # kill-switch (ALMANAK_GATEWAY_POOL_HISTORY_ENABLED) gates the handler;
        # the servicer is always registered so auth + telemetry surfaces work
        # from day 1. POOL-5 (VIB-4753) wires actual providers.
        self._pool_history_servicer: PoolHistoryServiceServicer | None = None
        self._token_servicer: TokenServiceServicer | None = None
        self._lifecycle_servicer: LifecycleServiceServicer | None = None
        self._teardown_servicer: TeardownServiceServicer | None = None
        # T24 / VIB-4210: PositionService for reconciliation control-plane RPC.
        # Holds in-process references to state_servicer + rpc_servicer for
        # registry reads + chain enumeration (ADR §4 algorithm steps 3–4).
        self._position_servicer: PositionServiceServicer | None = None
        self._state_servicer: StateServiceServicer | None = None

        # VIB-1280: background heartbeat TTL enforcer task
        self._heartbeat_ttl_task: asyncio.Task | None = None

        # VIB-3761: handle for the local-DB single-writer flock; held for
        # the gateway lifetime in local mode, ``None`` in hosted mode.
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
        # Phase 0: deployment-mode invariants (VIB-3760).
        # ALMANAK_IS_HOSTED and the gateway's deployment-shape settings must agree.
        # Runs before everything else so a misconfigured restart fails fast,
        # before we touch storage, ports, or interceptors.
        validate_deployment_invariants(self.settings)

        # Phase 1: interceptors + 2: grpc server build
        interceptors = build_interceptors(self.settings)
        self._executor = futures.ThreadPoolExecutor(max_workers=self.settings.grpc_max_workers)
        self.server = grpc.aio.server(self._executor, interceptors=interceptors)

        # Phase 3.25: single-writer flock on the local DB (VIB-3761).
        # Refuses to start a second gateway against the same DB path —
        # OS-level enforcement of the 1 strategy = 1 DB = 1 gateway rule.
        # No-op in hosted mode.
        self._local_db_lock = acquire_local_db_flock(self.settings)

        # Phase 3: storage singletons (timeline, registry, lifecycle).
        # Registry is a process-wide singleton; the returned handle is not
        # needed by ``start`` itself but we capture it for parity with
        # ``lifecycle_store`` and to make the bootstrap read symmetrically.
        initialize_timeline_store(self.settings, get_timeline_store)
        self._instance_registry = initialize_instance_registry(self.settings)
        lifecycle_store = initialize_lifecycle_store(self.settings)

        # Phase 3.5: schema-contract validation (VIB-3763).
        # Refuse to start when the live state backend is missing any column
        # the SDK's accounting writers require — eager so a bad schema fails
        # the supervisor restart loop instead of landing as silent first-
        # iteration accounting failures.
        await validate_state_schema_at_boot(self.settings)

        # Phase 4: pricing-source log
        log_pricing_source_configuration(self.settings)

        # Standard gRPC health protocol
        health_pb2_grpc.add_HealthServicer_to_server(self._health_servicer, self.server)

        # Phase 6: wallet-registry plugin
        wallet_registry = load_wallet_registry(self.settings)
        self._wallet_registry = wallet_registry

        # Phase 7: servicer registration
        self._register_services(wallet_registry, lifecycle_store)

        # Phase 8: reflection + NOT_SERVING + port bind
        reflection.enable_server_reflection(build_reflection_service_names(), self.server)
        # VIB-2413: mark NOT_SERVING BEFORE opening the port so clients cannot
        # race warmup and hit uninitialized providers.
        await self._health_servicer.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
        listen_addr = f"{self.settings.grpc_host}:{self.settings.grpc_port}"
        self.server.add_insecure_port(listen_addr)

        # Phase 9: optional metrics HTTP server
        if self.settings.metrics_enabled:
            self._metrics_server = MetricsServer(port=self.settings.metrics_port)
            self._metrics_server.start()

        # Phase 10: serve + heartbeat TTL enforcer
        await self.server.start()
        logger.info(f"Gateway gRPC server started on {listen_addr}")
        self._heartbeat_ttl_task = asyncio.create_task(
            self._heartbeat_ttl_loop(interval_seconds=60, stale_threshold_seconds=300),
            name="heartbeat-ttl-enforcer",
        )
        logger.debug("Heartbeat TTL enforcer task started (interval=60s, threshold=300s)")

        # Phase 11-12: warmup
        await self._warmup_market_service()
        await self._prewarm_if_chains_known()

        # Phase 13: flip SERVING (VIB-2413)
        await self._health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
        logger.info("Gateway marked SERVING (warmup complete)")

        # Phase 14: announce INITIALIZING in hosted mode (strategy-pod only).
        # Moves the agent out of V2_DEPLOYING as soon as the pod is reachable,
        # so the platform UI can light up step 4 ("Initializing agent") before
        # the strategy container has finished booting. Gated on
        # ``lifecycle_writer`` so the dashboard-pod gateway never participates
        # — see ``GatewaySettings.lifecycle_writer``.
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
            await asyncio.to_thread(lifecycle_store.write_state, aid, "INITIALIZING")
            logger.info("Announced INITIALIZING state for agent %s", aid)
        except Exception:
            logger.exception("Failed to announce INITIALIZING state for agent %s", aid)

    # ------------------------------------------------------------------
    # Phase 7 helper: servicer registration
    # ------------------------------------------------------------------
    def _register_services(self, wallet_registry: Any | None, lifecycle_store: Any) -> None:
        """Build + register every Phase-2/3 servicer on ``self.server``.

        Order matters only where one servicer captures a reference to
        another: execution needs wallet_registry and market_servicer;
        RegisterChains needs execution + market. Every other registration
        is order-independent.
        """
        # Phase 2: execution first (needed by RegisterChains custom health)
        self._execution_servicer = ExecutionServiceServicer(self.settings)
        gateway_pb2_grpc.add_ExecutionServiceServicer_to_server(self._execution_servicer, self.server)
        self._execution_servicer.wallet_registry = wallet_registry

        # Market servicer — created early so RegisterChains can upgrade it
        # from CoinGecko-only to the full 4-source stack once chain info
        # arrives.
        self._market_servicer = MarketServiceServicer(self.settings)
        self._market_servicer.wallet_registry = wallet_registry
        gateway_pb2_grpc.add_MarketServiceServicer_to_server(self._market_servicer, self.server)

        # Custom Health servicer carrying RegisterChains RPC.
        register_chains_servicer = _RegisterChainsServicer(
            self._health_servicer,
            self._execution_servicer,
            self.settings,
            wallet_registry=wallet_registry,
            market_servicer=self._market_servicer,
        )
        gateway_pb2_grpc.add_HealthServicer_to_server(register_chains_servicer, self.server)

        # Cross-reference so execution can self-serve prices through market.
        self._execution_servicer.market_servicer = self._market_servicer

        # Phase 2 state/observe
        state_servicer = StateServiceServicer(self.settings)
        gateway_pb2_grpc.add_StateServiceServicer_to_server(state_servicer, self.server)
        self._state_servicer = state_servicer

        self._observe_servicer = ObserveServiceServicer(self.settings)
        gateway_pb2_grpc.add_ObserveServiceServicer_to_server(self._observe_servicer, self.server)

        # Phase 3 data/integration services
        self._rpc_servicer = RpcServiceServicer(self.settings)
        gateway_pb2_grpc.add_RpcServiceServicer_to_server(self._rpc_servicer, self.server)

        self._integration_servicer = IntegrationServiceServicer(self.settings)
        gateway_pb2_grpc.add_IntegrationServiceServicer_to_server(self._integration_servicer, self.server)

        self._dashboard_servicer = DashboardServiceServicer(self.settings)
        gateway_pb2_grpc.add_DashboardServiceServicer_to_server(self._dashboard_servicer, self.server)

        self._funding_rate_servicer = FundingRateServiceServicer(self.settings)
        gateway_pb2_grpc.add_FundingRateServiceServicer_to_server(self._funding_rate_servicer, self.server)

        self._simulation_servicer = SimulationServiceServicer(self.settings)
        gateway_pb2_grpc.add_SimulationServiceServicer_to_server(self._simulation_servicer, self.server)

        # VIB-4812 — every connector that ships its own gRPC servicer
        # advertises ``GatewayServicerCapability`` on its
        # ``almanak.connectors.<protocol>.gateway.provider`` module. The
        # registry is the discovery surface: ``server.py`` knows nothing
        # about which protocols are connector-owned. Adding a new
        # connector-owned servicer is a one-line registration in
        # ``almanak.connectors._gateway_registry`` plus the connector's own
        # provider module — no edit here.
        #
        # Each provider's ``register_servicers(server, settings)`` performs
        # the underlying ``gateway_pb2_grpc.add_<X>ServiceServicer_to_server``
        # call and stashes the constructed servicer on itself (exposed via
        # ``provider.servicer``). We collect those references so the
        # shutdown loop can call ``close()`` on each without naming
        # individual protocols.
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

        # VIB-4727: pool analytics service. Owns the HTTP egress to
        # DefiLlama / GeckoTerminal so strategy containers do not.
        self._pool_analytics_servicer = PoolAnalyticsServiceServicer(self.settings)
        gateway_pb2_grpc.add_PoolAnalyticsServiceServicer_to_server(self._pool_analytics_servicer, self.server)

        # VIB-4728 / POOL-2: pool history skeleton. Default-disabled; POOL-5
        # wires providers. Registered here so the auth interceptor +
        # telemetry surface engage from day 1.
        self._pool_history_servicer = PoolHistoryServiceServicer(self.settings)
        gateway_pb2_grpc.add_PoolHistoryServiceServicer_to_server(self._pool_history_servicer, self.server)

        self._token_servicer = TokenServiceServicer(self.settings)
        gateway_pb2_grpc.add_TokenServiceServicer_to_server(self._token_servicer, self.server)
        # Wire TokenService into MarketService so balance providers can fall
        # back to the dynamic resolution stack for symbols absent from the
        # static registry (CoinGecko / DexScreener / protocol APIs).
        self._market_servicer._token_servicer = self._token_servicer

        self._lifecycle_servicer = LifecycleServiceServicer(store=lifecycle_store)
        gateway_pb2_grpc.add_LifecycleServiceServicer_to_server(self._lifecycle_servicer, self.server)

        self._teardown_servicer = TeardownServiceServicer(settings=self.settings)
        gateway_pb2_grpc.add_TeardownServiceServicer_to_server(self._teardown_servicer, self.server)

        # T24 / VIB-4210: PositionService — control-plane reconciliation RPC.
        # Holds cross-servicer references for in-process chain enumeration
        # (RpcServiceServicer) + registry reads/writes (StateServiceServicer).
        # The gateway IS the egress layer (CLAUDE.md gateway-boundary rule);
        # in-process invocation of sibling servicers is the correct path,
        # NOT a TCP loopback hop. Wired after rpc_servicer + state_servicer
        # exist so neither attribute can be None at first call.
        self._position_servicer = PositionServiceServicer(self.settings)
        self._position_servicer.rpc_servicer = self._rpc_servicer
        self._position_servicer.state_servicer = self._state_servicer
        self._position_servicer.wallet_registry = wallet_registry
        gateway_pb2_grpc.add_PositionServiceServicer_to_server(self._position_servicer, self.server)

        # VIB-4493 Phase 1C/D: wire PositionService into DashboardService so
        # GetReconciliationReport / PreviewReconcile / ApplyReconcile /
        # RefreshRegistryFromChain can invoke Reconcile in-process. Same
        # cross-servicer pattern as PositionService's own state/rpc refs above.
        self._dashboard_servicer.position_servicer = self._position_servicer

        logger.debug("Registered Phase 2 services: Market, State, Execution, Observe")
        logger.debug("Registered Phase 3 services: Rpc, Integration, FundingRate, Simulation")
        if self._connector_servicers:
            logger.debug(
                "Registered %d connector-owned servicer(s) via GATEWAY_REGISTRY",
                len(self._connector_servicers),
            )
        logger.debug("Registered Dashboard, Token, Lifecycle, Teardown, and Position services")

    # ------------------------------------------------------------------
    # Phase 11 helper: market service warmup
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Phase 12 helper: orchestrator pre-warm guard
    # ------------------------------------------------------------------
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
        # Registry-aware path
        if self._wallet_registry is not None:
            for chain in self._wallet_registry.all_chains():
                try:
                    resolved = self._wallet_registry.resolve(chain)
                    return resolved.account_address
                except Exception:
                    continue
            return None

        # Legacy path: Safe address first, then derive from private key
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

        # VIB-2580: In single-chain Anvil mode, only pre-warm the configured chain.
        # Warming all registry chains triggers RPC calls to non-running Anvil ports
        # (e.g., port 8548 for Base when only Arbitrum/8545 is running), producing
        # ERROR-level "Cannot connect to host" log entries that obscure real issues.
        configured_chains = set(self.settings.chains) if self.settings.chains else set()
        is_anvil_mode = self.settings.network == "anvil"

        # Registry-aware branch: iterate wallet_registry chains
        if self._wallet_registry is not None:
            for chain in self._wallet_registry.all_chains():
                # Skip non-configured chains in Anvil mode to avoid connecting to
                # Anvil ports that aren't running
                if is_anvil_mode and configured_chains and chain not in configured_chains:
                    logger.debug(f"Skipping non-configured chain {chain} in Anvil mode")
                    continue
                try:
                    resolved = self._wallet_registry.resolve(chain)
                    # Skip Solana chains
                    if hasattr(resolved, "family") and str(resolved.family) == "solana":
                        logger.info(f"Skipping Solana chain {chain} during pre-warm")
                        continue
                    wallet_address = resolved.account_address
                    await self._execution_servicer._get_orchestrator(chain, wallet_address)
                    self._execution_servicer._get_compiler(chain, wallet_address)
                    logger.info(f"Pre-warmed orchestrator for chain={chain} (wallet={wallet_address[:10]}...)")
                except Exception as e:
                    logger.warning(f"Failed to pre-warm chain {chain}: {e}")
            return

        # Legacy path: derive wallet from private key / Safe address
        for chain in self.settings.chains:
            await self._prewarm_chain_legacy(chain)

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
        # Cancel background heartbeat TTL enforcer (VIB-1280)
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
        # Shutdown thread pool executor
        if self._executor:
            self._executor.shutdown(wait=True)
        # Close servicer resources (HTTP sessions, etc.)
        # Note: _lifecycle_servicer is excluded -- it delegates to the
        # LifecycleStore singleton whose lifecycle is managed via
        # reset_lifecycle_store() and owns no HTTP sessions.
        gateway_owned_servicers: tuple[Any, ...] = (
            self._market_servicer,
            self._rpc_servicer,
            self._integration_servicer,
            self._observe_servicer,
            self._funding_rate_servicer,
            self._simulation_servicer,
            self._pool_analytics_servicer,
            self._token_servicer,
        )
        # VIB-4812: connector-owned servicers (Polymarket, Enso, …) are
        # discovered via ``GATEWAY_REGISTRY`` at boot and accumulated on
        # ``self._connector_servicers``. Adding a new connector-owned
        # servicer requires no edit here. The capability contract
        # (``GatewayServicerCapability``) mandates only ``register_servicers``;
        # ``close()`` is therefore best-effort for the connector-owned slice —
        # a future connector whose servicer holds no aiohttp / web3 resources
        # may legitimately not implement it.
        #
        # ``close()`` may be sync or async (gateway-owned helpers under
        # ``timeline/store.py``, ``registry/store.py`` and ``lifecycle/`` are
        # synchronous; aiohttp / web3-backed servicers are coroutines).
        # Inspect the return value rather than committing to ``await`` so a
        # future sync-close connector doesn't crash shutdown with ``TypeError:
        # object NoneType can't be used in 'await' expression``. Any error
        # from one ``close()`` is logged and shutdown continues with the
        # remaining servicers — losing a single connector's teardown is
        # better than stranding the rest.
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
        # Allow aiohttp's underlying connectors to finalize cleanup.
        # Without this yield, session.__del__ fires the "Unclosed client session"
        # warning before the TCP transport has been torn down (VIB-1832).
        await asyncio.sleep(_AIOHTTP_SHUTDOWN_GRACE_SECONDS)
        # Reset LifecycleStore singleton so a subsequent start() gets a fresh instance
        reset_lifecycle_store()
        # VIB-3761: release the local-DB single-writer flock so the next
        # gateway run on the same path can acquire it. No-op in hosted mode.
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

    # Install centralized secret redaction on all logging channels
    install_redaction()

    # Initialize structlog for audit logging
    configure_structlog()

    # Fire the deployment-start banner before any other gateway-boot log so
    # users can clearly see where this deployment's logs begin (vs the
    # previous deployment's logs in the same Cloud Logging stream).
    # Banner emission is observability — most failures (e.g. a formatting
    # bug in identity helpers) must not stop the gateway from booting. But
    # ``deployment_id()`` raises ``FatalBootError`` when hosted mode is
    # set with a blank id; that is the hosted-misconfig boot guard and must
    # propagate so the pod refuses to start rather than writing under an
    # empty identity. ``FatalBootError`` is imported lazily to keep
    # ``almanak.framework.deployment`` out of the gateway's module-load
    # closure (enforced by tests/gateway/test_imports_lean.py).
    try:
        emit_gateway_banner(logger)
    except Exception as exc:
        from almanak.framework.deployment.mode import FatalBootError

        if isinstance(exc, FatalBootError):
            raise
        logger.warning(f"Failed to emit deployment-start banner: {exc}")

    # Phase 1 (config-service plan): the standalone gateway entrypoint owns
    # its own dotenv ingest because there is no Click main group to call
    # ``_load_dotenv_once`` for it. ``load_config`` produces a fully-resolved
    # GatewaySettings (incl. unprefixed ALMANAK_* and Polymarket fallbacks).
    from almanak.config.service import load_config

    config = load_config()
    settings = config.gateway
    logger.info(f"Starting gateway with settings: grpc_port={settings.grpc_port}")
    asyncio.run(serve(settings))


if __name__ == "__main__":
    main()
