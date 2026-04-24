"""Phase helpers for ``GatewayServer.start`` (Phase 8.3d).

``GatewayServer.start`` used to be a single 280-LOC, CC-30 method that wove
together twelve distinct concerns of the gateway bootstrap sequence. Each
concern is isolated below so the main method can stay declarative and every
branch is directly unit-testable without spinning up a gRPC server or binding
a port:

1. :func:`build_interceptors` - auth/audit/metrics interceptor chain and the
   conflict-validation that must happen BEFORE any port bind.
2. :func:`initialize_timeline_store` - PostgreSQL-vs-SQLite selection for the
   shared :class:`TimelineStore` singleton.
3. :func:`initialize_instance_registry` - SQLite-backed :class:`InstanceRegistry`
   plus VIB-1279 startup reconciliation that marks ghost RUNNING rows STALE.
4. :func:`initialize_lifecycle_store` - platform :class:`LifecycleStore`
   singleton (shares the gateway DB).
5. :func:`log_pricing_source_configuration` - informational log about CoinGecko
   presence. Load-bearing for operator dashboards and support triage.
6. :func:`load_wallet_registry` - optional plugin discovery via entry points,
   guarded on ``ALMANAK_GATEWAY_WALLETS`` env var.
7. :func:`build_reflection_service_names` - static tuple of proto service names
   fed into the gRPC reflection service.

The helpers intentionally do NOT touch ``self`` - they accept exactly the
state they need and return exactly the state the caller must persist. This
keeps the decomposition testable and prevents the classic "helper that
quietly mutates the server" footgun.

Error messages and log strings are preserved byte-for-byte from the
pre-refactor code because they are load-bearing for observability grep.
"""

from __future__ import annotations

import logging
import os
from importlib.metadata import entry_points
from typing import TYPE_CHECKING, Any

from grpc_health.v1 import health_pb2
from grpc_reflection.v1alpha import reflection

from almanak.gateway.audit import AuditInterceptor
from almanak.gateway.auth import AuthInterceptor
from almanak.gateway.lifecycle import get_lifecycle_store
from almanak.gateway.metrics import MetricsInterceptor

if TYPE_CHECKING:  # pragma: no cover - typing only
    from almanak.gateway.core.settings import GatewaySettings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Phase 1: interceptor chain
# ---------------------------------------------------------------------------
def _handle_insecure_mode(settings: GatewaySettings) -> None:
    """Validate and log the insecure-mode configuration.

    Raises ``RuntimeError`` if the operator set ``allow_insecure=True`` AND
    ``auth_token`` on a non-test network - that contradictory config must not
    silently drop authentication on mainnet.
    """
    network = settings.network
    is_test_network = network in ("anvil", "sepolia")

    if not is_test_network and settings.auth_token:
        raise RuntimeError(
            f"Gateway startup aborted: conflicting configuration on network '{network}'. "
            "ALMANAK_GATEWAY_ALLOW_INSECURE=true is set alongside ALMANAK_GATEWAY_AUTH_TOKEN. "
            "Pick one: unset ALMANAK_GATEWAY_ALLOW_INSECURE to keep auth enabled, "
            "or unset ALMANAK_GATEWAY_AUTH_TOKEN to run unauthenticated (NOT RECOMMENDED on mainnet)."
        )

    if not is_test_network:
        logger.warning(
            "INSECURE MODE on network '%s': Auth interceptor disabled. "
            "Gateway authentication is DISABLED on a production network. "
            "Remove ALMANAK_GATEWAY_ALLOW_INSECURE to require auth.",
            network,
        )
    else:
        logger.warning(
            "INSECURE MODE: Auth interceptor disabled. This is acceptable for local development on '%s'.",
            network,
        )

    if settings.auth_token:
        logger.warning("Configured auth token ignored because allow_insecure=True on test network '%s'", network)


def build_interceptors(settings: GatewaySettings) -> list[Any]:
    """Return the ordered gRPC interceptor chain (auth, audit, metrics).

    Order matters: auth runs first so unauthenticated requests are rejected
    before we pay the cost of audit / metrics serialization.

    Raises ``RuntimeError`` when configuration is internally inconsistent
    (``allow_insecure`` on mainnet with an auth token, or no auth configured
    at all).
    """
    interceptors: list[Any] = []

    if settings.allow_insecure:
        _handle_insecure_mode(settings)
    elif settings.auth_token:
        interceptors.append(AuthInterceptor(settings.auth_token))
        logger.info("Auth interceptor enabled - token authentication required")
    else:
        raise RuntimeError(
            "Gateway startup aborted: No auth_token configured. "
            "Set ALMANAK_GATEWAY_AUTH_TOKEN environment variable or "
            "set allow_insecure=True for local development."
        )

    if settings.audit_enabled:
        interceptors.append(
            AuditInterceptor(
                enabled=True,
                log_level=settings.audit_log_level,
            )
        )
        logger.info("Audit interceptor enabled (level=%s)", settings.audit_log_level)

    if settings.metrics_enabled:
        interceptors.append(MetricsInterceptor())
        logger.info("Metrics interceptor enabled")

    return interceptors


# ---------------------------------------------------------------------------
# Phase 3: storage bootstrap
# ---------------------------------------------------------------------------
def initialize_timeline_store(settings: GatewaySettings, timeline_factory: Any) -> None:
    """Initialize the shared :class:`TimelineStore` singleton.

    PostgreSQL when ``settings.database_url`` is set; otherwise SQLite at the
    effective timeline path (``timeline_db_path`` override wins over
    ``gateway_db_path``).

    Takes ``timeline_factory`` explicitly so callers can inject
    ``get_timeline_store`` without helper code importing it at module scope
    (which would bypass the ``almanak.gateway.server.get_timeline_store`` alias
    that tests patch).
    """
    if settings.database_url:
        timeline_factory(database_url=settings.database_url)
        logger.debug("TimelineStore initialized with PostgreSQL backend")
    else:
        effective_timeline_db = settings.timeline_db_path or settings.gateway_db_path
        timeline_factory(db_path=effective_timeline_db)
        logger.debug(f"TimelineStore initialized with SQLite: {effective_timeline_db}")


def initialize_instance_registry(settings: GatewaySettings) -> Any:
    """Initialize :class:`InstanceRegistry`, run VIB-1279 startup reconciliation,
    and return the registry.

    The registry walks every RUNNING-marked row from the previous gateway
    process and marks them STALE. Strategies that are actually alive will
    heartbeat back to RUNNING within the first heartbeat interval.
    """
    from almanak.gateway.registry import get_instance_registry

    registry = get_instance_registry(db_path=settings.gateway_db_path)
    logger.debug(f"InstanceRegistry initialized with persistent storage: {settings.gateway_db_path}")

    stale_count = registry.reconcile_stale_on_startup()
    if stale_count:
        logger.warning("Gateway startup: reconciled %d ghost RUNNING instance(s) -> STALE", stale_count)
    return registry


def initialize_lifecycle_store(settings: GatewaySettings) -> Any:
    """Initialize the :class:`LifecycleStore` singleton (platform DB or SQLite)."""
    store = get_lifecycle_store(
        database_url=settings.database_url,
        sqlite_path=settings.gateway_db_path,
    )
    logger.debug("LifecycleStore initialized")
    return store


# ---------------------------------------------------------------------------
# Phase 4: pricing source log
# ---------------------------------------------------------------------------
def log_pricing_source_configuration(settings: GatewaySettings) -> None:
    """Log the pricing source stack when no CoinGecko API key is set.

    Absence of a CoinGecko key means the gateway falls back to Chainlink
    oracles + free CoinGecko. Operators grep for this exact string when
    triaging unexpected pricing behaviour.
    """
    if not settings.coingecko_api_key:
        logger.info(
            "No CoinGecko API key -- using on-chain pricing (Chainlink oracles) "
            "with free CoinGecko as fallback. Set COINGECKO_API_KEY "
            "for CoinGecko as primary source."
        )


# ---------------------------------------------------------------------------
# Phase 6: wallet-registry plugin discovery
# ---------------------------------------------------------------------------
def _log_wallet_registry_contents(wallet_registry: Any) -> None:
    """Log each chain/address/type resolved by the wallet registry at startup."""
    for chain in wallet_registry.all_chains():
        resolved = wallet_registry.resolve(chain)
        address = resolved.account_address
        redacted = address[:10] + "..." if len(address) > 10 else address
        logger.info(
            "Wallet config: chain=%s address=%s type=%s",
            chain,
            redacted,
            resolved.kind,
        )


def load_wallet_registry(settings: GatewaySettings) -> Any | None:
    """Discover and load the wallet-registry plugin via entry points.

    Only active when ``ALMANAK_GATEWAY_WALLETS`` is set in the environment.
    Returns the loaded registry instance, or ``None`` when either the env var
    is absent or the plugin package is not installed.

    The legacy Safe env vars (``SAFE_WALLET_ADDRESS``, ``ALMANAK_GATEWAY_SAFE_MODE``)
    use the pre-registry path and are intentionally NOT intercepted here.
    """
    if not os.environ.get("ALMANAK_GATEWAY_WALLETS"):
        return None

    wallet_eps = entry_points(group="almanak.wallets")
    registry_eps = [ep for ep in wallet_eps if ep.name == "registry"]

    if not registry_eps:
        logger.warning(
            "ALMANAK_GATEWAY_WALLETS is set but wallet plugin is not installed. "
            "Per-chain wallet config will be ignored. Install almanak-platform-plugins."
        )
        return None

    registry_cls = registry_eps[0].load()
    wallet_registry = registry_cls.from_env(default_chains=settings.chains or None)
    logger.info("Wallet registry plugin loaded: %s", registry_cls.__name__)

    if os.environ.get("SAFE_WALLET_ADDRESS"):
        logger.warning(
            "Both ALMANAK_GATEWAY_WALLETS and SAFE_WALLET_ADDRESS are set. "
            "ALMANAK_GATEWAY_WALLETS takes precedence; legacy safe env vars are ignored."
        )

    if wallet_registry is not None:
        _log_wallet_registry_contents(wallet_registry)
    return wallet_registry


# ---------------------------------------------------------------------------
# Phase 8: reflection service names
# ---------------------------------------------------------------------------
def build_reflection_service_names() -> tuple[str, ...]:
    """Return the full list of proto service names exposed via gRPC reflection.

    Operator tooling (``grpcurl``, CLI dashboards) discovers services through
    reflection. Every proto service that ships must appear in this tuple or it
    becomes invisible to those tools.
    """
    return (
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
