"""Gateway configuration using Pydantic Settings."""

import importlib
import logging
import math
from pathlib import Path
from typing import Annotated, Any

from pydantic import BaseModel, ValidationInfo, field_validator
from pydantic_settings import BaseSettings, NoDecode

logger = logging.getLogger(__name__)

# Default persistent DB path for gateway data (timeline events, instance registry)
DEFAULT_GATEWAY_DB_PATH = str(Path.home() / ".config" / "almanak" / "gateway.db")


def _connector_descriptor_module() -> Any:
    """Load connector descriptor foundation without a gateway-side import edge."""
    return importlib.import_module("almanak.connectors._connector")


def _load_gateway_settings_base(import_ref: Any) -> type[BaseModel]:
    """Load one manifest-declared gateway settings fragment."""
    settings_cls = import_ref.load()
    connector_discovery_error = _connector_descriptor_module().ConnectorDiscoveryError
    if not isinstance(settings_cls, type) or not issubclass(settings_cls, BaseModel):
        raise connector_discovery_error(
            f"{import_ref.module}.{import_ref.attribute} must be a pydantic BaseModel subclass"
        )
    if issubclass(settings_cls, BaseSettings):
        raise connector_discovery_error(
            f"{import_ref.module}.{import_ref.attribute} must be a pydantic BaseModel fragment, "
            "not a BaseSettings subclass. GatewaySettings is the single gateway env loader."
        )
    return settings_cls


def _gateway_settings_fragment_bases() -> tuple[type[BaseModel], ...]:
    """Return connector-owned settings fragments in deterministic composition order."""
    connector_registry = _connector_descriptor_module().CONNECTOR_REGISTRY
    refs = [
        (connector.name, connector.gateway_settings)
        for connector in connector_registry.with_gateway_settings()
        if connector.gateway_settings is not None
    ]
    ordered_refs = sorted(
        refs,
        key=lambda item: (
            item[1].order is None,
            item[1].order if item[1].order is not None else 0,
            item[0],
        ),
    )
    return tuple(_load_gateway_settings_base(import_ref) for _connector_name, import_ref in ordered_refs)


_GatewaySettingsBase = type(
    "_GatewaySettingsBase",
    (BaseSettings, *_gateway_settings_fragment_bases()),
    {"__module__": __name__},
)


class GatewaySettings(_GatewaySettingsBase):  # type: ignore[valid-type,misc]
    """Gateway configuration from environment variables.

    The gateway server supports both HTTP (FastAPI) and gRPC interfaces:
    - HTTP: External API access (docs, health endpoints)
    - gRPC: Internal strategy-gateway communication (secure, efficient)

    Phase 1 (config-service plan): the unprefixed ``ALMANAK_*`` and bare-name
    fallback ladders that used to live on this class as ``model_validator``
    methods now live at the service boundary in
    :mod:`almanak.config.env`. Construct via
    :func:`almanak.config.env.gateway_config_from_env` (or the higher-level
    :func:`almanak.config.service.load_config`) — calling
    ``GatewaySettings()`` directly only loads ``ALMANAK_GATEWAY_*`` prefixed
    env vars and skips the legacy unprefixed fallbacks.

    The model intentionally has **no** ``env_file`` in ``model_config``:
    dotenv ingest is owned by :func:`almanak.config.env._load_dotenv_once`,
    the single boundary for the SDK. ``GatewaySettings()`` reads only what
    is already in ``os.environ``; entrypoints that need ``.env`` must call
    ``load_config()`` (or ``gateway_config_from_env()``) which loads dotenv
    first.
    """

    # HTTP server settings (FastAPI)
    # Default to localhost for security - explicitly set ALMANAK_GATEWAY_HOST=0.0.0.0
    # to bind externally when needed (e.g., in containers or for external access)
    host: str = "127.0.0.1"
    port: int = 8000
    debug: bool = False
    log_level: str = "info"

    # gRPC server settings
    grpc_host: str = "127.0.0.1"  # Default to localhost for security
    grpc_port: int = 50051
    grpc_max_workers: int = 10

    # Default RPC timeout for gateway client calls (seconds). Read by
    # ``GatewayClientConfig.from_env`` on the strategy-side gRPC client;
    # the gateway server itself does not enforce this timeout — it is the
    # client's deadline. Surfaced on this model so the env reader stays
    # at the service boundary (binds ``ALMANAK_GATEWAY_TIMEOUT``).
    timeout: float = 30.0

    # Network settings - "mainnet" for production, "anvil" for local testing
    network: str = "mainnet"

    # Pre-initialize chains (comma-separated). Empty = accept any chain on-demand.
    # ``NoDecode`` disables pydantic-settings' default JSON decoding for the env
    # var so ``ALMANAK_GATEWAY_CHAINS=bnb,arb`` reaches the field validator below
    # as a plain string. Without this the env-var path raises ``SettingsError``
    # because ``bnb,arb`` is not valid JSON.
    chains: Annotated[list[str], NoDecode] = []

    # Enable the ManualPriceOverrideSource last-resort fallback. When True, the
    # gateway consults ``ALMANAK_PRICE_OVERRIDE_<TOKEN>`` env vars for tokens
    # that every real oracle source failed to price (e.g. W0G on 0G Chain).
    # Off by default because a mis-set env var can feed a wrong price into
    # slippage / teardown decisions. Operators with long-tail tokens should
    # enable it explicitly via ``ALMANAK_GATEWAY_ENABLE_MANUAL_PRICE_OVERRIDES=true``
    # alongside the per-token ``ALMANAK_PRICE_OVERRIDE_<TOKEN>`` values.
    enable_manual_price_overrides: bool = False

    # VIB-4841 / FR-5002 — stablecoin peg fast-path. When False (default), the
    # PriceAggregator short-circuits a stable/USD pair to the $1.00 peg without
    # any upstream price call (the peg is what the aggregate returns anyway), a
    # per-iteration cost cut for RSI/LP strategies that re-fetch USDC/USDT/DAI
    # every loop. Set True to force the full multi-source aggregate for stables
    # (live de-peg verification). Toggle via ``ALMANAK_GATEWAY_STABLECOIN_VERIFY``.
    stablecoin_verify: bool = False

    # How often (1-in-N peg-served calls) the fast-path runs an on-chain
    # Chainlink peg sanity check. Best-effort de-peg detector — logs loudly on
    # drift, never blocks the peg. Non-positive disables the check. Override via
    # ``ALMANAK_GATEWAY_STABLECOIN_CHAINLINK_CHECK_INTERVAL``.
    stablecoin_chainlink_check_interval: int = 50

    # VIB-5375 (RC-3) — PriceAggregator bounded timeouts. Without these a slow /
    # rate-limited non-CoinGecko price source (e.g. a cold Mantle RPC behind the
    # on-chain Chainlink source) could stall the concurrent price fan-out
    # indefinitely, blowing the 30s decide() budget → "timeout, 0 tx" (the Mantle
    # timeout class, VIB-2510/2511). ``price_source_timeout_seconds`` bounds each
    # source's get_price coroutine; ``price_aggregator_timeout_seconds`` bounds the
    # whole concurrent gather. A bounded source is recorded as an error
    # ("unmeasured", never a zero price) and never sinks the aggregate. Defaults
    # sit above each source's internal HTTP timeout and below the decide() budget /
    # 60s pre-warm window. Non-positive disables the respective bound. Override via
    # ``ALMANAK_GATEWAY_PRICE_SOURCE_TIMEOUT_SECONDS`` /
    # ``ALMANAK_GATEWAY_PRICE_AGGREGATOR_TIMEOUT_SECONDS``.
    price_source_timeout_seconds: float = 10.0
    price_aggregator_timeout_seconds: float = 15.0

    # PoolHistoryService kill-switch (VIB-4728 / POOL-2).
    # Default false until POOL-5 wires real providers. The servicer is
    # always REGISTERED on the gRPC server; this flag gates the handler.
    # When false, GetPoolHistory returns UNAVAILABLE with a clear message
    # pointing at VIB-4728. When true but providers are not yet wired
    # (POOL-2 → POOL-5 window), the handler returns UNIMPLEMENTED.
    # POOL-9 acceptance flips the default to true after end-to-end smoke
    # validates the feature (see ``docs/internal/PoolX.md`` POOL-2 §AC).
    # Set via ``ALMANAK_GATEWAY_POOL_HISTORY_ENABLED=true``.
    pool_history_enabled: bool = False

    # PoolHistory soft caps (days). POOL-3 (VIB-4751) exposes these for
    # POOL-6 (VIB-4754) to read when deciding truncation. 90d at 1h is the
    # spike R2 sizing for ~432 KB payloads; 180d at 4h is symmetric;
    # 730d at 1d covers a 2-year backtest. Operators override via
    # ``ALMANAK_GATEWAY_POOL_HISTORY_MAX_DAYS_{1H,4H,1D}``. The validator
    # does NOT enforce these — they are a handler concern (UAT card
    # §"Soft-cap vs hard-cap"). Non-positive overrides fall back to the
    # default via ``_validate_pool_history_max_days`` so a typo can't
    # silently kill the cap.
    pool_history_max_days_1h: int = 90
    pool_history_max_days_4h: int = 180
    pool_history_max_days_1d: int = 730

    # PoolHistory cache caps (POOL-4 / VIB-4752). Two-tier in-memory cache
    # bounds: entries ceiling mirrors the analytics-service ceiling
    # (VIB-4727) so they scale identically; bytes ceiling is sized for a
    # long-uptime gateway hosting multiple strategies without leaking
    # memory under unique-key traffic. Operators override via
    # ``ALMANAK_GATEWAY_POOL_HISTORY_CACHE_MAX_ENTRIES`` and
    # ``ALMANAK_GATEWAY_POOL_HISTORY_CACHE_MAX_BYTES``. Non-positive /
    # malformed overrides fall back to the default via
    # ``_validate_pool_history_cache_caps`` so a typo can't silently
    # disable the bound (mirrors the soft-cap fallback semantics).
    pool_history_cache_max_entries: int = 5000
    pool_history_cache_max_bytes: int = 64 * 1024 * 1024

    # PoolHistory TheGraph monthly-query budget breaker (POOL-5 / VIB-4753).
    # The Graph bills per query against a monthly plan quota. Once the
    # gateway's in-memory monthly query count reaches this max, the TheGraph
    # provider is skipped and the dispatcher falls through to DefiLlama /
    # CoinGecko Onchain (legacy geckoterminal provider key; UAT card D3.F11
    # trip). Operators tune this to their
    # Graph plan via ``ALMANAK_GATEWAY_POOL_HISTORY_THEGRAPH_MONTHLY_BUDGET_MAX``.
    # Non-positive / malformed overrides fall back to the default via
    # ``_validate_pool_history_thegraph_budget`` so a typo can't silently
    # disable the breaker (mirrors the soft-cap fallback semantics).
    pool_history_thegraph_monthly_budget_max: int = 100000

    # PoolHistory per-provider finality cutoff (seconds) — POOL-6 (VIB-4754).
    # A returned series is ``finalized_only`` iff its newest row is older than
    # the serving provider's cutoff; otherwise the trailing bar is provisional
    # and the cache entry is written under the short-TTL ``provisional`` band
    # (see ``_history_cache``). DefiLlama revises daily TVL / volume >24h after
    # the fact (PoolX.md §D4), so its cutoff is longer than The Graph's /
    # CoinGecko Onchain's 24h. Operators override via
    # ``ALMANAK_GATEWAY_POOL_HISTORY_FINALITY_CUTOFF_SECONDS_{THE_GRAPH,DEFILLAMA,GECKOTERMINAL}``.
    # Non-positive / malformed overrides fall back to the default via
    # ``_validate_pool_history_finality_cutoff`` so a typo can't silently mark
    # provisional data as finalized (which would over-cache revisable rows).
    pool_history_finality_cutoff_seconds_the_graph: int = 86400
    pool_history_finality_cutoff_seconds_defillama: int = 259200
    pool_history_finality_cutoff_seconds_geckoterminal: int = 86400

    # PoolHistory per-provider response row ceiling — POOL-6 (VIB-4754). When a
    # provider returns MORE rows than this for the (clamped) window, the handler
    # serves the OLDEST ceiling-many rows with ``truncation_reason=PROVIDER_PAGE_CAP``
    # and ``next_start_ts`` so the caller re-chunks forward. The default equals
    # the providers' internal pagination safety bound (100k), so it is
    # structurally unreachable after soft-cap clamping in production (the
    # largest clamped window is 90d-1h = 2160 rows); it exists so an operator
    # (or the D3.F7 acceptance test) can lower a single provider's ceiling.
    # Operators override via
    # ``ALMANAK_GATEWAY_POOL_HISTORY_PAGE_CAP_ROWS_{THE_GRAPH,DEFILLAMA,GECKOTERMINAL}``.
    # Non-positive / malformed overrides fall back to the default via
    # ``_validate_pool_history_page_cap_rows`` so a typo can't silently disable
    # the (already huge) ceiling.
    pool_history_page_cap_rows_the_graph: int = 100000
    pool_history_page_cap_rows_defillama: int = 100000
    pool_history_page_cap_rows_geckoterminal: int = 100000

    # Metrics settings
    metrics_enabled: bool = True
    metrics_port: int = 9090

    # Audit logging settings
    audit_enabled: bool = True
    audit_log_level: str = "info"  # debug, info, warning, error

    # Platform secrets - only gateway has access to these
    alchemy_api_key: str | None = None
    coingecko_api_key: str | None = None
    # Connector-specific gateway fields are contributed by manifest-declared
    # settings fragments. The composed class remains the single env-loader.
    thegraph_api_key: str | None = None
    portfolio_api_key: str | None = None
    portfolio_api_provider: str = "zerion"
    portfolio_api_cache_ttl: int = 300

    # Multi-provider portfolio valuation (takes precedence over single portfolio_api_key).
    # Comma-separated provider names in priority order, e.g. "zerion,moralis".
    # Each provider reads its API key from {NAME}_API_KEY env var.
    portfolio_providers: str | None = None

    # Gateway-side third-party integrations / service thresholds.
    tenderly_account_slug: str | None = None
    tenderly_project_slug: str | None = None
    tenderly_access_key: str | None = None
    dexscreener_min_liquidity_usd: float = 10_000.0
    dexscreener_min_volume_usd: float = 1_000.0
    dexscreener_min_turnover_ratio: float = 0.05
    dexscreener_dominance_multiple: float = 3.0
    anvil_watchdog_interval: float = 5.0

    # Execution secrets
    private_key: str | None = None  # EVM (hex secp256k1)
    solana_private_key: str | None = None  # Solana (base58 Ed25519)

    # Safe wallet integration (for vault operations requiring Safe signing)
    # Set ALMANAK_GATEWAY_SAFE_ADDRESS and ALMANAK_GATEWAY_SAFE_MODE env vars
    safe_address: str | None = None  # Safe wallet address
    safe_mode: str | None = None  # "direct" (Anvil/threshold-1) or "zodiac" (production)
    eoa_address: str | None = None  # EOA address (zodiac mode, key held by signer service)
    zodiac_roles_address: str | None = None  # Zodiac Roles module address (zodiac mode)
    signer_service_url: str | None = None  # Remote signer service URL (zodiac mode)
    signer_service_jwt: str | None = None  # Remote signer service JWT (zodiac mode)

    # Polymarket gateway-owned credentials/configuration are contributed by
    # the Polymarket connector's settings fragment. They are optional: local EOA
    # mode derives the signer from the gateway execution identity and
    # lazy-derives L2 credentials automatically when absent.

    # State persistence
    database_url: str | None = None

    # Unified gateway database path for persistent storage (timeline events, instance registry).
    # Defaults to ~/.config/almanak/gateway.db for persistence across restarts.
    gateway_db_path: str = DEFAULT_GATEWAY_DB_PATH

    # Timeline event persistence (override). If set, timeline uses this path instead of gateway_db_path.
    # If None (default), timeline events are stored in gateway_db_path.
    timeline_db_path: str | None = None

    # Alerting configuration
    slack_webhook_url: str | None = None
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None

    # Authentication - shared secret for gRPC authentication
    # When set, clients must provide this token in metadata to access services
    auth_token: str | None = None

    # Security: Allow running without auth_token (for local development only)
    # When False (default), gateway will fail to start without auth_token configured
    allow_insecure: bool = False

    # VIB-4493 Phase 1 — second-factor token for mutation RPCs on
    # DashboardService (PreviewReconcile / ApplyReconcile /
    # RefreshRegistryFromChain). When set, those handlers require the
    # caller to send the same value in the ``x-operator-token``
    # metadata header in addition to the regular ``auth_token``;
    # mismatch / missing → PERMISSION_DENIED. When unset (default),
    # the handlers fall back to ``auth_token``-only authentication —
    # safe for single-user / local deployments where the operator IS
    # the only caller. A proper RBAC system is the next ticket.
    operator_token: str | None = None

    # VIB-3761/-3835: standalone-mode opt-in. When True, the gateway resolves
    # its local SQLite path through the lenient ``local_db_path`` helper —
    # which can fall back to ``~/.local/share/almanak/utility/almanak_state.db``.
    # When False (default for local mode), the gateway uses the strict
    # ``local_strategy_db_path`` helper, which raises ``LocalPathError`` rather
    # than silently writing to the per-user utility DB. The CLI flag
    # ``almanak gateway --standalone`` is the operator-facing surface; tests
    # and ``almanak ax`` workflows that need a non-strategy gateway pass it
    # explicitly. Hosted mode (``ALMANAK_IS_HOSTED`` set) ignores this field entirely.
    standalone: bool = False

    # ALM-2732 follow-up: distinguishes the strategy-pod gateway (writer) from
    # the dashboard-pod gateway (reader). Both pods ship the same image with
    # the same ALMANAK_IS_HOSTED and metrics_db credentials, so a startup write to
    # ``agent_state`` from both would race — the late writer would clobber
    # whatever the strategy has already reported. Only the strategy-pod
    # gateway gets ``ALMANAK_GATEWAY_LIFECYCLE_WRITER=true``; the dashboard-
    # pod gateway leaves it at the default and stays explicitly read-only
    # for lifecycle state. Local mode ignores this field — the local SDK is
    # always its own writer.
    lifecycle_writer: bool = False

    model_config = {
        "env_prefix": "ALMANAK_GATEWAY_",
        # Intentionally no ``env_file`` — the dotenv boundary lives in
        # ``almanak.config.env._load_dotenv_once``. Construction via
        # ``gateway_config_from_env`` / ``load_config`` calls it; direct
        # ``GatewaySettings()`` reads only the live ``os.environ``.
        "extra": "ignore",
    }

    @field_validator("timeout")
    @classmethod
    def _validate_timeout(cls, value: float) -> float:
        # CodeRabbit review on PR 2156: a non-positive timeout would
        # collapse every gRPC client call into an immediate deadline
        # failure. Reject at the model boundary so the misconfiguration
        # surfaces at boot rather than after first request.
        if value <= 0:
            raise ValueError(f"timeout must be > 0 (got {value})")
        return value

    @field_validator(
        "dexscreener_min_liquidity_usd",
        "dexscreener_min_volume_usd",
        "dexscreener_dominance_multiple",
        "anvil_watchdog_interval",
    )
    @classmethod
    def _validate_positive_float(cls, value: float, info: ValidationInfo) -> float:
        # CodeRabbit review on PR 2324: env.py only normalizes the unprefixed
        # fallback path. Explicit kwargs / ALMANAK_GATEWAY_* still hit these
        # fields directly, so the same NaN / non-positive guards have to live
        # at the model boundary — otherwise a NaN DexScreener threshold
        # silently disables the scam gates and a non-positive watchdog
        # interval hot-loops the watchdog.
        if not math.isfinite(value):
            raise ValueError(f"{info.field_name} must be a finite number (got {value!r})")
        if value <= 0:
            raise ValueError(f"{info.field_name} must be > 0 (got {value})")
        return value

    @field_validator("dexscreener_min_turnover_ratio")
    @classmethod
    def _validate_turnover_ratio(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError(f"dexscreener_min_turnover_ratio must be a finite number (got {value!r})")
        if value < 0 or value > 1:
            raise ValueError(f"dexscreener_min_turnover_ratio must be in [0, 1] (got {value})")
        return value

    @field_validator(
        "price_source_timeout_seconds",
        "price_aggregator_timeout_seconds",
    )
    @classmethod
    def _validate_price_timeout_finite(cls, value: float, info: ValidationInfo) -> float:
        # CodeRabbit review on PR 2984 (VIB-5375): these bounds are clamped with
        # ``max(0.0, value)`` in the aggregator, where non-positive means "disable
        # the bound" (a deliberate sentinel). But a non-finite override escapes that
        # clamp: ``inf`` would make the bound effectively unbounded (re-opening the
        # Mantle timeout-class stall these fields exist to close), and ``NaN`` feeds
        # an undefined ``asyncio.wait(timeout=...)``. Reject non-finite at the model
        # boundary so the misconfiguration fails at boot. ``<= 0`` stays valid (the
        # disable sentinel); only NaN / +-inf are rejected.
        if not math.isfinite(value):
            raise ValueError(f"{info.field_name} must be a finite number (got {value!r})")
        return value

    # ``polymarket_market_cache_ttl_seconds`` validator is contributed by the
    # Polymarket connector's manifest-declared settings fragment.

    @field_validator("chains", mode="before")
    @classmethod
    def _normalize_chains(cls, value: object) -> list[str]:
        # Accepts a CSV string (env var or constructor) or a list of strings.
        # ``NoDecode`` on the field disables pydantic-settings' default JSON
        # decoding so the raw env-var string reaches us here. Each entry is
        # canonicalized via ``resolve_chain_name`` so callers can pass any
        # alias ("bsc"/"bnb"/"binance") and storage stays canonical. Unknown
        # aliases fall through unchanged so the gateway still surfaces a clear
        # error at request time rather than failing to start (lets operators
        # stage support for chains the SDK does not yet recognise).
        if value is None or value == "":
            return []
        if isinstance(value, str):
            raw = [item.strip() for item in value.split(",") if item.strip()]
        elif isinstance(value, list | tuple):
            raw = [str(item).strip() for item in value if item is not None and str(item).strip()]
        else:
            return value  # type: ignore[return-value]

        from almanak.core.constants import resolve_chain_name

        normalized: list[str] = []
        for chain in raw:
            try:
                normalized.append(resolve_chain_name(chain))
            except ValueError:
                normalized.append(chain.lower())
        return normalized

    @field_validator(
        "pool_history_max_days_1h",
        "pool_history_max_days_4h",
        "pool_history_max_days_1d",
        mode="before",
    )
    @classmethod
    def _validate_pool_history_max_days(cls, value: object, info: ValidationInfo) -> int:
        # Soft caps must be > 0; non-positive or malformed env values fall
        # back to the field default so a typo (``MAX_DAYS_1H=0``) can't
        # silently disable the cap. Defaults are sourced from the model
        # so a single edit point updates both branches.
        defaults: dict[str, int] = {
            "pool_history_max_days_1h": 90,
            "pool_history_max_days_4h": 180,
            "pool_history_max_days_1d": 730,
        }
        field_name = info.field_name or ""
        default = defaults[field_name]
        if value is None or value == "":
            return default
        try:
            days = int(value)  # type: ignore[call-overload]
        except (TypeError, ValueError):
            return default
        if days <= 0:
            return default
        return days

    @field_validator(
        "pool_history_cache_max_entries",
        "pool_history_cache_max_bytes",
        mode="before",
    )
    @classmethod
    def _validate_pool_history_cache_caps(cls, value: object, info: ValidationInfo) -> int:
        # Cache caps must be > 0; non-positive or malformed env values
        # fall back to the field default so a typo (``MAX_ENTRIES=0``)
        # can't silently disable the cap.
        defaults: dict[str, int] = {
            "pool_history_cache_max_entries": 5000,
            "pool_history_cache_max_bytes": 64 * 1024 * 1024,
        }
        field_name = info.field_name or ""
        default = defaults[field_name]
        if value is None or value == "":
            return default
        try:
            parsed = int(value)  # type: ignore[call-overload]
        except (TypeError, ValueError):
            return default
        if parsed <= 0:
            return default
        return parsed

    @field_validator("pool_history_thegraph_monthly_budget_max", mode="before")
    @classmethod
    def _validate_pool_history_thegraph_budget(cls, value: object) -> int:
        # The monthly-budget breaker max must be > 0; non-positive or
        # malformed env values fall back to the default so a typo
        # (``...BUDGET_MAX=0``) can't silently disable TheGraph entirely
        # (a max of 0 would trip the breaker on the very first query).
        default = 100000
        if value is None or value == "":
            return default
        try:
            parsed = int(value)  # type: ignore[call-overload]
        except (TypeError, ValueError):
            return default
        if parsed <= 0:
            return default
        return parsed

    @field_validator(
        "pool_history_finality_cutoff_seconds_the_graph",
        "pool_history_finality_cutoff_seconds_defillama",
        "pool_history_finality_cutoff_seconds_geckoterminal",
        mode="before",
    )
    @classmethod
    def _validate_pool_history_finality_cutoff(cls, value: object, info: ValidationInfo) -> int:
        # Per-provider finality cutoff (seconds) must be > 0; non-positive or
        # malformed env values fall back to the field default so a typo
        # (``...CUTOFF_SECONDS_DEFILLAMA=0``) can't silently mark every row
        # finalized (cutoff 0 would treat an in-the-future-tolerance bar as
        # finalized and over-cache revisable data). Defaults are sourced here
        # so a single edit point updates both branches.
        defaults: dict[str, int] = {
            "pool_history_finality_cutoff_seconds_the_graph": 86400,
            "pool_history_finality_cutoff_seconds_defillama": 259200,
            "pool_history_finality_cutoff_seconds_geckoterminal": 86400,
        }
        field_name = info.field_name or ""
        default = defaults[field_name]
        if value is None or value == "":
            return default
        try:
            parsed = int(value)  # type: ignore[call-overload]
        except (TypeError, ValueError):
            return default
        if parsed <= 0:
            return default
        return parsed

    @field_validator(
        "pool_history_page_cap_rows_the_graph",
        "pool_history_page_cap_rows_defillama",
        "pool_history_page_cap_rows_geckoterminal",
        mode="before",
    )
    @classmethod
    def _validate_pool_history_page_cap_rows(cls, value: object, info: ValidationInfo) -> int:
        # Per-provider response row ceiling must be > 0; non-positive or
        # malformed env values fall back to the field default so a typo
        # (``...PAGE_CAP_ROWS_THE_GRAPH=0``) can't silently truncate every
        # response to zero rows. Defaults sourced here for a single edit point.
        defaults: dict[str, int] = {
            "pool_history_page_cap_rows_the_graph": 100000,
            "pool_history_page_cap_rows_defillama": 100000,
            "pool_history_page_cap_rows_geckoterminal": 100000,
        }
        field_name = info.field_name or ""
        default = defaults[field_name]
        if value is None or value == "":
            return default
        try:
            parsed = int(value)  # type: ignore[call-overload]
        except (TypeError, ValueError):
            return default
        if parsed <= 0:
            return default
        return parsed
