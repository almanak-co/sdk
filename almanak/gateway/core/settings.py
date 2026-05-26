"""Gateway configuration using Pydantic Settings."""

import logging
import math
from pathlib import Path
from typing import Annotated

from pydantic import ValidationInfo, field_validator
from pydantic_settings import BaseSettings, NoDecode

# VIB-4812: per-connector settings fragments composed into the central
# ``GatewaySettings`` via multi-inheritance. Each fragment is a
# ``BaseModel`` (NOT ``BaseSettings``) — the composed class is the single
# env-loader. The env-var surface (``ALMANAK_GATEWAY_<FIELD>``) is
# preserved byte-identically; adding a new connector field is a one-line
# edit in the connector's ``gateway/settings.py`` plus one extra base
# class on ``GatewaySettings`` below.
from almanak.connectors.enso.gateway.settings import EnsoGatewaySettings
from almanak.connectors.pendle.gateway.settings import PendleGatewaySettings
from almanak.connectors.polymarket.gateway.settings import (
    PolymarketGatewaySettings,
)

logger = logging.getLogger(__name__)

# Default persistent DB path for gateway data (timeline events, instance registry)
DEFAULT_GATEWAY_DB_PATH = str(Path.home() / ".config" / "almanak" / "gateway.db")


class GatewaySettings(
    BaseSettings,
    PolymarketGatewaySettings,
    EnsoGatewaySettings,
    PendleGatewaySettings,
):
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

    # Metrics settings
    metrics_enabled: bool = True
    metrics_port: int = 9090

    # Audit logging settings
    audit_enabled: bool = True
    audit_log_level: str = "info"  # debug, info, warning, error

    # Platform secrets - only gateway has access to these
    alchemy_api_key: str | None = None
    coingecko_api_key: str | None = None
    # ``enso_api_key`` is contributed by ``EnsoGatewaySettings`` (VIB-4812).
    # ``pendle_api_key`` + ``pendle_api_cache_ttl`` by ``PendleGatewaySettings``.
    # ``polymarket_*`` by ``PolymarketGatewaySettings``.
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
    # ``PolymarketGatewaySettings`` (VIB-4812). They are optional: local EOA
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

    # ``polymarket_market_cache_ttl_seconds`` validator is contributed by
    # ``PolymarketGatewaySettings`` (VIB-4812).

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
