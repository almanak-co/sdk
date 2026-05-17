"""Gateway configuration using Pydantic Settings."""

import logging
import math
from pathlib import Path
from typing import Annotated

from pydantic import ValidationInfo, field_validator
from pydantic_settings import BaseSettings, NoDecode

logger = logging.getLogger(__name__)

# Default persistent DB path for gateway data (timeline events, instance registry)
DEFAULT_GATEWAY_DB_PATH = str(Path.home() / ".config" / "almanak" / "gateway.db")


class GatewaySettings(BaseSettings):
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

    # Metrics settings
    metrics_enabled: bool = True
    metrics_port: int = 9090

    # Audit logging settings
    audit_enabled: bool = True
    audit_log_level: str = "info"  # debug, info, warning, error

    # Platform secrets - only gateway has access to these
    alchemy_api_key: str | None = None
    coingecko_api_key: str | None = None
    enso_api_key: str | None = None
    pendle_api_key: str | None = None
    thegraph_api_key: str | None = None
    portfolio_api_key: str | None = None
    portfolio_api_provider: str = "zerion"
    portfolio_api_cache_ttl: int = 300

    # Multi-provider portfolio valuation (takes precedence over single portfolio_api_key).
    # Comma-separated provider names in priority order, e.g. "zerion,moralis".
    # Each provider reads its API key from {NAME}_API_KEY env var.
    portfolio_providers: str | None = None

    # Pendle API settings
    pendle_api_cache_ttl: float = 15.0  # seconds

    # Gateway-side third-party integrations / service thresholds.
    tenderly_account_slug: str | None = None
    tenderly_project_slug: str | None = None
    tenderly_access_key: str | None = None
    dexscreener_min_liquidity_usd: float = 10_000.0
    dexscreener_min_volume_usd: float = 1_000.0
    dexscreener_min_turnover_ratio: float = 0.05
    dexscreener_dominance_multiple: float = 3.0
    polymarket_network: str = "mainnet"
    polymarket_market_cache_ttl_seconds: float = 60.0
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

    # Polymarket gateway-owned credentials/configuration. These are optional:
    # local EOA mode derives the signer from the gateway execution identity and
    # lazy-derives L2 credentials automatically when absent.
    polymarket_wallet_address: str | None = None
    polymarket_private_key: str | None = None
    polymarket_api_key: str | None = None
    polymarket_secret: str | None = None
    polymarket_passphrase: str | None = None

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
    # explicitly. Hosted mode (``AGENT_ID`` set) ignores this field entirely.
    standalone: bool = False

    # ALM-2732 follow-up: distinguishes the strategy-pod gateway (writer) from
    # the dashboard-pod gateway (reader). Both pods ship the same image with
    # the same AGENT_ID and metrics_db credentials, so a startup write to
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

    @field_validator("polymarket_market_cache_ttl_seconds")
    @classmethod
    def _validate_cache_ttl(cls, value: float) -> float:
        # The legacy ``_parse_polymarket_market_cache_ttl_seconds`` helper
        # clamps to ``[0, 24h]`` for the unprefixed fallback path; mirror
        # ``>= 0`` here so the kwargs / ALMANAK_GATEWAY_* paths agree on the
        # floor, and reject NaN that would defeat the clamp.
        if not math.isfinite(value):
            raise ValueError(f"polymarket_market_cache_ttl_seconds must be a finite number (got {value!r})")
        if value < 0:
            raise ValueError(f"polymarket_market_cache_ttl_seconds must be >= 0 (got {value})")
        return value

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
