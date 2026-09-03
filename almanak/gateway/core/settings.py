"""Gateway configuration using Pydantic Settings."""

import importlib
import logging
import math
from pathlib import Path
from typing import Annotated, Any

from pydantic import BaseModel, ValidationInfo, field_serializer, field_validator
from pydantic_settings import BaseSettings, NoDecode

from almanak.core.rpc_network import Network

logger = logging.getLogger(__name__)

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

    # Bind HTTP externally only when the deployment boundary provides equivalent protection.
    host: str = "127.0.0.1"
    port: int = 8000
    debug: bool = False
    log_level: str = "info"

    # Bind gRPC externally only when the deployment boundary provides equivalent protection.
    grpc_host: str = "127.0.0.1"
    grpc_port: int = 50051
    grpc_max_workers: int = 10

    # Strategy-side client deadline in seconds; the gateway server does not enforce it.
    timeout: float = 30.0

    network: Network = Network.MAINNET

    # NoDecode preserves comma-separated env input for normalization; empty permits on-demand chains.
    chains: Annotated[list[str], NoDecode] = []

    # Last-resort env prices are opt-in because incorrect values corrupt slippage and teardown decisions.
    enable_manual_price_overrides: bool = False

    # Stable/USD uses a $1 fast path unless live multi-source de-peg verification is enabled.
    stablecoin_verify: bool = False

    # Run a non-blocking Chainlink check every N peg-served calls; non-positive disables it.
    stablecoin_chainlink_check_interval: int = 50

    # Warn once per token outage streak after N failures; non-positive disables warnings, not peg serving.
    stablecoin_verifier_failure_warning_threshold: int = 3

    # Bound individual sources and the aggregate; timeouts stay unmeasured, never zero.
    # Non-positive values disable the respective bound.
    price_source_timeout_seconds: float = 10.0
    price_aggregator_timeout_seconds: float = 15.0

    # The registered handler returns UNAVAILABLE while disabled. Hosted deployments should
    # enable it only after provider credentials and egress are provisioned.
    pool_history_enabled: bool = False

    # Handlers clamp history windows to these per-resolution soft caps.
    pool_history_max_days_1h: int = 90
    pool_history_max_days_4h: int = 180
    pool_history_max_days_1d: int = 730

    # Independent entry and byte ceilings bound pool-history memory use.
    pool_history_cache_max_entries: int = 5000
    pool_history_cache_max_bytes: int = 64 * 1024 * 1024

    # Funding history has an independent memory budget from pool history.
    funding_history_cache_max_entries: int = 5000
    funding_history_cache_max_bytes: int = 64 * 1024 * 1024

    # At this monthly query budget, The Graph is skipped and dispatch falls through.
    pool_history_thegraph_monthly_budget_max: int = 100000

    # Rows newer than a provider's cutoff remain provisional and use the short-TTL cache band.
    pool_history_finality_cutoff_seconds_the_graph: int = 86400
    pool_history_finality_cutoff_seconds_defillama: int = 259200
    pool_history_finality_cutoff_seconds_coingecko_onchain: int = 86400

    # Oversized responses return the oldest capped rows and a cursor for forward pagination.
    pool_history_page_cap_rows_the_graph: int = 100000
    pool_history_page_cap_rows_defillama: int = 100000
    pool_history_page_cap_rows_coingecko_onchain: int = 100000

    metrics_enabled: bool = True
    metrics_port: int = 9090

    audit_enabled: bool = True
    audit_log_level: str = "info"

    # Third-party API secrets remain gateway-side.
    alchemy_api_key: str | None = None
    coingecko_api_key: str | None = None
    # Connector manifests contribute fields to this environment loader. Polymarket local EOA
    # mode derives absent L2 credentials lazily.
    thegraph_api_key: str | None = None
    portfolio_api_key: str | None = None
    portfolio_api_provider: str = "zerion"
    portfolio_api_cache_ttl: int = 300

    # Ordered multi-provider valuation takes precedence; each provider reads its own API key.
    portfolio_providers: str | None = None

    tenderly_account_slug: str | None = None
    tenderly_project_slug: str | None = None
    tenderly_access_key: str | None = None
    dexscreener_min_liquidity_usd: float = 10_000.0
    dexscreener_min_volume_usd: float = 1_000.0
    dexscreener_min_turnover_ratio: float = 0.05
    dexscreener_dominance_multiple: float = 3.0
    anvil_watchdog_interval: float = 5.0

    # Signing material stays gateway-side; EVM keys are hex secp256k1 and Solana keys are base58 Ed25519.
    private_key: str | None = None
    solana_private_key: str | None = None

    # Direct Safe mode signs locally; Zodiac mode delegates EOA signing to the remote signer service.
    safe_address: str | None = None
    safe_mode: str | None = None
    eoa_address: str | None = None
    zodiac_roles_address: str | None = None
    signer_service_url: str | None = None
    signer_service_jwt: str | None = None

    database_url: str | None = None

    gateway_db_path: str = DEFAULT_GATEWAY_DB_PATH

    # timeline_db_path overrides gateway_db_path only for timeline events.
    timeline_db_path: str | None = None

    # Bound startup hydration so shared hosted history cannot exhaust gateway memory.
    timeline_startup_load_limit: int = 10000

    slack_webhook_url: str | None = None
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None

    # When set, clients must provide this shared secret in gRPC metadata.
    auth_token: str | None = None

    # Insecure startup is for local development only; hosted gateways must require authentication.
    allow_insecure: bool = False

    # Managed environments fail instead of spawning an uncredentialed gateway; local and self-hosted may auto-start.
    no_spawn: bool = False

    # Mutation RPCs require this in x-operator-token; unset falls back to auth_token-only authentication.
    operator_token: str | None = None

    # Local standalone mode may use the utility DB; strategy mode requires a strategy-scoped DB.
    # Hosted mode ignores this field.
    standalone: bool = False

    # Only the hosted strategy gateway writes lifecycle state; the dashboard gateway is read-only.
    # Local gateways always write their own lifecycle state.
    lifecycle_writer: bool = False

    # The managed child may expose a redacted, bounded startup failure to its controller.
    startup_error_file: str | None = None

    model_config = {
        "env_prefix": "ALMANAK_GATEWAY_",
        "extra": "ignore",
    }

    @field_validator("timeout")
    @classmethod
    def _validate_timeout(cls, value: float) -> float:
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
        # NaN thresholds bypass comparisons; non-positive watchdog intervals hot-loop.
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
        if not math.isfinite(value):
            raise ValueError(f"{info.field_name} must be a finite number (got {value!r})")
        return value

    @field_validator("network", mode="before")
    @classmethod
    def _parse_network(cls, value: object) -> Network:
        """Parse the env/config wire value once at the gateway boundary."""
        return Network.parse(value)

    @field_serializer("network")
    def _serialize_network(self, value: Network) -> str:
        """Keep the existing lowercase string representation on config wires."""
        return value.value

    @field_validator("chains", mode="before")
    @classmethod
    def _normalize_chains(cls, value: object) -> list[str]:
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
                # Preserve unknown aliases so staged chain support does not block startup.
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
        "funding_history_cache_max_entries",
        "funding_history_cache_max_bytes",
        mode="before",
    )
    @classmethod
    def _validate_history_cache_caps(cls, value: object, info: ValidationInfo) -> int:
        defaults: dict[str, int] = {
            "pool_history_cache_max_entries": 5000,
            "pool_history_cache_max_bytes": 64 * 1024 * 1024,
            "funding_history_cache_max_entries": 5000,
            "funding_history_cache_max_bytes": 64 * 1024 * 1024,
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

    @field_validator("timeline_startup_load_limit", mode="before")
    @classmethod
    def _validate_timeline_startup_load_limit(cls, value: object) -> int:
        default = 10000
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
        "pool_history_finality_cutoff_seconds_coingecko_onchain",
        mode="before",
    )
    @classmethod
    def _validate_pool_history_finality_cutoff(cls, value: object, info: ValidationInfo) -> int:
        defaults: dict[str, int] = {
            "pool_history_finality_cutoff_seconds_the_graph": 86400,
            "pool_history_finality_cutoff_seconds_defillama": 259200,
            "pool_history_finality_cutoff_seconds_coingecko_onchain": 86400,
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
        "pool_history_page_cap_rows_coingecko_onchain",
        mode="before",
    )
    @classmethod
    def _validate_pool_history_page_cap_rows(cls, value: object, info: ValidationInfo) -> int:
        defaults: dict[str, int] = {
            "pool_history_page_cap_rows_the_graph": 100000,
            "pool_history_page_cap_rows_defillama": 100000,
            "pool_history_page_cap_rows_coingecko_onchain": 100000,
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
