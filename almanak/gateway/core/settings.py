"""Gateway configuration using Pydantic Settings."""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings

# Default persistent DB path for gateway data (timeline events, instance registry)
DEFAULT_GATEWAY_DB_PATH = str(Path.home() / ".config" / "almanak" / "gateway.db")


class GatewaySettings(BaseSettings):
    """Gateway configuration from environment variables.

    The gateway server supports both HTTP (FastAPI) and gRPC interfaces:
    - HTTP: External API access (docs, health endpoints)
    - gRPC: Internal strategy-gateway communication (secure, efficient)
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

    # Network settings - "mainnet" for production, "anvil" for local testing
    network: str = "mainnet"

    # Pre-initialize chains (comma-separated). Empty = accept any chain on-demand.
    chains: list[str] = []

    # Metrics settings
    metrics_enabled: bool = True
    metrics_port: int = 9090

    # Audit logging settings
    audit_enabled: bool = True
    audit_log_level: str = "info"  # debug, info, warning, error

    # Platform secrets - only gateway has access to these
    alchemy_api_key: str | None = None
    coingecko_api_key: str | None = None
    pendle_api_key: str | None = None

    # Pendle API settings
    pendle_api_cache_ttl: float = 15.0  # seconds

    # Execution secrets
    private_key: str | None = None

    # Safe wallet integration (for vault operations requiring Safe signing)
    # Set ALMANAK_GATEWAY_SAFE_ADDRESS and ALMANAK_GATEWAY_SAFE_MODE env vars
    safe_address: str | None = None  # Safe wallet address
    safe_mode: str | None = None  # "direct" (Anvil/threshold-1) or "zodiac" (production)
    zodiac_roles_address: str | None = None  # Zodiac Roles module address (zodiac mode)
    signer_service_url: str | None = None  # Remote signer service URL (zodiac mode)
    signer_service_jwt: str | None = None  # Remote signer service JWT (zodiac mode)

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

    model_config = {
        "env_prefix": "ALMANAK_GATEWAY_",
        "env_file": ".env",
        "extra": "ignore",
    }


@lru_cache
def get_settings() -> GatewaySettings:
    """Get cached gateway settings."""
    return GatewaySettings()
