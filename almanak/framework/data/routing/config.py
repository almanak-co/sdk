"""Data routing configuration and provider protocol.

Defines the DataProvider protocol that all data providers must implement,
plus configuration models for routing, quotas, and strategy-level overrides.

Example:
    from almanak.framework.data.routing.config import (
        DataProvider, ProviderConfig, QuotaConfig, DataRoutingConfig,
    )

    class MyProvider:
        @property
        def name(self) -> str:
            return "my_provider"

        @property
        def data_class(self) -> DataClassification:
            return DataClassification.INFORMATIONAL

        def fetch(self, **kwargs: object) -> DataEnvelope:
            ...

        def health(self) -> dict[str, object]:
            return {"status": "healthy"}

    config = DataRoutingConfig.from_strategy_config({"data_overrides": {"ohlcv": {"primary": "geckoterminal"}}})
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from almanak.framework.data.models import DataClassification, DataEnvelope

logger = logging.getLogger(__name__)


@runtime_checkable
class DataProvider(Protocol):
    """Protocol for interchangeable data providers.

    All data providers (GeckoTerminal, DeFi Llama, Binance, on-chain readers)
    must implement this interface to participate in the DataRouter's
    provider selection and failover logic.

    Methods:
        fetch: Retrieve data for the given parameters. Returns a DataEnvelope.
        health: Return a health status dict with at least a "status" key.

    Properties:
        name: Unique provider identifier (e.g. "geckoterminal", "defillama").
        data_class: Classification controlling fail-closed vs fallback semantics.
    """

    @property
    def name(self) -> str:
        """Unique provider identifier (e.g. 'geckoterminal', 'alchemy_rpc')."""
        ...

    @property
    def data_class(self) -> DataClassification:
        """Classification controlling routing semantics for this provider."""
        ...

    def fetch(self, **kwargs: object) -> DataEnvelope:
        """Fetch data from this provider.

        Args:
            **kwargs: Provider-specific parameters (instrument, chain, timeframe, etc.)

        Returns:
            DataEnvelope wrapping the result with provenance metadata.

        Raises:
            DataSourceError: On any failure (timeout, rate limit, bad response).
        """
        ...

    def health(self) -> dict[str, object]:
        """Return health status for this provider.

        Returns:
            Dict with at least {"status": "healthy"|"degraded"|"unhealthy"}.
            May include additional keys like "latency_ms", "error_count", etc.
        """
        ...


@dataclass(frozen=True)
class QuotaConfig:
    """Quota limits for a data provider to control API usage costs.

    Attributes:
        monthly_limit: Maximum requests allowed per month.
        warn_at_pct: Percentage of monthly_limit at which to emit a warning (0-100).
        hard_stop_at_pct: Percentage of monthly_limit at which to stop requests (0-100).
        current_usage: Current number of requests used this month.
    """

    monthly_limit: int
    warn_at_pct: int = 80
    hard_stop_at_pct: int = 95
    current_usage: int = 0

    def __post_init__(self) -> None:
        if self.monthly_limit <= 0:
            raise ValueError(f"monthly_limit must be positive, got {self.monthly_limit}")
        if not 0 <= self.warn_at_pct <= 100:
            raise ValueError(f"warn_at_pct must be 0-100, got {self.warn_at_pct}")
        if not 0 <= self.hard_stop_at_pct <= 100:
            raise ValueError(f"hard_stop_at_pct must be 0-100, got {self.hard_stop_at_pct}")
        if self.warn_at_pct >= self.hard_stop_at_pct:
            raise ValueError(f"warn_at_pct ({self.warn_at_pct}) must be < hard_stop_at_pct ({self.hard_stop_at_pct})")
        if self.current_usage < 0:
            raise ValueError(f"current_usage must be >= 0, got {self.current_usage}")

    @property
    def usage_pct(self) -> float:
        """Current usage as a percentage of the monthly limit."""
        return (self.current_usage / self.monthly_limit) * 100

    @property
    def is_warning(self) -> bool:
        """Whether usage has exceeded the warning threshold."""
        return self.usage_pct >= self.warn_at_pct

    @property
    def is_exhausted(self) -> bool:
        """Whether usage has exceeded the hard stop threshold."""
        return self.usage_pct >= self.hard_stop_at_pct


@dataclass(frozen=True)
class ProviderConfig:
    """Configuration for a data type's provider routing.

    Specifies which provider is primary, which are fallbacks, timeout
    behavior, and optional quota limits.

    Attributes:
        primary: Name of the primary provider (e.g. "geckoterminal").
        fallback: Ordered list of fallback provider names.
        timeout_ms: Request timeout in milliseconds.
        quota: Optional quota configuration for the primary provider.
    """

    primary: str
    fallback: list[str] = field(default_factory=list)
    timeout_ms: int = 2000
    quota: QuotaConfig | None = None

    def __post_init__(self) -> None:
        if not self.primary:
            raise ValueError("primary provider name cannot be empty")
        if self.timeout_ms <= 0:
            raise ValueError(f"timeout_ms must be positive, got {self.timeout_ms}")
        # Ensure primary is not in fallback list
        if self.primary in self.fallback:
            raise ValueError(f"primary provider '{self.primary}' should not be in fallback list")

    @property
    def all_providers(self) -> list[str]:
        """Return ordered list of all providers: primary first, then fallbacks."""
        return [self.primary, *self.fallback]


# Default provider routing for each data type.
_DEFAULT_ROUTING: dict[str, ProviderConfig] = {
    "pool_price": ProviderConfig(primary="alchemy_rpc", timeout_ms=500),
    "twap": ProviderConfig(primary="alchemy_rpc", timeout_ms=500),
    "lwap": ProviderConfig(primary="alchemy_rpc", timeout_ms=500),
    "ohlcv": ProviderConfig(primary="binance", fallback=["geckoterminal", "defillama"], timeout_ms=2000),
    "pool_history": ProviderConfig(primary="thegraph", fallback=["defillama", "geckoterminal"], timeout_ms=5000),
    "lending_rate": ProviderConfig(primary="thegraph", fallback=["defillama"], timeout_ms=5000),
    "funding_rate": ProviderConfig(primary="protocol_api", timeout_ms=2000),
    "liquidity_depth": ProviderConfig(primary="alchemy_rpc", timeout_ms=500),
    "pool_analytics": ProviderConfig(primary="defillama", fallback=["geckoterminal", "thegraph"], timeout_ms=5000),
    "yield_opportunities": ProviderConfig(primary="defillama", timeout_ms=5000),
}


@dataclass
class DataRoutingConfig:
    """Top-level routing configuration for the data layer.

    Merges defaults, gateway-level config, and per-strategy overrides
    into a single resolved routing table.

    Resolution order (highest priority wins):
        1. Strategy config.json ``data_overrides`` section
        2. Gateway ``gateway_config.yaml`` data routing section
        3. Built-in defaults (``_DEFAULT_ROUTING``)

    Attributes:
        routes: Mapping of data_type -> ProviderConfig.
    """

    routes: dict[str, ProviderConfig] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Fill in defaults for any data types not explicitly configured
        for data_type, default_config in _DEFAULT_ROUTING.items():
            if data_type not in self.routes:
                self.routes[data_type] = default_config

    def get_route(self, data_type: str) -> ProviderConfig:
        """Get the resolved provider config for a data type.

        Args:
            data_type: The data type key (e.g. "ohlcv", "pool_price").

        Returns:
            ProviderConfig for the data type, or a generic default if unknown.
        """
        if data_type in self.routes:
            return self.routes[data_type]
        # Unknown data type: return a sensible generic default
        return ProviderConfig(primary="alchemy_rpc", timeout_ms=2000)

    @classmethod
    def from_strategy_config(cls, strategy_config: dict[str, Any]) -> DataRoutingConfig:
        """Build routing config from a strategy's config.json.

        Reads the optional ``data_overrides`` section and merges with defaults.

        Args:
            strategy_config: Parsed strategy config.json dict.

        Returns:
            DataRoutingConfig with strategy overrides applied.
        """
        overrides = strategy_config.get("data_overrides", {})
        routes = cls._parse_overrides(overrides)
        return cls(routes=routes)

    @classmethod
    def from_gateway_config(cls, config_path: str | Path) -> DataRoutingConfig:
        """Build routing config from a gateway_config.yaml or .json file.

        Reads the optional ``data_routing`` section and merges with defaults.

        Args:
            config_path: Path to the gateway config file.

        Returns:
            DataRoutingConfig with gateway-level overrides applied.
        """
        path = Path(config_path)
        if not path.exists():
            logger.debug("Gateway config not found at %s, using defaults", path)
            return cls()

        try:
            raw = path.read_text()
            if path.suffix in (".yaml", ".yml"):
                # Lazy import to avoid hard dependency on PyYAML
                try:
                    import yaml

                    data = yaml.safe_load(raw) or {}
                except ImportError:
                    logger.warning("PyYAML not installed, cannot read %s", path)
                    return cls()
            else:
                data = json.loads(raw)
        except Exception:
            logger.warning("Failed to read gateway config at %s, using defaults", path, exc_info=True)
            return cls()

        overrides = data.get("data_routing", {})
        routes = cls._parse_overrides(overrides)
        return cls(routes=routes)

    @classmethod
    def merge(cls, *configs: DataRoutingConfig) -> DataRoutingConfig:
        """Merge multiple configs with later configs taking priority.

        Args:
            *configs: Configs to merge, ordered from lowest to highest priority.

        Returns:
            Merged DataRoutingConfig.
        """
        merged_routes: dict[str, ProviderConfig] = {}
        for config in configs:
            merged_routes.update(config.routes)
        return cls(routes=merged_routes)

    @staticmethod
    def _parse_overrides(overrides: dict[str, Any]) -> dict[str, ProviderConfig]:
        """Parse a dict of data_type -> provider config overrides.

        Accepts both full ProviderConfig dicts and shorthand string values.

        Examples:
            # Full form
            {"ohlcv": {"primary": "geckoterminal", "fallback": ["defillama"], "timeout_ms": 3000}}
            # Shorthand: just a primary provider name
            {"ohlcv": "geckoterminal"}
        """
        routes: dict[str, ProviderConfig] = {}
        for data_type, value in overrides.items():
            if isinstance(value, str):
                # Shorthand: just the primary provider name
                routes[data_type] = ProviderConfig(primary=value)
            elif isinstance(value, dict):
                quota_data = value.get("quota")
                quota = QuotaConfig(**quota_data) if isinstance(quota_data, dict) else None
                routes[data_type] = ProviderConfig(
                    primary=value.get("primary", "alchemy_rpc"),
                    fallback=value.get("fallback", []),
                    timeout_ms=value.get("timeout_ms", 2000),
                    quota=quota,
                )
            else:
                logger.warning("Ignoring invalid data_overrides entry for '%s': %r", data_type, value)
        return routes
