"""Data routing package for provider selection, failover, and circuit breaking.

Provides the DataRouter, CircuitBreaker, and configuration models for
multi-provider data infrastructure with classification-aware routing.
"""

from .circuit_breaker import CircuitBreaker, CircuitState
from .config import (
    DataProvider,
    DataRoutingConfig,
    ProviderConfig,
    QuotaConfig,
)
from .router import DataRouter

__all__ = [
    "CircuitBreaker",
    "CircuitState",
    "DataProvider",
    "DataRouter",
    "DataRoutingConfig",
    "ProviderConfig",
    "QuotaConfig",
]
