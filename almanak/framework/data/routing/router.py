"""Data router with fallback and classification-aware routing.

Implements provider selection, fallback chains, and fail-closed semantics
for execution-grade data.

Routing logic:
    1. Strategy override -> global config -> default provider config
    2. For EXECUTION_GRADE: fail closed after primary timeout. Raises DataUnavailableError.
    3. For INFORMATIONAL: try fallback chain, each with timeout. Return best available
       with degraded confidence.

Usage:
    router = DataRouter(config=routing_config)
    router.register_provider(my_provider)

    result = router.route("ohlcv", instrument=inst)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
from almanak.framework.data.routing.circuit_breaker import CircuitBreaker
from almanak.framework.data.routing.config import DataProvider, DataRoutingConfig, ProviderConfig

logger = logging.getLogger(__name__)


@dataclass
class _ProviderMetrics:
    """Internal per-provider request metrics."""

    requests_total: int = 0
    failures_total: int = 0
    fallback_total: int = 0
    total_latency_ms: float = 0.0

    @property
    def avg_latency_ms(self) -> float:
        if self.requests_total == 0:
            return 0.0
        return self.total_latency_ms / self.requests_total


@dataclass
class DataRouter:
    """Routes data requests to providers with fallback and circuit-breaking.

    For EXECUTION_GRADE data types, the router fails closed after the primary
    provider fails -- no fallback to degraded sources is attempted.

    For INFORMATIONAL data types, the router tries the fallback chain in order,
    each with its own timeout, and returns the best available result with
    degraded confidence.

    Attributes:
        config: Routing configuration specifying provider assignments.
    """

    config: DataRoutingConfig = field(default_factory=DataRoutingConfig)
    _providers: dict[str, DataProvider] = field(default_factory=dict, init=False, repr=False)
    _breakers: dict[str, CircuitBreaker] = field(default_factory=dict, init=False, repr=False)
    _metrics: dict[str, _ProviderMetrics] = field(default_factory=dict, init=False, repr=False)

    def register_provider(self, provider: DataProvider) -> None:
        """Register a data provider for routing.

        Args:
            provider: A DataProvider implementation to register.
        """
        self._providers[provider.name] = provider
        if provider.name not in self._breakers:
            self._breakers[provider.name] = CircuitBreaker(name=provider.name)
        if provider.name not in self._metrics:
            self._metrics[provider.name] = _ProviderMetrics()
        logger.debug("provider_registered name=%s data_class=%s", provider.name, provider.data_class.value)

    def route(
        self,
        data_type: str,
        *,
        instrument: str = "",
        strategy_config: dict | None = None,
        **fetch_kwargs: object,
    ) -> DataEnvelope:
        """Select a provider and fetch data with fallback logic.

        Provider selection order:
            1. Strategy-level override (if strategy_config provided)
            2. Global routing config
            3. Built-in defaults

        Args:
            data_type: Data type key (e.g. "ohlcv", "pool_price").
            instrument: Instrument identifier for logging/error context.
            strategy_config: Optional strategy config dict with data_overrides.
            **fetch_kwargs: Additional kwargs passed through to provider.fetch().

        Returns:
            DataEnvelope from the selected provider.

        Raises:
            DataUnavailableError: When all providers fail.
        """
        route_config = self._resolve_route(data_type, strategy_config)
        classification = self._classify_data_type(data_type, route_config)

        if classification == DataClassification.EXECUTION_GRADE:
            return self._route_execution_grade(data_type, route_config, instrument, fetch_kwargs)
        return self._route_informational(data_type, route_config, instrument, fetch_kwargs)

    def get_metrics(self, provider_name: str | None = None) -> dict[str, object]:
        """Return metrics for a specific provider or all providers.

        Args:
            provider_name: If provided, return metrics for this provider only.

        Returns:
            Dict of metric data.
        """
        if provider_name is not None:
            m = self._metrics.get(provider_name)
            if m is None:
                return {}
            return {
                "requests_total": m.requests_total,
                "failures_total": m.failures_total,
                "fallback_total": m.fallback_total,
                "avg_latency_ms": m.avg_latency_ms,
            }
        return {
            name: {
                "requests_total": m.requests_total,
                "failures_total": m.failures_total,
                "fallback_total": m.fallback_total,
                "avg_latency_ms": m.avg_latency_ms,
            }
            for name, m in self._metrics.items()
        }

    def health(self) -> dict[str, object]:
        """Return health status for all registered providers.

        Returns:
            Dict mapping provider name -> circuit breaker health dict.
        """
        return {name: breaker.health() for name, breaker in self._breakers.items()}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_route(self, data_type: str, strategy_config: dict | None) -> ProviderConfig:
        """Resolve the provider config for a data type.

        Priority: strategy override -> global config -> defaults.
        """
        if strategy_config:
            overrides = strategy_config.get("data_overrides", {})
            if data_type in overrides:
                override = overrides[data_type]
                if isinstance(override, str):
                    return ProviderConfig(primary=override)
                if isinstance(override, dict):
                    return ProviderConfig(
                        primary=override.get("primary", "alchemy_rpc"),
                        fallback=override.get("fallback", []),
                        timeout_ms=override.get("timeout_ms", 2000),
                    )
        return self.config.get_route(data_type)

    def _classify_data_type(self, data_type: str, route_config: ProviderConfig) -> DataClassification:
        """Determine classification for a data type.

        Uses the primary provider's data_class if registered, otherwise
        infers from the route config timeout (on-chain timeouts <= 500ms
        are EXECUTION_GRADE).
        """
        primary = self._providers.get(route_config.primary)
        if primary is not None:
            return primary.data_class
        # Heuristic: short-timeout on-chain reads are execution-grade
        if route_config.timeout_ms <= 500:
            return DataClassification.EXECUTION_GRADE
        return DataClassification.INFORMATIONAL

    def _route_execution_grade(
        self,
        data_type: str,
        route_config: ProviderConfig,
        instrument: str,
        fetch_kwargs: dict,
    ) -> DataEnvelope:
        """Route EXECUTION_GRADE request: primary only, fail closed."""
        provider_name = route_config.primary
        result = self._try_provider(provider_name, data_type, instrument, route_config.timeout_ms, fetch_kwargs)
        if result is not None:
            return result

        raise DataUnavailableError(
            data_type=data_type,
            instrument=instrument,
            reason=f"Primary provider '{provider_name}' failed (EXECUTION_GRADE: no fallback)",
        )

    def _route_informational(
        self,
        data_type: str,
        route_config: ProviderConfig,
        instrument: str,
        fetch_kwargs: dict,
    ) -> DataEnvelope:
        """Route INFORMATIONAL request: try primary, then fallback chain."""
        all_providers = route_config.all_providers
        fallback_timeout_ms = 2000

        for i, provider_name in enumerate(all_providers):
            timeout = route_config.timeout_ms if i == 0 else fallback_timeout_ms
            result = self._try_provider(provider_name, data_type, instrument, timeout, fetch_kwargs)
            if result is not None:
                if i > 0:
                    self._record_fallback(provider_name, data_type, instrument)
                    result = self._degrade_confidence(result, fallback_position=i)
                return result

        raise DataUnavailableError(
            data_type=data_type,
            instrument=instrument,
            reason=f"All providers failed: {all_providers}",
        )

    def _try_provider(
        self,
        provider_name: str,
        data_type: str,
        instrument: str,
        timeout_ms: int,
        fetch_kwargs: dict,
    ) -> DataEnvelope | None:
        """Attempt to fetch from a single provider, respecting circuit breaker.

        Returns None on failure (provider not registered, circuit open, or fetch error).
        """
        provider = self._providers.get(provider_name)
        if provider is None:
            logger.warning(
                "provider_not_found provider=%s data_type=%s instrument=%s",
                provider_name,
                data_type,
                instrument,
            )
            return None

        breaker = self._breakers.get(provider_name)
        if breaker and not breaker.allow_request():
            logger.info(
                "circuit_open provider=%s data_type=%s instrument=%s",
                provider_name,
                data_type,
                instrument,
            )
            return None

        metrics = self._metrics.setdefault(provider_name, _ProviderMetrics())
        metrics.requests_total += 1

        logger.debug(
            "provider_selected provider=%s data_type=%s instrument=%s timeout_ms=%d",
            provider_name,
            data_type,
            instrument,
            timeout_ms,
        )

        start = time.monotonic()
        try:
            result = provider.fetch(data_type=data_type, instrument=instrument, timeout_ms=timeout_ms, **fetch_kwargs)
            elapsed_ms = (time.monotonic() - start) * 1000
            metrics.total_latency_ms += elapsed_ms

            if breaker:
                breaker.record_success()
            return result
        except Exception as exc:
            elapsed_ms = (time.monotonic() - start) * 1000
            metrics.total_latency_ms += elapsed_ms
            metrics.failures_total += 1

            if breaker:
                breaker.record_failure()

            logger.warning(
                "provider_failed provider=%s data_type=%s instrument=%s error=%s elapsed_ms=%.1f",
                provider_name,
                data_type,
                instrument,
                exc,
                elapsed_ms,
            )
            return None

    def _record_fallback(self, provider_name: str, data_type: str, instrument: str) -> None:
        """Record a fallback event in metrics and logs."""
        metrics = self._metrics.get(provider_name)
        if metrics:
            metrics.fallback_total += 1
        logger.info(
            "fallback_triggered provider=%s data_type=%s instrument=%s",
            provider_name,
            data_type,
            instrument,
        )

    @staticmethod
    def _degrade_confidence(envelope: DataEnvelope, *, fallback_position: int) -> DataEnvelope:
        """Return a new DataEnvelope with confidence reduced for fallback results.

        Each fallback position applies a 0.1 penalty (clamped to 0.0).
        """
        penalty = 0.1 * fallback_position
        degraded = max(0.0, envelope.meta.confidence - penalty)
        new_meta = DataMeta(
            source=envelope.meta.source,
            observed_at=envelope.meta.observed_at,
            block_number=envelope.meta.block_number,
            finality=envelope.meta.finality,
            staleness_ms=envelope.meta.staleness_ms,
            latency_ms=envelope.meta.latency_ms,
            confidence=degraded,
            cache_hit=envelope.meta.cache_hit,
        )
        return DataEnvelope(
            value=envelope.value,
            meta=new_meta,
            classification=envelope.classification,
        )
