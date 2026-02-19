"""Unit tests for DataRouter.

Tests cover:
- Provider registration
- Routing logic: strategy override -> global config -> default
- EXECUTION_GRADE fail-closed behavior
- INFORMATIONAL fallback chain
- Circuit breaker integration
- Metrics tracking
- Structured log events
- Edge cases (no providers, unregistered provider, etc.)
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
from almanak.framework.data.routing.circuit_breaker import CircuitBreaker, CircuitState
from almanak.framework.data.routing.config import DataRoutingConfig, ProviderConfig
from almanak.framework.data.routing.router import DataRouter

# ---------------------------------------------------------------------------
# Test helpers: mock providers
# ---------------------------------------------------------------------------


def _make_meta(source: str = "test") -> DataMeta:
    return DataMeta(
        source=source,
        observed_at=datetime.now(UTC),
        staleness_ms=10,
        latency_ms=5,
        confidence=0.95,
    )


def _make_envelope(source: str = "test", value: object = "data") -> DataEnvelope:
    return DataEnvelope(value=value, meta=_make_meta(source))


class StubProvider:
    """Minimal DataProvider implementation for testing."""

    def __init__(
        self,
        name: str,
        data_class: DataClassification = DataClassification.INFORMATIONAL,
        return_value: object = "data",
        raise_on_fetch: Exception | None = None,
    ) -> None:
        self._name = name
        self._data_class = data_class
        self._return_value = return_value
        self._raise_on_fetch = raise_on_fetch
        self.fetch_calls: list[dict] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_class(self) -> DataClassification:
        return self._data_class

    def fetch(self, **kwargs: object) -> DataEnvelope:
        self.fetch_calls.append(kwargs)
        if self._raise_on_fetch is not None:
            raise self._raise_on_fetch
        return _make_envelope(source=self._name, value=self._return_value)

    def health(self) -> dict[str, object]:
        return {"status": "healthy"}


class FailingProvider(StubProvider):
    """Provider that always raises on fetch."""

    def __init__(self, name: str, data_class: DataClassification = DataClassification.INFORMATIONAL) -> None:
        super().__init__(name, data_class, raise_on_fetch=RuntimeError(f"{name} failed"))


# ---------------------------------------------------------------------------
# Registration tests
# ---------------------------------------------------------------------------


class TestProviderRegistration:
    def test_register_provider(self) -> None:
        router = DataRouter()
        provider = StubProvider("test_prov")
        router.register_provider(provider)
        assert "test_prov" in router._providers
        assert "test_prov" in router._breakers
        assert "test_prov" in router._metrics

    def test_register_multiple_providers(self) -> None:
        router = DataRouter()
        router.register_provider(StubProvider("a"))
        router.register_provider(StubProvider("b"))
        assert len(router._providers) == 2

    def test_re_register_same_provider(self) -> None:
        router = DataRouter()
        router.register_provider(StubProvider("a"))
        router.register_provider(StubProvider("a", return_value="new"))
        assert len(router._providers) == 1
        # Should update the provider but keep existing breaker
        assert router._providers["a"]._return_value == "new"


# ---------------------------------------------------------------------------
# Basic routing tests
# ---------------------------------------------------------------------------


class TestBasicRouting:
    def test_route_to_primary(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="my_prov", timeout_ms=2000)})
        router = DataRouter(config=config)
        provider = StubProvider("my_prov")
        router.register_provider(provider)

        result = router.route("ohlcv", instrument="WETH/USDC")
        assert result.meta.source == "my_prov"
        assert len(provider.fetch_calls) == 1
        assert provider.fetch_calls[0]["instrument"] == "WETH/USDC"

    def test_route_passes_fetch_kwargs(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="prov")})
        router = DataRouter(config=config)
        provider = StubProvider("prov")
        router.register_provider(provider)

        router.route("ohlcv", instrument="ETH", chain="arbitrum", timeframe="1h")
        assert provider.fetch_calls[0]["chain"] == "arbitrum"
        assert provider.fetch_calls[0]["timeframe"] == "1h"


# ---------------------------------------------------------------------------
# Strategy override tests
# ---------------------------------------------------------------------------


class TestStrategyOverride:
    def test_strategy_override_string(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="default_prov")})
        router = DataRouter(config=config)
        router.register_provider(StubProvider("default_prov"))
        router.register_provider(StubProvider("custom_prov"))

        strategy_config = {"data_overrides": {"ohlcv": "custom_prov"}}
        result = router.route("ohlcv", strategy_config=strategy_config)
        assert result.meta.source == "custom_prov"

    def test_strategy_override_dict(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="default_prov")})
        router = DataRouter(config=config)
        router.register_provider(StubProvider("default_prov"))
        router.register_provider(StubProvider("gecko"))

        strategy_config = {
            "data_overrides": {
                "ohlcv": {"primary": "gecko", "fallback": ["default_prov"], "timeout_ms": 3000},
            }
        }
        result = router.route("ohlcv", strategy_config=strategy_config)
        assert result.meta.source == "gecko"

    def test_strategy_override_only_for_matching_data_type(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="default_prov")})
        router = DataRouter(config=config)
        router.register_provider(StubProvider("default_prov"))
        router.register_provider(StubProvider("custom_prov"))

        # Override is for "pool_price", not "ohlcv"
        strategy_config = {"data_overrides": {"pool_price": "custom_prov"}}
        result = router.route("ohlcv", strategy_config=strategy_config)
        assert result.meta.source == "default_prov"

    def test_no_strategy_config_uses_global(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="global_prov")})
        router = DataRouter(config=config)
        router.register_provider(StubProvider("global_prov"))

        result = router.route("ohlcv")
        assert result.meta.source == "global_prov"


# ---------------------------------------------------------------------------
# EXECUTION_GRADE fail-closed tests
# ---------------------------------------------------------------------------


class TestExecutionGradeRouting:
    def test_execution_grade_succeeds(self) -> None:
        config = DataRoutingConfig(routes={"pool_price": ProviderConfig(primary="rpc", timeout_ms=500)})
        router = DataRouter(config=config)
        router.register_provider(StubProvider("rpc", data_class=DataClassification.EXECUTION_GRADE))

        result = router.route("pool_price", instrument="0xpool")
        assert result.meta.source == "rpc"

    def test_execution_grade_fails_closed(self) -> None:
        config = DataRoutingConfig(
            routes={"pool_price": ProviderConfig(primary="rpc", fallback=["fallback_prov"], timeout_ms=500)}
        )
        router = DataRouter(config=config)
        router.register_provider(FailingProvider("rpc", data_class=DataClassification.EXECUTION_GRADE))
        router.register_provider(StubProvider("fallback_prov"))

        with pytest.raises(DataUnavailableError, match="EXECUTION_GRADE: no fallback"):
            router.route("pool_price", instrument="0xpool")

    def test_execution_grade_no_fallback_attempted(self) -> None:
        """Even if fallback providers exist, they should NOT be tried for EXECUTION_GRADE."""
        config = DataRoutingConfig(
            routes={"pool_price": ProviderConfig(primary="rpc", fallback=["backup"], timeout_ms=500)}
        )
        router = DataRouter(config=config)
        router.register_provider(FailingProvider("rpc", data_class=DataClassification.EXECUTION_GRADE))
        backup = StubProvider("backup")
        router.register_provider(backup)

        with pytest.raises(DataUnavailableError):
            router.route("pool_price", instrument="test")

        # Backup should never have been called
        assert len(backup.fetch_calls) == 0

    def test_execution_grade_inferred_from_timeout(self) -> None:
        """If provider not registered, timeout <= 500ms implies EXECUTION_GRADE."""
        config = DataRoutingConfig(routes={"pool_price": ProviderConfig(primary="unregistered", timeout_ms=500)})
        router = DataRouter(config=config)

        with pytest.raises(DataUnavailableError, match="EXECUTION_GRADE"):
            router.route("pool_price", instrument="test")


# ---------------------------------------------------------------------------
# INFORMATIONAL fallback tests
# ---------------------------------------------------------------------------


class TestInformationalRouting:
    def test_informational_primary_success(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="primary", fallback=["fallback"])})
        router = DataRouter(config=config)
        router.register_provider(StubProvider("primary"))
        router.register_provider(StubProvider("fallback"))

        result = router.route("ohlcv", instrument="WETH/USDC")
        assert result.meta.source == "primary"

    def test_informational_falls_back_on_primary_failure(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="primary", fallback=["fallback"])})
        router = DataRouter(config=config)
        router.register_provider(FailingProvider("primary"))
        router.register_provider(StubProvider("fallback"))

        result = router.route("ohlcv", instrument="WETH/USDC")
        assert result.meta.source == "fallback"

    def test_informational_tries_multiple_fallbacks(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="p1", fallback=["p2", "p3"])})
        router = DataRouter(config=config)
        router.register_provider(FailingProvider("p1"))
        router.register_provider(FailingProvider("p2"))
        router.register_provider(StubProvider("p3"))

        result = router.route("ohlcv", instrument="test")
        assert result.meta.source == "p3"

    def test_informational_all_fail_raises(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="p1", fallback=["p2"])})
        router = DataRouter(config=config)
        router.register_provider(FailingProvider("p1"))
        router.register_provider(FailingProvider("p2"))

        with pytest.raises(DataUnavailableError, match="All providers failed"):
            router.route("ohlcv", instrument="test")

    def test_informational_classification_from_provider(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="prov", fallback=["backup"])})
        router = DataRouter(config=config)
        router.register_provider(FailingProvider("prov", data_class=DataClassification.INFORMATIONAL))
        router.register_provider(StubProvider("backup"))

        result = router.route("ohlcv", instrument="test")
        assert result.meta.source == "backup"


# ---------------------------------------------------------------------------
# Circuit breaker integration tests
# ---------------------------------------------------------------------------


class TestCircuitBreakerIntegration:
    def test_circuit_opens_after_failures(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="prov", fallback=["backup"])})
        router = DataRouter(config=config)
        router.register_provider(FailingProvider("prov"))
        router.register_provider(StubProvider("backup"))

        # Set low threshold
        router._breakers["prov"] = CircuitBreaker(name="prov", failure_threshold=2)

        # First two calls open the circuit
        router.route("ohlcv", instrument="test")  # falls back to backup
        router.route("ohlcv", instrument="test")  # opens circuit

        # Circuit should now be open
        assert router._breakers["prov"].state == CircuitState.OPEN

    def test_circuit_open_skips_provider(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="prov", fallback=["backup"])})
        router = DataRouter(config=config)
        failing = FailingProvider("prov")
        router.register_provider(failing)
        backup = StubProvider("backup")
        router.register_provider(backup)

        # Manually open the circuit
        router._breakers["prov"] = CircuitBreaker(name="prov", failure_threshold=1)
        router._breakers["prov"].record_failure()
        assert router._breakers["prov"].state == CircuitState.OPEN

        # Route should skip prov entirely and go to backup
        result = router.route("ohlcv", instrument="test")
        assert result.meta.source == "backup"
        # Failing provider should NOT have been called (circuit was open)
        assert len(failing.fetch_calls) == 0

    def test_circuit_success_resets(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="prov")})
        router = DataRouter(config=config)
        provider = StubProvider("prov")
        router.register_provider(provider)

        # Record some failures (below threshold)
        router._breakers["prov"] = CircuitBreaker(name="prov", failure_threshold=5)
        router._breakers["prov"].record_failure()
        router._breakers["prov"].record_failure()
        assert router._breakers["prov"].failure_count == 2

        # Successful route resets the breaker
        router.route("ohlcv", instrument="test")
        assert router._breakers["prov"].failure_count == 0


# ---------------------------------------------------------------------------
# Metrics tests
# ---------------------------------------------------------------------------


class TestMetrics:
    def test_metrics_track_requests(self) -> None:
        router = DataRouter()
        router.register_provider(StubProvider("prov"))
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="prov")})
        router.config = config

        router.route("ohlcv", instrument="test")
        router.route("ohlcv", instrument="test")

        metrics = router.get_metrics("prov")
        assert metrics["requests_total"] == 2
        assert metrics["failures_total"] == 0

    def test_metrics_track_failures(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="prov", fallback=["backup"])})
        router = DataRouter(config=config)
        router.register_provider(FailingProvider("prov"))
        router.register_provider(StubProvider("backup"))

        router.route("ohlcv", instrument="test")

        metrics = router.get_metrics("prov")
        assert metrics["requests_total"] == 1
        assert metrics["failures_total"] == 1

    def test_metrics_track_fallback(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="prov", fallback=["backup"])})
        router = DataRouter(config=config)
        router.register_provider(FailingProvider("prov"))
        router.register_provider(StubProvider("backup"))

        router.route("ohlcv", instrument="test")

        backup_metrics = router.get_metrics("backup")
        assert backup_metrics["fallback_total"] == 1

    def test_metrics_track_latency(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="prov")})
        router = DataRouter(config=config)
        router.register_provider(StubProvider("prov"))

        router.route("ohlcv", instrument="test")

        metrics = router.get_metrics("prov")
        assert metrics["avg_latency_ms"] >= 0

    def test_all_metrics(self) -> None:
        router = DataRouter()
        router.register_provider(StubProvider("a"))
        router.register_provider(StubProvider("b"))

        all_metrics = router.get_metrics()
        assert "a" in all_metrics
        assert "b" in all_metrics

    def test_metrics_unknown_provider(self) -> None:
        router = DataRouter()
        assert router.get_metrics("nonexistent") == {}


# ---------------------------------------------------------------------------
# Health tests
# ---------------------------------------------------------------------------


class TestHealth:
    def test_health_returns_all_breakers(self) -> None:
        router = DataRouter()
        router.register_provider(StubProvider("a"))
        router.register_provider(StubProvider("b"))

        h = router.health()
        assert "a" in h
        assert "b" in h
        assert h["a"]["state"] == "closed"

    def test_health_empty_router(self) -> None:
        router = DataRouter()
        assert router.health() == {}


# ---------------------------------------------------------------------------
# Edge case tests
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_unregistered_primary_provider(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="missing")})
        router = DataRouter(config=config)

        with pytest.raises(DataUnavailableError):
            router.route("ohlcv", instrument="test")

    def test_unknown_data_type_uses_default(self) -> None:
        router = DataRouter()
        provider = StubProvider("alchemy_rpc", data_class=DataClassification.EXECUTION_GRADE)
        router.register_provider(provider)

        # "exotic_data" is not in _DEFAULT_ROUTING; config.get_route returns
        # generic ProviderConfig(primary="alchemy_rpc", timeout_ms=2000)
        result = router.route("exotic_data", instrument="test")
        assert result.meta.source == "alchemy_rpc"

    def test_empty_instrument(self) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="prov")})
        router = DataRouter(config=config)
        router.register_provider(StubProvider("prov"))

        result = router.route("ohlcv")
        assert result is not None

    def test_default_config_construction(self) -> None:
        router = DataRouter()
        # Should have default routes from _DEFAULT_ROUTING
        route = router.config.get_route("ohlcv")
        assert route.primary == "binance"

    def test_fallback_provider_not_registered(self) -> None:
        """Fallback provider not registered should be skipped gracefully."""
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="prov", fallback=["missing"])})
        router = DataRouter(config=config)
        router.register_provider(StubProvider("prov"))

        # Primary succeeds, so missing fallback is irrelevant
        result = router.route("ohlcv", instrument="test")
        assert result.meta.source == "prov"

    def test_all_providers_missing(self) -> None:
        """When primary and fallbacks are all unregistered."""
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="a", fallback=["b", "c"])})
        router = DataRouter(config=config)

        with pytest.raises(DataUnavailableError, match="All providers failed"):
            router.route("ohlcv", instrument="test")


# ---------------------------------------------------------------------------
# Log event tests (verify structured log messages)
# ---------------------------------------------------------------------------


class TestLogEvents:
    def test_provider_selected_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="prov")})
        router = DataRouter(config=config)
        router.register_provider(StubProvider("prov"))

        with caplog.at_level("DEBUG", logger="almanak.framework.data.routing.router"):
            router.route("ohlcv", instrument="WETH/USDC")

        assert any("provider_selected" in msg for msg in caplog.messages)

    def test_provider_failed_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="prov", fallback=["backup"])})
        router = DataRouter(config=config)
        router.register_provider(FailingProvider("prov"))
        router.register_provider(StubProvider("backup"))

        with caplog.at_level("WARNING", logger="almanak.framework.data.routing.router"):
            router.route("ohlcv", instrument="test")

        assert any("provider_failed" in msg for msg in caplog.messages)

    def test_fallback_triggered_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="prov", fallback=["backup"])})
        router = DataRouter(config=config)
        router.register_provider(FailingProvider("prov"))
        router.register_provider(StubProvider("backup"))

        with caplog.at_level("INFO", logger="almanak.framework.data.routing.router"):
            router.route("ohlcv", instrument="test")

        assert any("fallback_triggered" in msg for msg in caplog.messages)

    def test_circuit_open_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        config = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="prov", fallback=["backup"])})
        router = DataRouter(config=config)
        router.register_provider(FailingProvider("prov"))
        router.register_provider(StubProvider("backup"))

        # Open the circuit
        router._breakers["prov"] = CircuitBreaker(name="prov", failure_threshold=1)
        router._breakers["prov"].record_failure()

        with caplog.at_level("INFO", logger="almanak.framework.data.routing.router"):
            router.route("ohlcv", instrument="test")

        assert any("circuit_open" in msg for msg in caplog.messages)
