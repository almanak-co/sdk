"""Unit tests for DataProvider protocol and routing config models.

Tests cover:
- QuotaConfig validation and properties
- ProviderConfig validation and defaults
- DataRoutingConfig construction, merging, and override parsing
- DataProvider protocol compliance
- DataRoutingConfig.from_strategy_config
- DataRoutingConfig.from_gateway_config
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest

from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
from almanak.framework.data.routing.config import (
    _DEFAULT_ROUTING,
    DataProvider,
    DataRoutingConfig,
    ProviderConfig,
    QuotaConfig,
)

# ---------------------------------------------------------------------------
# QuotaConfig tests
# ---------------------------------------------------------------------------


class TestQuotaConfig:
    def test_basic_construction(self) -> None:
        q = QuotaConfig(monthly_limit=10000, warn_at_pct=70, hard_stop_at_pct=90)
        assert q.monthly_limit == 10000
        assert q.warn_at_pct == 70
        assert q.hard_stop_at_pct == 90
        assert q.current_usage == 0

    def test_defaults(self) -> None:
        q = QuotaConfig(monthly_limit=1000)
        assert q.warn_at_pct == 80
        assert q.hard_stop_at_pct == 95
        assert q.current_usage == 0

    def test_usage_pct(self) -> None:
        q = QuotaConfig(monthly_limit=1000, current_usage=500)
        assert q.usage_pct == 50.0

    def test_is_warning(self) -> None:
        q = QuotaConfig(monthly_limit=100, warn_at_pct=80, hard_stop_at_pct=95, current_usage=80)
        assert q.is_warning is True
        q2 = QuotaConfig(monthly_limit=100, warn_at_pct=80, hard_stop_at_pct=95, current_usage=79)
        assert q2.is_warning is False

    def test_is_exhausted(self) -> None:
        q = QuotaConfig(monthly_limit=100, warn_at_pct=80, hard_stop_at_pct=95, current_usage=95)
        assert q.is_exhausted is True
        q2 = QuotaConfig(monthly_limit=100, warn_at_pct=80, hard_stop_at_pct=95, current_usage=94)
        assert q2.is_exhausted is False

    def test_invalid_monthly_limit(self) -> None:
        with pytest.raises(ValueError, match="monthly_limit must be positive"):
            QuotaConfig(monthly_limit=0)
        with pytest.raises(ValueError, match="monthly_limit must be positive"):
            QuotaConfig(monthly_limit=-1)

    def test_invalid_warn_at_pct(self) -> None:
        with pytest.raises(ValueError, match="warn_at_pct must be 0-100"):
            QuotaConfig(monthly_limit=100, warn_at_pct=101)
        with pytest.raises(ValueError, match="warn_at_pct must be 0-100"):
            QuotaConfig(monthly_limit=100, warn_at_pct=-1)

    def test_invalid_hard_stop_at_pct(self) -> None:
        with pytest.raises(ValueError, match="hard_stop_at_pct must be 0-100"):
            QuotaConfig(monthly_limit=100, hard_stop_at_pct=101)

    def test_warn_must_be_less_than_hard_stop(self) -> None:
        with pytest.raises(ValueError, match="warn_at_pct .* must be < hard_stop_at_pct"):
            QuotaConfig(monthly_limit=100, warn_at_pct=90, hard_stop_at_pct=90)
        with pytest.raises(ValueError, match="warn_at_pct .* must be < hard_stop_at_pct"):
            QuotaConfig(monthly_limit=100, warn_at_pct=95, hard_stop_at_pct=80)

    def test_negative_current_usage(self) -> None:
        with pytest.raises(ValueError, match="current_usage must be >= 0"):
            QuotaConfig(monthly_limit=100, current_usage=-1)

    def test_frozen(self) -> None:
        q = QuotaConfig(monthly_limit=1000)
        with pytest.raises(AttributeError):
            q.monthly_limit = 2000  # type: ignore[misc]


# ---------------------------------------------------------------------------
# ProviderConfig tests
# ---------------------------------------------------------------------------


class TestProviderConfig:
    def test_basic_construction(self) -> None:
        pc = ProviderConfig(primary="binance", fallback=["geckoterminal"], timeout_ms=3000)
        assert pc.primary == "binance"
        assert pc.fallback == ["geckoterminal"]
        assert pc.timeout_ms == 3000
        assert pc.quota is None

    def test_defaults(self) -> None:
        pc = ProviderConfig(primary="alchemy_rpc")
        assert pc.fallback == []
        assert pc.timeout_ms == 2000
        assert pc.quota is None

    def test_all_providers(self) -> None:
        pc = ProviderConfig(primary="binance", fallback=["geckoterminal", "defillama"])
        assert pc.all_providers == ["binance", "geckoterminal", "defillama"]

    def test_all_providers_no_fallback(self) -> None:
        pc = ProviderConfig(primary="alchemy_rpc")
        assert pc.all_providers == ["alchemy_rpc"]

    def test_with_quota(self) -> None:
        quota = QuotaConfig(monthly_limit=10000)
        pc = ProviderConfig(primary="binance", quota=quota)
        assert pc.quota is not None
        assert pc.quota.monthly_limit == 10000

    def test_empty_primary_raises(self) -> None:
        with pytest.raises(ValueError, match="primary provider name cannot be empty"):
            ProviderConfig(primary="")

    def test_zero_timeout_raises(self) -> None:
        with pytest.raises(ValueError, match="timeout_ms must be positive"):
            ProviderConfig(primary="binance", timeout_ms=0)

    def test_negative_timeout_raises(self) -> None:
        with pytest.raises(ValueError, match="timeout_ms must be positive"):
            ProviderConfig(primary="binance", timeout_ms=-100)

    def test_primary_in_fallback_raises(self) -> None:
        with pytest.raises(ValueError, match="primary provider .* should not be in fallback list"):
            ProviderConfig(primary="binance", fallback=["binance", "defillama"])

    def test_frozen(self) -> None:
        pc = ProviderConfig(primary="binance")
        with pytest.raises(AttributeError):
            pc.primary = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# DataRoutingConfig tests
# ---------------------------------------------------------------------------


class TestDataRoutingConfig:
    def test_defaults_populated(self) -> None:
        config = DataRoutingConfig()
        # All default data types should be present
        for data_type in _DEFAULT_ROUTING:
            assert data_type in config.routes

    def test_get_route_known_type(self) -> None:
        config = DataRoutingConfig()
        route = config.get_route("ohlcv")
        assert route.primary == "binance"
        assert "geckoterminal" in route.fallback

    def test_get_route_unknown_type(self) -> None:
        config = DataRoutingConfig()
        route = config.get_route("some_unknown_type")
        assert route.primary == "alchemy_rpc"
        assert route.timeout_ms == 2000

    def test_custom_routes_override_defaults(self) -> None:
        custom = {"ohlcv": ProviderConfig(primary="geckoterminal", timeout_ms=3000)}
        config = DataRoutingConfig(routes=custom)
        route = config.get_route("ohlcv")
        assert route.primary == "geckoterminal"
        assert route.timeout_ms == 3000
        # Other defaults still present
        assert "pool_price" in config.routes

    def test_from_strategy_config_with_overrides(self) -> None:
        strategy_config: dict[str, Any] = {
            "strategy_id": "test",
            "data_overrides": {
                "ohlcv": {"primary": "geckoterminal", "fallback": ["defillama"], "timeout_ms": 4000},
                "pool_price": "thegraph",
            },
        }
        config = DataRoutingConfig.from_strategy_config(strategy_config)
        # Overridden
        ohlcv = config.get_route("ohlcv")
        assert ohlcv.primary == "geckoterminal"
        assert ohlcv.fallback == ["defillama"]
        assert ohlcv.timeout_ms == 4000
        # Shorthand override
        pool = config.get_route("pool_price")
        assert pool.primary == "thegraph"
        # Other defaults still present
        assert "twap" in config.routes

    def test_from_strategy_config_without_overrides(self) -> None:
        config = DataRoutingConfig.from_strategy_config({"strategy_id": "test"})
        # All defaults present
        for data_type in _DEFAULT_ROUTING:
            assert data_type in config.routes

    def test_from_strategy_config_with_quota(self) -> None:
        strategy_config: dict[str, Any] = {
            "data_overrides": {
                "ohlcv": {
                    "primary": "binance",
                    "quota": {"monthly_limit": 5000, "warn_at_pct": 70, "hard_stop_at_pct": 90},
                }
            }
        }
        config = DataRoutingConfig.from_strategy_config(strategy_config)
        ohlcv = config.get_route("ohlcv")
        assert ohlcv.quota is not None
        assert ohlcv.quota.monthly_limit == 5000

    def test_from_gateway_config_json(self) -> None:
        gateway_data = {
            "data_routing": {
                "ohlcv": {"primary": "defillama", "fallback": ["binance"]},
                "pool_price": "thegraph",
            }
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(gateway_data, f)
            f.flush()
            config = DataRoutingConfig.from_gateway_config(f.name)

        assert config.get_route("ohlcv").primary == "defillama"
        assert config.get_route("pool_price").primary == "thegraph"

    def test_from_gateway_config_missing_file(self) -> None:
        config = DataRoutingConfig.from_gateway_config("/nonexistent/path.json")
        # Should return defaults
        for data_type in _DEFAULT_ROUTING:
            assert data_type in config.routes

    def test_merge_priority(self) -> None:
        low = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="binance")})
        high = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="geckoterminal")})
        merged = DataRoutingConfig.merge(low, high)
        assert merged.get_route("ohlcv").primary == "geckoterminal"

    def test_merge_combines_types(self) -> None:
        a = DataRoutingConfig(routes={"ohlcv": ProviderConfig(primary="binance")})
        b = DataRoutingConfig(routes={"pool_price": ProviderConfig(primary="thegraph")})
        merged = DataRoutingConfig.merge(a, b)
        assert merged.get_route("ohlcv").primary == "binance"
        assert merged.get_route("pool_price").primary == "thegraph"

    def test_parse_overrides_ignores_invalid(self) -> None:
        config = DataRoutingConfig.from_strategy_config({"data_overrides": {"bad": 12345}})
        # The invalid entry should be ignored, defaults still applied
        assert "bad" not in config.routes or config.routes.get("bad") is None
        assert "pool_price" in config.routes


# ---------------------------------------------------------------------------
# DataProvider protocol compliance tests
# ---------------------------------------------------------------------------


class _MockProvider:
    """Concrete mock implementing the DataProvider protocol."""

    def __init__(self, name: str = "mock", classification: DataClassification = DataClassification.INFORMATIONAL):
        self._name = name
        self._classification = classification

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_class(self) -> DataClassification:
        return self._classification

    def fetch(self, **kwargs: object) -> DataEnvelope:
        meta = DataMeta(source=self._name, observed_at=datetime.now(UTC))
        return DataEnvelope(value={"test": True}, meta=meta)

    def health(self) -> dict[str, object]:
        return {"status": "healthy", "latency_ms": 42}


class TestDataProviderProtocol:
    def test_isinstance_check(self) -> None:
        provider = _MockProvider()
        assert isinstance(provider, DataProvider)

    def test_name_property(self) -> None:
        provider = _MockProvider(name="geckoterminal")
        assert provider.name == "geckoterminal"

    def test_data_class_property(self) -> None:
        provider = _MockProvider(classification=DataClassification.EXECUTION_GRADE)
        assert provider.data_class == DataClassification.EXECUTION_GRADE

    def test_fetch_returns_envelope(self) -> None:
        provider = _MockProvider()
        result = provider.fetch(instrument="WETH/USDC", chain="arbitrum")
        assert isinstance(result, DataEnvelope)
        assert result.meta.source == "mock"

    def test_health_returns_dict(self) -> None:
        provider = _MockProvider()
        health = provider.health()
        assert "status" in health
        assert health["status"] == "healthy"

    def test_non_compliant_class_fails_isinstance(self) -> None:
        @dataclass
        class NotAProvider:
            x: int = 1

        assert not isinstance(NotAProvider(), DataProvider)

    def test_partial_implementation_fails_isinstance(self) -> None:
        class PartialProvider:
            @property
            def name(self) -> str:
                return "partial"

            # Missing data_class, fetch, health

        assert not isinstance(PartialProvider(), DataProvider)


# ---------------------------------------------------------------------------
# Default routing sanity checks
# ---------------------------------------------------------------------------


class TestDefaultRouting:
    def test_pool_price_is_rpc(self) -> None:
        assert _DEFAULT_ROUTING["pool_price"].primary == "alchemy_rpc"

    def test_pool_price_timeout_is_fast(self) -> None:
        assert _DEFAULT_ROUTING["pool_price"].timeout_ms <= 1000

    def test_ohlcv_has_fallbacks(self) -> None:
        assert len(_DEFAULT_ROUTING["ohlcv"].fallback) >= 1

    def test_all_defaults_have_valid_timeout(self) -> None:
        for data_type, config in _DEFAULT_ROUTING.items():
            assert config.timeout_ms > 0, f"{data_type} has invalid timeout"
