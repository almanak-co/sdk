"""Tests for TokenService health check endpoint."""

from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio

from almanak.gateway.services.token_service import TokenServiceServicer


@pytest.fixture
def mock_settings():
    """Create mock gateway settings."""
    settings = MagicMock()
    settings.network = "mainnet"
    return settings


@pytest_asyncio.fixture
async def token_service(mock_settings):
    """Create TokenService with mock settings."""
    with patch("almanak.gateway.services.token_service.get_token_resolver") as mock_resolver:
        resolver = MagicMock()
        resolver.stats.return_value = {
            "cache_hits": 100,
            "static_hits": 20,
            "gateway_lookups": 5,
            "gateway_errors": 0,
            "errors": 1,
        }
        resolver.cache_stats.return_value = {
            "memory_hits": 90,
            "disk_hits": 10,
            "misses": 5,
            "evictions": 0,
        }
        resolver.is_gateway_connected.return_value = False
        mock_resolver.return_value = resolver
        service = TokenServiceServicer(mock_settings)
    yield service
    await service.close()


class TestTokenServiceHealthCheck:
    """Tests for TokenServiceServicer.health_check() method."""

    @pytest.mark.asyncio
    async def test_health_check_returns_dict(self, token_service):
        """health_check returns a dict with expected keys."""
        result = await token_service.health_check()
        assert isinstance(result, dict)
        assert "healthy" in result
        assert "status" in result
        assert "resolver_stats" in result
        assert "cache_stats" in result
        assert "gateway_connected" in result
        assert "onchain_lookups_active" in result

    @pytest.mark.asyncio
    async def test_health_check_healthy_state(self, token_service):
        """health_check reports healthy when error rate is low."""
        result = await token_service.health_check()
        assert result["healthy"] is True
        assert result["status"] == "serving"

    @pytest.mark.asyncio
    async def test_health_check_includes_resolver_stats(self, token_service):
        """health_check includes resolver stats from resolver.stats()."""
        result = await token_service.health_check()
        stats = result["resolver_stats"]
        assert stats["cache_hits"] == 100
        assert stats["static_hits"] == 20
        assert stats["gateway_lookups"] == 5
        assert stats["errors"] == 1

    @pytest.mark.asyncio
    async def test_health_check_includes_cache_stats(self, token_service):
        """health_check includes cache stats from resolver.cache_stats()."""
        result = await token_service.health_check()
        cache = result["cache_stats"]
        assert cache["memory_hits"] == 90
        assert cache["disk_hits"] == 10

    @pytest.mark.asyncio
    async def test_health_check_gateway_not_connected(self, token_service):
        """health_check reports gateway_connected=False when no gateway."""
        result = await token_service.health_check()
        assert result["gateway_connected"] is False

    @pytest.mark.asyncio
    async def test_health_check_gateway_connected(self, mock_settings):
        """health_check reports gateway_connected=True when gateway is available."""
        with patch("almanak.gateway.services.token_service.get_token_resolver") as mock_resolver:
            resolver = MagicMock()
            resolver.stats.return_value = {"cache_hits": 0, "static_hits": 0, "gateway_lookups": 0, "gateway_errors": 0, "errors": 0}
            resolver.cache_stats.return_value = {"memory_hits": 0, "disk_hits": 0, "misses": 0, "evictions": 0}
            resolver.is_gateway_connected.return_value = True
            mock_resolver.return_value = resolver
            service = TokenServiceServicer(mock_settings)

        result = await service.health_check()
        assert result["gateway_connected"] is True

    @pytest.mark.asyncio
    async def test_health_check_degraded_high_error_rate(self, mock_settings):
        """health_check reports degraded when error rate exceeds 10%."""
        with patch("almanak.gateway.services.token_service.get_token_resolver") as mock_resolver:
            resolver = MagicMock()
            # 50 errors out of 150 total = 33% error rate
            resolver.stats.return_value = {
                "cache_hits": 50,
                "static_hits": 50,
                "gateway_lookups": 0,
                "gateway_errors": 0,
                "errors": 50,
            }
            resolver.cache_stats.return_value = {"memory_hits": 0, "disk_hits": 0, "misses": 0, "evictions": 0}
            resolver.is_gateway_connected.return_value = False
            mock_resolver.return_value = resolver
            service = TokenServiceServicer(mock_settings)

        result = await service.health_check()
        assert result["healthy"] is False
        assert result["status"] == "degraded_high_error_rate"

    @pytest.mark.asyncio
    async def test_health_check_healthy_with_low_error_count(self, mock_settings):
        """health_check stays healthy when total lookups < 100 even with errors."""
        with patch("almanak.gateway.services.token_service.get_token_resolver") as mock_resolver:
            resolver = MagicMock()
            # Only 50 total lookups - below threshold for degraded check
            resolver.stats.return_value = {
                "cache_hits": 30,
                "static_hits": 20,
                "gateway_lookups": 0,
                "gateway_errors": 0,
                "errors": 40,
            }
            resolver.cache_stats.return_value = {"memory_hits": 0, "disk_hits": 0, "misses": 0, "evictions": 0}
            resolver.is_gateway_connected.return_value = False
            mock_resolver.return_value = resolver
            service = TokenServiceServicer(mock_settings)

        result = await service.health_check()
        assert result["healthy"] is True

    @pytest.mark.asyncio
    async def test_health_check_onchain_lookups_active(self, token_service):
        """health_check reports the number of active on-chain lookup instances."""
        result = await token_service.health_check()
        assert result["onchain_lookups_active"] == 0
