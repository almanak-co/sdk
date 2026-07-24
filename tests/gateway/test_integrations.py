"""Tests for gateway integrations."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.gateway.integrations.base import (
    BaseIntegration,
    CacheEntry,
    HealthMetrics,
    IntegrationError,
    IntegrationRateLimitError,
    IntegrationRegistry,
    RateLimiter,
)
from almanak.gateway.integrations.binance import BinanceIntegration
from almanak.gateway.integrations.coingecko import CoinGeckoIntegration
from almanak.gateway.integrations.thegraph import TheGraphIntegration
from almanak.gateway.integrations.zerion import ZerionIntegration

# =============================================================================
# Base Integration Tests
# =============================================================================


@pytest.fixture
def thegraph():
    """Module-level TheGraph fixture for tests outside the original class."""
    return TheGraphIntegration()


class TestRateLimiter:
    """Tests for RateLimiter."""

    @pytest.mark.asyncio
    async def test_allows_requests_under_limit(self):
        """Requests under limit are allowed immediately."""
        limiter = RateLimiter(requests_per_minute=60)

        for _ in range(5):
            wait_time = await limiter.acquire()
            assert wait_time == 0.0

    def test_get_wait_time_returns_zero_when_available(self):
        """get_wait_time returns 0 when tokens available."""
        limiter = RateLimiter(requests_per_minute=60)

        wait_time = limiter.get_wait_time()
        assert wait_time == 0.0


class TestCacheEntry:
    """Tests for CacheEntry."""

    def test_not_expired_when_fresh(self):
        """Cache entry is not expired when fresh."""
        from datetime import UTC, datetime

        entry = CacheEntry(
            data={"test": "data"},
            cached_at=datetime.now(UTC),
            ttl_seconds=60,
        )

        assert entry.is_expired() is False

    def test_expired_when_old(self):
        """Cache entry is expired after TTL."""
        from datetime import UTC, datetime, timedelta

        entry = CacheEntry(
            data={"test": "data"},
            cached_at=datetime.now(UTC) - timedelta(seconds=120),
            ttl_seconds=60,
        )

        assert entry.is_expired() is True


class TestHealthMetrics:
    """Tests for HealthMetrics."""

    def test_success_rate_100_when_no_requests(self):
        """Success rate is 100% when no requests made."""
        metrics = HealthMetrics()
        assert metrics.success_rate == 100.0

    def test_success_rate_calculation(self):
        """Success rate is calculated correctly."""
        metrics = HealthMetrics(total_requests=10, successful_requests=8)
        assert metrics.success_rate == 80.0

    def test_average_latency_calculation(self):
        """Average latency is calculated correctly."""
        metrics = HealthMetrics(
            successful_requests=4,
            total_latency_ms=100.0,
        )
        assert metrics.average_latency_ms == 25.0


class TestIntegrationRegistry:
    """Tests for IntegrationRegistry."""

    def setup_method(self):
        """Reset registry before each test."""
        IntegrationRegistry.reset()

    def test_singleton_pattern(self):
        """Registry is a singleton."""
        registry1 = IntegrationRegistry.get_instance()
        registry2 = IntegrationRegistry.get_instance()
        assert registry1 is registry2

    def test_register_integration(self):
        """Integration can be registered."""

        class TestIntegration(BaseIntegration):
            name = "test"

            async def health_check(self) -> bool:
                return True

        registry = IntegrationRegistry.get_instance()
        integration = TestIntegration()

        registry.register(integration)
        assert registry.get("test") is integration

    def test_list_integrations(self):
        """List returns all registered integration names."""

        class TestIntegration(BaseIntegration):
            async def health_check(self) -> bool:
                return True

        registry = IntegrationRegistry.get_instance()

        int1 = TestIntegration()
        int1.name = "integration1"
        int2 = TestIntegration()
        int2.name = "integration2"

        registry.register(int1)
        registry.register(int2)

        names = registry.list_integrations()
        assert "integration1" in names
        assert "integration2" in names


# =============================================================================
# Binance Integration Tests
# =============================================================================


class TestBinanceIntegration:
    """Tests for BinanceIntegration."""

    @pytest.fixture
    def binance(self):
        """Create Binance integration."""
        return BinanceIntegration()

    def test_initialization(self, binance):
        """Binance integration initializes correctly."""
        assert binance.name == "binance"
        assert binance.rate_limit_requests == 1200

    def test_valid_intervals(self, binance):
        """Valid intervals are defined."""
        assert "1m" in binance.VALID_INTERVALS
        assert "1h" in binance.VALID_INTERVALS
        assert "1d" in binance.VALID_INTERVALS

    @pytest.mark.asyncio
    async def test_get_ticker_caches_result(self, binance):
        """get_ticker caches the result."""
        mock_data = {
            "symbol": "BTCUSDT",
            "lastPrice": "50000.00",
            "priceChange": "1000.00",
        }

        with patch.object(binance, "_fetch", return_value=mock_data):
            # First call fetches
            result1 = await binance.get_ticker("BTCUSDT")

            # Second call should use cache
            result2 = await binance.get_ticker("BTCUSDT")

            assert result1 == result2
            # _fetch should only be called once
            binance._fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_klines_validates_interval(self, binance):
        """get_klines validates interval parameter."""
        with pytest.raises(ValueError, match="Invalid interval"):
            await binance.get_klines("BTCUSDT", interval="invalid")


# =============================================================================
# CoinGecko Integration Tests
# =============================================================================


class TestCoinGeckoIntegration:
    """Tests for CoinGeckoIntegration."""

    @pytest.fixture
    def coingecko(self, monkeypatch):
        """Create CoinGecko integration without API key (free tier)."""
        # Ensure no API key in environment so we test free tier behavior
        monkeypatch.delenv("COINGECKO_API_KEY", raising=False)
        return CoinGeckoIntegration()

    def test_initialization_free_tier(self, coingecko):
        """CoinGecko free tier initialization."""
        assert coingecko.name == "coingecko"
        # Free tier has rate limit of 30 requests/min
        assert coingecko.rate_limit_requests == 30

    def test_initialization_pro_tier(self):
        """CoinGecko pro tier initialization."""
        coingecko = CoinGeckoIntegration(api_key="test-key")
        assert coingecko.rate_limit_requests == 500

    @pytest.mark.asyncio
    async def test_get_price_returns_dict(self, coingecko):
        """get_price returns price dictionary."""
        mock_data = {"ethereum": {"usd": 2500.50, "eur": 2300.25}}

        with patch.object(coingecko, "_fetch", return_value=mock_data):
            result = await coingecko.get_price("ethereum", vs_currencies=["usd", "eur"])

            assert "usd" in result
            assert "eur" in result
            assert result["usd"] == "2500.5"

    @pytest.mark.asyncio
    async def test_get_prices_returns_dict_of_dicts(self, coingecko):
        """get_prices returns nested dictionary."""
        mock_data = {
            "ethereum": {"usd": 2500.50},
            "bitcoin": {"usd": 45000.00},
        }

        with patch.object(coingecko, "_fetch", return_value=mock_data):
            result = await coingecko.get_prices(["ethereum", "bitcoin"], vs_currencies=["usd"])

            assert "ethereum" in result
            assert "bitcoin" in result
            assert result["ethereum"]["usd"] == "2500.5"


# =============================================================================
# TheGraph Integration Tests
# =============================================================================


class TestTheGraphIntegration:
    """Tests for TheGraphIntegration."""

    @pytest.fixture
    def thegraph(self):
        """Create TheGraph integration."""
        return TheGraphIntegration()

    def test_initialization(self, thegraph):
        """TheGraph integration initializes correctly."""
        assert thegraph.name == "thegraph"

    def test_default_allowed_subgraphs(self, thegraph):
        """Default subgraphs are in allowlist."""
        assert "uniswap-v3-arbitrum" in thegraph.list_allowed_subgraphs()
        assert "aave-v3-arbitrum" in thegraph.list_allowed_subgraphs()

    def test_get_subgraph_url_returns_url_for_allowed(self, thegraph):
        """get_subgraph_url returns URL for allowed subgraphs."""
        url = thegraph.get_subgraph_url("uniswap-v3-ethereum")
        assert url is not None
        assert "thegraph" in url

    def test_get_subgraph_url_returns_none_for_unknown(self, thegraph):
        """get_subgraph_url returns None for unknown subgraphs."""
        url = thegraph.get_subgraph_url("unknown-subgraph")
        assert url is None

    def test_get_subgraph_url_accepts_base58_deployment_id(self):
        """Base58 network subgraph ids resolve when a key is present (ALM-2952)."""
        base58_id = "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
        keyed = TheGraphIntegration(api_key="test-key")
        url = keyed.get_subgraph_url(base58_id)
        assert url == f"https://gateway.thegraph.com/api/test-key/subgraphs/id/{base58_id}"

        keyless = TheGraphIntegration(api_key=None)
        assert keyless.get_subgraph_url(base58_id) is None

    def test_get_subgraph_url_rejects_non_base58_junk(self, thegraph):
        """Arbitrary strings still fail the allowlist (0/O/I/l are not base58)."""
        assert thegraph.get_subgraph_url("l" * 44) is None
        base58_id = "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
        assert TheGraphIntegration(api_key="k").get_subgraph_url(base58_id + "\n") is None
        assert thegraph.get_subgraph_url("../../etc/passwd/aaaaaaaaaaaaaaaaaaaaaaaaaaaaa") is None

    def test_add_allowed_subgraph(self, thegraph):
        """Subgraphs can be added to allowlist."""
        thegraph.add_allowed_subgraph("custom", "https://custom.subgraph.url")

        url = thegraph.get_subgraph_url("custom")
        assert url == "https://custom.subgraph.url"

    @pytest.mark.asyncio
    async def test_query_rejects_unallowed_subgraph(self, thegraph):
        """query rejects subgraphs not in allowlist."""
        with pytest.raises(IntegrationError, match="not in allowlist"):
            await thegraph.query(
                subgraph_id="unknown-subgraph",
                query="{ _meta { block { number } } }",
            )


# =============================================================================
# Zerion Integration Tests
# =============================================================================


class TestZerionIntegration:
    """Tests for ZerionIntegration."""

    @pytest.fixture
    def zerion(self):
        """Create Zerion integration."""
        return ZerionIntegration(api_key="test-portfolio-key", cache_ttl=60)

    def test_initialization(self, zerion):
        """Zerion integration initializes correctly."""
        assert zerion.name == "zerion"
        assert zerion.default_cache_ttl == 60

    def test_auth_header_uses_basic_prefix(self, zerion):
        """Zerion uses the expected Authorization header shape (base64 encoded)."""
        import base64

        headers = zerion._get_headers()
        expected = base64.b64encode(b"test-portfolio-key:").decode()
        assert headers["Authorization"] == f"Basic {expected}"

    @pytest.mark.asyncio
    async def test_get_wallet_positions_normalizes_and_caches(self, zerion):
        """Wallet positions are normalized and cached per wallet+chain."""
        mock_payload = {
            "data": [
                {
                    "id": "pos-1",
                    "type": "liquidity_position",
                    "attributes": {
                        "protocol_name": "traderjoe_v2",
                        "name": "WAVAX/USDT LB",
                        "value": "4.70",
                        "pool_address": "0xpool",
                        "tokens": [{"symbol": "WAVAX"}, {"symbol": "USDT"}],
                    },
                }
            ]
        }

        with patch.object(zerion, "_fetch", return_value=mock_payload) as fetch_mock:
            first = await zerion.get_wallet_positions("0x1234567890123456789012345678901234567890", "avalanche")
            second = await zerion.get_wallet_positions("0x1234567890123456789012345678901234567890", "avalanche")

        assert first.total_value_usd == "4.70"
        assert len(first.positions) == 1
        assert first.positions[0].protocol == "traderjoe_v2"
        assert first.positions[0].pool_address == "0xpool"
        assert first.positions[0].token_symbols == ["WAVAX", "USDT"]
        assert second.cache_hit is True
        fetch_mock.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_wallet_portfolio_extracts_embedded_total(self, zerion):
        """Wallet portfolio total is extracted from Zerion payload."""
        mock_payload = {
            "data": {
                "attributes": {
                    "total_value": "152.25",
                }
            }
        }

        with patch.object(zerion, "_fetch", return_value=mock_payload):
            snapshot = await zerion.get_wallet_portfolio("0x1234567890123456789012345678901234567890", "base")

        assert snapshot.total_value_usd == "152.25"
        assert snapshot.chain == "base"



# =============================================================================
# TheGraph query() HTTP-path tests (mocked session; no network)
# =============================================================================


class _GraphResponse:
    """Minimal stand-in for aiohttp.ClientResponse for TheGraph POSTs."""

    def __init__(self, status: int = 200, body: object = None, text_body: str = "") -> None:
        self.status = status
        self._body = body
        self._text = text_body

    async def json(self):
        return self._body

    async def text(self):
        return self._text


def _graph_session(
    responses: list[_GraphResponse],
    calls: list[dict] | None = None,
    post_exc: Exception | None = None,
):
    """Build a fake aiohttp session whose ``post`` serves canned responses."""
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def fake_post(url, json=None, headers=None):
        if calls is not None:
            calls.append({"url": url, "json": json, "headers": headers})
        if post_exc is not None:
            raise post_exc
        yield responses.pop(0)

    session = AsyncMock()
    session.post = fake_post
    return session


class TestTheGraphQuery:
    """Branch coverage for TheGraphIntegration.query (mocked HTTP)."""

    SUBGRAPH = "uniswap-v3-ethereum"
    QUERY = "{ pools(first: 1) { id } }"

    @pytest.fixture
    def thegraph(self):
        return TheGraphIntegration()

    def _patched(self, thegraph, session):
        return patch.object(thegraph, "_get_session", AsyncMock(return_value=session))

    @pytest.mark.asyncio
    async def test_success_without_errors_returns_data_and_caches(self, thegraph):
        calls: list[dict] = []
        body = {"data": {"pools": [{"id": "0xpool"}]}}
        session = _graph_session([_GraphResponse(body=body)], calls)

        with self._patched(thegraph, session):
            result = await thegraph.query(self.SUBGRAPH, self.QUERY)
            again = await thegraph.query(self.SUBGRAPH, self.QUERY)

        assert result == {"data": {"pools": [{"id": "0xpool"}]}, "success": True}
        # Second call is served from cache: only one HTTP POST happened.
        assert again == result
        assert len(calls) == 1
        assert thegraph._metrics.successful_requests == 1
        # Alias resolved through the allowlist to the subgraph URL.
        assert calls[0]["url"] == thegraph.get_subgraph_url(self.SUBGRAPH)
        assert calls[0]["json"] == {"query": self.QUERY}

    @pytest.mark.asyncio
    async def test_variables_included_in_payload(self, thegraph):
        calls: list[dict] = []
        session = _graph_session([_GraphResponse(body={"data": {}})], calls)
        variables = {"first": 5, "skip": 10}

        with self._patched(thegraph, session):
            await thegraph.query(self.SUBGRAPH, self.QUERY, variables=variables)

        assert calls[0]["json"] == {"query": self.QUERY, "variables": variables}

    @pytest.mark.asyncio
    async def test_graphql_errors_with_partial_data_reports_success(self, thegraph):
        errors = [{"message": "indexing delay"}]
        body = {"data": {"pools": []}, "errors": errors}
        session = _graph_session([_GraphResponse(body=body)])

        with self._patched(thegraph, session):
            result = await thegraph.query(self.SUBGRAPH, self.QUERY)

        assert result == {"data": {"pools": []}, "errors": errors, "success": True}

    @pytest.mark.asyncio
    async def test_graphql_errors_without_data_reports_failure(self, thegraph):
        errors = [{"message": "syntax error"}]
        session = _graph_session([_GraphResponse(body={"errors": errors})])

        with self._patched(thegraph, session):
            result = await thegraph.query(self.SUBGRAPH, self.QUERY)

        assert result == {"data": None, "errors": errors, "success": False}

    @pytest.mark.asyncio
    async def test_http_429_raises_rate_limit_error(self, thegraph):
        session = _graph_session([_GraphResponse(status=429)])

        with self._patched(thegraph, session):
            with pytest.raises(IntegrationRateLimitError):
                await thegraph.query(self.SUBGRAPH, self.QUERY)

        assert thegraph._metrics.rate_limited_requests == 1

    @pytest.mark.asyncio
    async def test_http_error_raises_integration_error_with_status_code(self, thegraph):
        session = _graph_session([_GraphResponse(status=502, text_body="bad gateway")])

        with self._patched(thegraph, session):
            with pytest.raises(IntegrationError, match="HTTP 502: bad gateway") as exc_info:
                await thegraph.query(self.SUBGRAPH, self.QUERY)

        assert exc_info.value.code == "HTTP_502"
        assert thegraph._metrics.failed_requests == 1

    @pytest.mark.asyncio
    async def test_network_error_wrapped_as_integration_error(self, thegraph):
        import aiohttp

        session = _graph_session([], post_exc=aiohttp.ClientError("connection reset"))

        with self._patched(thegraph, session):
            with pytest.raises(IntegrationError, match="connection reset") as exc_info:
                await thegraph.query(self.SUBGRAPH, self.QUERY)

        assert exc_info.value.code == "NETWORK_ERROR"
        assert thegraph._metrics.failed_requests == 1
        assert isinstance(exc_info.value.__cause__, aiohttp.ClientError)

    @pytest.mark.asyncio
    async def test_unallowed_subgraph_raises_before_any_http(self, thegraph):
        calls: list[dict] = []
        session = _graph_session([], calls)

        with self._patched(thegraph, session):
            with pytest.raises(IntegrationError, match="not in allowlist") as exc_info:
                await thegraph.query("definitely-not-allowed", self.QUERY)

        assert exc_info.value.code == "SUBGRAPH_NOT_ALLOWED"
        assert calls == []


# =============================================================================
# Zerion _extract_protocol tests
# =============================================================================


class TestZerionExtractProtocol:
    """Branch coverage for ZerionIntegration._extract_protocol."""

    @pytest.fixture
    def zerion(self):
        return ZerionIntegration(api_key="test-portfolio-key", cache_ttl=60)

    def test_item_level_string_protocol_wins(self, zerion):
        assert zerion._extract_protocol({"protocol": "aave-v3"}, {"protocol": "ignored"}) == "aave-v3"

    def test_dict_candidate_prefers_name(self, zerion):
        item = {"protocol": {"name": "Aave V3", "slug": "aave-v3", "id": "aave3"}}
        assert zerion._extract_protocol(item, {}) == "Aave V3"

    def test_dict_candidate_falls_back_to_slug_then_id(self, zerion):
        assert zerion._extract_protocol({"protocol": {"slug": "aave-v3", "id": "aave3"}}, {}) == "aave-v3"
        assert zerion._extract_protocol({"protocol": {"id": "aave3"}}, {}) == "aave3"

    def test_empty_dict_candidate_skipped_in_favor_of_attributes(self, zerion):
        # {} is a dict candidate that resolves to None -> falls through to
        # the attributes-level candidates.
        assert zerion._extract_protocol({"protocol": {}}, {"protocol": "compound"}) == "compound"

    def test_attribute_fallback_order(self, zerion):
        assert zerion._extract_protocol({}, {"protocol_slug": "uniswap-v3"}) == "uniswap-v3"
        assert zerion._extract_protocol({}, {"protocol_name": "Uniswap V3"}) == "Uniswap V3"
        # protocol beats protocol_slug beats protocol_name
        attrs = {"protocol": "one", "protocol_slug": "two", "protocol_name": "three"}
        assert zerion._extract_protocol({}, attrs) == "one"

    def test_relationships_data_id_used_when_no_direct_candidates(self, zerion):
        item = {"relationships": {"protocol": {"data": {"id": "lido"}}}}
        assert zerion._extract_protocol(item, {}) == "lido"

    def test_relationships_data_name_used_when_id_missing(self, zerion):
        item = {"relationships": {"protocol": {"data": {"name": "Lido"}}}}
        assert zerion._extract_protocol(item, {}) == "Lido"

    def test_malformed_relationships_shapes_fall_through_to_unknown(self, zerion):
        # relationships not a dict
        assert zerion._extract_protocol({"relationships": ["nope"]}, {}) == "unknown"
        # protocol relationship not a dict
        assert zerion._extract_protocol({"relationships": {"protocol": "nope"}}, {}) == "unknown"
        # data not a dict
        assert zerion._extract_protocol({"relationships": {"protocol": {"data": None}}}, {}) == "unknown"
        # data dict but id/name empty
        item = {"relationships": {"protocol": {"data": {"id": "", "name": ""}}}}
        assert zerion._extract_protocol(item, {}) == "unknown"

    def test_no_signals_at_all_returns_unknown(self, zerion):
        assert zerion._extract_protocol({}, {}) == "unknown"
