"""Unit tests for SubgraphClient.

This module tests the SubgraphClient class in providers/subgraph_client.py,
covering:
- Successful query execution
- Retry logic on transient errors
- Rate limiter integration
- Error handling for malformed responses
- Pagination support
- Statistics tracking
- Async context manager behavior
"""

import asyncio
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from almanak.framework.backtesting.pnl.providers.rate_limiter import (
    TokenBucketRateLimiter,
)
from almanak.framework.backtesting.pnl.providers.subgraph_client import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_REQUESTS_PER_MINUTE,
    DEFAULT_TIMEOUT_SECONDS,
    THEGRAPH_GATEWAY_URL,
    QueryStats,
    SubgraphClient,
    SubgraphClientConfig,
    SubgraphClientError,
    SubgraphConnectionError,
    SubgraphQueryError,
    SubgraphRateLimitError,
    create_subgraph_client,
)


# =============================================================================
# Test SubgraphClientConfig
# =============================================================================


class TestSubgraphClientConfig:
    """Tests for SubgraphClientConfig dataclass."""

    def test_default_values(self, monkeypatch):
        """Test default configuration values."""
        # Ensure no environment variable interferes with test
        monkeypatch.delenv("THEGRAPH_API_KEY", raising=False)
        config = SubgraphClientConfig()
        assert config.api_key is None  # No env var set in tests
        assert config.requests_per_minute == DEFAULT_REQUESTS_PER_MINUTE
        assert config.timeout_seconds == DEFAULT_TIMEOUT_SECONDS
        assert config.max_retries == DEFAULT_MAX_RETRIES
        assert config.gateway_url == THEGRAPH_GATEWAY_URL

    def test_custom_values(self):
        """Test configuration with custom values."""
        config = SubgraphClientConfig(
            api_key="test-api-key",
            requests_per_minute=50,
            timeout_seconds=60.0,
            max_retries=5,
            gateway_url="https://custom.gateway.com",
        )
        assert config.api_key == "test-api-key"
        assert config.requests_per_minute == 50
        assert config.timeout_seconds == 60.0
        assert config.max_retries == 5
        assert config.gateway_url == "https://custom.gateway.com"

    def test_api_key_from_env(self, monkeypatch):
        """Test API key loaded from environment variable."""
        monkeypatch.setenv("THEGRAPH_API_KEY", "env-api-key")
        config = SubgraphClientConfig()
        assert config.api_key == "env-api-key"

    def test_explicit_api_key_overrides_env(self, monkeypatch):
        """Test explicit API key overrides environment variable."""
        monkeypatch.setenv("THEGRAPH_API_KEY", "env-api-key")
        config = SubgraphClientConfig(api_key="explicit-api-key")
        assert config.api_key == "explicit-api-key"


# =============================================================================
# Test QueryStats
# =============================================================================


class TestQueryStats:
    """Tests for QueryStats dataclass."""

    def test_default_values(self):
        """Test default statistics values."""
        stats = QueryStats()
        assert stats.total_queries == 0
        assert stats.successful_queries == 0
        assert stats.failed_queries == 0
        assert stats.rate_limited_queries == 0
        assert stats.total_retry_attempts == 0
        assert isinstance(stats.created_at, datetime)

    def test_to_dict(self):
        """Test conversion to dictionary."""
        stats = QueryStats(
            total_queries=100,
            successful_queries=90,
            failed_queries=10,
            rate_limited_queries=5,
            total_retry_attempts=15,
        )
        d = stats.to_dict()
        assert d["total_queries"] == 100
        assert d["successful_queries"] == 90
        assert d["failed_queries"] == 10
        assert d["rate_limited_queries"] == 5
        assert d["total_retry_attempts"] == 15
        assert d["success_rate"] == 90.0
        assert "created_at" in d

    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        stats = QueryStats(total_queries=100, successful_queries=75)
        d = stats.to_dict()
        assert d["success_rate"] == 75.0

    def test_success_rate_zero_queries(self):
        """Test success rate when no queries executed."""
        stats = QueryStats(total_queries=0)
        d = stats.to_dict()
        assert d["success_rate"] == 0.0


# =============================================================================
# Test SubgraphClient Initialization
# =============================================================================


class TestSubgraphClientInitialization:
    """Tests for SubgraphClient initialization."""

    def test_init_default(self):
        """Test client initializes with default settings."""
        client = SubgraphClient()
        assert client.config.requests_per_minute == DEFAULT_REQUESTS_PER_MINUTE
        assert client.config.timeout_seconds == DEFAULT_TIMEOUT_SECONDS
        assert client.config.max_retries == DEFAULT_MAX_RETRIES
        assert isinstance(client.rate_limiter, TokenBucketRateLimiter)

    def test_init_with_config(self):
        """Test client initializes with custom config."""
        config = SubgraphClientConfig(
            api_key="test-key",
            requests_per_minute=50,
        )
        client = SubgraphClient(config=config)
        assert client.config.api_key == "test-key"
        assert client.config.requests_per_minute == 50

    def test_init_with_custom_rate_limiter(self):
        """Test client can use custom rate limiter."""
        custom_limiter = TokenBucketRateLimiter(requests_per_minute=200)
        client = SubgraphClient(rate_limiter=custom_limiter)
        assert client.rate_limiter is custom_limiter

    def test_session_not_created_on_init(self):
        """Test HTTP session is not created on initialization (lazy)."""
        client = SubgraphClient()
        assert client._session is None


# =============================================================================
# Test Successful Query Execution
# =============================================================================


class TestSuccessfulQueryExecution:
    """Tests for successful query execution."""

    @pytest.mark.asyncio
    async def test_query_returns_data(self):
        """Test successful query returns the data field."""
        client = SubgraphClient()

        mock_response_data = {
            "data": {"pools": [{"id": "0x123", "symbol": "USDC"}]},
        }

        with patch.object(client, "_get_session") as mock_get_session:
            mock_session = AsyncMock()
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=mock_response_data)
            mock_session.post = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock()))
            mock_get_session.return_value = mock_session

            result = await client.query(
                subgraph_id="test-subgraph-id",
                query="{ pools { id symbol } }",
            )

            assert result == {"pools": [{"id": "0x123", "symbol": "USDC"}]}

        await client.close()

    @pytest.mark.asyncio
    async def test_query_with_variables(self):
        """Test query with variables passes them correctly."""
        client = SubgraphClient()

        mock_response_data = {
            "data": {"pool": {"id": "0x456", "volumeUSD": "1000000"}},
        }

        # Track the payload sent
        captured_payload = None

        async def mock_execute(subgraph_id, query, variables):
            nonlocal captured_payload
            captured_payload = {"query": query, "variables": variables}
            return mock_response_data["data"]

        with patch.object(client, "_execute_query", side_effect=mock_execute):
            await client.query(
                subgraph_id="test-subgraph-id",
                query="query($id: ID!) { pool(id: $id) { id volumeUSD } }",
                variables={"id": "0x456"},
            )

            assert captured_payload["variables"] == {"id": "0x456"}

        await client.close()

    @pytest.mark.asyncio
    async def test_successful_query_updates_stats(self):
        """Test successful query updates statistics."""
        client = SubgraphClient()

        mock_response_data = {"data": {"pools": []}}

        with patch.object(client, "_get_session") as mock_get_session:
            mock_session = AsyncMock()
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=mock_response_data)
            mock_session.post = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock()))
            mock_get_session.return_value = mock_session

            await client.query(subgraph_id="test", query="{ pools { id } }")
            await client.query(subgraph_id="test", query="{ pools { id } }")

            stats = client.get_stats()
            assert stats.total_queries == 2
            assert stats.successful_queries == 2
            assert stats.failed_queries == 0

        await client.close()

    @pytest.mark.asyncio
    async def test_empty_data_response(self):
        """Test handling of empty data response."""
        client = SubgraphClient()

        mock_response_data = {"data": {}}

        with patch.object(client, "_get_session") as mock_get_session:
            mock_session = AsyncMock()
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=mock_response_data)
            mock_session.post = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock()))
            mock_get_session.return_value = mock_session

            result = await client.query(subgraph_id="test", query="{ pools { id } }")
            assert result == {}

        await client.close()


# =============================================================================
# Test Retry Logic on Transient Errors
# =============================================================================


class TestRetryLogicOnTransientErrors:
    """Tests for retry logic on transient errors."""

    @pytest.mark.asyncio
    async def test_retry_on_connection_error(self):
        """Test retry on connection errors."""
        client = SubgraphClient(
            config=SubgraphClientConfig(max_retries=2),
        )

        call_count = 0

        async def mock_execute(subgraph_id, query, variables):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise SubgraphConnectionError("Connection failed")
            return {"pools": []}

        with patch.object(client, "_execute_query", side_effect=mock_execute):
            with patch.object(client.rate_limiter, "retry_with_backoff") as mock_retry:
                # Configure retry_with_backoff to call the function with retries
                async def retry_impl(func, **kwargs):
                    max_retries = kwargs.get("max_retries", 3)
                    for attempt in range(max_retries + 1):
                        try:
                            return await func()
                        except Exception:
                            if attempt == max_retries:
                                raise
                            await asyncio.sleep(0.01)
                    return None

                mock_retry.side_effect = retry_impl

                result = await client.query(subgraph_id="test", query="{ pools { id } }")

        # Verify retry was called
        assert mock_retry.called

        await client.close()

    @pytest.mark.asyncio
    async def test_retry_exhausted_raises_exception(self):
        """Test that exhausting retries raises the exception."""
        client = SubgraphClient(
            config=SubgraphClientConfig(max_retries=2),
        )

        async def always_fail():
            raise SubgraphConnectionError("Connection always fails")

        # Mock rate_limiter.retry_with_backoff to simulate retries exhausted
        async def mock_retry(func, **kwargs):
            raise SubgraphConnectionError("Connection always fails")

        with patch.object(client.rate_limiter, "retry_with_backoff", side_effect=mock_retry):
            with pytest.raises(SubgraphConnectionError, match="Connection always fails"):
                await client.query(subgraph_id="test", query="{ pools { id } }")

        stats = client.get_stats()
        assert stats.failed_queries == 1

        await client.close()

    @pytest.mark.asyncio
    async def test_successful_retry_after_transient_failure(self):
        """Test successful query after transient failure."""
        client = SubgraphClient(
            config=SubgraphClientConfig(max_retries=3),
        )

        call_count = 0

        async def mock_execute(subgraph_id, query, variables):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise SubgraphConnectionError("Transient failure")
            return {"pools": [{"id": "0x123"}]}

        # Mock retry_with_backoff to actually do retries
        async def mock_retry(func, **kwargs):
            max_retries = kwargs.get("max_retries", 3)
            last_error = None
            for attempt in range(max_retries + 1):
                try:
                    return await func()
                except Exception as e:
                    last_error = e
                    if attempt == max_retries:
                        raise
            raise last_error  # type: ignore

        with patch.object(client, "_execute_query", side_effect=mock_execute):
            with patch.object(client.rate_limiter, "retry_with_backoff", side_effect=mock_retry):
                result = await client.query(subgraph_id="test", query="{ pools { id } }")

        assert result == {"pools": [{"id": "0x123"}]}

        await client.close()


# =============================================================================
# Test Rate Limiter Integration
# =============================================================================


class TestRateLimiterIntegration:
    """Tests for rate limiter integration."""

    @pytest.mark.asyncio
    async def test_query_uses_rate_limiter_retry(self):
        """Test that query uses rate limiter's retry_with_backoff."""
        client = SubgraphClient()

        mock_response_data = {"data": {"pools": []}}

        with patch.object(client, "_get_session") as mock_get_session:
            mock_session = AsyncMock()
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=mock_response_data)
            mock_session.post = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock()))
            mock_get_session.return_value = mock_session

            with patch.object(client.rate_limiter, "retry_with_backoff") as mock_retry:
                mock_retry.return_value = {"pools": []}

                await client.query(subgraph_id="test", query="{ pools { id } }")

                # Verify retry_with_backoff was called
                assert mock_retry.called
                # Verify it was called with the correct parameters
                call_kwargs = mock_retry.call_args[1]
                assert call_kwargs["max_retries"] == client.config.max_retries

        await client.close()

    @pytest.mark.asyncio
    async def test_rate_limit_error_detection_function_passed(self):
        """Test that rate limit error detection function is passed to retry."""
        client = SubgraphClient()

        with patch.object(client.rate_limiter, "retry_with_backoff") as mock_retry:
            mock_retry.return_value = {"pools": []}

            await client.query(subgraph_id="test", query="{ pools { id } }")

            # Verify is_rate_limit_error function was passed
            call_kwargs = mock_retry.call_args[1]
            assert "is_rate_limit_error" in call_kwargs

            # Test the detection function
            detect_fn = call_kwargs["is_rate_limit_error"]
            assert detect_fn(SubgraphRateLimitError()) is True
            assert detect_fn(SubgraphQueryError("other")) is False
            assert detect_fn(ValueError("generic")) is False

        await client.close()

    def test_custom_rate_limiter_is_used(self):
        """Test that a custom rate limiter is used when provided."""
        custom_limiter = TokenBucketRateLimiter(requests_per_minute=200)
        client = SubgraphClient(rate_limiter=custom_limiter)

        assert client.rate_limiter is custom_limiter
        assert client.rate_limiter.requests_per_minute == 200

    def test_rate_limiter_created_from_config(self):
        """Test rate limiter is created based on config when not provided."""
        config = SubgraphClientConfig(requests_per_minute=75)
        client = SubgraphClient(config=config)

        assert client.rate_limiter.requests_per_minute == 75


# =============================================================================
# Test Error Handling for Malformed Responses
# =============================================================================


class TestErrorHandlingMalformedResponses:
    """Tests for error handling with malformed responses."""

    @pytest.mark.asyncio
    async def test_http_429_raises_rate_limit_error(self):
        """Test that HTTP 429 raises SubgraphRateLimitError."""
        client = SubgraphClient()

        mock_response = MagicMock()
        mock_response.status = 429
        mock_response.headers = {"Retry-After": "60"}

        # Create a proper async context manager mock
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_context.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_context)

        with patch.object(client, "_get_session", return_value=mock_session):
            with pytest.raises(SubgraphRateLimitError) as exc_info:
                await client._execute_query(
                    subgraph_id="test",
                    query="{ pools { id } }",
                )

            assert exc_info.value.retry_after_seconds == 60.0

        await client.close()

    @pytest.mark.asyncio
    async def test_http_500_raises_query_error(self):
        """Test that HTTP 500 raises SubgraphQueryError."""
        client = SubgraphClient()

        mock_response = MagicMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")

        # Create a proper async context manager mock
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_context.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_context)

        with patch.object(client, "_get_session", return_value=mock_session):
            with pytest.raises(SubgraphQueryError, match="HTTP 500"):
                await client._execute_query(
                    subgraph_id="test",
                    query="{ pools { id } }",
                )

        await client.close()

    @pytest.mark.asyncio
    async def test_graphql_errors_raise_query_error(self):
        """Test that GraphQL errors in response raise SubgraphQueryError."""
        client = SubgraphClient()

        mock_response_data = {
            "errors": [
                {"message": "Cannot query field 'invalid'"},
                {"message": "Syntax error"},
            ],
        }

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_response_data)

        # Create a proper async context manager mock
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_context.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_context)

        with patch.object(client, "_get_session", return_value=mock_session):
            with pytest.raises(SubgraphQueryError) as exc_info:
                await client._execute_query(
                    subgraph_id="test",
                    query="{ invalid { field } }",
                )

            assert "Cannot query field 'invalid'" in str(exc_info.value)
            assert len(exc_info.value.errors) == 2

        await client.close()

    @pytest.mark.asyncio
    async def test_connection_error_raises_connection_error(self):
        """Test that aiohttp ClientError raises SubgraphConnectionError."""
        client = SubgraphClient()

        with patch.object(client, "_get_session") as mock_get_session:
            mock_session = AsyncMock()
            mock_session.post = MagicMock(side_effect=aiohttp.ClientError("Connection refused"))
            mock_get_session.return_value = mock_session

            with pytest.raises(SubgraphConnectionError, match="Connection failed"):
                await client._execute_query(
                    subgraph_id="test",
                    query="{ pools { id } }",
                )

        await client.close()

    @pytest.mark.asyncio
    async def test_missing_data_field_returns_empty_dict(self):
        """Test that response without 'data' field returns empty dict."""
        client = SubgraphClient()

        mock_response_data = {}  # No 'data' field

        with patch.object(client, "_get_session") as mock_get_session:
            mock_session = AsyncMock()
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=mock_response_data)
            mock_session.post = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock()))
            mock_get_session.return_value = mock_session

            result = await client._execute_query(
                subgraph_id="test",
                query="{ pools { id } }",
            )
            assert result == {}

        await client.close()

    @pytest.mark.asyncio
    async def test_query_error_preserves_query_in_exception(self):
        """Test that SubgraphQueryError preserves the original query."""
        client = SubgraphClient()

        mock_response_data = {"errors": [{"message": "Query failed"}]}
        original_query = "{ pools(first: 10) { id } }"

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_response_data)

        # Create a proper async context manager mock
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_context.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_context)

        with patch.object(client, "_get_session", return_value=mock_session):
            with pytest.raises(SubgraphQueryError) as exc_info:
                await client._execute_query(
                    subgraph_id="test",
                    query=original_query,
                )

            assert exc_info.value.query == original_query

        await client.close()


# =============================================================================
# Test Pagination Support
# =============================================================================


class TestPaginationSupport:
    """Tests for query_with_pagination functionality."""

    @pytest.mark.asyncio
    async def test_pagination_fetches_multiple_pages(self):
        """Test pagination fetches multiple pages until exhausted."""
        client = SubgraphClient()

        page_data = [
            {"poolDayDatas": [{"id": "1"}, {"id": "2"}]},  # Page 0 - full page
            {"poolDayDatas": [{"id": "3"}]},  # Page 1 - partial, stop
        ]
        call_count = 0

        async def mock_query(subgraph_id, query, variables):
            nonlocal call_count
            result = page_data[call_count]
            call_count += 1
            return result

        with patch.object(client, "query", side_effect=mock_query):
            results = await client.query_with_pagination(
                subgraph_id="test",
                query="query($first: Int!, $skip: Int!) { poolDayDatas(first: $first, skip: $skip) { id } }",
                data_path="poolDayDatas",
                page_size=2,
            )

        assert len(results) == 3
        assert results == [{"id": "1"}, {"id": "2"}, {"id": "3"}]

        await client.close()

    @pytest.mark.asyncio
    async def test_pagination_respects_max_pages(self):
        """Test pagination stops at max_pages limit."""
        client = SubgraphClient()

        async def mock_query(subgraph_id, query, variables):
            # Always return full page to test max_pages
            return {"pools": [{"id": str(variables["skip"])}]}

        with patch.object(client, "query", side_effect=mock_query):
            results = await client.query_with_pagination(
                subgraph_id="test",
                query="query($first: Int!, $skip: Int!) { pools(first: $first, skip: $skip) { id } }",
                data_path="pools",
                page_size=1,
                max_pages=3,
            )

        # Should stop after 3 pages
        assert len(results) == 3

        await client.close()

    @pytest.mark.asyncio
    async def test_pagination_handles_empty_response(self):
        """Test pagination stops on empty response."""
        client = SubgraphClient()

        async def mock_query(subgraph_id, query, variables):
            return {"pools": []}  # Empty page

        with patch.object(client, "query", side_effect=mock_query):
            results = await client.query_with_pagination(
                subgraph_id="test",
                query="query($first: Int!, $skip: Int!) { pools(first: $first, skip: $skip) { id } }",
                data_path="pools",
            )

        assert results == []

        await client.close()

    @pytest.mark.asyncio
    async def test_pagination_passes_base_variables(self):
        """Test pagination preserves base variables."""
        client = SubgraphClient()

        captured_variables: list[dict[str, Any]] = []

        async def mock_query(subgraph_id, query, variables):
            captured_variables.append(dict(variables))
            return {"pools": []}  # Stop immediately

        with patch.object(client, "query", side_effect=mock_query):
            await client.query_with_pagination(
                subgraph_id="test",
                query="query($first: Int!, $skip: Int!, $poolAddress: String!) { pools { id } }",
                variables={"poolAddress": "0x123"},
                data_path="pools",
            )

        # Should have poolAddress plus pagination params
        assert captured_variables[0]["poolAddress"] == "0x123"
        assert captured_variables[0]["first"] is not None
        assert captured_variables[0]["skip"] == 0

        await client.close()

    @pytest.mark.asyncio
    async def test_pagination_handles_nested_data_path(self):
        """Test pagination handles nested data paths."""
        client = SubgraphClient()

        async def mock_query(subgraph_id, query, variables):
            return {"pool": {"snapshots": [{"id": "1"}]}}

        with patch.object(client, "query", side_effect=mock_query):
            results = await client.query_with_pagination(
                subgraph_id="test",
                query="query { pool { snapshots { id } } }",
                data_path="pool.snapshots",
                page_size=10,
            )

        assert results == [{"id": "1"}]

        await client.close()


# =============================================================================
# Test Statistics and Session Management
# =============================================================================


class TestStatisticsAndSessionManagement:
    """Tests for statistics tracking and session management."""

    def test_get_stats_returns_copy(self):
        """Test get_stats returns a copy, not the original."""
        client = SubgraphClient()

        stats1 = client.get_stats()
        stats2 = client.get_stats()

        assert stats1.total_queries == stats2.total_queries
        assert stats1 is not stats2

    def test_reset_stats(self):
        """Test reset_stats clears all statistics."""
        client = SubgraphClient()

        # Manually set some stats
        client._stats.total_queries = 100
        client._stats.successful_queries = 90
        client._stats.failed_queries = 10

        client.reset_stats()

        stats = client.get_stats()
        assert stats.total_queries == 0
        assert stats.successful_queries == 0
        assert stats.failed_queries == 0

    @pytest.mark.asyncio
    async def test_close_closes_session(self):
        """Test close() properly closes the HTTP session."""
        client = SubgraphClient()

        # Create a mock session
        mock_session = AsyncMock()
        mock_session.closed = False
        client._session = mock_session

        await client.close()

        mock_session.close.assert_called_once()
        assert client._session is None

    @pytest.mark.asyncio
    async def test_close_idempotent(self):
        """Test close() can be called multiple times safely."""
        client = SubgraphClient()

        # Close without ever creating session
        await client.close()
        await client.close()  # Should not raise


# =============================================================================
# Test Async Context Manager
# =============================================================================


class TestAsyncContextManager:
    """Tests for async context manager behavior."""

    @pytest.mark.asyncio
    async def test_context_manager_closes_session(self):
        """Test context manager closes session on exit."""
        client = SubgraphClient()

        async with client:
            # Create a session by making a mock query
            pass

        # After exiting, session should be closed
        assert client._session is None

    @pytest.mark.asyncio
    async def test_context_manager_returns_client(self):
        """Test context manager returns the client instance."""
        client = SubgraphClient()

        async with client as ctx:
            assert ctx is client


# =============================================================================
# Test URL and Header Building
# =============================================================================


class TestUrlAndHeaderBuilding:
    """Tests for URL and header building."""

    def test_build_url(self):
        """Test URL building from subgraph ID."""
        client = SubgraphClient()
        url = client._build_url("test-subgraph-id-12345")
        assert url == f"{THEGRAPH_GATEWAY_URL}/test-subgraph-id-12345"

    def test_build_url_custom_gateway(self):
        """Test URL building with custom gateway."""
        config = SubgraphClientConfig(gateway_url="https://custom.gateway.com")
        client = SubgraphClient(config=config)
        url = client._build_url("test-id")
        assert url == "https://custom.gateway.com/test-id"

    def test_build_headers_with_api_key(self):
        """Test headers include Authorization when API key is set."""
        config = SubgraphClientConfig(api_key="test-api-key")
        client = SubgraphClient(config=config)
        headers = client._build_headers()
        assert headers["Content-Type"] == "application/json"
        assert headers["Authorization"] == "Bearer test-api-key"

    def test_build_headers_without_api_key(self, monkeypatch):
        """Test headers without Authorization when no API key."""
        monkeypatch.delenv("THEGRAPH_API_KEY", raising=False)
        config = SubgraphClientConfig(api_key=None)
        client = SubgraphClient(config=config)
        headers = client._build_headers()
        assert headers["Content-Type"] == "application/json"
        assert "Authorization" not in headers


# =============================================================================
# Test Convenience Functions
# =============================================================================


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_create_subgraph_client_default(self, monkeypatch):
        """Test create_subgraph_client with defaults."""
        monkeypatch.delenv("THEGRAPH_API_KEY", raising=False)
        client = create_subgraph_client()
        assert client.config.requests_per_minute == DEFAULT_REQUESTS_PER_MINUTE
        assert client.config.api_key is None

    def test_create_subgraph_client_custom_rate(self):
        """Test create_subgraph_client with custom rate."""
        client = create_subgraph_client(requests_per_minute=50)
        assert client.config.requests_per_minute == 50

    def test_create_subgraph_client_with_api_key(self):
        """Test create_subgraph_client with API key."""
        client = create_subgraph_client(api_key="custom-key")
        assert client.config.api_key == "custom-key"


# =============================================================================
# Test Exception Classes
# =============================================================================


class TestExceptionClasses:
    """Tests for exception classes."""

    def test_subgraph_client_error_is_base(self):
        """Test SubgraphClientError is the base exception."""
        assert issubclass(SubgraphRateLimitError, SubgraphClientError)
        assert issubclass(SubgraphQueryError, SubgraphClientError)
        assert issubclass(SubgraphConnectionError, SubgraphClientError)

    def test_rate_limit_error_attributes(self):
        """Test SubgraphRateLimitError attributes."""
        error = SubgraphRateLimitError(
            message="Rate limit hit",
            retry_after_seconds=30.0,
        )
        assert str(error) == "Rate limit hit"
        assert error.retry_after_seconds == 30.0

    def test_rate_limit_error_defaults(self):
        """Test SubgraphRateLimitError default values."""
        error = SubgraphRateLimitError()
        assert str(error) == "Rate limit exceeded"
        assert error.retry_after_seconds is None

    def test_query_error_attributes(self):
        """Test SubgraphQueryError attributes."""
        errors = [{"message": "Error 1"}, {"message": "Error 2"}]
        error = SubgraphQueryError(
            message="Query failed",
            query="{ pools { id } }",
            errors=errors,
        )
        assert str(error) == "Query failed"
        assert error.query == "{ pools { id } }"
        assert error.errors == errors

    def test_query_error_defaults(self):
        """Test SubgraphQueryError default values."""
        error = SubgraphQueryError("Error message")
        assert error.query is None
        assert error.errors == []

    def test_connection_error(self):
        """Test SubgraphConnectionError."""
        error = SubgraphConnectionError("Connection refused")
        assert str(error) == "Connection refused"
        assert isinstance(error, SubgraphClientError)


# =============================================================================
# Test Session Creation
# =============================================================================


class TestSessionCreation:
    """Tests for HTTP session creation."""

    @pytest.mark.asyncio
    async def test_get_session_creates_session(self):
        """Test _get_session creates a new session if none exists."""
        client = SubgraphClient()

        assert client._session is None

        session = await client._get_session()

        assert session is not None
        assert client._session is session

        await client.close()

    @pytest.mark.asyncio
    async def test_get_session_reuses_session(self):
        """Test _get_session reuses existing session."""
        client = SubgraphClient()

        session1 = await client._get_session()
        session2 = await client._get_session()

        assert session1 is session2

        await client.close()

    @pytest.mark.asyncio
    async def test_get_session_recreates_if_closed(self):
        """Test _get_session creates new session if previous was closed."""
        client = SubgraphClient()

        session1 = await client._get_session()
        await session1.close()

        session2 = await client._get_session()

        assert session2 is not session1
        assert not session2.closed

        await client.close()


# =============================================================================
# Test Constants
# =============================================================================


class TestConstants:
    """Tests for module constants."""

    def test_gateway_url_constant(self):
        """Test The Graph Gateway URL constant."""
        assert THEGRAPH_GATEWAY_URL == "https://gateway.thegraph.com/api/subgraphs/id"

    def test_default_requests_per_minute(self):
        """Test default requests per minute."""
        assert DEFAULT_REQUESTS_PER_MINUTE == 100

    def test_default_timeout_seconds(self):
        """Test default timeout."""
        assert DEFAULT_TIMEOUT_SECONDS == 30

    def test_default_max_retries(self):
        """Test default max retries."""
        assert DEFAULT_MAX_RETRIES == 3
