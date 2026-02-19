"""Unified SubgraphClient for querying The Graph subgraphs.

This module provides a unified client for querying The Graph's decentralized
network of subgraphs. It supports rate limiting, retry logic with exponential
backoff, connection pooling, and multiple subgraph endpoints.

Key Features:
    - Unified client for all subgraph queries
    - Configurable API key via THEGRAPH_API_KEY environment variable
    - Rate limiting integration with TokenBucketRateLimiter
    - Exponential backoff retry logic for transient failures
    - Connection pooling with aiohttp ClientSession
    - Support for multiple subgraph endpoints via subgraph_id

Example:
    from almanak.framework.backtesting.pnl.providers.subgraph_client import (
        SubgraphClient,
    )

    # Create client (uses THEGRAPH_API_KEY env var)
    client = SubgraphClient()

    # Query a specific subgraph
    query = '''
    query GetPoolDayData($poolAddress: String!) {
        poolDayDatas(where: { pool: $poolAddress }) {
            volumeUSD
        }
    }
    '''
    result = await client.query(
        subgraph_id="5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
        query=query,
        variables={"poolAddress": "0x..."},
    )
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import aiohttp

from .rate_limiter import TokenBucketRateLimiter

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# The Graph Gateway URL (decentralized network)
THEGRAPH_GATEWAY_URL = "https://gateway.thegraph.com/api/subgraphs/id"

# Default rate limit: 100 requests per minute for The Graph
DEFAULT_REQUESTS_PER_MINUTE = 100

# Default HTTP timeout in seconds
DEFAULT_TIMEOUT_SECONDS = 30

# Default max retries for failed requests
DEFAULT_MAX_RETRIES = 3


# =============================================================================
# Exceptions
# =============================================================================


class SubgraphClientError(Exception):
    """Base exception for SubgraphClient errors."""


class SubgraphRateLimitError(SubgraphClientError):
    """Raised when The Graph rate limit is exceeded."""

    def __init__(
        self,
        message: str = "Rate limit exceeded",
        retry_after_seconds: float | None = None,
    ) -> None:
        """Initialize rate limit error.

        Args:
            message: Error message
            retry_after_seconds: Suggested wait time before retrying
        """
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class SubgraphQueryError(SubgraphClientError):
    """Raised when a subgraph query fails."""

    def __init__(
        self,
        message: str,
        query: str | None = None,
        errors: list[dict[str, Any]] | None = None,
    ) -> None:
        """Initialize query error.

        Args:
            message: Error message
            query: The GraphQL query that failed
            errors: List of error details from the response
        """
        super().__init__(message)
        self.query = query
        self.errors = errors or []


class SubgraphConnectionError(SubgraphClientError):
    """Raised when connection to subgraph fails."""


# =============================================================================
# Data Classes
# =============================================================================


def _mask_api_key(key: str | None) -> str:
    """Mask an API key for safe logging.

    Args:
        key: The API key to mask (or None)

    Returns:
        Masked key showing first 4 and last 4 characters, or "not_set" if None
    """
    if not key:
        return "not_set"
    if len(key) <= 8:
        return "***"
    return f"{key[:4]}...{key[-4:]}"


@dataclass
class SubgraphClientConfig:
    """Configuration for SubgraphClient.

    Attributes:
        api_key: The Graph API key (defaults to THEGRAPH_API_KEY env var)
        requests_per_minute: Rate limit for requests (default: 100)
        timeout_seconds: HTTP request timeout (default: 30)
        max_retries: Maximum retry attempts for failed requests (default: 3)
        gateway_url: The Graph Gateway URL (default: production gateway)
    """

    api_key: str | None = None
    requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    max_retries: int = DEFAULT_MAX_RETRIES
    gateway_url: str = THEGRAPH_GATEWAY_URL

    def __post_init__(self) -> None:
        """Load API key from environment if not provided."""
        if self.api_key is None:
            self.api_key = os.environ.get("THEGRAPH_API_KEY")

    def __repr__(self) -> str:
        """Return a safe representation without exposing the API key."""
        return (
            f"SubgraphClientConfig("
            f"api_key={_mask_api_key(self.api_key)}, "
            f"requests_per_minute={self.requests_per_minute}, "
            f"timeout_seconds={self.timeout_seconds}, "
            f"max_retries={self.max_retries})"
        )


@dataclass
class QueryStats:
    """Statistics for SubgraphClient queries.

    Attributes:
        total_queries: Total number of queries executed
        successful_queries: Number of successful queries
        failed_queries: Number of failed queries
        rate_limited_queries: Number of queries rate limited
        total_retry_attempts: Total retry attempts across all queries
        created_at: When the client was created
    """

    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0
    rate_limited_queries: int = 0
    total_retry_attempts: int = 0
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging/metrics."""
        return {
            "total_queries": self.total_queries,
            "successful_queries": self.successful_queries,
            "failed_queries": self.failed_queries,
            "rate_limited_queries": self.rate_limited_queries,
            "total_retry_attempts": self.total_retry_attempts,
            "success_rate": (self.successful_queries / self.total_queries * 100 if self.total_queries > 0 else 0.0),
            "created_at": self.created_at.isoformat(),
        }


# =============================================================================
# SubgraphClient
# =============================================================================


class SubgraphClient:
    """Unified client for querying The Graph subgraphs.

    This client provides a unified interface for querying any subgraph on
    The Graph's decentralized network. It handles:
    - Rate limiting via TokenBucketRateLimiter
    - Retry logic with exponential backoff
    - Connection pooling with aiohttp
    - API key authentication

    Attributes:
        config: Client configuration
        rate_limiter: TokenBucketRateLimiter for rate limiting

    Example:
        client = SubgraphClient()

        # Query Uniswap V3 subgraph on Ethereum
        result = await client.query(
            subgraph_id="5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
            query="{ pools(first: 10) { id token0 { symbol } token1 { symbol } } }",
        )

        # Close when done
        await client.close()
    """

    def __init__(
        self,
        config: SubgraphClientConfig | None = None,
        rate_limiter: TokenBucketRateLimiter | None = None,
    ) -> None:
        """Initialize the SubgraphClient.

        Args:
            config: Client configuration. If None, uses defaults with
                    THEGRAPH_API_KEY from environment.
            rate_limiter: Optional rate limiter. If None, creates one
                          based on config.requests_per_minute.
        """
        self._config = config or SubgraphClientConfig()

        # Create or use provided rate limiter
        if rate_limiter is not None:
            self._rate_limiter = rate_limiter
        else:
            self._rate_limiter = TokenBucketRateLimiter(
                requests_per_minute=self._config.requests_per_minute,
            )

        # HTTP session (lazy initialized for connection pooling)
        self._session: aiohttp.ClientSession | None = None

        # Query statistics
        self._stats = QueryStats()

        # Log initialization
        api_key_status = "provided" if self._config.api_key else "not provided"
        logger.debug(
            "Initialized SubgraphClient: rate_limit=%d req/min, timeout=%ds, max_retries=%d, api_key=%s",
            self._config.requests_per_minute,
            self._config.timeout_seconds,
            self._config.max_retries,
            api_key_status,
        )

    @property
    def config(self) -> SubgraphClientConfig:
        """Get the client configuration."""
        return self._config

    @property
    def rate_limiter(self) -> TokenBucketRateLimiter:
        """Get the rate limiter."""
        return self._rate_limiter

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session with connection pooling.

        Returns:
            aiohttp.ClientSession with configured timeout
        """
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._config.timeout_seconds)
            connector = aiohttp.TCPConnector(
                limit=10,  # Connection pool size
                limit_per_host=5,  # Max connections per host
            )
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
            )
        return self._session

    async def close(self) -> None:
        """Close the HTTP session and release resources."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None
        logger.debug("SubgraphClient session closed")

    def _build_url(self, subgraph_id: str) -> str:
        """Build the subgraph URL from the subgraph ID.

        Args:
            subgraph_id: The subgraph deployment ID

        Returns:
            Full URL for the subgraph endpoint
        """
        return f"{self._config.gateway_url}/{subgraph_id}"

    def _build_headers(self) -> dict[str, str]:
        """Build HTTP headers for the request.

        Returns:
            Dictionary of HTTP headers
        """
        headers = {"Content-Type": "application/json"}
        if self._config.api_key:
            headers["Authorization"] = f"Bearer {self._config.api_key}"
        return headers

    async def _execute_query(
        self,
        subgraph_id: str,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a single GraphQL query without retry logic.

        Args:
            subgraph_id: The subgraph deployment ID
            query: GraphQL query string
            variables: Optional query variables

        Returns:
            Query response data

        Raises:
            SubgraphRateLimitError: If rate limit is exceeded
            SubgraphQueryError: If query returns errors
            SubgraphConnectionError: If connection fails
        """
        session = await self._get_session()
        url = self._build_url(subgraph_id)
        headers = self._build_headers()

        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        logger.debug(
            "Executing subgraph query: subgraph_id=%s, query_length=%d",
            subgraph_id[:20] + "..." if len(subgraph_id) > 20 else subgraph_id,
            len(query),
        )

        try:
            async with session.post(url, json=payload, headers=headers) as response:
                # Handle rate limiting
                if response.status == 429:
                    self._stats.rate_limited_queries += 1
                    retry_after = response.headers.get("Retry-After")
                    retry_seconds = float(retry_after) if retry_after else None
                    logger.warning(
                        "Subgraph rate limit exceeded: subgraph_id=%s",
                        subgraph_id[:20] + "...",
                    )
                    raise SubgraphRateLimitError(
                        "Rate limit exceeded",
                        retry_after_seconds=retry_seconds,
                    )

                # Handle other HTTP errors
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(
                        "Subgraph request failed: status=%d, subgraph_id=%s, body=%s",
                        response.status,
                        subgraph_id[:20] + "...",
                        error_text[:500],
                    )
                    raise SubgraphQueryError(
                        f"HTTP {response.status}: {error_text}",
                        query=query,
                    )

                # Parse response
                data = await response.json()

                # Check for GraphQL errors
                if "errors" in data and data["errors"]:
                    error_msgs = [e.get("message", str(e)) for e in data["errors"]]
                    logger.error(
                        "Subgraph query returned errors: %s",
                        "; ".join(error_msgs),
                    )
                    raise SubgraphQueryError(
                        f"GraphQL errors: {'; '.join(error_msgs)}",
                        query=query,
                        errors=data["errors"],
                    )

                return data.get("data", {})

        except aiohttp.ClientError as e:
            logger.error(
                "Subgraph connection error: subgraph_id=%s, error=%s",
                subgraph_id[:20] + "...",
                str(e),
            )
            raise SubgraphConnectionError(f"Connection failed: {e}") from e

    async def query(
        self,
        subgraph_id: str,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL query with rate limiting and retry logic.

        This method:
        1. Acquires a rate limit token before making the request
        2. Executes the query
        3. Retries with exponential backoff on transient failures
        4. Tracks query statistics

        Args:
            subgraph_id: The subgraph deployment ID (from The Graph Explorer)
            query: GraphQL query string
            variables: Optional query variables

        Returns:
            Query response data (the "data" field from GraphQL response)

        Raises:
            SubgraphRateLimitError: If rate limit exceeded after retries
            SubgraphQueryError: If query fails after retries
            SubgraphConnectionError: If connection fails after retries

        Example:
            result = await client.query(
                subgraph_id="5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
                query='''
                    query GetPool($id: ID!) {
                        pool(id: $id) {
                            token0 { symbol }
                            token1 { symbol }
                        }
                    }
                ''',
                variables={"id": "0x..."},
            )
        """
        self._stats.total_queries += 1

        async def execute_with_rate_limit() -> dict[str, Any]:
            return await self._execute_query(subgraph_id, query, variables)

        try:
            # Use rate limiter's retry_with_backoff for automatic retries
            result = await self._rate_limiter.retry_with_backoff(
                execute_with_rate_limit,
                max_retries=self._config.max_retries,
                is_rate_limit_error=lambda e: isinstance(e, SubgraphRateLimitError),
            )
            self._stats.successful_queries += 1
            logger.info(
                "Subgraph query successful: subgraph_id=%s",
                subgraph_id[:20] + "...",
            )
            return result

        except SubgraphRateLimitError:
            self._stats.failed_queries += 1
            raise

        except SubgraphQueryError:
            self._stats.failed_queries += 1
            raise

        except SubgraphConnectionError:
            self._stats.failed_queries += 1
            raise

        except Exception as e:
            self._stats.failed_queries += 1
            self._stats.total_retry_attempts += 1
            logger.error(
                "Subgraph query failed after retries: subgraph_id=%s, error=%s",
                subgraph_id[:20] + "...",
                str(e),
            )
            raise SubgraphQueryError(
                f"Query failed after retries: {e}",
                query=query,
            ) from e

    async def query_with_pagination(
        self,
        subgraph_id: str,
        query: str,
        variables: dict[str, Any] | None = None,
        data_path: str = "",
        page_size: int = 1000,
        max_pages: int = 10,
    ) -> list[Any]:
        """Execute a paginated GraphQL query.

        This method handles pagination automatically by:
        1. Adding skip/first parameters to the query
        2. Fetching pages until no more data or max_pages reached
        3. Combining all results into a single list

        Note: The query must support `first` and `skip` parameters.

        Args:
            subgraph_id: The subgraph deployment ID
            query: GraphQL query string (must include $first and $skip vars)
            variables: Base query variables (first/skip will be added)
            data_path: Dot-separated path to the list in the response
                       (e.g., "poolDayDatas" or "pools.items")
            page_size: Number of items per page (default: 1000)
            max_pages: Maximum number of pages to fetch (default: 10)

        Returns:
            Combined list of all items from all pages

        Example:
            results = await client.query_with_pagination(
                subgraph_id="...",
                query='''
                    query GetPools($first: Int!, $skip: Int!) {
                        pools(first: $first, skip: $skip) { id }
                    }
                ''',
                data_path="pools",
                page_size=100,
            )
        """
        all_results: list[Any] = []
        variables = dict(variables) if variables else {}

        for page in range(max_pages):
            # Add pagination parameters
            variables["first"] = page_size
            variables["skip"] = page * page_size

            # Execute query
            data = await self.query(subgraph_id, query, variables)

            # Extract results using data_path
            result = data
            if data_path:
                for key in data_path.split("."):
                    result = result.get(key, []) if isinstance(result, dict) else []

            # Check if we got results
            if not isinstance(result, list) or len(result) == 0:
                logger.debug(
                    "Pagination complete: fetched %d items in %d pages",
                    len(all_results),
                    page + 1,
                )
                break

            all_results.extend(result)

            # If we got less than page_size, we're done
            if len(result) < page_size:
                logger.debug(
                    "Pagination complete (last page partial): fetched %d items in %d pages",
                    len(all_results),
                    page + 1,
                )
                break

        return all_results

    def get_stats(self) -> QueryStats:
        """Get query statistics.

        Returns:
            Copy of current query statistics
        """
        return QueryStats(
            total_queries=self._stats.total_queries,
            successful_queries=self._stats.successful_queries,
            failed_queries=self._stats.failed_queries,
            rate_limited_queries=self._stats.rate_limited_queries,
            total_retry_attempts=self._stats.total_retry_attempts,
            created_at=self._stats.created_at,
        )

    def reset_stats(self) -> None:
        """Reset query statistics to zero."""
        self._stats = QueryStats()

    async def __aenter__(self) -> "SubgraphClient":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit: close the session."""
        await self.close()


# =============================================================================
# Convenience Functions
# =============================================================================


def create_subgraph_client(
    requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE,
    api_key: str | None = None,
) -> SubgraphClient:
    """Create a SubgraphClient with common settings.

    Args:
        requests_per_minute: Rate limit for requests (default: 100)
        api_key: The Graph API key (defaults to THEGRAPH_API_KEY env var)

    Returns:
        Configured SubgraphClient instance

    Example:
        client = create_subgraph_client(requests_per_minute=50)
        async with client:
            result = await client.query(...)
    """
    config = SubgraphClientConfig(
        api_key=api_key,
        requests_per_minute=requests_per_minute,
    )
    return SubgraphClient(config=config)


__all__ = [
    "SubgraphClient",
    "SubgraphClientConfig",
    "SubgraphClientError",
    "SubgraphRateLimitError",
    "SubgraphQueryError",
    "SubgraphConnectionError",
    "QueryStats",
    "create_subgraph_client",
    "DEFAULT_REQUESTS_PER_MINUTE",
    "DEFAULT_TIMEOUT_SECONDS",
    "DEFAULT_MAX_RETRIES",
    "THEGRAPH_GATEWAY_URL",
]
