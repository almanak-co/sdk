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
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import aiohttp

from almanak.config.backtest import backtest_config_from_env

from ...exceptions import DataSourceUnavailableError
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

# The Graph rejects ``skip`` values above 5000, so skip-based pagination tops
# out at 6 pages of 1000 rows. Windows that need more rows must use cursor
# pagination (VIB-5089).
THE_GRAPH_MAX_SKIP = 5000


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
# Pagination Core
# =============================================================================


def _extract_data_path(data: Any, data_path: str) -> list[Any]:
    """Extract the list at a dot-separated path from a query response.

    Args:
        data: The query response data (the GraphQL "data" payload)
        data_path: Dot-separated path to the list (e.g. "pool.snapshots").
                   Empty string means the response itself is the list.

    Returns:
        The list at the path, or an empty list when the path is missing or
        does not resolve to a list.
    """
    result = data
    if data_path:
        for key in data_path.split("."):
            result = result.get(key, []) if isinstance(result, dict) else []
    return result if isinstance(result, list) else []


def _window_too_large_error(context: str, message: str, remediation: str) -> DataSourceUnavailableError:
    """Build the loud-failure error for pagination overflow.

    Pagination must never silently truncate (VIB-5089): when a window cannot
    be fully fetched, the caller gets a
    :class:`~almanak.framework.backtesting.exceptions.DataSourceUnavailableError`
    with concrete remediation instead of a partial series.
    """
    return DataSourceUnavailableError(
        data_type="subgraph",
        identifier=context,
        message=message,
        remediation=remediation,
    )


async def paginate_subgraph_query(
    execute: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]],
    query: str,
    variables: dict[str, Any] | None = None,
    *,
    data_path: str = "",
    page_size: int = 1000,
    max_pages: int = 10,
    cursor_field: str | None = None,
    cursor_variable: str | None = None,
    context: str = "subgraph",
) -> list[Any]:
    """Paginate a GraphQL query through an arbitrary execute callable.

    This is the single pagination implementation shared by
    :meth:`SubgraphClient.query_with_pagination` and providers that own their
    own HTTP path (e.g. ``subgraph.py``'s :class:`SubgraphVolumeProvider`).

    Two modes exist:

    **Cursor mode** (``cursor_field`` set) - preferred. The query must:

    - select both ``id`` and ``cursor_field`` on every item,
    - order ascending by ``cursor_field`` (``orderBy: <cursor_field>,
      orderDirection: asc``),
    - bind its lower-bound ``where`` filter to ``$<cursor_variable>`` with a
      ``_gte`` comparison (e.g. ``timestamp_gte: $startTimestamp``), and
    - take ``first: $first``.

    After each full page the lower bound advances to the last item's cursor
    value. Because the bound is inclusive (``_gte``), items sharing the
    boundary value are re-fetched and deduplicated by ``id`` - no boundary
    duplicates and no boundary gaps, as long as fewer than ``page_size``
    items share a single cursor value (otherwise pagination stalls and fails
    loudly).

    **Skip mode** (``cursor_field`` is None) - legacy. The query must take
    ``first: $first, skip: $skip``. The Graph caps ``skip`` at
    :data:`THE_GRAPH_MAX_SKIP` (5000); needing to page past the cap fails
    loudly.

    Both modes fail loudly (``DataSourceUnavailableError``) instead of
    silently truncating when more data remains after ``max_pages``.

    Args:
        execute: Async callable ``(query, variables) -> response data dict``.
        query: GraphQL query string.
        variables: Base query variables. Mutated copies are passed to
                   ``execute``; the caller's dict is never mutated.
        data_path: Dot-separated path to the result list in the response.
        page_size: Items per page (default: 1000, The Graph's max ``first``).
        max_pages: Safety valve on total pages fetched.
        cursor_field: Response field to cursor on (e.g. "timestamp", "date").
        cursor_variable: Query variable holding the lower bound (e.g.
                         "startTimestamp"). Required in cursor mode; its
                         initial value must be present in ``variables``.
        context: Identifier used in error messages (subgraph id / provider).

    Returns:
        Combined, deduplicated list of all items across pages, in response
        order.

    Raises:
        DataSourceUnavailableError: When the window cannot be fetched without
            truncation (skip cap, max_pages exhaustion, or a stalled cursor).
        ValueError: When cursor mode is misconfigured (missing
            ``cursor_variable`` or initial cursor value).
        SubgraphQueryError: When cursor mode encounters an item without the
            ``id`` or cursor field it needs to make progress.
    """
    variables = dict(variables) if variables else {}

    if cursor_field is not None:
        return await _paginate_with_cursor(
            execute,
            query,
            variables,
            data_path=data_path,
            page_size=page_size,
            max_pages=max_pages,
            cursor_field=cursor_field,
            cursor_variable=cursor_variable,
            context=context,
        )
    return await _paginate_with_skip(
        execute,
        query,
        variables,
        data_path=data_path,
        page_size=page_size,
        max_pages=max_pages,
        context=context,
    )


async def _paginate_with_cursor(
    execute: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]],
    query: str,
    variables: dict[str, Any],
    *,
    data_path: str,
    page_size: int,
    max_pages: int,
    cursor_field: str,
    cursor_variable: str | None,
    context: str,
) -> list[Any]:
    """Cursor-mode pagination: inclusive lower bound + dedup by id."""
    if cursor_variable is None:
        raise ValueError("cursor_variable is required when cursor_field is set")
    if cursor_variable not in variables:
        raise ValueError(f"variables must contain the initial cursor value for {cursor_variable!r}")

    # GraphQL Int variables are JSON numbers; BigInt variables are JSON
    # strings. Preserve whichever shape the caller used for the lower bound.
    cursor_is_str = isinstance(variables[cursor_variable], str)

    all_results: list[Any] = []
    seen_ids: set[str] = set()

    for page in range(max_pages):
        variables["first"] = page_size
        data = await execute(query, variables)
        page_items = _extract_data_path(data, data_path)

        new_items: list[Any] = []
        for item in page_items:
            item_id = item.get("id") if isinstance(item, dict) else None
            if item_id is None:
                raise SubgraphQueryError(
                    f"Cursor pagination requires every item to select 'id' (context={context})",
                    query=query,
                )
            if item_id in seen_ids:
                continue
            seen_ids.add(item_id)
            new_items.append(item)
        all_results.extend(new_items)

        if len(page_items) < page_size:
            logger.debug(
                "Cursor pagination complete: fetched %d items in %d pages (context=%s)",
                len(all_results),
                page + 1,
                context,
            )
            return all_results

        if not new_items:
            # A full page of already-seen items: more than page_size rows
            # share one cursor value, so the inclusive bound cannot advance.
            raise _window_too_large_error(
                context,
                message=(
                    f"Cursor pagination stalled after {len(all_results)} rows: {page_size} or more "
                    f"items share {cursor_field}={page_items[-1].get(cursor_field)!r} so the "
                    f"inclusive lower bound cannot advance"
                ),
                remediation=(
                    "Raise page_size above the number of rows sharing a single "
                    f"{cursor_field} value, or cursor on a finer-grained field"
                ),
            )

        raw_cursor = page_items[-1].get(cursor_field)
        if raw_cursor is None:
            raise SubgraphQueryError(
                f"Cursor pagination requires every item to select {cursor_field!r} (context={context})",
                query=query,
            )
        variables[cursor_variable] = str(raw_cursor) if cursor_is_str else int(raw_cursor)

    raise _window_too_large_error(
        context,
        message=(
            f"Query window too large: fetched {len(all_results)} rows but exhausted "
            f"max_pages={max_pages} (page_size={page_size}) with more data remaining"
        ),
        remediation="Narrow the query window or raise max_pages",
    )


async def _paginate_with_skip(
    execute: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]],
    query: str,
    variables: dict[str, Any],
    *,
    data_path: str,
    page_size: int,
    max_pages: int,
    context: str,
) -> list[Any]:
    """Skip-mode pagination: legacy first/skip with a loud cap failure."""
    all_results: list[Any] = []

    for page in range(max_pages):
        skip = page * page_size
        if skip > THE_GRAPH_MAX_SKIP:
            raise _window_too_large_error(
                context,
                message=(
                    f"Query window too large for skip-based pagination: fetched "
                    f"{len(all_results)} rows but The Graph caps skip at {THE_GRAPH_MAX_SKIP} "
                    f"and more data remains"
                ),
                remediation=(
                    "Narrow the query window, or switch this query to cursor pagination "
                    "(cursor_field on a timestamp-ordered field)"
                ),
            )

        variables["first"] = page_size
        variables["skip"] = skip
        data = await execute(query, variables)
        page_items = _extract_data_path(data, data_path)

        if len(page_items) == 0:
            logger.debug(
                "Skip pagination complete: fetched %d items in %d pages (context=%s)",
                len(all_results),
                page + 1,
                context,
            )
            return all_results

        all_results.extend(page_items)

        if len(page_items) < page_size:
            logger.debug(
                "Skip pagination complete (last page partial): fetched %d items in %d pages (context=%s)",
                len(all_results),
                page + 1,
                context,
            )
            return all_results

    raise _window_too_large_error(
        context,
        message=(
            f"Query window too large: fetched {len(all_results)} rows but exhausted "
            f"max_pages={max_pages} (page_size={page_size}) with more data remaining"
        ),
        remediation="Narrow the query window or raise max_pages",
    )


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
        """Load API key from typed backtest config if not provided.

        Phase 5c: env reading is centralised in
        :func:`almanak.config.backtest.backtest_config_from_env`. The
        legacy ``os.environ.get("THEGRAPH_API_KEY")`` lookup is gone;
        the config factory reads the same env var and exposes it as
        ``BacktestConfig.thegraph_api_key`` (``None`` when unset, the
        same shape this dataclass expects).
        """
        if self.api_key is None:
            self.api_key = backtest_config_from_env().thegraph_api_key

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
        *,
        cursor_field: str | None = None,
        cursor_variable: str | None = None,
    ) -> list[Any]:
        """Execute a paginated GraphQL query.

        Delegates to :func:`paginate_subgraph_query`; see it for the full
        contract of both modes.

        **Cursor mode** (``cursor_field`` set) - preferred. The query must
        order ascending by ``cursor_field``, select ``id`` and
        ``cursor_field`` on every item, take ``first: $first``, and bind its
        inclusive lower bound to ``$<cursor_variable>`` (a ``_gte`` filter).
        The lower bound advances past each full page; boundary items are
        deduplicated by ``id``. This is the only mode that can fetch more
        than ~6000 rows - The Graph caps ``skip`` at
        :data:`THE_GRAPH_MAX_SKIP`.

        **Skip mode** (default) - legacy. The query must take
        ``first: $first, skip: $skip``. Fails loudly at the skip cap.

        Both modes raise
        :class:`~almanak.framework.backtesting.exceptions.DataSourceUnavailableError`
        instead of silently truncating when more data remains (VIB-5089).

        Args:
            subgraph_id: The subgraph deployment ID
            query: GraphQL query string (must declare $first, plus $skip in
                   skip mode or the cursor variable in cursor mode)
            variables: Base query variables (pagination vars will be added;
                       in cursor mode must contain the initial lower bound)
            data_path: Dot-separated path to the list in the response
                       (e.g., "poolDayDatas" or "pools.items")
            page_size: Number of items per page (default: 1000)
            max_pages: Maximum number of pages to fetch (default: 10)
            cursor_field: Response field to cursor on (e.g. "timestamp")
            cursor_variable: Query variable holding the inclusive lower bound
                             (e.g. "startTimestamp")

        Returns:
            Combined list of all items from all pages

        Example:
            results = await client.query_with_pagination(
                subgraph_id="...",
                query='''
                    query GetSnapshots($first: Int!, $startTimestamp: Int!, $endTimestamp: Int!) {
                        snapshots(
                            first: $first
                            where: { timestamp_gte: $startTimestamp, timestamp_lte: $endTimestamp }
                            orderBy: timestamp
                            orderDirection: asc
                        ) { id timestamp }
                    }
                ''',
                variables={"startTimestamp": 1704067200, "endTimestamp": 1735689600},
                data_path="snapshots",
                cursor_field="timestamp",
                cursor_variable="startTimestamp",
            )
        """

        async def execute(q: str, v: dict[str, Any]) -> dict[str, Any]:
            return await self.query(subgraph_id, q, v)

        return await paginate_subgraph_query(
            execute,
            query,
            variables,
            data_path=data_path,
            page_size=page_size,
            max_pages=max_pages,
            cursor_field=cursor_field,
            cursor_variable=cursor_variable,
            context=subgraph_id,
        )

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
    "paginate_subgraph_query",
    "DEFAULT_REQUESTS_PER_MINUTE",
    "DEFAULT_TIMEOUT_SECONDS",
    "DEFAULT_MAX_RETRIES",
    "THEGRAPH_GATEWAY_URL",
    "THE_GRAPH_MAX_SKIP",
]
