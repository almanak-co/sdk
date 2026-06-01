"""Gateway-side GraphQL client for The Graph subgraphs (VIB-4753 / POOL-5).

Ported from ``almanak.framework.backtesting.pnl.providers.subgraph_client``.
The framework copy stays where it is (a separate cleanup follows POOL-7);
this gateway copy lives under ``almanak/gateway/data/`` where outbound
egress is the *correct* layer (AGENTS.md "Gateway boundary").

Differences from the framework client:

* No ``backtest_config_from_env`` dependency — the API key is passed in by
  the dispatcher (which reads it from ``GatewaySettings.thegraph_api_key``).
  Strategy containers never see this module.
* No built-in ``TokenBucketRateLimiter`` — this client is deliberately
  rate-limiter-agnostic. The dispatcher acquires the gateway ``_TokenBucket``
  (and the monthly-budget breaker) BEFORE calling ``query`` / ``execute``,
  mirroring the ``pool_analytics_service`` pattern. Keeping rate-limit policy
  out of the transport keeps the budget breaker auditable in one place.
* The exception taxonomy (``SubgraphRateLimitError`` / ``SubgraphQueryError``
  / ``SubgraphConnectionError``) is preserved verbatim so the dispatcher can
  map each to the 3-state provider taxonomy.

API-key safety: ``_mask_api_key`` + an API-key-safe ``__repr__`` ensure the
bearer token never lands in a log line or a traceback repr. The
``Authorization`` header is built fresh per request and never logged.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import aiohttp

from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

#: Default HTTP timeout in seconds for a single subgraph POST.
DEFAULT_TIMEOUT_SECONDS = 30.0


# =============================================================================
# Exceptions (preserved taxonomy from the framework client)
# =============================================================================


class SubgraphClientError(Exception):
    """Base exception for ``GatewayGraphQLClient`` errors."""


class SubgraphRateLimitError(SubgraphClientError):
    """Raised when The Graph returns HTTP 429.

    The dispatcher maps this to a ``_ProviderError`` so the fallback chain
    continues to the next eligible provider (TheGraph being throttled must
    be observable, NOT a silent local skip — UAT card decision #6).
    """

    def __init__(self, message: str = "Rate limit exceeded", retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class SubgraphQueryError(SubgraphClientError):
    """Raised when a subgraph query returns a non-200 status or GraphQL errors."""

    def __init__(
        self,
        message: str,
        query: str | None = None,
        errors: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(message)
        self.query = query
        self.errors = errors or []


class SubgraphConnectionError(SubgraphClientError):
    """Raised when the connection to the subgraph endpoint fails."""


# =============================================================================
# API-key masking
# =============================================================================


def _mask_api_key(key: str | None) -> str:
    """Mask an API key for safe logging.

    Returns the first 4 and last 4 chars for keys long enough to make that
    safe, ``***`` for short keys, ``not_set`` for ``None``. NEVER returns the
    raw key — the masked form is the only representation allowed near a log
    line or a ``repr``.
    """
    if not key:
        return "not_set"
    if len(key) <= 8:
        return "***"
    return f"{key[:4]}...{key[-4:]}"


# =============================================================================
# GatewayGraphQLClient
# =============================================================================


@dataclass
class _GraphQLStats:
    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0
    rate_limited_queries: int = 0


class GatewayGraphQLClient:
    """Minimal async GraphQL client for The Graph subgraphs (gateway-side).

    The client owns its own ``aiohttp.ClientSession`` (lazy, pooled) with the
    gateway's SSL context. It does NOT rate-limit or retry — the dispatcher
    owns rate-limit policy and the monthly-budget breaker so that all quota
    accounting lives in one auditable place.

    The ``api_key`` is stored privately and only ever emitted in masked form.
    """

    def __init__(self, *, api_key: str | None = None, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> None:
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds
        self._session: aiohttp.ClientSession | None = None
        self._stats = _GraphQLStats()
        logger.debug(
            "Initialized GatewayGraphQLClient (timeout=%ss, api_key=%s)",
            timeout_seconds,
            _mask_api_key(api_key),
        )

    def __repr__(self) -> str:
        """API-key-safe repr. A traceback or debug dump never leaks the key."""
        return f"GatewayGraphQLClient(api_key={_mask_api_key(self._api_key)}, timeout_seconds={self._timeout_seconds})"

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(ssl=build_ssl_context(), limit=10, limit_per_host=5)
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._timeout_seconds),
                connector=connector,
            )
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None
        logger.debug("GatewayGraphQLClient session closed")

    def _build_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        return headers

    async def query(
        self,
        *,
        url: str,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a single GraphQL POST against ``url`` and return the ``data`` object.

        Raises:
            SubgraphRateLimitError: HTTP 429.
            SubgraphQueryError: non-200 status OR a GraphQL ``errors`` array.
            SubgraphConnectionError: transport-level failure (timeout, DNS, …).
        """
        self._stats.total_queries += 1
        session = await self._get_session()
        headers = self._build_headers()
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        try:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 429:
                    self._stats.rate_limited_queries += 1
                    self._stats.failed_queries += 1
                    retry_after = response.headers.get("Retry-After")
                    # Retry-After may be a non-numeric HTTP-date
                    # (e.g. "Wed, 21 Oct 2025 07:28:00 GMT"); don't let that
                    # raise ValueError and mask the clean SubgraphRateLimitError.
                    retry_seconds: float | None = None
                    if retry_after:
                        try:
                            retry_seconds = float(retry_after)
                        except ValueError:
                            retry_seconds = None
                    logger.warning("Subgraph rate limit (HTTP 429) for %s", url)
                    raise SubgraphRateLimitError("Rate limit exceeded", retry_after_seconds=retry_seconds)

                if response.status != 200:
                    error_text = await response.text()
                    self._stats.failed_queries += 1
                    logger.error("Subgraph HTTP %d for %s: %s", response.status, url, error_text[:500])
                    raise SubgraphQueryError(f"HTTP {response.status}: {error_text[:500]}", query=query)

                data = await response.json()
                if isinstance(data, dict) and data.get("errors"):
                    self._stats.failed_queries += 1
                    error_msgs = [e.get("message", str(e)) for e in data["errors"]]
                    logger.error("Subgraph GraphQL errors for %s: %s", url, "; ".join(error_msgs))
                    raise SubgraphQueryError(
                        f"GraphQL errors: {'; '.join(error_msgs)}",
                        query=query,
                        errors=data["errors"],
                    )

                self._stats.successful_queries += 1
                return data.get("data", {}) if isinstance(data, dict) else {}

        except aiohttp.ClientError as exc:
            self._stats.failed_queries += 1
            logger.error("Subgraph connection error for %s: %s", url, exc)
            raise SubgraphConnectionError(f"Connection failed: {exc}") from exc
        except ValueError as exc:
            # json.JSONDecodeError / UnicodeDecodeError from response.json() /
            # .text() on a 200-with-malformed-body is NOT an aiohttp.ClientError;
            # map it into the query-error taxonomy instead of letting it escape
            # query() as an unhandled exception (breaks the dispatcher's 3-state
            # provider mapping).
            self._stats.failed_queries += 1
            logger.error("Subgraph response parsing error for %s: %s", url, exc)
            raise SubgraphQueryError(f"Response parsing failed: {exc}", query=query) from exc


__all__ = [
    "GatewayGraphQLClient",
    "SubgraphClientError",
    "SubgraphConnectionError",
    "SubgraphQueryError",
    "SubgraphRateLimitError",
    "DEFAULT_TIMEOUT_SECONDS",
    "_mask_api_key",
]
