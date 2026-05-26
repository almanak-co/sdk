"""TheGraph integration for gateway.

Provides access to TheGraph subgraph queries through the gateway:
- Query subgraphs by deployment ID or name
- Support for variables in queries
- Caching with configurable TTL

The gateway can optionally restrict queries to allowlisted subgraphs.

The default subgraph allowlist is assembled lazily from the gateway
connector registry (VIB-4811 / VIB-4817):

``GATEWAY_REGISTRY.capability_providers(GatewaySubgraphCapability)`` —
each registered gateway connector publishes its own
``subgraph_endpoints()`` mapping and the dispatcher merges them.

VIB-4817 retired the ``_PENDING_SUBGRAPHS`` fallback dict; the two
Curve entries that previously lived there now ride on
``CurveGatewayConnector.subgraph_endpoints()``.

``DEFAULT_ALLOWED_SUBGRAPHS`` is a module-level proxy dict that builds
itself on first access — building it eagerly at import time would
trigger a circular import between this module and
``almanak.gateway.services`` (which imports ``TheGraphIntegration``
via ``integration_service``). The dict is built once and cached.

Collisions (two connectors publishing the same alias with diverging
URLs) raise ``RuntimeError`` at first access — a silent overwrite
would make subgraph identity ambiguous and is a registry contract
violation.

Strategy-side code MUST NOT import this module.
"""

import logging
from collections.abc import Iterator
from typing import Any

from almanak.gateway.integrations.base import BaseIntegration, IntegrationError

logger = logging.getLogger(__name__)


def _build_default_allowed_subgraphs() -> dict[str, str]:
    """Assemble the default allowlist from the gateway-connector registry.

    Iterates ``GATEWAY_REGISTRY.capability_providers(GatewaySubgraphCapability)``
    and merges each connector's ``subgraph_endpoints()`` mapping into a
    single dict. Collisions on alias with diverging URLs raise — silent
    overwrite would make subgraph identity ambiguous.

    Imports are local so this module can be imported without immediately
    triggering ``almanak.connectors._gateway_registry`` loading — that
    chain pulls in every concrete gateway connector module, which in
    turn pulls in ``almanak.gateway.services``, which pulls in this
    module again (circular).
    """
    from almanak.connectors._base.gateway_capabilities import (
        GatewaySubgraphCapability,
    )
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    merged: dict[str, str] = {}
    # mypy: ``@runtime_checkable`` Protocol is the registry contract;
    # see ``pool_history_service._derive_pool_history_tables``.
    for connector in GATEWAY_REGISTRY.capability_providers(GatewaySubgraphCapability):  # type: ignore[type-abstract]
        for alias, url in connector.subgraph_endpoints().items():
            existing = merged.get(alias)
            if existing is not None and existing != url:
                raise RuntimeError(
                    f"Subgraph alias collision for {alias!r}: "
                    f"already registered as {existing!r}, refusing to "
                    f"overwrite with {url!r} from "
                    f"{type(connector).__qualname__}"
                )
            merged[alias] = url
    return merged


class _LazyAllowedSubgraphs(dict[str, str]):
    """A ``dict[str, str]`` that builds its contents from the registry on first access.

    Eager construction would trigger a circular import (see module
    docstring). Building lazily lets ``TheGraphIntegration`` import this
    module safely; the dict is fully populated by the time the gateway's
    boot sequence asks for an allowlist.

    Treating the proxy as a plain ``dict`` (.keys() / .items() / membership)
    triggers the build; mutation is allowed and matches the historical
    ``DEFAULT_ALLOWED_SUBGRAPHS.copy()`` behaviour.
    """

    __slots__ = ("_built",)

    def __init__(self) -> None:
        super().__init__()
        self._built = False

    def _ensure_built(self) -> None:
        if not self._built:
            super().update(_build_default_allowed_subgraphs())
            self._built = True

    def __contains__(self, key: object) -> bool:
        self._ensure_built()
        return super().__contains__(key)

    def __iter__(self) -> Iterator[str]:
        self._ensure_built()
        return super().__iter__()

    def __len__(self) -> int:
        self._ensure_built()
        return super().__len__()

    def __getitem__(self, key: str) -> str:
        self._ensure_built()
        return super().__getitem__(key)

    def __eq__(self, other: object) -> bool:
        self._ensure_built()
        return super().__eq__(other)

    def __ne__(self, other: object) -> bool:
        self._ensure_built()
        return super().__ne__(other)

    def __hash__(self) -> int:  # type: ignore[override]
        # dict is unhashable; explicit override silences ruff's
        # "defined __eq__ without __hash__" lint without changing
        # behaviour.
        raise TypeError("unhashable type: '_LazyAllowedSubgraphs'")

    def keys(self):
        self._ensure_built()
        return super().keys()

    def values(self):
        self._ensure_built()
        return super().values()

    def items(self):
        self._ensure_built()
        return super().items()

    def get(self, key, default=None):
        self._ensure_built()
        return super().get(key, default)

    def copy(self) -> dict[str, str]:
        self._ensure_built()
        return dict(self)


# Default allowlisted subgraphs (can be extended via configuration).
# Built lazily on first access from the connector registry + pending rows.
DEFAULT_ALLOWED_SUBGRAPHS: dict[str, str] = _LazyAllowedSubgraphs()


class TheGraphIntegration(BaseIntegration):
    """TheGraph subgraph query integration.

    Provides access to TheGraph subgraphs for on-chain data queries.
    Supports both hosted service and decentralized network.

    Rate limits:
    - Free hosted service: 1000 queries per day
    - Decentralized network: Based on GRT staking

    Supported operations:
    - query: Execute a GraphQL query on a subgraph

    Example:
        integration = TheGraphIntegration()
        result = await integration.query(
            subgraph_id="uniswap-v3-arbitrum",
            query="{ pools(first: 10) { id token0 { symbol } token1 { symbol } } }",
        )
    """

    name = "thegraph"
    rate_limit_requests = 100  # Conservative rate limit
    default_cache_ttl = 30  # 30 second cache for query results

    def __init__(
        self,
        api_key: str | None = None,
        allowed_subgraphs: dict[str, str] | None = None,
        request_timeout: float = 30.0,
    ):
        """Initialize TheGraph integration.

        Args:
            api_key: Optional TheGraph API key for decentralized network
            allowed_subgraphs: Optional dict mapping subgraph names to URLs.
                If None, uses default allowlist.
            request_timeout: HTTP request timeout in seconds
        """
        super().__init__(
            api_key=api_key,
            base_url="",  # URLs are per-subgraph
            request_timeout=request_timeout,
        )

        # Set up allowed subgraphs
        self._allowed_subgraphs = allowed_subgraphs or DEFAULT_ALLOWED_SUBGRAPHS.copy()

        logger.info(
            "Initialized TheGraph integration with %d allowed subgraphs",
            len(self._allowed_subgraphs),
        )

    def _get_headers(self) -> dict[str, str]:
        """Get headers for TheGraph API requests."""
        headers = super()._get_headers()
        headers["Content-Type"] = "application/json"
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        return headers

    def get_subgraph_url(self, subgraph_id: str) -> str | None:
        """Get URL for a subgraph.

        Args:
            subgraph_id: Subgraph ID or name

        Returns:
            Subgraph URL or None if not in allowlist
        """
        # First check if it's a known alias
        if subgraph_id in self._allowed_subgraphs:
            return self._allowed_subgraphs[subgraph_id]

        # Check if it's a direct URL (for subgraph IDs)
        if subgraph_id.startswith("Qm") or subgraph_id.startswith("0x"):
            # This is a deployment ID - construct URL
            if self._api_key:
                return f"https://gateway.thegraph.com/api/{self._api_key}/subgraphs/id/{subgraph_id}"
            return None  # Deployment IDs require API key

        return None

    def add_allowed_subgraph(self, name: str, url: str) -> None:
        """Add a subgraph to the allowlist.

        Args:
            name: Subgraph name/alias
            url: Subgraph URL
        """
        self._allowed_subgraphs[name] = url
        logger.info("Added subgraph to allowlist: %s", name)

    def list_allowed_subgraphs(self) -> list[str]:
        """List allowed subgraph names.

        Returns:
            List of allowed subgraph names
        """
        return list(self._allowed_subgraphs.keys())

    async def health_check(self) -> bool:
        """Check if TheGraph is healthy.

        Tries to query a known subgraph.

        Returns:
            True if healthy, False otherwise
        """
        try:
            # Try a simple query on Uniswap V3
            result = await self.query(
                subgraph_id="uniswap-v3-ethereum",
                query="{ _meta { block { number } } }",
            )
            return "data" in result or result.get("success", False)
        except Exception as e:
            logger.warning("TheGraph health check failed: %s", e)
            return False

    async def query(
        self,
        subgraph_id: str,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL query on a subgraph.

        Args:
            subgraph_id: Subgraph ID or name from allowlist
            query: GraphQL query string
            variables: Optional query variables

        Returns:
            Query result with "data" and optional "errors" fields

        Raises:
            IntegrationError: On API errors or if subgraph not allowed
        """
        # Get subgraph URL
        url = self.get_subgraph_url(subgraph_id)
        if url is None:
            raise IntegrationError(
                self.name,
                f"Subgraph '{subgraph_id}' is not in allowlist. Allowed: {', '.join(self.list_allowed_subgraphs())}",
                code="SUBGRAPH_NOT_ALLOWED",
            )

        # Build cache key from query (simple hash)
        import hashlib

        query_hash = hashlib.md5(f"{subgraph_id}:{query}:{variables}".encode()).hexdigest()[:16]
        cache_key = f"query:{query_hash}"

        # Check cache
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        # Build request payload
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        # Make request (override base URL for this request)
        import aiohttp

        session = await self._get_session()
        headers = self._get_headers()

        try:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 429:
                    self._metrics.rate_limited_requests += 1
                    from almanak.gateway.integrations.base import IntegrationRateLimitError

                    raise IntegrationRateLimitError(self.name, 60.0)

                if response.status >= 400:
                    error_text = await response.text()
                    self._metrics.failed_requests += 1
                    raise IntegrationError(
                        self.name,
                        f"HTTP {response.status}: {error_text}",
                        code=f"HTTP_{response.status}",
                    )

                data = await response.json()
                self._metrics.successful_requests += 1

                # Check for GraphQL errors
                if "errors" in data:
                    # Return both data and errors (GraphQL can have partial results)
                    result = {
                        "data": data.get("data"),
                        "errors": data.get("errors"),
                        "success": data.get("data") is not None,
                    }
                else:
                    result = {
                        "data": data.get("data"),
                        "success": True,
                    }

                # Update cache
                self._update_cache(cache_key, result)

                return result

        except aiohttp.ClientError as e:
            self._metrics.failed_requests += 1
            raise IntegrationError(
                self.name,
                str(e),
                code="NETWORK_ERROR",
            ) from e
