"""TWAP (Time-Weighted Average Price) data provider for Uniswap V3.

**VIB-4859 / W7**: This module is now a thin gRPC client of the gateway's
``RateHistoryService.GetDexTwap`` RPC. All Web3 / archive-RPC egress has
moved into the gateway sidecar via :class:`GatewayDexTwapCapability`
implementations on the corresponding DEX connectors (Uniswap V3,
PancakeSwap V3, SushiSwap V3). The strategy container holds no pool
tables, no observe() selectors, and no ``Web3(HTTPProvider(...))``
construction.

The :class:`TWAPDataProvider` public API + :class:`TWAPResult` /
:class:`TWAPObservation` / :class:`CachedTWAP` dataclasses are
preserved verbatim for back-compat. The pool tables
(``UNISWAP_V3_POOLS``, ``TOKEN_TO_POOL``) and the selectors
(``OBSERVE_SELECTOR``, etc.) are kept here so callers that imported
them by name (e.g. for ad-hoc address lookups) don't break — they are
no longer load-bearing for the W7 dispatch, which queries the gateway
by ``(dex, chain, pool_address)`` triple.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.data.interfaces import DataSourceUnavailable

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def _twap_get_connected_gateway_client() -> tuple[Any, Any]:
    """Return ``(client, gateway_pb2)`` with the client connected, or raise.

    Centralises the import + connect dance so the gateway-backed fetchers
    don't each carry the boilerplate.
    """
    try:
        from almanak.framework.gateway_client import get_gateway_client
        from almanak.gateway.proto import gateway_pb2
    except ImportError as exc:
        raise DataSourceUnavailable(
            source="gateway",
            reason=f"Gateway client unavailable: {exc}",
        ) from exc

    client = get_gateway_client()
    if not client.is_connected:
        try:
            client.connect()
        except Exception as exc:
            raise DataSourceUnavailable(
                source="gateway",
                reason=f"Gateway connect failed: {exc}",
            ) from exc
    return client, gateway_pb2


_TWAP_STABLE_SYMBOLS: frozenset[str] = frozenset({"USDC", "USDT", "DAI", "FRAX", "LUSD", "BUSD", "USD"})


def _twap_is_stable_symbol(symbol: str) -> bool:
    """Return True for stablecoin symbols that resolve to a flat $1 TWAP."""
    return symbol in _TWAP_STABLE_SYMBOLS


def _build_flat_ohlcv_bar(timestamp: datetime, price: Decimal) -> Any:
    """Build a flat OHLCV bar where ``O=H=L=C=price``.

    Tries the typed ``OHLCV`` dataclass first; falls back to a plain
    dict when the backtesting module isn't importable.
    """
    try:
        from almanak.framework.backtesting.pnl.data_provider import OHLCV as _OHLCV
    except ImportError:
        return {
            "timestamp": timestamp,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": None,
        }
    return _OHLCV(
        timestamp=timestamp,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=None,
    )


# =============================================================================
# Constants — preserved for back-compat with callers that imported them
# =============================================================================
#
# Pre-W7 callers imported these dicts / selectors directly for ad-hoc
# Uniswap V3 pool lookups. The gateway-side capability now owns the
# real egress; these tables stay here so the import surface is stable.

OBSERVE_SELECTOR = "883bdbfd"
SLOT0_SELECTOR = "3850c7bd"

# Default TWAP window (30 minutes, matches Uniswap V3 oracle convention).
DEFAULT_TWAP_WINDOW_SECONDS = 1800

# Archive-RPC env-var pattern preserved for back-compat (no longer used
# by this module — the gateway resolves RPC URLs server-side).
ARCHIVE_RPC_URL_ENV_PATTERN = "ARCHIVE_RPC_URL_{CHAIN}"
ARCHIVE_RPC_CHAINS = ("ethereum", "arbitrum", "base", "optimism", "polygon")

# The per-chain pool address tables and the token -> pool-key resolution are
# connector-owned reference data (``almanak/connectors/uniswap_v3/
# backtest_pools.py``), declared via ``DexVolumeDecl.twap_reference_pools``
# and merged through ``DexVolumeRegistry.twap_reference_pools()`` — VIB-4851
# Phase D. The legacy module names (``UNISWAP_V3_POOLS``, ``TOKEN_TO_POOL``)
# stay importable via ``__getattr__`` below.


def _reference_pools() -> dict[str, dict]:
    """Connector-declared TWAP reference tables (lazy; never at import)."""
    from almanak.connectors._strategy_base.dex_volume_registry import DexVolumeRegistry

    return DexVolumeRegistry.twap_reference_pools()


_PER_CHAIN_TABLE_NAMES = {
    "ETHEREUM_POOLS": "ethereum",
    "ARBITRUM_POOLS": "arbitrum",
    "BASE_POOLS": "base",
    "OPTIMISM_POOLS": "optimism",
    "POLYGON_POOLS": "polygon",
}


def __getattr__(name: str):  # noqa: ANN202 - PEP 562 lazy back-compat hook
    """Serve the legacy table names without import-time discovery."""
    if name == "UNISWAP_V3_POOLS":
        return _reference_pools()["pools"]
    if name == "TOKEN_TO_POOL":
        return _reference_pools()["token_to_pool"]
    chain = _PER_CHAIN_TABLE_NAMES.get(name)
    if chain is not None:
        return _reference_pools()["pools"].get(chain, {})
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# =============================================================================
# Exceptions
# =============================================================================


class TWAPInsufficientHistoryError(Exception):
    """Raised when the pool lacks sufficient observation history for the window."""

    def __init__(
        self,
        token: str,
        pool_address: str,
        window_seconds: int,
    ) -> None:
        self.token = token
        self.pool_address = pool_address
        self.window_seconds = window_seconds
        super().__init__(f"Pool {pool_address} has insufficient TWAP history for {token} (needed: {window_seconds}s)")


class TWAPPoolNotFoundError(Exception):
    """Raised when no Uniswap V3 pool is available for the token on the chain."""

    def __init__(self, token: str, chain: str) -> None:
        self.token = token
        self.chain = chain
        super().__init__(f"No TWAP pool available for {token!r} on {chain!r}")


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class TWAPObservation:
    """Single observation result from a Uniswap V3 pool's ``observe()`` call."""

    tick_cumulative: int
    seconds_per_liquidity_cumulative_x128: int
    timestamp: datetime | None = None


@dataclass
class TWAPResult:
    """Result of a TWAP calculation.

    ``tick_observation_count`` is the Uniswap-V3-style sanity-check counter
    (number of observations the pool's ring-buffer held over the window),
    mirroring ``DexTwapPoint.tick_observation_count`` in the gateway proto.
    It is NOT the computed arithmetic-mean tick — the gateway TWAP capability
    returns only the human-readable ``price`` plus this counter, so no
    computed-tick value is available on the framework side.
    """

    price: Decimal
    tick_observation_count: int
    observation_window_seconds: int
    pool_address: str
    token0_is_base: bool


@dataclass
class CachedTWAP:
    """A single cached TWAP entry with TTL tracking."""

    price: Decimal
    result: TWAPResult
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    ttl_seconds: int = 60

    @property
    def is_expired(self) -> bool:
        return (datetime.now(UTC) - self.fetched_at).total_seconds() > self.ttl_seconds

    @property
    def age_seconds(self) -> float:
        return (datetime.now(UTC) - self.fetched_at).total_seconds()


# Re-export HistoricalDataConfig / MarketState / OHLCV from the
# backtesting data_provider module so strategy code that imports them
# via this module keeps working post-W7.
from almanak.framework.backtesting.pnl.data_provider import (  # noqa: E402,F401
    OHLCV,
    HistoricalDataConfig,
    MarketState,
)

# =============================================================================
# TWAPDataProvider (thin gRPC client — VIB-4859 / W7)
# =============================================================================


class TWAPDataProvider:
    """Uniswap V3 TWAP data provider — gRPC client of ``RateHistoryService``.

    All Web3 / archive-RPC egress lives gateway-side via
    :class:`GatewayDexTwapCapability` on the Uniswap V3 connector. The
    framework consumer is a thin client that resolves
    ``(token, chain) → pool_address`` from the preserved ``TOKEN_TO_POOL``
    /``UNISWAP_V3_POOLS`` tables and issues a ``GetDexTwap`` RPC.

    The public API (``get_latest_price``, ``get_price``, ``get_ohlcv``,
    ``iterate``, ``get_pool_address``, ``get_pool_key``) and dataclass
    shapes are preserved for back-compat. When the gateway is
    unreachable, the provider raises (no silent zero-fill — matches the
    "no silent zeros" rule per VIB-4859 decision 4).

    Args:
        chain: Blockchain network identifier (ethereum, arbitrum, base, etc.)
        rpc_url: Ignored (kept for back-compat). Egress lives gateway-side.
        observation_window_seconds: TWAP observation window (default: 1800s = 30 min)
        cache_ttl_seconds: TTL for cached TWAP data (default: 60s)
        priority: Provider priority for registry selection (lower = higher).
    """

    DEFAULT_PRIORITY = 20

    def __init__(
        self,
        chain: str = "arbitrum",
        rpc_url: str = "",
        observation_window_seconds: int | None = None,
        cache_ttl_seconds: int = 60,
        priority: int | None = None,
    ) -> None:
        self._chain = chain.lower()
        pools_by_chain = _reference_pools()["pools"]
        if self._chain not in pools_by_chain:
            available = ", ".join(pools_by_chain.keys())
            raise ValueError(f"Unsupported chain {chain!r}. Available: {available}")
        self._observation_window_seconds = (
            observation_window_seconds if observation_window_seconds is not None else DEFAULT_TWAP_WINDOW_SECONDS
        )
        self._cache_ttl_seconds = cache_ttl_seconds
        self._priority = priority if priority is not None else self.DEFAULT_PRIORITY
        # Preserved for back-compat with pre-W7 callers — the gateway
        # client ignores it (RPC egress lives gateway-side now).
        self._rpc_url = rpc_url
        self._pools = pools_by_chain[self._chain]
        self._cache: dict[str, CachedTWAP] = {}

    @property
    def chain(self) -> str:
        return self._chain

    @property
    def priority(self) -> int:
        return self._priority

    @property
    def observation_window_seconds(self) -> int:
        return self._observation_window_seconds

    @property
    def cache_ttl_seconds(self) -> int:
        return self._cache_ttl_seconds

    def get_pool_address(self, token: str) -> str | None:
        """Resolve a token symbol to a Uniswap V3 pool address on this chain."""
        token_upper = token.upper()
        chain_pools = _reference_pools()["token_to_pool"].get(token_upper, {})
        pool_key = chain_pools.get(self._chain)
        if pool_key is None:
            return None
        return self._pools.get(pool_key)

    def get_pool_key(self, token: str) -> str | None:
        """Resolve a token symbol to the pool key (e.g. "WETH/USDC-500")."""
        token_upper = token.upper()
        chain_pools = _reference_pools()["token_to_pool"].get(token_upper, {})
        return chain_pools.get(self._chain)

    def supported_tokens(self) -> list[str]:
        """List of tokens supported on the configured chain."""
        token_to_pool = _reference_pools()["token_to_pool"]
        return [token for token, chain_pools in token_to_pool.items() if self._chain in chain_pools]

    def _cached_price_if_fresh(self, token: str, token_upper: str) -> Decimal | None:
        """Return the cached TWAP price for ``token_upper`` if non-expired, else ``None``."""
        cached = self._cache.get(token_upper)
        if cached is None or cached.is_expired:
            return None
        logger.debug(
            "TWAP cache hit for %s: $%.4f (age: %.1fs)",
            token,
            float(cached.price),
            cached.age_seconds,
        )
        return cached.price

    async def _twap_price_with_two_hop_if_needed(
        self,
        token: str,
        pool_key: str,
        twap_price: Decimal,
    ) -> Decimal | None:
        """Apply the two-hop ETH conversion when the pool is WETH-paired.

        Returns ``None`` when the ETH price lookup fails (matches the
        pre-W7 fallback behaviour — caller surfaces ``None`` to the
        consumer instead of raising).
        """
        if "USDC" in pool_key.upper():
            return twap_price
        try:
            eth_price = await self._fetch_eth_usd_price()
        except DataSourceUnavailable:
            logger.warning("Could not get ETH price for %s two-hop pricing", token)
            return None
        return twap_price * eth_price

    def _resolve_pool_for_token(self, token_upper: str) -> tuple[str, str]:
        """Resolve ``(pool_address, pool_key)`` for ``token_upper``.

        Raises ``TWAPPoolNotFoundError`` when no pool is registered for the
        token on the configured chain.
        """
        pool_address = self.get_pool_address(token_upper)
        pool_key = self.get_pool_key(token_upper)
        if pool_address is None or pool_key is None:
            raise TWAPPoolNotFoundError(token_upper, self._chain)
        return pool_address, pool_key

    def _cache_twap_price(
        self,
        token_upper: str,
        *,
        price: Decimal,
        tick_observation_count: int,
        pool_address: str,
    ) -> None:
        """Stamp the latest TWAP for ``token_upper`` into the per-instance cache."""
        result = TWAPResult(
            price=price,
            tick_observation_count=tick_observation_count,
            observation_window_seconds=self._observation_window_seconds,
            pool_address=pool_address,
            token0_is_base=True,
        )
        self._cache[token_upper] = CachedTWAP(
            price=price,
            result=result,
            ttl_seconds=self._cache_ttl_seconds,
        )

    def _try_fast_path_price(
        self,
        token: str,
        token_upper: str,
        *,
        use_cache: bool,
    ) -> Decimal | None:
        """Return an immediate price for stables / fresh cache, else ``None``."""
        if _twap_is_stable_symbol(token_upper):
            return Decimal("1")
        if use_cache:
            return self._cached_price_if_fresh(token, token_upper)
        return None

    async def _fetch_twap_point_or_raise(
        self,
        pool_address: str,
        token_upper: str,
    ) -> dict[str, Any]:
        """Wrap ``_fetch_twap_via_gateway`` so ``DataSourceUnavailable`` becomes
        ``TWAPInsufficientHistoryError`` with the right attribution."""
        try:
            return await self._fetch_twap_via_gateway(pool_address)
        except DataSourceUnavailable as exc:
            raise TWAPInsufficientHistoryError(token_upper, pool_address, self._observation_window_seconds) from exc

    async def get_latest_price(
        self,
        token: str,
        use_cache: bool = True,
    ) -> Decimal | None:
        """Get the latest TWAP price for a token (via gateway).

        Stablecoins return ``Decimal("1")`` without a gateway round-trip.
        Tokens paired with USDC return the direct TWAP. Tokens paired with
        WETH require a two-hop conversion via the ETH price (pulled from
        the gateway too).
        """
        token_upper = token.upper()
        fast_price = self._try_fast_path_price(token, token_upper, use_cache=use_cache)
        if fast_price is not None:
            return fast_price

        pool_address, pool_key = self._resolve_pool_for_token(token_upper)
        twap_point = await self._fetch_twap_point_or_raise(pool_address, token_upper)

        price = await self._twap_price_with_two_hop_if_needed(token, pool_key, twap_point["price"])
        if price is None:
            return None

        self._cache_twap_price(
            token_upper,
            price=price,
            tick_observation_count=twap_point["tick_observation_count"],
            pool_address=pool_address,
        )
        logger.debug("TWAP price for %s: $%.4f", token, float(price))
        return price

    async def _fetch_twap_via_gateway(self, pool_address: str) -> dict[str, Any]:
        """Issue ``GetDexTwap`` RPC for the Uniswap V3 pool on this chain."""
        client, gateway_pb2 = _twap_get_connected_gateway_client()

        request = gateway_pb2.GetDexTwapRequest(
            dex="uniswap_v3",
            chain=self._chain,
            pool_address=pool_address,
            secs_ago_start=self._observation_window_seconds,
            secs_ago_end=0,
        )
        try:
            response = client.rate_history.GetDexTwap(request)
        except Exception as exc:
            raise DataSourceUnavailable(
                source="gateway",
                reason=f"GetDexTwap RPC failed: {exc}",
            ) from exc
        if not response.success:
            raise DataSourceUnavailable(
                source=response.source or "gateway",
                reason=response.error or "GetDexTwap returned success=false",
            )
        return {
            "price": Decimal(response.point.price),
            # ``tick_observation_count`` is the pool's ring-buffer observation
            # counter (a sanity-check field), NOT the computed tick TWAP. The
            # gateway TWAP point carries no computed-tick value, so we surface
            # the counter under its honest name rather than mislabeling it.
            "tick_observation_count": int(response.point.tick_observation_count),
        }

    async def _fetch_eth_usd_price(self) -> Decimal:
        """Two-hop convenience: ask the gateway for WETH/USDC TWAP on this chain."""
        pool_key = _reference_pools()["token_to_pool"].get("WETH", {}).get(self._chain)
        if pool_key is None:
            raise DataSourceUnavailable(
                source="gateway",
                reason=f"No WETH/USDC pool configured for {self._chain!r}",
            )
        pool_address = self._pools.get(pool_key)
        if pool_address is None:
            raise DataSourceUnavailable(
                source="gateway",
                reason=f"Pool key {pool_key!r} missing from {self._chain!r} pool table",
            )
        twap = await self._fetch_twap_via_gateway(pool_address)
        return twap["price"]

    async def get_price(
        self,
        token: str,
        timestamp: datetime | None = None,
    ) -> Decimal:
        """Get the price of a token at a specific timestamp.

        Note: TWAP provider primarily supports live / near-live prices.
        Historical TWAP at a pinned block requires the gateway-side
        ``as_of_block`` parameter (tracked in VIB-4870).
        """
        price = await self.get_latest_price(token)
        if price is None:
            raise ValueError(f"TWAP price not available for {token}")
        return price

    async def get_ohlcv(
        self,
        token: str,
        start: datetime,
        end: datetime,
        interval_seconds: int = 3600,
    ) -> list[Any]:
        """Generate pseudo-OHLCV from the current TWAP for compatibility.

        TWAP provides spot prices only — returns flat OHLCV bars at the
        requested interval where O = H = L = C = current TWAP.
        """
        price = await self.get_latest_price(token)
        if price is None:
            return []

        current = start.replace(tzinfo=UTC) if start.tzinfo is None else start
        end_tz = end.replace(tzinfo=UTC) if end.tzinfo is None else end
        interval = timedelta(seconds=interval_seconds)

        ohlcv_list: list[Any] = []
        while current <= end_tz:
            ohlcv_list.append(_build_flat_ohlcv_bar(current, price))
            current += interval

        return ohlcv_list

    async def iterate(self, config: Any) -> AsyncIterator[tuple[datetime, Any]]:
        """Iterate through historical market states.

        Historical-window TWAP iteration requires the gateway-side
        ``fetch_twap_series`` capability, which lands in VIB-4870. Until
        then the capability is genuinely unavailable, so this raises
        :class:`DataSourceUnavailable` rather than yielding nothing.

        Raising — not returning an empty iterator — is deliberate: an empty
        async generator looks like a *successful* zero-row iteration to
        :meth:`AggregatedDataProvider.iterate`, which stops at the first
        provider that completes without error. That would silently run a
        backtest with 0 ticks and no strategy execution (the "no silent
        zeros" rule, VIB-4859 decision 4). Raising lets the aggregator fall
        back to the next iterate-capable provider, or to manual
        ``get_price()`` iteration.

        The ``yield`` below is unreachable but keeps the function an async
        generator so type inference / mypy treat it as ``AsyncIterator``.
        """
        raise DataSourceUnavailable(
            source="gateway",
            reason=(
                "TWAPDataProvider.iterate() requires the gateway-side "
                "fetch_twap_series capability (tracked in VIB-4870)."
            ),
        )
        # Unreachable but keeps the body a generator function for mypy.
        yield  # pragma: no cover

    async def close(self) -> None:
        """No-op shutdown hook."""

    async def __aenter__(self) -> TWAPDataProvider:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()


# NOTE: the legacy table names (UNISWAP_V3_POOLS, TOKEN_TO_POOL, and the
# per-chain *_POOLS dicts) remain importable via the module __getattr__
# above but are deliberately absent from __all__ — ruff/mypy can't see
# PEP 562 names, and star-imports shouldn't pull derived views anyway.
__all__ = [
    "ARCHIVE_RPC_CHAINS",
    "ARCHIVE_RPC_URL_ENV_PATTERN",
    "CachedTWAP",
    "DEFAULT_TWAP_WINDOW_SECONDS",
    "OBSERVE_SELECTOR",
    "SLOT0_SELECTOR",
    "TWAPDataProvider",
    "TWAPInsufficientHistoryError",
    "TWAPObservation",
    "TWAPPoolNotFoundError",
    "TWAPResult",
]
