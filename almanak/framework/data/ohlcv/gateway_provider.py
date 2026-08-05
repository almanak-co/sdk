"""Gateway-backed OHLCV provider.

This module provides an OHLCV provider that fetches data through the gateway
sidecar instead of making direct HTTP requests. This is the preferred mode
for production deployments where strategies run in isolated containers.

The provider uses the gateway's Binance integration for OHLCV data.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from almanak.integrations.chains import integration_market_symbol, integration_market_symbol_map

from ..interfaces import (
    DataSourceUnavailable,
    OHLCVCandle,
    validate_timeframe,
)
from ..timeframes import (
    BINANCE_OHLCV_TIMEFRAMES,
    COINGECKO_OHLCV_TIMEFRAMES,
    COINGECKO_ONCHAIN_OHLCV_TIMEFRAMES,
    OHLCVTimeframe,
)

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


# Token symbol to Binance trading pair mapping
# Assumes USDT as quote currency for most pairs
TOKEN_TO_BINANCE_SYMBOL = {
    base: symbol
    for (provider, base, quote), symbol in integration_market_symbol_map().items()
    if provider == "binance" and quote == "USDT"
}

# Compatibility export. The immutable mapping is owned by the canonical
# exhaustive provider specification in ``data.timeframes``.
TIMEFRAME_TO_BINANCE_INTERVAL = BINANCE_OHLCV_TIMEFRAMES.mapping


@dataclass
class OHLCVHealthMetrics:
    """Health metrics for OHLCV provider."""

    total_requests: int = 0
    successful_requests: int = 0
    cache_hits: int = 0
    errors: int = 0

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_requests == 0:
            return 100.0
        return (self.successful_requests / self.total_requests) * 100


@dataclass
class CacheEntry:
    """Cache entry for OHLCV data."""

    candles: list[OHLCVCandle]
    timestamp: float = field(default_factory=time.monotonic)


class GatewayOHLCVProvider:
    """Gateway-backed OHLCV provider implementing the OHLCVProvider protocol.

    Fetches OHLCV data through the gateway's Binance integration, ensuring
    all external API access is mediated by the gateway sidecar.

    This provider maps token symbols to Binance trading pairs and uses
    the gateway's BinanceGetKlines RPC for data fetching.

    Includes configurable TTL caching: shorter for live data (1m, 5m timeframes)
    and longer for historical data (1h+).

    Example:
        from almanak.framework.gateway_client import GatewayClient
        from almanak.framework.data.ohlcv.gateway_provider import GatewayOHLCVProvider

        with GatewayClient() as client:
            provider = GatewayOHLCVProvider(gateway_client=client)
            candles = await provider.get_ohlcv("WETH", timeframe="1h", limit=100)
            print(f"Got {len(candles)} candles")
    """

    _SUPPORTED_TIMEFRAMES: ClassVar[tuple[OHLCVTimeframe, ...]] = BINANCE_OHLCV_TIMEFRAMES.supported
    _LIVE_TIMEFRAMES: ClassVar[frozenset[OHLCVTimeframe]] = frozenset(
        {OHLCVTimeframe.ONE_MINUTE, OHLCVTimeframe.FIVE_MINUTES}
    )

    def __init__(
        self,
        gateway_client: "GatewayClient",
        cache_ttl_live: float = 10.0,
        cache_ttl_historical: float = 60.0,
    ) -> None:
        """Initialize the gateway OHLCV provider.

        Args:
            gateway_client: Connected GatewayClient instance
            cache_ttl_live: Cache TTL in seconds for live timeframes (1m, 5m). Default: 10s
            cache_ttl_historical: Cache TTL in seconds for historical timeframes (15m+). Default: 60s
        """
        self._gateway_client = gateway_client
        self._metrics = OHLCVHealthMetrics()
        self._cache: dict[tuple[str, OHLCVTimeframe, int], CacheEntry] = {}
        self._cache_ttl_live = cache_ttl_live
        self._cache_ttl_historical = cache_ttl_historical

        logger.info(
            "Initialized GatewayOHLCVProvider with cache TTLs: live=%ss, historical=%ss",
            cache_ttl_live,
            cache_ttl_historical,
        )

    @property
    def supported_timeframes(self) -> tuple[OHLCVTimeframe, ...]:
        """Return the list of timeframes this provider supports.

        Returns:
            List of supported timeframe strings.
        """
        return self._SUPPORTED_TIMEFRAMES

    def _resolve_binance_symbol(self, token: str) -> str | None:
        """Resolve token symbol to Binance trading pair.

        Consults the canonical ``CEX_SYMBOL_MAP`` (preferring the USDT pair, then
        USDC) before the connector-local ``TOKEN_TO_BINANCE_SYMBOL``. The local
        table had drifted from ``CEX_SYMBOL_MAP`` — CBBTC/DAI/GMX/PENDLE/BTCB are
        declared there (e.g. CBBTC -> BTCUSDT as a BTC spot proxy) but were
        absent locally, so this resolver returned ``None`` and Binance was
        silently skipped in favour of a sparse CoinGecko fallback, breaking
        realized-vol. Reading the canonical map first keeps the two in sync; the
        local table remains the fallback for tokens not in ``CEX_SYMBOL_MAP``.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")

        Returns:
            Binance trading pair (e.g., "ETHUSDT") or None if not found
        """
        token_upper = token.upper()
        if "0X" in token_upper:
            # Address-native instrument base (``CHAIN:0XADDR`` composite from
            # resolve_instrument, or a raw address). The CEX maps are keyed by
            # symbol only, so without resolution the Binance fallback in the
            # defi_primary provider chain was dead on arrival for address
            # inputs — the address lane had exactly one provider and no
            # redundancy. Resolution is offline and best-effort; an
            # unresolvable address keeps the previous "Unknown token" outcome.
            from almanak.framework.data.tokens.address_resolution import resolve_token_symbol

            resolved = resolve_token_symbol(token, None)
            if resolved:
                token_upper = resolved
        for quote in ("USDT", "USDC"):
            mapped = integration_market_symbol("binance", token_upper, quote)
            if mapped:
                return mapped
        return TOKEN_TO_BINANCE_SYMBOL.get(token_upper)

    def _get_cache_ttl(self, timeframe: OHLCVTimeframe) -> float:
        """Get the appropriate cache TTL for a timeframe.

        Args:
            timeframe: Candle timeframe

        Returns:
            Cache TTL in seconds
        """
        if timeframe in self._LIVE_TIMEFRAMES:
            return self._cache_ttl_live
        return self._cache_ttl_historical

    def _get_cached(
        self,
        cache_key: tuple[str, OHLCVTimeframe, int],
        ttl: float,
    ) -> list[OHLCVCandle] | None:
        """Get cached data if still valid.

        Args:
            cache_key: Cache key tuple (token, timeframe, limit)
            ttl: TTL in seconds

        Returns:
            Cached candles if valid, None otherwise
        """
        entry = self._cache.get(cache_key)
        if entry is None:
            return None

        age = time.monotonic() - entry.timestamp
        if age > ttl:
            # Cache expired
            del self._cache[cache_key]
            return None

        return entry.candles

    async def get_ohlcv(
        self,
        token: str,
        quote: str = "USD",  # noqa: ARG002 - unused, internally uses USDT pairs
        timeframe: OHLCVTimeframe = OHLCVTimeframe.ONE_HOUR,
        limit: int = 100,
    ) -> list[OHLCVCandle]:
        """Get OHLCV data for a token through gateway.

        Uses the gateway's Binance integration to fetch kline data.
        Results are cached with configurable TTL based on timeframe.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            quote: Quote currency (unused - internally uses USDT pairs)
            timeframe: Candle timeframe. Supported: "1m", "5m", "15m", "1h", "4h", "1d"
            limit: Number of candles to fetch (max 1000)

        Returns:
            List of OHLCVCandle objects sorted by timestamp ascending.

        Raises:
            DataSourceUnavailable: If data cannot be fetched
            ValueError: If timeframe is not supported
        """
        timeframe = validate_timeframe(timeframe)
        self._metrics.total_requests += 1

        # Check cache first
        cache_key = (token.upper(), timeframe, limit)
        ttl = self._get_cache_ttl(timeframe)
        cached = self._get_cached(cache_key, ttl)
        if cached is not None:
            self._metrics.cache_hits += 1
            self._metrics.successful_requests += 1
            logger.debug("Cache hit for %s %s (limit=%d)", token, timeframe, limit)
            return cached

        # Resolve token to Binance symbol
        binance_symbol = self._resolve_binance_symbol(token)
        if binance_symbol is None:
            error_msg = f"Unknown token for Binance: {token}"
            self._metrics.errors += 1
            raise DataSourceUnavailable(source="gateway_ohlcv", reason=error_msg)

        # Map timeframe to Binance interval
        binance_interval = BINANCE_OHLCV_TIMEFRAMES.resolve(timeframe)

        try:
            from almanak.gateway.proto import gateway_pb2

            # Call gateway's Binance klines endpoint in a thread to avoid blocking
            request = gateway_pb2.BinanceKlinesRequest(
                symbol=binance_symbol,
                interval=binance_interval,
                limit=min(limit, 1000),  # Binance max is 1000
            )
            response = await asyncio.to_thread(
                self._gateway_client.integration.BinanceGetKlines,
                request,
                self._gateway_client.config.timeout,
            )

            if not response.klines:
                error_msg = f"No kline data returned for {binance_symbol}"
                self._metrics.errors += 1
                raise DataSourceUnavailable(source="gateway_ohlcv", reason=error_msg)

            # Convert Binance klines to OHLCVCandle
            candles = []
            for kline in response.klines:
                candles.append(
                    OHLCVCandle(
                        timestamp=datetime.fromtimestamp(kline.open_time / 1000, tz=UTC),
                        open=Decimal(kline.open) if kline.open else Decimal(0),
                        high=Decimal(kline.high) if kline.high else Decimal(0),
                        low=Decimal(kline.low) if kline.low else Decimal(0),
                        close=Decimal(kline.close) if kline.close else Decimal(0),
                        volume=Decimal(kline.volume) if kline.volume else None,
                    )
                )

            # Sort by timestamp (oldest first)
            candles.sort(key=lambda x: x.timestamp)

            # Cache the result
            self._cache[cache_key] = CacheEntry(candles=candles)

            self._metrics.successful_requests += 1
            logger.debug(
                "Fetched %d OHLCV candles for %s via gateway",
                len(candles),
                token,
            )

            return candles

        except DataSourceUnavailable:
            raise
        except Exception as e:
            # VIB-3800: surface typed errors from the gateway when available.
            from almanak.framework.data.interfaces import data_source_error_from_grpc

            typed = data_source_error_from_grpc(e, default_source="gateway_ohlcv")
            self._metrics.errors += 1
            if typed is not None:
                logger.error("Gateway OHLCV request failed: %s", e)
                raise typed from e

            error_msg = f"Gateway OHLCV request failed: {e}"
            logger.exception(error_msg)
            raise DataSourceUnavailable(source="gateway_ohlcv", reason=error_msg) from e

    def get_health_metrics(self) -> dict[str, Any]:
        """Get health metrics for observability."""
        return {
            "total_requests": self._metrics.total_requests,
            "successful_requests": self._metrics.successful_requests,
            "cache_hits": self._metrics.cache_hits,
            "errors": self._metrics.errors,
            "success_rate": round(self._metrics.success_rate, 2),
            "cache_size": len(self._cache),
        }

    def clear_cache(self) -> None:
        """Clear the OHLCV cache."""
        self._cache.clear()
        logger.debug("OHLCV cache cleared")


class GatewayCoinGeckoOnchainOHLCVProvider:
    """Gateway-backed CoinGecko Onchain OHLCV provider.

    Proxies CoinGecko Onchain OHLCV requests through the gateway's
    CoinGeckoOnchainGetOHLCV gRPC endpoint. This allows deployed strategy
    containers (which have no internet) to access DEX OHLCV data.

    Mirrors the GatewayOHLCVProvider pattern but targets CoinGecko Onchain
    instead of Binance.
    """

    _SUPPORTED_TIMEFRAMES: ClassVar[tuple[OHLCVTimeframe, ...]] = COINGECKO_ONCHAIN_OHLCV_TIMEFRAMES.supported
    _LIVE_TIMEFRAMES: ClassVar[frozenset[OHLCVTimeframe]] = frozenset(
        {OHLCVTimeframe.ONE_MINUTE, OHLCVTimeframe.FIVE_MINUTES}
    )

    def __init__(
        self,
        gateway_client: "GatewayClient",
        chain: str = "ethereum",
        cache_ttl_live: float = 15.0,
        cache_ttl_historical: float = 60.0,
    ) -> None:
        self._gateway_client = gateway_client
        self._chain = chain
        self._metrics = OHLCVHealthMetrics()
        self._cache: dict[tuple[str, str, OHLCVTimeframe, int], CacheEntry] = {}
        self._cache_ttl_live = cache_ttl_live
        self._cache_ttl_historical = cache_ttl_historical

    @property
    def supported_timeframes(self) -> tuple[OHLCVTimeframe, ...]:
        """Return the canonical intervals supported by CoinGecko Onchain."""
        return self._SUPPORTED_TIMEFRAMES

    async def get_ohlcv(
        self,
        token: str,
        quote: str = "USD",
        timeframe: OHLCVTimeframe = OHLCVTimeframe.ONE_HOUR,
        limit: int = 100,
        *,
        pool_address: str | None = None,
        chain: str | None = None,
    ) -> list[OHLCVCandle]:
        """Get DEX OHLCV data through the gateway's CoinGecko Onchain proxy.

        Args:
            token: Token symbol (e.g., "ALMANAK", "WETH")
            quote: Quote currency (default "USD")
            timeframe: Candle timeframe (1m, 5m, 15m, 1h, 4h, 1d)
            limit: Number of candles (max 1000)
            pool_address: Explicit pool address (optional)
            chain: Chain override (defaults to constructor chain)

        Returns:
            List of OHLCVCandle sorted by timestamp ascending.
        """
        timeframe = validate_timeframe(timeframe)
        self._metrics.total_requests += 1
        target_chain = chain or self._chain

        cache_key = (f"{token}:{target_chain}:{pool_address or ''}", quote, timeframe, limit)
        ttl = self._cache_ttl_live if timeframe in self._LIVE_TIMEFRAMES else self._cache_ttl_historical
        entry = self._cache.get(cache_key)
        if entry is not None and (time.monotonic() - entry.timestamp) < ttl:
            self._metrics.cache_hits += 1
            self._metrics.successful_requests += 1
            return entry.candles

        try:
            from almanak.gateway.proto import gateway_pb2

            request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(
                token=token,
                chain=target_chain,
                timeframe=timeframe.value,
                limit=min(limit, 1000),
                pool_address=pool_address or "",
                quote=quote,
                # CoinGecko Onchain is DEX-native: request continuous buckets so
                # interior no-trade intervals don't fragment indicator windows.
                # The trailing-edge (last-trade -> now) gap is closed by the
                # OHLCV router's DEX forward-fill, not by this flag.
                include_empty_intervals=True,
            )
            response = await asyncio.to_thread(
                self._gateway_client.integration.CoinGeckoOnchainGetOHLCV,
                request,
                self._gateway_client.config.timeout,
            )

            if not response.candles:
                self._metrics.errors += 1
                raise DataSourceUnavailable(
                    source="gateway_coingecko_onchain",
                    reason=f"No OHLCV data for {token} on {target_chain}",
                )

            candles = []
            for c in response.candles:
                candles.append(
                    OHLCVCandle(
                        timestamp=datetime.fromtimestamp(c.timestamp, tz=UTC),
                        open=Decimal(c.open) if c.open else Decimal(0),
                        high=Decimal(c.high) if c.high else Decimal(0),
                        low=Decimal(c.low) if c.low else Decimal(0),
                        close=Decimal(c.close) if c.close else Decimal(0),
                        volume=Decimal(c.volume) if c.volume else None,
                    )
                )

            candles.sort(key=lambda x: x.timestamp)
            self._cache[cache_key] = CacheEntry(candles=candles)
            self._metrics.successful_requests += 1
            return candles

        except DataSourceUnavailable:
            raise
        except Exception as e:
            # VIB-3800: surface typed errors from the gateway when available.
            from almanak.framework.data.interfaces import data_source_error_from_grpc

            self._metrics.errors += 1
            typed = data_source_error_from_grpc(e, default_source="gateway_coingecko_onchain")
            if typed is not None:
                raise typed from e
            raise DataSourceUnavailable(
                source="gateway_coingecko_onchain",
                reason=str(e),
            ) from e

    def get_health_metrics(self) -> dict[str, Any]:
        """Return health metrics."""
        return {
            "total_requests": self._metrics.total_requests,
            "successful_requests": self._metrics.successful_requests,
            "cache_hits": self._metrics.cache_hits,
            "errors": self._metrics.errors,
            "success_rate": round(self._metrics.success_rate, 2),
        }

    def clear_cache(self) -> None:
        """Clear the CoinGecko Onchain OHLCV cache."""
        self._cache.clear()
        logger.debug("CoinGecko Onchain OHLCV cache cleared")


class GatewayCoinGeckoOHLCVProvider:
    """Gateway-backed CoinGecko OHLCV provider (second CEX source, VIB-4847).

    Thin client over the gateway's ``CoinGeckoGetOHLCV`` gRPC endpoint. Exists
    so the router's ``cex_primary`` chain has a real fallback after Binance:
    when the ALM-2697 staleness guard rejects stale Binance klines, the router
    falls through here instead of returning a permanent ``DATA_ERROR``.

    Mirrors the :class:`GatewayOHLCVProvider` / :class:`GatewayCoinGeckoOnchainOHLCVProvider`
    pattern. Candles are price-only (``volume=None``) because CoinGecko's OHLC
    endpoint carries no volume.
    """

    _SUPPORTED_TIMEFRAMES: ClassVar[tuple[OHLCVTimeframe, ...]] = COINGECKO_OHLCV_TIMEFRAMES.supported
    _LIVE_TIMEFRAMES: ClassVar[frozenset[OHLCVTimeframe]] = frozenset({OHLCVTimeframe.ONE_HOUR})

    def __init__(
        self,
        gateway_client: "GatewayClient",
        cache_ttl_live: float = 30.0,
        cache_ttl_historical: float = 60.0,
    ) -> None:
        self._gateway_client = gateway_client
        self._metrics = OHLCVHealthMetrics()
        self._cache: dict[tuple[str, str, OHLCVTimeframe, int], CacheEntry] = {}
        self._cache_ttl_live = cache_ttl_live
        self._cache_ttl_historical = cache_ttl_historical

    @property
    def supported_timeframes(self) -> tuple[OHLCVTimeframe, ...]:
        """Return the list of timeframes this provider supports."""
        return self._SUPPORTED_TIMEFRAMES

    async def get_ohlcv(
        self,
        token: str,
        quote: str = "USD",
        timeframe: OHLCVTimeframe = OHLCVTimeframe.ONE_HOUR,
        limit: int = 100,
    ) -> list[OHLCVCandle]:
        """Get CEX-reference OHLCV data for a token through the gateway.

        Args:
            token: Token symbol (e.g., "WETH", "ARB").
            quote: Quote currency (CoinGecko OHLC is fiat-quoted).
            timeframe: Candle timeframe. Supported: 1h, 4h, 1d.
            limit: Number of candles (max 1000).

        Returns:
            List of OHLCVCandle sorted ascending by timestamp. ``volume`` is
            ``None`` (CoinGecko OHLC is price-only).

        Raises:
            DataSourceUnavailable: If data cannot be fetched.
            ValueError: If timeframe is not supported.
        """
        timeframe = validate_timeframe(timeframe)
        self._metrics.total_requests += 1

        try:
            COINGECKO_OHLCV_TIMEFRAMES.resolve(timeframe)
        except ValueError as exc:
            self._metrics.errors += 1
            raise DataSourceUnavailable(source="gateway_coingecko", reason=str(exc)) from exc

        cache_key = (token.upper(), quote.upper(), timeframe, limit)
        ttl = self._cache_ttl_live if timeframe in self._LIVE_TIMEFRAMES else self._cache_ttl_historical
        entry = self._cache.get(cache_key)
        if entry is not None and (time.monotonic() - entry.timestamp) < ttl:
            self._metrics.cache_hits += 1
            self._metrics.successful_requests += 1
            return entry.candles

        try:
            from almanak.gateway.proto import gateway_pb2

            request = gateway_pb2.CoinGeckoOHLCVRequest(
                token=token,
                timeframe=timeframe.value,
                limit=min(limit, 1000),
                quote=quote,
            )
            response = await asyncio.to_thread(
                self._gateway_client.integration.CoinGeckoGetOHLCV,
                request,
                self._gateway_client.config.timeout,
            )

            if not response.candles:
                self._metrics.errors += 1
                raise DataSourceUnavailable(
                    source="gateway_coingecko",
                    reason=f"No OHLCV data for {token}",
                )

            candles = [
                OHLCVCandle(
                    timestamp=datetime.fromtimestamp(c.timestamp, tz=UTC),
                    open=Decimal(c.open) if c.open else Decimal(0),
                    high=Decimal(c.high) if c.high else Decimal(0),
                    low=Decimal(c.low) if c.low else Decimal(0),
                    close=Decimal(c.close) if c.close else Decimal(0),
                    volume=None,  # CoinGecko OHLC carries no volume (unmeasured).
                )
                for c in response.candles
            ]

            candles.sort(key=lambda x: x.timestamp)
            self._cache[cache_key] = CacheEntry(candles=candles)
            self._metrics.successful_requests += 1
            return candles

        except DataSourceUnavailable:
            raise
        except Exception as e:
            from almanak.framework.data.interfaces import data_source_error_from_grpc

            self._metrics.errors += 1
            typed = data_source_error_from_grpc(e, default_source="gateway_coingecko")
            if typed is not None:
                raise typed from e
            raise DataSourceUnavailable(
                source="gateway_coingecko",
                reason=str(e),
            ) from e

    def get_health_metrics(self) -> dict[str, Any]:
        """Return health metrics."""
        return {
            "total_requests": self._metrics.total_requests,
            "successful_requests": self._metrics.successful_requests,
            "cache_hits": self._metrics.cache_hits,
            "errors": self._metrics.errors,
            "success_rate": round(self._metrics.success_rate, 2),
        }

    def clear_cache(self) -> None:
        """Clear the CoinGecko OHLCV cache."""
        self._cache.clear()
        logger.debug("CoinGecko OHLCV cache cleared")


__all__ = [
    "GatewayCoinGeckoOHLCVProvider",
    "GatewayCoinGeckoOnchainOHLCVProvider",
    "GatewayOHLCVProvider",
    "OHLCVHealthMetrics",
    "TOKEN_TO_BINANCE_SYMBOL",
]
