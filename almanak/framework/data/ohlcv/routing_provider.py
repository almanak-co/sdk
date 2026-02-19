"""Routing OHLCV provider -- adapts OHLCVRouter to the OHLCVProvider protocol.

Binds chain and pool_address context at construction so that the chainless
``OHLCVProvider.get_ohlcv()`` signature works transparently with the
multi-provider routing infrastructure.

Example:
    from almanak.framework.data.ohlcv.routing_provider import RoutingOHLCVProvider
    from almanak.framework.data.ohlcv.ohlcv_router import OHLCVRouter

    router = OHLCVRouter(default_chain="base")
    provider = RoutingOHLCVProvider(router=router, chain="base", pool_address="0x...")

    # OHLCVProvider protocol -- indicators call this directly
    candles = await provider.get_ohlcv("ALMANAK", timeframe="1h", limit=100)
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from almanak.framework.data.interfaces import OHLCVCandle, validate_timeframe

if TYPE_CHECKING:
    from almanak.framework.data.ohlcv.ohlcv_router import OHLCVRouter

logger = logging.getLogger(__name__)

# Intersection of timeframes supported by both GeckoTerminal and Gateway/Binance
_SUPPORTED_TIMEFRAMES: list[str] = ["1m", "5m", "15m", "1h", "4h", "1d"]


class RoutingOHLCVProvider:
    """Implements the OHLCVProvider protocol by delegating to OHLCVRouter.

    The OHLCVProvider protocol (used by RSICalculator and other indicators)
    has no ``chain`` parameter.  This adapter binds chain and pool_address
    at construction time so the router receives full context on every call.
    """

    def __init__(
        self,
        router: OHLCVRouter,
        chain: str,
        pool_address: str | None = None,
        closeable_providers: list[Any] | None = None,
    ) -> None:
        """Initialise the routing provider.

        Args:
            router: Configured OHLCVRouter with providers already registered.
            chain: Chain name (e.g. "base", "arbitrum") bound to every request.
            pool_address: Optional pool address for DEX lookups.
            closeable_providers: Providers with an async ``close()`` method
                that should be cleaned up when this adapter is closed.
        """
        self._router = router
        self._chain = chain.lower()
        self._pool_address = pool_address
        self._closeable_providers = closeable_providers or []
        logger.info(
            "RoutingOHLCVProvider initialised chain=%s pool_address=%s",
            self._chain,
            self._pool_address or "(auto)",
        )

    # -- OHLCVProvider protocol ------------------------------------------------

    @property
    def supported_timeframes(self) -> list[str]:
        """Return timeframes supported by the routing layer."""
        return _SUPPORTED_TIMEFRAMES.copy()

    async def get_ohlcv(
        self,
        token: str,
        quote: str = "USD",
        timeframe: str = "1h",
        limit: int = 100,
    ) -> list[OHLCVCandle]:
        """Fetch OHLCV candles via multi-provider routing.

        Delegates to ``OHLCVRouter.get_ohlcv()`` (sync) via
        ``asyncio.to_thread`` and unwraps the DataEnvelope.

        Args:
            token: Token symbol (e.g. "ALMANAK", "WETH").
            quote: Quote currency (passed through to router).
            timeframe: Candle timeframe (1m, 5m, 15m, 1h, 4h, 1d).
            limit: Number of candles to fetch.

        Returns:
            List of OHLCVCandle sorted by timestamp ascending.

        Raises:
            DataSourceUnavailable: If all providers in the chain fail.
            ValueError: If timeframe is not valid.
        """
        validate_timeframe(timeframe)
        if timeframe not in _SUPPORTED_TIMEFRAMES:
            raise ValueError(
                f"Timeframe '{timeframe}' not supported by routing provider. "
                f"Supported: {', '.join(_SUPPORTED_TIMEFRAMES)}"
            )

        envelope = await asyncio.to_thread(
            self._router.get_ohlcv,
            token,
            chain=self._chain,
            timeframe=timeframe,
            limit=limit,
            pool_address=self._pool_address,
            quote=quote,
        )

        candles: list[OHLCVCandle] = envelope.value
        logger.debug(
            "RoutingOHLCVProvider returned %d candles for %s via %s",
            len(candles),
            token,
            envelope.meta.source,
        )
        return candles

    # -- Lifecycle -------------------------------------------------------------

    async def close(self) -> None:
        """Close any registered closeable providers (e.g. HTTP sessions)."""
        for provider in self._closeable_providers:
            if hasattr(provider, "close"):
                try:
                    await provider.close()
                except (OSError, RuntimeError):
                    logger.debug("Error closing provider %s", provider, exc_info=True)


__all__ = ["RoutingOHLCVProvider"]
