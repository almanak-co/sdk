"""Adapter: GatewayOHLCVProvider -> DataProvider protocol.

Wraps the async ``GatewayOHLCVProvider`` (which only implements the
``OHLCVProvider`` protocol) so it can register with ``OHLCVRouter``
as a sync ``DataProvider``.

Uses the same async-to-sync wrapping pattern as
``GeckoTerminalOHLCVProvider.fetch()``.

Example:
    from almanak.framework.data.ohlcv.gateway_data_adapter import GatewayOHLCVDataProvider
    from almanak.framework.data.ohlcv.gateway_provider import GatewayOHLCVProvider

    gateway_provider = GatewayOHLCVProvider(gateway_client=client)
    adapter = GatewayOHLCVDataProvider(gateway_provider)

    # Now usable as a DataProvider
    router.register_provider(adapter)
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta

if TYPE_CHECKING:
    from almanak.framework.data.ohlcv.gateway_provider import GatewayOHLCVProvider

logger = logging.getLogger(__name__)


class GatewayOHLCVDataProvider:
    """Adapts GatewayOHLCVProvider to the DataProvider protocol for the router.

    Properties:
        name: Returns ``"binance"`` to match the router's provider chain key.
        data_class: INFORMATIONAL -- OHLCV is never execution-grade.

    Methods:
        fetch: Sync wrapper around ``GatewayOHLCVProvider.get_ohlcv()``.
        health: Delegates to ``GatewayOHLCVProvider.get_health_metrics()``.
    """

    def __init__(self, gateway_provider: GatewayOHLCVProvider) -> None:
        self._provider = gateway_provider

    # -- DataProvider protocol -------------------------------------------------

    @property
    def name(self) -> str:
        """Provider name matching the router's ``_PROVIDER_CHAINS`` key."""
        return "binance"

    @property
    def data_class(self) -> DataClassification:
        """OHLCV data is informational (not execution-grade)."""
        return DataClassification.INFORMATIONAL

    def fetch(self, **kwargs: object) -> DataEnvelope:
        """Synchronous DataProvider entry point.

        Keyword Args:
            token: Token symbol (str).
            quote: Quote currency (str, default "USD").
            timeframe: Candle timeframe (str, default "1h").
            limit: Number of candles (int, default 100).

        Returns:
            DataEnvelope wrapping a list of OHLCVCandle.
        """
        token = str(kwargs.get("token", ""))
        quote = str(kwargs.get("quote", "USD"))
        timeframe = str(kwargs.get("timeframe", "1h"))
        limit = int(kwargs.get("limit", 100))  # type: ignore[call-overload]

        start = time.monotonic()

        # Async-to-sync wrapping (same 3-tier pattern as GeckoTerminalOHLCVProvider)
        coro = self._provider.get_ohlcv(
            token=token,
            quote=quote,
            timeframe=timeframe,
            limit=limit,
        )
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    candles = pool.submit(asyncio.run, coro).result()
            else:
                candles = loop.run_until_complete(coro)
        except RuntimeError:
            candles = asyncio.run(coro)

        latency_ms = int((time.monotonic() - start) * 1000)
        meta = DataMeta(
            source=self.name,
            observed_at=datetime.now(UTC),
            finality="off_chain",
            staleness_ms=0,
            latency_ms=latency_ms,
            confidence=1.0,
            cache_hit=False,
        )
        return DataEnvelope(value=candles, meta=meta)

    def health(self) -> dict[str, object]:
        """Delegate to the underlying gateway provider's health metrics."""
        return self._provider.get_health_metrics()


__all__ = ["GatewayOHLCVDataProvider"]
