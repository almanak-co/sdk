"""CoinGecko OHLCV Data Provider (gateway egress layer).

Provides CEX-reference OHLCV candlestick data from CoinGecko's
``/coins/{id}/ohlc`` endpoint. This is the second CEX-capable OHLCV source
(after Binance) and exists so the router's ``cex_primary`` failover chain has
a real fallback when the Binance staleness guard (ALM-2697) rejects a stale
kline response — without it, a stale/rebranded Binance ticker yields a
permanent ``DATA_ERROR`` (VIB-4847).

This provider belongs to the **gateway egress layer**: the actual HTTP call
runs through :class:`CoinGeckoIntegration` (aiohttp), which is correct here.
The strategy container reaches this data only via the gateway ``CoinGeckoGetOHLCV``
gRPC endpoint and the thin framework-side ``GatewayCoinGeckoOHLCVProvider``.

CoinGecko OHLC granularity caveats (intentional, documented):
    - The endpoint has **no explicit interval** — candle granularity is a
      function of the ``days`` window (``days=1`` -> 30m, ``7|14|30`` -> 4h,
      ``>=31`` -> 4d). We pick the smallest window that yields candles at or
      finer than the requested timeframe, then bucket-aggregate to the
      requested timeframe so the returned candles line up with the router's
      staleness budget.
    - The framework's canonical timeframes are ``1m, 5m, 15m, 1h, 4h, 1d``
      (``VALID_TIMEFRAMES``). Of those, CoinGecko OHLC can serve ``1h``
      (30m native aggregated), ``4h`` (native), and ``1d`` (4h native
      aggregated). Sub-hour timeframes (``1m`` / ``5m`` / ``15m``) are below the
      native 30m floor and are rejected with ``DataSourceUnavailable`` so the
      router records a clean provider miss rather than returning
      coarse-but-mislabeled candles.
    - **No volume.** CoinGecko OHLC carries price-only candles, so
      ``OHLCVCandle.volume`` is ``None`` (unmeasured, never ``0``).

Example:
    from almanak.gateway.data.ohlcv.coingecko_provider import CoinGeckoOHLCVProvider

    provider = CoinGeckoOHLCVProvider()
    candles = await provider.get_ohlcv("WETH", timeframe="1h", limit=100)
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import ClassVar

from almanak.framework.data.interfaces import (
    DataSourceUnavailable,
    OHLCVCandle,
    validate_timeframe,
)
from almanak.framework.data.ohlcv.aggregation import aggregate_complete_candles, open_time_from_close_time
from almanak.framework.data.timeframes import COINGECKO_OHLCV_TIMEFRAMES, OHLCVTimeframe
from almanak.integrations.coingecko.gateway.client import CoinGeckoIntegration
from almanak.integrations.coingecko.gateway.price_source import GLOBAL_TOKEN_IDS

logger = logging.getLogger(__name__)


class CoinGeckoOHLCVProvider:
    """CoinGecko OHLCV provider (CEX-reference candles, gateway egress).

    Implements the ``OHLCVProvider`` protocol surface (``get_ohlcv`` +
    ``supported_timeframes``) used by the gateway ``CoinGeckoGetOHLCV`` RPC
    handler. Egress runs through :class:`CoinGeckoIntegration`.
    """

    SUPPORTED_TIMEFRAMES: ClassVar[tuple[OHLCVTimeframe, ...]] = COINGECKO_OHLCV_TIMEFRAMES.supported

    def __init__(self, integration: CoinGeckoIntegration | None = None) -> None:
        """Initialize the provider.

        Args:
            integration: Optional pre-built :class:`CoinGeckoIntegration`.
                When omitted, one is constructed lazily (free/pro tier is
                auto-selected from the gateway API-key environment).
        """
        self._integration = integration or CoinGeckoIntegration()

    @property
    def name(self) -> str:
        """Provider identifier matching the router chain key."""
        return "coingecko"

    @property
    def supported_timeframes(self) -> tuple[OHLCVTimeframe, ...]:
        """Return the canonical timeframes CoinGecko can serve exactly."""
        return self.SUPPORTED_TIMEFRAMES

    def _resolve_token_id(self, token: str) -> str | None:
        """Resolve a token SYMBOL to a CoinGecko coin id, or None.

        Resolution order mirrors the price-side
        :meth:`CoinGeckoPriceSource._resolve_token_id` so this provider covers
        the same CEX symbol universe Binance does. Without registry coverage the
        failover chain ``cex_primary = [binance, coingecko]`` was hollow for
        every symbol present in the token registry but absent from the (much
        smaller) ``GLOBAL_TOKEN_IDS`` table — OP / SUSHI / YFI / BAL / 1INCH and
        every other Binance-listed token carried in ``tokens.json``.

        1. ``GLOBAL_TOKEN_IDS`` — the curated per-chain + registry-derived slug
           table used by the price source's hardcoded fallback. This is
           consulted **first** because it carries exact native and wrapped
           asset IDs (for example ``AVAX -> avalanche-2`` and
           ``WAVAX -> wrapped-avax``, plus ``WMATIC -> wmatic``) as well as
           other curated rows.
        2. ``get_coingecko_id`` over the canonical ``DEFAULT_TOKENS`` registry
           (``tokens.json``) fills genuine misses such as OP, SUSHI, YFI, BAL,
           and 1INCH. Returns ``None`` for ambiguous symbols (one symbol
           mapping to multiple coin ids), in which case we fall through rather
           than guess.

        The import is local: eager import of the framework token defaults at
        module load would widen this gateway-egress module's import graph and
        risks a cycle with the price-source registry build.
        """
        token_upper = token.upper()

        explicit_id = GLOBAL_TOKEN_IDS.get(token_upper)
        if explicit_id:
            return explicit_id

        try:
            from almanak.framework.data.tokens.defaults import get_coingecko_id

            return get_coingecko_id(token_upper)
        except ImportError:
            return None

    async def get_ohlcv(
        self,
        token: str,
        quote: str = "USD",
        timeframe: OHLCVTimeframe = OHLCVTimeframe.ONE_HOUR,
        limit: int = 100,
    ) -> list[OHLCVCandle]:
        """Fetch OHLCV candles for a token from CoinGecko.

        Args:
            token: Token symbol (e.g. "WETH", "ARB").
            quote: Quote currency (CoinGecko OHLC is fiat-quoted; non-fiat
                quotes are priced against USD).
            timeframe: Canonical candle timeframe. Supported: 1h, 4h, 1d.
            limit: Maximum number of candles to return. This gateway routing
                surface is intentionally best-effort for historical fallback:
                a fixed CoinGecko window may contain fewer candles than the
                requested maximum. Indicator code that requires an exact
                minimum negotiates the plan capacity before calling its strict
                framework provider.

        Returns:
            List of OHLCVCandle sorted ascending by timestamp. Volume is
            ``None`` (CoinGecko OHLC is price-only).

        Raises:
            DataSourceUnavailable: Unknown token, unsupported timeframe, or
                API error.
            ValueError: If the timeframe is structurally invalid.
        """
        timeframe = validate_timeframe(timeframe)

        try:
            plan = COINGECKO_OHLCV_TIMEFRAMES.resolve(timeframe)
        except ValueError as exc:
            raise DataSourceUnavailable(
                source="coingecko",
                reason=str(exc),
            ) from exc

        token_id = self._resolve_token_id(token)
        if token_id is None:
            raise DataSourceUnavailable(
                source="coingecko",
                reason=f"Unknown token for CoinGecko OHLC: {token}",
            )

        # CoinGecko OHLC is fiat-quoted; map any quote to a fiat currency.
        vs_currency = quote.lower()
        if vs_currency in ("usdt", "usdc", "usd", "dai"):
            vs_currency = "usd"

        try:
            rows = await self._integration.get_ohlc(
                token_id=token_id,
                days=plan.days,
                vs_currency=vs_currency,
            )
        except DataSourceUnavailable:
            raise
        except Exception as e:
            raise DataSourceUnavailable(
                source="coingecko",
                reason=f"CoinGecko OHLC request failed for {token}: {e}",
            ) from e

        candles = _rows_to_candles(rows, native_stride_seconds=plan.native_stride_seconds)
        if not candles:
            raise DataSourceUnavailable(
                source="coingecko",
                reason=f"No CoinGecko OHLC data for {token} ({token_id})",
            )

        target_stride = timeframe.seconds
        if target_stride > plan.native_stride_seconds:
            candles = aggregate_complete_candles(
                candles,
                native_stride_seconds=plan.native_stride_seconds,
                target_stride_seconds=target_stride,
            )

        return candles[-limit:] if limit and len(candles) > limit else candles


def _rows_to_candles(rows: list[list[float]], *, native_stride_seconds: int) -> list[OHLCVCandle]:
    """Convert CoinGecko close-stamped rows to SDK open-stamped candles."""
    candles: list[OHLCVCandle] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 5:
            continue
        try:
            candles.append(
                OHLCVCandle(
                    timestamp=open_time_from_close_time(
                        datetime.fromtimestamp(row[0] / 1000, tz=UTC),
                        native_stride_seconds,
                    ),
                    open=Decimal(str(row[1])),
                    high=Decimal(str(row[2])),
                    low=Decimal(str(row[3])),
                    close=Decimal(str(row[4])),
                    volume=None,  # CoinGecko OHLC carries no volume (unmeasured).
                )
            )
        except (ValueError, TypeError, IndexError, InvalidOperation):
            logger.debug("Skipping malformed CoinGecko OHLC row: %s", row)
            continue
    candles.sort(key=lambda c: c.timestamp)
    return candles


__all__ = ["CoinGeckoOHLCVProvider"]
