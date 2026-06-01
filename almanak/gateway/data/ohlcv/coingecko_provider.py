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
from almanak.gateway.data.price.coingecko import GLOBAL_TOKEN_IDS
from almanak.gateway.integrations.coingecko import CoinGeckoIntegration

logger = logging.getLogger(__name__)


# Canonical framework timeframes CoinGecko OHLC can serve. Constrained to the
# intersection of ``VALID_TIMEFRAMES`` (1m,5m,15m,1h,4h,1d) and what the
# endpoint's days-keyed granularity supports. Sub-hour is below the native
# 30m floor and is intentionally excluded (see module docstring).
_SUPPORTED_TIMEFRAMES: list[str] = ["1h", "4h", "1d"]

# Per-timeframe ``days`` window that yields candles at or finer than the
# requested timeframe, paired with the native candle stride (seconds) that
# window returns. We aggregate the native stride up to the requested timeframe.
#   days=1       -> 30-minute native candles
#   days=7/14/30 -> 4-hour native candles
#   days>=31     -> 4-day native candles
_NATIVE_30M = 30 * 60
_NATIVE_4H = 4 * 3600

_TIMEFRAME_PLAN: dict[str, tuple[str, int]] = {
    # timeframe: (coingecko days window, native candle stride seconds)
    "1h": ("1", _NATIVE_30M),  # 30m native -> aggregate to 1h
    "4h": ("30", _NATIVE_4H),  # 4h native (no aggregation)
    "1d": ("30", _NATIVE_4H),  # 4h native -> aggregate to 1d
}

_TIMEFRAME_SECONDS: dict[str, int] = {
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


class CoinGeckoOHLCVProvider:
    """CoinGecko OHLCV provider (CEX-reference candles, gateway egress).

    Implements the ``OHLCVProvider`` protocol surface (``get_ohlcv`` +
    ``supported_timeframes``) used by the gateway ``CoinGeckoGetOHLCV`` RPC
    handler. Egress runs through :class:`CoinGeckoIntegration`.
    """

    SUPPORTED_TIMEFRAMES: ClassVar[list[str]] = _SUPPORTED_TIMEFRAMES

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
    def supported_timeframes(self) -> list[str]:
        """Return supported timeframes (30m and coarser)."""
        return list(self.SUPPORTED_TIMEFRAMES)

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
           consulted **first** because it carries the deliberate wrapped-CEX
           proxies that the Binance/CoinGecko failover chain depends on:
           ``WBNB -> binancecoin`` and ``WAVAX -> avalanche-2`` map the wrapped
           token to the SAME underlying asset the CEX leg already priced
           (``BNBUSDT`` / ``AVAXUSDT``). The token registry instead resolves
           ``WBNB -> wbnb`` and ``WAVAX -> wrapped-avax`` (the wrapped asset's
           own CoinGecko id), which would price a DIFFERENT asset on failover —
           the wrong-asset-candles regression Codex flagged (VIB-4847 re-audit).
           Explicit/curated mappings must win.
        2. ``get_coingecko_id`` over the canonical ``DEFAULT_TOKENS`` registry
           (``tokens.json``) fills genuine misses — symbols absent from
           ``GLOBAL_TOKEN_IDS`` entirely. This is where the MATIC/WMATIC/POL ->
           ``polygon-ecosystem-token`` rebrand that motivated VIB-4847 resolves:
           none of those symbols appear in ``GLOBAL_TOKEN_IDS`` (no Polygon
           chain table), so the registry-encoded rebrand is the resolved id.
           Returns ``None`` for ambiguous symbols (one symbol mapping to
           multiple coin ids), in which case we fall through rather than guess.

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
        timeframe: str = "1h",
        limit: int = 100,
    ) -> list[OHLCVCandle]:
        """Fetch OHLCV candles for a token from CoinGecko.

        Args:
            token: Token symbol (e.g. "WETH", "ARB").
            quote: Quote currency (CoinGecko OHLC is fiat-quoted; non-fiat
                quotes are priced against USD).
            timeframe: Candle timeframe. Supported: 30m, 1h, 2h, 4h, 6h, 8h,
                12h, 1d.
            limit: Number of candles to return (most-recent ``limit``).

        Returns:
            List of OHLCVCandle sorted ascending by timestamp. Volume is
            ``None`` (CoinGecko OHLC is price-only).

        Raises:
            DataSourceUnavailable: Unknown token, unsupported timeframe, or
                API error.
            ValueError: If the timeframe is structurally invalid.
        """
        validate_timeframe(timeframe)

        if timeframe not in _TIMEFRAME_PLAN:
            raise DataSourceUnavailable(
                source="coingecko",
                reason=(
                    f"CoinGecko OHLC cannot serve timeframe {timeframe} at native "
                    f"granularity; supported: {', '.join(self.SUPPORTED_TIMEFRAMES)}"
                ),
            )

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

        days, native_stride = _TIMEFRAME_PLAN[timeframe]

        try:
            rows = await self._integration.get_ohlc(
                token_id=token_id,
                days=days,
                vs_currency=vs_currency,
            )
        except DataSourceUnavailable:
            raise
        except Exception as e:
            raise DataSourceUnavailable(
                source="coingecko",
                reason=f"CoinGecko OHLC request failed for {token}: {e}",
            ) from e

        candles = _rows_to_candles(rows)
        if not candles:
            raise DataSourceUnavailable(
                source="coingecko",
                reason=f"No CoinGecko OHLC data for {token} ({token_id})",
            )

        target_stride = _TIMEFRAME_SECONDS[timeframe]
        if target_stride > native_stride:
            candles = _aggregate_candles(candles, target_stride)

        return candles[-limit:] if limit and len(candles) > limit else candles


def _rows_to_candles(rows: list[list[float]]) -> list[OHLCVCandle]:
    """Convert CoinGecko ``[ts_ms, o, h, l, c]`` rows to ascending candles."""
    candles: list[OHLCVCandle] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 5:
            continue
        try:
            candles.append(
                OHLCVCandle(
                    timestamp=datetime.fromtimestamp(row[0] / 1000, tz=UTC),
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


def _aggregate_candles(candles: list[OHLCVCandle], target_stride_s: int) -> list[OHLCVCandle]:
    """Bucket finer-grained candles up to ``target_stride_s`` boundaries.

    Buckets are aligned to epoch multiples of the target stride so the
    produced candles' start-times are deterministic and line up with the
    router's wall-clock staleness budget. Open = first, close = last, high =
    max, low = min within the bucket. Volume stays ``None`` (price-only).
    """
    if not candles:
        return []

    buckets: dict[int, list[OHLCVCandle]] = {}
    for c in candles:
        bucket_start = int(c.timestamp.timestamp()) // target_stride_s * target_stride_s
        buckets.setdefault(bucket_start, []).append(c)

    aggregated: list[OHLCVCandle] = []
    for bucket_start in sorted(buckets):
        group = sorted(buckets[bucket_start], key=lambda c: c.timestamp)
        aggregated.append(
            OHLCVCandle(
                timestamp=datetime.fromtimestamp(bucket_start, tz=UTC),
                open=group[0].open,
                high=max(c.high for c in group),
                low=min(c.low for c in group),
                close=group[-1].close,
                volume=None,
            )
        )
    return aggregated


__all__ = ["CoinGeckoOHLCVProvider"]
