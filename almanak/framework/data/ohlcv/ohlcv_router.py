"""Multi-provider OHLCV router with CEX/DEX classification.

Routes OHLCV requests to the appropriate provider chain based on instrument
classification:

- **DeFi-native pairs**: GeckoTerminal -> DeFi Llama -> Binance
  Tokens that have GeckoTerminal pool data but no known CEX symbol.

- **Major CEX-listed tokens**: Binance -> CoinGecko -> DeFi Llama
  Tokens with known CEX symbols in CEX_SYMBOL_MAP.

When a CEX source (e.g. Binance) is used for a DeFi strategy, the
DataMeta.confidence is reduced to 0.7 and a warning is logged to flag
the CEX/DEX basis risk.

Finalized candles (>24h old) are cached to disk; recent candles are
tagged provisional and refreshed on next access.

Example:
    from almanak.framework.data.ohlcv.ohlcv_router import OHLCVRouter

    router = OHLCVRouter()
    router.register_provider(gecko_provider)
    router.register_provider(binance_wrapper)

    envelope = router.get_ohlcv(
        token="WETH",
        chain="arbitrum",
        timeframe="1h",
        limit=100,
    )
    candles = envelope.value
    print(envelope.meta.source)  # e.g. "geckoterminal"
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.models import (
    CEX_SYMBOL_MAP,
    OHLCV_PROXY_MAP,
    DataClassification,
    DataEnvelope,
    DataMeta,
    Instrument,
    resolve_instrument,
)
from almanak.framework.data.routing.config import DataProvider

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CEX/DEX classification
# ---------------------------------------------------------------------------

# All base symbols that have a known CEX mapping (for any exchange)
_CEX_KNOWN_BASES: frozenset[str] = frozenset(base for (_exchange, base, _quote) in CEX_SYMBOL_MAP)


def classify_instrument(instrument: Instrument) -> str:
    """Classify an instrument as "cex_primary" or "defi_primary".

    If the instrument's base token has a known CEX symbol mapping in
    CEX_SYMBOL_MAP, it's classified as CEX-primary (major token).
    Otherwise, it's DeFi-native.

    Args:
        instrument: Canonical Instrument to classify.

    Returns:
        "cex_primary" or "defi_primary".
    """
    if instrument.base in _CEX_KNOWN_BASES:
        return "cex_primary"
    return "defi_primary"


# Provider chain ordering per classification
_PROVIDER_CHAINS: dict[str, list[str]] = {
    "cex_primary": ["binance", "coingecko", "defillama"],
    "defi_primary": ["geckoterminal", "defillama", "binance"],
}


# ---------------------------------------------------------------------------
# Disk cache for finalized candles
# ---------------------------------------------------------------------------

# Candles older than this are considered finalized and cached immutably
_FINALIZATION_AGE = timedelta(hours=24)

# Default disk cache directory
_DEFAULT_CACHE_DIR = Path.home() / ".almanak" / "data_cache" / "ohlcv"


@dataclass(frozen=True)
class _CachedCandle:
    """Serializable candle representation for disk cache."""

    timestamp_iso: str
    open: str
    high: str
    low: str
    close: str
    volume: str | None


def _candle_to_dict(candle: OHLCVCandle) -> dict[str, Any]:
    return {
        "timestamp": candle.timestamp.isoformat(),
        "open": str(candle.open),
        "high": str(candle.high),
        "low": str(candle.low),
        "close": str(candle.close),
        "volume": str(candle.volume) if candle.volume is not None else None,
    }


def _dict_to_candle(d: dict[str, Any]) -> OHLCVCandle:
    return OHLCVCandle(
        timestamp=datetime.fromisoformat(d["timestamp"]),
        open=Decimal(d["open"]),
        high=Decimal(d["high"]),
        low=Decimal(d["low"]),
        close=Decimal(d["close"]),
        volume=Decimal(d["volume"]) if d.get("volume") is not None else None,
    )


class _OHLCVDiskCache:
    """Simple JSON-based disk cache for finalized OHLCV candles.

    Only candles older than _FINALIZATION_AGE are written to disk.
    Recent (provisional) candles are never persisted.
    """

    def __init__(self, cache_dir: Path | None = None) -> None:
        self._cache_dir = cache_dir or _DEFAULT_CACHE_DIR

    def _key_path(self, cache_key: str) -> Path:
        safe_name = hashlib.sha256(cache_key.encode()).hexdigest()[:32]
        return self._cache_dir / f"{safe_name}.json"

    def get(self, cache_key: str) -> list[OHLCVCandle] | None:
        """Load finalized candles from disk, or None if not cached."""
        path = self._key_path(cache_key)
        if not path.exists():
            return None
        try:
            raw = json.loads(path.read_text())
            # Validate checksum
            payload = raw.get("candles", [])
            stored_checksum = raw.get("checksum", "")
            computed = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
            if computed != stored_checksum:
                logger.warning("Disk cache checksum mismatch for %s, evicting", cache_key)
                path.unlink(missing_ok=True)
                return None
            return [_dict_to_candle(c) for c in payload]
        except Exception:
            logger.debug("Failed to read disk cache for %s", cache_key, exc_info=True)
            return None

    def put(self, cache_key: str, candles: list[OHLCVCandle]) -> None:
        """Write finalized candles to disk with sha256 checksum."""
        if not candles:
            return
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        payload = [_candle_to_dict(c) for c in candles]
        checksum = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        data = {
            "candles": payload,
            "checksum": checksum,
            "cached_at": datetime.now(UTC).isoformat(),
        }
        path = self._key_path(cache_key)
        path.write_text(json.dumps(data))
        logger.debug("Wrote %d finalized candles to disk cache: %s", len(candles), cache_key)


# ---------------------------------------------------------------------------
# OHLCV Router
# ---------------------------------------------------------------------------


@dataclass
class OHLCVRouter:
    """Routes OHLCV requests to providers with CEX/DEX classification.

    Classifies instruments and routes to the appropriate provider chain.
    Applies CEX/DEX basis warnings, disk caches finalized candles, and
    tags recent candles as provisional.

    Attributes:
        default_chain: Default chain for instrument resolution.
        disk_cache_dir: Optional disk cache directory override.
    """

    default_chain: str = "arbitrum"
    disk_cache_dir: Path | None = None
    _providers: dict[str, DataProvider] = field(default_factory=dict, init=False, repr=False)
    _disk_cache: _OHLCVDiskCache = field(init=False, repr=False)
    _proxy_warned: set[str] = field(default_factory=set, init=False, repr=False)

    def __post_init__(self) -> None:
        self._disk_cache = _OHLCVDiskCache(self.disk_cache_dir)

    def register_provider(self, provider: DataProvider) -> None:
        """Register a data provider for OHLCV routing.

        Args:
            provider: A DataProvider implementation (e.g. GeckoTerminalOHLCVProvider).
        """
        self._providers[provider.name] = provider
        logger.debug("ohlcv_router_registered provider=%s", provider.name)

    def get_ohlcv(
        self,
        token: str | Instrument,
        chain: str | None = None,
        timeframe: str = "1h",
        limit: int = 100,
        *,
        pool_address: str | None = None,
        force_provider: str | None = None,
        quote: str | None = None,
        _is_proxy_retry: bool = False,
    ) -> DataEnvelope[list[OHLCVCandle]]:
        """Fetch OHLCV candles with multi-provider routing.

        Classifies the instrument as CEX-primary or DeFi-primary and
        routes through the appropriate provider chain.

        Args:
            token: Token symbol, "BASE/QUOTE" string, or Instrument.
            chain: Chain name (default: self.default_chain).
            timeframe: Candle timeframe (1m, 5m, 15m, 1h, 4h, 1d).
            limit: Number of candles.
            pool_address: Explicit pool address for DEX providers.
            force_provider: Force a specific provider (bypass classification).

        Returns:
            DataEnvelope[list[OHLCVCandle]] with meta.source and finality tags.

        Raises:
            DataSourceUnavailable: If all providers fail.
        """
        target_chain = (chain or self.default_chain).lower()
        instrument = (
            resolve_instrument(token, target_chain, quote=quote) if not isinstance(token, Instrument) else token
        )

        # Determine provider chain
        if force_provider:
            provider_chain = [force_provider]
        else:
            classification = classify_instrument(instrument)
            provider_chain = _PROVIDER_CHAINS.get(classification, ["binance"])
            logger.debug(
                "ohlcv_classified instrument=%s classification=%s providers=%s",
                instrument.pair,
                classification,
                provider_chain,
            )

        # Check disk cache for finalized candles
        addr = (pool_address or "auto").lower()
        disk_key = f"{instrument.base}:{instrument.quote}:{target_chain}:{timeframe}:{limit}:{addr}"
        cached = self._disk_cache.get(disk_key)
        if cached is not None and len(cached) >= limit:
            logger.debug("ohlcv_disk_cache_hit key=%s count=%d", disk_key, len(cached))
            now = datetime.now(UTC)
            meta = DataMeta(
                source="disk_cache",
                observed_at=now,
                finality="off_chain",
                staleness_ms=0,
                latency_ms=0,
                confidence=1.0,
                cache_hit=True,
            )
            return DataEnvelope(
                value=cached[-limit:],
                meta=meta,
                classification=DataClassification.INFORMATIONAL,
            )

        # Try each provider in chain
        last_error: Exception | None = None
        for provider_name in provider_chain:
            provider = self._providers.get(provider_name)
            if provider is None:
                logger.debug("ohlcv_provider_not_registered provider=%s", provider_name)
                continue

            start = time.monotonic()
            try:
                envelope = provider.fetch(
                    token=instrument.base,
                    quote=instrument.quote,
                    chain=target_chain,
                    timeframe=timeframe,
                    limit=limit,
                    pool_address=pool_address or "",
                )
                latency_ms = int((time.monotonic() - start) * 1000)
                candles: list[OHLCVCandle] = envelope.value

                if not candles:
                    logger.debug("ohlcv_empty_result provider=%s", provider_name)
                    continue

                # Determine if this is a CEX/DEX basis mismatch
                is_cex_source = provider_name in ("binance", "coingecko")
                classification = classify_instrument(instrument)
                has_basis_risk = is_cex_source and classification == "defi_primary"

                confidence = envelope.meta.confidence
                if has_basis_risk:
                    confidence = min(confidence, 0.7)
                    logger.warning(
                        "cex_dex_basis_warning instrument=%s provider=%s confidence=%.2f "
                        "reason='CEX source used for DeFi-native pair, basis risk'",
                        instrument.pair,
                        provider_name,
                        confidence,
                    )

                # Split candles into finalized and provisional
                now = datetime.now(UTC)
                finalized, provisional = _split_by_finality(candles, now)

                # Cache finalized candles to disk
                if finalized:
                    self._disk_cache.put(disk_key, finalized)

                meta = DataMeta(
                    source=provider_name,
                    observed_at=now,
                    finality="off_chain",
                    staleness_ms=0,
                    latency_ms=latency_ms,
                    confidence=confidence,
                    cache_hit=False,
                )

                logger.info(
                    "ohlcv_fetched provider=%s instrument=%s candles=%d finalized=%d provisional=%d",
                    provider_name,
                    instrument.pair,
                    len(candles),
                    len(finalized),
                    len(provisional),
                )

                return DataEnvelope(
                    value=candles,
                    meta=meta,
                    classification=DataClassification.INFORMATIONAL,
                )

            except Exception as exc:
                last_error = exc
                elapsed = int((time.monotonic() - start) * 1000)
                logger.warning(
                    "ohlcv_provider_failed provider=%s instrument=%s error=%s elapsed_ms=%d",
                    provider_name,
                    instrument.pair,
                    exc,
                    elapsed,
                )
                continue

        # ---------------------------------------------------------------
        # Wrapped token proxy fallback: if all providers failed and the
        # token has a known unwrapped equivalent, retry with the proxy.
        # ---------------------------------------------------------------
        if not _is_proxy_retry:
            proxy_symbol = OHLCV_PROXY_MAP.get(instrument.base)
            if proxy_symbol:
                original_symbol = instrument.base
                if original_symbol not in self._proxy_warned:
                    self._proxy_warned.add(original_symbol)
                    logger.warning(
                        "ohlcv_proxy_fallback using %s as proxy for %s (1:1 wrapped native, no direct data source)",
                        proxy_symbol,
                        original_symbol,
                    )
                else:
                    logger.debug(
                        "ohlcv_proxy_fallback using %s as proxy for %s",
                        proxy_symbol,
                        original_symbol,
                    )

                # Build Instrument directly to bypass _canonicalize_symbol
                # which would map MNT -> WMNT (the token we're falling back FROM)
                proxy_instrument = Instrument(
                    base=proxy_symbol,
                    quote=instrument.quote,
                    chain=instrument.chain,
                    venue=instrument.venue,
                )
                proxy_envelope = self.get_ohlcv(
                    token=proxy_instrument,
                    chain=target_chain,
                    timeframe=timeframe,
                    limit=limit,
                    pool_address=pool_address,
                    force_provider=force_provider,
                    quote=quote,
                    _is_proxy_retry=True,
                )

                # Tag the returned data with proxy provenance
                from dataclasses import replace

                proxied_meta = replace(proxy_envelope.meta, proxy_source=original_symbol)
                return DataEnvelope(
                    value=proxy_envelope.value,
                    meta=proxied_meta,
                    classification=proxy_envelope.classification,
                )

        raise DataSourceUnavailable(
            source="ohlcv_router",
            reason=f"All providers failed for {instrument.pair} on {target_chain}: {last_error}",
        )


def _split_by_finality(
    candles: list[OHLCVCandle],
    now: datetime,
) -> tuple[list[OHLCVCandle], list[OHLCVCandle]]:
    """Split candles into finalized (>24h old) and provisional (recent).

    Args:
        candles: Sorted list of candles.
        now: Current UTC time.

    Returns:
        Tuple of (finalized, provisional) candle lists.
    """
    cutoff = now - _FINALIZATION_AGE
    finalized: list[OHLCVCandle] = []
    provisional: list[OHLCVCandle] = []

    for candle in candles:
        if candle.timestamp < cutoff:
            finalized.append(candle)
        else:
            provisional.append(candle)

    return finalized, provisional


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "OHLCVRouter",
    "classify_instrument",
]
