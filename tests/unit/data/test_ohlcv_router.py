"""Tests for OHLCVRouter multi-provider routing with CEX/DEX awareness.

Tests cover:
- Instrument classification (CEX-primary vs DeFi-primary)
- Provider chain selection based on classification
- CEX/DEX basis warning (confidence reduction)
- Disk cache for finalized candles (>24h)
- Provisional vs finalized candle splitting
- Fallback behavior when providers fail
- Force provider override
- MarketSnapshot integration with OHLCVRouter
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pandas as pd
import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.models import (
    DataClassification,
    DataEnvelope,
    DataMeta,
    Instrument,
)
from almanak.framework.data.ohlcv.ohlcv_router import (
    _PROVIDER_CHAINS,
    OHLCVRouter,
    _OHLCVDiskCache,
    _split_by_finality,
    classify_instrument,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_candle(timestamp: datetime, close: float = 1800.0) -> OHLCVCandle:
    """Create an OHLCVCandle with sensible defaults."""
    return OHLCVCandle(
        timestamp=timestamp,
        open=Decimal(str(close - 10)),
        high=Decimal(str(close + 5)),
        low=Decimal(str(close - 15)),
        close=Decimal(str(close)),
        volume=Decimal("50000"),
    )


def _make_candles(count: int = 5, start_hours_ago: int = 48) -> list[OHLCVCandle]:
    """Create a list of candles spanning from start_hours_ago to now."""
    now = datetime.now(UTC)
    candles = []
    for i in range(count):
        ts = now - timedelta(hours=start_hours_ago - i * (start_hours_ago // max(count - 1, 1)))
        candles.append(_make_candle(ts, close=1800.0 + i * 10))
    return candles


def _make_envelope(
    candles: list[OHLCVCandle],
    source: str = "geckoterminal",
    confidence: float = 0.9,
) -> DataEnvelope[list[OHLCVCandle]]:
    """Wrap candles in a DataEnvelope."""
    meta = DataMeta(
        source=source,
        observed_at=datetime.now(UTC),
        finality="off_chain",
        staleness_ms=0,
        latency_ms=50,
        confidence=confidence,
        cache_hit=False,
    )
    return DataEnvelope(value=candles, meta=meta, classification=DataClassification.INFORMATIONAL)


def _mock_provider(name: str, candles: list[OHLCVCandle] | None = None, fail: bool = False) -> MagicMock:
    """Create a mock DataProvider."""
    provider = MagicMock()
    provider.name = name
    provider.data_class = DataClassification.INFORMATIONAL

    if fail:
        provider.fetch.side_effect = DataSourceUnavailable(
            source=name,
            reason="Mock failure",
        )
    elif candles is not None:
        provider.fetch.return_value = _make_envelope(candles, source=name)
    else:
        provider.fetch.return_value = _make_envelope(_make_candles(), source=name)

    return provider


# ---------------------------------------------------------------------------
# Tests: classify_instrument
# ---------------------------------------------------------------------------


class TestClassifyInstrument:
    """Tests for instrument CEX/DEX classification."""

    def test_cex_primary_weth_usdc(self):
        inst = Instrument(base="WETH", quote="USDC", chain="arbitrum")
        assert classify_instrument(inst) == "cex_primary"

    def test_cex_primary_wbtc_usdt(self):
        inst = Instrument(base="WBTC", quote="USDT", chain="ethereum")
        assert classify_instrument(inst) == "cex_primary"

    def test_cex_primary_link(self):
        inst = Instrument(base="LINK", quote="USDT", chain="arbitrum")
        assert classify_instrument(inst) == "cex_primary"

    def test_cex_primary_arb(self):
        inst = Instrument(base="ARB", quote="USDT", chain="arbitrum")
        assert classify_instrument(inst) == "cex_primary"

    def test_defi_primary_unknown_token(self):
        inst = Instrument(base="OBSCUREDEFI", quote="USDC", chain="base")
        assert classify_instrument(inst) == "defi_primary"

    def test_defi_primary_exotic_pair(self):
        inst = Instrument(base="AERODROME", quote="USDC", chain="base")
        assert classify_instrument(inst) == "defi_primary"

    def test_classification_is_case_insensitive(self):
        """Instrument normalizes to uppercase, so classification works."""
        inst = Instrument(base="weth", quote="usdc", chain="arbitrum")
        assert inst.base == "WETH"
        assert classify_instrument(inst) == "cex_primary"


class TestProviderChains:
    """Tests for provider chain ordering."""

    def test_cex_primary_chain(self):
        assert _PROVIDER_CHAINS["cex_primary"] == ["binance", "coingecko", "defillama"]

    def test_defi_primary_chain(self):
        assert _PROVIDER_CHAINS["defi_primary"] == ["geckoterminal", "defillama", "binance"]


# ---------------------------------------------------------------------------
# Tests: _split_by_finality
# ---------------------------------------------------------------------------


class TestSplitByFinality:
    """Tests for finalized vs provisional candle splitting."""

    def test_all_finalized(self):
        """Candles >24h old are all finalized."""
        now = datetime.now(UTC)
        candles = [
            _make_candle(now - timedelta(hours=48)),
            _make_candle(now - timedelta(hours=36)),
            _make_candle(now - timedelta(hours=25)),
        ]
        finalized, provisional = _split_by_finality(candles, now)
        assert len(finalized) == 3
        assert len(provisional) == 0

    def test_all_provisional(self):
        """Candles <24h old are all provisional."""
        now = datetime.now(UTC)
        candles = [
            _make_candle(now - timedelta(hours=12)),
            _make_candle(now - timedelta(hours=6)),
            _make_candle(now - timedelta(hours=1)),
        ]
        finalized, provisional = _split_by_finality(candles, now)
        assert len(finalized) == 0
        assert len(provisional) == 3

    def test_mixed_finality(self):
        """Mix of old and recent candles."""
        now = datetime.now(UTC)
        candles = [
            _make_candle(now - timedelta(hours=48)),
            _make_candle(now - timedelta(hours=25)),
            _make_candle(now - timedelta(hours=12)),
            _make_candle(now - timedelta(hours=1)),
        ]
        finalized, provisional = _split_by_finality(candles, now)
        assert len(finalized) == 2
        assert len(provisional) == 2

    def test_empty_candles(self):
        finalized, provisional = _split_by_finality([], datetime.now(UTC))
        assert finalized == []
        assert provisional == []

    def test_exactly_24h_is_provisional(self):
        """Candle exactly at the 24h boundary is provisional (not finalized)."""
        now = datetime.now(UTC)
        candle = _make_candle(now - timedelta(hours=24))
        finalized, provisional = _split_by_finality([candle], now)
        # exactly 24h ago = not strictly less than cutoff
        assert len(finalized) == 0
        assert len(provisional) == 1


# ---------------------------------------------------------------------------
# Tests: _OHLCVDiskCache
# ---------------------------------------------------------------------------


class TestOHLCVDiskCache:
    """Tests for disk cache operations."""

    def test_put_and_get(self, tmp_path):
        cache = _OHLCVDiskCache(cache_dir=tmp_path)
        candles = [
            _make_candle(datetime(2024, 1, 1, tzinfo=UTC)),
            _make_candle(datetime(2024, 1, 1, 1, tzinfo=UTC)),
        ]
        cache.put("test_key", candles)

        loaded = cache.get("test_key")
        assert loaded is not None
        assert len(loaded) == 2
        assert loaded[0].close == candles[0].close
        assert loaded[1].timestamp == candles[1].timestamp

    def test_get_nonexistent(self, tmp_path):
        cache = _OHLCVDiskCache(cache_dir=tmp_path)
        assert cache.get("nonexistent") is None

    def test_checksum_validation(self, tmp_path):
        """Corrupted cache entries are evicted."""
        cache = _OHLCVDiskCache(cache_dir=tmp_path)
        candles = [_make_candle(datetime(2024, 1, 1, tzinfo=UTC))]
        cache.put("test_key", candles)

        # Corrupt the checksum
        path = cache._key_path("test_key")
        data = json.loads(path.read_text())
        data["checksum"] = "corrupted"
        path.write_text(json.dumps(data))

        assert cache.get("test_key") is None
        # File should be evicted
        assert not path.exists()

    def test_empty_candles_not_cached(self, tmp_path):
        cache = _OHLCVDiskCache(cache_dir=tmp_path)
        cache.put("empty_key", [])
        assert cache.get("empty_key") is None

    def test_creates_directory(self, tmp_path):
        nested_dir = tmp_path / "nested" / "cache"
        cache = _OHLCVDiskCache(cache_dir=nested_dir)
        candles = [_make_candle(datetime(2024, 1, 1, tzinfo=UTC))]
        cache.put("key", candles)
        assert nested_dir.exists()

    def test_volume_none_roundtrip(self, tmp_path):
        """Candles with volume=None survive serialization."""
        cache = _OHLCVDiskCache(cache_dir=tmp_path)
        candle = OHLCVCandle(
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("90"),
            close=Decimal("105"),
            volume=None,
        )
        cache.put("vol_none", [candle])
        loaded = cache.get("vol_none")
        assert loaded is not None
        assert loaded[0].volume is None


# ---------------------------------------------------------------------------
# Tests: OHLCVRouter
# ---------------------------------------------------------------------------


class TestOHLCVRouter:
    """Tests for the main OHLCV routing logic."""

    def test_register_provider(self):
        router = OHLCVRouter()
        provider = _mock_provider("geckoterminal")
        router.register_provider(provider)
        assert "geckoterminal" in router._providers

    def test_routes_cex_primary_to_binance_first(self):
        """CEX-primary tokens should try Binance first."""
        router = OHLCVRouter()
        binance = _mock_provider("binance")
        gecko = _mock_provider("geckoterminal")
        router.register_provider(binance)
        router.register_provider(gecko)

        envelope = router.get_ohlcv("WETH/USDC", chain="arbitrum")
        assert envelope.meta.source == "binance"
        binance.fetch.assert_called_once()
        gecko.fetch.assert_not_called()

    def test_routes_defi_primary_to_gecko_first(self):
        """DeFi-primary tokens should try GeckoTerminal first."""
        router = OHLCVRouter()
        binance = _mock_provider("binance")
        gecko = _mock_provider("geckoterminal")
        router.register_provider(binance)
        router.register_provider(gecko)

        envelope = router.get_ohlcv("OBSCUREDEFI/USDC", chain="base")
        assert envelope.meta.source == "geckoterminal"
        gecko.fetch.assert_called_once()
        binance.fetch.assert_not_called()

    def test_fallback_on_primary_failure(self):
        """When primary provider fails, falls back to next in chain."""
        router = OHLCVRouter()
        gecko = _mock_provider("geckoterminal", fail=True)
        defillama = _mock_provider("defillama")
        router.register_provider(gecko)
        router.register_provider(defillama)

        # DeFi-primary: gecko -> defillama -> binance
        envelope = router.get_ohlcv("OBSCUREDEFI/USDC", chain="base")
        assert envelope.meta.source == "defillama"

    def test_all_providers_fail_raises(self):
        """When all providers fail, raises DataSourceUnavailable."""
        router = OHLCVRouter()
        gecko = _mock_provider("geckoterminal", fail=True)
        defillama = _mock_provider("defillama", fail=True)
        binance = _mock_provider("binance", fail=True)
        router.register_provider(gecko)
        router.register_provider(defillama)
        router.register_provider(binance)

        with pytest.raises(DataSourceUnavailable, match="All providers failed"):
            router.get_ohlcv("OBSCUREDEFI/USDC", chain="base")

    def test_force_provider_bypasses_classification(self):
        """force_provider should use the specified provider directly."""
        router = OHLCVRouter()
        gecko = _mock_provider("geckoterminal")
        binance = _mock_provider("binance")
        router.register_provider(gecko)
        router.register_provider(binance)

        # WETH would normally route to Binance, but force GeckoTerminal
        envelope = router.get_ohlcv("WETH/USDC", chain="arbitrum", force_provider="geckoterminal")
        assert envelope.meta.source == "geckoterminal"

    def test_cex_dex_basis_warning(self, caplog):
        """CEX source for DeFi-native pair reduces confidence to 0.7."""
        router = OHLCVRouter()
        # Only register Binance (skip gecko/defillama)
        binance = _mock_provider("binance")
        router.register_provider(binance)

        with caplog.at_level("WARNING", logger="almanak.framework.data.ohlcv.ohlcv_router"):
            envelope = router.get_ohlcv("OBSCUREDEFI/USDC", chain="base")

        assert envelope.meta.confidence <= 0.7
        assert "cex_dex_basis_warning" in caplog.text

    def test_no_basis_warning_for_cex_primary(self, caplog):
        """CEX source for CEX-primary pair does NOT reduce confidence."""
        router = OHLCVRouter()
        binance = _mock_provider("binance", candles=_make_candles())
        router.register_provider(binance)

        with caplog.at_level("WARNING", logger="almanak.framework.data.ohlcv.ohlcv_router"):
            envelope = router.get_ohlcv("WETH/USDC", chain="arbitrum")

        assert envelope.meta.confidence == 0.9  # Default from mock
        assert "cex_dex_basis_warning" not in caplog.text

    def test_empty_result_skips_provider(self):
        """Provider returning empty candles is skipped."""
        router = OHLCVRouter()
        gecko = _mock_provider("geckoterminal", candles=[])
        defillama = _mock_provider("defillama")
        router.register_provider(gecko)
        router.register_provider(defillama)

        envelope = router.get_ohlcv("OBSCUREDEFI/USDC", chain="base")
        assert envelope.meta.source == "defillama"

    def test_accepts_instrument_object(self):
        """OHLCVRouter accepts an Instrument directly."""
        router = OHLCVRouter()
        gecko = _mock_provider("geckoterminal")
        router.register_provider(gecko)

        inst = Instrument(base="MYTOKEN", quote="USDC", chain="base")
        envelope = router.get_ohlcv(inst)
        assert envelope.meta.source == "geckoterminal"

    def test_accepts_plain_string(self):
        """OHLCVRouter accepts a plain token string."""
        router = OHLCVRouter()
        binance = _mock_provider("binance")
        router.register_provider(binance)

        envelope = router.get_ohlcv("WETH", chain="arbitrum")
        assert envelope.meta.source == "binance"

    def test_default_chain_used(self):
        """When chain is not specified, default_chain is used."""
        router = OHLCVRouter(default_chain="base")
        gecko = _mock_provider("geckoterminal")
        router.register_provider(gecko)

        router.get_ohlcv("MYTOKEN/USDC")
        # Provider should be called with chain="base"
        call_kwargs = gecko.fetch.call_args
        assert call_kwargs.kwargs["chain"] == "base"

    def test_unregistered_provider_skipped(self):
        """Providers in the chain that aren't registered are skipped."""
        router = OHLCVRouter()
        # Only register Binance (last in defi chain)
        binance = _mock_provider("binance")
        router.register_provider(binance)

        envelope = router.get_ohlcv("OBSCUREDEFI/USDC", chain="base")
        assert envelope.meta.source == "binance"

    def test_disk_cache_used_for_finalized(self, tmp_path):
        """Finalized candles are stored in disk cache and reused."""
        router = OHLCVRouter(disk_cache_dir=tmp_path)
        now = datetime.now(UTC)
        # All candles >24h old (finalized)
        old_candles = [
            _make_candle(now - timedelta(hours=48), close=1800.0),
            _make_candle(now - timedelta(hours=36), close=1810.0),
            _make_candle(now - timedelta(hours=25), close=1820.0),
        ]
        gecko = _mock_provider("geckoterminal", candles=old_candles)
        router.register_provider(gecko)

        # First call: fetches from provider
        envelope1 = router.get_ohlcv("MYTOKEN/USDC", chain="base", limit=3)
        assert envelope1.meta.source == "geckoterminal"
        assert gecko.fetch.call_count == 1

        # Second call: should use disk cache
        envelope2 = router.get_ohlcv("MYTOKEN/USDC", chain="base", limit=3)
        assert envelope2.meta.source == "disk_cache"
        assert envelope2.meta.cache_hit is True
        # Provider not called again
        assert gecko.fetch.call_count == 1


class TestOHLCVRouterCacheBehavior:
    """Tests for cache finality tagging."""

    def test_finalized_candles_cached_to_disk(self, tmp_path):
        """Only finalized candles (>24h old) are written to disk cache."""
        router = OHLCVRouter(disk_cache_dir=tmp_path)
        now = datetime.now(UTC)
        candles = [
            _make_candle(now - timedelta(hours=48)),  # finalized
            _make_candle(now - timedelta(hours=1)),  # provisional
        ]
        gecko = _mock_provider("geckoterminal", candles=candles)
        router.register_provider(gecko)

        router.get_ohlcv("MYTOKEN/USDC", chain="base")

        # Disk cache should only contain the finalized candle
        disk_candles = router._disk_cache.get("MYTOKEN:USDC:base:1h:100:auto")
        assert disk_candles is not None
        assert len(disk_candles) == 1

    def test_provisional_candles_not_cached_to_disk(self, tmp_path):
        """All-provisional candles result in no disk cache write."""
        router = OHLCVRouter(disk_cache_dir=tmp_path)
        now = datetime.now(UTC)
        candles = [
            _make_candle(now - timedelta(hours=1)),
            _make_candle(now - timedelta(minutes=30)),
        ]
        gecko = _mock_provider("geckoterminal", candles=candles)
        router.register_provider(gecko)

        router.get_ohlcv("MYTOKEN/USDC", chain="base")

        disk_candles = router._disk_cache.get("MYTOKEN:USDC:base:1h:100:auto")
        assert disk_candles is None


# ---------------------------------------------------------------------------
# Tests: MarketSnapshot integration
# ---------------------------------------------------------------------------


class TestMarketSnapshotOHLCVRouter:
    """Tests for MarketSnapshot.ohlcv() with OHLCVRouter."""

    def test_ohlcv_routes_through_router(self):
        """ohlcv() uses OHLCVRouter when configured."""
        from almanak.framework.data.market_snapshot import MarketSnapshot

        candles = _make_candles(count=3, start_hours_ago=2)
        gecko = _mock_provider("geckoterminal", candles=candles)

        router = OHLCVRouter(default_chain="arbitrum")
        router.register_provider(gecko)

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x1234",
            ohlcv_router=router,
        )

        df = snapshot.ohlcv("MYTOKEN/USDC", timeframe="1h", limit=3)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ["timestamp", "open", "high", "low", "close", "volume"]
        assert df.attrs["source"] == "geckoterminal"
        assert "confidence" in df.attrs

    def test_ohlcv_accepts_instrument(self):
        """ohlcv() accepts an Instrument object."""
        from almanak.framework.data.market_snapshot import MarketSnapshot

        candles = _make_candles(count=2)
        gecko = _mock_provider("geckoterminal", candles=candles)

        router = OHLCVRouter()
        router.register_provider(gecko)

        snapshot = MarketSnapshot(
            chain="base",
            wallet_address="0x1234",
            ohlcv_router=router,
        )

        inst = Instrument(base="MYTOKEN", quote="USDC", chain="base")
        df = snapshot.ohlcv(inst)
        assert len(df) == 2
        assert df.attrs["base"] == "MYTOKEN"

    def test_ohlcv_falls_back_to_legacy_module(self):
        """When no OHLCVRouter, ohlcv() uses the legacy OHLCVModule."""
        from almanak.framework.data.market_snapshot import MarketSnapshot

        ohlcv_module = MagicMock()
        ohlcv_module.get_ohlcv.return_value = pd.DataFrame(
            {"timestamp": [], "open": [], "high": [], "low": [], "close": [], "volume": []}
        )

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x1234",
            ohlcv_module=ohlcv_module,
        )

        snapshot.ohlcv("WETH")
        ohlcv_module.get_ohlcv.assert_called_once()

    def test_ohlcv_no_module_no_router_raises(self):
        """With no OHLCVModule and no OHLCVRouter, ohlcv() raises ValueError."""
        from almanak.framework.data.market_snapshot import MarketSnapshot

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x1234",
        )

        with pytest.raises(ValueError, match="No OHLCV module or router configured"):
            snapshot.ohlcv("WETH")

    def test_ohlcv_router_error_raises_ohlcv_unavailable(self):
        """When all providers fail, ohlcv() raises OHLCVUnavailableError."""
        from almanak.framework.data.market_snapshot import MarketSnapshot, OHLCVUnavailableError

        router = OHLCVRouter()
        # No providers registered -> all will fail
        gecko = _mock_provider("geckoterminal", fail=True)
        defillama = _mock_provider("defillama", fail=True)
        binance = _mock_provider("binance", fail=True)
        router.register_provider(gecko)
        router.register_provider(defillama)
        router.register_provider(binance)

        snapshot = MarketSnapshot(
            chain="base",
            wallet_address="0x1234",
            ohlcv_router=router,
        )

        with pytest.raises(OHLCVUnavailableError):
            snapshot.ohlcv("OBSCUREDEFI/USDC")

    def test_ohlcv_empty_result_raises(self):
        """All providers returning empty candles raises OHLCVUnavailableError."""
        from almanak.framework.data.market_snapshot import MarketSnapshot, OHLCVUnavailableError

        gecko = _mock_provider("geckoterminal", candles=[])
        defillama = _mock_provider("defillama", candles=[])
        binance = _mock_provider("binance", candles=[])
        router = OHLCVRouter()
        router.register_provider(gecko)
        router.register_provider(defillama)
        router.register_provider(binance)

        snapshot = MarketSnapshot(
            chain="base",
            wallet_address="0x1234",
            ohlcv_router=router,
        )

        # All providers return empty -> router raises -> snapshot wraps
        with pytest.raises(OHLCVUnavailableError):
            snapshot.ohlcv("OBSCUREDEFI/USDC")

    def test_ohlcv_gap_strategy_ffill(self):
        """gap_strategy='ffill' forward-fills NaN volumes."""
        from almanak.framework.data.market_snapshot import MarketSnapshot

        now = datetime.now(UTC)
        candles = [
            OHLCVCandle(
                timestamp=now - timedelta(hours=2),
                open=Decimal("100"),
                high=Decimal("110"),
                low=Decimal("90"),
                close=Decimal("105"),
                volume=Decimal("1000"),
            ),
            OHLCVCandle(
                timestamp=now - timedelta(hours=1),
                open=Decimal("105"),
                high=Decimal("115"),
                low=Decimal("95"),
                close=Decimal("110"),
                volume=None,  # NaN volume
            ),
        ]
        gecko = _mock_provider("geckoterminal", candles=candles)
        router = OHLCVRouter()
        router.register_provider(gecko)

        snapshot = MarketSnapshot(
            chain="base",
            wallet_address="0x1234",
            ohlcv_router=router,
        )

        df = snapshot.ohlcv("MYTOKEN/USDC", gap_strategy="ffill")
        # After ffill, second row's volume should be filled
        assert not pd.isna(df.iloc[1]["volume"])

    def test_ohlcv_confidence_propagated_to_attrs(self):
        """DataMeta.confidence is propagated to DataFrame.attrs."""
        from almanak.framework.data.market_snapshot import MarketSnapshot

        candles = _make_candles(count=2)
        # Simulate CEX/DEX basis: binance as source for defi token
        binance = _mock_provider("binance", candles=candles)
        router = OHLCVRouter()
        router.register_provider(binance)

        snapshot = MarketSnapshot(
            chain="base",
            wallet_address="0x1234",
            ohlcv_router=router,
        )

        # DeFi-native token routed through Binance -> confidence reduced
        df = snapshot.ohlcv("OBSCUREDEFI/USDC")
        assert df.attrs["confidence"] <= 0.7


# ---------------------------------------------------------------------------
# Tests: Edge cases
# ---------------------------------------------------------------------------


class TestOHLCVRouterEdgeCases:
    """Tests for edge cases and error handling."""

    def test_no_providers_registered(self):
        """Router with no providers raises immediately."""
        router = OHLCVRouter()
        with pytest.raises(DataSourceUnavailable, match="All providers failed"):
            router.get_ohlcv("WETH/USDC", chain="arbitrum")

    def test_provider_returns_none_value(self):
        """Provider returning envelope with None value is treated as empty."""
        router = OHLCVRouter()
        provider = MagicMock()
        provider.name = "geckoterminal"
        provider.data_class = DataClassification.INFORMATIONAL
        provider.fetch.return_value = _make_envelope([], source="geckoterminal")
        router.register_provider(provider)

        binance = _mock_provider("binance")
        router.register_provider(binance)

        # defi_primary: gecko (empty) -> defillama (not registered) -> binance (success)
        envelope = router.get_ohlcv("OBSCUREDEFI/USDC", chain="base")
        assert envelope.meta.source == "binance"

    def test_single_token_resolved_with_default_quote(self):
        """Single token string gets USDC as default quote."""
        router = OHLCVRouter()
        binance = _mock_provider("binance")
        router.register_provider(binance)

        router.get_ohlcv("WETH", chain="arbitrum")
        # resolve_instrument("WETH", "arbitrum") -> Instrument(base="WETH", quote="USDC")
        call_kwargs = binance.fetch.call_args.kwargs
        assert call_kwargs["token"] == "WETH"

    def test_pool_address_passed_through(self):
        """pool_address kwarg is passed to provider.fetch()."""
        router = OHLCVRouter()
        gecko = _mock_provider("geckoterminal")
        router.register_provider(gecko)

        router.get_ohlcv("MYTOKEN/USDC", chain="base", pool_address="0xpool123")
        call_kwargs = gecko.fetch.call_args.kwargs
        assert call_kwargs["pool_address"] == "0xpool123"

    def test_timeframe_passed_through(self):
        """timeframe kwarg is passed to provider.fetch()."""
        router = OHLCVRouter()
        binance = _mock_provider("binance")
        router.register_provider(binance)

        router.get_ohlcv("WETH/USDC", chain="arbitrum", timeframe="4h")
        call_kwargs = binance.fetch.call_args.kwargs
        assert call_kwargs["timeframe"] == "4h"

    def test_limit_passed_through(self):
        """limit kwarg is passed to provider.fetch()."""
        router = OHLCVRouter()
        binance = _mock_provider("binance")
        router.register_provider(binance)

        router.get_ohlcv("WETH/USDC", chain="arbitrum", limit=50)
        call_kwargs = binance.fetch.call_args.kwargs
        assert call_kwargs["limit"] == 50
