"""Tests for OHLCV wrapped-token proxy fallback in OHLCVRouter."""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.models import (
    OHLCV_PROXY_MAP,
    DataClassification,
    DataEnvelope,
    DataMeta,
)
from almanak.framework.data.ohlcv.ohlcv_router import OHLCVRouter


def _make_candles(n: int = 5) -> list[OHLCVCandle]:
    """Create n dummy candles for testing."""
    return [
        OHLCVCandle(
            timestamp=datetime(2026, 1, 1, i, tzinfo=UTC),
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("90"),
            close=Decimal("105"),
            volume=Decimal("1000"),
        )
        for i in range(n)
    ]


def _make_envelope(candles: list[OHLCVCandle] | None = None, source: str = "binance") -> DataEnvelope:
    """Create a DataEnvelope wrapping candles."""
    if candles is None:
        candles = _make_candles()
    meta = DataMeta(
        source=source,
        observed_at=datetime.now(UTC),
        finality="off_chain",
        confidence=1.0,
    )
    return DataEnvelope(value=candles, meta=meta, classification=DataClassification.INFORMATIONAL)


def _make_failing_provider(name: str) -> MagicMock:
    """Create a provider mock that always raises DataSourceUnavailable."""
    provider = MagicMock()
    provider.name = name
    provider.fetch.side_effect = DataSourceUnavailable(source=name, reason="test failure")
    return provider


def _make_succeeding_provider(name: str, candles: list[OHLCVCandle] | None = None) -> MagicMock:
    """Create a provider mock that returns an envelope."""
    provider = MagicMock()
    provider.name = name
    provider.fetch.return_value = _make_envelope(candles, source=name)
    return provider


class TestOHLCVProxyMap:
    """Test OHLCV_PROXY_MAP contents and constraints."""

    def test_proxy_map_contains_wmnt(self):
        assert OHLCV_PROXY_MAP["WMNT"] == "MNT"

    def test_proxy_map_contains_expected_entries(self):
        expected = {"WMNT": "MNT", "WS": "S", "WXPL": "XPL", "WETH": "ETH", "WAVAX": "AVAX", "WMATIC": "MATIC", "WBNB": "BNB"}
        for wrapped, unwrapped in expected.items():
            assert OHLCV_PROXY_MAP.get(wrapped) == unwrapped, f"Missing or wrong: {wrapped} -> {unwrapped}"

    def test_proxy_map_does_not_contain_rebasing_tokens(self):
        """Ensure non-1:1 tokens are NOT in the proxy map."""
        forbidden = ["STETH", "WSTETH", "RETH", "CBETH"]
        for token in forbidden:
            assert token not in OHLCV_PROXY_MAP, f"{token} should not be in OHLCV_PROXY_MAP"


class TestProxyFallback:
    """Test OHLCVRouter proxy fallback behavior."""

    def test_proxy_fallback_triggers_on_failure(self):
        """When all providers fail for WMNT, router retries with MNT."""
        router = OHLCVRouter(default_chain="mantle")

        # Provider fails on WMNT, succeeds on MNT
        provider = MagicMock()
        provider.name = "binance"

        call_count = 0

        def selective_fetch(**kwargs):
            nonlocal call_count
            call_count += 1
            token = kwargs.get("token", "")
            if token == "WMNT":
                raise DataSourceUnavailable(source="binance", reason="Unknown token WMNT")
            return _make_envelope(source="binance")

        provider.fetch.side_effect = selective_fetch
        router.register_provider(provider)

        result = router.get_ohlcv("WMNT", chain="mantle")

        assert result is not None
        assert len(result.value) == 5
        assert result.meta.proxy_source == "WMNT"
        assert call_count == 2  # Once for WMNT (fail), once for MNT (success)

    def test_proxy_source_populated_in_meta(self):
        """DataMeta.proxy_source is set to original symbol when proxy is used."""
        router = OHLCVRouter(default_chain="mantle")

        provider = MagicMock()
        provider.name = "binance"

        def selective_fetch(**kwargs):
            token = kwargs.get("token", "")
            if token == "WMNT":
                raise DataSourceUnavailable(source="binance", reason="fail")
            return _make_envelope(source="binance")

        provider.fetch.side_effect = selective_fetch
        router.register_provider(provider)

        result = router.get_ohlcv("WMNT", chain="mantle")
        assert result.meta.proxy_source == "WMNT"

    def test_no_proxy_source_when_direct_succeeds(self):
        """DataMeta.proxy_source is None when data is fetched directly."""
        router = OHLCVRouter(default_chain="arbitrum")
        provider = _make_succeeding_provider("binance")
        router.register_provider(provider)

        result = router.get_ohlcv("WETH", chain="arbitrum")
        assert result.meta.proxy_source is None

    def test_no_fallback_when_no_proxy_entry(self):
        """Tokens not in OHLCV_PROXY_MAP raise immediately without retry."""
        router = OHLCVRouter(default_chain="arbitrum")
        provider = _make_failing_provider("binance")
        router.register_provider(provider)

        with pytest.raises(DataSourceUnavailable):
            router.get_ohlcv("LINK", chain="arbitrum")

        # Provider.fetch called once (no retry)
        assert provider.fetch.call_count == 1

    def test_no_infinite_recursion_when_proxy_also_fails(self):
        """If proxy token also fails, raise DataSourceUnavailable (no loop)."""
        router = OHLCVRouter(default_chain="mantle")
        provider = _make_failing_provider("binance")
        router.register_provider(provider)

        with pytest.raises(DataSourceUnavailable):
            router.get_ohlcv("WMNT", chain="mantle")

        # Provider.fetch called twice: WMNT (fail) -> MNT (fail) -> raise
        assert provider.fetch.call_count == 2

    def test_warning_logged_first_time_debug_thereafter(self, caplog):
        """First proxy use logs WARNING, subsequent logs DEBUG."""
        router = OHLCVRouter(default_chain="mantle")

        provider = MagicMock()
        provider.name = "binance"

        def selective_fetch(**kwargs):
            token = kwargs.get("token", "")
            if token == "WMNT":
                raise DataSourceUnavailable(source="binance", reason="fail")
            return _make_envelope(source="binance")

        provider.fetch.side_effect = selective_fetch
        router.register_provider(provider)

        # First call: should log WARNING
        with caplog.at_level(logging.DEBUG, logger="almanak.framework.data.ohlcv.ohlcv_router"):
            router.get_ohlcv("WMNT", chain="mantle")

        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING and "ohlcv_proxy_fallback" in r.message]
        assert len(warning_records) >= 1

        caplog.clear()

        # Second call: should log DEBUG, not WARNING
        with caplog.at_level(logging.DEBUG, logger="almanak.framework.data.ohlcv.ohlcv_router"):
            router.get_ohlcv("WMNT", chain="mantle")

        warning_records_2 = [r for r in caplog.records if r.levelno == logging.WARNING and "ohlcv_proxy_fallback" in r.message]
        debug_records = [r for r in caplog.records if r.levelno == logging.DEBUG and "ohlcv_proxy_fallback" in r.message]
        assert len(warning_records_2) == 0
        assert len(debug_records) >= 1

    def test_proxy_does_not_add_extra_confidence_penalty(self):
        """Proxy fallback itself should not add any confidence penalty beyond
        what the underlying provider chain already applies.

        The CEX/DEX basis risk penalty (0.7) may apply if the unwrapped proxy
        symbol is classified as defi_primary. That penalty is orthogonal to
        the proxy mechanism. We verify the proxy does not stack additional
        penalties on top of whatever the provider chain returns.
        """
        router = OHLCVRouter(default_chain="mantle")

        # Use a non-CEX provider to avoid the basis risk penalty entirely
        provider = MagicMock()
        provider.name = "geckoterminal"

        def selective_fetch(**kwargs):
            token = kwargs.get("token", "")
            if token == "WMNT":
                raise DataSourceUnavailable(source="geckoterminal", reason="fail")
            return _make_envelope(source="geckoterminal")

        provider.fetch.side_effect = selective_fetch
        router.register_provider(provider)

        result = router.get_ohlcv("WMNT", chain="mantle")
        # geckoterminal is not a CEX source, so no basis penalty applied
        # Proxy itself adds no penalty
        assert result.meta.confidence == 1.0
        assert result.meta.proxy_source == "WMNT"


class TestDataMetaProxySource:
    """Test DataMeta.proxy_source field."""

    def test_default_proxy_source_is_none(self):
        meta = DataMeta(source="binance", observed_at=datetime.now(UTC))
        assert meta.proxy_source is None

    def test_proxy_source_set_via_replace(self):
        meta = DataMeta(source="binance", observed_at=datetime.now(UTC))
        proxied = replace(meta, proxy_source="WMNT")
        assert proxied.proxy_source == "WMNT"
        assert proxied.source == "binance"  # Unchanged


class TestProxyMapConsistency:
    """Test that OHLCV_PROXY_MAP entries have valid proxy targets."""

    def test_proxy_targets_are_unwrapped_natives(self):
        """Every proxy target should be the unwrapped form of the key."""
        from almanak.framework.data.models import _NATIVE_TO_WRAPPED

        wrapped_to_native = {v: k for k, v in _NATIVE_TO_WRAPPED.items()}
        for wrapped, unwrapped in OHLCV_PROXY_MAP.items():
            assert wrapped in wrapped_to_native, f"{wrapped} not a known wrapped token"
            assert wrapped_to_native[wrapped] == unwrapped, (
                f"OHLCV_PROXY_MAP[{wrapped}] = {unwrapped}, "
                f"but _NATIVE_TO_WRAPPED[{unwrapped}] -> {_NATIVE_TO_WRAPPED.get(unwrapped)}"
            )

    def test_wmnt_has_coingecko_path(self):
        """MNT (proxy for WMNT) should be resolvable via CoinGecko."""
        from almanak.framework.data.tokens import get_coingecko_id

        cg_id = get_coingecko_id("MNT")
        assert cg_id is not None, "MNT should have a CoinGecko ID for OHLCV proxy to work"
