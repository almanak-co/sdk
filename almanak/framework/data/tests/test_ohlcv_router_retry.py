"""Tests for VIB-3802: router retry-after handling.

Covers:
- ``_is_retryable_exc`` recognises typed VIB-3800 exceptions.
- ``_extract_upstream_retry_delay`` reads ``retry_after`` from typed exceptions.
- ``_retry_delay_for`` honors upstream-advised retry delay (capped) and falls
  back to full-jitter backoff when no advice is available.
- The router's retry loop sleeps for the upstream-advised duration when the
  failure carries a ``RetryInfo`` payload, and falls back to jitter otherwise.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.interfaces import (
    DataSourceRateLimited,
    DataSourceTimeout,
    DataSourceUnavailable,
    OHLCVCandle,
)
from almanak.framework.data.models import (
    DataClassification,
    DataEnvelope,
    DataMeta,
)
from almanak.framework.data.ohlcv import ohlcv_router
from almanak.framework.data.ohlcv.ohlcv_router import (
    _UPSTREAM_RETRY_MAX_DELAY,
    OHLCVRouter,
    _backoff_delay,
    _extract_upstream_retry_delay,
    _is_retryable_exc,
    _retry_delay_for,
)

# ---------------------------------------------------------------------------
# _is_retryable_exc
# ---------------------------------------------------------------------------


class TestIsRetryableExc:
    def test_data_source_rate_limited(self) -> None:
        assert _is_retryable_exc(DataSourceRateLimited("upstream", retry_after=1.0))

    def test_data_source_timeout(self) -> None:
        assert _is_retryable_exc(DataSourceTimeout("upstream", timeout_seconds=10.0))

    def test_data_source_unavailable_with_retry_after_is_retryable(self) -> None:
        # Typed signal from VIB-3800: gateway populated retry_after → transient.
        assert _is_retryable_exc(
            DataSourceUnavailable("upstream", reason="down", retry_after=1.0),
        )

    def test_data_source_unavailable_with_transient_hint_is_retryable(self) -> None:
        # Legacy: no typed signal, but the reason matches the transient hint
        # heuristic → retry. Preserves backwards compat with un-migrated paths.
        assert _is_retryable_exc(
            DataSourceUnavailable("upstream", reason="connection reset"),
        )

    def test_data_source_unavailable_with_permanent_reason_is_not_retryable(self) -> None:
        # Critical regression guard: a DSU raised from client-side validation
        # ("Unknown token", "Pool not found", "unsupported chain") must NOT
        # be retried — that wastes the retry budget on a deterministic failure.
        assert not _is_retryable_exc(
            DataSourceUnavailable("upstream", reason="Unknown token for Binance: 0x123"),
        )
        assert not _is_retryable_exc(
            DataSourceUnavailable("upstream", reason="Pool not found"),
        )
        assert not _is_retryable_exc(
            DataSourceUnavailable("upstream", reason="unsupported chain: foobar"),
        )

    def test_legacy_string_match_still_works(self) -> None:
        # Non-typed exception with a transient hint in the message — falls
        # back to the legacy heuristic for backwards compat with un-migrated
        # paths.
        assert _is_retryable_exc(RuntimeError("rate limit exceeded"))
        assert _is_retryable_exc(RuntimeError("StatusCode.UNAVAILABLE: …"))

    def test_non_transient_exception_skipped(self) -> None:
        assert not _is_retryable_exc(ValueError("bad input"))


# ---------------------------------------------------------------------------
# _extract_upstream_retry_delay
# ---------------------------------------------------------------------------


class TestExtractUpstreamRetryDelay:
    def test_rate_limited_with_retry_after(self) -> None:
        exc = DataSourceRateLimited("upstream", retry_after=2.5)
        assert _extract_upstream_retry_delay(exc) == pytest.approx(2.5)

    def test_timeout_with_retry_after(self) -> None:
        exc = DataSourceTimeout("upstream", timeout_seconds=10.0, retry_after=1.0)
        assert _extract_upstream_retry_delay(exc) == pytest.approx(1.0)

    def test_unavailable_with_retry_after(self) -> None:
        exc = DataSourceUnavailable("upstream", reason="x", retry_after=0.5)
        assert _extract_upstream_retry_delay(exc) == pytest.approx(0.5)

    def test_unavailable_without_retry_after_returns_none(self) -> None:
        exc = DataSourceUnavailable("upstream", reason="x")
        assert _extract_upstream_retry_delay(exc) is None

    def test_unrelated_exception_returns_none(self) -> None:
        assert _extract_upstream_retry_delay(RuntimeError("x")) is None

    def test_none_returns_none(self) -> None:
        assert _extract_upstream_retry_delay(None) is None


# ---------------------------------------------------------------------------
# _retry_delay_for
# ---------------------------------------------------------------------------


class TestRetryDelayFor:
    def test_honors_upstream_retry_after(self) -> None:
        exc = DataSourceRateLimited("upstream", retry_after=1.5)
        assert _retry_delay_for(exc, attempt=1) == pytest.approx(1.5)

    def test_caps_excessive_upstream_retry_after(self) -> None:
        # Upstream advises 60s. We cap at _UPSTREAM_RETRY_MAX_DELAY because
        # any longer is breaker territory, not retry territory.
        exc = DataSourceRateLimited("upstream", retry_after=60.0)
        assert _retry_delay_for(exc, attempt=1) == pytest.approx(_UPSTREAM_RETRY_MAX_DELAY)

    def test_falls_back_to_backoff_without_advice(self) -> None:
        exc = DataSourceUnavailable("upstream", reason="down")  # no retry_after
        with patch.object(ohlcv_router.random, "uniform", return_value=0.42):
            assert _retry_delay_for(exc, attempt=1) == pytest.approx(0.42)

    def test_falls_back_to_backoff_for_legacy_exception(self) -> None:
        with patch.object(ohlcv_router.random, "uniform", return_value=0.13):
            assert _retry_delay_for(RuntimeError("rate limited"), attempt=1) == pytest.approx(0.13)

    def test_zero_retry_after_is_honored(self) -> None:
        # retry_after=0 is a valid signal ("retry immediately"); not the same
        # as None ("no advice").
        exc = DataSourceRateLimited("upstream", retry_after=0.0)
        assert _retry_delay_for(exc, attempt=1) == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Backoff bounds preserved (regression guard)
# ---------------------------------------------------------------------------


class TestBackoffBounds:
    def test_backoff_within_default_cap(self) -> None:
        for attempt in range(1, 6):
            delay = _backoff_delay(attempt)
            assert 0 <= delay <= 2.0  # _RETRY_MAX_DELAY


# ---------------------------------------------------------------------------
# Router-level: retry honors RetryInfo end-to-end
# ---------------------------------------------------------------------------


def _candle_now() -> OHLCVCandle:
    return OHLCVCandle(
        timestamp=datetime.now(UTC),
        open=Decimal("1"),
        high=Decimal("1"),
        low=Decimal("1"),
        close=Decimal("1"),
        volume=Decimal("0"),
    )


def _envelope_with(candles: list[OHLCVCandle], source: str) -> DataEnvelope[list[OHLCVCandle]]:
    return DataEnvelope(
        value=candles,
        meta=DataMeta(
            source=source,
            observed_at=datetime.now(UTC),
            finality="off_chain",
            staleness_ms=0,
            latency_ms=10,
            confidence=0.9,
            cache_hit=False,
        ),
        classification=DataClassification.INFORMATIONAL,
    )


class TestRouterRetryConsumesUpstreamRetryAfter:
    """End-to-end: a transient failure with RetryInfo ⇒ router sleeps for the
    advised duration, then succeeds on the retry."""

    def test_router_sleeps_for_upstream_advised_delay(self) -> None:
        provider = MagicMock()
        provider.name = "geckoterminal"
        provider.data_class = DataClassification.INFORMATIONAL

        candles = [_candle_now()]
        # First call raises with retry_after=1.7; second call succeeds.
        provider.fetch.side_effect = [
            DataSourceRateLimited(source="geckoterminal", retry_after=1.7),
            _envelope_with(candles, source="geckoterminal"),
        ]

        router = OHLCVRouter()
        router.register_provider(provider)

        sleeps: list[float] = []
        with patch.object(ohlcv_router.time, "sleep", side_effect=sleeps.append):
            envelope = router.get_ohlcv("UNITLESS", chain="arbitrum", timeframe="1h", limit=1)

        assert envelope.meta.source == "geckoterminal"
        assert provider.fetch.call_count == 2
        # Exactly one sleep, exactly the advised value (no jitter applied).
        assert sleeps == [pytest.approx(1.7)]

    def test_router_caps_upstream_advised_delay(self) -> None:
        provider = MagicMock()
        provider.name = "geckoterminal"
        provider.data_class = DataClassification.INFORMATIONAL

        candles = [_candle_now()]
        provider.fetch.side_effect = [
            DataSourceRateLimited(source="geckoterminal", retry_after=999.0),
            _envelope_with(candles, source="geckoterminal"),
        ]

        router = OHLCVRouter()
        router.register_provider(provider)

        sleeps: list[float] = []
        with patch.object(ohlcv_router.time, "sleep", side_effect=sleeps.append):
            router.get_ohlcv("UNITLESS", chain="arbitrum", timeframe="1h", limit=1)

        assert sleeps == [pytest.approx(_UPSTREAM_RETRY_MAX_DELAY)]

    def test_router_falls_back_to_backoff_without_advice(self) -> None:
        # First call raises DataSourceUnavailable with a transient-hint reason
        # but NO retry_after — exercises the legacy heuristic + jitter fallback
        # path. Second call succeeds.
        provider = MagicMock()
        provider.name = "geckoterminal"
        provider.data_class = DataClassification.INFORMATIONAL

        candles = [_candle_now()]
        provider.fetch.side_effect = [
            DataSourceUnavailable(source="geckoterminal", reason="connection reset"),
            _envelope_with(candles, source="geckoterminal"),
        ]

        router = OHLCVRouter()
        router.register_provider(provider)

        sleeps: list[float] = []
        # Force the jitter to a known value so we can assert.
        with (
            patch.object(ohlcv_router.time, "sleep", side_effect=sleeps.append),
            patch.object(ohlcv_router.random, "uniform", return_value=0.33),
        ):
            router.get_ohlcv("UNITLESS", chain="arbitrum", timeframe="1h", limit=1)

        assert sleeps == [pytest.approx(0.33)]
