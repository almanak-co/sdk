"""Unit tests for OHLCV sad-flow hardening (VIB-3446, VIB-3447).

Covers:
- FR-2: DataSourceUnavailable reason surfaces primary (DEX) error, not only last_error.
- FR-3: classify_critical_data_failures returns "mixed" (not "permanent") when a
        combined string contains both a transient gRPC hint and "unknown token".
- FR-1: Bounded retry with backoff for DEX providers on transient errors; no retry
        for CEX providers; deterministic errors do not trigger retries.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
from almanak.framework.data.ohlcv.ohlcv_router import (
    OHLCVRouter,
    _is_transient_exc,
)
from almanak.framework.market import MarketSnapshot

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_ohlcv_cache(tmp_path, monkeypatch):
    """Give each test its own disk-cache directory.

    _make_candles produces timestamps in January 2026, which are all finalized
    (>24h old) and would be written to the shared default cache.  Without
    isolation, a successful test can pre-populate the cache and cause
    subsequent tests that expect provider calls to instead hit the cache,
    making call_count and exception assertions flaky.
    """
    monkeypatch.setattr(
        "almanak.framework.data.ohlcv.ohlcv_router._DEFAULT_CACHE_DIR",
        tmp_path,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_candles(n: int = 5) -> list[OHLCVCandle]:
    """Build a contiguous candle series ending at wall-clock "now".

    Timestamps are anchored to wall-clock so the youngest candle is fresh
    relative to the router's upstream-staleness guard (ALM-2697); the
    fixed-date fixture this replaces became stale once the project clock
    rolled past it.
    """
    now = datetime.now(UTC)
    return [
        OHLCVCandle(
            timestamp=now - timedelta(hours=n - 1 - i),
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("90"),
            close=Decimal("105"),
            volume=Decimal("1000"),
        )
        for i in range(n)
    ]


def _make_envelope(candles: list[OHLCVCandle] | None = None, source: str = "geckoterminal") -> DataEnvelope:
    if candles is None:
        candles = _make_candles()
    meta = DataMeta(
        source=source,
        observed_at=datetime.now(UTC),
        finality="off_chain",
        confidence=1.0,
    )
    return DataEnvelope(value=candles, meta=meta, classification=DataClassification.INFORMATIONAL)


def _failing_provider(name: str, error_msg: str = "test failure") -> MagicMock:
    p = MagicMock()
    p.name = name
    p.fetch.side_effect = DataSourceUnavailable(source=name, reason=error_msg)
    return p


def _succeeding_provider(name: str) -> MagicMock:
    p = MagicMock()
    p.name = name
    p.fetch.return_value = _make_envelope(source=name)
    return p


def _snapshot_with_failure(detail: str) -> MarketSnapshot:
    market = MarketSnapshot(chain="base", wallet_address="0xtest")
    # _record_critical_data_failure is the internal method that indicator helpers use
    market._record_critical_data_failure("ohlcv_router", "cbBTC", detail)
    return market


# ---------------------------------------------------------------------------
# FR-2: Error envelope surfaces primary DEX error
# ---------------------------------------------------------------------------


class TestErrorEnvelope:
    """DataSourceUnavailable reason leads with the primary (DEX) failure."""

    def test_primary_error_in_reason_when_gecko_and_binance_both_fail(self):
        """When Gecko fails then Binance fails, reason includes both errors."""
        router = OHLCVRouter(default_chain="base")
        gecko = _failing_provider("geckoterminal", "StatusCode.INTERNAL — GeckoTerminal OHLCV request failed")
        binance = _failing_provider("binance", "Unknown token for Binance: 0xcbbtc")
        router.register_provider(gecko)
        router.register_provider(binance)

        with patch("almanak.framework.data.ohlcv.ohlcv_router.time.sleep"):
            with pytest.raises(DataSourceUnavailable) as exc_info:
                router.get_ohlcv("cbBTC", chain="base")

        reason = exc_info.value.reason
        assert "StatusCode.INTERNAL" in reason or "GeckoTerminal" in reason, (
            f"Primary Gecko error not found in reason: {reason}"
        )
        assert "Unknown token for Binance" in reason, f"Last Binance error not found in reason: {reason}"

    def test_only_one_provider_fails_reason_is_flat(self):
        """When only one provider is registered and fails, reason is the plain error."""
        router = OHLCVRouter(default_chain="arbitrum")
        binance = _failing_provider("binance", "Unknown token for Binance: LINK")
        router.register_provider(binance)

        with pytest.raises(DataSourceUnavailable) as exc_info:
            router.get_ohlcv("LINK", chain="arbitrum")

        reason = exc_info.value.reason
        assert "Unknown token for Binance: LINK" in reason

    def test_primary_error_leads_reason(self):
        """The primary error text appears before the last error in the reason."""
        router = OHLCVRouter(default_chain="base")
        gecko = _failing_provider("geckoterminal", "GECKO_FAIL")
        binance = _failing_provider("binance", "BINANCE_FAIL")
        router.register_provider(gecko)
        router.register_provider(binance)

        with patch("almanak.framework.data.ohlcv.ohlcv_router.time.sleep"):
            with pytest.raises(DataSourceUnavailable) as exc_info:
                router.get_ohlcv("cbBTC", chain="base")

        reason = exc_info.value.reason
        assert reason.index("GECKO_FAIL") < reason.index("BINANCE_FAIL"), (
            "Primary error should appear before last error in reason"
        )


# ---------------------------------------------------------------------------
# FR-3: Classification fix — "mixed" not "permanent" for Gecko+Binance combo
# ---------------------------------------------------------------------------


class TestClassifyGeckoPlusBinanceCombined:
    """classify_critical_data_failures should return 'mixed', not 'permanent',
    when a combined reason string contains both a transient gRPC hint and
    a permanent 'unknown token' hint."""

    def test_grpc_internal_plus_unknown_token_is_mixed(self):
        """Gecko INTERNAL + Binance unknown token → classification is 'mixed'."""
        combined = (
            "All providers failed for cbBTC/USD on base"
            " — primary: Data source 'gateway_geckoterminal' unavailable:"
            " <RpcError status = StatusCode.INTERNAL details = 'GeckoTerminal OHLCV request failed'>;"
            " last: Data source 'gateway_ohlcv' unavailable: Unknown token for Binance: 0xcbbtc"
        )
        market = _snapshot_with_failure(combined)
        classification = market.classify_critical_data_failures()
        assert classification == "mixed", f"Expected 'mixed' for Gecko+Binance combined error, got '{classification}'"

    def test_unknown_token_only_is_permanent(self):
        """A clean 'unknown token' error with no transient hints → 'permanent'."""
        market = _snapshot_with_failure("Unknown token for Binance: 0xcbbtc")
        assert market.classify_critical_data_failures() == "permanent"

    def test_grpc_internal_only_is_transient(self):
        """A pure gRPC INTERNAL error with no permanent hints → 'transient'."""
        market = _snapshot_with_failure("StatusCode.INTERNAL — GeckoTerminal OHLCV request failed")
        assert market.classify_critical_data_failures() == "transient"

    def test_grpc_unavailable_only_is_transient(self):
        market = _snapshot_with_failure("StatusCode.UNAVAILABLE — upstream timeout")
        assert market.classify_critical_data_failures() == "transient"

    def test_two_separate_failures_gecko_transient_binance_permanent(self):
        """Two separate recorded failures (one each) → 'mixed'."""
        market = MarketSnapshot(chain="base", wallet_address="0xtest")
        market._record_critical_data_failure("ohlcv_router", "cbBTC_primary", "StatusCode.INTERNAL: GeckoTerminal blip")
        market._record_critical_data_failure("ohlcv_router", "cbBTC_last", "Unknown token for Binance: 0xcbbtc")
        assert market.classify_critical_data_failures() == "mixed"

    def test_empty_failures_returns_none(self):
        market = MarketSnapshot(chain="base", wallet_address="0xtest")
        assert market.classify_critical_data_failures() == "none"

    def test_wrapped_dsu_unknown_token_is_permanent(self):
        """DataSourceUnavailable wrapping 'unknown token' must classify as 'permanent'.

        When _record_critical_data_failure receives a DataSourceUnavailable, it stores
        str(exc) which includes the boilerplate 'Data source X unavailable: <reason>'.
        The word 'unavailable' in the boilerplate must NOT trigger a transient hit.
        """
        market = MarketSnapshot(chain="base", wallet_address="0xtest")
        market._record_critical_data_failure(
            "ohlcv",
            "cbBTC",
            DataSourceUnavailable(source="gateway_ohlcv", reason="Unknown token for Binance: cbBTC"),
        )
        assert market.classify_critical_data_failures() == "permanent", (
            "A DSU wrapping 'unknown token' must be 'permanent', not 'mixed' — "
            "the 'unavailable' boilerplate must not trigger a false transient hit"
        )


# ---------------------------------------------------------------------------
# FR-1: Retry with backoff for DEX providers
# ---------------------------------------------------------------------------


class TestRetryLogic:
    """OHLCVRouter retries DEX providers on transient errors."""

    def test_retry_succeeds_on_second_attempt(self):
        """A transient Gecko failure on attempt 1 is retried; attempt 2 succeeds."""
        router = OHLCVRouter(default_chain="base")

        gecko = MagicMock()
        gecko.name = "geckoterminal"
        gecko.fetch.side_effect = [
            DataSourceUnavailable(
                source="geckoterminal",
                reason="StatusCode.INTERNAL: GeckoTerminal OHLCV request failed",
            ),
            _make_envelope(source="geckoterminal"),
        ]
        router.register_provider(gecko)

        with patch("almanak.framework.data.ohlcv.ohlcv_router.time.sleep"):
            result = router.get_ohlcv("cbBTC", chain="base")

        assert result.meta.source == "geckoterminal"
        assert gecko.fetch.call_count == 2

    def test_retry_exhausted_falls_through_to_binance(self):
        """Gecko fails all retry attempts → falls through to Binance."""
        router = OHLCVRouter(default_chain="base")

        transient_err = DataSourceUnavailable(
            source="geckoterminal",
            reason="StatusCode.INTERNAL: GeckoTerminal OHLCV request failed",
        )
        gecko = MagicMock()
        gecko.name = "geckoterminal"
        gecko.fetch.side_effect = [transient_err, transient_err, transient_err]

        binance = _succeeding_provider("binance")
        router.register_provider(gecko)
        router.register_provider(binance)

        with patch("almanak.framework.data.ohlcv.ohlcv_router.time.sleep"):
            # cbBTC is defi_primary → chain is [geckoterminal, defillama, binance]
            result = router.get_ohlcv("cbBTC", chain="base")

        assert result.meta.source == "binance"
        assert gecko.fetch.call_count == 3  # 1 original + 2 retries

    def test_no_retry_for_deterministic_errors(self):
        """A non-transient (permanent) error on Gecko is NOT retried."""
        router = OHLCVRouter(default_chain="base")

        gecko = _failing_provider("geckoterminal", "Pool not found for this token")
        router.register_provider(gecko)

        with patch("almanak.framework.data.ohlcv.ohlcv_router.time.sleep") as mock_sleep:
            with pytest.raises(DataSourceUnavailable):
                router.get_ohlcv("cbBTC", chain="base")

        assert gecko.fetch.call_count == 1  # No retry for permanent error
        mock_sleep.assert_not_called()

    def test_no_retry_for_cex_providers(self):
        """Binance (CEX) never gets retried even on transient-looking errors."""
        router = OHLCVRouter(default_chain="arbitrum")

        # Force binance directly to isolate the retry behaviour (bypass token
        # classification and proxy fallback, which can add extra calls).
        binance = _failing_provider("binance", "StatusCode.INTERNAL: server error")
        router.register_provider(binance)

        with patch("almanak.framework.data.ohlcv.ohlcv_router.time.sleep") as mock_sleep:
            with pytest.raises(DataSourceUnavailable):
                router.get_ohlcv("cbBTC", chain="base", force_provider="binance")

        assert binance.fetch.call_count == 1  # CEX: one attempt only
        mock_sleep.assert_not_called()

    def test_backoff_sleep_called_between_retries(self):
        """time.sleep is called once between the first and second attempt."""
        router = OHLCVRouter(default_chain="base")

        transient_err = DataSourceUnavailable(
            source="geckoterminal",
            reason="StatusCode.INTERNAL: blip",
        )
        gecko = MagicMock()
        gecko.name = "geckoterminal"
        gecko.fetch.side_effect = [transient_err, _make_envelope(source="geckoterminal")]
        router.register_provider(gecko)

        with patch("almanak.framework.data.ohlcv.ohlcv_router.time.sleep") as mock_sleep:
            router.get_ohlcv("cbBTC", chain="base")

        assert mock_sleep.call_count == 1
        delay = mock_sleep.call_args[0][0]
        assert 0 <= delay <= 2.0  # within jitter cap


# ---------------------------------------------------------------------------
# _is_transient_exc helper
# ---------------------------------------------------------------------------


class TestIsTransientExc:
    """Unit tests for the transient-detection helper."""

    @pytest.mark.parametrize(
        "reason",
        [
            "StatusCode.INTERNAL: GeckoTerminal OHLCV request failed",
            "StatusCode.UNAVAILABLE: upstream blip",
            "StatusCode.RESOURCE_EXHAUSTED: rate limited",
            "StatusCode.DEADLINE_EXCEEDED: timed out",
            "connection reset by peer",
            "Request timed out",
            "429 Too Many Requests",
            "service unavailable",
        ],
    )
    def test_transient_reasons_detected(self, reason: str):
        exc = DataSourceUnavailable(source="x", reason=reason)
        assert _is_transient_exc(exc) is True

    @pytest.mark.parametrize(
        "reason",
        [
            "Unknown token for Binance: 0x123",
            "Pool not found",
            "unsupported chain",
        ],
    )
    def test_permanent_reasons_not_transient(self, reason: str):
        exc = DataSourceUnavailable(source="x", reason=reason)
        assert _is_transient_exc(exc) is False

    def test_plain_exception_uses_str(self):
        """For non-DataSourceUnavailable exceptions, str() is checked."""
        exc = RuntimeError("StatusCode.INTERNAL inside a raw exception")
        assert _is_transient_exc(exc) is True
