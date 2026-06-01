"""Tests for the DEX quiet-pool liveness backstop (fix A).

A DEX pool with no recent swaps returns *stale* (not absent) trade-derived
OHLCV, so swap-based indicators (RSI, …) can't be computed — but the asset is
still continuously priceable from the 24/7 aggregated oracle. Holding through
that is benign and must NOT be escalated to a DATA_ERROR (which trips the
circuit breaker). ``MarketSnapshot.is_quiet_pool_hold()`` is the gate: every
recorded critical failure must be a quiet-pool staleness miss AND the affected
token must still be priceable.

This is the NVDAON/USD incident: geckoterminal returned stale OHLCV (no weekend
swaps), the Binance fallback can't price a tokenized stock, RSI failed — yet the
pool was alive and priceable the whole time.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.market import MarketSnapshot

# The exact router phrasing (ohlcv_router._build_stale_response_miss), wrapped by
# the snapshot's AllDataSourcesFailed boilerplate + Binance fallback, as recorded
# into _critical_data_failures during the NVDAON incident.
_NVDAON_STALE_DETAIL = (
    "Data source 'ohlcv_router' unavailable: All providers failed for NVDAON/USD on ethereum "
    "— primary: geckoterminal returned stale OHLCV for NVDAON/USD on ethereum: youngest candle "
    "is 87288s behind wall-clock (budget 86400s for timeframe 1h); treating as provider miss; "
    "last: Unknown token for Binance: NVDAON"
)


def _snapshot(price_oracle):
    return MarketSnapshot(chain="ethereum", wallet_address="0xtest", price_oracle=price_oracle)


class TestIsQuietPoolHold:
    def test_stale_but_priceable_is_benign(self) -> None:
        # Oracle prices NVDAON 24/7 → pool is alive, just quiet → benign HOLD.
        market = _snapshot(lambda token, quote="USD", chain=None: Decimal("178.50"))
        market._record_critical_data_failure("rsi", "('NVDAON', '1h', 14)", _NVDAON_STALE_DETAIL)

        assert market.has_critical_data_failures()
        assert market.is_quiet_pool_hold() is True

    def test_stale_and_not_priceable_escalates(self) -> None:
        # Oracle also can't price it → genuinely dark → stays critical (escalate).
        def dead_oracle(token, quote="USD", chain=None):
            raise ValueError(f"Unknown token: {token}")

        market = _snapshot(dead_oracle)
        market._record_critical_data_failure("rsi", "('NVDAON', '1h', 14)", _NVDAON_STALE_DETAIL)

        assert market.is_quiet_pool_hold() is False

    def test_zero_price_is_not_priceable(self) -> None:
        # A 0 price is not a liveness signal — treat as not priceable.
        market = _snapshot(lambda token, quote="USD", chain=None: Decimal("0"))
        market._record_critical_data_failure("rsi", "('NVDAON', '1h', 14)", _NVDAON_STALE_DETAIL)

        assert market.is_quiet_pool_hold() is False

    def test_no_failures_returns_false(self) -> None:
        market = _snapshot(lambda token, quote="USD", chain=None: Decimal("1"))
        assert market.is_quiet_pool_hold() is False

    def test_non_quiet_failure_escalates_even_if_priceable(self) -> None:
        # A hard failure (no "returned stale OHLCV" phrase) is never benign,
        # regardless of priceability.
        market = _snapshot(lambda token, quote="USD", chain=None: Decimal("178.50"))
        market._record_critical_data_failure(
            "rsi",
            "('NVDAON', '1h', 14)",
            "Data source 'ohlcv_router' unavailable: Unknown token for Binance: NVDAON",
        )
        assert market.is_quiet_pool_hold() is False

    def test_any_non_quiet_failure_in_the_set_escalates(self) -> None:
        # One quiet-pool stale (priceable) + one genuine failure → escalate.
        market = _snapshot(lambda token, quote="USD", chain=None: Decimal("178.50"))
        market._record_critical_data_failure("rsi", "('NVDAON', '1h', 14)", _NVDAON_STALE_DETAIL)
        market._record_critical_data_failure("price", "WETH/USD@ethereum", "gateway oracle exploded")
        assert market.is_quiet_pool_hold() is False

    def test_multiple_quiet_pools_all_priceable(self) -> None:
        prices = {"NVDAON": Decimal("178.50"), "AAPLON": Decimal("212.10")}
        market = _snapshot(lambda token, quote="USD", chain=None: prices[token])
        market._record_critical_data_failure("rsi", "('NVDAON', '1h', 14)", _NVDAON_STALE_DETAIL)
        market._record_critical_data_failure(
            "rsi",
            "('AAPLON', '1h', 14)",
            "geckoterminal returned stale OHLCV for AAPLON/USD on ethereum: youngest candle is "
            "90000s behind wall-clock (budget 86400s for timeframe 1h); treating as provider miss",
        )
        assert market.is_quiet_pool_hold() is True

    def test_probe_never_raises(self) -> None:
        # Oracle raising an unexpected error type must be swallowed (probe → False).
        def boom(token, quote="USD", chain=None):
            raise RuntimeError("oracle connection reset")

        market = _snapshot(boom)
        market._record_critical_data_failure("rsi", "('NVDAON', '1h', 14)", _NVDAON_STALE_DETAIL)
        # Must not raise, just escalate.
        assert market.is_quiet_pool_hold() is False

    def test_hyphenated_chain_is_priceable_and_benign(self) -> None:
        # Capture happens via the hyphen-tolerant regex; the probe must receive
        # the full chain name "arbitrum-one", not a truncated "arbitrum".
        seen: dict[str, str | None] = {}

        def oracle(token, quote="USD", chain=None):
            seen["chain"] = chain
            return Decimal("178.50")

        market = _snapshot(oracle)
        market._record_critical_data_failure(
            "rsi",
            "('FOO', '1h', 14)",
            "geckoterminal returned stale OHLCV for FOO/USD on arbitrum-one: youngest candle is "
            "90000s behind wall-clock (budget 86400s for timeframe 1h); treating as provider miss",
        )
        assert market.is_quiet_pool_hold() is True
        assert seen["chain"] == "arbitrum-one"


def test_quiet_pool_regex_captures_hyphenated_chain() -> None:
    from almanak.framework.market.snapshot import _QUIET_POOL_STALE_RE

    m = _QUIET_POOL_STALE_RE.search(
        "geckoterminal returned stale OHLCV for WBTC/USD on polygon-zkevm: youngest candle ..."
    )
    assert m is not None
    assert m.group("base") == "WBTC"
    assert m.group("chain") == "polygon-zkevm"
