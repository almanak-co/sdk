"""Tests for the VIB-4841 / FR-5002 stablecoin peg fast-path and the T1
CoinGecko cooldown failover, both in ``PriceAggregator``.

FR-5002 — stablecoin peg fast-path:
    A stable/USD pair returns the $1.00 peg immediately WITHOUT any upstream
    price call (the aggregate returns ~$1.00 anyway after outlier-discarding).
    Disabled by ``stablecoin_verify=True``. A low-frequency, latency-bounded
    on-chain Chainlink sanity check runs to surface a de-peg. VIB-4841 (Codex
    review): when that check COMPLETES and detects a de-peg, the fast-path FAILS
    CLOSED — it returns the live on-chain price instead of $1.00, or falls
    through to the full aggregate if the live price is unusable. A check that
    times out / cannot run still returns the peg best-effort.

T1 — CoinGecko cooldown failover:
    When CoinGecko fails fast with ``DataSourceRateLimited`` (its 429 cooldown
    is open), the aggregator must still return a price from a healthy peer
    (Binance) — failover stays intact, and the aggregate resolves at the
    healthy source's latency, not behind the rate-limited one.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.data.interfaces import (
    BasePriceSource,
    DataSourceRateLimited,
    PriceResult,
)
from almanak.gateway.data.price.aggregator import PriceAggregator


class _Source(BasePriceSource):
    """Configurable mock price source.

    ``raises`` takes precedence over ``price``. ``record_calls`` lets a test
    assert whether the source was hit at all (to prove the fast-path skipped
    upstream). ``price`` may be reassigned mid-test to simulate a stable
    drifting off peg and back (de-peg / re-peg).
    """

    def __init__(
        self,
        name: str,
        *,
        price: Decimal | None = None,
        raises: Exception | None = None,
        delay: float = 0.0,
    ) -> None:
        self._name = name
        self.price = price
        self._raises = raises
        self._delay = delay
        self.calls: list[str] = []

    @property
    def source_name(self) -> str:
        return self._name

    async def get_price(self, token: str, quote: str = "USD", **kwargs) -> PriceResult:
        self.calls.append(token)
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._raises is not None:
            raise self._raises
        assert self.price is not None
        return PriceResult(
            price=self.price,
            source=self._name,
            timestamp=datetime.now(UTC),
            confidence=0.95,
        )

    async def close(self) -> None:
        pass


# =============================================================================
# FR-5002 — stablecoin peg fast-path
# =============================================================================


class TestStablecoinPegFastPath:
    @pytest.mark.asyncio
    async def test_stable_usd_returns_peg_without_upstream_call(self) -> None:
        """A stable/USD pair returns $1.00 from the fast-path and NONE of the
        upstream sources are queried — that's the per-iteration cost cut."""
        cg = _Source("coingecko", price=Decimal("0.999"))
        binance = _Source("binance", price=Decimal("1.001"))
        aggregator = PriceAggregator(sources=[binance, cg])

        result = await aggregator.get_aggregated_price("USDC", "USD")

        assert result.price == Decimal("1.00")
        assert result.source == "stablecoin_peg"
        # Critical: no upstream price call for either source.
        assert cg.calls == []
        assert binance.calls == []

    @pytest.mark.asyncio
    async def test_non_stable_token_skips_fast_path(self) -> None:
        """A non-stablecoin token must NOT hit the fast-path — it goes through
        the normal aggregate."""
        cg = _Source("coingecko", price=Decimal("2500"))
        aggregator = PriceAggregator(sources=[cg])

        result = await aggregator.get_aggregated_price("WETH", "USD")

        assert result.price == Decimal("2500")
        assert result.source != "stablecoin_peg"
        assert cg.calls == ["WETH"]

    @pytest.mark.asyncio
    async def test_non_usd_quote_skips_fast_path(self) -> None:
        """USDC/EUR is not a USD peg, so the fast-path must not fire."""
        cg = _Source("coingecko", price=Decimal("0.92"))
        aggregator = PriceAggregator(sources=[cg])

        result = await aggregator.get_aggregated_price("USDC", "EUR")

        assert result.source != "stablecoin_peg"
        assert cg.calls == ["USDC"]

    @pytest.mark.asyncio
    async def test_stablecoin_verify_disables_fast_path(self) -> None:
        """With ``stablecoin_verify=True`` the fast-path is off: the full
        multi-source aggregate runs and the upstream sources ARE queried."""
        cg = _Source("coingecko", price=Decimal("1.0"))
        aggregator = PriceAggregator(sources=[cg], stablecoin_verify=True)

        result = await aggregator.get_aggregated_price("USDC", "USD")

        assert result.source != "stablecoin_peg"
        assert cg.calls == ["USDC"]


class TestStablecoinPegChainlinkSanityCheck:
    @pytest.mark.asyncio
    async def test_periodic_chainlink_check_runs_on_first_call(self) -> None:
        """The on-chain sanity check runs on the first peg-served call (1-in-N
        with N anchored to call #1) and again every Nth call thereafter — even
        though the peg is what's returned to the caller."""
        onchain = _Source("onchain", price=Decimal("1.0"))
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=3,
        )

        # Call #1 -> check runs; #2,#3 -> skip; #4 -> check runs again.
        for _ in range(4):
            result = await aggregator.get_aggregated_price("USDC", "USD")
            assert result.source == "stablecoin_peg"

        # Sanity check hit the on-chain source on calls #1 and #4 only.
        assert onchain.calls == ["USDC", "USDC"]

    @pytest.mark.asyncio
    async def test_check_interval_zero_disables_onchain_check(self) -> None:
        """A non-positive interval disables the periodic on-chain check
        entirely — the peg is still served, but the on-chain source is never
        consulted."""
        onchain = _Source("onchain", price=Decimal("1.0"))
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=0,
        )

        result = await aggregator.get_aggregated_price("USDC", "USD")

        assert result.source == "stablecoin_peg"
        assert onchain.calls == []

    @pytest.mark.asyncio
    async def test_depeg_fails_closed_returns_live_price_not_peg(self, caplog) -> None:
        """VIB-4841 (Codex review): when the on-chain check COMPLETES and detects
        a de-peg, the fast-path FAILS CLOSED — it must NOT return $1.00. It
        returns the live on-chain price so a real USDC/USDT/DAI de-peg is not
        masked."""
        onchain = _Source("onchain", price=Decimal("0.80"))  # 20% off peg
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=1,
        )

        with caplog.at_level("WARNING"):
            result = await aggregator.get_aggregated_price("USDC", "USD")

        # Fail closed: the peg is NOT returned.
        assert result.price != Decimal("1.00")
        assert result.price == Decimal("0.80")
        assert result.source != "stablecoin_peg"
        assert any("DE-PEGGED" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_depeg_with_unusable_price_falls_through_to_aggregate(self) -> None:
        """When a de-peg is detected but the on-chain price is unusable
        (<= 0, i.e. unavailable), the fast-path must NOT return the peg — it
        falls through to the full multi-source aggregate. Here the only source
        IS the on-chain feed, so the aggregate resolves to its (queried) price.
        The key assertion is that $1.00 is never returned via the fast-path."""
        onchain = _Source("onchain", price=Decimal("0"))  # de-peg, unusable price
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=1,
        )

        result = await aggregator.get_aggregated_price("USDC", "USD")

        # Did NOT short-circuit to the $1.00 peg fast-path.
        assert result.source != "stablecoin_peg"
        # Fell through to the aggregate, which queried the on-chain source.
        assert onchain.calls == ["USDC", "USDC"]  # sanity check + aggregate fetch

    @pytest.mark.asyncio
    async def test_onchain_check_failure_is_swallowed_returns_peg(self) -> None:
        """If the on-chain source raises (no RPC / no feed / Anvil), the check
        cannot determine a de-peg, so the peg is returned best-effort — the
        check must never break the fast-path."""
        onchain = _Source("onchain", raises=RuntimeError("no RPC URL"))
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=1,
        )

        result = await aggregator.get_aggregated_price("USDC", "USD")

        assert result.price == Decimal("1.00")
        assert result.source == "stablecoin_peg"

    @pytest.mark.asyncio
    async def test_slow_onchain_check_is_latency_bounded_and_returns_peg(self) -> None:
        """VIB-4841 (Codex review, P2): a slow on-chain RPC must NOT stall the
        fast-path. The inline check is bounded by a tight timeout; on timeout the
        check is treated as 'could not run' and the peg is returned best-effort.
        We patch the timeout to a tiny value so the test stays fast while still
        exercising the bound."""
        from almanak.gateway.data.price import aggregator as agg_mod

        # On-chain source sleeps far longer than the (patched) timeout.
        onchain = _Source("onchain", price=Decimal("1.0"), delay=5.0)
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=1,
        )

        loop = asyncio.get_event_loop()
        start = loop.time()
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(agg_mod, "STABLECOIN_PEG_CHECK_TIMEOUT_SECONDS", 0.05)
            result = await aggregator.get_aggregated_price("USDC", "USD")
        elapsed = loop.time() - start

        # Latency-bounded: nowhere near the 5s source delay.
        assert elapsed < 1.0
        # Best-effort peg returned on timeout.
        assert result.price == Decimal("1.00")
        assert result.source == "stablecoin_peg"


class TestStablecoinDepegLatch:
    """VIB-4841 (Codex re-audit): a detected de-peg must be LATCHED so the next
    (non-sampled) calls keep failing closed, instead of resuming the $1.00
    fast-path until the next 1-in-N sample. The latch clears on confirmed
    recovery so the fast-path resumes."""

    @pytest.mark.asyncio
    async def test_depeg_latches_subsequent_non_sampled_calls_do_not_return_peg(self) -> None:
        """De-peg detected on the sampled call (#1 with interval=50). The NEXT
        call would normally skip the 1-in-N check and serve $1.00 — but the
        latch must suppress the peg and fall through to the live price."""
        onchain = _Source("onchain", price=Decimal("0.80"))  # 20% off peg
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=50,
        )

        # Call #1: sampled -> de-peg detected, fails closed, latches.
        first = await aggregator.get_aggregated_price("USDC", "USD")
        assert first.source != "stablecoin_peg"
        assert first.price == Decimal("0.80")

        # Call #2: NOT a sampled call (interval=50). Without the latch this would
        # return $1.00. With the latch it must keep failing closed.
        second = await aggregator.get_aggregated_price("USDC", "USD")
        assert second.source != "stablecoin_peg"
        assert second.price != Decimal("1.00")
        assert second.price == Decimal("0.80")

        # Latch forces an on-chain check on EVERY call while de-pegged.
        assert onchain.calls == ["USDC", "USDC"]

    @pytest.mark.asyncio
    async def test_latch_clears_on_recovery_and_peg_resumes(self, caplog) -> None:
        """Once the on-chain price returns within threshold, the latch clears and
        the $1.00 peg fast-path resumes — without waiting for the next sample."""
        onchain = _Source("onchain", price=Decimal("0.80"))  # de-pegged
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=50,
        )

        # Call #1: de-peg detected -> latched, live price returned.
        first = await aggregator.get_aggregated_price("USDC", "USD")
        assert first.source != "stablecoin_peg"
        assert first.price == Decimal("0.80")

        # Call #2: still latched, still de-pegged -> still failing closed.
        second = await aggregator.get_aggregated_price("USDC", "USD")
        assert second.source != "stablecoin_peg"
        assert second.price == Decimal("0.80")

        # Stable re-pegs on-chain.
        onchain.price = Decimal("1.0")

        # Call #3: latch forces a check this call; recovery detected -> latch
        # clears AND the peg fast-path resumes on the SAME call.
        with caplog.at_level("WARNING"):
            third = await aggregator.get_aggregated_price("USDC", "USD")
        assert third.source == "stablecoin_peg"
        assert third.price == Decimal("1.00")
        assert any("RE-PEGGED" in rec.message for rec in caplog.records)

        # Call #4: back on the normal 1-in-N cadence — the peg is served and (not
        # being a sampled call) the on-chain source is NOT consulted again.
        calls_before = len(onchain.calls)
        fourth = await aggregator.get_aggregated_price("USDC", "USD")
        assert fourth.source == "stablecoin_peg"
        assert len(onchain.calls) == calls_before


# =============================================================================
# T1 — CoinGecko cooldown failover at the aggregate level
# =============================================================================


class TestCoinGeckoCooldownFailover:
    @pytest.mark.asyncio
    async def test_coingecko_rate_limited_binance_serves(self) -> None:
        """CoinGecko fails fast with ``DataSourceRateLimited`` (cooldown open);
        the aggregator must still return Binance's price — failover intact."""
        cg = _Source("coingecko", raises=DataSourceRateLimited(source="coingecko", retry_after=8.0))
        binance = _Source("binance", price=Decimal("2500"))
        aggregator = PriceAggregator(sources=[cg, binance])

        result = await aggregator.get_aggregated_price("WETH", "USD")

        assert result.price == Decimal("2500")
        # CoinGecko was attempted (and fast-failed), Binance served.
        assert cg.calls == ["WETH"]
        assert binance.calls == ["WETH"]

    @pytest.mark.asyncio
    async def test_rate_limited_source_does_not_stall_aggregate(self) -> None:
        """A fast-failing rate-limited CoinGecko must not add latency: the
        aggregate resolves at the healthy source's latency. We bound the
        wall-clock: even with a tiny Binance delay, the rate-limited CG returns
        instantly (no sleep) so total time stays well under a 1s retry budget."""
        cg = _Source("coingecko", raises=DataSourceRateLimited(source="coingecko", retry_after=8.0))
        binance = _Source("binance", price=Decimal("2500"), delay=0.02)
        aggregator = PriceAggregator(sources=[cg, binance])

        loop = asyncio.get_event_loop()
        start = loop.time()
        result = await aggregator.get_aggregated_price("WETH", "USD")
        elapsed = loop.time() - start

        assert result.price == Decimal("2500")
        # No multi-second retry/sleep on the rate-limited source.
        assert elapsed < 0.5
