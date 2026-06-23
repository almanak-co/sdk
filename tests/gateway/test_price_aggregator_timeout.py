"""Tests for the VIB-5375 (RC-3) bounded PriceAggregator timeouts.

Root cause: ``PriceAggregator._fetch_all_sources`` used to ``asyncio.gather``
every price source with NO overall deadline and no reliable per-source bound on
the whole ``get_price`` coroutine. On a cold/rate-limited fork a slow
non-CoinGecko source (e.g. a Mantle RPC behind the on-chain Chainlink source)
could stall indefinitely, blowing the 30s ``decide()`` budget → "timeout, 0 tx"
(the Mantle timeout class, VIB-2510/2511).

These tests prove:
  (a) a deliberately-slow/hanging source is cut off at the PER-SOURCE bound and
      recorded as an error while a fast source's price still returns;
  (b) the GLOBAL bound caps total wall-time when several sources are slow, and
      whatever valid results arrived still win;
  (c) a timed-out source is "unmeasured" (recorded as an error / health
      failure), never a zero price (Empty≠Zero).

The CoinGecko-429 / pre-warm behaviour is exercised (and proven un-regressed)
by ``tests/gateway/test_price_aggregator_stablecoin_peg.py`` and the existing
``tests/unit/data/price/test_aggregator.py`` suite, which are run alongside this
file in ``make test-gateway`` / the targeted pytest path.

A non-stablecoin token (``WETH``) is used throughout so the stablecoin peg
fast-path never short-circuits the full multi-source aggregate under test.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.data.interfaces import (
    AllDataSourcesFailed,
    BasePriceSource,
    PriceResult,
)
from almanak.gateway.data.price.aggregator import (
    DEFAULT_GLOBAL_TIMEOUT_SECONDS,
    DEFAULT_PER_SOURCE_TIMEOUT_SECONDS,
    PriceAggregator,
)


class _Source(BasePriceSource):
    """Configurable mock price source.

    ``delay`` simulates a slow/hanging upstream. ``hang=True`` simulates an
    unbounded stall (sleeps far past any test timeout) so we prove the
    aggregator's own bound — not the source's internal one — is what cuts it off.
    ``raises`` takes precedence over ``price``.
    """

    def __init__(
        self,
        name: str,
        *,
        price: Decimal | None = None,
        raises: Exception | None = None,
        delay: float = 0.0,
        hang: bool = False,
    ) -> None:
        self._name = name
        self.price = price
        self._raises = raises
        self._delay = delay
        self._hang = hang
        self.calls: list[str] = []
        self.cancelled = False

    @property
    def source_name(self) -> str:
        return self._name

    async def get_price(self, token: str, quote: str = "USD", **kwargs) -> PriceResult:
        self.calls.append(token)
        try:
            if self._hang:
                # Far longer than any per-source / global bound under test, so if
                # the aggregator did NOT bound us the test would hang for 1h.
                await asyncio.sleep(3600)
            elif self._delay:
                await asyncio.sleep(self._delay)
        except asyncio.CancelledError:
            # The aggregator must cancel us when it cuts the bound.
            self.cancelled = True
            raise
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
# (a) Per-source bound cuts off a hanging source; fast source still returns
# =============================================================================


class TestPerSourceTimeout:
    @pytest.mark.asyncio
    async def test_hanging_source_cut_off_fast_source_returns(self) -> None:
        """A hanging source is cut at the per-source bound and recorded as an
        error, while the fast source's price still resolves the aggregate."""
        fast = _Source("binance", price=Decimal("2500"))
        slow = _Source("onchain", hang=True)
        aggregator = PriceAggregator(
            sources=[fast, slow],
            per_source_timeout_seconds=0.2,
            global_timeout_seconds=2.0,
        )

        start = time.monotonic()
        result = await aggregator.get_aggregated_price("WETH", "USD")
        elapsed = time.monotonic() - start

        # The fast source's price wins; the hanging source did not sink it.
        assert result.price == Decimal("2500")
        # Bounded near the per-source timeout, nowhere near 3600s.
        assert elapsed < 1.0

        # The hanging source is recorded as an error (unmeasured), not a price.
        details = aggregator.get_last_details("WETH", "USD")
        assert details is not None
        assert "binance" in details["sources_ok"]
        assert "onchain" in details["sources_failed"]
        assert "timeout" in details["sources_failed"]["onchain"].lower()

        # Health metrics reflect the timeout as a failure, and the source was
        # actually cancelled (not left dangling).
        health = aggregator.get_source_health("onchain")
        assert health is not None
        assert health["failed_requests"] >= 1
        assert slow.cancelled is True

    @pytest.mark.asyncio
    async def test_all_sources_time_out_raises_all_failed(self) -> None:
        """If every source is cut off, the aggregator raises AllDataSourcesFailed
        (not a zero price) — a timed-out source is unmeasured, never $0."""
        slow_a = _Source("onchain", hang=True)
        slow_b = _Source("dexscreener", hang=True)
        aggregator = PriceAggregator(
            sources=[slow_a, slow_b],
            per_source_timeout_seconds=0.2,
            global_timeout_seconds=2.0,
        )

        with pytest.raises(AllDataSourcesFailed) as exc_info:
            await aggregator.get_aggregated_price("WETH", "USD")

        # Both surface as timeout errors — Empty≠Zero (no $0 ever leaks out).
        errors = exc_info.value.errors
        assert set(errors) == {"onchain", "dexscreener"}
        assert all("timeout" in msg.lower() for msg in errors.values())

    @pytest.mark.asyncio
    async def test_per_source_timeout_disabled_when_non_positive(self) -> None:
        """A non-positive per-source timeout disables the bound: a healthy (if
        slightly delayed) source completes normally."""
        slow_ok = _Source("binance", price=Decimal("2500"), delay=0.1)
        aggregator = PriceAggregator(
            sources=[slow_ok],
            per_source_timeout_seconds=0.0,
            global_timeout_seconds=0.0,
        )

        result = await aggregator.get_aggregated_price("WETH", "USD")
        assert result.price == Decimal("2500")


# =============================================================================
# (b) Global bound caps total wall-time across many slow sources
# =============================================================================


class TestGlobalTimeout:
    @pytest.mark.asyncio
    async def test_global_bound_caps_walltime_partial_results_win(self) -> None:
        """With several slow sources and one fast source, the global bound caps
        total wall-time and the fast source's result still resolves the
        aggregate — partial results are preserved on the global cutoff."""
        fast = _Source("binance", price=Decimal("2500"))
        # Per-source bound is generous (these would each be allowed individually),
        # so it is the GLOBAL bound that does the capping here.
        slow1 = _Source("onchain", hang=True)
        slow2 = _Source("dexscreener", hang=True)
        slow3 = _Source("coingecko", hang=True)
        aggregator = PriceAggregator(
            sources=[fast, slow1, slow2, slow3],
            per_source_timeout_seconds=30.0,
            global_timeout_seconds=0.3,
        )

        start = time.monotonic()
        result = await aggregator.get_aggregated_price("WETH", "USD")
        elapsed = time.monotonic() - start

        assert result.price == Decimal("2500")
        # Capped by the 0.3s global bound, not the 30s per-source bound.
        assert elapsed < 2.0

        details = aggregator.get_last_details("WETH", "USD")
        assert details is not None
        assert details["sources_ok"] == ["binance"]
        # All three hanging sources are recorded as (global-timeout) errors.
        assert set(details["sources_failed"]) == {"onchain", "dexscreener", "coingecko"}
        for msg in details["sources_failed"].values():
            assert "timeout" in msg.lower()

        # The cut-off sources were actually cancelled, not leaked.
        assert slow1.cancelled and slow2.cancelled and slow3.cancelled

    @pytest.mark.asyncio
    async def test_bounds_applied_independently(self) -> None:
        """The two bounds are applied independently and not coerced against each
        other — a tight global bound fires even when the per-source bound is
        generous (whichever fires first caps the wall-time)."""
        aggregator = PriceAggregator(
            sources=[_Source("binance", price=Decimal("2500"))],
            per_source_timeout_seconds=5.0,
            global_timeout_seconds=1.0,
        )
        assert aggregator._global_timeout_seconds == 1.0
        assert aggregator._per_source_timeout_seconds == 5.0


# =============================================================================
# Defaults are coherent with the decide() budget
# =============================================================================


class TestTimeoutDefaults:
    def test_default_bounds_fit_under_decide_budget(self) -> None:
        """The default per-source (10s) and global (15s) bounds sit under the 30s
        decide() budget and the 60s pre-warm window, and the global bound is not
        below the per-source bound."""
        assert DEFAULT_PER_SOURCE_TIMEOUT_SECONDS == 10.0
        assert DEFAULT_GLOBAL_TIMEOUT_SECONDS == 15.0
        assert DEFAULT_GLOBAL_TIMEOUT_SECONDS >= DEFAULT_PER_SOURCE_TIMEOUT_SECONDS
        # Under the 30s decide() budget with headroom for the rest of decide().
        assert DEFAULT_GLOBAL_TIMEOUT_SECONDS < 30.0
        # Under the 60s pre-warm window.
        assert DEFAULT_GLOBAL_TIMEOUT_SECONDS < 60.0

    def test_aggregator_uses_defaults_when_unset(self) -> None:
        aggregator = PriceAggregator(sources=[_Source("binance", price=Decimal("2500"))])
        assert aggregator._per_source_timeout_seconds == DEFAULT_PER_SOURCE_TIMEOUT_SECONDS
        assert aggregator._global_timeout_seconds == DEFAULT_GLOBAL_TIMEOUT_SECONDS
