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
from unittest.mock import AsyncMock, patch

import pytest

from almanak.framework.data.interfaces import (
    AllDataSourcesFailed,
    BasePriceSource,
    DataSourceRateLimited,
    PriceResult,
)
from almanak.framework.data.tokens.models import BridgeType, ResolvedToken
from almanak.gateway.data.price.aggregator import PriceAggregator

USDC_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7"


def _resolved_stable(symbol: str, address: str) -> ResolvedToken:
    return ResolvedToken(
        symbol=symbol,
        address=address,
        decimals=6,
        chain="ethereum",
        chain_id=1,
        name=symbol,
        is_stablecoin=True,
        is_native=False,
        is_wrapped_native=False,
        canonical_symbol=symbol,
        bridge_type=BridgeType.NATIVE,
        source="static",
    )


USDC_RESOLVED = _resolved_stable("USDC", USDC_ADDRESS)
USDT_RESOLVED = _resolved_stable("USDT", USDT_ADDRESS)
USDC_IDENTITY = USDC_RESOLVED.token_ref.identity_key
USDT_IDENTITY = USDT_RESOLVED.token_ref.identity_key
USDC_IDENTITY_LABEL = f"{USDC_IDENTITY[0]}:{USDC_IDENTITY[1]}"
USDT_IDENTITY_LABEL = f"{USDT_IDENTITY[0]}:{USDT_IDENTITY[1]}"


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
        bypass_price: Decimal | None = None,
        confidence: float = 0.95,
        stale: bool = False,
        peg_tokens: tuple[str, ...] = (),
    ) -> None:
        self._name = name
        self.price = price
        self._raises = raises
        self._delay = delay
        self._bypass_price = bypass_price
        self._confidence = confidence
        self._stale = stale
        self._peg_tokens = peg_tokens
        self.calls: list[str] = []
        self.call_kwargs: list[dict[str, object]] = []

    @property
    def source_name(self) -> str:
        return self._name

    @property
    def provides_stablecoin_verification(self) -> bool:
        return self._name in {"chainlink", "onchain", "onchain_chainlink"}

    async def get_price(self, token: str, quote: str = "USD", **kwargs) -> PriceResult:
        self.calls.append(token)
        self.call_kwargs.append(dict(kwargs))
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._raises is not None:
            raise self._raises
        price = (
            self._bypass_price
            if kwargs.get("bypass_stablecoin_fallback") and self._bypass_price is not None
            else self.price
        )
        assert price is not None
        return PriceResult(
            price=price,
            source=self._name,
            timestamp=datetime.now(UTC),
            confidence=self._confidence,
            stale=self._stale,
            peg_tokens=self._peg_tokens,
        )

    async def close(self) -> None:
        pass


# =============================================================================
# FR-5002 — stablecoin peg fast-path
# =============================================================================


class TestStablecoinPegFastPath:
    @pytest.mark.asyncio
    async def test_bare_stable_symbol_does_not_authorize_peg(self) -> None:
        """Symbol text alone cannot grant synthetic pricing."""
        source = _Source(
            "coingecko",
            raises=DataSourceRateLimited(source="coingecko", retry_after=1.0),
        )
        aggregator = PriceAggregator(sources=[source])

        with pytest.raises(AllDataSourcesFailed):
            await aggregator.get_aggregated_price("USDC", "USD")

    @pytest.mark.asyncio
    async def test_stable_usd_returns_peg_without_upstream_call(self) -> None:
        """A stable/USD pair returns $1.00 from the fast-path and NONE of the
        upstream sources are queried — that's the per-iteration cost cut."""
        cg = _Source("coingecko", price=Decimal("0.999"))
        binance = _Source("binance", price=Decimal("1.001"))
        aggregator = PriceAggregator(sources=[binance, cg])

        result = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

        assert result.price == Decimal("1.00")
        assert result.source == "stablecoin_peg"
        assert result.peg_tokens == (USDC_IDENTITY_LABEL,)
        # Critical: no upstream price call for either source.
        assert cg.calls == []
        assert binance.calls == []

    @pytest.mark.asyncio
    async def test_provider_synthetic_provenance_survives_aggregation(self) -> None:
        """Fan-in cannot disguise a provider-authored peg as measured data."""
        synthetic = _Source(
            "binance",
            price=Decimal("1"),
            peg_tokens=(USDC_IDENTITY_LABEL,),
        )
        measured = _Source("chainlink", price=Decimal("0.999"))
        aggregator = PriceAggregator(sources=[synthetic, measured])

        # Exercise fan-in rather than the aggregate-level stablecoin fast path.
        with patch.object(aggregator, "_maybe_stablecoin_peg", new=AsyncMock(return_value=None)):
            result = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

        assert result.source == "aggregated"
        assert result.peg_tokens == (USDC_IDENTITY_LABEL,)

    @pytest.mark.asyncio
    async def test_verification_excludes_provider_pegs_that_would_outvote_depeg(self) -> None:
        """Two synthetic $1 inputs cannot outlier a measured $0.80 depeg."""
        measured = _Source(
            "chainlink",
            price=Decimal("1"),
            bypass_price=Decimal("0.80"),
            stale=True,
        )
        binance = _Source(
            "binance",
            price=Decimal("1"),
            peg_tokens=(USDC_IDENTITY_LABEL,),
        )
        hypercore = _Source(
            "hypercore_oracle",
            price=Decimal("1"),
            peg_tokens=(USDC_IDENTITY_LABEL,),
        )
        aggregator = PriceAggregator(
            sources=[measured, binance, hypercore],
            stablecoin_verify=True,
        )

        result = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

        assert result.price == Decimal("0.80")
        assert result.source == "aggregated"
        assert result.peg_tokens == ()
        assert measured.call_kwargs == [{"resolved_token": USDC_RESOLVED, "bypass_stablecoin_fallback": True}]
        details = aggregator.get_last_details("USDC", "USD")
        assert details["sources_ok"] == ["chainlink"]
        assert "binance" in details["sources_failed"]
        assert "hypercore_oracle" in details["sources_failed"]

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

        result = await aggregator.get_aggregated_price("USDC", "EUR", resolved_token=USDC_RESOLVED)

        assert result.source != "stablecoin_peg"
        assert cg.calls == ["USDC"]

    @pytest.mark.asyncio
    async def test_stablecoin_verify_disables_fast_path(self) -> None:
        """With ``stablecoin_verify=True`` the fast-path is off: the full
        multi-source aggregate runs and the upstream sources ARE queried."""
        cg = _Source("coingecko", price=Decimal("1.0"))
        aggregator = PriceAggregator(sources=[cg], stablecoin_verify=True)

        result = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

        assert result.source != "stablecoin_peg"
        assert cg.calls == ["USDC"]

    @pytest.mark.asyncio
    async def test_stablecoin_verify_forces_measured_verifier_price(self) -> None:
        """Live verification must bypass a verifier's own synthetic peg."""
        onchain = _Source(
            "chainlink",
            price=Decimal("1.00"),
            bypass_price=Decimal("0.80"),
        )
        aggregator = PriceAggregator(sources=[onchain], stablecoin_verify=True)

        result = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

        assert result.price == Decimal("0.80")
        assert onchain.call_kwargs == [{"resolved_token": USDC_RESOLVED, "bypass_stablecoin_fallback": True}]


class TestStablecoinPegChainlinkSanityCheck:
    @pytest.mark.asyncio
    async def test_check_forces_measured_path_and_preserves_measured_provenance(self) -> None:
        onchain = _Source(
            "onchain_chainlink",
            price=Decimal("1.00"),
            bypass_price=Decimal("0.80"),
        )
        aggregator = PriceAggregator(sources=[onchain], stablecoin_chainlink_check_interval=1)

        result = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

        assert result.price == Decimal("0.80")
        assert result.source == "onchain_chainlink"
        assert onchain.call_kwargs == [{"resolved_token": USDC_RESOLVED, "bypass_stablecoin_fallback": True}]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("confidence", "stale"),
        [(0.89, False), (0.99, True)],
    )
    async def test_stale_or_low_confidence_offpeg_measurement_suppresses_peg(
        self,
        confidence: float,
        stale: bool,
    ) -> None:
        onchain = _Source(
            "chainlink",
            price=Decimal("1.00"),
            bypass_price=Decimal("0.80"),
            confidence=confidence,
            stale=stale,
        )
        aggregator = PriceAggregator(sources=[onchain], stablecoin_chainlink_check_interval=1)

        result = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

        assert result.source != "stablecoin_peg"
        assert result.price == Decimal("0.80")
        assert aggregator._depegged_tokens == {USDC_IDENTITY}
        assert onchain.calls == ["USDC", "USDC"]
        assert onchain.call_kwargs == [
            {"resolved_token": USDC_RESOLVED, "bypass_stablecoin_fallback": True},
            {"resolved_token": USDC_RESOLVED, "bypass_stablecoin_fallback": True},
        ]

    @pytest.mark.asyncio
    async def test_runtime_verifier_type_error_is_treated_as_upstream_unavailability(self) -> None:
        onchain = _Source("chainlink", raises=TypeError("unexpected bypass keyword"))
        aggregator = PriceAggregator(sources=[onchain], stablecoin_chainlink_check_interval=1)

        result = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

        assert result.source == "stablecoin_peg"

    def test_verifier_signature_is_validated_when_sources_are_wired(self) -> None:
        class WrongSignatureSource(_Source):
            async def get_price(self, token: str, quote: str = "USD") -> PriceResult:
                return await super().get_price(token, quote)

        with pytest.raises(TypeError, match="incompatible stablecoin verifier signature"):
            PriceAggregator(sources=[WrongSignatureSource("chainlink", price=Decimal("1.00"))])

    @pytest.mark.asyncio
    async def test_periodic_chainlink_check_runs_on_first_call(self) -> None:
        """The on-chain sanity check runs on the first peg-served call (1-in-N
        with N anchored to call #1) and again every Nth call thereafter — even
        though the peg is what's returned to the caller."""
        onchain = _Source("chainlink", price=Decimal("1.0"))
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=3,
        )

        # Call #1 -> check runs; #2,#3 -> skip; #4 -> check runs again.
        for _ in range(4):
            result = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)
            assert result.source == "stablecoin_peg"

        # Sanity check hit the on-chain source on calls #1 and #4 only.
        assert onchain.calls == ["USDC", "USDC"]

    @pytest.mark.asyncio
    async def test_check_interval_zero_disables_onchain_check(self) -> None:
        """A non-positive interval disables the periodic on-chain check
        entirely — the peg is still served, but the on-chain source is never
        consulted."""
        onchain = _Source("chainlink", price=Decimal("1.0"))
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=0,
        )

        result = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

        assert result.source == "stablecoin_peg"
        assert onchain.calls == []

    @pytest.mark.asyncio
    async def test_depeg_fails_closed_returns_live_price_not_peg(self, caplog) -> None:
        """VIB-4841 (Codex review): when the on-chain check COMPLETES and detects
        a de-peg, the fast-path FAILS CLOSED — it must NOT return $1.00. It
        returns the live on-chain price so a real USDC/USDT/DAI de-peg is not
        masked."""
        onchain = _Source("chainlink", price=Decimal("0.80"))  # 20% off peg
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=1,
        )

        with caplog.at_level("WARNING"):
            result = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

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
        onchain = _Source("chainlink", price=Decimal("0"))  # de-peg, unusable price
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=1,
        )

        result = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

        # Did NOT short-circuit to the $1.00 peg fast-path.
        assert result.source != "stablecoin_peg"
        # Fell through to the aggregate, which queried the on-chain source.
        assert onchain.calls == ["USDC", "USDC"]  # sanity check + aggregate fetch

    @pytest.mark.asyncio
    async def test_onchain_check_failure_is_swallowed_returns_peg(self) -> None:
        """If the on-chain source raises (no RPC / no feed / Anvil), the check
        cannot determine a de-peg, so the peg is returned best-effort — the
        check must never break the fast-path."""
        onchain = _Source("chainlink", raises=RuntimeError("no RPC URL"))
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=1,
        )

        result = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

        assert result.price == Decimal("1.00")
        assert result.source == "stablecoin_peg"

    @pytest.mark.asyncio
    async def test_repeated_verifier_failures_warn_once_per_token_outage(self, caplog) -> None:
        onchain = _Source("chainlink", raises=RuntimeError("no RPC URL"))
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=1,
            stablecoin_verifier_failure_warning_threshold=2,
        )

        with caplog.at_level("WARNING"):
            for _ in range(4):
                result = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)
                assert result.source == "stablecoin_peg"

        outage_warnings = [
            record
            for record in caplog.records
            if f"peg verifier unavailable for {USDC_IDENTITY_LABEL}" in record.message
        ]
        assert len(outage_warnings) == 1
        assert "after 2 consecutive checks" in outage_warnings[0].message
        assert aggregator._stablecoin_verifier_failures == {USDC_IDENTITY: 4}

    @pytest.mark.asyncio
    async def test_verifier_failure_streaks_are_token_scoped(self, caplog) -> None:
        onchain = _Source("chainlink", raises=RuntimeError("no RPC URL"))
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=1,
            stablecoin_verifier_failure_warning_threshold=2,
        )

        with caplog.at_level("WARNING"):
            await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)
            await aggregator.get_aggregated_price("USDT", "USD", resolved_token=USDT_RESOLVED)

        assert aggregator._stablecoin_verifier_failures == {USDC_IDENTITY: 1, USDT_IDENTITY: 1}
        assert not any("peg verifier unavailable" in record.message for record in caplog.records)

        with caplog.at_level("WARNING"):
            await aggregator.get_aggregated_price("USDT", "USD", resolved_token=USDT_RESOLVED)

        assert aggregator._stablecoin_verifier_failures == {USDC_IDENTITY: 1, USDT_IDENTITY: 2}
        assert any(f"peg verifier unavailable for {USDT_IDENTITY_LABEL}" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_successful_verification_resets_failure_streak(self, caplog) -> None:
        onchain = _Source("chainlink", raises=RuntimeError("temporary outage"))
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=1,
            stablecoin_verifier_failure_warning_threshold=2,
        )

        await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)
        assert aggregator._stablecoin_verifier_failures == {USDC_IDENTITY: 1}

        onchain._raises = None
        onchain.price = Decimal("1.00")
        await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)
        assert aggregator._stablecoin_verifier_failures == {}

        onchain._raises = RuntimeError("new outage")
        with caplog.at_level("WARNING"):
            await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

        assert aggregator._stablecoin_verifier_failures == {USDC_IDENTITY: 1}
        assert not any(
            f"peg verifier unavailable for {USDC_IDENTITY_LABEL}" in record.message for record in caplog.records
        )

        with caplog.at_level("WARNING"):
            await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

        assert aggregator._stablecoin_verifier_failures == {USDC_IDENTITY: 2}
        assert (
            sum(f"peg verifier unavailable for {USDC_IDENTITY_LABEL}" in record.message for record in caplog.records)
            == 1
        )

    @pytest.mark.asyncio
    async def test_slow_onchain_check_is_latency_bounded_and_returns_peg(self, caplog) -> None:
        """VIB-4841 (Codex review, P2): a slow on-chain RPC must NOT stall the
        fast-path. The inline check is bounded by a tight timeout; on timeout the
        check is treated as 'could not run' and the peg is returned best-effort.
        We patch the timeout to a tiny value so the test stays fast while still
        exercising the bound."""
        from almanak.gateway.data.price import aggregator as agg_mod

        # On-chain source sleeps far longer than the (patched) timeout.
        onchain = _Source("chainlink", price=Decimal("1.0"), delay=5.0)
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=1,
            stablecoin_verifier_failure_warning_threshold=1,
        )

        loop = asyncio.get_event_loop()
        start = loop.time()
        with caplog.at_level("WARNING"), pytest.MonkeyPatch.context() as mp:
            mp.setattr(agg_mod, "STABLECOIN_PEG_CHECK_TIMEOUT_SECONDS", 0.05)
            result = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)
        elapsed = loop.time() - start

        # Latency-bounded: nowhere near the 5s source delay.
        assert elapsed < 1.0
        # Best-effort peg returned on timeout.
        assert result.price == Decimal("1.00")
        assert result.source == "stablecoin_peg"
        assert any("latest failure: timeout" in record.message for record in caplog.records)


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
        onchain = _Source("chainlink", price=Decimal("0.80"))  # 20% off peg
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=50,
        )

        # Call #1: sampled -> de-peg detected, fails closed, latches.
        first = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)
        assert first.source != "stablecoin_peg"
        assert first.price == Decimal("0.80")

        # Call #2: NOT a sampled call (interval=50). Without the latch this would
        # return $1.00. With the latch it must keep failing closed.
        second = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)
        assert second.source != "stablecoin_peg"
        assert second.price != Decimal("1.00")
        assert second.price == Decimal("0.80")

        # Latch forces an on-chain check on EVERY call while de-pegged.
        assert onchain.calls == ["USDC", "USDC"]

    @pytest.mark.asyncio
    async def test_latch_clears_on_recovery_and_peg_resumes(self, caplog) -> None:
        """Once the on-chain price returns within threshold, the latch clears and
        the $1.00 peg fast-path resumes — without waiting for the next sample."""
        onchain = _Source("chainlink", price=Decimal("0.80"))  # de-pegged
        aggregator = PriceAggregator(
            sources=[onchain],
            stablecoin_chainlink_check_interval=50,
        )

        # Call #1: de-peg detected -> latched, live price returned.
        first = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)
        assert first.source != "stablecoin_peg"
        assert first.price == Decimal("0.80")

        # Call #2: still latched, still de-pegged -> still failing closed.
        second = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)
        assert second.source != "stablecoin_peg"
        assert second.price == Decimal("0.80")

        # Stable re-pegs on-chain.
        onchain.price = Decimal("1.0")

        # Call #3: latch forces a check this call; recovery detected -> latch
        # clears AND the peg fast-path resumes on the SAME call.
        with caplog.at_level("WARNING"):
            third = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)
        assert third.source == "stablecoin_peg"
        assert third.price == Decimal("1.00")
        assert any("RE-PEGGED" in rec.message for rec in caplog.records)

        # Call #4: back on the normal 1-in-N cadence — the peg is served and (not
        # being a sampled call) the on-chain source is NOT consulted again.
        calls_before = len(onchain.calls)
        fourth = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)
        assert fourth.source == "stablecoin_peg"
        assert len(onchain.calls) == calls_before

    @pytest.mark.asyncio
    async def test_stale_onpeg_measurement_cannot_clear_existing_latch(self) -> None:
        onchain = _Source("chainlink", price=Decimal("0.80"))
        aggregator = PriceAggregator(sources=[onchain], stablecoin_chainlink_check_interval=1)

        first = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)
        assert first.source != "stablecoin_peg"
        assert aggregator._depegged_tokens == {USDC_IDENTITY}

        onchain.price = Decimal("1.00")
        onchain._stale = True
        second = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

        assert second.source != "stablecoin_peg"
        assert aggregator._depegged_tokens == {USDC_IDENTITY}

        onchain._stale = False
        third = await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)
        assert third.source == "stablecoin_peg"
        assert aggregator._depegged_tokens == set()

    @pytest.mark.asyncio
    async def test_all_sources_failed_fallback_cannot_bypass_depeg_latch(self) -> None:
        onchain = _Source("chainlink", price=Decimal("0.80"))
        aggregator = PriceAggregator(sources=[onchain], stablecoin_chainlink_check_interval=1)

        await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)
        assert aggregator._depegged_tokens == {USDC_IDENTITY}

        onchain._raises = RuntimeError("feed unavailable")
        with pytest.raises(AllDataSourcesFailed):
            await aggregator.get_aggregated_price("USDC", "USD", resolved_token=USDC_RESOLVED)

        assert aggregator._depegged_tokens == {USDC_IDENTITY}


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
