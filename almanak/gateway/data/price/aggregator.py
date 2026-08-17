"""Price Aggregator for multi-source price validation and aggregation.

This module provides a production-ready price aggregator that combines prices
from multiple sources, detects outliers, and handles partial failures gracefully.

Key Features:
    - Single source support with confidence based on staleness
    - Multi-source aggregation using median price
    - Outlier detection (>2% deviation from median)
    - Partial failure handling with adjusted confidence
    - Source health tracking for routing decisions

Example:
    from almanak.gateway.data.price.aggregator import PriceAggregator
    from almanak.integrations.coingecko.gateway.price_source import CoinGeckoPriceSource

    sources = [CoinGeckoPriceSource()]
    aggregator = PriceAggregator(sources=sources)
    result = await aggregator.get_aggregated_price("WETH", "USD")
    print(f"Price: {result.price}, Confidence: {result.confidence}")
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import statistics
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

if TYPE_CHECKING:
    from almanak.framework.data.tokens.models import ResolvedToken

from almanak.framework.data.interfaces import (
    AllDataSourcesFailed,
    BasePriceSource,
    PriceResult,
)
from almanak.framework.data.tokens.pegs import (
    PEG_DEVIATION_THRESHOLD_BPS,
    is_pegged,
    is_within_peg,
    peg_deviation_bps,
)

logger = logging.getLogger(__name__)


@runtime_checkable
class StablecoinVerificationSource(Protocol):
    """Price source capable of forcing a measured stablecoin observation."""

    @property
    def provides_stablecoin_verification(self) -> bool: ...

    async def get_price(
        self,
        token: str,
        quote: str = "USD",
        *,
        resolved_token: ResolvedToken | None = None,
        bypass_stablecoin_fallback: bool = False,
    ) -> PriceResult: ...


# Default configuration constants
DEFAULT_OUTLIER_DEVIATION_THRESHOLD = 0.02  # 2% deviation from median
DEFAULT_STALE_CONFIDENCE_PENALTY = 0.3  # Reduce confidence by 30% for stale data
DEFAULT_PARTIAL_FAILURE_CONFIDENCE_PENALTY = 0.1  # Reduce by 10% per failed source

# VIB-5375 (RC-3) — bounded per-source + global aggregator timeout.
#
# Why this exists: ``_fetch_all_sources`` used to ``asyncio.gather`` every price
# source with NO overall deadline. On a cold/rate-limited fork a slow non-CoinGecko
# source (e.g. a Mantle RPC behind the on-chain Chainlink source) could stall
# indefinitely — blowing the 30s ``decide()`` budget → "timeout, 0 tx" (the Mantle
# timeout class: VIB-2510/2511, shared RC behind 2338/2339/2460/2803). Each source
# already self-bounds its *individual HTTP request* (Binance/onchain 5s, CoinGecko/
# DexScreener 10s ``aiohttp.ClientTimeout``), but that does NOT bound the whole
# ``get_price`` coroutine — retries, symbol→address fallbacks, multi-pair scans, or a
# hang outside the HTTP call all escape the inner timeout. These two bounds wrap the
# *coroutine* so the aggregator can never exceed a known wall-time regardless of a
# source's internal behaviour.
#
# Defaults are chosen to sit ABOVE each source's internal single-request timeout (so
# a healthy-but-slow source is not double-cut) and BELOW the 30s ``decide()`` budget
# and the 60s pre-warm window (so a stalled source can never consume either):
#   - Per-source 10s: ≥ the slowest source's internal HTTP timeout (10s), yet 3× under
#     the decide() budget — a true hang is cut at 10s, not at "never".
#   - Global 15s: sources run CONCURRENTLY, so the wall-time floor is one per-source
#     bound (10s); 15s adds slack for event-loop scheduling / many-source fan-out while
#     still leaving ≥15s of the 30s decide() budget for the rest of decide() and
#     comfortably fitting inside the 60s pre-warm window.
# A timed-out source is recorded as an error (Empty≠Zero: "unmeasured", never a zero
# price) and does NOT sink the aggregation — whatever valid results arrived still win.
DEFAULT_PER_SOURCE_TIMEOUT_SECONDS = 10.0
DEFAULT_GLOBAL_TIMEOUT_SECONDS = 15.0

# Magnitude outlier threshold: if max/min price ratio exceeds this, the sources
# fundamentally disagree (feed misconfiguration, wrong units, decimal mismatch).
# Example: wstETH/ETH exchange rate feed (~1.228) decoded with 8-decimal assumption
# produces ~$12.28B, while CoinGecko returns ~$3,400. Ratio ≈ 3.6M× >> 100×.
# At this scale, averaging produces nonsense -- we must raise AllDataSourcesFailed.
DEFAULT_MAGNITUDE_OUTLIER_RATIO = 100.0

# Absolute price ceiling for single-source results (USD per token).
# Any single-source price above this is almost certainly a feed misconfiguration
# (wrong decimals, exchange rate vs USD, etc.). BTC at ~$100K is the most expensive
# fungible DeFi token; $10M/token provides >100× headroom.
DEFAULT_SINGLE_SOURCE_PRICE_CEILING = Decimal("10_000_000")  # $10M per token

# How often the proactive peg fast-path runs a Chainlink on-chain sanity check
# (1 in N peg-served calls). The check confirms the stable is still trading at
# ~$1.00 on-chain; when it detects a de-peg the fast-path FAILS CLOSED — it does
# not return the peg (see ``_maybe_stablecoin_peg``). Kept low-frequency to
# honour the whole point of the fast-path (don't hit upstream every iteration).
# Operators tune it via ``ALMANAK_GATEWAY_STABLECOIN_CHAINLINK_CHECK_INTERVAL``.
DEFAULT_STABLECOIN_CHAINLINK_CHECK_INTERVAL = 50

# Wall-clock budget for the on-chain peg sanity check. The fast-path's whole
# point is low latency, so a slow RPC must not stall it: we bound the inline
# ``get_price`` await with this timeout. On timeout the check is treated as
# "could not run" — the peg is still returned (best-effort) — but a check that
# DOES complete and detects a de-peg makes the fast-path fail closed.
STABLECOIN_PEG_CHECK_TIMEOUT_SECONDS = 1.5

# Consecutive verifier failures per token before the gateway emits one WARNING
# for the current outage streak. Successful verifier execution resets the
# streak. Operators can tune this through GatewaySettings; non-positive values
# disable the warning without changing the best-effort peg behavior.
DEFAULT_STABLECOIN_VERIFIER_FAILURE_WARNING_THRESHOLD = 3

# A stale or low-confidence observation is not execution-grade enough to clear
# a latch or be returned directly, but measured off-peg evidence still suppresses
# the synthetic peg and falls through to the multi-source aggregate.
STABLECOIN_VERIFIER_MIN_CONFIDENCE = 0.9


def is_stablecoin_for_fallback(token: str, resolved_token: ResolvedToken | None) -> bool:
    """Return whether an exact resolved identity has a registry peg.

    ``token`` remains in the compatibility signature used by price sources;
    symbol text never authorizes a synthetic price.
    """
    del token
    return resolved_token is not None and is_pegged(resolved_token.token_ref) is not None


def _peg_identity(resolved_token: ResolvedToken | None) -> tuple[str, str] | None:
    """Return the exact identity key when the registry authorizes a peg."""
    if resolved_token is None or is_pegged(resolved_token.token_ref) is None:
        return None
    return resolved_token.token_ref.identity_key


# Prefixes for derivative tokens that are known to be unpriceable on standard feeds.
# These tokens (PT, YT, LP, etc.) don't have Chainlink/Binance/CoinGecko listings,
# so "all sources failed" is expected -- log at WARNING, not ERROR.
KNOWN_UNPRICEABLE_PREFIXES = ("PT-", "YT-", "LP-", "SY-", "aToken-", "vToken-", "sToken-")


def _is_known_unpriceable(token: str) -> bool:
    """Check if a token is known to be unpriceable on standard price feeds."""
    upper = token.upper()
    return any(upper.startswith(prefix.upper()) for prefix in KNOWN_UNPRICEABLE_PREFIXES)


@dataclass
class SourceHealthMetrics:
    """Health metrics for a single price source.

    Tracks success rate, latency, and error information for making
    routing decisions and observability.

    Attributes:
        source_name: Name of the data source
        total_requests: Total number of price requests
        successful_requests: Number of successful requests
        failed_requests: Number of failed requests
        total_latency_ms: Total latency for successful requests
        last_success_time: Time of last successful request
        last_error_time: Time of last error
        last_error: Last error message
    """

    source_name: str
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_latency_ms: float = 0.0
    last_success_time: datetime | None = None
    last_error_time: datetime | None = None
    last_error: str | None = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage (0-100)."""
        if self.total_requests == 0:
            return 100.0
        return (self.successful_requests / self.total_requests) * 100

    @property
    def average_latency_ms(self) -> float:
        """Calculate average latency in milliseconds."""
        if self.successful_requests == 0:
            return 0.0
        return self.total_latency_ms / self.successful_requests

    def record_success(self, latency_ms: float) -> None:
        """Record a successful request."""
        self.total_requests += 1
        self.successful_requests += 1
        self.total_latency_ms += latency_ms
        self.last_success_time = datetime.now(UTC)

    def record_failure(self, error: str) -> None:
        """Record a failed request."""
        self.total_requests += 1
        self.failed_requests += 1
        self.last_error = error
        self.last_error_time = datetime.now(UTC)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "source_name": self.source_name,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "success_rate": round(self.success_rate, 2),
            "average_latency_ms": round(self.average_latency_ms, 2),
            "last_success_time": (self.last_success_time.isoformat() if self.last_success_time else None),
            "last_error_time": (self.last_error_time.isoformat() if self.last_error_time else None),
            "last_error": self.last_error,
        }


@dataclass
class AggregationResult:
    """Internal result from price aggregation including outlier info.

    Attributes:
        price: Aggregated price (median for multiple sources)
        valid_results: List of valid PriceResults used in aggregation
        outliers: List of PriceResults flagged as outliers
        errors: Dict mapping source names to error messages
    """

    price: Decimal
    valid_results: list[PriceResult]
    outliers: list[PriceResult] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


class PriceAggregator:
    """Price aggregator with multi-source validation, outlier detection, and graceful degradation.

    This class implements the PriceOracle protocol and provides aggregated prices
    from multiple BasePriceSource implementations with proper error handling.

    Key behaviors:
    - Single source: Returns price with confidence based on staleness
    - Multiple sources: Returns median price, flags outliers (>2% deviation)
    - Partial failure: Continues with available sources, adjusts confidence
    - Total failure: Raises AllDataSourcesFailed with individual error details

    Attributes:
        sources: List of BasePriceSource implementations
        outlier_threshold: Deviation threshold for outlier detection (default 2%)
        stale_confidence_penalty: Confidence reduction for stale data
        partial_failure_penalty: Confidence reduction per failed source

    Example:
        # Single source
        aggregator = PriceAggregator(sources=[CoinGeckoPriceSource()])
        result = await aggregator.get_aggregated_price("ETH")

        # Multiple sources
        aggregator = PriceAggregator(sources=[
            CoinGeckoPriceSource(),
            ChainlinkPriceSource(),
        ])
        result = await aggregator.get_aggregated_price("ETH")
        print(f"Median price: {result.price}, Confidence: {result.confidence}")
    """

    def __init__(
        self,
        sources: Sequence[BasePriceSource],
        outlier_threshold: float = DEFAULT_OUTLIER_DEVIATION_THRESHOLD,
        stale_confidence_penalty: float = DEFAULT_STALE_CONFIDENCE_PENALTY,
        partial_failure_penalty: float = DEFAULT_PARTIAL_FAILURE_CONFIDENCE_PENALTY,
        magnitude_outlier_ratio: float = DEFAULT_MAGNITUDE_OUTLIER_RATIO,
        *,
        stablecoin_verify: bool = False,
        stablecoin_chainlink_check_interval: int = DEFAULT_STABLECOIN_CHAINLINK_CHECK_INTERVAL,
        stablecoin_verifier_failure_warning_threshold: int = (DEFAULT_STABLECOIN_VERIFIER_FAILURE_WARNING_THRESHOLD),
        per_source_timeout_seconds: float = DEFAULT_PER_SOURCE_TIMEOUT_SECONDS,
        global_timeout_seconds: float = DEFAULT_GLOBAL_TIMEOUT_SECONDS,
    ) -> None:
        """Initialize the PriceAggregator.

        Args:
            sources: List of BasePriceSource implementations (1 to N sources)
            outlier_threshold: Deviation threshold for outlier detection (default 0.02 = 2%)
            stale_confidence_penalty: Confidence reduction for stale data (default 0.3)
            partial_failure_penalty: Confidence reduction per failed source (default 0.1)
            magnitude_outlier_ratio: When max/min price ratio exceeds this, treat as feed
                misconfiguration (wrong units/decimals) and raise AllDataSourcesFailed
                instead of averaging garbage values. Default 100× (e.g., $3,400 vs $12.28B
                wstETH case triggers at ~3,600,000×). Set higher to allow more divergence.
            stablecoin_verify: VIB-4841 / FR-5002. When False (default), a
                stable/USD pair short-circuits to the $1.00 peg without any
                upstream call (the peg is what the aggregator returns anyway
                after outlier-discarding). When True, the proactive fast-path is
                disabled and every stablecoin price goes through the full
                multi-source aggregate — use when you must verify the live peg
                (e.g. de-peg monitoring). Off by default to cut the per-iteration
                price-call count. Operators set
                ``ALMANAK_GATEWAY_STABLECOIN_VERIFY=true`` to flip it.
            stablecoin_chainlink_check_interval: When the fast-path is active,
                run an on-chain Chainlink peg sanity check on 1-in-N peg-served
                calls (default 50). The check is latency-bounded
                (``STABLECOIN_PEG_CHECK_TIMEOUT_SECONDS``). When it completes and
                detects a de-peg the fast-path FAILS CLOSED — it returns the live
                on-chain price, or falls through to the full multi-source
                aggregate if no usable on-chain price is available; it never
                masks a de-peg by returning $1.00. When the check times out or
                cannot run, the peg is returned best-effort. Non-positive values
                disable the periodic check.
            stablecoin_verifier_failure_warning_threshold: Consecutive failed
                on-chain verification attempts per token before emitting one
                WARNING for that outage streak (default 3). A successful
                verifier call resets the token's streak. Non-positive values
                disable the warning; verifier failures remain DEBUG and the peg
                remains best-effort.
            per_source_timeout_seconds: VIB-5375 (RC-3). Wall-clock bound applied
                to EACH source's ``get_price`` coroutine. A source that exceeds
                this is recorded as an error (Empty≠Zero — "unmeasured", never a
                zero price) and does not sink the aggregation. Default 10s — above
                each source's internal single-request HTTP timeout (so a
                healthy-but-slow source is not double-cut) yet well under the 30s
                ``decide()`` budget. Non-positive disables the per-source bound.
            global_timeout_seconds: VIB-5375 (RC-3). Wall-clock bound on the whole
                concurrent gather across all sources, so many slow sources cannot
                stack wall-time past a known limit. On the global cutoff, any
                source that hasn't returned is recorded as a timeout error and the
                aggregator proceeds with whatever valid results arrived. Default
                15s — leaves ≥15s of the 30s ``decide()`` budget for the rest of
                ``decide()`` and fits inside the 60s pre-warm window. Non-positive
                disables the global bound. Applied independently of
                ``per_source_timeout_seconds`` — whichever bound fires first caps
                the wall-time.

        Raises:
            ValueError: If sources list is empty
        """
        if not sources:
            raise ValueError("At least one price source is required")

        self._sources = list(sources)
        # VIB-5375: timeouts are non-negative wall-clock seconds; <=0 disables the
        # respective bound. The two bounds are applied INDEPENDENTLY — per-source
        # cuts one slow source, global caps the whole concurrent gather — so the
        # effective wall-time is whichever bound fires first. We deliberately do
        # NOT coerce one against the other: an operator who sets a tight global
        # ceiling means it, and one who sets a tight per-source bound means that.
        self._per_source_timeout_seconds = max(0.0, per_source_timeout_seconds)
        self._global_timeout_seconds = max(0.0, global_timeout_seconds)
        self._outlier_threshold = outlier_threshold
        self._stale_confidence_penalty = stale_confidence_penalty
        self._partial_failure_penalty = partial_failure_penalty
        self._magnitude_outlier_ratio = magnitude_outlier_ratio
        self._single_source_price_ceiling = DEFAULT_SINGLE_SOURCE_PRICE_CEILING

        # VIB-4841 / FR-5002 stablecoin peg fast-path configuration.
        self._stablecoin_verify = stablecoin_verify
        self._stablecoin_chainlink_check_interval = stablecoin_chainlink_check_interval
        self._stablecoin_verifier_failure_warning_threshold = max(
            0,
            stablecoin_verifier_failure_warning_threshold,
        )
        self._stablecoin_verifier = self._select_stablecoin_verifier(self._sources)
        # Counter of peg-served calls; drives the 1-in-N Chainlink sanity check.
        self._stablecoin_peg_calls = 0
        # VIB-4841 (Codex re-audit): per-token de-peg latch. With the 1-in-N
        # check, a stable that de-pegs on the sampled call would otherwise be
        # served at $1.00 again for the next ~N-1 (non-sampled) calls. Once a
        # de-peg is detected we LATCH the exact ``(chain, address)`` identity:
        # while latched the peg fast-path is suppressed (every call falls
        # through to the live price / full aggregate) AND the on-chain sanity
        # check runs every call so recovery is detected promptly. The latch is
        # cleared when the on-chain price returns within the peg threshold.
        self._depegged_tokens: set[tuple[str, str]] = set()
        # Per-token outage streaks for the measured peg verifier. The WARNING
        # fires only when a streak first reaches the configured threshold, so a
        # long outage cannot spam logs; a successful verifier call clears it.
        self._stablecoin_verifier_failures: dict[tuple[str, str], int] = {}

        # Health metrics per source
        self._health_metrics: dict[str, SourceHealthMetrics] = {
            source.source_name: SourceHealthMetrics(source_name=source.source_name) for source in sources
        }

        # Per-call diagnostics: stores last aggregation details per token/quote pair
        self._last_details: dict[str, dict[str, Any]] = {}

        logger.info(
            "Initialized PriceAggregator",
            extra={
                "source_count": len(sources),
                "sources": [s.source_name for s in sources],
                "outlier_threshold": outlier_threshold,
                "magnitude_outlier_ratio": magnitude_outlier_ratio,
                "stablecoin_verify": stablecoin_verify,
                "stablecoin_chainlink_check_interval": stablecoin_chainlink_check_interval,
                "stablecoin_verifier_failure_warning_threshold": (self._stablecoin_verifier_failure_warning_threshold),
                "per_source_timeout_seconds": self._per_source_timeout_seconds,
                "global_timeout_seconds": self._global_timeout_seconds,
            },
        )

    @staticmethod
    def _select_stablecoin_verifier(
        sources: Sequence[BasePriceSource],
    ) -> StablecoinVerificationSource | None:
        """Select and validate the measured stablecoin verifier at wiring time."""
        for source in sources:
            # Require an explicit capability declaration. Dynamic test doubles
            # such as MagicMock synthesize arbitrary truthy attributes and must
            # not accidentally opt into the verifier contract.
            if getattr(source, "provides_stablecoin_verification", False) is not True:
                continue
            if not isinstance(source, StablecoinVerificationSource):
                raise TypeError(f"Price source {source.source_name!r} has an invalid stablecoin verifier contract")
            try:
                inspect.signature(source.get_price).bind(
                    "TOKEN",
                    "USD",
                    resolved_token=None,
                    bypass_stablecoin_fallback=True,
                )
            except (TypeError, ValueError) as exc:
                raise TypeError(
                    f"Price source {source.source_name!r} has an incompatible stablecoin verifier signature"
                ) from exc
            return source
        return None

    @property
    def sources(self) -> list[BasePriceSource]:
        """Return the list of configured price sources."""
        return self._sources.copy()

    async def get_aggregated_price(
        self,
        token: str,
        quote: str = "USD",
        *,
        resolved_token: ResolvedToken | None = None,
    ) -> PriceResult:
        """Get aggregated price from multiple sources.

        Fetches prices from all configured sources concurrently, filters outliers,
        and returns the median price with adjusted confidence.

        Args:
            token: Token symbol to get price for (e.g., "ETH", "WETH")
            quote: Quote currency (default "USD")
            resolved_token: Pre-resolved token with contract address for
                address-based lookup instead of symbol matching.

        Returns:
            PriceResult with aggregated price and confidence score

        Raises:
            AllDataSourcesFailed: If all sources fail to provide data
        """
        # VIB-4841 / FR-5002: proactive stablecoin peg fast-path. A stable/USD
        # pair returns $1.00 immediately without any upstream call (the peg is
        # what the aggregate returns anyway after outlier-discarding). Disabled
        # when stablecoin_verify is set. A low-frequency Chainlink sanity check
        # still runs to surface a de-peg.
        peg_result = await self._maybe_stablecoin_peg(token, quote, resolved_token)
        if peg_result is not None:
            return peg_result

        logger.debug(
            "Getting aggregated price for %s/%s from %d sources",
            token,
            quote,
            len(self._sources),
        )

        # Fetch from all sources concurrently
        results = await self._fetch_all_sources(token, quote, resolved_token=resolved_token)

        # Store per-call diagnostics BEFORE the failure check so that
        # get_last_details() is populated even when all sources fail.
        detail_key = f"{token.upper()}/{quote.upper()}"
        self._last_details[detail_key] = {
            "sources_ok": [r.source for r in results.valid_results],
            "sources_failed": results.errors,
            "outliers": [r.source for r in results.outliers],
        }

        # Check if all sources failed
        if not results.valid_results:
            # Synthetic fallback is authorized only by an exact registry identity.
            peg_identity = _peg_identity(resolved_token)
            peg_value = is_pegged(resolved_token.token_ref) if resolved_token is not None else None
            if (
                quote.upper() == "USD"
                and peg_value is not None
                and not self._stablecoin_verify
                and peg_identity not in self._depegged_tokens
            ):
                assert peg_identity is not None
                logger.warning(
                    "All price sources failed for stablecoin %s/%s, using %s fallback. Errors: %s",
                    token,
                    quote,
                    peg_value,
                    results.errors,
                )
                return PriceResult(
                    price=peg_value,
                    source="stablecoin_fallback",
                    timestamp=datetime.now(UTC),
                    confidence=0.8,
                    stale=False,
                    peg_tokens=(f"{peg_identity[0]}:{peg_identity[1]}",),
                )

            log_fn = logger.warning if _is_known_unpriceable(token) else logger.error
            log_fn(
                "All data sources failed for %s/%s: %s",
                token,
                quote,
                results.errors,
            )
            raise AllDataSourcesFailed(errors=results.errors)

        # Calculate confidence based on results
        confidence = self._calculate_confidence(results)

        # Determine staleness
        stale = any(r.stale for r in results.valid_results)

        # Log aggregation result (round price to 6 significant figures for readability)
        display_price = f"{results.price:.6g}" if results.price else str(results.price)
        logger.info(
            "Aggregated price for %s/%s: %s (confidence: %.2f, sources: %d/%d, outliers: %d)",
            token,
            quote,
            display_price,
            confidence,
            len(results.valid_results),
            len(self._sources),
            len(results.outliers),
        )

        # Log outliers with structured fields for alerting.
        # Extreme deviations (>100%) are logged at DEBUG — they're almost certainly
        # bad upstream data (e.g. DexScreener stablecoin mispricing) and create
        # noise at WARNING level.  Moderate deviations stay at WARNING.
        if results.outliers:
            for outlier in results.outliers:
                median = results.price
                deviation_pct = abs(outlier.price - median) / median * 100 if median else 0
                log_fn = logger.debug if deviation_pct > 100 else logger.warning
                log_fn(
                    "Outlier detected from %s: %s (median: %s, deviation: %.1f%%)",
                    outlier.source,
                    outlier.price,
                    results.price,
                    deviation_pct,
                )

        peg_tokens = tuple(sorted({identity for result in results.valid_results for identity in result.peg_tokens}))
        return PriceResult(
            price=results.price,
            source="aggregated",
            timestamp=datetime.now(UTC),
            confidence=confidence,
            stale=stale,
            peg_tokens=peg_tokens,
        )

    async def _maybe_stablecoin_peg(
        self,
        token: str,
        quote: str,
        resolved_token: ResolvedToken | None,
    ) -> PriceResult | None:
        """Return the $1.00 peg for a stable/USD pair, or None to fall through.

        VIB-4841 / FR-5002. Short-circuits the multi-source aggregate for
        USD-pegged stablecoins: with the fast-path active we return the peg
        without any upstream price call (CoinGecko/Binance/DexScreener were
        being hit every iteration only to confirm ~$1.00). The aggregator's own
        all-sources-failed fallback already returns $1.00 for these tokens, so
        the fast-path doesn't change the value — only the cost.

        Returns ``None`` (fall through to the full aggregate) when:
          - ``stablecoin_verify`` is set (operator wants the live peg verified), or
          - the quote currency isn't USD, or
          - the token isn't a recognised fallback stablecoin, or
          - the periodic on-chain sanity check detects a de-peg but could not
            obtain a usable live price to return in the peg's place, or
          - the token is currently LATCHED as de-pegged (the on-chain check
            could not confirm recovery on this call) — fall through so every
            call gets the real price, not just the sampled one.

        Fail-closed de-peg handling (VIB-4841, Codex review): a low-frequency,
        latency-bounded Chainlink on-chain sanity check runs on the fast-path.
        When it COMPLETES and detects the stable trading off-peg, the fast-path
        must NOT mask the de-peg by returning $1.00 — instead it returns the
        live on-chain price, or (if that price is unusable) falls through to the
        full multi-source aggregate. The peg is only returned when the check
        passes, times out, or cannot run AND the token is not latched.

        De-peg latch (VIB-4841, Codex re-audit): detecting a de-peg on the
        sampled call is not enough — with the 1-in-N cadence the next ~N-1 calls
        would skip the check and serve $1.00 again for a known-de-pegged token.
        So a detected de-peg LATCHES the token: while latched the peg is
        suppressed on EVERY call and the on-chain check runs every call (not
        just 1-in-N) so recovery is detected promptly. The latch clears when the
        on-chain price returns within ``PEG_DEVIATION_THRESHOLD_BPS``.
        """
        if self._stablecoin_verify:
            return None
        if quote.upper() != "USD":
            return None
        peg_identity = _peg_identity(resolved_token)
        if peg_identity is None:
            return None
        assert resolved_token is not None
        peg_value = is_pegged(resolved_token.token_ref)
        if peg_value is None:
            return None

        self._stablecoin_peg_calls += 1
        # Latency-bounded on-chain sanity check. Runs on the 1-in-N cadence for
        # healthy tokens, but EVERY call while a token is latched de-pegged.
        # Returns the live price when a de-peg is detected (also sets the latch),
        # else None (passed -> latch cleared / timed out / could not run / not
        # this call's turn). Whether the peg may be served also depends on the
        # latch state AFTER the check (see below).
        depeg_result = await self._maybe_check_stablecoin_peg_onchain(token, resolved_token)
        if depeg_result is not None:
            # Fail closed: a confirmed de-peg must never be masked by the peg.
            if depeg_result.price > 0:
                logger.warning(
                    "Stablecoin %s/%s fast-path returning LIVE on-chain price %s instead of the "
                    "$1.00 peg — de-peg detected by the on-chain sanity check.",
                    token,
                    quote,
                    depeg_result.price,
                )
                return depeg_result
            # De-peg detected but no usable live price — fall through to the
            # full multi-source aggregate rather than returning the peg.
            logger.warning(
                "Stablecoin %s/%s de-peg detected but on-chain price unusable (%s); "
                "falling through to the full multi-source aggregate.",
                token,
                quote,
                depeg_result.price,
            )
            return None

        # No de-peg confirmed on THIS call. If the token is still latched as
        # de-pegged (the check was skipped/timed-out/errored this call and so
        # could not confirm recovery), do NOT serve the peg — fall through to
        # the full aggregate so the caller gets the real price every call, not
        # just on sampled calls. The latch only clears when a completed on-chain
        # check shows the price back within threshold (handled in the check).
        if peg_identity in self._depegged_tokens:
            logger.warning(
                "Stablecoin %s/%s is LATCHED de-pegged; suppressing the $1.00 peg "
                "fast-path and falling through to the full multi-source aggregate "
                "until an on-chain check confirms recovery.",
                token,
                quote,
            )
            return None

        logger.debug(
            "Stablecoin peg fast-path for %s/%s -> %s (no upstream call; "
            "set ALMANAK_GATEWAY_STABLECOIN_VERIFY=true to verify live)",
            token,
            quote,
            peg_value,
        )
        return PriceResult(
            price=peg_value,
            source="stablecoin_peg",
            timestamp=datetime.now(UTC),
            confidence=0.95,
            stale=False,
            peg_tokens=(f"{peg_identity[0]}:{peg_identity[1]}",),
        )

    async def _maybe_check_stablecoin_peg_onchain(
        self,
        token: str,
        resolved_token: ResolvedToken | None,
    ) -> PriceResult | None:
        """Verify the stable is ~$1.00 on-chain on the 1-in-N cadence (or every
        call while the token is latched de-pegged) and maintain the de-peg latch.

        Latency-bounded, fail-closed de-peg detector for the fast-path. Reads
        the live price from the on-chain (Chainlink) source if one is wired into
        the aggregator, bounding the inline await with
        ``STABLECOIN_PEG_CHECK_TIMEOUT_SECONDS`` so a slow RPC cannot stall the
        "fast" path.

        Cadence: runs on the 1st, (1+N)th, (1+2N)th, ... peg-served call for a
        healthy token. While the token is LATCHED de-pegged the cadence is
        overridden and the check runs on EVERY call so recovery is detected
        promptly (VIB-4841, Codex re-audit).

        Latch maintenance (VIB-4841, Codex re-audit):
            - Any measured off-peg result adds the token to
              ``self._depegged_tokens`` (keyed by ``(chain, address)``), even if
              the observation is stale or low-confidence.
            - Only a fresh, high-confidence result back within
              ``PEG_DEVIATION_THRESHOLD_BPS`` removes the token from the
              latch, so the peg fast-path resumes.
            - A check that times out / errors / is skipped does NOT change the
              latch — a latched token stays latched until a check confirms
              recovery.

        Returns:
            - The live on-chain :class:`PriceResult` when a fresh,
              high-confidence check detects a de-peg (drift past
              ``PEG_DEVIATION_THRESHOLD_BPS``). The caller fails closed
              on this — it must not return the peg.
            - ``None`` when the check passes (on-peg), is skipped (not this
              call's turn and not latched, no on-chain source wired, interval
              disabled), times out, produces stale/low-confidence evidence, or
              the source errors. The caller then serves the peg best-effort
              UNLESS the token is still latched.

        Never raises. Egress here is correct: this is the gateway layer and the
        on-chain source already owns its RPC path.
        """
        interval = self._stablecoin_chainlink_check_interval
        peg_identity = _peg_identity(resolved_token)
        if peg_identity is None:
            return None
        assert resolved_token is not None
        peg_value = is_pegged(resolved_token.token_ref)
        if peg_value is None:
            return None
        latched = peg_identity in self._depegged_tokens
        # While latched, the check runs every call (override the cadence) so a
        # re-peg is detected promptly and the fast-path can resume. A non-latched
        # token with a disabled interval skips the check entirely.
        if not latched:
            if interval <= 0:
                return None
            # Run on the 1st, (1+N)th, (1+2N)th, ... peg-served call so the very
            # first fast-path hit is sanity-checked, then every Nth thereafter.
            # ``(calls - 1) % interval`` is 0 on exactly those calls and, unlike
            # ``calls % interval == 1``, also fires every call when interval == 1.
            if (self._stablecoin_peg_calls - 1) % interval != 0:
                return None

        onchain_source = self._stablecoin_verifier
        if onchain_source is None:
            return None

        try:
            # Latency-bounded: a slow RPC must not stall the fast-path. On
            # timeout we treat the check as "could not run" and return the peg
            # best-effort (the 1-in-N cadence keeps steady-state latency low).
            result = await asyncio.wait_for(
                onchain_source.get_price(
                    token,
                    "USD",
                    resolved_token=resolved_token,
                    bypass_stablecoin_fallback=True,
                ),
                timeout=STABLECOIN_PEG_CHECK_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            self._record_stablecoin_verifier_failure(token, resolved_token, "timeout")
            logger.debug(
                "Stablecoin peg on-chain sanity check timed out for %s after %.1fs; returning the peg best-effort.",
                token,
                STABLECOIN_PEG_CHECK_TIMEOUT_SECONDS,
            )
            return None
        except Exception as exc:
            self._record_stablecoin_verifier_failure(token, resolved_token, type(exc).__name__)
            # On-chain feed unavailable (no RPC, no feed, Anvil) — not a de-peg
            # signal, so stay quiet at DEBUG. The peg is still returned upstream.
            logger.debug(
                "Stablecoin peg on-chain sanity check skipped for %s: %s",
                token,
                exc,
            )
            return None

        self._reset_stablecoin_verifier_failures(token, resolved_token)
        deviation_bps = peg_deviation_bps(result.price, peg_value)
        deviation_pct = float(deviation_bps / Decimal("100"))
        execution_grade = not result.stale and result.confidence >= STABLECOIN_VERIFIER_MIN_CONFIDENCE
        if not is_within_peg(result.price, peg_value):
            # Any measured off-peg evidence suppresses the synthetic peg. A
            # stale/low-confidence result is not returned directly; the latch
            # makes the caller fall through to the full aggregate instead.
            self._depegged_tokens.add(peg_identity)
            logger.warning(
                "Stablecoin %s may be DE-PEGGED: Chainlink reports %s (%.2f%% off $1.00 peg). "
                "Fast-path is FAILING CLOSED and the token is now LATCHED — every subsequent call "
                "returns the live on-chain price or falls through to the full aggregate until an "
                "on-chain check confirms recovery.",
                token,
                result.price,
                deviation_pct,
            )
            return result if execution_grade else None

        if not execution_grade:
            logger.debug(
                "Stablecoin peg on-chain recovery evidence rejected for %s: stale=%s confidence=%.3f",
                token,
                result.stale,
                result.confidence,
            )
            return None

        # On-peg: clear any prior latch so the fast-path resumes. ``discard``
        # is a no-op when the token was never latched.
        if peg_identity in self._depegged_tokens:
            self._depegged_tokens.discard(peg_identity)
            logger.warning(
                "Stablecoin %s has RE-PEGGED: Chainlink=%s (within %.1f%% of $1.00). "
                "Clearing the de-peg latch — the $1.00 peg fast-path resumes.",
                token,
                result.price,
                float(PEG_DEVIATION_THRESHOLD_BPS / Decimal("100")),
            )
        else:
            logger.debug(
                "Stablecoin peg sanity check OK for %s: Chainlink=%s (within %.1f%% of $1.00)",
                token,
                result.price,
                float(PEG_DEVIATION_THRESHOLD_BPS / Decimal("100")),
            )
        return None

    def _record_stablecoin_verifier_failure(
        self,
        token: str,
        resolved_token: ResolvedToken | None,
        reason: str,
    ) -> None:
        """Record one failed measured peg check and warn once per outage streak."""
        token_key = _peg_identity(resolved_token)
        if token_key is None:
            return
        failures = self._stablecoin_verifier_failures.get(token_key, 0) + 1
        self._stablecoin_verifier_failures[token_key] = failures

        threshold = self._stablecoin_verifier_failure_warning_threshold
        if threshold > 0 and failures == threshold:
            logger.warning(
                "Stablecoin peg verifier unavailable for %s after %d consecutive checks "
                "(latest failure: %s). Continuing the synthetic $1.00 peg best-effort for "
                "unlatched tokens; the outage warning is suppressed until verification recovers.",
                f"{token_key[0]}:{token_key[1]}",
                failures,
                reason,
            )

    def _reset_stablecoin_verifier_failures(
        self,
        token: str,
        resolved_token: ResolvedToken | None,
    ) -> None:
        """Clear a token's verifier outage streak after any completed check."""
        token_key = _peg_identity(resolved_token)
        if token_key is None:
            return
        failures = self._stablecoin_verifier_failures.pop(token_key, 0)
        if failures:
            logger.debug(
                "Stablecoin peg verifier recovered for %s after %d consecutive failed checks.",
                f"{token_key[0]}:{token_key[1]}",
                failures,
            )

    async def _fetch_all_sources(
        self,
        token: str,
        quote: str,
        *,
        resolved_token: ResolvedToken | None = None,
    ) -> AggregationResult:
        """Fetch prices from all sources concurrently.

        Args:
            token: Token symbol
            quote: Quote currency
            resolved_token: Pre-resolved token with contract address for
                address-based lookup instead of symbol matching.

        Returns:
            AggregationResult with valid results, outliers, and errors
        """
        # VIB-5375 (RC-3): bound the whole fan-out. Each source's coroutine is
        # wrapped in a PER-SOURCE timeout so one slow/hanging source (e.g. a cold
        # Mantle RPC) is cut off and recorded as an error rather than stalling the
        # decide() budget. The concurrent gather is additionally bounded by a
        # GLOBAL timeout so many slow sources cannot stack wall-time past a known
        # limit. A timed-out source is "unmeasured" (Empty≠Zero) — recorded as an
        # error, never a zero price — and never sinks the aggregation: whatever
        # valid results arrived still win.
        task_results = await self._gather_bounded(token, quote, resolved_token=resolved_token)

        # Separate successes and failures
        valid_results: list[PriceResult] = []
        errors: dict[str, str] = {}

        for source, result in zip(self._sources, task_results, strict=False):
            if isinstance(result, Exception):
                errors[source.source_name] = str(result)
            elif isinstance(result, PriceResult):
                valid_results.append(result)
            else:
                errors[source.source_name] = f"Unexpected result type: {type(result)}"

        # A provider may manufacture the exact registry peg itself (Binance,
        # HyperCore, Chainlink, CoinGecko). During explicit verification, or
        # while an identity is depeg-latched, those synthetic observations are
        # not independent market evidence and must never outvote the measured
        # depeg that disabled the aggregate-level fast path.
        peg_identity = _peg_identity(resolved_token)
        suppress_synthetic = (
            quote.upper() == "USD"
            and peg_identity is not None
            and (self._stablecoin_verify or peg_identity in self._depegged_tokens)
        )
        if suppress_synthetic:
            measured_results: list[PriceResult] = []
            for result in valid_results:
                if result.peg_tokens:
                    errors[result.source] = "synthetic peg excluded while live peg verification is required"
                else:
                    measured_results.append(result)
            valid_results = measured_results

        # If no valid results, return early
        if not valid_results:
            return AggregationResult(
                price=Decimal("0"),
                valid_results=[],
                outliers=[],
                errors=errors,
            )

        # Single source: return as-is, with absolute price ceiling check
        if len(valid_results) == 1:
            price = valid_results[0].price
            if price > self._single_source_price_ceiling:
                source_name = valid_results[0].source
                logger.error(
                    "Single-source price from %s exceeds ceiling: %s > %s. "
                    "Likely feed misconfiguration (wrong decimals or exchange rate vs USD). "
                    "Rejecting to prevent corrupted price from reaching strategies.",
                    source_name,
                    price,
                    self._single_source_price_ceiling,
                )
                ceiling_errors = {
                    source_name: (
                        f"Price {price} exceeds single-source ceiling of {self._single_source_price_ceiling}"
                    ),
                }
                ceiling_errors.update(errors)
                raise AllDataSourcesFailed(errors=ceiling_errors)
            return AggregationResult(
                price=price,
                valid_results=valid_results,
                outliers=[],
                errors=errors,
            )

        # Multiple sources: detect outliers and compute median
        return self._aggregate_multiple(valid_results, errors)

    async def _gather_bounded(
        self,
        token: str,
        quote: str,
        *,
        resolved_token: ResolvedToken | None = None,
    ) -> list[PriceResult | Exception]:
        """Fetch all sources concurrently under per-source + global timeouts.

        VIB-5375 (RC-3). Returns a list positionally aligned with ``self._sources``
        (same contract as the previous ``asyncio.gather(..., return_exceptions=True)``):
        each entry is the source's :class:`PriceResult`, or an :class:`Exception`
        when the source failed OR was cut off by the per-source / global timeout.

        Two bounds, both fail-soft (a timeout is recorded as an error, never sinks
        the aggregation, and is "unmeasured" — never a zero price):

        - **Per-source** (``_per_source_timeout_seconds``): each source coroutine is
          wrapped in :func:`asyncio.wait_for`, so one slow/hanging source is cut off
          independently while the others keep running.
        - **Global** (``_global_timeout_seconds``): the concurrent set is awaited
          with :func:`asyncio.wait(..., timeout=...)`. On the global cutoff, any
          source that has not finished is cancelled and recorded as a timeout error,
          and the aggregator proceeds with whatever results arrived. This preserves
          partial results — unlike wrapping the whole gather in ``wait_for``, which
          would discard everything on the first slow source.
        """
        tasks: list[asyncio.Task[PriceResult]] = [
            asyncio.ensure_future(self._fetch_with_timeout(source, token, quote, resolved_token=resolved_token))
            for source in self._sources
        ]

        global_timeout = self._global_timeout_seconds if self._global_timeout_seconds > 0 else None
        # ``asyncio.wait`` (not ``wait_for``) so that a global cutoff still leaves
        # the already-completed tasks' results retrievable.
        done, pending = await asyncio.wait(tasks, timeout=global_timeout)

        results: list[PriceResult | Exception] = []
        for source, task in zip(self._sources, tasks, strict=True):
            if task in pending:
                # Global cutoff reached before this source returned. Cancel it and
                # record a timeout error so it counts as "unmeasured", not zero.
                task.cancel()
                metrics = self._health_metrics[source.source_name]
                err_msg = f"Global aggregator timeout after {self._global_timeout_seconds:.1f}s"
                metrics.record_failure(err_msg)
                logger.warning(
                    "Price source %s cut off by global aggregator timeout (%.1fs) for %s/%s; "
                    "recording as unmeasured (not a zero price).",
                    source.source_name,
                    self._global_timeout_seconds,
                    token,
                    quote,
                )
                results.append(TimeoutError(err_msg))
                continue
            try:
                results.append(task.result())
            except (Exception, asyncio.CancelledError) as exc:  # noqa: BLE001 - mirror gather(return_exceptions=True)
                results.append(exc if isinstance(exc, Exception) else TimeoutError(str(exc)))

        # Drain cancellations so the cancelled coroutines don't leak warnings.
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        return results

    async def _fetch_with_timeout(
        self,
        source: BasePriceSource,
        token: str,
        quote: str,
        *,
        resolved_token: ResolvedToken | None = None,
    ) -> PriceResult:
        """Wrap :meth:`_fetch_with_metrics` in the per-source wall-clock bound.

        VIB-5375 (RC-3). A source whose ``get_price`` coroutine exceeds
        ``_per_source_timeout_seconds`` is cut off here and the timeout is recorded
        as a source failure (so health metrics and the per-call diagnostics reflect
        it). The :class:`TimeoutError` propagates to ``_gather_bounded`` which treats
        it like any other source error. Non-positive timeout disables the bound.
        """
        if self._per_source_timeout_seconds <= 0:
            return await self._fetch_with_metrics(source, token, quote, resolved_token=resolved_token)
        try:
            return await asyncio.wait_for(
                self._fetch_with_metrics(source, token, quote, resolved_token=resolved_token),
                timeout=self._per_source_timeout_seconds,
            )
        except TimeoutError:
            err_msg = f"Per-source timeout after {self._per_source_timeout_seconds:.1f}s"
            self._health_metrics[source.source_name].record_failure(err_msg)
            logger.warning(
                "Price source %s exceeded per-source timeout (%.1fs) for %s/%s; "
                "recording as unmeasured (not a zero price).",
                source.source_name,
                self._per_source_timeout_seconds,
                token,
                quote,
            )
            raise TimeoutError(err_msg) from None

    async def _fetch_with_metrics(
        self,
        source: BasePriceSource,
        token: str,
        quote: str,
        *,
        resolved_token: ResolvedToken | None = None,
    ) -> PriceResult:
        """Fetch price from a source and track metrics.

        Args:
            source: Price source to fetch from
            token: Token symbol
            quote: Quote currency
            resolved_token: Pre-resolved token with contract address for
                address-based lookup instead of symbol matching.

        Returns:
            PriceResult from the source

        Raises:
            DataSourceError: If the source fails
        """
        metrics = self._health_metrics[source.source_name]
        start_time = time.time()

        try:
            verifier = self._stablecoin_verifier
            if (
                verifier is not None
                # Wiring validated that the protocol object is this exact
                # BasePriceSource instance; the cast expresses that runtime
                # intersection to mypy, which cannot model intersection types.
                and source is cast(BasePriceSource, verifier)
                and quote.upper() == "USD"
                and is_stablecoin_for_fallback(token, resolved_token)
                and (self._stablecoin_verify or _peg_identity(resolved_token) in self._depegged_tokens)
            ):
                # The verifier's ordinary path may synthesize the same $1 peg
                # that this aggregator is deliberately suppressing. Preserve a
                # measured price throughout the full aggregate when live
                # verification is requested or a measured de-peg is latched.
                result = await verifier.get_price(
                    token,
                    quote,
                    resolved_token=resolved_token,
                    bypass_stablecoin_fallback=True,
                )
            else:
                result = await source.get_price(token, quote, resolved_token=resolved_token)
            latency_ms = (time.time() - start_time) * 1000
            metrics.record_success(latency_ms)
            return result
        except Exception as e:
            metrics.record_failure(str(e))
            raise

    def _aggregate_multiple(
        self,
        results: list[PriceResult],
        errors: dict[str, str],
    ) -> AggregationResult:
        """Aggregate multiple price results using median and outlier detection.

        Args:
            results: List of valid PriceResults
            errors: Dict of source errors

        Returns:
            AggregationResult with median price and outlier list
        """
        # Calculate median price
        prices = [float(r.price) for r in results]
        median_price = Decimal(str(statistics.median(prices)))

        # Detect outliers (>2% deviation from median)
        valid_results: list[PriceResult] = []
        outliers: list[PriceResult] = []

        for result in results:
            deviation = abs(float(result.price) - float(median_price)) / float(median_price)
            if deviation > self._outlier_threshold:
                outliers.append(result)
                logger.debug(
                    "Flagged outlier from %s: %s (%.2f%% deviation from median %s)",
                    result.source,
                    result.price,
                    deviation * 100,
                    median_price,
                )
            else:
                valid_results.append(result)

        # If all results are outliers, check whether the divergence is due to feed
        # misconfiguration (magnitude-level disagreement) vs genuine market volatility.
        if not valid_results:
            prices_float = sorted(float(r.price) for r in results)
            min_price = prices_float[0]
            max_price = prices_float[-1]

            ratio = 0.0
            if min_price > 0:
                ratio = max_price / min_price
            elif max_price > 0:  # min_price <= 0: zero or negative price is always extreme
                ratio = float("inf")

            if ratio > self._magnitude_outlier_ratio:
                # Extreme divergence: max/min ratio far exceeds normal market volatility.
                # Likely cause: feed returning price in wrong units (e.g., wstETH/ETH
                # exchange rate decoded as USD via 8-decimal assumption gives ~$12.28B
                # while correct USD price is ~$3,400, ratio ≈ 3,600,000×).
                # Averaging these values produces nonsense -- fail explicitly.
                ratio_str = "inf" if ratio == float("inf") else f"{ratio:.0f}"
                logger.error(
                    "Extreme price divergence detected across %d sources: min=%s, max=%s "
                    "(ratio=%s× exceeds limit of %.0f×). This indicates a feed "
                    "configuration error (wrong units/decimals), not market volatility. "
                    "Raising AllDataSourcesFailed to prevent corrupted price from being used.",
                    len(results),
                    min_price,
                    max_price,
                    ratio_str,
                    self._magnitude_outlier_ratio,
                )
                magnitude_errors = {
                    r.source: (
                        f"Magnitude outlier: price={r.price} (min={min_price:.4g}, "
                        f"max={max_price:.4g}, ratio={ratio_str}×)"
                    )
                    for r in results
                }
                magnitude_errors.update(errors)
                raise AllDataSourcesFailed(errors=magnitude_errors)

            # VIB-4439 / MorphoMay15 §6.1 (F1): with EXACTLY 2 sources, "median"
            # degenerates to the arithmetic mean. If both sources are flagged
            # as outliers vs that mean (i.e. they disagree by > outlier_threshold)
            # we have no consensus and no robust statistic available — returning
            # the midpoint is the worst possible answer when one source is wrong.
            # Fail closed so the strategy halts on a bad oracle instead of trading
            # at half-price. (3+ sources still fall through to the "use all" path
            # below: with 3+ values one outlier cannot move the median past the
            # threshold, so reaching ``not valid_results`` requires the whole
            # group to genuinely disagree — i.e. volatile market conditions, not
            # one bad feed.)
            if len(results) == 2:
                sorted_results = sorted(results, key=lambda r: r.price)
                lo, hi = sorted_results[0], sorted_results[1]
                divergence_pct = float((hi.price - lo.price) / lo.price) * 100 if lo.price > 0 else float("inf")
                logger.error(
                    "Two-source price divergence: %s=%s, %s=%s (%.2f%% apart, "
                    "outlier threshold=%.2f%%, magnitude ratio=%.2f below %.0f× cap). "
                    "Cannot consensus-resolve with only 2 sources — raising "
                    "AllDataSourcesFailed to prevent corrupted price from being used.",
                    lo.source,
                    lo.price,
                    hi.source,
                    hi.price,
                    divergence_pct,
                    self._outlier_threshold * 100,
                    ratio,
                    self._magnitude_outlier_ratio,
                )
                # Symmetric error message: both sources see the same statement
                # of facts (both prices, divergence between them). Phrasing
                # "other source differs by X%" was asymmetric — for [100, 3500]
                # the lower source IS 3400% away from the higher, but the
                # higher is only ~97% away from the lower, so the same string
                # was technically wrong on one of the two error entries.
                two_source_errors = {
                    r.source: (
                        f"Two-source divergence: {lo.source}={lo.price} vs "
                        f"{hi.source}={hi.price} ({divergence_pct:.2f}% apart, "
                        f"no consensus possible)"
                    )
                    for r in results
                }
                two_source_errors.update(errors)
                raise AllDataSourcesFailed(errors=two_source_errors)

            # Normal divergence across 3+ sources (e.g., volatile market with each
            # source 15% apart). Use all results -- median is still meaningful.
            logger.warning(
                "All prices flagged as outliers, using all %d results",
                len(results),
            )
            valid_results = results
            outliers = []

        # Recalculate median after outlier removal if needed
        if outliers and valid_results:
            prices = [float(r.price) for r in valid_results]
            median_price = Decimal(str(statistics.median(prices)))

        return AggregationResult(
            price=median_price,
            valid_results=valid_results,
            outliers=outliers,
            errors=errors,
        )

    def _calculate_confidence(self, result: AggregationResult) -> float:
        """Calculate confidence score for aggregated result.

        Confidence is calculated based on:
        - Number of sources that succeeded vs failed
        - Whether any results are stale
        - Number of outliers detected

        Args:
            result: AggregationResult from aggregation

        Returns:
            Confidence score from 0.0 to 1.0
        """
        # Start with full confidence
        confidence = 1.0

        # Penalty for failed sources
        failed_count = len(result.errors)
        if failed_count > 0:
            confidence -= failed_count * self._partial_failure_penalty

        # Penalty for stale data
        stale_count = sum(1 for r in result.valid_results if r.stale)
        if stale_count > 0:
            stale_ratio = stale_count / len(result.valid_results)
            confidence -= stale_ratio * self._stale_confidence_penalty

        # Small penalty for outliers (data quality concern)
        if result.outliers:
            outlier_penalty = len(result.outliers) * 0.05
            confidence -= outlier_penalty

        # If single source, use its confidence directly (with stale penalty if applicable)
        if len(result.valid_results) == 1:
            single_confidence = result.valid_results[0].confidence
            if result.valid_results[0].stale:
                single_confidence *= 1 - self._stale_confidence_penalty
            confidence = min(confidence, single_confidence)

        # Clamp to valid range
        return max(0.0, min(1.0, confidence))

    def get_source_health(self, source_name: str) -> dict[str, Any] | None:
        """Get health metrics for a specific source.

        Args:
            source_name: Name of the source to query

        Returns:
            Dictionary with health metrics, or None if source unknown
        """
        metrics = self._health_metrics.get(source_name)
        if metrics is None:
            return None
        return metrics.to_dict()

    def get_all_source_health(self) -> dict[str, dict[str, Any]]:
        """Get health metrics for all sources.

        Returns:
            Dictionary mapping source names to their health metrics
        """
        return {name: metrics.to_dict() for name, metrics in self._health_metrics.items()}

    def get_last_details(self, token: str, quote: str = "USD") -> dict[str, Any] | None:
        """Get per-source diagnostics from the last aggregation call for a token pair.

        Returns:
            Dict with sources_ok, sources_failed, and outliers lists, or None if
            no aggregation has been performed for this pair yet.
        """
        return self._last_details.get(f"{token.upper()}/{quote.upper()}")

    def reset_health_metrics(self, source_name: str | None = None) -> None:
        """Reset health metrics for one or all sources.

        Args:
            source_name: Specific source to reset, or None to reset all
        """
        if source_name is not None:
            if source_name in self._health_metrics:
                self._health_metrics[source_name] = SourceHealthMetrics(source_name=source_name)
        else:
            for name in self._health_metrics:
                self._health_metrics[name] = SourceHealthMetrics(source_name=name)

    async def close(self) -> None:
        """Close all underlying price sources.

        This should be called when the aggregator is no longer needed
        to properly release resources (HTTP sessions, etc.).
        """
        for source in self._sources:
            if hasattr(source, "close"):
                try:
                    await source.close()
                except Exception as e:
                    logger.warning("Error closing source %s: %s", source.source_name, e)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "PriceAggregator",
    "SourceHealthMetrics",
    "AggregationResult",
]
