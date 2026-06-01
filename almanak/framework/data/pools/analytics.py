"""Pool analytics - thin gRPC client over the gateway's PoolAnalyticsService.

This module used to do its own HTTP egress to DefiLlama / GeckoTerminal,
which violated the gateway-boundary rule (AGENTS.md "Gateway boundary":
strategy containers have no outbound network access except the gateway
gRPC channel). VIB-4727 moves all HTTP egress server-side; this module
becomes a thin client that translates gRPC responses into the typed
``DataEnvelope[PoolAnalytics]`` framework shape.

Public surface:

- ``PoolAnalytics`` — dataclass returned inside ``DataEnvelope``.
- ``PoolAnalyticsResult`` — kept for ``best_pool()`` API parity; the
  method itself is deferred to a follow-up (see ``best_pool`` docstring).
- ``PoolAnalyticsReader`` — the live reader, takes a ``GatewayClient``
  and translates gRPC responses.
- ``NullPoolAnalyticsReader`` — deterministic stub for backtest factories
  (``for_pnl_backtest_state``, ``for_paper_fork``). Always raises
  ``DataSourceUnavailable("backtest")`` so backtests don't make
  analytics-driven decisions and stay reproducible across runs.

HOLD contract: any caller that catches ``PoolAnalyticsUnavailableError``
(or ``DataSourceUnavailable``) MUST either re-raise it or return
``Intent.hold(...)``. A bare catch breaks the runner's HOLD inference
via ``classify_failure`` walking ``__cause__``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING

import grpc

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.models import (
    DataClassification,
    DataEnvelope,
    DataMeta,
)

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


# =============================================================================
# Data Models (public API shape — preserved across the gRPC migration)
# =============================================================================


@dataclass(frozen=True)
class PoolAnalytics:
    """Analytics for a single pool.

    Attributes:
        pool_address: Pool contract address.
        chain: Chain name.
        protocol: Protocol name (e.g. ``"uniswap_v3"``).
        tvl_usd: Total value locked in USD. For backwards compatibility
            with the pre-VIB-4727 callers, ``tvl_usd`` is non-Optional and
            an unmeasured TVL surfaces as ``Decimal("0")``. To distinguish
            measured-zero from unmeasured, inspect ``unmeasured_fields``
            below (preferred — explicit) or ``DataMeta.confidence`` (decays
            from the baseline 0.85 by ~0.15 per unmeasured field).
        volume_24h_usd: 24-hour trading volume in USD.
        volume_7d_usd: 7-day trading volume in USD.
        fee_apr: Annualized fee return as a percentage.
        fee_apy: Compounded annual fee return as a percentage.
        utilization_rate: Utilization for lending pools (0.0-1.0), None for DEX.
        token0_weight: Fraction of TVL in token0 (0.0-1.0). ``0.5`` is the
            balanced default for unmeasured weights; ``0.0`` from a 0/100
            pool is preserved (Empty != Zero).
        token1_weight: See ``token0_weight``.
        unmeasured_fields: Names of money-critical fields the upstream
            provider did NOT measure (i.e. came back as empty-string on
            the wire). A field appearing here means the corresponding
            ``Decimal("0")`` / ``0.0`` value is a placeholder, not a
            measurement. Used by callers that need to skip pools with
            unknown TVL rather than treating them as $0 TVL. Money-
            critical fields tracked: ``tvl_usd``, ``volume_24h_usd``,
            ``volume_7d_usd``, ``fee_apr``, ``fee_apy``.
    """

    pool_address: str
    chain: str
    protocol: str
    tvl_usd: Decimal
    volume_24h_usd: Decimal
    volume_7d_usd: Decimal
    fee_apr: float
    fee_apy: float
    utilization_rate: float | None = None
    token0_weight: float = 0.5
    token1_weight: float = 0.5
    unmeasured_fields: frozenset[str] = frozenset()


@dataclass(frozen=True)
class PoolAnalyticsResult:
    """Result from ``best_pool()`` with pool address and analytics."""

    pool_address: str
    analytics: PoolAnalytics
    metric_value: float
    metric_name: str


# =============================================================================
# Wire-shape decoding helpers (decimal-as-string → Decimal / float)
# =============================================================================


def _decimal_or_zero(value: str) -> Decimal:
    """Parse a decimal-string field from the proto, returning ``Decimal(0)`` for empty.

    The gateway uses ``""`` to mean "not measured by this provider" (per
    AGENTS.md "Empty ≠ Zero"). At the framework boundary the public
    ``PoolAnalytics`` dataclass is non-Optional on these fields, so we
    surface unmeasured as ``Decimal(0)`` — callers needing the distinction
    should inspect the envelope's ``DataMeta.confidence`` or the response
    ``source`` field. New callers under VIB-4727 should treat ``Decimal(0)``
    as "the gateway has no signal for this," not "the value is exactly zero."
    """
    if not value:
        return Decimal(0)
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError, TypeError):
        logger.debug("pool_analytics: dropped unparseable decimal wire value %r", value)
        return Decimal(0)


def _float_or_zero(value: str) -> float:
    if not value:
        return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


# =============================================================================
# PoolAnalyticsReader — thin gRPC client
# =============================================================================


class PoolAnalyticsReader:
    """Thin gRPC client over the gateway's ``PoolAnalyticsService``.

    This class no longer owns any HTTP egress. All upstream provider
    fetching (DefiLlama, GeckoTerminal) happens inside the gateway
    sidecar. The constructor REQUIRES a connected ``GatewayClient`` —
    constructing one without it deliberately raises ``TypeError`` so any
    stale ``PoolAnalyticsReader()`` call from before VIB-4727 fails loudly.

    Args:
        gateway_client: The connected gateway client.
        timeout_seconds: gRPC call timeout (default 15s, matching the
            gateway's upstream HTTP timeout).
    """

    def __init__(
        self,
        gateway_client: GatewayClient,
        *,
        timeout_seconds: float = 15.0,
    ) -> None:
        if gateway_client is None:
            raise TypeError(
                "PoolAnalyticsReader now requires a connected GatewayClient. "
                "VIB-4727 moved HTTP egress to the gateway side; constructing a "
                "reader without a gateway client is a programming error.",
            )
        self._gateway_client = gateway_client
        self._timeout_seconds = timeout_seconds

    # -- Public API -----------------------------------------------------------

    def get_pool_analytics(
        self,
        pool_address: str,
        chain: str,
        protocol: str | None = None,
    ) -> DataEnvelope[PoolAnalytics]:
        """Get real-time analytics for a pool via the gateway.

        Args:
            pool_address: Pool contract address.
            chain: Chain name (e.g. ``"arbitrum"``).
            protocol: Optional protocol hint (e.g. ``"uniswap_v3"``).

        Returns:
            ``DataEnvelope[PoolAnalytics]`` with INFORMATIONAL classification.

        Raises:
            DataSourceUnavailable: When the gateway returns a non-OK status
                or when both upstream providers fail. Callers that catch
                this exception MUST either re-raise or return ``Intent.hold(...)``
                so the runner's HOLD inference still fires.
        """
        # Import the proto symbols lazily so the framework reader can be
        # imported without forcing the gateway stubs to load (matters for
        # CLI / test surfaces that import the dataclasses only).
        from almanak.gateway.proto import gateway_pb2

        chain_norm = chain.lower()
        # Chain-aware address normalize: strip on both branches so a
        # copy-pasted EVM address with stray whitespace can't reach the
        # gateway as a different string and fail validation. EVM is
        # case-insensitive hex → lowercase; Solana base58 is
        # case-sensitive → preserve case (lower-casing yields a
        # different address).
        pool_addr_norm = pool_address.strip()
        descriptor = ChainRegistry.try_resolve(chain_norm)
        is_solana = descriptor is not None and descriptor.family is ChainFamily.SOLANA
        if not is_solana:
            pool_addr_norm = pool_addr_norm.lower()
        protocol_norm = (protocol or "").lower()

        request = gateway_pb2.PoolAnalyticsRequest(
            pool_address=pool_addr_norm,
            chain=chain_norm,
            protocol=protocol_norm,
        )

        try:
            response = self._gateway_client.pool_analytics.GetPoolAnalytics(
                request,
                timeout=self._timeout_seconds,
            )
        except grpc.RpcError as exc:
            raise DataSourceUnavailable(
                source="pool_analytics",
                reason=f"gateway error: {exc}",
            ) from exc
        except RuntimeError as exc:
            # GatewayClient raises RuntimeError("Gateway client not connected")
            # from the `pool_analytics` property when the channel is None.
            # Map to the typed exception so the runner's HOLD inference fires
            # via the same DATA_UNAVAILABLE path as a real outage, rather
            # than leaking a RuntimeError up through the iteration loop.
            # CodeRabbit PR #2389 review thread, 2026-05-21.
            raise DataSourceUnavailable(
                source="pool_analytics",
                reason=f"gateway client not connected: {exc}",
            ) from exc

        if not response.success:
            # Dual-channel: success=False with OK status means "degraded
            # data" (not used in v1; gateway returns non-OK in that case
            # already). Treat any success=False as a hard failure to keep
            # the contract tight and force a typed raise.
            raise DataSourceUnavailable(
                source="pool_analytics",
                reason=response.error or "pool analytics returned success=False",
            )

        # Empty != Zero (AGENTS.md "Accounting"): track which money-
        # critical fields the gateway marked unmeasured so callers can
        # distinguish "no data" from "measured zero" without re-parsing
        # the wire. ``confidence`` decays proportionally to the unmeasured
        # count — the docstring promise (callers should inspect
        # confidence to disambiguate) is only real because of this.
        # Blocker #2 from the multi-auditor review on PR #2389.
        unmeasured: set[str] = set()
        for field in ("tvl_usd", "volume_24h_usd", "volume_7d_usd", "fee_apr", "fee_apy"):
            if not getattr(response, field):
                unmeasured.add(field)

        analytics = PoolAnalytics(
            pool_address=response.pool_address or pool_addr_norm,
            chain=response.chain or chain_norm,
            protocol=response.protocol or protocol_norm,
            tvl_usd=_decimal_or_zero(response.tvl_usd),
            volume_24h_usd=_decimal_or_zero(response.volume_24h_usd),
            volume_7d_usd=_decimal_or_zero(response.volume_7d_usd),
            fee_apr=_float_or_zero(response.fee_apr),
            fee_apy=_float_or_zero(response.fee_apy),
            # Empty-string from the wire = unmeasured (DEX pool) -> None.
            # A measured "0" (legit zero utilization on a lending pool with
            # no borrowers) survives as 0.0. Same Empty != Zero contract as
            # token weights below.
            utilization_rate=(_float_or_zero(response.utilization_rate) if response.utilization_rate != "" else None),
            # Empty-string from the wire = unmeasured -> default to balanced 0.5;
            # a measured "0" survives as 0.0 (Empty != Zero per AGENTS.md
            # "Accounting"). A 0/100 or 100/0 pool must report 0.0, not 0.5.
            token0_weight=(_float_or_zero(response.token0_weight) if response.token0_weight else 0.5),
            token1_weight=(_float_or_zero(response.token1_weight) if response.token1_weight else 0.5),
            unmeasured_fields=frozenset(unmeasured),
        )

        observed_at = (
            datetime.fromtimestamp(response.observed_at, tz=UTC) if response.observed_at else datetime.now(UTC)
        )
        # Confidence reflects the count of unmeasured load-bearing fields
        # (5 total: tvl, vol24, vol7d, fee_apr, fee_apy). Each missing
        # field drops confidence by ~0.15 from the baseline 0.85; fully
        # unmeasured -> 0.10. The strategy author can compare against any
        # threshold they like.
        baseline_confidence = 0.85
        confidence = max(0.10, baseline_confidence - 0.15 * len(unmeasured))
        meta = DataMeta(
            source=response.source or "gateway",
            observed_at=observed_at,
            finality="off_chain",
            staleness_ms=0,
            latency_ms=0,
            confidence=confidence,
            cache_hit=not response.is_live_data,
        )
        return DataEnvelope(
            value=analytics,
            meta=meta,
            classification=DataClassification.INFORMATIONAL,
        )

    def best_pool(
        self,
        token_a: str,  # noqa: ARG002
        token_b: str,  # noqa: ARG002
        chain: str,  # noqa: ARG002
        metric: str = "fee_apr",  # noqa: ARG002
        protocols: list[str] | None = None,  # noqa: ARG002
    ) -> DataEnvelope[PoolAnalyticsResult]:
        """Deferred to a follow-up gateway RPC.

        The pre-VIB-4727 implementation enumerated all DefiLlama pools and
        filtered locally — that egress path can't ship as-is from the
        strategy container. A second gateway RPC (``SearchPools``) is
        required. Tracking ticket: **VIB-4729**.

        Until the RPC lands this raises ``DataSourceUnavailable`` rather
        than ``NotImplementedError`` so that the wrap-and-reraise in
        ``MarketSnapshot.best_pool(...)`` produces a
        ``PoolAnalyticsUnavailableError`` whose ``__cause__`` chain
        classifies as ``DATA_UNAVAILABLE`` — the runner treats that as
        HOLD-worthy, the same contract a real provider outage uses. A
        bare ``NotImplementedError`` would have crashed the iteration loop
        instead of producing a HOLD.
        """
        raise DataSourceUnavailable(
            source="pool_analytics",
            reason="best_pool requires the SearchPools gateway RPC (VIB-4729); not yet available",
        )

    # -- Compatibility surface for legacy tests / health probes ---------------

    def health(self) -> dict[str, dict[str, int]]:
        """Provider health is now owned by the gateway servicer.

        The legacy class exposed this for the old aiohttp-using providers.
        Strategy-container code that wants provider stats should call the
        gateway's metrics endpoint instead. Returning an empty dict here
        keeps the attribute non-throwing for any callers that still poll
        it during the cut-over.
        """
        return {}


# =============================================================================
# NullPoolAnalyticsReader — deterministic stub for backtest factories
# =============================================================================


class NullPoolAnalyticsReader:
    """Always-raises stub used by backtest factories (VIB-4727).

    Live gateway HTTP at backtest time = nondeterministic results across
    runs — strategies that "work in backtest" then silently change behavior
    in production. The agreed contract (per the VIB-4727 design discussion)
    is: backtest factories inject this null reader; strategies that depend
    on ``pool_analytics(...)`` must take a deterministic code path inside
    backtests (a static fee assumption, a fixture-backed analytics, or HOLD).

    Any call raises ``DataSourceUnavailable("backtest")`` so the runner's
    HOLD inference path is exercised identically to a real gateway outage.
    """

    def get_pool_analytics(
        self,
        pool_address: str,  # noqa: ARG002
        chain: str,  # noqa: ARG002
        protocol: str | None = None,  # noqa: ARG002
    ) -> DataEnvelope[PoolAnalytics]:
        raise DataSourceUnavailable(
            source="pool_analytics",
            reason="backtest",
        )

    def best_pool(
        self,
        token_a: str,  # noqa: ARG002
        token_b: str,  # noqa: ARG002
        chain: str,  # noqa: ARG002
        metric: str = "fee_apr",  # noqa: ARG002
        protocols: list[str] | None = None,  # noqa: ARG002
    ) -> DataEnvelope[PoolAnalyticsResult]:
        raise DataSourceUnavailable(
            source="pool_analytics",
            reason="backtest",
        )

    def health(self) -> dict[str, dict[str, int]]:
        return {}


__all__ = [
    "NullPoolAnalyticsReader",
    "PoolAnalytics",
    "PoolAnalyticsReader",
    "PoolAnalyticsResult",
]


# =============================================================================
# Sanity check: aiohttp must NOT be imported by this module post-VIB-4727.
# This is the runtime equivalent of the test-time `sys.modules` assertion
# in the UAT card D1.S2. A backslide here would re-introduce the boundary
# violation that PR #2379 was closed for.
# =============================================================================
_FORBIDDEN_IMPORT_NAMES: tuple[str, ...] = ("aiohttp", "requests", "httpx")
del _FORBIDDEN_IMPORT_NAMES  # documentation only; tests enforce via `sys.modules`.
