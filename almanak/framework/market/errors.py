"""Typed errors for VIB-4062 MarketSnapshot.

Every error inherits from ``MarketSnapshotError`` and carries
source/chain/instrument/severity/retryability metadata so the runner can
classify the failure without parsing exception messages (PRD §4.4).

The snapshot **always raises** typed errors on missing/stale/invalid data
and **records** the failure in its critical-data-failure register before
the exception bubbles up. The runner/runtime decides halt-vs-continue.
The snapshot is mode-agnostic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# =============================================================================
# Base
# =============================================================================


class MarketSnapshotError(Exception):
    """Base class for typed snapshot errors.

    Subclasses carry structured fields. Stringifying gives a stable shape
    for log lines: ``"<ClassName>(<key>=<value>, …): <reason>"``.

    For backward compat with the legacy data-layer error idiom, positional
    arguments are accepted as ``(chain, reason)`` or just ``(reason,)``;
    callers using kwargs (the canonical post-VIB-4062 form) continue to work.
    """

    severity: str = "error"  # "info" | "warning" | "error" | "critical"
    retryable: bool = False  # True → runner may retry; False → permanent.

    # Subclasses set this to a tuple of field names for legacy positional-arg
    # compat. Example: ``_positional_fields = ("token", "reason")`` lets the
    # subclass be raised as ``PriceUnavailableError("ETH", "stale data")``.
    # The reason is ALWAYS the trailing positional. The base class uses this
    # mapping so callers don't have to override ``__init__``.
    _positional_fields: tuple[str, ...] = ("chain", "reason")

    def __init__(self, *args: Any, reason: str = "", **fields: Any) -> None:
        # Legacy compat: map positional args to ``_positional_fields``.
        if args:
            if len(args) == 1 and not reason:
                # Single arg → just the reason (no leading field).
                reason = str(args[0])
            else:
                # Multi-arg → map all but the last into named fields, last → reason.
                names = self._positional_fields
                # Number of leading named fields to consume = min(len(args)-1, len(names)-1)
                n_named = min(len(args) - 1, max(len(names) - 1, 0))
                for i in range(n_named):
                    fields.setdefault(names[i], args[i])
                # Last positional is the reason (matches legacy idiom).
                if not reason:
                    reason = str(args[len(args) - 1])
        self._fields = fields
        self._reason = reason
        if reason:
            kv = ", ".join(f"{k}={v!r}" for k, v in fields.items() if v is not None)
            super().__init__(f"{type(self).__name__}({kv}): {reason}" if kv else f"{type(self).__name__}: {reason}")
        else:
            kv = ", ".join(f"{k}={v!r}" for k, v in fields.items() if v is not None)
            super().__init__(f"{type(self).__name__}({kv})")

    def __getattr__(self, name: str) -> Any:
        if name in {"_fields", "_reason"}:
            raise AttributeError(name)
        try:
            return self._fields[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    @property
    def reason(self) -> str:
        return self._reason

    @property
    def fields(self) -> dict[str, Any]:
        return dict(self._fields)


# =============================================================================
# Chain resolution (PRD §4.2)
# =============================================================================


class ChainNotConfiguredError(MarketSnapshotError):
    """A specific ``chain=`` argument was supplied but is not in ``chains``.

    Raised in BOTH single-chain (mismatch with the only configured chain)
    and multi-chain (chain not in the configured set) cases. The runner
    has no recovery path — strategy code must update its chain handling.
    """

    severity = "critical"
    retryable = False


class AmbiguousChainError(MarketSnapshotError):
    """A multi-chain snapshot was queried with ``chain=None``.

    Raised when ``len(self.chains) > 1`` and no explicit chain= was given.
    Multi-chain callers must be explicit; default-to-primary policy hides
    chain-routing bugs (PRD §4.2 R2, Codex finding HIGH).
    """

    severity = "critical"
    retryable = False


class StaleDataError(MarketSnapshotError):
    """A provider returned data older than the freshness policy permits."""

    severity = "warning"
    retryable = True


# =============================================================================
# Per-domain unavailability (one type per domain so callers can branch)
# =============================================================================


class PriceUnavailableError(MarketSnapshotError):
    severity = "error"
    retryable = True
    _positional_fields = ("token", "reason")


class BalanceUnavailableError(MarketSnapshotError):
    severity = "error"
    retryable = True
    _positional_fields = ("token", "reason")


class RSIUnavailableError(MarketSnapshotError):
    severity = "error"
    retryable = True
    _positional_fields = ("token", "reason")


class IndicatorUnavailableError(MarketSnapshotError):
    """Generic indicator (sma, ema, macd, …) unavailable."""

    severity = "error"
    retryable = True
    _positional_fields = ("token", "reason")


class OHLCVUnavailableError(MarketSnapshotError):
    severity = "error"
    retryable = True
    _positional_fields = ("token", "reason")


class GasUnavailableError(MarketSnapshotError):
    severity = "error"
    retryable = True
    _positional_fields = ("chain", "reason")


class PoolPriceUnavailableError(MarketSnapshotError):
    severity = "error"
    retryable = True
    # Legacy data-layer used ``identifier`` (pool address or pair string).
    _positional_fields = ("identifier", "reason")


class PoolReservesUnavailableError(MarketSnapshotError):
    severity = "error"
    retryable = True
    _positional_fields = ("pool_address", "reason")


class PoolHistoryUnavailableError(MarketSnapshotError):
    severity = "warning"
    retryable = True
    _positional_fields = ("pool_address", "reason")


class HealthUnavailableError(MarketSnapshotError):
    severity = "error"
    retryable = True
    _positional_fields = ("reason",)


class LendingRateUnavailableError(MarketSnapshotError):
    severity = "error"
    retryable = True
    _positional_fields = ("protocol", "token", "reason")


class LendingRateHistoryUnavailableError(MarketSnapshotError):
    severity = "warning"
    retryable = True
    _positional_fields = ("protocol", "token", "reason")


class FundingRateUnavailableError(MarketSnapshotError):
    severity = "error"
    retryable = True
    _positional_fields = ("market", "reason")


class FundingRateHistoryUnavailableError(MarketSnapshotError):
    severity = "warning"
    retryable = True
    # Legacy data-layer signature: (venue, market, reason).
    _positional_fields = ("venue", "market", "reason")


class DexQuoteUnavailableError(MarketSnapshotError):
    severity = "error"
    retryable = True
    _positional_fields = ("pair", "reason")


class ILExposureUnavailableError(MarketSnapshotError):
    severity = "warning"
    retryable = True
    _positional_fields = ("position", "reason")


class PredictionUnavailableError(MarketSnapshotError):
    severity = "error"
    retryable = True
    _positional_fields = ("market_id", "reason")


class PredictionMarketNotFoundError(MarketSnapshotError):
    severity = "critical"
    retryable = False
    _positional_fields = ("market_id", "reason")


class LiquidityDepthUnavailableError(MarketSnapshotError):
    severity = "warning"
    retryable = True
    _positional_fields = ("pool_address", "reason")


class SlippageEstimateUnavailableError(MarketSnapshotError):
    severity = "warning"
    retryable = True
    _positional_fields = ("pool_address", "reason")


class VolatilityUnavailableError(MarketSnapshotError):
    severity = "warning"
    retryable = True
    _positional_fields = ("token", "reason")


class VolConeUnavailableError(MarketSnapshotError):
    severity = "warning"
    retryable = True
    _positional_fields = ("token", "reason")


class PortfolioRiskUnavailableError(MarketSnapshotError):
    severity = "warning"
    retryable = True
    _positional_fields = ("reason",)


class RollingSharpeUnavailableError(MarketSnapshotError):
    severity = "info"
    retryable = True
    _positional_fields = ("reason",)


class PoolAnalyticsUnavailableError(MarketSnapshotError):
    severity = "warning"
    retryable = True
    _positional_fields = ("pool_address", "reason")


class YieldOpportunitiesUnavailableError(MarketSnapshotError):
    severity = "warning"
    retryable = True
    _positional_fields = ("token", "reason")


class LSTDataUnavailableError(MarketSnapshotError):
    severity = "warning"
    retryable = True
    _positional_fields = ("symbol", "reason")


# =============================================================================
# Critical-data-failure register record (PRD §4.4)
# =============================================================================


@dataclass(frozen=True)
class CriticalDataFailureRecord:
    """One entry in MarketSnapshot's critical-data-failure register.

    Written from inside the snapshot's ``_record_critical_failure`` chokepoint;
    read from outside via the public ``has_critical_data_failures`` /
    ``critical_data_failure_count`` / ``classify_critical_data_failures`` /
    ``summarize_critical_data_failures`` accessors.
    """

    error_type: str
    severity: str
    retryable: bool
    chain: str | None
    instrument: str | None
    reason: str

    @classmethod
    def from_error(cls, err: MarketSnapshotError) -> CriticalDataFailureRecord:
        fields = err.fields
        return cls(
            error_type=type(err).__name__,
            severity=err.severity,
            retryable=err.retryable,
            chain=fields.get("chain"),
            instrument=fields.get("token") or fields.get("market") or fields.get("pool_address"),
            reason=err.reason,
        )
