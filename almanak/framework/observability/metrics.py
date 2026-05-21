"""Framework-level Prometheus metrics.

This module owns metrics emitted from inside the strategy container (parsers,
handlers, accounting writes). Gateway-side metrics live in
``almanak.gateway.metrics`` on a separate registry; the two are intentionally
isolated so a process that hosts only one of the two sides does not export
labels it cannot populate.

Counters defined here are exposed by the gateway sidecar via its own scrape
endpoint when the framework runs in-process with the gateway, and through the
shared default registry otherwise; either path is acceptable for the operator
dashboard use case these counters serve.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from prometheus_client import CollectorRegistry, Counter

# VIB-4426 — outcome label is a closed set: ``"drop"`` for return-None paths,
# ``"raise"`` for typed-error paths. Free-form strings would let a typo
# silently bloat label cardinality (each unique value spawns a new
# Prometheus time series) and break operator dashboards filtering on the
# two canonical values.
V4LPDropOutcome = Literal["drop", "raise"]

FRAMEWORK_REGISTRY = CollectorRegistry()


class V4LPDropReason(StrEnum):
    """Stable error codes for Uniswap V4 LP receipt-parser drop paths.

    These string values are the label value emitted on the
    ``v4_lp_parser_drops_total`` counter AND the ``reason=`` token in the
    structured WARNING line. Operator dashboards and alert rules MUST use
    these constants directly -- the strings are part of the observability
    contract and not safe to rename without a coordinated dashboard update.
    """

    NON_POSITION_MANAGER_SENDER = "non_position_manager_sender"
    SALT_TOKENID_MISMATCH = "salt_tokenid_mismatch"
    MISSING_POSITION_ID = "missing_position_id"
    TRANSFER_SET_MISMATCH = "transfer_set_mismatch"
    NATIVE_CURRENCY_UNSUPPORTED = "native_currency_unsupported"
    POOL_KEY_NOT_FOUND = "pool_key_not_found"
    POOL_KEY_LOOKUP_ERROR = "pool_key_lookup_error"
    MISSING_POOL_KEY_LOOKUP = "missing_pool_key_lookup"


V4_LP_PARSER_DROPS_TOTAL = Counter(
    "v4_lp_parser_drops_total",
    "Total Uniswap V4 LP receipt-parser drops by drop reason.",
    ["chain", "reason", "outcome"],
    registry=FRAMEWORK_REGISTRY,
)


def record_v4_lp_parser_drop(*, chain: str, reason: V4LPDropReason | str, outcome: V4LPDropOutcome) -> None:
    """Increment the ``v4_lp_parser_drops_total`` counter.

    Args:
        chain: Chain name (lowercased; e.g. "arbitrum", "base").
        reason: One of :class:`V4LPDropReason`. Strings are accepted to keep
            the call site terse; the value is coerced via the enum so an
            unknown string fails fast at test time rather than silently
            polluting label cardinality in production.
        outcome: Must be ``"drop"`` (return-None paths) or ``"raise"``
            (typed-error path). Closed-set Literal so a typo at a future
            call site is caught by static type-checking rather than
            silently spawning a new Prometheus time-series.

    Raises:
        ValueError: if ``outcome`` is not one of the two accepted values.
            Defence-in-depth for callers that bypass the type-checker.
    """
    if outcome not in ("drop", "raise"):
        raise ValueError(f"record_v4_lp_parser_drop: outcome must be 'drop' or 'raise', got {outcome!r}")
    reason_value = reason.value if isinstance(reason, V4LPDropReason) else V4LPDropReason(reason).value
    V4_LP_PARSER_DROPS_TOTAL.labels(chain=chain, reason=reason_value, outcome=outcome).inc()


__all__ = [
    "FRAMEWORK_REGISTRY",
    "V4_LP_PARSER_DROPS_TOTAL",
    "V4LPDropOutcome",
    "V4LPDropReason",
    "record_v4_lp_parser_drop",
]
