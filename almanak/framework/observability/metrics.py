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


# VIB-4780 / W1-5 — decimal-unit soft-fail guard counter.  Incremented when
# :func:`almanak.framework.accounting.decimal_guards._check_decimal_unit_soft_fail`
# detects a payload field whose magnitude / shape looks like raw-wei instead
# of a human-form decimal.  This is a Wave-1 soft-fail observability metric;
# Wave 3 (W3-1) will flip the same detection into a hard reject.
ACCOUNTING_RAW_WEI_SUSPECTED_TOTAL = Counter(
    "accounting_raw_wei_suspected_total",
    "Accounting payload-write fields whose value looked like raw-wei "
    "(soft-fail / observability only; see VIB-4780 W1-5).",
    ["chain", "field", "event_type", "token_symbol"],
    registry=FRAMEWORK_REGISTRY,
)


# VIB-5052 — money-path on-chain read fallback observability.
#
# The VIB-5038 getSlot0 selector bug survived ~2 months in production ONLY
# because a hard on-chain read failure (StateView.getSlot0 reverting) was
# silently converted to an estimated value with NO metric and NO operator
# signal. The selector was the trigger; the defect *class* is "invisible
# money-path fallback".
#
# Contract: any money-path on-chain read that can fall back to an
# estimate / None MUST increment this counter on the fallback, labelled
# ``{protocol, chain, call, reason}`` so the degradation is visible the day
# it starts (an alert rule on ``rate(onchain_read_fallback_total[…]) > 0``
# fires immediately instead of waiting for a downstream accounting-drift
# report). Provenance is preserved separately downstream (e.g. the V4 LP
# adapter stamps ``price_source`` / ``compile_time_current_tick_source`` into
# bundle metadata) — the metric is the operator-visible half of the contract,
# provenance is the reader-trust half. See
# ``docs/internal/blueprints/27-accounting.md`` §7.5 (ValueConfidence —
# degrade rather than fabricate) and §7.10 (the read-fallback observability
# contract).
#
# Labels are an OPEN set across protocols (any connector adopts the helper),
# but each label VALUE must be a short, bounded identifier — never a
# user-supplied or unbounded string — or it spawns a divergent Prometheus
# time-series. The ``reason`` token is a stable error code (mirrors
# :class:`V4LPDropReason`'s discipline) so dashboards/alerts can filter on it.
class OnchainReadFallbackReason(StrEnum):
    """Stable error codes for money-path on-chain read fallback paths.

    These string values are the ``reason=`` label on
    ``onchain_read_fallback_total`` AND (by convention) the ``reason=`` token
    in the structured WARNING that accompanies the fallback. They are part of
    the observability contract and not safe to rename without a coordinated
    dashboard/alert-rule update.
    """

    RPC_CALL_FAILED = "rpc_call_failed"
    EMPTY_RESULT = "empty_result"
    DECODE_FAILED = "decode_failed"
    POOL_UNINITIALIZED = "pool_uninitialized"
    READER_UNAVAILABLE = "reader_unavailable"


ONCHAIN_READ_FALLBACK_TOTAL = Counter(
    "onchain_read_fallback_total",
    "Total money-path on-chain reads that fell back to an estimate/None, "
    "by protocol, chain, call and reason (VIB-5052).",
    ["protocol", "chain", "call", "reason"],
    registry=FRAMEWORK_REGISTRY,
)


def record_onchain_read_fallback(
    *,
    protocol: str,
    chain: str,
    call: str,
    reason: OnchainReadFallbackReason | str,
) -> None:
    """Increment ``onchain_read_fallback_total`` for a money-path read fallback.

    Call this at the exact site where a money-path on-chain read fails and the
    code falls back to an estimate or ``None`` (VIB-5052). The fallback may be
    legitimate, but it MUST be visible: an operator alert on this counter is
    what would have surfaced the VIB-5038 ``getSlot0`` selector regression on
    day one instead of two months later.

    Args:
        protocol: Connector / protocol name (e.g. ``"uniswap_v4"``). Short,
            bounded identifier — never user-supplied.
        chain: Chain name (lowercased; e.g. ``"base"``, ``"arbitrum"``).
        call: The on-chain call that fell back (e.g. ``"getSlot0"``). Stable
            short identifier naming the read, not a free-form message.
        reason: One of :class:`OnchainReadFallbackReason`. Strings are accepted
            to keep call sites terse; the value is coerced via the enum so an
            unknown string fails fast at test time rather than silently
            polluting label cardinality in production.

    Raises:
        ValueError: if ``reason`` is not a recognised
            :class:`OnchainReadFallbackReason` value (defence-in-depth for
            callers that bypass the type-checker).
    """
    reason_value = (
        reason.value if isinstance(reason, OnchainReadFallbackReason) else OnchainReadFallbackReason(reason).value
    )
    ONCHAIN_READ_FALLBACK_TOTAL.labels(
        protocol=(protocol or "unknown").lower() or "unknown",
        chain=(chain or "unknown").lower() or "unknown",
        call=call or "unknown",
        reason=reason_value,
    ).inc()


# VIB-5218 — transaction-ledger intent-fallback observability.
#
# The ledger dispatcher (``almanak.framework.observability.ledger
# ._extract_tokens_and_amounts``) prefers a connector-DECLARED ``PrimitiveMoneyLeg``
# set (VIB-5212 / US-008) when one is present, and only otherwise walks the legacy
# ``_extract_from_intent_fallback`` guesser. That guesser is the "patch-hub" failure
# mode the VIB-5200 epic is closing: every primitive whose shape it can't guess
# re-emits empty money columns until a follow-up patch.
#
# Contract: each time the fallback produces a MONEY-BEARING row (a non-empty
# ``token_in`` or ``amount_in``) the ledger MUST increment this counter (labelled
# by intent type) AND emit a structured WARNING. A shrinking
# ``rate(ledger_intent_fallback_total[…])`` as connectors migrate to declared legs
# is the success signal for the accounting contract layer; a non-zero rate on a
# given ``intent_type`` is the operator-visible "this primitive still guesses".
#
# ``intent_type`` is a bounded vocabulary (SWAP / LP_OPEN / REPAY / …), so it is a
# safe label value — never a user-supplied or unbounded string.
LEDGER_INTENT_FALLBACK_TOTAL = Counter(
    "ledger_intent_fallback_total",
    "Total transaction_ledger money rows attributed by the legacy intent-attribute "
    "fallback guesser instead of a connector-declared PrimitiveMoneyLeg, by intent "
    "type (VIB-5218). A shrinking rate is the accounting-contract-layer success signal.",
    ["intent_type"],
    registry=FRAMEWORK_REGISTRY,
)


def record_ledger_intent_fallback(*, intent_type: str) -> None:
    """Increment ``ledger_intent_fallback_total`` for one fallback-attributed row.

    Call this at the exact site where the ledger's
    :func:`almanak.framework.observability.ledger._extract_from_intent_fallback`
    produces a money-bearing row because no connector-declared
    :class:`~almanak.connectors._strategy_base.primitive_money_leg.PrimitiveMoneyLegs`
    was available (VIB-5218). It is the metric half of the fallback-observability
    contract; the WARNING at the call site is the human-readable half.

    Args:
        intent_type: The intent category whose row the fallback produced (e.g.
            ``"SWAP"``, ``"STAKE"``). A bounded vocabulary — empty / unknown is
            defaulted to ``"unknown"`` so a missing value never spawns a divergent
            Prometheus time-series.
    """
    LEDGER_INTENT_FALLBACK_TOTAL.labels(intent_type=(intent_type or "unknown") or "unknown").inc()


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


def record_raw_wei_suspected(
    *,
    chain: str,
    field: str,
    event_type: str,
    token_symbol: str,
) -> None:
    """Increment ``accounting_raw_wei_suspected_total`` (W1-5 soft-fail metric).

    Labels are lower-cased and defaulted so a missing value never spawns
    a divergent time-series.  All values must be short identifiers — the
    chokepoint in :mod:`almanak.framework.accounting.decimal_guards` is
    responsible for ensuring no high-cardinality user-supplied string
    reaches this counter.
    """
    ACCOUNTING_RAW_WEI_SUSPECTED_TOTAL.labels(
        chain=(chain or "unknown").lower() or "unknown",
        field=(field or "unknown") or "unknown",
        event_type=(event_type or "unknown") or "unknown",
        token_symbol=(token_symbol or "unknown") or "unknown",
    ).inc()


__all__ = [
    "ACCOUNTING_RAW_WEI_SUSPECTED_TOTAL",
    "FRAMEWORK_REGISTRY",
    "LEDGER_INTENT_FALLBACK_TOTAL",
    "ONCHAIN_READ_FALLBACK_TOTAL",
    "OnchainReadFallbackReason",
    "V4_LP_PARSER_DROPS_TOTAL",
    "V4LPDropOutcome",
    "V4LPDropReason",
    "record_ledger_intent_fallback",
    "record_onchain_read_fallback",
    "record_raw_wei_suspected",
    "record_v4_lp_parser_drop",
]
