"""Run-validity classifier for finished PnL backtests (blueprint 31 §4.2).

The engine refuses to fabricate data and records every fill and rejection;
this module reads those ledgers once, at finalize, and turns them into the
verdict that decides whether the run's metrics may be published as strategy
performance. Reasons carry the wire prefix that lands on ``BacktestResult.error``
so ``success`` (and therefore the hosted outcome) follows the verdict.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any

from almanak.framework.backtesting.models import RunValidity, RunValidityReason, RunValidityVerdict

NO_TICKS = "NO_TICKS"
ZERO_INITIAL_CAPITAL = "ZERO_INITIAL_CAPITAL"
INTENTS_UNRECORDED = "INTENTS_UNRECORDED"
FAMILY_ALL_REJECTED = "FAMILY_ALL_REJECTED"
ENGINE_ERROR = "ENGINE_ERROR"
INPUT_STARVED = "INPUT_STARVED"
UNREACHABLE_EXIT_PATH = "UNREACHABLE_EXIT_PATH"
INPUT_STARVED_LANE = "INPUT_STARVED_LANE"

REASON_VALIDITY: dict[str, RunValidity] = {
    NO_TICKS: RunValidity.INVALID,
    ZERO_INITIAL_CAPITAL: RunValidity.INVALID,
    INTENTS_UNRECORDED: RunValidity.INVALID,
    FAMILY_ALL_REJECTED: RunValidity.INVALID,
    ENGINE_ERROR: RunValidity.INVALID,
    INPUT_STARVED: RunValidity.NOT_EVALUABLE,
    UNREACHABLE_EXIT_PATH: RunValidity.PARTIAL_LIFECYCLE,
}

# The two older prefixes predate the verdict; consumers already match on them.
_WIRE_CODES: dict[str, str] = {
    FAMILY_ALL_REJECTED: "BACKTEST_EXECUTION_REJECTED",
    INPUT_STARVED: "BACKTEST_UNSUPPORTED_DATA",
}
_DEFAULT_WIRE_CODES: dict[RunValidity, str] = {
    RunValidity.INVALID: "BACKTEST_INVALID",
    RunValidity.NOT_EVALUABLE: "BACKTEST_NOT_EVALUABLE",
}
_METRICS_DISCLAIMER = (
    "Any return, PnL, Sharpe, or drawdown in the diagnostic artifact describes passive "
    "mark-to-market of funded assets, not strategy performance."
)


def reason_validity(reason: RunValidityReason) -> RunValidity:
    """Validity a reason implies; an unknown code is never read as VALID."""
    return REASON_VALIDITY.get(reason.code, RunValidity.INVALID)


def wire_code(reason: RunValidityReason) -> str | None:
    """Prefix carried on ``BacktestResult.error``; None when the reason keeps ``success``."""
    explicit = _WIRE_CODES.get(reason.code)
    if explicit is not None:
        return explicit
    return _DEFAULT_WIRE_CODES.get(reason_validity(reason))


def terminal_errors(verdict: RunValidityVerdict) -> list[str]:
    """Error strings for the reasons that make the run unpublishable, most severe first."""
    ordered = sorted(verdict.reasons, key=lambda reason: -reason_validity(reason).severity)
    errors: list[str] = []
    for reason in ordered:
        code = wire_code(reason)
        if code is not None:
            errors.append(f"{code}: {reason.message}")
    return errors


def build_verdict(
    reasons: Sequence[RunValidityReason],
    *,
    warnings: Sequence[RunValidityReason] = (),
    executed_fills: int,
) -> RunValidityVerdict:
    """Fold reasons into a verdict: the highest-severity reason decides."""
    validity = RunValidity.VALID
    for reason in reasons:
        candidate = reason_validity(reason)
        if candidate.severity > validity.severity:
            validity = candidate
    return RunValidityVerdict(
        validity=validity,
        reasons=tuple(reasons),
        warnings=tuple(warnings),
        executed_fills=executed_fills,
        passive_only=validity is RunValidity.VALID and executed_fills == 0,
    )


def engine_error_verdict(error: BaseException) -> RunValidityVerdict:
    """Verdict for a run that raised out of the simulation loop."""
    reason = RunValidityReason(
        code=ENGINE_ERROR,
        message=f"the simulation raised {type(error).__name__}: {error}",
        details={"exception_type": type(error).__name__},
    )
    return build_verdict([reason], executed_fills=0)


def classify_run_validity(
    *,
    tick_count: int,
    initial_capital_usd: Decimal,
    decision_summary: Mapping[str, Any],
    decision_input_failures: Sequence[Mapping[str, Any]],
    executed_fills: int,
) -> RunValidityVerdict:
    """Classify a finished run from its ledgers.

    Precedence is by severity (INVALID > NOT_EVALUABLE > PARTIAL_LIFECYCLE >
    VALID); every applicable reason is kept so the artifact explains all of
    them, and warnings never change the verdict.
    """
    reasons: list[RunValidityReason] = []
    warnings: list[RunValidityReason] = []

    if tick_count <= 0:
        reasons.append(
            RunValidityReason(
                code=NO_TICKS,
                message="no market ticks were simulated inside the window, so nothing was evaluated.",
                details={"tick_count": tick_count},
            )
        )
    elif initial_capital_usd <= Decimal("0"):
        reasons.append(
            RunValidityReason(
                code=ZERO_INITIAL_CAPITAL,
                message=(
                    "the seeded portfolio had no capital (token_funding valued at $0 at the first tick), so no "
                    f"position could be taken. {_METRICS_DISCLAIMER}"
                ),
                details={"initial_capital_usd": str(initial_capital_usd), "tick_count": tick_count},
            )
        )

    executions = decision_summary.get("executions", {})
    terminal_records = int(executions.get("fills", 0) or 0) + int(executions.get("rejected", 0) or 0)
    intent_ticks = int(decision_summary.get("intent_ticks", 0) or 0)
    if intent_ticks > 0 and terminal_records == 0:
        reasons.append(
            RunValidityReason(
                code=INTENTS_UNRECORDED,
                message=(
                    f"{intent_ticks} intent tick(s) were emitted but none reached a terminal fill-or-rejection "
                    "record; the execution ledger is incomplete and the metrics are not trustworthy."
                ),
                details={"intent_ticks": intent_ticks},
            )
        )

    rejected = family_all_rejected_reason(decision_summary)
    if rejected is not None:
        reasons.append(rejected)

    persistent = [failure for failure in decision_input_failures if failure.get("pattern") == "persistent"]
    required_exact_pool_ohlcv = [
        failure
        for failure in persistent
        if failure.get("source") == "ohlcv" and str(failure.get("key", "")).endswith(":pool_scoped")
    ]
    if required_exact_pool_ohlcv and tick_count > 0:
        reasons.append(
            input_starved_reason(
                required_exact_pool_ohlcv,
                tick_count=tick_count,
                intent_ticks=intent_ticks,
                executed_fills=executed_fills,
            )
        )
    elif persistent and executed_fills == 0 and intent_ticks == 0 and tick_count > 0:
        reasons.append(input_starved_reason(persistent, tick_count=tick_count))
    elif persistent and executed_fills > 0:
        warnings.append(
            RunValidityReason(
                code=INPUT_STARVED_LANE,
                message=(
                    f"the run traded ({executed_fills} fill(s)) but {len(persistent)} decision input(s) failed "
                    "persistently; strategy branches gated on them may never have run."
                ),
                details={"inputs": [_failure_ref(failure, tick_count) for failure in persistent[:5]]},
            )
        )

    return build_verdict(reasons, warnings=warnings, executed_fills=executed_fills)


def family_all_rejected_reason(decision_summary: Mapping[str, Any]) -> RunValidityReason | None:
    """An emitted intent family that never reached a fill invalidates the run.

    A busy secondary leg must not hide a completely unmodeled or rejected core
    leg. Intermittent rejections stay valid once the same family has a fill;
    only a 100%-rejected family is a reason.
    """
    blocked = [
        (intent_type, counts)
        for intent_type, counts in decision_summary.get("execution_by_intent_type", {}).items()
        if counts.get("rejected", 0) > 0 and counts.get("fills", 0) == 0
    ]
    if not blocked:
        return None
    blocked_types = {intent_type for intent_type, _counts in blocked}
    dominant: Mapping[str, Any] = next(
        (
            rejection
            for rejection in decision_summary.get("rejections", [])
            if rejection.get("intent_type") in blocked_types
        ),
        {},
    )
    families = ", ".join(f"{intent_type} ({counts['rejected']} rejected)" for intent_type, counts in blocked[:5])
    reason = dominant.get("example", "fill rejected")
    return RunValidityReason(
        code=FAMILY_ALL_REJECTED,
        message=(
            f"no successful execution was modeled for emitted intent family/families {families}. "
            f"Dominant rejection: {reason}. Headline return, PnL, Sharpe, and drawdown are invalid "
            "because at least one strategy action lane never executed. See decision_summary.rejections "
            "for the bounded structured breakdown."
        ),
        details={
            "families": {intent_type: dict(counts) for intent_type, counts in blocked},
            "dominant_rejection": dict(dominant) if dominant else None,
        },
    )


def input_starved_reason(
    persistent: Sequence[Mapping[str, Any]],
    *,
    tick_count: int,
    intent_ticks: int = 0,
    executed_fills: int = 0,
) -> RunValidityReason:
    """Reason for a held run whose required inputs failed persistently."""
    blocking = "; ".join(
        f"{failure['source']}:{failure['key']} ({failure['ticks']}/{tick_count} ticks: {failure['detail']})"
        for failure in persistent[:3]
    )
    if executed_fills or intent_ticks:
        outcome = (
            f"The run recorded {executed_fills} fill(s) across {intent_ticks} intent tick(s), but that activity "
            "does not evaluate the signal path gated on these exact-pool inputs."
        )
    else:
        outcome = "The strategy emitted zero intents, so no executable simulation was performed."
    return RunValidityReason(
        code=INPUT_STARVED,
        message=(
            f"{len(persistent)} required decision input(s) were unavailable on nearly every one of "
            f"{tick_count} tick(s). {outcome} Blocking input(s): {blocking}. {_METRICS_DISCLAIMER}"
        ),
        details={"inputs": [_failure_ref(failure, tick_count) for failure in persistent[:5]]},
    )


def _failure_ref(failure: Mapping[str, Any], tick_count: int) -> dict[str, Any]:
    """Compact pointer to a decision-input failure for verdict details."""
    return {
        "source": failure.get("source"),
        "key": failure.get("key"),
        "ticks": failure.get("ticks"),
        "tick_count": tick_count,
        "detail": failure.get("detail"),
    }
