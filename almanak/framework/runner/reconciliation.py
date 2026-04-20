"""Pre/post balance reconciliation for intent execution (VIB-3158).

The prior post-only check recorded post-execution balances and only warned
when enriched swap amounts were non-positive. It did not snapshot pre
balances, compute deltas, or compare to expected deltas — Codex named it
"observability theater" in the SDK audit.

This module implements the real thing:

- ``BalanceSnapshot``   — per-token balance at a point in time.
- ``ExpectedRange``     — min/max bounds on a per-token delta.
- ``DeltaMismatch``     — one token whose actual delta fell outside its range.
- ``ReconciliationReport`` — structured result with pre, post, actual deltas,
  expected ranges, mismatches, incident flag.
- ``compute_actual_deltas``        — pure function (post - pre).
- ``compute_expected_swap_deltas`` — pure function, SwapIntent only for now.
- ``build_reconciliation_report``  — orchestrates the above.

Only ``SwapIntent`` is currently enforced (mismatches flagged as incidents);
LP / supply / borrow / perp intents still produce actual-delta observations
without enforcement. Extending coverage is tracked as follow-up under the
same epic (VIB-3152).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from ..intents.vocabulary import AnyIntent, SwapIntent

if TYPE_CHECKING:
    from ..execution.orchestrator import ExecutionResult


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BalanceSnapshot:
    """Per-token balance snapshot at a point in time."""

    timestamp: datetime
    balances: dict[str, Decimal] = field(default_factory=dict)

    @classmethod
    def now(cls, balances: dict[str, Decimal]) -> BalanceSnapshot:
        return cls(timestamp=datetime.now(UTC), balances=dict(balances))


@dataclass(frozen=True)
class ExpectedRange:
    """Inclusive min/max bounds on an expected balance delta for one token."""

    token: str
    min: Decimal
    max: Decimal

    def contains(self, actual: Decimal) -> bool:
        return self.min <= actual <= self.max


@dataclass(frozen=True)
class DeltaMismatch:
    """One token whose actual delta fell outside its expected range."""

    token: str
    actual: Decimal
    expected_min: Decimal
    expected_max: Decimal


@dataclass
class ReconciliationReport:
    """Structured result of pre/post reconciliation for a single intent."""

    tokens_checked: list[str]
    pre_balances: dict[str, Decimal]
    post_balances: dict[str, Decimal]
    actual_deltas: dict[str, Decimal]
    expected_ranges: dict[str, ExpectedRange]
    mismatches: list[DeltaMismatch]
    warnings: list[str]
    incident: bool
    enforced: bool

    def to_dict(self) -> dict[str, Any]:
        """Serialize for storage (Decimal → str for JSON compatibility)."""
        return {
            "tokens_checked": list(self.tokens_checked),
            "pre_balances": {k: str(v) for k, v in self.pre_balances.items()},
            "post_balances": {k: str(v) for k, v in self.post_balances.items()},
            "actual_deltas": {k: str(v) for k, v in self.actual_deltas.items()},
            "expected_ranges": {
                token: {"min": str(r.min), "max": str(r.max)} for token, r in self.expected_ranges.items()
            },
            "mismatches": [
                {
                    "token": m.token,
                    "actual": str(m.actual),
                    "expected_min": str(m.expected_min),
                    "expected_max": str(m.expected_max),
                }
                for m in self.mismatches
            ],
            "warnings": list(self.warnings),
            "incident": self.incident,
            "enforced": self.enforced,
        }


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------


def compute_actual_deltas(pre: BalanceSnapshot, post: BalanceSnapshot) -> dict[str, Decimal]:
    """Compute per-token delta as ``post - pre``.

    Only tokens present in BOTH snapshots are included — if a balance query
    failed on one side, it is safer to skip than to invent a zero.
    """
    deltas: dict[str, Decimal] = {}
    for token, post_bal in post.balances.items():
        if token in pre.balances:
            deltas[token] = post_bal - pre.balances[token]
    return deltas


def compute_expected_swap_deltas(
    intent: SwapIntent,
    execution_result: ExecutionResult | None,
    *,
    gas_token: str | None = None,
    gas_cost_native: Decimal | None = None,
) -> dict[str, ExpectedRange]:
    """Compute expected balance-delta ranges for a SwapIntent.

    Uses the enriched ``swap_amounts`` from ``execution_result`` as the
    authoritative in/out amounts (the enricher has already parsed the
    receipt). We then build symmetric slippage bounds around each side:

    - from_token: delta is negative. We expect ``-amount_in``; allow a
      slippage-sized band in either direction. If gas is paid in the
      from-token (native wallet), the lower bound is stretched by
      ``gas_cost_native``.
    - to_token: delta is positive. Lower bound is ``amount_out * (1 -
      max_slippage)``; upper bound is ``amount_out * (1 + max_slippage)``
      (a modest positive-slippage allowance).

    Returns an empty dict if swap_amounts are not available — callers fall
    back to warnings-only mode in that case rather than raising a false
    mismatch.
    """
    if execution_result is None:
        return {}
    sa = getattr(execution_result, "swap_amounts", None)
    if sa is None:
        return {}
    amount_in = getattr(sa, "amount_in_decimal", None)
    amount_out = getattr(sa, "amount_out_decimal", None)
    if amount_in is None or amount_out is None:
        return {}
    if amount_in <= 0 or amount_out <= 0:
        return {}

    slippage = intent.max_slippage
    slack_in = amount_in * slippage
    slack_out = amount_out * slippage

    extra_gas_out = Decimal("0")
    if gas_token == intent.from_token and gas_cost_native is not None:
        extra_gas_out = gas_cost_native

    from_min = -(amount_in + slack_in + extra_gas_out)
    from_max = -(amount_in - slack_in)

    to_min = amount_out - slack_out
    to_max = amount_out + slack_out

    return {
        intent.from_token: ExpectedRange(intent.from_token, from_min, from_max),
        intent.to_token: ExpectedRange(intent.to_token, to_min, to_max),
    }


def build_reconciliation_report(
    pre: BalanceSnapshot,
    post: BalanceSnapshot,
    intent: AnyIntent,
    execution_result: ExecutionResult | None,
    *,
    gas_token: str | None = None,
    gas_cost_native: Decimal | None = None,
) -> ReconciliationReport:
    """Build a full reconciliation report for one intent.

    Computes actual deltas from pre/post, derives expected ranges (only for
    supported intent types — currently SwapIntent), and records mismatches.
    """
    actual = compute_actual_deltas(pre, post)
    expected: dict[str, ExpectedRange] = {}
    enforced = False

    mismatches: list[DeltaMismatch] = []
    warnings: list[str] = []
    missing_expectations = False

    if isinstance(intent, SwapIntent):
        expected = compute_expected_swap_deltas(
            intent,
            execution_result,
            gas_token=gas_token,
            gas_cost_native=gas_cost_native,
        )
        if expected:
            enforced = True
        elif execution_result is not None and getattr(execution_result, "success", False):
            # Fail-closed: a successful SwapIntent that cannot produce an
            # expected-delta range (missing / unparsable / non-positive
            # swap_amounts) must not silently bypass reconciliation, or a
            # receipt-parser regression would blind the safety net. Mark the
            # reconciliation as enforced + record the condition so the caller
            # flags an incident (see the final `incident=` assignment).
            enforced = True
            missing_expectations = True
            warnings.append(
                "SwapIntent reconciliation could not derive expected deltas "
                "(missing/unparsable swap_amounts); treating as incident "
                "(fail-closed)."
            )

    if enforced and not missing_expectations:
        for token, rng in expected.items():
            if token not in actual:
                warnings.append(f"expected-delta check skipped for {token} (balance unavailable pre or post)")
                continue
            if not rng.contains(actual[token]):
                mismatches.append(
                    DeltaMismatch(
                        token=token,
                        actual=actual[token],
                        expected_min=rng.min,
                        expected_max=rng.max,
                    )
                )

    # Preserve the legacy zero/negative-amount warning for continuity.
    sa = getattr(execution_result, "swap_amounts", None) if execution_result else None
    if sa is not None:
        amount_out = getattr(sa, "amount_out_decimal", None)
        amount_in = getattr(sa, "amount_in_decimal", None)
        if amount_out is not None and amount_out <= 0:
            warnings.append(f"Swap output amount is zero or negative: {amount_out}")
        if amount_in is not None and amount_in <= 0:
            warnings.append(f"Swap input amount is zero or negative: {amount_in}")

    return ReconciliationReport(
        tokens_checked=sorted(actual.keys()),
        pre_balances=dict(pre.balances),
        post_balances=dict(post.balances),
        actual_deltas=actual,
        expected_ranges=expected,
        mismatches=mismatches,
        warnings=warnings,
        incident=bool(mismatches) or missing_expectations,
        enforced=enforced,
    )
