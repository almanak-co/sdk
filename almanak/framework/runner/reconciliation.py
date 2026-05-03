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
    # VIB-3888: post-balance capture timestamp. Surfaces into the ledger
    # row's ``post_state.captured_at`` field so reconciliation rows are
    # symmetric with ``pre_state.captured_at`` (which the runner already
    # populates). Pre-VIB-3888 ``post_state.captured_at`` was always
    # empty — Accountant Test G6 per-intent reconciliation needed it.
    post_timestamp: datetime | None = None
    pre_timestamp: datetime | None = None

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
            "pre_timestamp": self.pre_timestamp.isoformat() if self.pre_timestamp else "",
            "post_timestamp": self.post_timestamp.isoformat() if self.post_timestamp else "",
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
            # Fail-closed fallback (VIB-3292): a successful SwapIntent whose
            # enriched swap_amounts are missing/unparsable cannot be checked
            # against a magnitude range, but the actual on-chain pre/post
            # deltas are still ground truth. Verify the swap moved balances in
            # the directionally correct way — from_token decreased (or was
            # absorbed by gas), to_token increased — and only fail closed when
            # a sign is wrong. This preserves the safety net against
            # receipt-parser regressions while eliminating the false
            # positive where a successful swap was flagged as an incident
            # with an empty mismatch list (the original VIB-3292 bug: two
            # strategies — velodrome_swap_optimism and solana_swap — both
            # confirmed on-chain, both tripped RECONCILIATION_FAILED with
            # mismatches=[]).
            #
            # Gate the fallback on BOTH sides having a pre/post delta
            # available: if either the from-token or to-token balance was
            # uncheckable, the directional check cannot be honest about
            # whether it verified anything, so we downgrade to warnings-only
            # and leave enforced=False (mirrors the line 252-254 guard that
            # handles missing per-token balances in the enriched path).
            from_delta_present = intent.from_token in actual
            to_delta_present = intent.to_token in actual
            if from_delta_present and to_delta_present:
                enforced = True
                mismatches.extend(
                    _directional_sanity_mismatches(
                        intent,
                        actual,
                        gas_token=gas_token,
                        gas_cost_native=gas_cost_native,
                    )
                )
                warnings.append(
                    "SwapIntent reconciliation could not derive expected deltas "
                    "(missing/unparsable swap_amounts); falling back to "
                    "directional sanity check on actual balance deltas."
                )
            else:
                missing = [
                    tok
                    for tok, present in (
                        (intent.from_token, from_delta_present),
                        (intent.to_token, to_delta_present),
                    )
                    if not present
                ]
                warnings.append(
                    "SwapIntent reconciliation could not derive expected deltas "
                    "and directional fallback skipped (balance snapshot missing "
                    f"for: {', '.join(missing)})."
                )

    if enforced and expected:
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
        incident=bool(mismatches),
        enforced=enforced,
        # VIB-3888: propagate snapshot timestamps so the ledger writer
        # can stamp ``post_state.captured_at`` symmetrically with
        # ``pre_state.captured_at``.
        pre_timestamp=getattr(pre, "timestamp", None),
        post_timestamp=getattr(post, "timestamp", None),
    )


def _directional_sanity_mismatches(
    intent: SwapIntent,
    actual: dict[str, Decimal],
    *,
    gas_token: str | None,
    gas_cost_native: Decimal | None,
) -> list[DeltaMismatch]:
    """Directional sanity check used when swap_amounts are unavailable.

    Rules, designed so a real swap (or a swap-then-gas-spend on a native
    from-token) never trips a false positive, while a wrong-direction move
    (sign flipped) or a no-op ("success" but zero movement on either side)
    DOES surface a structured mismatch that callers can surface in logs and
    alerts — no more empty-list incidents.

    - from_token: actual delta must be ≤ 0 (wallet lost funds). When the
      from-token is the native gas token we tolerate gas-only outflow but
      still require at least *some* outflow beyond gas; otherwise a
      "successful" tx that never reached the swap would pass unchecked.
    - to_token: actual delta must be > 0 (wallet gained funds). A
      non-positive to-token delta on a successful swap is unambiguous
      accounting corruption.

    Returns a list of structured ``DeltaMismatch`` entries for each
    violation (empty list means the swap passed the directional check).
    """
    mismatches: list[DeltaMismatch] = []

    # Minimum expected outflow on from_token. If from_token pays gas we
    # accept outflow of (at least) the gas cost as evidence the transaction
    # reached the signer; anything smaller implies the wallet paid neither
    # swap input nor gas, which is impossible for a truly successful tx.
    from_floor_abs = Decimal("0")
    if gas_token == intent.from_token and gas_cost_native is not None and gas_cost_native > 0:
        from_floor_abs = gas_cost_native

    from_delta = actual.get(intent.from_token)
    if from_delta is not None:
        # We expect: from_delta <= -from_floor_abs  (i.e. at least some outflow).
        # Equivalent: from_delta + from_floor_abs <= 0 AND from_delta < 0.
        if from_delta >= 0 and from_floor_abs == 0:
            # Pure swap (non-gas-token in) that showed no outflow at all.
            mismatches.append(
                DeltaMismatch(
                    token=intent.from_token,
                    actual=from_delta,
                    expected_min=Decimal("-Infinity"),
                    expected_max=Decimal("0"),
                )
            )
        elif from_delta > -from_floor_abs:
            # Wallet moved less than the observed gas — impossible for a
            # real successful swap.
            mismatches.append(
                DeltaMismatch(
                    token=intent.from_token,
                    actual=from_delta,
                    expected_min=Decimal("-Infinity"),
                    expected_max=-from_floor_abs,
                )
            )

    # Minimum expected inflow on to_token. When the to-token is the native
    # gas token (e.g. USDC -> ETH where ETH pays for gas), a successful
    # small swap can still show a non-positive `to_delta` because gas was
    # paid from the same balance. The real on-chain invariant is
    # `post - pre >= swap_out - gas_cost`; without swap_out we cannot
    # verify the magnitude, but we CAN tolerate a dip of up to gas_cost
    # before flagging a mismatch. Anything below that is unambiguous
    # accounting corruption (the wallet paid gas and got nothing back).
    to_floor = Decimal("0")
    if gas_token == intent.to_token and gas_cost_native is not None and gas_cost_native > 0:
        to_floor = -gas_cost_native

    to_delta = actual.get(intent.to_token)
    if to_delta is not None:
        violates_to_direction = (
            to_delta < to_floor
            if gas_token == intent.to_token and gas_cost_native is not None and gas_cost_native > 0
            else to_delta <= to_floor
        )
        if violates_to_direction:
            mismatches.append(
                DeltaMismatch(
                    token=intent.to_token,
                    actual=to_delta,
                    expected_min=to_floor,
                    expected_max=Decimal("Infinity"),
                )
            )

    return mismatches
