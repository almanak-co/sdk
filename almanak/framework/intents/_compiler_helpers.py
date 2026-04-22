"""Pure compiler skeleton helpers shared by ``IntentCompiler._compile_swap``
and ``IntentCompiler._compile_lp_open`` (Phase 6B.2).

These helpers are pure functions (no I/O, no side effects) that implement
the shared skeleton of the highest-complexity compile paths. They are
designed to be wired into the existing ``_compile_swap`` and
``_compile_lp_open`` methods in Phase 6B.3 / 6B.4 respectively. This PR
adds the helpers and their isolation tests only; the consuming methods
are unchanged.

Scope / contract:
    - Every helper here MUST be pure. Anything that needs ``self._gateway_client``,
      ``self._allowance_cache``, RPC access, logging, or time-of-day MUST NOT
      live here. Use callables the caller passes in (e.g. ``now_ts``) if a
      deterministic clock is needed.
    - Helpers do NOT build error messages that tests grep. The caller builds
      the final ``CompilationResult.error`` string so existing string assertions
      in ``tests/unit/intents/test_compiler_swap_lp_characterization.py``
      continue to hold at the wiring stage.
    - No circular imports: this module only imports from the standard library,
      ``..models.reproduction_bundle.ActionBundle``, and the compiler's own
      data classes in ``.compiler_models``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..models.reproduction_bundle import ActionBundle
    from .compiler_models import TransactionData


# ---------------------------------------------------------------------------
# Slippage / min-amount math
# ---------------------------------------------------------------------------


def compute_min_amount_out(expected_amount: int, max_slippage: Decimal) -> int:
    """Return the lower bound for a transaction output given slippage tolerance.

    Mirrors the single-line math used both in swap compilation (one min_amount_out)
    and in LP_OPEN compilation (one per token). Using a helper here means callers
    can't accidentally apply slippage twice or drift on truncation behaviour.

    Args:
        expected_amount: The expected output amount (integer wei / smallest units).
            Must be >= 0.
        max_slippage: Slippage tolerance as a fraction (``Decimal("0.01")`` = 1%).
            Must be in ``[0, 1]`` inclusive. ``0`` is allowed and yields
            ``expected_amount`` unchanged.

    Returns:
        ``int(expected_amount * (1 - max_slippage))`` using Decimal arithmetic
        so rounding matches the existing compiler code exactly. Always ``int``.

    Raises:
        ValueError: If ``expected_amount`` is negative, or ``max_slippage`` is
            outside ``[0, 1]``.
    """
    if expected_amount < 0:
        raise ValueError(f"expected_amount must be >= 0 (got {expected_amount})")
    if max_slippage < Decimal("0") or max_slippage > Decimal("1"):
        raise ValueError(f"max_slippage must be in [0, 1] (got {max_slippage})")
    return int(Decimal(str(expected_amount)) * (Decimal("1") - max_slippage))


def choose_safer_quote(oracle_estimate: int, quoter_amount: int | None) -> tuple[int, bool]:
    """Pick the safer of the oracle estimate and the on-chain quoter amount.

    Callers use this to produce the slippage basis for ``min_amount_out``:
    when the quoter is available AND lower than the oracle estimate, it
    reflects real pool depth and is the safer floor. Otherwise the oracle
    estimate is used as-is.

    Args:
        oracle_estimate: Oracle-derived expected output (must be > 0 for the
            caller's slippage math to make sense; this helper does not enforce).
        quoter_amount: On-chain quoter output, or ``None`` if the quoter call
            was skipped / failed.

    Returns:
        ``(safer_amount, used_quoter)`` — ``used_quoter`` is True iff the
        quoter value was chosen (strictly less than the oracle estimate).
    """
    if quoter_amount is not None and quoter_amount < oracle_estimate:
        return quoter_amount, True
    return oracle_estimate, False


# ---------------------------------------------------------------------------
# Price impact guard
# ---------------------------------------------------------------------------


class PriceImpactDecision(Enum):
    """Outcome categories for ``check_price_impact``.

    Kept as an ``Enum`` (not strings) so callers can dispatch without
    grep-brittle string matching. Error messages are built by the caller
    because the real compiler's messages include protocol-specific context
    (token symbols, pool address, etc.) that doesn't belong in a pure helper.
    """

    OK = "OK"
    IMPACT_TOO_HIGH = "IMPACT_TOO_HIGH"
    QUOTER_MISSING_FAIL_CLOSED = "QUOTER_MISSING_FAIL_CLOSED"
    SKIPPED_OFFLINE = "SKIPPED_OFFLINE"
    SKIPPED_NO_ORACLE = "SKIPPED_NO_ORACLE"


@dataclass(frozen=True)
class PriceImpactCheckResult:
    """Result of a price-impact guard check (pure data).

    Attributes:
        decision: Categorical outcome.
        price_impact: Computed impact as a ``Decimal`` fraction when the
            computation actually ran. ``None`` for non-IMPACT decisions.
        effective_max_impact: The maximum impact threshold that was applied
            (``intent.max_price_impact`` override or config default).
            ``None`` when the check was skipped.
    """

    decision: PriceImpactDecision
    price_impact: Decimal | None = None
    effective_max_impact: Decimal | None = None


def check_price_impact(
    *,
    oracle_estimate: int,
    quoter_amount: int | None,
    intent_max_impact: Decimal | None,
    config_max_impact: Decimal,
    offline_mode: bool,
    using_placeholders: bool,
) -> PriceImpactCheckResult:
    """Compute and evaluate swap price impact.

    Mirrors the guard in ``IntentCompiler._compile_swap`` (~lines 1510-1554 in
    the current file) so a future wiring PR (Phase 6B.3) can replace that
    branch with a single call plus a result-switch.

    Decision table:
        - ``quoter_amount is not None`` AND ``oracle_estimate > 0`` AND NOT
          ``using_placeholders`` → compute impact, compare to
          ``intent_max_impact or config_max_impact``. Returns OK or
          IMPACT_TOO_HIGH with the computed impact attached.
        - ``using_placeholders`` (unit-test mode): SKIPPED_OFFLINE. Callers
          must not use oracle-only slippage in this mode; the real compiler
          permits it because placeholder prices already decouple the math
          from reality.
        - ``quoter_amount is None`` AND ``oracle_estimate > 0`` AND
          ``offline_mode`` True → SKIPPED_OFFLINE.
        - ``quoter_amount is None`` AND ``oracle_estimate > 0`` AND
          ``offline_mode`` False → QUOTER_MISSING_FAIL_CLOSED (caller fails
          compilation).
        - ``oracle_estimate == 0`` → SKIPPED_NO_ORACLE (nothing to compare
          against).

    Args:
        oracle_estimate: Oracle-derived expected output (wei).
        quoter_amount: On-chain quoter output or None.
        intent_max_impact: Per-intent override or None.
        config_max_impact: Compiler config default (always set by
            ``IntentCompilerConfig.__post_init__``).
        offline_mode: ``using_placeholders OR permission_discovery``
            — matches the compiler's offline-mode semantics and relaxes the
            quoter-missing rule.
        using_placeholders: Whether ``IntentCompiler._using_placeholders``
            is True. Distinct from ``offline_mode`` because the IMPACT branch
            is skipped when using placeholders, but the QUOTER_MISSING branch
            is only relaxed when ``offline_mode`` is True.

    Returns:
        PriceImpactCheckResult with the decision and (for IMPACT decisions)
        the computed impact and effective max.
    """
    if oracle_estimate <= 0:
        return PriceImpactCheckResult(decision=PriceImpactDecision.SKIPPED_NO_ORACLE)

    max_impact = intent_max_impact if intent_max_impact is not None else config_max_impact

    if quoter_amount is not None:
        if using_placeholders:
            return PriceImpactCheckResult(decision=PriceImpactDecision.SKIPPED_OFFLINE)
        price_impact = Decimal(1) - (Decimal(quoter_amount) / Decimal(oracle_estimate))
        if price_impact > max_impact:
            return PriceImpactCheckResult(
                decision=PriceImpactDecision.IMPACT_TOO_HIGH,
                price_impact=price_impact,
                effective_max_impact=max_impact,
            )
        return PriceImpactCheckResult(
            decision=PriceImpactDecision.OK,
            price_impact=price_impact,
            effective_max_impact=max_impact,
        )

    # quoter_amount is None
    if offline_mode:
        return PriceImpactCheckResult(decision=PriceImpactDecision.SKIPPED_OFFLINE)
    return PriceImpactCheckResult(decision=PriceImpactDecision.QUOTER_MISSING_FAIL_CLOSED)


# ---------------------------------------------------------------------------
# Deadline
# ---------------------------------------------------------------------------


def compute_deadline(default_deadline_seconds: int, *, now_ts: int | None = None) -> int:
    """Compute a Unix deadline timestamp for a compiled transaction.

    Mirrors the ``int(datetime.now(UTC).timestamp()) + self.default_deadline_seconds``
    line that appears in both ``_compile_swap`` and ``_compile_lp_open``. Accepts
    an injectable ``now_ts`` so tests don't need to patch module globals.

    Args:
        default_deadline_seconds: Seconds to add to the current time. Must be
            a positive integer (a zero-or-negative deadline is a misconfiguration
            that would produce immediately-expired transactions).
        now_ts: Optional explicit current Unix timestamp (seconds). When None,
            uses ``datetime.now(UTC)``.

    Returns:
        Unix timestamp in seconds.

    Raises:
        ValueError: If ``default_deadline_seconds`` is not positive.
    """
    if default_deadline_seconds <= 0:
        raise ValueError(f"default_deadline_seconds must be > 0 (got {default_deadline_seconds})")
    base = now_ts if now_ts is not None else int(datetime.now(UTC).timestamp())
    return base + default_deadline_seconds


# ---------------------------------------------------------------------------
# ActionBundle assembly
# ---------------------------------------------------------------------------


def sum_transaction_gas(transactions: list[TransactionData]) -> int:
    """Sum ``gas_estimate`` across a list of ``TransactionData`` entries.

    Trivial on its own, but extracted so the assembly helper below can stay
    small and the ``total_gas = sum(tx.gas_estimate for tx in transactions)``
    pattern has exactly one definition.
    """
    return sum(tx.gas_estimate for tx in transactions)


def assemble_action_bundle(
    *,
    intent_type: str,
    transactions: list[TransactionData],
    metadata: dict,
) -> ActionBundle:
    """Build an ``ActionBundle`` from a list of typed transactions and metadata.

    Mirrors the identical block that appears at the end of ``_compile_swap``
    (line ~1611) and ``_compile_lp_open`` (line ~2724):

        action_bundle = ActionBundle(
            intent_type=IntentType.SWAP.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata={ ... caller-built ... },
        )

    The caller still owns metadata construction (keys are intent-specific);
    this helper only standardises the ``transactions=[tx.to_dict() ...]``
    mapping so no one accidentally passes raw dataclass instances into a
    field that ``ActionBundle.to_dict()`` already assumes is JSON-serialisable.

    Args:
        intent_type: Typically ``IntentType.SWAP.value`` / ``IntentType.LP_OPEN.value``.
        transactions: Ordered list of ``TransactionData`` to serialize.
        metadata: Intent-specific metadata dict. Passed through untouched so
            existing tests that grep specific keys (``amount_in``,
            ``min_amount_out``, ``tick_lower``, ...) continue to match.

    Returns:
        A new ``ActionBundle`` ready for assignment to
        ``CompilationResult.action_bundle``.
    """
    # Imported here (not at module scope) to keep the helper module's import
    # graph minimal at test-collection time. ``ActionBundle`` is also
    # imported under TYPE_CHECKING at the top of this file for type hints.
    from ..models.reproduction_bundle import ActionBundle as _ActionBundle

    return _ActionBundle(
        intent_type=intent_type,
        transactions=[tx.to_dict() for tx in transactions],
        metadata=metadata,
    )
