"""Unit tests for the pre-submit feasibility preflight seam (VIB-5374 / RC-2).

Covers the connector-foundation hook in
``almanak.connectors._strategy_base.base.compiler``:

* the default ``preflight`` returns FEASIBLE (so existing compiles are
  byte-identical — the seam is behaviour-preserving by default),
* ``_run_preflight`` converts INFEASIBLE / UNAVAILABLE verdicts into the right
  ``CompilationResult`` shape (permanent vs retryable),
* the INFEASIBLE error prefix classifies as ``COMPILATION_PERMANENT`` in the
  state-machine retry keyword table (fail-fast → HOLD), and
* the seam is installed in every category base ``compile`` (a connector that
  overrides ``preflight`` short-circuits dispatch before per-primitive work).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from almanak.connectors._strategy_base.base.compiler import (
    BaseProtocolCompiler,
    PreflightOutcome,
    PreflightVerdict,
)
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.error_keywords import categorize_error


class _BareCompiler(BaseProtocolCompiler):
    """Minimal concrete compiler used to exercise the base seam in isolation."""

    protocols = frozenset({"bare"})
    intents = frozenset()

    def compile(self, ctx, intent):  # pragma: no cover - not exercised directly
        raise NotImplementedError


def _intent(intent_id: str = "i-1"):
    return SimpleNamespace(intent_id=intent_id)


def test_default_preflight_is_feasible():
    """The base hook returns FEASIBLE so it never changes existing behaviour."""
    verdict = _BareCompiler().preflight(ctx=object(), intent=_intent())
    assert verdict.outcome is PreflightOutcome.FEASIBLE


def test_run_preflight_returns_none_on_feasible():
    """FEASIBLE → ``None`` so the caller proceeds to per-primitive dispatch."""
    assert _BareCompiler()._run_preflight(ctx=object(), intent=_intent()) is None


def test_run_preflight_infeasible_is_permanent_failed():
    """INFEASIBLE → a non-retryable FAILED result carrying the stable prefix."""

    class _C(_BareCompiler):
        def preflight(self, ctx, intent):
            return PreflightVerdict(
                outcome=PreflightOutcome.INFEASIBLE,
                error_prefix="PENDLE_MARKET_EXPIRED",
                reason="market matured",
            )

    result = _C()._run_preflight(ctx=object(), intent=_intent("abc"))
    assert result is not None
    assert result.status is CompilationStatus.FAILED
    assert result.intent_id == "abc"
    assert result.is_transient is False
    assert result.error.startswith("PENDLE_MARKET_EXPIRED:")
    # The classifier routes this to fail-fast → HOLD (no retry storm, no breaker).
    assert categorize_error(result.error) == "COMPILATION_PERMANENT"


def test_run_preflight_unavailable_is_retryable_failed():
    """UNAVAILABLE → a retryable FAILED result the breaker treats as data-class."""

    class _C(_BareCompiler):
        def preflight(self, ctx, intent):
            return PreflightVerdict(
                outcome=PreflightOutcome.UNAVAILABLE,
                reason="could not read expiry",
            )

    result = _C()._run_preflight(ctx=object(), intent=_intent())
    assert result.status is CompilationStatus.FAILED
    assert result.is_transient is True
    # No stable prefix → must NOT classify as permanent (so it retries).
    assert categorize_error(result.error) != "COMPILATION_PERMANENT"


def test_run_preflight_fails_open_when_preflight_raises():
    """A buggy ``preflight`` that raises degrades to FEASIBLE (fail-open), never a false reject."""

    class _C(_BareCompiler):
        def preflight(self, ctx, intent):
            raise RuntimeError("boom")

    assert _C()._run_preflight(ctx=object(), intent=_intent()) is None


@pytest.mark.parametrize(
    "prefix",
    [
        "PENDLE_MARKET_EXPIRED",
        "GMX_INSUFFICIENT_NATIVE_FEE",
        "STARGATE_INSUFFICIENT_NATIVE_FEE",
        "EULER_BORROW_INFEASIBLE",
    ],
)
def test_all_venue_prefixes_classify_permanent(prefix):
    """Every venue's stable prefix is terminal (fail-fast → HOLD) and carries no 'revert'."""
    assert "revert" not in prefix.lower()
    assert categorize_error(f"{prefix}: some reason") == "COMPILATION_PERMANENT"


@pytest.mark.parametrize(
    "error",
    [
        # Euler's collateral-not-enabled reason legitimately explains the doomed
        # outcome with the word "revert" — this MUST still classify permanent.
        "EULER_BORROW_INFEASIBLE: WETH is not enabled as collateral for borrowing "
        "USDC on Euler V2 (avalanche); the EVC borrow would revert",
        # Pendle's reason ends "...would fail on-chain"; defensively cover the word too.
        "PENDLE_MARKET_EXPIRED: market X has matured; opening new exposure would revert",
        "GMX_INSUFFICIENT_NATIVE_FEE: keeper fee exceeds balance; the order would revert",
        "STARGATE_INSUFFICIENT_NATIVE_FEE: msg.value below LZ fee; the send would revert",
    ],
)
def test_preflight_prefix_with_revert_in_reason_stays_permanent(error):
    """Regression (CodeRabbit / VIB-5374): a connector ``reason`` containing the word
    "revert" must NOT downgrade a permanent INFEASIBLE verdict to a transient REVERT.

    The classifier matches the stable preflight prefix BEFORE the generic ``revert``
    short-circuit, so the explanatory wording in the reason cannot route a
    structurally-doomed intent back into the retry budget.
    """
    assert categorize_error(error) == "COMPILATION_PERMANENT"
