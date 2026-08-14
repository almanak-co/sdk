"""Protective-minimum derivation and the encode-boundary floor guard (VIB-6212).

This module is the single shared contract for "what minimum output may be encoded
into router calldata". It exists because the same defect was written independently
by ten connectors: a slippage tolerance applied as a *multiplier on a
caller-supplied minimum* rather than as a haircut on a *quoted expected output*.

The inverted model cannot protect anything::

    min_out = caller_min * (10_000 - slippage_bps) // 10_000

- a caller with no estimate passes ``0`` -> encodes ``0``
- a caller wanting a deliberate floor passes ``1`` -> ``1 * 9950 // 10000 == 0``
  (integer-floor annihilation: a "1 wei floor" is not a floor)

The correct model, used here and by every connector that gets it right::

    min_out = quoted_expected * (1 - max_slippage)   # then assert > 0

Two entry points, deliberately separated:

``derive_min_out``
    For compilers that hold a quoted expected output. Applies the haircut EXACTLY
    ONCE and refuses to return a non-positive minimum.

``require_protective_min``
    The chokepoint. Call immediately before ABI-encoding a minimum into calldata,
    including on paths where the minimum arrived from somewhere else. This is what
    makes a future regression impossible to ship silently.

Both ``raise`` rather than ``assert`` — ``python -O`` strips assertions, and a
safety guard that evaporates under an optimisation flag is not a safety guard.

Failure classification
----------------------
``UnprotectedTradeError`` is a SAFETY REFUSAL, not a fault. Compilers catching it
must return ``CompilationStatus.FAILED`` with ``is_safety_refusal=True`` so the
runner maps it to :class:`FailureKind.GUARD_REFUSED` and it does not count toward
the circuit breaker (VIB-5746) — zero transactions were built, the on-chain
position is untouched, the guard did its job.

A failure to *read* the quote is a different thing entirely: that is
``is_transient=True``, because teardown reads ``retryable=result.is_transient``
(``teardown_manager.py``) and an RPC blip must not become a permanent teardown
failure. Deciding transient-ness is the caller's job, not this module's.

See ``docs/internal/blueprints/03-intent-system.md`` and blueprint 14's
"MEV protection on all swaps" invariant.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.intents._compiler_helpers import BPS_PER_UNIT, compute_min_amount_out

__all__ = [
    "MAX_SLIPPAGE_BPS_EXCLUSIVE",
    "UnprotectedTradeError",
    "derive_min_out",
    "require_protective_min",
    "slippage_bps_to_fraction",
    "validate_max_slippage_fraction",
]

#: A slippage of 100% is not a tolerance, it is the absence of one: it derives a
#: minimum of zero from any expected output. Callers must stay strictly below.
MAX_SLIPPAGE_BPS_EXCLUSIVE = BPS_PER_UNIT


class UnprotectedTradeError(ValueError):
    """Raised when a trade would be encoded with no effective output protection.

    A safety refusal, not a fault — see the module docstring. Compilers should
    surface this as ``FAILED`` with ``is_safety_refusal=True``.
    """

    def __init__(self, context: str, detail: str) -> None:
        self.context = context
        self.detail = detail
        super().__init__(
            f"refusing to encode an unprotected minimum for {context}: {detail} "
            f"(MEV theft vector — see almanak/framework/intents/min_out_guard.py)"
        )


def validate_max_slippage_fraction(
    value: Decimal | None,
    *,
    field_name: str = "max_slippage",
    error_type: type[Exception] = ValueError,
) -> None:
    """Enforce the ``[0, 1)`` bound on a slippage tolerance expressed as a fraction.

    This is the single definition of "which tolerances are legal" for every intent
    that carries one. It exists because the bound was previously written out
    longhand at seven validator sites, which is seven chances for the bound and
    its message to drift apart — and one of those sites having an inclusive ``1``
    is enough to defeat downstream guards: the unvalidated arithmetic
    ``x * (1 - Decimal("1"))`` collapses every positive ``x`` to zero.

    ``1`` is rejected rather than clamped. A caller asking for 100% tolerance is
    asking for no floor at all; degrading that to "the largest tolerance we do
    allow" would grant the request silently, which is how the defect class this
    guards against got written ten times over.

    Args:
        value: The tolerance as a fraction (``Decimal("0.005")`` = 0.5%), or
            ``None`` for the optional fields, which are skipped.
        field_name: The attribute name to quote in the error, so the message
            still reads correctly on intents that name the field differently.
        error_type: Exception class to raise. Call sites keep their own error
            type — ``BridgeIntent`` raises ``InvalidBridgeError``,
            ``EnsureBalanceIntent`` raises ``InvalidEnsureBalanceError``, and the
            rest raise ``ValueError`` — so passing the type here centralises the
            bound and the wording without flattening the exception taxonomy that
            callers already catch on.

    Raises:
        error_type: If ``value`` is outside ``[0, 1)``.
    """
    if value is None:
        return
    if value < Decimal("0") or value >= Decimal("1"):
        raise error_type(
            f"{field_name} must be in [0, 1) (got {value}) — a tolerance of 1 (100%) "
            f"derives a minimum output of zero from any expected output, which "
            f"accepts near-total loss and is not a tolerance at all"
        )


def slippage_bps_to_fraction(slippage_bps: int, *, context: str) -> Decimal:
    """Convert basis points to the ``[0, 1)`` fraction ``compute_min_amount_out`` takes.

    Args:
        slippage_bps: Tolerance in basis points. Must be in ``[0, 10_000)``.
            ``10_000`` (100%) is rejected because it derives a zero minimum from
            any expected output, silently defeating the guard.
        context: Human-readable description of the call site, used in errors.

    Raises:
        UnprotectedTradeError: If ``slippage_bps`` is outside ``[0, 10_000)``.
    """
    if slippage_bps < 0 or slippage_bps >= MAX_SLIPPAGE_BPS_EXCLUSIVE:
        raise UnprotectedTradeError(
            context,
            f"slippage_bps must be in [0, {MAX_SLIPPAGE_BPS_EXCLUSIVE}) (got {slippage_bps})",
        )
    return Decimal(slippage_bps) / Decimal(MAX_SLIPPAGE_BPS_EXCLUSIVE)


def derive_min_out(expected_out: int, max_slippage: Decimal, *, context: str) -> int:
    """Derive a protective minimum from a quoted expected output.

    The haircut is applied EXACTLY ONCE here. Callers must not multiply the result
    by a slippage factor again — a compiler deriving ``expected * (1 - slip)`` and
    an SDK builder doing the same yields ``expected * (1 - slip)**2``.

    Args:
        expected_out: Quoted expected output in smallest units. Must be > 0; a
            non-positive expected output means the quote is unusable, which is a
            refusal rather than a licence to encode zero.
        max_slippage: Tolerance as a fraction, in ``[0, 1)``.
        context: Human-readable description of the call site, used in errors.

    Returns:
        A strictly positive minimum output.

    Raises:
        UnprotectedTradeError: If the quote is unusable, the tolerance is out of
            range, or the derived minimum would be non-positive (which happens
            whenever ``expected_out`` is small enough that the haircut truncates
            it to zero under integer arithmetic).
    """
    if expected_out <= 0:
        raise UnprotectedTradeError(context, f"expected_out must be > 0 to derive a minimum (got {expected_out})")
    if max_slippage < Decimal("0") or max_slippage >= Decimal("1"):
        raise UnprotectedTradeError(context, f"max_slippage must be in [0, 1) (got {max_slippage})")
    return require_protective_min(compute_min_amount_out(expected_out, max_slippage), context=context)


def require_protective_min(min_out: int, *, context: str) -> int:
    """Chokepoint: refuse a non-positive minimum immediately before encoding.

    Call this at the ABI-encode boundary, on every path, including ones where the
    minimum was computed elsewhere. It is the assertion that makes an unprotected
    trade impossible to ship rather than merely unlikely.

    Args:
        min_out: The minimum output about to be encoded, in smallest units.
        context: Human-readable description of the call site, used in errors.

    Returns:
        ``min_out`` unchanged, so this can wrap an argument in place.

    Raises:
        UnprotectedTradeError: If ``min_out`` is not strictly positive.
    """
    if min_out <= 0:
        raise UnprotectedTradeError(
            context,
            f"minimum output must be > 0, got {min_out} — a zero minimum accepts any output, including near-total loss",
        )
    return min_out
