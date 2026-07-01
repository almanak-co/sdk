"""Shared, money-critical ERC-20 approval sequencing (VIB-5492).

ONE implementation of the approve-sequence *decision*, consumed by both the
framework intent compiler (``IntentCompiler._build_approve_tx``) and connector
adapters that build approvals directly (e.g. Curve
``CurveAdapter._build_approve_txs``). Two independent approval-sequencing
implementations on a fund-safety path WILL drift — a USDT-class token that
reverts on a non-zero -> non-zero ``approve`` (``require(value == 0 || allowance
== 0)``) silently kills the whole bundle — so this module is the single source
of truth for the ordering and skip logic:

    * **seed from the current allowance** — never assume zero, and never skip an
      approve on cache alone;
    * **skip only on POSITIVELY confirmed sufficiency** — an unconfirmed
      (``None``) allowance never short-circuits the approve;
    * **reset-to-zero BEFORE approving** a non-zero (or unconfirmed) allowance on
      a reset-requiring token, ordered so a mid-bundle failure can only ever
      strand a *zero* allowance (re-approvable), never a stale non-zero one.

Everything that is a **caller policy** — how the current allowance is resolved
(typed gateway ``query_allowance`` vs raw ``eth_call``; whether a failed read is
reported as ``0`` or ``None``), whether a given token needs a reset (an allowlist
vs "always"), what value to approve (``MAX_UINT256`` vs a buffered amount), and
how the concrete transaction object is built — stays with the caller and is
injected here. This keeps the risk-carrying control flow in one tested place
while letting each connector keep its own resolution mechanism and posture.

The function is generic over the transaction type ``T`` (the framework compiler
and the Curve adapter use *different* ``TransactionData`` classes), so it never
imports either — it only orders the caller's own builders.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def build_approval_sequence(
    *,
    amount: int,
    current_allowance: int | None,
    reset_before_change: bool,
    approval_amount: int,
    build_reset_tx: Callable[[], T],
    build_approve_tx: Callable[[int], T],
) -> list[T]:
    """Return the ordered approve transaction(s) needed to make ``amount`` spendable.

    Args:
        amount: The amount that must become spendable by the spender.
        current_allowance: The current on-chain allowance, or ``None`` when it
            could NOT be positively confirmed (read failed, or no transport to
            read with). ``None`` is treated as "possibly non-zero" so the caller
            fails toward a safe reset — the allowance is never assumed zero on an
            unconfirmed read.
        reset_before_change: Caller policy — does a non-zero -> non-zero
            ``approve`` on this token revert (USDT-class
            ``require(value == 0 || allowance == 0)``)? When ``True``, a
            reset-to-zero is emitted before changing a non-zero (or unconfirmed)
            allowance. When ``False``, the token tolerates a direct re-approve and
            no reset is emitted.
        approval_amount: The value to approve once (e.g. ``MAX_UINT256`` or a
            buffered amount) — a caller posture, not a safety property.
        build_reset_tx: Builds the ``approve(spender, 0)`` reset transaction.
        build_approve_tx: Builds the ``approve(spender, value)`` transaction for
            the passed value.

    Returns:
        * ``[]`` when ``current_allowance`` is CONFIRMED to already cover
          ``amount``;
        * ``[approve]`` when no reset is required;
        * ``[reset, approve]`` (reset FIRST — partial-bundle safe) when an
          existing non-zero or unconfirmed allowance must be changed on a
          reset-requiring token.
    """
    # Nothing needs to become spendable for a non-positive amount, so emit no
    # tx at all — never a reset, never an approve(0)/approve(MAX) for a zero (or
    # negative) spend. Guards the ``current_allowance is None`` path, where the
    # sufficiency check below cannot short-circuit: without this, ``amount <= 0``
    # + an unconfirmed allowance would emit a pointless reset+approve. (Real
    # callers already early-skip a zero spend upstream; this is a defensive
    # invariant on the shared primitive.)
    if amount <= 0:
        return []

    # Skip only when the allowance is POSITIVELY confirmed to already cover the
    # amount. An unconfirmed (None) allowance never short-circuits an approve —
    # "never skip approve on cache/unknown alone".
    if current_allowance is not None and current_allowance >= amount:
        return []

    txs: list[T] = []
    # Reset-to-zero before changing the allowance when the token needs it AND the
    # existing allowance is non-zero OR unconfirmed. approve(0) never reverts on a
    # USDT-class token, so failing toward a reset is the always-safe default and
    # costs at most one extra tx — versus a lone approve(value) that would revert
    # on a token that turns out to still hold a non-zero allowance.
    if reset_before_change and (current_allowance is None or current_allowance > 0):
        txs.append(build_reset_tx())
    txs.append(build_approve_tx(approval_amount))
    return txs
