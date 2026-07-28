"""Decode lending / Safe-Roles revert selectors into operator-clear teardown
error messages — VIB-5470 (subsumes VIB-5152).

When a lending teardown reverts, the gateway / orchestrator surfaces the raw
4-byte custom-error selector (e.g. ``Reverted 0x6679996d``) with no human
context. An operator staring at ``0x6679996d`` cannot tell a dust-debt
withdraw-all trap from a Safe-Roles authorization denial — yet the correct
operator action is completely different for each. This module maps the
selectors observed on the lending-unwind teardown path to a plain-English
explanation of *why* the teardown reverted and *what to do about it*.

It **layers on top of** the canonical selector → signature registry
(``KNOWN_CUSTOM_ERRORS`` in
``almanak.framework.execution.submitter.public``) rather than duplicating it:
the ``Signature()`` label comes from that registry, the operator explanation
comes from :data:`_OPERATOR_HINTS` here. The submitter registry is extended in
the same change so the bare signature also decodes everywhere a raw selector is
surfaced (submitter + local simulator).

Selectors are the first 4 bytes of ``keccak256("ErrorName(argTypes)")``. Each
entry below was verified that way (see ``tests/unit/teardown/test_revert_hints.py``):

* ``0x6679996d`` — ``HealthFactorLowerThanLiquidationThreshold()`` — Aave V3
  ``Errors`` library. The dust-debt / withdraw-all-with-residual-debt revert:
  ``withdraw(MAX_UINT256)`` reverts while ANY debt remains (VIB-5448 / VIB-4466).
* ``0xd27b44a9`` — ``ModuleTransactionFailed()`` — Gnosis Safe / Zodiac module
  wrapper. The Roles modifier (``execTransactionWithRole``) re-wraps an inner
  Safe-exec revert under this selector, masking the real cause. NOT itself an
  authorization denial.
* ``0xd0a9bf58`` — ``ConditionViolation(uint8,bytes32)`` — Zodiac Roles v2
  unified permission denial. The call's target / function / parameters are not
  authorized by the deployment's Roles manifest (the ``uint8`` Status enum
  encodes which sub-rule failed). A policy denial, not a protocol revert.
* ``0x21e5c4ae`` — ``UserHasAssetWithZeroLtv()`` — Aave V3 ``Errors`` library.
  The zero-LTV market revert (VIB-6111): governance set ``ltv = 0`` on the
  reserve, so ``setUserUseReserveAsCollateral(asset, true)`` reverts. Measured
  on an Aave V3 Mantle fork, where all 10 reserves carry ``ltv = 0``.
* ``0x5b263df7`` — ``LtvValidationFailed()`` — Aave V3. The post-action LTV
  validation for the user's whole position failed.
* ``0xd0739dae`` — ``UnderlyingCannotBeUsedAsCollateral()`` — Aave V3. The
  reserve is configured as non-collateral (``usageAsCollateralEnabled ==
  false``).
* ``0x911ceb81`` — ``CollateralCannotCoverNewBorrow()`` — Aave V3. The enabled
  collateral is insufficient for the requested borrow.
"""

from __future__ import annotations

import re

# A 4-byte selector (``0x`` + exactly 8 hex chars) embedded anywhere in a
# free-form revert string. We deliberately do NOT anchor the right edge: a
# parameterized custom error surfaces as ``<selector><abi-args>`` (one contiguous
# hex blob), so the selector is the *head* of a longer hex run. Membership is
# then checked against the closed :data:`_OPERATOR_HINTS` key set, so the only
# false-positive vector is an address whose first 4 bytes equal one of our
# hinted lending selectors exactly — cryptographically negligible, and even then the
# effect is only an appended (correct-for-that-selector) hint.
_SELECTOR_RE = re.compile(r"0x[0-9a-fA-F]{8}")

# Marker that makes :func:`annotate_teardown_error` idempotent — re-annotating an
# already-annotated string is a no-op, so wiring the helper at several teardown
# surfaces never double-appends.
_HINT_MARKER = " | operator hint:"

# selector → plain-English operator explanation. Detection order matters: a
# message that somehow carries BOTH the actionable root cause AND the opaque
# Safe wrapper should resolve to the root cause, so the wrapper
# (``ModuleTransactionFailed``) is listed LAST and matched only when no more
# specific selector is present. Data-driven: extend by adding one row.
_OPERATOR_HINTS: dict[str, str] = {
    "0x6679996d": (
        "Aave-family lending revert (HealthFactorLowerThanLiquidationThreshold): "
        "the requested withdraw/borrow would push the position's health factor "
        "below the liquidation threshold. On teardown this is the dust-debt trap "
        "— a withdraw-all (MAX_UINT256) of collateral reverts while ANY debt "
        "remains. Repay the outstanding debt to zero before withdrawing all "
        "collateral (HF-safe unwind staircase: VIB-5448 / VIB-4466)."
    ),
    "0xd0a9bf58": (
        "Zodiac Roles v2 permission denial (ConditionViolation): the call was "
        "blocked by the Safe's Roles policy, NOT by the protocol. The teardown "
        "call's target + function selector + parameters are not authorized in "
        "the deployment's Roles manifest (the uint8 Status arg encodes which "
        "sub-rule failed). Extend the permission manifest to cover this "
        "lending-unwind call."
    ),
    "0x21e5c4ae": (
        "Aave-family lending revert (UserHasAssetWithZeroLtv): this reserve's "
        "LTV is zero on this market, so the market no longer accepts it as "
        "collateral and setUserUseReserveAsCollateral(asset, true) reverts. "
        "Supplying the SAME asset WITHOUT use_as_collateral still works — the "
        "supply leg itself is fine, only the collateral toggle is rejected. "
        "This is almost always a governance risk-parameter change (an LTV cut "
        "to 0 on the market's reserves), NOT a bug in the strategy: re-check "
        "the reserve's configured LTV before treating it as a code defect."
    ),
    "0x5b263df7": (
        "Aave-family lending revert (LtvValidationFailed): the post-action LTV "
        "validation over the user's whole position failed — enabling or keeping "
        "this asset as collateral would leave the account's aggregate LTV "
        "invalid (typically because one or more supplied reserves now carry "
        "LTV = 0). Disable collateral on the zero-LTV reserve, or supply "
        "without use_as_collateral."
    ),
    "0xd0739dae": (
        "Aave-family lending revert (UnderlyingCannotBeUsedAsCollateral): the "
        "market has usageAsCollateralEnabled = false for this reserve, so it "
        "can never be toggled on as collateral here. Supply without "
        "use_as_collateral, or pick a collateral-eligible reserve. A market "
        "risk-parameter fact, not a strategy bug."
    ),
    "0x911ceb81": (
        "Aave-family lending revert (CollateralCannotCoverNewBorrow): the "
        "collateral currently enabled for the account cannot support the "
        "requested borrow. On a market that has cut LTV to 0, previously "
        "usable collateral contributes zero borrowing power — enable "
        "additional collateral or reduce the borrow amount."
    ),
    "0xd27b44a9": (
        "Safe/Zodiac module wrapper revert (ModuleTransactionFailed): this is "
        "Zodiac's OUTER wrapper, not the root cause and not an authorization "
        "denial. The Roles modifier (execTransactionWithRole) caught the inner "
        "revert and re-threw this selector, so the inner protocol error is NOT "
        "present anywhere in this receipt — the receipt cannot tell you why the "
        "call failed. To surface it, re-run the failing call via eth_call "
        "against the PARENT block (the block before the one that reverted) with "
        "the same from/to/data, and decode the selector that comes back. On a "
        "batched (MultiSend) intent the whole bundle reverts atomically, so "
        "replay the individual leg you suspect, not just the outer batch. "
        "Common inner causes on the lending path: an Aave health-factor revert "
        "(HealthFactorLowerThanLiquidationThreshold) or a zero-LTV collateral "
        "toggle (UserHasAssetWithZeroLtv)."
    ),
}
# NOTE: hint text names inner causes by SIGNATURE, never by hex selector.
# ``annotate_teardown_error`` appends the hint to the raw error string, so a hex
# selector embedded in a hint would be re-detected by ``find_revert_selector``
# on the annotated string and mis-attribute the root cause.


def _signature_for(selector: str) -> str | None:
    """Return the canonical ``Signature()`` label for ``selector`` from the
    submitter's shared registry, or ``None`` if unregistered.

    Imported lazily (mirrors ``almanak.framework.execution.simulator.local``) to
    keep the submitter's heavy import graph out of teardown import time.
    """
    from almanak.framework.execution.submitter.public import KNOWN_CUSTOM_ERRORS

    return KNOWN_CUSTOM_ERRORS.get(selector)


def _normalize_selector(selector: str) -> str:
    """Lowercase + ``0x``-prefix a selector so lookups are case/prefix tolerant."""
    sel = selector.strip().lower()
    if not sel.startswith("0x"):
        sel = "0x" + sel
    return sel


def operator_hint_for_selector(selector: str) -> str | None:
    """Decode a single revert ``selector`` into an operator-clear message.

    Composes ``"Signature() — explanation"`` when the selector has both a
    canonical signature (from ``KNOWN_CUSTOM_ERRORS``) and an operator hint;
    falls back to the explanation alone if the signature is unregistered.
    Returns ``None`` for selectors we have no hint for, so callers leave the raw
    error untouched rather than guessing.
    """
    if not selector:
        return None
    sel = _normalize_selector(selector)
    hint = _OPERATOR_HINTS.get(sel)
    if hint is None:
        return None
    signature = _signature_for(sel)
    return f"{signature} — {hint}" if signature else hint


def find_revert_selector(error_text: str | None) -> str | None:
    """Return the most actionable known selector embedded in ``error_text``.

    Scans the free-form revert string for any 4-byte selector we carry an
    operator hint for, honouring :data:`_OPERATOR_HINTS` insertion order so a
    specific root-cause selector wins over the opaque Safe wrapper when both are
    present. Returns ``None`` when no hinted selector is found.

    Defensive: the teardown failure surface feeds this whatever the orchestrator
    produced, so a non-``str`` value (e.g. a stray exception object) or an
    empty / ``None`` value degrades to "no hint" rather than raising inside the
    diagnostics path.
    """
    if not isinstance(error_text, str) or not error_text:
        return None
    found = {m.group(0).lower() for m in _SELECTOR_RE.finditer(error_text)}
    if not found:
        return None
    for sel in _OPERATOR_HINTS:  # insertion order = detection priority
        if sel in found:
            return sel
    return None


def annotate_teardown_error(error_text: str | None) -> str | None:
    """Append the operator-clear explanation to a raw teardown revert string.

    Idempotent and lossless: the original message is preserved verbatim and the
    decoded hint is appended after :data:`_HINT_MARKER`. Returns ``error_text``
    unchanged when there's nothing to annotate — a non-``str`` value (passed
    through untouched, never raising), an empty / ``None`` value, a string with
    no known selector, or one that is already annotated.
    """
    if not isinstance(error_text, str) or not error_text:
        return error_text
    if _HINT_MARKER in error_text:
        return error_text
    selector = find_revert_selector(error_text)
    if selector is None:
        return error_text
    hint = operator_hint_for_selector(selector)
    if hint is None:  # pragma: no cover - find_revert_selector guarantees a hint
        return error_text
    return f"{error_text}{_HINT_MARKER} {hint}"
