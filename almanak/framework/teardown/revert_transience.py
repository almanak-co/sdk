"""Classify a teardown revert as TRANSIENT vs PERMANENT — VIB-5573 (WI-2).

Teardown's existing revert handling is two things, and neither answers the
question this module exists for:

* :mod:`almanak.framework.teardown.revert_hints` decodes a selector into an
  operator-clear *explanation* (diagnostics only — it never decides retry).
* the teardown retry path retries on the *slippage* axis only (bump min-out and
  re-quote), which does nothing for a revert that is not slippage-shaped.

Some teardown reverts, however, are **transient**: they clear within a few
blocks with no change to the intent. The motivating case is MetaMorpho's
withdraw path, which can revert with an arithmetic ``Panic(0x11)`` (underflow)
when its internal withdraw-queue is momentarily inconsistent (e.g. a supply
that has not yet propagated across the queue). Re-submitting the *same* redeem a
few blocks later succeeds. That is a retry on the **TIME axis** — wait and
re-fire — which is a different lane from the slippage retry.

**CRITICAL SAFETY CONSTRAINT (Codex audit).** A bare ``Panic(0x11)`` is *not*
on its own a transient signal — an arithmetic underflow/overflow elsewhere is
just as likely a deterministic bug that will revert identically on every retry,
burning gas and delaying risk-reduction. So we do NOT return TRANSIENT for
"any Panic(17)". The trigger is **context-scoped** by the tuple
``(intent_type, protocol, error-signature)``: a rule fires only when all three
match a curated, human-reviewed entry in :data:`_TRANSIENT_RULES`. Everything
outside the allowlist is :attr:`Transience.UNKNOWN` — the caller then falls
back to its default (non-time-retry) handling.

The :attr:`Transience.PERMANENT` enum member is reserved for a future
data-driven "definitely will not clear — stop retrying now" classification
(e.g. an authorization denial or an exhausted-allowance revert). No PERMANENT
rules are seeded yet; today the module only distinguishes a narrow, vetted
TRANSIENT set from everything-else-UNKNOWN.

The allowlist is a module-level list of :class:`_TransientRule` rows so
connectors can contribute their own vetted (intent, protocol, signature)
signatures later without touching the matcher — add a row, not a branch.

This module is stdlib-only (``re`` + ``enum``) and imports nothing heavy at
import time, so it is safe to reference from any teardown surface.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

__all__ = [
    "Transience",
    "classify_revert_transience",
]


class Transience(StrEnum):
    """How a teardown revert is expected to behave on re-submission.

    * :attr:`TRANSIENT` — expected to clear within a few blocks with no change
      to the intent; safe to retry on the time axis.
    * :attr:`PERMANENT` — expected to revert identically forever; do not retry.
      (Reserved for future rules; none are seeded yet — see module docstring.)
    * :attr:`UNKNOWN` — not in the vetted allowlist; the caller should fall back
      to its default handling (i.e. do NOT time-retry on this alone).
    """

    TRANSIENT = "transient"
    PERMANENT = "permanent"
    UNKNOWN = "unknown"


# ``Panic(uint256)`` selector — first 4 bytes of ``keccak256("Panic(uint256)")``.
# Verified: matches ``ERROR_SELECTORS`` / ``KNOWN_CUSTOM_ERRORS`` in
# ``almanak.framework.execution.submitter.public`` (0x4e487b71). The raw revert
# payload is this selector followed by one 32-byte ABI word holding the panic
# code, so the code is the payload's last byte:
#   0x4e487b71 <62 hex zeros> <2 hex code>  e.g. ...0011 == arithmetic (0x11).
_PANIC_SELECTOR = "0x4e487b71"

# Raw hex form: the panic selector immediately followed by a full 32-byte word
# whose final byte is 0x11 (arithmetic under/overflow). We anchor on exactly
# 62 intervening hex chars + ``11`` so we do NOT match a different panic code
# (0x01 assert, 0x12 div-by-zero, 0x32 array-oob, …) whose word also starts
# with the same zero-run. Case-insensitive; tolerant of surrounding text.
_RAW_ARITHMETIC_PANIC_RE = re.compile(
    _PANIC_SELECTOR + r"0{62}11\b",
    re.IGNORECASE,
)

# Decoded human form emitted by the submitter's Panic decoder:
#   f"Panic({panic_code}): {description}"  ->  "Panic(17)" for 0x11.
# Match ``Panic(17)`` as a whole token so "Panic(170)" / "Panic(171)" etc. do
# not slip through, and so a decimal 18 (0x12 div-by-zero) is NOT matched.
_DECODED_ARITHMETIC_PANIC_RE = re.compile(r"\bPanic\(17\)", re.IGNORECASE)


def _is_arithmetic_panic(error_text: str) -> bool:
    """True iff ``error_text`` carries a Solidity arithmetic ``Panic(0x11)``.

    Detects BOTH surfaced forms:

    * the decoded human string ``"Panic(17)"`` (submitter's Panic decoder), and
    * the raw payload ``0x4e487b71`` + a 32-byte word ending in ``11``.

    Deliberately matches *only* panic code 0x11 (arithmetic under/overflow) —
    other panic codes (0x01 assert, 0x12 div-by-zero, 0x32 array-oob, …) are
    NOT arithmetic-underflow and must not be treated as the MetaMorpho
    withdraw-queue transient.
    """
    return bool(_DECODED_ARITHMETIC_PANIC_RE.search(error_text) or _RAW_ARITHMETIC_PANIC_RE.search(error_text))


@dataclass(frozen=True)
class _TransientRule:
    """One vetted (intent, protocol, signature) → TRANSIENT allowlist entry.

    A rule fires only when ALL of:

    * ``intent_types`` contains the (lower-cased) intent type, AND
    * ``protocols`` contains the (lower-cased) protocol, AND
    * ``signature(error_text)`` is truthy.

    ``why`` documents the vetted transient mechanism for the human reviewer; it
    is not consumed by the matcher.
    """

    intent_types: frozenset[str]
    protocols: frozenset[str]
    signature: Callable[[str], bool]
    why: str


# Data-driven allowlist. EXTEND by adding a row here (ideally contributed by the
# owning connector once it has a vetted transient signature) — never by loosening
# a signature predicate or adding a branch to the matcher. Each row must be
# human-reviewed: it asserts "this exact (intent, protocol, revert) reliably
# clears on its own within blocks", which is a safety claim, not a heuristic.
_TRANSIENT_RULES: tuple[_TransientRule, ...] = (
    _TransientRule(
        # MetaMorpho redeem: an arithmetic Panic(0x11) underflow from a
        # momentarily-inconsistent internal withdraw-queue (e.g. a supply not yet
        # propagated across the queue). Re-submitting the same redeem a few blocks
        # later succeeds. Context-scoped so a bare Panic(0x11) from anywhere else
        # stays UNKNOWN (Codex over-broad-retry constraint).
        #
        # Keyed on "metamorpho" — the protocol SLUG a metamorpho vault position
        # actually carries (confirmed on the E2E real-fork run), NOT the connector
        # folder name "morpho_vault" (no position emits that as its protocol, and
        # it would trip the chain/protocol coupling ratchet as a framework-side
        # connector-name literal). Connector-owned vetted transient signatures —
        # so other vault connectors contribute their own without a framework
        # literal — are the proper generalization, tracked as VIB-5581.
        intent_types=frozenset({"vault_redeem"}),
        protocols=frozenset({"metamorpho"}),
        signature=_is_arithmetic_panic,
        why=(
            "MetaMorpho withdraw-queue transient arithmetic underflow "
            "(Panic 0x11) that clears within blocks once the queue settles."
        ),
    ),
)


def _norm(value: str | None) -> str | None:
    """Lower-case + strip a context field, or ``None`` if absent/blank.

    Returns ``None`` for a non-``str`` or empty/whitespace value so a rule
    whose ``intent_types`` / ``protocols`` set can never contain ``None`` simply
    fails to match — never raises inside the classification path.
    """
    if not isinstance(value, str):
        return None
    stripped = value.strip().lower()
    return stripped or None


def classify_revert_transience(
    error_text: str | None,
    *,
    intent_type: str | None = None,
    protocol: str | None = None,
) -> Transience:
    """Classify a teardown revert as TRANSIENT / PERMANENT / UNKNOWN.

    Returns :attr:`Transience.TRANSIENT` only when ``error_text`` +
    ``intent_type`` + ``protocol`` all match a vetted row in
    :data:`_TRANSIENT_RULES` (context-scoped by design — see module docstring's
    safety constraint). No PERMANENT rules are seeded yet. Everything else,
    including a bare arithmetic panic with the wrong intent/protocol, is
    :attr:`Transience.UNKNOWN`.

    Defensive: this sits on the teardown failure surface and is fed whatever the
    orchestrator produced, so a non-``str`` / empty / ``None`` ``error_text``
    (or ``intent_type`` / ``protocol``) degrades to
    :attr:`Transience.UNKNOWN` rather than raising inside the retry-decision
    path. Matching on ``intent_type`` / ``protocol`` is case-insensitive.
    """
    if not isinstance(error_text, str) or not error_text:
        return Transience.UNKNOWN

    it = _norm(intent_type)
    proto = _norm(protocol)
    if it is None or proto is None:
        return Transience.UNKNOWN

    for rule in _TRANSIENT_RULES:
        if it in rule.intent_types and proto in rule.protocols and rule.signature(error_text):
            return Transience.TRANSIENT

    return Transience.UNKNOWN
