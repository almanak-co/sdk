"""Teardown failure classification — VIB-4532 / VIB-4664 / VIB-4258.

Maps a teardown execution/simulation failure string to a *disposition* that
tells the :class:`EscalatingSlippageManager` how to react:

* ``ESCALATE`` — genuine slippage shortfall; walk the slippage ladder
  (current behaviour, including the 5%/8% operator-approval gates).
* ``NON_RETRYABLE`` — deterministic revert that no slippage level can fix
  (insufficient balance/collateral, contract-arg validation, ERC-721
  not-approved, gas underestimate, protocol-permanent). Short-circuit and
  surface; teardown moves on to the next risk-reducing intent.
* ``RETRY_SAME_LEVEL`` — transient transport/RPC failure (Anvil ``Fork Error``,
  DNS, timeout, nonce, rate-limit). Retry at the SAME slippage level; never
  escalate, never ask the operator to approve loss for a network blip.

The shared keyword core lives in
:func:`almanak.framework.intents.error_keywords.categorize_error` (VIB-2866 /
VIB-1215). This module layers the teardown-specific disposition on top and adds
the transport phrasings the intent classifier does not carry, so the two never
duplicate keyword lists.

Note: ``estimated loss == $0.00`` is a useful secondary tell that a failure was
not slippage-related (VIB-4258), but classification stays string-only here so it
is deterministic and unit-testable; the loss heuristic is not a branch input.
"""

from __future__ import annotations

from enum import StrEnum

from almanak.framework.intents.error_keywords import categorize_error


class RevertClass(StrEnum):
    """Coarse class of a teardown execution failure."""

    SLIPPAGE_MINIMUM_VIOLATED = "slippage_minimum_violated"
    INSUFFICIENT_BALANCE = "insufficient_balance"
    CONTRACT_ARG_INVALID = "contract_arg_invalid"
    LIQUIDITY_UNAVAILABLE = "liquidity_unavailable"
    GAS_UNDERESTIMATE = "gas_underestimate"
    TRANSPORT_TRANSIENT = "transport_transient"
    UNKNOWN = "unknown"


class Disposition(StrEnum):
    """How the slippage manager should react to a failure."""

    ESCALATE = "escalate"
    NON_RETRYABLE = "non_retryable"
    RETRY_SAME_LEVEL = "retry_same_level"


# Transport / RPC phrasings the intent classifier does not carry verbatim.
# VIB-4258: Anvil lazy-fetch surfaces alloy/reqwest wording. Teardown retries
# these at the same slippage level rather than escalating — bumping slippage can
# never make a DNS failure resolve.
# Lending-vault cash shortage (VIB-5801). The vault cannot settle a redeem right now
# because the underlying is lent out — distinct from the CALLER being short (which is
# INSUFFICIENT_BALANCE / NON_RETRYABLE). Selector forms are included because a bare
# revert often carries only the 4-byte selector, not a decoded name.
_VAULT_CASH_SHORTAGE_KEYWORDS = (
    "e_insufficientcash",  # EVK / euler_v2
    "0xf077d877",  # keccak("E_InsufficientCash()")[:4]
    "notenoughliquidity",  # Silo V2
    "0x4323a555",  # keccak("NotEnoughLiquidity()")[:4]
)

_TRANSPORT_KEYWORDS = (
    "fork error",
    "transport",
    "dns error",
    "failed to lookup address",
    "host unreachable",
    "connection reset",
    "broken pipe",
    "eof",
)

# Deterministic contract-argument / approval / allowance reverts that repeat
# byte-identical at every slippage level. VIB-4532 (Morpho ``INCONSISTENT_INPUT``),
# the ERC-721 "Not approved" close-path revert observed on the lp_dual teardown,
# and the ERC-20 allowance/transfer family (``ERC20: transfer amount exceeds
# allowance``, ``insufficient allowance``, Uniswap ``STF`` / ``TRANSFER_FROM_FAILED``)
# — none of which a higher slippage tolerance can fix.
_CONTRACT_ARG_KEYWORDS = (
    "inconsistent input",
    "inconsistent inputs",
    "not approved",
    "invalidparam",
    "invalid param",
    "allowance",
    "transfer amount exceeds",
    "transferfrom failed",
    "transfer_from_failed",
)

# Slippage-minimum reverts — the ONLY class the escalation ladder can fix.
_SLIPPAGE_KEYWORDS = (
    "slippage",
    "insufficientoutputamount",
    "insufficient output amount",
    "too_little_received",
    "too little received",
    "min_amount_out",
    "minimum output",
    "price impact",
)

# Map the shared intent-classifier category -> teardown (RevertClass, Disposition).
# "REVERT" is intentionally absent: a bare, unclassified revert falls through to
# UNKNOWN/ESCALATE so historical behaviour is preserved for ambiguous reverts.
_CATEGORY_DISPOSITION: dict[str, tuple[RevertClass, Disposition]] = {
    "INSUFFICIENT_FUNDS": (RevertClass.INSUFFICIENT_BALANCE, Disposition.NON_RETRYABLE),
    "COMPILATION_PERMANENT": (RevertClass.LIQUIDITY_UNAVAILABLE, Disposition.NON_RETRYABLE),
    "GAS_ERROR": (RevertClass.GAS_UNDERESTIMATE, Disposition.NON_RETRYABLE),
    "SLIPPAGE": (RevertClass.SLIPPAGE_MINIMUM_VIOLATED, Disposition.ESCALATE),
    "TIMEOUT": (RevertClass.TRANSPORT_TRANSIENT, Disposition.RETRY_SAME_LEVEL),
    "NETWORK_ERROR": (RevertClass.TRANSPORT_TRANSIENT, Disposition.RETRY_SAME_LEVEL),
    "RATE_LIMIT": (RevertClass.TRANSPORT_TRANSIENT, Disposition.RETRY_SAME_LEVEL),
    "NONCE_ERROR": (RevertClass.TRANSPORT_TRANSIENT, Disposition.RETRY_SAME_LEVEL),
}


def classify_teardown_failure(error_message: str | None) -> tuple[RevertClass, Disposition]:
    """Classify a teardown execution/simulation failure string.

    Returns ``(RevertClass, Disposition)``. Check order is significant:
    teardown-specific slippage, transport, and deterministic-revert phrasings are
    matched BEFORE delegating to the shared intent classifier, because teardown's
    correct reaction to a transport blip (retry same level) differs from the
    intent state machine's (fail fast). An empty / ``None`` error preserves the
    historical escalate behaviour.
    """
    if not error_message:
        return RevertClass.UNKNOWN, Disposition.ESCALATE

    e = error_message.lower()

    # 1. Genuine slippage FIRST — a message can carry both "slippage" and a
    #    generic "revert"; the slippage signal wins and escalates.
    if any(k in e for k in _SLIPPAGE_KEYWORDS):
        return RevertClass.SLIPPAGE_MINIMUM_VIOLATED, Disposition.ESCALATE

    # 2. Transport / RPC transient (VIB-4258) — retry same level, never escalate.
    if any(k in e for k in _TRANSPORT_KEYWORDS):
        return RevertClass.TRANSPORT_TRANSIENT, Disposition.RETRY_SAME_LEVEL

    # 2b. Lending vault has no CASH to settle right now (VIB-5801) — EVK's
    #     ``E_InsufficientCash`` (``0xf077d877``, euler_v2) and Silo V2's
    #     ``NotEnoughLiquidity`` (``0x4323a555``). These are NOT the caller's balance
    #     being short (step 3) — the caller owns the shares; the vault has lent the
    #     underlying out. Liquidity returns as borrowers repay, so retry at the SAME
    #     level: escalating slippage is meaningless for a cash shortage and merely
    #     re-broadcasts a reverting redeem at each rung. Must precede step 3, whose
    #     "insufficient" test would otherwise be the nearest match, and step 6, which
    #     would fall through to UNKNOWN/ESCALATE.
    if any(k in e for k in _VAULT_CASH_SHORTAGE_KEYWORDS):
        return RevertClass.LIQUIDITY_UNAVAILABLE, Disposition.RETRY_SAME_LEVEL

    # 3. Insufficient balance / collateral (VIB-4664) — deterministic pre-flight.
    #    Two real shapes: the prefixed orchestrator message ("Pre-flight balance
    #    check failed: Insufficient <SYMBOL>: have X, need Y") and the
    #    InsufficientFundsError form ("Insufficient <SYMBOL>: need Y, have X
    #    (deficit: Z)") — the latter carries the token symbol, not the literal
    #    word "balance"/"funds", so match the structured have/need signature too.
    if (
        "pre-flight balance check failed" in e
        or ("insufficient" in e and any(k in e for k in ("balance", "funds", "collateral")))
        or ("insufficient" in e and "have" in e and "need" in e)
    ):
        return RevertClass.INSUFFICIENT_BALANCE, Disposition.NON_RETRYABLE

    # 4. Contract-arg / approval reverts (VIB-4532) — deterministic.
    if any(k in e for k in _CONTRACT_ARG_KEYWORDS):
        return RevertClass.CONTRACT_ARG_INVALID, Disposition.NON_RETRYABLE

    # 5. Gas underestimate — classify only; the gas*1.5 retry is VIB-4533's job.
    if "out of gas" in e or ("gas" in e and "estimat" in e):
        return RevertClass.GAS_UNDERESTIMATE, Disposition.NON_RETRYABLE

    # 6. Delegate to the shared intent classifier for the rich permanent set
    #    (enso selectors, orca/drift, market/pool-not-found, comptroller
    #    insufficient_liquidity, host-unreachable, ...).
    category = categorize_error(error_message)
    if category in _CATEGORY_DISPOSITION:
        return _CATEGORY_DISPOSITION[category]

    # 7. Unknown / bare REVERT -> preserve historical escalate behaviour.
    return RevertClass.UNKNOWN, Disposition.ESCALATE
