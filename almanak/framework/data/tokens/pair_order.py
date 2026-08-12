"""Address-sorted token-pair realignment for V3-family on-chain order.

Uniswap V3 / Solidly / similar pools order ``token0()`` / ``token1()`` by
**numeric address** (lower address first). Receipt parsers emit
``amount0`` / ``amount1`` in that order. Strategy config and intent pool
labels often use a human-preferred order (e.g. ``\"WETH/USDC/500\"``) that
can disagree with on-chain order.

When symbols stay in label order while amounts stay in chain order, both
decimals and prices mis-pair — the VIB-5851 / VIB-5983 phantom-basis class
(~$1bn on a few-dollar LP).

This module is the **shared pure sort**: resolve each symbol offline via
the static token registry and swap so ``token0`` is the lower address.
Callers gate *when* to apply it (typed raw amounts, not N-coin, not
declared money legs, etc.).
"""

from __future__ import annotations

import logging

# Module-level import (VIB-6167): a function-local import is a deferred
# module-body execution, so an import-time fault would land one frame above the
# seam's guards — on an accounting write path. Paying it at load moves that to
# boot. No cycle: ``best_effort`` imports the resolver, which imports neither
# this module nor ``best_effort``.
from almanak.framework.data.tokens.best_effort import resolve_token_best_effort

logger = logging.getLogger(__name__)


def realign_token_pair_by_address(
    token0: str,
    token1: str,
    chain: str,
) -> tuple[str, str]:
    """Return ``(token0, token1)`` reordered so token0 is the lower on-chain address.

    Fail-open: on any *resolution* / parse failure returns the inputs unchanged
    (no worse than pre-fix label order) and never hits the network
    (``skip_gateway=True``).

    VIB-6100: "fail-open" stays scoped to genuine token-resolution failures in
    the LOG. A defect in the seam — a ``TypeError`` from a signature mismatch, an
    ``AttributeError`` from a malformed resolver — still keeps the input order
    (the seam is total and never raises), but it is recorded at ERROR with a
    traceback and reported to the defect observer, so it can no longer pass for
    a legitimate resolver miss. Under test, that observer fails the run.
    """
    if not token0 or not token1 or not chain:
        return token0, token1

    # VIB-6100 — shared seam. It pins ``chain`` as a keyword and
    # ``skip_gateway=True``, which is what removed the original trap: this site
    # passed ``chain`` by keyword while the ledger passed it positionally, so one
    # hand-rolled double worked here and raised ``TypeError`` there.
    ti0 = resolve_token_best_effort(token0, chain, context="token pair address-sort")
    ti1 = resolve_token_best_effort(token1, chain, context="token pair address-sort")

    addr0 = getattr(ti0, "address", None) if ti0 is not None else None
    addr1 = getattr(ti1, "address", None) if ti1 is not None else None
    if not addr0 or not addr1:
        logger.warning(
            "token pair address-sort: could not resolve addresses for (%s, %s) on %s; keeping input order",
            token0,
            token1,
            chain,
        )
        return token0, token1

    try:
        int0 = int(str(addr0), 16)
        int1 = int(str(addr1), 16)
    except (TypeError, ValueError):
        return token0, token1
    if int0 == int1:
        return token0, token1
    # On-chain token0() is the numerically-lower address.
    if int0 <= int1:
        return token0, token1
    return token1, token0


def place_token_pair_by_observed_identity(
    token0: str,
    token1: str,
    chain: str,
    currency0: str | None,
    currency1: str | None,
) -> tuple[str, str] | None:
    """Place the label symbols into the slots their OBSERVED identities name.

    The address sort above recovers slot order by assuming the venue orders its
    coins by address. That assumption is true for the V3 family and Solidly, and
    FALSE for TraderJoe Liquidity Book (``tokenX``/``tokenY`` are fixed at pool
    creation) and for Curve (``coins(i)`` is pool-index order). Applying it to
    those venues transposes the row — VIB-6383's ``$322,107,799,472.28`` on a
    ``$2.47`` position.

    This function needs no such assumption. When the receipt observed which
    token sits in a slot, that IS the answer: resolve both label symbols to
    addresses offline and put each symbol in the slot whose observed currency
    matches its address. Sorting never enters into it, so the result is correct
    for address-sorted and non-address-sorted venues alike — which is what
    VIB-6471 means by "the fix must be family-agnostic".

    A slot with no observed currency is filled with whichever label symbol the
    matched slot did not claim. That is sound precisely when the unobserved slot
    moved no money (:func:`..._strategy_base.lp_leg_identity.identity_is_complete`
    is the caller's guard): a zero scales to zero under either token's decimals,
    so the leftover assignment cannot mis-scale anything.

    Returns ``None`` — never a guess — when the placement cannot be proven:
    no observation at all, resolution failure, or an observed currency matching
    neither label symbol. **Fail closed**; the caller decides the fallback.
    """
    if not token0 or not token1 or not chain:
        return None
    if not currency0 and not currency1:
        return None

    # VIB-6100 — routed through the shared seam. This site arrived after the
    # original migration (VIB-6471) carrying the verbatim pre-fix shape: a bare
    # ``except Exception`` around ``resolver.resolve``, with the same flags the
    # seam pins. It fails CLOSED rather than open, which bounds the damage, but
    # the swallow is identical — a broken double read as "resolver failed" and
    # this function returned ``None``, so a test asserting the fallback stayed
    # green while the placement logic it names was never entered.
    #
    # The seam is total, so no ``try`` is needed here at all.
    ti0 = resolve_token_best_effort(token0, chain, context="token pair identity placement")
    ti1 = resolve_token_best_effort(token1, chain, context="token pair identity placement")

    addr0 = getattr(ti0, "address", None) if ti0 is not None else None
    addr1 = getattr(ti1, "address", None) if ti1 is not None else None
    if not addr0 or not addr1:
        return None

    by_address = {str(addr0).strip().lower(): token0, str(addr1).strip().lower(): token1}
    if len(by_address) != 2:  # both labels resolved to one address — unusable
        return None

    slots: list[str | None] = [None, None]
    for index, currency in ((0, currency0), (1, currency1)):
        if not currency:
            continue
        matched = by_address.get(str(currency).strip().lower())
        if matched is None:
            # An observed identity that is not in this pair at all. The receipt
            # and the label disagree about which pool this is; guessing here is
            # how a transposed row gets written with confidence.
            logger.warning(
                "token pair identity placement: observed currency %s on %s is neither %s nor %s; refusing to place",
                currency,
                chain,
                token0,
                token1,
            )
            return None
        slots[index] = matched

    if slots[0] is not None and slots[1] is not None:
        if slots[0] == slots[1]:  # both slots claimed the same symbol
            return None
        return slots[0], slots[1]

    # Exactly one slot observed — the other takes the symbol left over.
    if slots[0] is not None:
        return slots[0], (token1 if slots[0] == token0 else token0)
    assert slots[1] is not None  # noqa: S101 — one of the two is set by construction
    return (token1 if slots[1] == token0 else token0), slots[1]
