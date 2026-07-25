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

logger = logging.getLogger(__name__)


def realign_token_pair_by_address(
    token0: str,
    token1: str,
    chain: str,
) -> tuple[str, str]:
    """Return ``(token0, token1)`` reordered so token0 is the lower on-chain address.

    Fail-open: on any resolution / parse failure returns the inputs unchanged
    (no worse than pre-fix label order). Never raises, never hits the network
    (``skip_gateway=True``).
    """
    if not token0 or not token1 or not chain:
        return token0, token1

    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        ti0 = resolver.resolve(token0, chain=chain, skip_gateway=True, log_errors=False)
        ti1 = resolver.resolve(token1, chain=chain, skip_gateway=True, log_errors=False)
    except Exception:  # noqa: BLE001 — money path: fail-open, never raise/block
        logger.warning(
            "token pair address-sort: resolver failed for (%s, %s) on %s; keeping input order",
            token0,
            token1,
            chain,
        )
        return token0, token1

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
