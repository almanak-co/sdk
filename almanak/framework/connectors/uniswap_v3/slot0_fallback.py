"""VIB-3893 — slot0() fallback for ``LPOpenData.current_tick``.

The Uniswap V3 receipt parser populates ``current_tick`` from a Swap event
emitted by the same pool inside the LP_OPEN receipt (the canonical swap-
then-mint atomic path). When a strategy splits the swap and the mint into
two cycles — as ``AccountingQuantLPStrategy`` does (iter 1: SWAP, iter 2:
LP_OPEN) — the LP_OPEN receipt contains only the NPM.mint, with no Swap
event on the pool. In that case the receipt parser leaves ``current_tick=
None`` and ``position_events.in_range`` stays NULL.

This helper closes the gap by issuing a single ``slot0()`` ``eth_call`` via
the gateway after the receipt has been parsed but before the position
event is written. A new :class:`LPOpenData` is returned with
``current_tick`` populated. If the call fails or the inputs are not LP-
shaped, the original payload is returned unchanged so the writer keeps
its degraded-but-correct fallback.
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any

from ...execution.extracted_data import LPOpenData

logger = logging.getLogger(__name__)

SLOT0_SELECTOR = "0x3850c7bd"


def _decode_slot0_tick(hex_result: str) -> int | None:
    """Decode the ``tick`` field (int24) from a slot0() return blob.

    slot0() returns:
      [0]  uint160 sqrtPriceX96   (offset 0)
      [1]  int24   tick            (offset 32)
      [2]  uint16  observationIndex
      ...

    All values are right-padded to 32 bytes regardless of declared width.
    The signed int24 needs to be sign-extended from the low 24 bits — but
    Solidity actually right-aligns it as a full-width two's-complement
    int256, so reading the full 256-bit word as signed gives the value
    directly.
    """
    if not hex_result:
        return None
    blob = hex_result.removeprefix("0x")
    if len(blob) < 128:  # need at least 2 words
        return None
    tick_word = blob[64:128]
    raw = int(tick_word, 16)
    # Two's-complement sign extension from 256-bit
    if raw >= (1 << 255):
        raw -= 1 << 256
    return raw


def fetch_slot0_tick(
    gateway_client: Any,
    chain: str,
    pool_address: str,
) -> int | None:
    """Fetch ``slot0().tick`` from the pool via gateway eth_call.

    Returns None on any error so the caller can fall back gracefully.
    """
    if not (gateway_client and chain and pool_address):
        return None
    try:
        result = gateway_client.eth_call(chain, pool_address, SLOT0_SELECTOR)
    except Exception:
        logger.debug("slot0 eth_call failed", exc_info=True)
        return None
    # CodeRabbit 2026-05-04: ``_decode_slot0_tick`` can raise on a bytes
    # response (no ``.removeprefix``) or non-hex content (``int(..., 16)``
    # ValueError). The helper's contract is "swallow errors and return
    # None"; let the decode path inherit that contract too rather than
    # leaking exceptions into the caller's enrichment loop.
    try:
        if isinstance(result, bytes | bytearray):
            result = "0x" + bytes(result).hex()
        return _decode_slot0_tick(str(result) if result is not None else "")
    except Exception:
        logger.debug("slot0 decode failed", exc_info=True)
        return None


def enrich_lp_open_with_slot0(
    lp_open: LPOpenData | None,
    *,
    gateway_client: Any,
    chain: str,
) -> LPOpenData | None:
    """Return a new ``LPOpenData`` with ``current_tick`` populated.

    No-ops:
      * ``lp_open`` is None or not an ``LPOpenData``
      * ``current_tick`` is already populated (receipt-parser path won)
      * ``pool_address`` is empty (parser couldn't identify the pool)
      * gateway eth_call fails or returns garbage

    Errors are swallowed and the original input is returned unchanged —
    ``in_range`` derivation downstream will fall back to None, exactly as
    it did pre-fix. This is fail-open by design: a missing tick must not
    block ledger / position-event writes.
    """
    if not isinstance(lp_open, LPOpenData):
        return lp_open
    if lp_open.current_tick is not None:
        return lp_open
    if not lp_open.pool_address:
        return lp_open
    tick = fetch_slot0_tick(gateway_client, chain, lp_open.pool_address)
    if tick is None:
        return lp_open
    logger.info(
        "filled LP_OPEN current_tick from slot0() fallback (chain=%s pool=%s tick=%d)",
        chain,
        lp_open.pool_address,
        tick,
    )
    return dataclasses.replace(lp_open, current_tick=tick)


def enrich_lp_close_with_slot0(
    lp_close: Any,
    *,
    gateway_client: Any,
    chain: str,
) -> Any:
    """VIB-3940 — fill ``LPCloseData.current_tick`` from a slot0() RPC.

    Mirror of :func:`enrich_lp_open_with_slot0`. The Uniswap V3 close path
    captures ``current_tick`` from a Swap event in the same receipt when
    present (post-decreaseLiquidity automatic Swap is rare; multicall paths
    that include a router swap before/after the close emit one). When no
    Swap is in the receipt — the canonical pure-burn close — the parser
    leaves ``current_tick=None`` and the LP_CLOSE accounting event would
    inherit it as null, breaking lane symmetry vs. LP_OPEN (VIB-3893).

    No-ops:
      * ``lp_close`` is None or not an ``LPCloseData``
      * ``current_tick`` is already populated (receipt-parser path won)
      * ``pool_address`` is empty (parser couldn't identify the pool)
      * gateway eth_call fails or returns garbage

    Errors are swallowed and the original input is returned unchanged —
    same fail-open contract as the LP_OPEN sibling.
    """
    from ...execution.extracted_data import LPCloseData

    if not isinstance(lp_close, LPCloseData):
        return lp_close
    if lp_close.current_tick is not None:
        return lp_close
    if not lp_close.pool_address:
        return lp_close
    tick = fetch_slot0_tick(gateway_client, chain, lp_close.pool_address)
    if tick is None:
        return lp_close
    logger.info(
        "filled LP_CLOSE current_tick from slot0() fallback (chain=%s pool=%s tick=%d)",
        chain,
        lp_close.pool_address,
        tick,
    )
    return dataclasses.replace(lp_close, current_tick=tick)


__all__ = [
    "SLOT0_SELECTOR",
    "enrich_lp_close_with_slot0",
    "enrich_lp_open_with_slot0",
    "fetch_slot0_tick",
]
