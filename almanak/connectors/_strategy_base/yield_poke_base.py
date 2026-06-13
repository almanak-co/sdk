"""Strategy-side shared infrastructure for Anvil-fork yield-poke functions.

Each lending connector that supports on-fork interest accrual publishes a
``PokeFunction`` in its ``backtest_poke`` module and declares it on
``CONNECTOR.yield_poke``. This module owns the shared types and low-level
transport helpers those functions use.

Types exported here are re-exported by
``almanak.framework.backtesting.paper.yield_poker`` for backward compatibility
with consumers that imported them from there (``paper/engine.py``).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

__all__ = [
    "PokeFunction",
    "PokeResult",
    "_pad_address",
    "_pad_uint256",
    "_send_tx",
    "_verify_receipt_status",
]


@dataclass
class PokeResult:
    """Result of a protocol poke transaction."""

    protocol: str
    success: bool
    error: str | None = None
    tx_hash: str | None = None


PokeFunction = Callable[[str, str], Coroutine[Any, Any, PokeResult]]


def _pad_address(addr: str) -> str:
    """Left-pad an address to 32 bytes for ABI encoding."""
    hex_part = addr[2:] if addr.startswith("0x") else None
    if hex_part is None or len(hex_part) != 40 or any(c not in "0123456789abcdefABCDEF" for c in hex_part):
        raise ValueError(f"Address must be a 0x-prefixed 40-hex-char string, got {addr!r}")
    return hex_part.lower().zfill(64)


def _pad_uint256(value: int) -> str:
    """Encode a uint256 as 32-byte hex."""
    if not isinstance(value, int) or value < 0 or value >= 2**256:
        raise ValueError(f"uint256 must be a non-negative int below 2**256, got {value!r}")
    return hex(value)[2:].zfill(64)


# Receipt polling bounds: with Anvil's default automine (the paper harness
# default, block_time=None) the receipt is available on the first poll, so
# the window size never affects a normal run. The loop only matters under
# --block-time interval mining; the ~5s window comfortably exceeds any block
# time used here (tests cap at 2s) without sitting on that boundary. A genuinely
# stuck poke is non-fatal and is also bounded by the 30s per-call RPC timeout.
_RECEIPT_POLL_ATTEMPTS = 25
_RECEIPT_POLL_INTERVAL_SECONDS = 0.2


async def _rpc(session: Any, rpc_url: str, method: str, params: list[Any]) -> Any:
    """Issue a single JSON-RPC call and return its ``result`` (None if absent)."""
    payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    async with session.post(rpc_url, json=payload) as resp:
        resp.raise_for_status()
        result = await resp.json()
    if not isinstance(result, dict):
        raise RuntimeError(f"invalid JSON-RPC response: {result!r}")
    if "error" in result:
        raise RuntimeError(result["error"].get("message", str(result["error"])))
    return result.get("result")


async def _verify_receipt_status(session: Any, rpc_url: str, tx_hash: str) -> None:
    """Poll for the transaction receipt and raise unless its status is 0x1.

    Anvil mines reverting transactions (receipt status 0x0) instead of
    rejecting them, so a returned tx hash alone does not prove execution —
    both the Aave V3 ``supply(0)`` poke and the old Compound V3 ``scale()``
    poke "succeeded" for months while doing nothing (VIB-2630 spike).
    """
    import asyncio

    for _ in range(_RECEIPT_POLL_ATTEMPTS):
        receipt = await _rpc(session, rpc_url, "eth_getTransactionReceipt", [tx_hash])
        if receipt is not None:
            # Anvil returns the status as a hex string ("0x1"), but some
            # clients / mocks return a bare int — accept both.
            status = receipt.get("status")
            try:
                status_val = int(status, 16) if isinstance(status, str) else int(status)
            except (TypeError, ValueError):
                status_val = None
            if status_val != 1:
                raise RuntimeError(f"poke transaction {tx_hash} reverted (receipt status {status!r})")
            return
        await asyncio.sleep(_RECEIPT_POLL_INTERVAL_SECONDS)
    raise RuntimeError(
        f"poke transaction {tx_hash} has no receipt after "
        f"{_RECEIPT_POLL_ATTEMPTS * _RECEIPT_POLL_INTERVAL_SECONDS:.1f}s; cannot confirm execution"
    )


async def _send_tx(rpc_url: str, from_addr: str, to: str, data: str) -> str | None:
    """Send a transaction via eth_sendTransaction on Anvil (auto-impersonate).

    Raises RuntimeError when the transaction reverted or its receipt never
    appeared, so callers' ``PokeResult.success`` reflects actual execution.
    """
    import aiohttp

    # Bounded so an unresponsive fork RPC cannot hang the paper session;
    # generous enough for cold upstream-state fetches on freshly forked Anvil.
    timeout = aiohttp.ClientTimeout(total=30.0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tx_hash = await _rpc(
            session,
            rpc_url,
            "eth_sendTransaction",
            [{"from": from_addr, "to": to, "data": data, "gas": "0x500000"}],
        )
        if tx_hash is None:
            return None
        await _verify_receipt_status(session, rpc_url, tx_hash)
        return tx_hash
