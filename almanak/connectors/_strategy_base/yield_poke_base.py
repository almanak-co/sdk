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


async def _send_tx(rpc_url: str, from_addr: str, to: str, data: str) -> str | None:
    """Send a transaction via eth_sendTransaction on Anvil (auto-impersonate)."""
    import aiohttp

    payload = {
        "jsonrpc": "2.0",
        "method": "eth_sendTransaction",
        "params": [{"from": from_addr, "to": to, "data": data, "gas": "0x500000"}],
        "id": 1,
    }
    # Bounded so an unresponsive fork RPC cannot hang the paper session;
    # generous enough for cold upstream-state fetches on freshly forked Anvil.
    timeout = aiohttp.ClientTimeout(total=30.0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(rpc_url, json=payload) as resp:
            result = await resp.json()
            if "result" in result:
                return result["result"]
            if "error" in result:
                raise RuntimeError(result["error"].get("message", str(result["error"])))
            return None
