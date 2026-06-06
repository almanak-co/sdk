"""Strategy-side shared infrastructure for connector pool-existence validation.

Each DEX connector owns the *protocol-specific* half of pool validation — its
factory contract-kind, its ``getPool`` selector, its calldata encoder, its
result decoder, and its ``validate(...) -> PoolValidationResult`` entry point.
What every connector shares — the result vocabulary
(:class:`PoolValidationResult` / :class:`PoolValidationReason`), the single-word
address decoder (:func:`decode_address`), and the gateway-routed
:func:`eth_call` — lives here so the protocol-leaf validators import from a
strategy-side foundation rather than from ``almanak/framework/**``.

Gateway-boundary note: :func:`eth_call` routes through a connected
``GatewayClient`` when one is available. The direct JSON-RPC branch is a
local-only fallback used solely when no connected gateway client is supplied
(tests, ad-hoc local tooling); it carries the ``vib-2986-exempt`` marker the
sidecar regression honours. No new egress path is introduced by this module —
the behaviour is byte-for-byte the pre-existing ``framework/intents``
``_eth_call`` / ``_decode_address`` implementation, relocated strategy-side.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

__all__ = [
    "ZERO_ADDRESS",
    "PoolValidationReason",
    "PoolValidationResult",
    "decode_address",
    "eth_call",
]

ZERO_ADDRESS = "0x" + "0" * 40


class PoolValidationReason(StrEnum):
    """Typed reasons for a PoolValidationResult outcome.

    The compiler uses this enum to decide whether to fail compilation
    (fail-closed) or warn-and-proceed (fail-open, impossible-to-verify cases).
    """

    # Positive outcome — pool confirmed on-chain.
    CONFIRMED = "CONFIRMED"

    # Negative outcomes — callers MUST fail closed.
    NOT_FOUND = "NOT_FOUND"  # Factory returned zero address / pool is absent
    RPC_FAILED = "RPC_FAILED"  # RPC call was attempted but errored / bad response

    # Impossible-to-verify outcomes — callers may warn and proceed.
    RPC_UNAVAILABLE = "RPC_UNAVAILABLE"  # No RPC URL configured
    FACTORY_MISSING = "FACTORY_MISSING"  # No factory entry for chain in registry
    PROTOCOL_UNKNOWN = "PROTOCOL_UNKNOWN"  # Protocol not recognised by validator
    NOT_CONFIGURED = "NOT_CONFIGURED"  # Other misconfiguration (e.g. unexpected response shape)


@dataclass
class PoolValidationResult:
    """Result of a pool existence check.

    Attributes:
        exists: True if pool exists, False if confirmed missing, None if unknown.
        reason: Typed outcome category used by callers to decide fail-closed vs warn-and-proceed.
        pool_address: Pool address if found, None otherwise.
        warning: Set when validation could not be performed (exists=None).
        error: Set when validation confirmed the pool is absent/broken (exists=False).
    """

    exists: bool | None
    reason: PoolValidationReason
    pool_address: str | None = None
    warning: str | None = None
    error: str | None = None


def eth_call(
    rpc_url: str,
    to: str,
    data: str,
    timeout: float = 5.0,
    *,
    chain: str | None = None,
    gateway_client: GatewayClient | None = None,
    raise_errors: bool = False,
) -> bytes | None:
    """Perform an eth_call via gateway when available, otherwise direct JSON-RPC.

    Args:
        rpc_url: Direct RPC URL fallback when no connected gateway is available.
        to: Contract address to call.
        data: Encoded calldata.
        timeout: Direct JSON-RPC request timeout in seconds.
        chain: Chain name used for gateway routing.
        gateway_client: Optional connected gateway client for routed calls.
        raise_errors: When True, raise ``ValueError`` on gateway or direct-RPC
            failures. When False, the default, suppress those failures and
            return ``None`` so callers can treat the read as unavailable.

    Returns:
        Raw result bytes, or ``None`` when the call returns no data or fails
        while ``raise_errors`` is False.
    """
    if gateway_client is not None and getattr(gateway_client, "is_connected", False) and chain:
        try:
            result = gateway_client.eth_call(chain=chain, to=to, data=data)
            if not result or result == "0x":
                return None
            return bytes.fromhex(result.removeprefix("0x"))
        except Exception as exc:
            if raise_errors:
                raise ValueError(f"Gateway eth_call failed for {to} on {chain}: {exc}") from exc
            return None

    if not rpc_url:
        return None

    import requests

    try:
        resp = requests.post(  # vib-2986-exempt: local-only fallback when no connected gateway client is available
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": to, "data": data}, "latest"],
                "id": 1,
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
        rpc_error = payload.get("error")
        if rpc_error is not None:
            raise ValueError(f"RPC eth_call error for {to}: {rpc_error}")
        result = payload.get("result")
        if not result or result == "0x":
            return None
        if not isinstance(result, str) or not result.startswith("0x"):
            raise ValueError(f"RPC eth_call result for {to} must be 0x hex, got {result!r}")
        return bytes.fromhex(result[2:])
    except Exception as exc:
        if raise_errors:
            raise ValueError(f"RPC eth_call failed for {to}: {exc}") from exc
        return None


def decode_address(data: bytes) -> str:
    """Decode a single address return value (rightmost 20 bytes of 32-byte word)."""
    if len(data) < 32:
        return ZERO_ADDRESS
    return "0x" + data[12:32].hex()
