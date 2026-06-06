"""Shared connector RPC helpers.

Protocol connectors own ABI selectors, calldata encoding, contract addresses,
and result decoding. Transport belongs here so swap quoting, pool validation,
and future connector reads all share the same gateway-first boundary.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urlparse

from almanak.connectors._strategy_base.pool_validation_base import eth_call as _eth_call

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

__all__ = ["decode_uint256", "eth_call", "eth_call_hex", "eth_call_uint256"]


_SUPPORTED_DIRECT_RPC_SCHEMES: frozenset[str] = frozenset({"http", "https"})


def _gateway_connected(gateway_client: GatewayClient | None, chain: str | None) -> bool:
    return bool(gateway_client is not None and getattr(gateway_client, "is_connected", False) and chain)


def _validate_direct_rpc_url(rpc_url: str | None) -> None:
    if not rpc_url:
        return
    scheme = urlparse(rpc_url).scheme.lower()
    if scheme not in _SUPPORTED_DIRECT_RPC_SCHEMES:
        raise ValueError(f"Unsupported RPC URL scheme {scheme!r}; direct connector RPC calls require http or https")


def eth_call(
    *,
    chain: str | None,
    to: str,
    data: str,
    rpc_url: str | None = None,
    gateway_client: GatewayClient | None = None,
    timeout: float = 5.0,
) -> bytes | None:
    """Perform a gateway-first eth_call and return raw bytes."""
    if not _gateway_connected(gateway_client, chain):
        _validate_direct_rpc_url(rpc_url)
    return _eth_call(
        rpc_url or "",
        to,
        data,
        timeout=timeout,
        chain=chain,
        gateway_client=gateway_client,
        raise_errors=True,
    )


def eth_call_hex(
    *,
    chain: str | None,
    to: str,
    data: str,
    rpc_url: str | None = None,
    gateway_client: GatewayClient | None = None,
    timeout: float = 5.0,
) -> str | None:
    """Perform eth_call and return a 0x-prefixed hex string."""
    raw = eth_call(
        chain=chain,
        to=to,
        data=data,
        rpc_url=rpc_url,
        gateway_client=gateway_client,
        timeout=timeout,
    )
    if raw is None:
        return None
    return "0x" + raw.hex()


def decode_uint256(data: bytes) -> int:
    """Decode a single uint256 word."""
    if len(data) < 32:
        raise ValueError(f"uint256 response must be at least 32 bytes, got {len(data)}")
    return int.from_bytes(data[:32], "big")


def eth_call_uint256(
    *,
    chain: str | None,
    to: str,
    data: str,
    rpc_url: str | None = None,
    gateway_client: GatewayClient | None = None,
    timeout: float = 5.0,
) -> int | None:
    """Perform eth_call and decode a single uint256 word."""
    raw = eth_call(
        chain=chain,
        to=to,
        data=data,
        rpc_url=rpc_url,
        gateway_client=gateway_client,
        timeout=timeout,
    )
    if raw is None:
        return None
    return decode_uint256(raw)
