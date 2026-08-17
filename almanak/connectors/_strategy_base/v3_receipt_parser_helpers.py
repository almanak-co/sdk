"""Protocol-neutral codecs and token helpers for the V3 fork family.

Helpers that log accept a ``log`` parameter so each parser's records keep its
own module logger name (tests filter ``caplog`` by logger name). Receipt parse
failures are handled by :mod:`fail_closed_extract`, not by this codec module.

This module must not import any concrete connector.
"""

from __future__ import annotations

import logging
from typing import Any

from almanak.connectors._strategy_base.base import HexDecoder, resolve_token_decimals
from almanak.framework.data.tokens import TokenResolutionError, build_token_meta_hint_map

logger = logging.getLogger(__name__)


def resolve_token_info(token: str, chain: str) -> tuple[str, int | None]:
    """Resolve token symbol and decimals via TokenResolver.

    Args:
        token: Token address or symbol
        chain: Chain identifier for the resolver

    Returns:
        Tuple of (symbol, decimals) or ("", None) if not found
    """
    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        try:
            resolver = get_token_resolver()
        except Exception as exc:
            raise TokenResolutionError(
                token=token, chain=chain, reason=f"Token resolver is unavailable: {exc}"
            ) from exc
        resolved = resolver.resolve(token, chain)
        decimals = resolve_token_decimals(token, chain, resolver=resolver)
        return resolved.symbol, decimals
    except TokenResolutionError:
        return "", None


def decode_swap_data(
    topics: list[Any],
    data: str,
    address: str,
    *,
    log: logging.Logger = logger,
) -> dict[str, Any]:
    """Decode Swap event data.

    Swap event structure:
    - topic1: sender (indexed)
    - topic2: recipient (indexed)
    - data: amount0 (int256), amount1 (int256), sqrtPriceX96 (uint160), liquidity (uint128), tick (int24)
    """
    try:
        # Indexed: sender, recipient
        sender = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        recipient = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

        # Non-indexed: amount0, amount1, sqrtPriceX96, liquidity, tick
        amount0 = HexDecoder.decode_int256(data, 0)
        amount1 = HexDecoder.decode_int256(data, 32)
        sqrt_price_x96 = HexDecoder.decode_uint160(data, 64)
        liquidity = HexDecoder.decode_uint128(data, 96)
        tick = HexDecoder.decode_int24(data, 128)

        # Normalize address
        pool_address = address.lower() if isinstance(address, str) else ""
        if isinstance(address, bytes):
            pool_address = "0x" + address.hex()

        return {
            "sender": sender,
            "recipient": recipient,
            "amount0": amount0,
            "amount1": amount1,
            "sqrt_price_x96": sqrt_price_x96,
            "liquidity": liquidity,
            "tick": tick,
            "pool_address": pool_address,
        }

    except Exception as e:
        log.warning(f"Failed to decode Swap data: {e}")
        return {"raw_data": data}


def decode_transfer_data(
    topics: list[Any],
    data: str,
    address: str,
    *,
    log: logging.Logger = logger,
) -> dict[str, Any]:
    """Decode Transfer event data.

    Transfer event structure:
    - topic1: from (indexed)
    - topic2: to (indexed)
    - data: value (uint256)
    """
    try:
        from_addr = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        to_addr = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        value = HexDecoder.decode_uint256(data, 0)

        token_address = address.lower() if isinstance(address, str) else ""
        if isinstance(address, bytes):
            token_address = "0x" + address.hex()

        return {
            "from_addr": from_addr,
            "to_addr": to_addr,
            "value": value,
            "token_address": token_address,
        }

    except Exception as e:
        log.warning(f"Failed to decode Transfer data: {e}")
        return {"raw_data": data}


def build_hint_map(
    swap_token_meta: dict[str, dict[str, Any]] | None,
    *,
    log: logging.Logger = logger,
) -> dict[str, tuple[str, int]]:
    """Map compiler token metadata to ``{address: (symbol, decimals)}``."""
    return {address.lower(): value for address, value in build_token_meta_hint_map(swap_token_meta).items()}
