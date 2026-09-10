"""Wallet settlement for active-hook swaps with nested PoolManager events."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from .addresses import UNISWAP_V4
from .pool_key import PoolKey

if TYPE_CHECKING:
    from .receipt_parser import SwapEventData, TransferEventData

_ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}\Z")


def _positive_raw(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, str | int) or not str(value).isdigit() or int(value) <= 0:
        raise ValueError("V4 settlement requires positive integral operation bounds")
    return int(value)


def _trader_swap(chain: str, key: PoolKey, swaps: list[SwapEventData]) -> SwapEventData:
    router = UNISWAP_V4.get(chain, {}).get("universal_router")
    if not router:
        raise ValueError("V4 settlement requires a configured Universal Router")
    router = router.lower()
    user_swaps = [swap for swap in swaps if swap.sender.lower() == router]
    if len(user_swaps) != 1 or any(swap.pool_id.lower() != key.pool_id for swap in swaps):
        raise ValueError("V4 settlement has an ambiguous trader swap or a different pool")
    return user_swaps[0]


def active_hook_swap_settlement(
    *,
    chain: str,
    key: PoolKey,
    operation: dict[str, Any] | None,
    swaps: list[SwapEventData],
    transfers: list[TransferEventData],
) -> tuple[SwapEventData, int, int, str, str]:
    """Separate the trader's wallet movement from hook-owned rebalancing flows."""
    if not isinstance(operation, dict):
        raise ValueError("Active-hook V4 settlement requires bound operation metadata")
    evidence = operation.get("hook_evidence")
    if (
        type(operation.get("schema_version")) is not int
        or operation["schema_version"] != 2
        or operation.get("operation") != "swap_exact_in"
        or operation.get("chain") != chain
        or operation.get("pool_key") != key.to_wire()
        or not isinstance(evidence, dict)
        or evidence.get("operation") != "swap_exact_in"
        or evidence.get("route") != "universal_router_eoa"
        or evidence.get("hook") != key.hooks
        or evidence.get("chain") != chain
    ):
        raise ValueError("V4 settlement operation does not bind the selected hook route")
    wallet = operation.get("wallet")
    if not isinstance(wallet, str) or not _ADDRESS.fullmatch(wallet) or int(wallet, 16) == 0:
        raise ValueError("V4 settlement requires a canonical wallet identity")
    wallet = wallet.lower()
    token_in, token_out = operation.get("token_in"), operation.get("token_out")
    if not isinstance(token_in, str) or not isinstance(token_out, str):
        raise ValueError("V4 settlement token identities are absent")
    direction = key.direction(token_in, token_out)
    token_in, token_out = token_in.lower(), token_out.lower()
    if int(key.currency0, 16) == 0:
        raise ValueError("Active-hook native settlement requires separate native balance evidence")
    swap = _trader_swap(chain, key, swaps)
    specified, reciprocal = (swap.amount0, swap.amount1) if direction else (swap.amount1, swap.amount0)
    if not specified < 0 < reciprocal:
        raise ValueError("V4 trader swap direction conflicts with operation identity")
    deltas = {token_in: 0, token_out: 0}
    for transfer in transfers:
        token = transfer.token.lower()
        if token not in deltas:
            continue
        if transfer.from_address.lower() == wallet:
            deltas[token] -= transfer.amount
        if transfer.to_address.lower() == wallet:
            deltas[token] += transfer.amount
    amount_in, amount_out = -deltas[token_in], deltas[token_out]
    maximum_in = _positive_raw(operation.get("amount_in"))
    minimum_out = _positive_raw(operation.get("minimum_out"))
    if amount_in != -specified or not 0 < amount_in <= maximum_in or amount_out < minimum_out:
        raise ValueError("V4 wallet settlement is absent or conflicts with executable amount bounds")
    return swap, amount_in, amount_out, token_in, token_out
