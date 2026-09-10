"""Activation evidence for the reviewed SecuritiesToken storage layout."""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any

from web3.types import RPCEndpoint

from almanak.integrations.bstocks.catalog import TokenReferenceProfile

_INITIAL_MULTIPLIER = 10**18
_UNSCHEDULED = 2**256 - 1


async def read_activation_storage(w3: Any, profile: TokenReferenceProfile, block_ref: dict) -> tuple[int, int, int]:
    async def read(method: str, params: list) -> bytes:
        response = await w3.provider.make_request(RPCEndpoint(method), params)
        raw = response.get("result")
        if response.get("error") is not None or not isinstance(raw, str) or not raw.startswith("0x"):
            raise ValueError("multiplier_storage_read_failed")
        try:
            return bytes.fromhex(raw[2:])
        except ValueError as exc:
            raise ValueError("multiplier_storage_malformed") from exc

    code, *words = await asyncio.gather(
        read("eth_getCode", [w3.to_checksum_address(profile.implementation), block_ref]),
        *(
            read("eth_getStorageAt", [w3.to_checksum_address(profile.address), hex(slot), block_ref])
            for slot in range(3)
        ),
    )
    if not profile.implementation_code_sha256 or hashlib.sha256(code).hexdigest() != profile.implementation_code_sha256:
        raise ValueError("multiplier_storage_implementation_unverified")
    if any(len(word) != 32 for word in words):
        raise ValueError("multiplier_storage_malformed")
    base, scheduled, activation = (int.from_bytes(word, "big") for word in words)
    return base, scheduled, activation


def verify_activation_storage(
    stored: tuple[int, int, int], *, active: int, following: int, effective: int, block_timestamp: int
) -> int | None:
    """Return the retained activation floor, zero for initialization, or pending.

    The reviewed implementation keeps its last schedule in slots 0–2 after
    activation, while effectiveAt() hides it. A new schedule seals the previous
    active value before replacing the next value and timestamp.
    """
    base, scheduled, activation = stored
    if not 10**9 <= base <= 10**27 or not 10**9 <= scheduled <= 10**27 or activation <= 0:
        raise ValueError("multiplier_storage_incoherent")
    if activation == _UNSCHEDULED:
        if (base, scheduled, active, following, effective) != (_INITIAL_MULTIPLIER,) * 4 + (0,):
            raise ValueError("multiplier_storage_initialization_incoherent")
        return 0
    if activation > block_timestamp:
        if (active, following, effective) != (base, scheduled, activation):
            raise ValueError("multiplier_storage_schedule_incoherent")
        return None
    if (active, following, effective) != (scheduled, scheduled, 0):
        raise ValueError("multiplier_storage_activation_incoherent")
    return activation
