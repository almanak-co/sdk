"""Bounded ERC20 approvals using pinned allowance observations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from eth_abi import encode
from eth_utils import keccak

from almanak.framework.venues import VenueTargetRole

from .sdk import PERMIT2_ADDRESS
from .venue_verifier import address_ref

if TYPE_CHECKING:
    from almanak.framework.venues import VenueVerificationGateway


def observe_permit2_allowance(
    gateway: VenueVerificationGateway, *, chain: str, token: str, wallet: str, block_number: int
) -> int:
    result = gateway.read(
        chain=chain,
        target=address_ref(VenueTargetRole.PERMISSION_TARGET, token),
        payload=keccak(text="allowance(address,address)")[:4]
        + encode(["address", "address"], [wallet, PERMIT2_ADDRESS]),
        block_number=block_number,
    )
    if not isinstance(result, bytes) or len(result) != 32:
        raise ValueError("V4 ERC20 allowance observation must be one canonical uint256")
    return int.from_bytes(result, "big")


def approval_amounts(current: int | None, required: int) -> tuple[int, ...]:
    if type(required) is not int or not 0 <= required < 2**160:
        raise ValueError("V4 approval budget must fit Permit2 uint160")
    if current is not None and (type(current) is not int or not 0 <= current < 2**256):
        raise ValueError("V4 observed allowance must fit uint256")
    if current is not None and current >= required:
        # Some tokens fix Permit2 allowance at infinity and reject finite approvals.
        return ()
    if current:
        # Zero-first approval supports tokens that forbid replacing nonzero allowance.
        return (0, required)
    return (required,)
