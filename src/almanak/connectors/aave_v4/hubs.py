"""The Aave V4 Hubs of each supported chain.

A Hub lists the Spokes it serves, and each Spoke lists its reserves, so the
Hubs list the chain's markets for discovery. A Hub only names candidates: a
Spoke, its Hub and its reserve are accepted by their own verified revisions
(`deployments.py`), never because a Hub listed them. The addresses are those of
Aave's address book (bgd-labs/aave-address-book, `src/AaveV4<Network>.sol`).
"""

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.models import Address

HUBS: dict[ChainId, tuple[Address, ...]] = {
    ChainId("eip155:1"): (
        "0xcca852bc40e560adc3b1cc58ca5b55638ce826c9",
        "0x06002e9c4412cb7814a791ea3666d905871e536a",
        "0x943827dca022d0f354a8a8c332da1e5eb9f9f931",
        "0x62d63197660c080236193ca60b70e49a08e90368",
    ),
    ChainId("eip155:43114"): ("0xd07369fae4a5bb13c9ce446b052c7867b1abdf6e",),
}


def hubs_for(chain: ChainId) -> tuple[Address, ...]:
    hubs = HUBS.get(chain)
    if hubs is None:
        raise DataError("unsupported_chain", f"Aave V4 has no Hub listed on {chain}")
    return hubs
