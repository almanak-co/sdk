"""The reviewed Aave V3 Pool of each supported chain.

One explicit protocol deployment root per chain. These are Pool contracts, never
markets or reserves: every reserve is resolved from the verified Pool. A resolver
never searches for a deployment, substitutes another chain's Pool or accepts a
responding contract: an unlisted chain refuses.
"""

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.models import Address

POOLS: dict[ChainId, Address] = {
    ChainId("eip155:1"): "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
    ChainId("eip155:10"): "0x794a61358d6845594f94dc1db02a252b5b4814ad",
    ChainId("eip155:56"): "0x6807dc923806fe8fd134338eabca509979a7e0cb",
    ChainId("eip155:137"): "0x794a61358d6845594f94dc1db02a252b5b4814ad",
    ChainId("eip155:8453"): "0xa238dd80c259a72e81d7e4664a9801593f98d1c5",
    ChainId("eip155:42161"): "0x794a61358d6845594f94dc1db02a252b5b4814ad",
    ChainId("eip155:43114"): "0x794a61358d6845594f94dc1db02a252b5b4814ad",
}


def pool_for(chain: ChainId) -> Address:
    pool = POOLS.get(chain)
    if pool is None:
        raise DataError("unsupported_chain", f"Aave V3 has no reviewed Pool on {chain}")
    return pool
