"""The reviewed SparkLend Pool of each supported chain.

SparkLend is an Aave V3 fork with its own Ethereum deployment root. This is a
Pool contract, never a market or reserve: every reserve is resolved from the
verified Pool. An unlisted chain refuses.
"""

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.models import Address

POOLS: dict[ChainId, Address] = {
    ChainId("eip155:1"): "0xc13e21b648a5ee794902342038ff3adab66be987",
}


def pool_for(chain: ChainId) -> Address:
    pool = POOLS.get(chain)
    if pool is None:
        raise DataError("unsupported_chain", f"SparkLend has no reviewed Pool on {chain}")
    return pool
