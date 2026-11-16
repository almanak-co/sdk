"""The reviewed Morpho Blue core contract of each supported chain.

The core is the protocol deployment root, never a market: every market is
resolved from this contract by its own id. A resolver never searches for a core,
substitutes another chain's address or accepts a responding contract: an
unlisted chain refuses. `revisions.json` stays the compiled-code evidence of the
core, IRM and oracle roles; this table stays their deployment addresses.
"""

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.models import Address

CORES: dict[ChainId, Address] = {
    ChainId("eip155:1"): "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb",
    ChainId("eip155:10"): "0xce95afbb8ea029495c66020883f87aae8864af92",
    ChainId("eip155:56"): "0x01b0bd309aa75547f7a37ad7b1219a898e67a83a",
    ChainId("eip155:137"): "0x1bf0c2541f820e775182832f06c0b7fc27a25f67",
    ChainId("eip155:999"): "0x68e37de8d93d3496ae143f2e900490f6280c57cd",
    ChainId("eip155:4663"): "0x9d53d5e3bd5e8d4cbfa6db1ca238aea02e651010",
    ChainId("eip155:8453"): "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb",
    ChainId("eip155:42161"): "0x6c247b1f6182318877311737bac0844baa518f5e",
    ChainId("eip155:43114"): "0x895383274303aa19fe978afb4ac55c7f094f982c",
}


def core_for(chain: ChainId) -> Address:
    core = CORES.get(chain)
    if core is None:
        raise DataError("unsupported_chain", f"Morpho Blue has no reviewed core on {chain}")
    return core
