"""The Compound V3 Configurator of each supported chain.

The Configurator deploys every Comet on its chain and records each one in a
`CometDeployed` event, so its logs list the chain's markets for discovery. It
only names candidates: a Comet is accepted by its own verified revision
(`deployment.py`), never because a Configurator listed it. The addresses are
those of Compound's deployment manifests (compound-finance/comet,
`deployments/<network>/<market>/roots.json`).
"""

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.models import Address

CONFIGURATORS: dict[ChainId, Address] = {
    ChainId("eip155:1"): "0x316f9708bb98af7da9c68c1c3b5e79039cd336e3",
    ChainId("eip155:10"): "0x84e93ec6170ed630f5ebd89a1aae72d4f63f2713",
    ChainId("eip155:8453"): "0x45939657d1ca34a8fa39a924b71d28fe8431e581",
    ChainId("eip155:42161"): "0xb21b06d71c75973babde35b49ffdac3f82ad3775",
}


def configurator_for(chain: ChainId) -> Address:
    configurator = CONFIGURATORS.get(chain)
    if configurator is None:
        raise DataError("unsupported_chain", f"Compound V3 has no Configurator listed on {chain}")
    return configurator
