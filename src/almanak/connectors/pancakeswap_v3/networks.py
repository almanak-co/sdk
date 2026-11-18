"""The reviewed PancakeSwap V3 deployment of each supported chain.

One explicit factory and periphery set per chain. A resolver never searches for a
deployment, substitutes another chain's addresses or accepts a responding
contract: an unlisted chain refuses.
"""

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.models import Address, Model


class Deployment(Model):
    factory: Address
    router: Address
    position_manager: Address
    quoter: Address


# PancakeSwap ships one V3 factory and periphery set at the same addresses on
# every listed chain. The pool deployer behind the factory is read from the
# factory itself, never listed here, so a changed deployer refuses on identity.
CANONICAL = Deployment(
    factory="0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865",
    router="0x1b81d678ffb9c0263b24a97847620c99d213eb14",
    position_manager="0x46a15b0b27311cedf172ab29e4f4766fbe7f4364",
    quoter="0xb048bbc1ee6b733fffcfb9e9cef7375518e25997",
)

DEPLOYMENTS: dict[ChainId, Deployment] = {
    ChainId("eip155:1"): CANONICAL,
    ChainId("eip155:56"): CANONICAL,
    ChainId("eip155:8453"): CANONICAL,
    ChainId("eip155:42161"): CANONICAL,
}


def deployment_for(chain: ChainId) -> Deployment:
    deployment = DEPLOYMENTS.get(chain)
    if deployment is None:
        raise DataError(
            "unsupported_chain", f"PancakeSwap V3 has no reviewed deployment on {chain}"
        )
    return deployment
