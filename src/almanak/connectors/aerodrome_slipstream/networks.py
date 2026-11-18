"""The reviewed Aerodrome Slipstream deployment of each supported chain.

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


DEPLOYMENTS: dict[ChainId, Deployment] = {
    ChainId("eip155:8453"): Deployment(
        factory="0xf8f2eb4940cfe7d13603dddd87f123820fc061ef",
        router="0x698cb2b6dd822994581fea6ea4fc755d1363a92f",
        position_manager="0xe1f8cd9ac4e4a65f54f38a5cdafca44f6dd68b53",
        quoter="0x514c8b5f54112481e28028f1166bd78501089259",
    ),
}


def deployment_for(chain: ChainId) -> Deployment:
    deployment = DEPLOYMENTS.get(chain)
    if deployment is None:
        raise DataError(
            "unsupported_chain", f"Aerodrome Slipstream has no reviewed deployment on {chain}"
        )
    return deployment
