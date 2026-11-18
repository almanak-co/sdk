"""The reviewed Aerodrome deployment of each supported chain.

One explicit factory and router per chain, seeded from the shipped references. A
resolver never searches for a deployment, substitutes another chain's addresses
or accepts a responding contract: an unlisted chain refuses. The pool
implementation, the fee contract and the factory registry are not listed here;
each is read from the verified factory, pool or router at the anchor.
"""

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.models import Address, Model


class Deployment(Model):
    factory: Address
    router: Address


DEPLOYMENTS: dict[ChainId, Deployment] = {
    ChainId("eip155:8453"): Deployment(
        factory="0x420dd381b31aef6683db6b902084cb0ffece40da",
        router="0xcf77a3ba9a5ca399b7c97c74d54e5b1beb874e43",
    ),
}


def deployment_for(chain: ChainId) -> Deployment:
    deployment = DEPLOYMENTS.get(chain)
    if deployment is None:
        raise DataError("unsupported_chain", f"Aerodrome has no reviewed deployment on {chain}")
    return deployment
