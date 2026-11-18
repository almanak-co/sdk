"""The reviewed Uniswap V4 deployment of each supported chain.

`deployments.NETWORK_ROLES` stays the single reviewed table; this module gives it
a typed, validated view for public resolution. A resolver never searches for a
deployment, substitutes another chain's addresses or accepts a responding
contract: an unlisted chain refuses.
"""

from almanak.caip import ChainId
from almanak.connectors.uniswap_v4.deployments import NETWORK_ROLES
from almanak.connectors.uniswap_v4.models import Deployment
from almanak.gateway.ethereum import DataError
from almanak.models import Address, Model


class NetworkRoles(Model):
    pool_manager: Address
    state_view: Address
    router: Address
    position_manager: Address
    quoter: Address
    permit2: Address

    @property
    def contracts(self) -> Deployment:
        return Deployment(
            router=self.router,
            position_manager=self.position_manager,
            quoter=self.quoter,
            permit2=self.permit2,
        )

    @property
    def addresses(self) -> tuple[str, ...]:
        return tuple(sorted(self.model_dump().values()))


def roles_for(chain: ChainId) -> NetworkRoles:
    roles = NETWORK_ROLES.get(str(chain))
    if roles is None:
        raise DataError("unsupported_chain", f"Uniswap V4 has no reviewed deployment on {chain}")
    return NetworkRoles.model_validate(roles)
