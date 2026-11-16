"""The reviewed Uniswap V3 deployment of each supported chain.

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


CANONICAL = Deployment(
    factory="0x1f98431c8ad98523631ae4a59f267346ea31f984",
    router="0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",
    position_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
    quoter="0x61ffe014ba17989e743c5f6cb21bf9697530b21e",
)

DEPLOYMENTS: dict[ChainId, Deployment] = {
    ChainId("eip155:1"): CANONICAL,
    ChainId("eip155:10"): CANONICAL,
    ChainId("eip155:137"): CANONICAL,
    ChainId("eip155:42161"): CANONICAL,
    ChainId("eip155:8453"): Deployment(
        factory="0x33128a8fc17869897dce68ed026d694621f6fdfd",
        router="0x2626664c2603336e57b271c5c0b26f421741e481",
        position_manager="0x03a520b32c04bf3beef7beb72e919cf822ed34f1",
        quoter="0x3d4e44eb1374240ce5f1b871ab261cd16335b76a",
    ),
    ChainId("eip155:43114"): Deployment(
        factory="0x740b1c1de25031c31ff4fc9a62f554a55cdc1bad",
        router="0xbb00ff08d01d300023c629e8ffffcb65a5a578ce",
        position_manager="0x655c406ebfa14ee2006250925e54ec43ad184f8b",
        quoter="0xbe0f5544ec67e9b3b2d979aaa43f18fd87e6257f",
    ),
    ChainId("eip155:4663"): Deployment(
        factory="0x1f7d7550b1b028f7571e69a784071f0205fd2efa",
        router="0xcaf681a66d020601342297493863e78c959e5cb2",
        position_manager="0x73991a25c818bf1f1128deaab1492d45638de0d3",
        quoter="0x33e885ed0ec9bf04ecfb19341582aadcb4c8a9e7",
    ),
    ChainId("eip155:56"): Deployment(
        factory="0xdb1d10011ad0ff90774d0c6bb92e5c5c8b4461f7",
        router="0xb971ef87ede563556b2ed4b1c0b0019111dd85d2",
        position_manager="0x7b8a01b39d58278b5de7e48c8449c9f4f5170613",
        quoter="0x78d78e420da98ad378d7799be8f4af69033eb077",
    ),
}


def deployment_for(chain: ChainId) -> Deployment:
    deployment = DEPLOYMENTS.get(chain)
    if deployment is None:
        raise DataError("unsupported_chain", f"Uniswap V3 has no reviewed deployment on {chain}")
    return deployment
