"""The reviewed Curve factories of each supported chain and pool family.

One explicit factory per chain and family. A resolver never searches for a
deployment, never infers a family from a responding contract or a coin symbol
and never accepts another chain's addresses: an unlisted chain refuses, and a
pool that no reviewed factory registers refuses.
"""

from typing import Literal

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS, Address, Block, Model

Family = Literal["stableswap_ng", "twocrypto_ng", "tricrypto_ng"]

# Each family's factory returns its registered coins in its own ABI shape. An
# unregistered pool reads back as an empty list or as zero addresses, so the
# membership probe never depends on a revert.
COIN_VIEWS: dict[Family, str] = {
    "stableswap_ng": "address[]",
    "twocrypto_ng": "address[2]",
    "tricrypto_ng": "address[3]",
}


class Factories(Model):
    """The reviewed factory of each family this chain supports."""

    stableswap_ng: Address | None = None
    twocrypto_ng: Address | None = None
    tricrypto_ng: Address | None = None

    def listed(self) -> tuple[tuple[Family, str], ...]:
        entries: tuple[tuple[Family, str | None], ...] = (
            ("stableswap_ng", self.stableswap_ng),
            ("twocrypto_ng", self.twocrypto_ng),
            ("tricrypto_ng", self.tricrypto_ng),
        )
        return tuple((family, value) for family, value in entries if value is not None)


FACTORIES: dict[ChainId, Factories] = {
    ChainId("eip155:1"): Factories(
        stableswap_ng="0x6a8cbed756804b16e05e741edabd5cb544ae21bf",
        twocrypto_ng="0x98ee851a00abee0d95d08cf4ca2bdce32aeaaf7f",
        tricrypto_ng="0x0c0e5f2ff0ff18a3be9b835635039256dc4b4963",
    ),
    ChainId("eip155:10"): Factories(
        stableswap_ng="0x5eee3091f747e60a045a2e715a4c71e600e31f6e",
        twocrypto_ng="0x98ee851a00abee0d95d08cf4ca2bdce32aeaaf7f",
        tricrypto_ng="0xc6c09471ee39c7e30a067952fcc89c8922f9ab53",
    ),
    ChainId("eip155:56"): Factories(
        stableswap_ng="0xd7e72f3615aa65b92a4dbdc211e296a35512988b",
        twocrypto_ng="0x98ee851a00abee0d95d08cf4ca2bdce32aeaaf7f",
    ),
    ChainId("eip155:137"): Factories(
        stableswap_ng="0x1764ee18e8b3cca4787249ceb249356192594585",
        twocrypto_ng="0x98ee851a00abee0d95d08cf4ca2bdce32aeaaf7f",
        tricrypto_ng="0xc1b393efef38140662b91441c6710aa704973228",
    ),
    ChainId("eip155:4663"): Factories(
        stableswap_ng="0x8271e06e5887fe5ba05234f5315c19f3ec90e8ad",
        twocrypto_ng="0xe7fbd704b938cb8fe26313c3464d4b7b7348c88c",
        tricrypto_ng="0x6e28493348446503db04a49621d8e6c9a40015fb",
    ),
    ChainId("eip155:8453"): Factories(
        stableswap_ng="0xd2002373543ce3527023c75e7518c274a51ce712",
        twocrypto_ng="0xc9fe0c63af9a39402e8a5514f9c43af0322b665f",
        tricrypto_ng="0xa5961898870943c68037f6848d2d866ed2016bcb",
    ),
    ChainId("eip155:42161"): Factories(
        stableswap_ng="0x9af14d26075f142eb3f292d5065eb3faa646167b",
        twocrypto_ng="0x98ee851a00abee0d95d08cf4ca2bdce32aeaaf7f",
        tricrypto_ng="0xbc0797015fcfc47d9c1856639cae50d0e69fbee8",
    ),
    ChainId("eip155:43114"): Factories(
        stableswap_ng="0x1764ee18e8b3cca4787249ceb249356192594585",
    ),
}


class Registration(Model):
    """One factory's own registry answer for the authored pool address."""

    family: Family
    factory: Address
    coins: tuple[Address, ...]


def factories_for(chain: ChainId) -> Factories:
    listed = FACTORIES.get(chain)
    if listed is None:
        raise DataError("unsupported_chain", f"Curve has no reviewed factory on {chain}")
    return listed


def registered_coins(reader, family: Family, factory: str, pool: str, block: Block):
    """The factory's own coin registry; an unregistered pool holds zero addresses."""
    coins = reader.call(
        factory,
        "get_coins(address)",
        block,
        inputs=("address",),
        args=(pool,),
        outputs=(COIN_VIEWS[family],),
    )[0]
    return () if any(coin == ZERO_ADDRESS for coin in coins) else tuple(coins)


def registered_family(reader, listed: Factories, pool: str, block: Block) -> Registration:
    """Exactly one reviewed factory of this chain must register the authored pool."""
    matches = []
    for family, factory in listed.listed():
        coins = registered_coins(reader, family, factory, pool, block)
        if coins:
            matches.append(Registration(family=family, factory=factory, coins=coins))
    if not matches:
        raise DataError(
            "unsupported_pool", "no reviewed Curve factory on this chain registers the pool"
        )
    if len(matches) > 1:
        raise DataError("ambiguous_pool", "more than one reviewed Curve factory registers the pool")
    return matches[0]
