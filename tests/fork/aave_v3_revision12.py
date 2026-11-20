"""Fork anchors where each Aave V3 Pool runs revision 12.

The reference fork fixtures pin blocks where every Pool still ran revision 11. Each
anchor here is the last block at or before 2026-11-18T00:00:00Z, a week after the Pools
were upgraded, so a rehearsal can run the same reference against the newer Pool.
"""

from pathlib import Path

import yaml
from eth_utils import keccak

from almanak.caip import AssetType, ChainId
from almanak.connectors.aave_v3.networks import POOLS
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.read_endpoints import read_endpoint
from almanak.models import Chain

ROOT = Path(__file__).parents[2]
POOL_REVISION = "0x" + keccak(text="POOL_REVISION()")[:4].hex()
IMPLEMENTATION_SLOT = hex(int.from_bytes(keccak(text="eip1967.proxy.implementation"), "big") - 1)
# The one library revision 12 links that revision 11 does not: the same source, compiled
# for Cancun, deployed at this address on every chain.
FLASH_LOAN_LOGIC = "0xc27a4d7ac0623b129919786d88714ef16132beae"
ANCHORS = {
    "aave_v3_ethereum_lending": {
        "chain": "eip155:1",
        "height": 26438213,
        "reference": "0xd6336a288be3006a5412e09a1986f9fbeeced9d2cfa977872ccac4252b32a20c",
    },
    "aave_v3_base_lending": {
        "chain": "eip155:8453",
        "height": 58848126,
        "reference": "0x48f1c217f74648a207e1722e91c7285a1769030edfe35fc864a8383d369b09d0",
    },
    "aave_v3_arbitrum_lending": {
        "chain": "eip155:42161",
        "height": 525175779,
        "reference": "0x5d951b44c06b571537fbc528821e48761644817555e0bc303428aadcbf42aff6",
    },
    "aave_v3_optimism_lending": {
        "chain": "eip155:10",
        "height": 159680611,
        "reference": "0x94372a36d76f77444fd8bf4255121242acfd1688381b54c4ddfae221e6021e5d",
    },
    "aave_v3_bnb_lending": {
        "chain": "eip155:56",
        "height": 134211720,
        "reference": "0xdc1d372b4641a8e6818f6a099bcf89875feff64df852cf7c5aa3d391a2eb22c3",
    },
    "aave_v3_polygon_lending": {
        "chain": "eip155:137",
        "height": 97503020,
        "reference": "0x3cd049917aa50d2808cd9a866585df0965ac9909030c837585fd41ed54e521bd",
    },
    "aave_v3_avalanche_lending": {
        "chain": "eip155:43114",
        "height": 100609250,
        "reference": "0x0bfe2c1f0753b513f7e805055530d54066b94e96fe5791afd593a1a63836be42",
    },
}
# Avalanche caps WAVAX supply at 12,000,000 and held above 15,300,000 at every block
# sampled from the upgrade to 2026-11-20, so the Pool admits no WAVAX supply there. The
# direct-action rehearsal supplies WETH.e on that cell instead.
COLLATERAL = {
    "aave_v3_avalanche_lending": AssetType(
        "eip155:43114/erc20:0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab"
    ),
}


def operator_at_revision_12(reference: str, directory: Path) -> Path:
    """A copy of the reference's fork operator file, anchored where its Pool is revision 12."""
    operator = yaml.safe_load((ROOT / f"examples/operators/{reference}.fork.yaml").read_text())
    (anchor,) = operator["fork"]["anchors"]
    assert anchor["chain"] == ANCHORS[reference]["chain"]
    operator["fork"]["anchors"] = [ANCHORS[reference]]
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{reference}.fork.yaml"
    path.write_text(yaml.safe_dump(operator, sort_keys=False))
    return path


def pool_revision(rpc, pool: str) -> int:
    """The revision the Pool on a fork reports at its head."""
    return int(rpc("eth_call", [{"to": pool, "data": POOL_REVISION}, "latest"]), 16)


def anchored_revision(env: Path, reference: str) -> int:
    """The revision the reference's Pool reports at its anchor, read from the origin chain."""
    anchor = ANCHORS[reference]
    chain = ChainId(anchor["chain"])
    origin = Chain(chain_id=chain, environment="mainnet")
    with EthereumReader(origin, read_endpoint(chain, env)) as reader:
        block = reader.pin_block(anchor["height"])
        assert block.hash == anchor["reference"]
        return reader.call(POOLS[chain], "POOL_REVISION()", block)[0]


def upgrade_pool(env: Path, reference: str, rpc) -> None:
    """Move a fork's Pool to revision 12, as the governance payload's `setPoolImpl` did.

    The implementation the origin chain runs at the reference's revision 12 anchor, and
    the library only it links, are copied onto the fork and the proxy is pointed at it.
    """
    anchor = ANCHORS[reference]
    chain = ChainId(anchor["chain"])
    pool = POOLS[chain]
    origin = Chain(chain_id=chain, environment="mainnet")
    with EthereumReader(origin, read_endpoint(chain, env)) as reader:
        at = hex(anchor["height"])
        word = reader._rpc("eth_getStorageAt", [pool, IMPLEMENTATION_SLOT, at])
        codes = {
            address: reader._rpc("eth_getCode", [address, at])
            for address in ("0x" + word[-40:], FLASH_LOAN_LOGIC)
        }
    for address, code in codes.items():
        rpc("anvil_setCode", [address, code])
    rpc("anvil_setStorageAt", [pool, IMPLEMENTATION_SLOT, word])
    rpc("evm_mine")
    assert pool_revision(rpc, pool) == 12
