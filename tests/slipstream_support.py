"""Independent identified-fork calls provide Slipstream operation test inputs."""

import gzip
import json
from pathlib import Path

from eth_abi import decode

from almanak.caip import AssetId
from almanak.connectors.aerodrome_slipstream import operations
from almanak.connectors.aerodrome_slipstream.models import Target
from almanak.models import AssetAmount

EVIDENCE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/slipstream-base-discovery"
)
CAPTURE = json.loads(gzip.decompress((EVIDENCE / "operations.json.gz").read_bytes()))
READER = json.loads(gzip.decompress((EVIDENCE / "reader.json.gz").read_bytes()))


def target():
    pool = dict(READER["source"], chain=CAPTURE["fund"]["chain"])
    return Target.model_validate_json(
        json.dumps(
            dict(
                allocation="lp",
                account="fund",
                fund=CAPTURE["fund"],
                pool=pool,
                contracts=READER["contracts"],
                range=CAPTURE["range"],
                funding_asset=CAPTURE["assets"][0],
            )
        )
    )


def amounts(values):
    return tuple(
        AssetAmount(asset=asset, amount=value)
        for asset, value in zip(target().assets, values, strict=True)
    )


def nft():
    selected = target()
    return AssetId(
        f"{selected.pool.chain.chain_id}/erc721:{selected.contracts.position_manager}/{CAPTURE['nft']}"
    )


def addition(step):
    raw = bytes.fromhex(step["call"]["data"][10:])
    if step["kind"] == "mint":
        (p,) = decode(
            (
                "(address,address,int24,int24,int24,uint256,uint256,uint256,uint256,address,uint256,uint160)",
            ),
            raw,
        )
        maximum, minimum, deadline = p[5:7], p[7:9], p[10]
        constructor, extra = operations.Mint, {}
    else:
        (p,) = decode(("(uint256,uint256,uint256,uint256,uint256,uint256)",), raw)
        maximum, minimum, deadline = p[1:3], p[3:5], p[5]
        constructor, extra = operations.Increase, {"nft": nft()}
    return constructor(
        target=target(),
        maximum0=amounts(maximum)[0],
        maximum1=amounts(maximum)[1],
        minimum0=amounts(minimum)[0],
        minimum1=amounts(minimum)[1],
        minimum_liquidity=1,
        deadline=deadline,
        **extra,
    )


def operation(index):
    step = CAPTURE["steps"][index]
    raw = bytes.fromhex(step["call"]["data"][10:])
    if step["kind"] in ("mint", "increase"):
        return addition(step)
    if step["kind"] == "swap":
        (p,) = decode(("(address,address,int24,address,uint256,uint256,uint256,uint160)",), raw)
        currencies = {a.reference: a for a in target().assets}
        return operations.Swap(
            target=target(),
            amount=AssetAmount(asset=currencies[p[0]], amount=p[5]),
            minimum=AssetAmount(asset=currencies[p[1]], amount=p[6]),
            deadline=p[4],
        )
    if step["kind"] == "collect":
        return operations.Collect(target=target(), nft=nft(), deadline=2**64)
    spender, amount = decode(("address", "uint256"), raw)
    asset = next(a for a in target().assets if a.reference == step["call"]["to"])
    return operations.Approval(target=target(), asset=asset, spender=spender, amount=amount)
