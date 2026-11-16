"""Pinned Reader ABI for the verified GMX position revision."""

from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.gmx_v2.positions import Position, PositionObservation
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS, AssetAmount

POSITION_ABI = (
    "((address,address,address),"
    "(uint256,uint256,uint256,int256,uint256,uint256,uint256,uint256,uint256,uint256),(bool))"
)


def position_key(target):
    return keccak(
        encode(
            ("address", "address", "address", "bool"),
            (
                target.fund.address,
                target.market.share_asset.reference,
                target.collateral_asset.reference,
                target.side == "long",
            ),
        )
    )


def verify_market(reader, market, block):
    actual = reader.call(
        market.contracts.reader,
        "getMarket(address,address)",
        block,
        inputs=("address", "address"),
        args=(market.contracts.data_store, market.share_asset.reference),
        outputs=("(address,address,address,address)",),
    )[0]
    expected = tuple(
        asset.reference
        for asset in (market.share_asset, market.index_asset, market.long_asset, market.short_asset)
    )
    if actual != expected:
        raise DataError("unsupported", "GMX configured market differs from the pinned Reader state")


def read_position(reader, target, block):
    if target.fund.chain != block.chain:
        raise ValueError("GMX position read must retain its target environment")
    verify_market(reader, target.market, block)
    key = position_key(target)
    addresses, numbers, flags = reader.call(
        target.market.contracts.reader,
        "getPosition(address,bytes32)",
        block,
        inputs=("address", "bytes32"),
        args=(target.market.contracts.data_store, key),
        outputs=(POSITION_ABI,),
    )[0]
    position = decoded_position(target, key, addresses, numbers, flags)
    return PositionObservation(target=target, block=block, position=position)


def decoded_position(target, key, addresses, numbers, flags):
    if addresses == (ZERO_ADDRESS,) * 3:
        if any(numbers) or flags != (False,):
            raise DataError(
                "integrity", "GMX absent position retains inconsistent accounting fields"
            )
        return None
    expected = (
        target.fund.address,
        target.market.share_asset.reference,
        target.collateral_asset.reference,
    )
    if addresses != expected or flags != (target.side == "long",):
        raise DataError(
            "integrity", "GMX position differs from its exact aggregate ownership scope"
        )
    return Position(
        key="0x" + key.hex(),
        size_usd_e30=numbers[0],
        index_amount=AssetAmount(asset=target.market.index_asset, amount=numbers[1]),
        collateral=AssetAmount(asset=target.collateral_asset, amount=numbers[2]),
        pending_impact_amount=numbers[3],
        borrowing_factor=numbers[4],
        funding_fee_amount_per_size=numbers[5],
        long_claimable_funding_amount_per_size=numbers[6],
        short_claimable_funding_amount_per_size=numbers[7],
        increased_at=numbers[8],
        decreased_at=numbers[9],
    )
