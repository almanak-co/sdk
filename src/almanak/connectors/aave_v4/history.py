"""Aave V4 market observations; hypothetical user positions are never RPC reads."""

from almanak.connectors.aave_v4.deployments import (
    SUPPORTED_REVISIONS as SUPPORTED_REVISIONS,
)
from almanak.connectors.aave_v4.deployments import (
    ContractRevision as ContractRevision,
)
from almanak.connectors.aave_v4.deployments import (
    revision as revision,
)
from almanak.connectors.aave_v4.domain import AaveReserve, ReserveObservation
from almanak.connectors.aave_v4.reader import read_reserve
from almanak.gateway.ethereum import DataError
from almanak.models import Block, Model, UInt

VIRTUAL = 10**6


class Market(Model):
    reserve: ReserveObservation
    added_assets: UInt
    added_shares: UInt
    liquidity: UInt
    spoke_shares: UInt
    add_cap: UInt
    active: bool
    halted: bool
    revisions: tuple[ContractRevision, ...]

    def supplied_assets(self, shares: int) -> int:
        return shares * (self.added_assets + VIRTUAL) // (self.added_shares + VIRTUAL)

    def supplied_shares(self, assets: int) -> int:
        return assets * (self.added_shares + VIRTUAL) // (self.added_assets + VIRTUAL)

    def burned_shares(self, assets: int) -> int:
        return ceiling(assets * (self.added_shares + VIRTUAL), self.added_assets + VIRTUAL)

    def cap_exposure(self, shares: int) -> int:
        return ceiling(shares * (self.added_assets + VIRTUAL), self.added_shares + VIRTUAL)


def ceiling(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator


def read_market(reader, source: AaveReserve, block: Block) -> Market:
    reserve = read_reserve(reader, source, block)
    asset_id = reserve.hub_asset_id
    args = (asset_id, source.spoke)
    add_cap, _, _, active, halted = reader.call(
        source.hub,
        "getSpokeConfig(uint256,address)",
        block,
        inputs=("uint256", "address"),
        args=args,
        outputs=("uint40", "uint40", "uint24", "bool", "bool"),
    )
    (spoke_shares,) = reader.call(
        source.hub,
        "getSpokeAddedShares(uint256,address)",
        block,
        inputs=("uint256", "address"),
        args=args,
    )
    amounts = {}
    for name, signature in (
        ("added_assets", "getAddedAssets(uint256)"),
        ("added_shares", "getAddedShares(uint256)"),
        ("liquidity", "getAssetLiquidity(uint256)"),
    ):
        (amounts[name],) = reader.call(
            source.hub, signature, block, inputs=("uint256",), args=(asset_id,)
        )
    return Market(
        reserve=reserve,
        **amounts,
        spoke_shares=spoke_shares,
        add_cap=add_cap,
        active=active,
        halted=halted,
        revisions=tuple(revision(reader, address, block) for address in (source.spoke, source.hub)),
    )


def verify_conversion(reader, market: Market, assets: int, shares: int):
    source = market.reserve.source
    for signature, amount, expected in (
        ("previewAddByAssets(uint256,uint256)", assets, market.supplied_shares(assets)),
        ("previewRemoveByShares(uint256,uint256)", shares, market.supplied_assets(shares)),
    ):
        (actual,) = reader.call(
            source.hub,
            signature,
            market.reserve.block,
            inputs=("uint256", "uint256"),
            args=(market.reserve.hub_asset_id, amount),
        )
        if actual != expected:
            raise DataError(
                "unsupported_model", "historical Hub conversion differs from this model"
            )
