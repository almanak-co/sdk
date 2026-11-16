"""Hash-pinned Aave accounting and versioned risk reads for dataset preparation."""

from almanak.backtest.economic_data import (
    RAY,
    AccountingSource,
    DynamicConfig,
    LiquidationConfig,
    Rates,
    RiskParameters,
    supply_rate,
)
from almanak.connectors.aave_v4.domain import ReserveObservation
from almanak.connectors.aave_v4.history import VIRTUAL
from almanak.connectors.aave_v4.reader import (
    read_dynamic_reserve_config,
    read_liquidation_config,
    read_reserve,
)
from almanak.gateway.ethereum import DataError
from almanak.models import Block, address


def read_rates(reader, reserve: ReserveObservation, block: Block) -> Rates:
    source = AccountingSource(
        chain=reserve.source.chain,
        hub=reserve.source.hub,
        asset=reserve.source.asset,
        hub_asset_id=reserve.hub_asset_id,
    )
    if reader.chain != source.chain or block.chain != source.chain:
        raise DataError("chain_mismatch", "accounting source differs from its historical chain")
    fields = accounting_fields(reader, source, block)
    underlying, decimals = reader.call(
        source.hub,
        "getAssetUnderlyingAndDecimals(uint256)",
        block,
        inputs=("uint256",),
        args=(source.hub_asset_id,),
        outputs=("address", "uint8"),
    )
    if (address(underlying), decimals) != (source.asset.reference, reserve.asset.decimals):
        raise DataError("source_mismatch", "Hub asset metadata differs from its declared reserve")
    return Rates(
        source=source,
        convention="annual_fraction_ray_365_days",
        supply_apr_ray=supply_rate(
            fields["index"],
            fields["drawn"],
            fields["premium"],
            fields["rate"],
            fields["fee"],
            fields["assets"],
        ),
        borrow_apr_ray=fields["rate"],
        supply_index_ray=(fields["assets"] + VIRTUAL) * RAY // (fields["shares"] + VIRTUAL),
        borrow_index_ray=fields["index"],
        added_assets=fields["assets"],
        added_shares=fields["shares"],
        liquidity_fee_bps=fields["fee"],
    )


def accounting_fields(reader, source: AccountingSource, block):
    result = {}
    for key, signature in (
        ("assets", "getAddedAssets(uint256)"),
        ("shares", "getAddedShares(uint256)"),
        ("index", "getAssetDrawnIndex(uint256)"),
        ("drawn", "getAssetDrawnShares(uint256)"),
        ("rate", "getAssetDrawnRate(uint256)"),
    ):
        (result[key],) = reader.call(
            source.hub,
            signature,
            block,
            inputs=("uint256",),
            args=(source.hub_asset_id,),
        )
    result["premium"], _ = reader.call(
        source.hub,
        "getAssetPremiumData(uint256)",
        block,
        inputs=("uint256",),
        args=(source.hub_asset_id,),
        outputs=("uint256", "int256"),
    )
    _, result["fee"], _, _ = reader.call(
        source.hub,
        "getAssetConfig(uint256)",
        block,
        inputs=("uint256",),
        args=(source.hub_asset_id,),
        outputs=("address", "uint16", "address", "address"),
    )
    return result


def read_risk(reader, source, block) -> RiskParameters:
    reserve = read_reserve(reader, source, block)
    configs = []
    for key in range(reserve.dynamic_config_key + 1):
        config = read_dynamic_reserve_config(reader, source.spoke, source.reserve_id, key, block)
        configs.append(DynamicConfig(key=key, **config._asdict()))
    liquidation = read_liquidation_config(reader, source.spoke, block)
    return RiskParameters(
        reserve=reserve,
        configs=tuple(configs),
        liquidation=LiquidationConfig(**liquidation._asdict()),
    )


def accounting_requests(reserve, block):
    from eth_abi import encode
    from eth_utils import keccak

    from almanak.backtest.history import ReadRequest

    signatures = (
        "getAddedAssets(uint256)",
        "getAddedShares(uint256)",
        "getAssetDrawnIndex(uint256)",
        "getAssetDrawnShares(uint256)",
        "getAssetDrawnRate(uint256)",
        "getAssetPremiumData(uint256)",
        "getAssetConfig(uint256)",
        "getAssetUnderlyingAndDecimals(uint256)",
    )
    selector = {"blockHash": block.hash, "requireCanonical": True}
    argument = encode(("uint256",), (reserve.hub_asset_id,))
    return tuple(
        ReadRequest(
            chain=reserve.source.chain,
            method="eth_call",
            params=(
                {
                    "to": reserve.source.hub,
                    "data": "0x" + (keccak(text=signature)[:4] + argument).hex(),
                },
                selector,
            ),
        )
        for signature in signatures
    )
