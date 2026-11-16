"""Trusted observations verify the precision used to resolve author quantities."""

from almanak.gateway.ethereum import DataError


def verify(config, asset):
    metadata = config.asset_metadata.get(asset.identifier)
    if metadata is not None and metadata.decimals != asset.decimals:
        raise DataError(
            "asset_precision_mismatch",
            f"{asset.identifier}: configured decimals {metadata.decimals} "
            f"differ from observed {asset.decimals}",
        )


def verify_native(config):
    for asset, metadata in config.asset_metadata.items():
        if asset.namespace == "slip44" and asset.chain.namespace == "eip155":
            from almanak.gateway.evm import native_asset

            if asset != native_asset(asset.chain) or metadata.decimals != 18:
                raise ValueError(
                    "configured native asset precision or identity differs from the EVM adapter"
                )
