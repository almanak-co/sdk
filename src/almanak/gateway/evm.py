"""EVM identity conversion, including the pre-CAIP local claim compatibility key."""

import json
from typing import TYPE_CHECKING

from almanak.caip import AssetType, ChainId

if TYPE_CHECKING:
    from almanak.models import Account, AssetMetadata


def chain_number(chain: ChainId) -> int:
    if not isinstance(chain, ChainId) or chain.namespace != "eip155":
        raise ValueError("EVM adapter requires an EIP-155 chain reference")
    return int(chain.reference)


def erc20_address(asset: AssetType, chain: ChainId) -> str:
    chain_number(chain)
    if not isinstance(asset, AssetType) or asset.chain != chain or asset.namespace != "erc20":
        raise ValueError("EVM ERC-20 adapter requires the requested chain and erc20 asset type")
    if asset.reference == "0x" + "00" * 20:
        raise ValueError("ERC-20 contract cannot be the zero address")
    return asset.reference


def native_asset(chain: ChainId) -> AssetType:
    from almanak.chains import evm_network

    if chain_number(chain) == 31337:
        return AssetType(f"{chain}/slip44:60")
    return evm_network(chain).native_asset


def native_metadata(chain: ChainId) -> tuple[AssetType, "AssetMetadata"]:
    from almanak.models import AssetMetadata

    return native_asset(chain), AssetMetadata(decimals=18)


def currency_address(asset: AssetType, chain: ChainId) -> str:
    if (
        isinstance(asset, AssetType)
        and asset.namespace == "slip44"
        and asset == native_asset(chain)
    ):
        return "0x" + "00" * 20
    return erc20_address(asset, chain)


def legacy_account_claim(account: "Account") -> str | None:
    if account.chain.chain_id.namespace != "eip155":
        return None
    # Byte-for-byte schema-1/2 Pydantic account serialization. Claim both formats
    # permanently so a preceding SDK cannot acquire a new deployment's fund.
    return json.dumps(
        {
            "chain": {
                "chain_id": chain_number(account.chain.chain_id),
                "environment": account.chain.environment,
            },
            "address": account.address,
        },
        separators=(",", ":"),
    )
