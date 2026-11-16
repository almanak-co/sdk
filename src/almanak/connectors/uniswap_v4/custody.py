"""What the fund holds for each pool currency: a native currency is held as its wrapped token.

A PoolKey names the chain's native coin (ETH, POL, BNB, AVAX) as `address(0)`. The Safe never
holds or sends native value: the Universal Router unwraps the fund's wrapped native (WETH,
WPOL, WBNB, WAVAX) into the pool and wraps native output back to it, so capital, inventory,
allowances and receipts all keep two ERC-20 assets, in PoolKey order. Which chains admit a
native leg is decided by the reviewed router routes, not by the coin.
"""

from almanak.caip import AssetType
from almanak.chains import evm_network


def is_native(currency: AssetType) -> bool:
    return currency.namespace == "slip44"


def custody_asset(currency: AssetType) -> AssetType:
    """The ERC-20 a fund holds for a pool currency."""
    if currency.namespace == "erc20":
        return currency
    if not is_native(currency):
        raise ValueError("Uniswap V4 currencies must be ERC-20 tokens or the native coin")
    try:
        network = evm_network(currency.chain)
    except ValueError as error:
        raise ValueError(f"Uniswap V4 native pools need a known chain: {error}") from None
    if network.native_asset != currency:
        raise ValueError("Uniswap V4 native currency differs from the chain native asset")
    return network.wrapped_native


def custody_assets(pool) -> tuple[AssetType, AssetType]:
    return custody_asset(pool.currency0), custody_asset(pool.currency1)


def native_index(pool) -> int | None:
    """The PoolKey position of a native currency, or None for two ERC-20 currencies."""
    return next((i for i, c in enumerate((pool.currency0, pool.currency1)) if is_native(c)), None)


def require_custody(pool):
    """Refusals for pools whose native leg cannot be held as its wrapped token."""
    if native_index(pool) is None:
        return
    held = custody_assets(pool)
    if held[0] == held[1]:
        raise ValueError("a native/wrapped-native pool would hold one asset for both currencies")
