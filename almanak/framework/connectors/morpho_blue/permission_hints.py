"""Morpho Blue permission hints for permission discovery."""

from almanak.framework.permissions.hints import PermissionHints

# Well-known Morpho Blue market ID (WETH/USDC on Ethereum).
# Used as a synthetic market_id for permission discovery - the actual
# market_id value doesn't affect which selectors are discovered.
_SYNTHETIC_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"

PERMISSION_HINTS = PermissionHints(
    synthetic_market_id=_SYNTHETIC_MARKET_ID,
)
