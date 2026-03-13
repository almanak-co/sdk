"""Uniswap V3 permission hints for permission discovery."""

from almanak.framework.permissions.hints import PermissionHints

# Agni Finance on Mantle is a Uniswap V3 fork that doesn't have a pool
# at fee tier 3000 for the synthetic USDC/WETH pair.  Fee tier 500 works.
PERMISSION_HINTS = PermissionHints(
    synthetic_fee_tier={
        "mantle": 500,
    },
)
