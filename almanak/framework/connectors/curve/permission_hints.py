"""Curve Finance permission hints for permission discovery."""

from almanak.framework.permissions.hints import PermissionHints

# Curve pools are pair-specific (stableswap, tricrypto).
# The default USDC/WETH pair doesn't match any pool, so we override
# with token pairs that exist in known Curve pools per chain.
PERMISSION_HINTS = PermissionHints(
    synthetic_swap_pair={
        # ethereum: 3pool has DAI/USDC/USDT
        "ethereum": (
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
            "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
        ),
        # arbitrum: 2pool has USDC.e/USDT
        "arbitrum": (
            "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",  # USDC.e
            "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",  # USDT
        ),
    },
)
