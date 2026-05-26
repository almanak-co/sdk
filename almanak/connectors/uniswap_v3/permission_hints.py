"""Uniswap V3 permission hints for permission discovery."""

from almanak.framework.permissions.hints import PermissionHints

# Agni Finance on Mantle is a Uniswap V3 fork that doesn't have a pool
# at fee tier 3000 for the synthetic USDC/WETH pair.  Fee tier 500 works.
#
# bsc declares both ``weth`` (Binance-pegged ETH) and ``wbnb`` in
# ``CHAIN_TOKENS``; ``_get_token_pair`` picks the bridged ETH first, but
# uniswap_v3 on bsc has no liquid USDC/ETH pool. The canonical liquid pair
# is ``(USDT, WBNB)``. Pinning the override here is preventive — mirrors
# the sushiswap_v3 fix in #1902 so LP tests on bnb for uniswap_v3 cannot
# fall into the same trap.
PERMISSION_HINTS = PermissionHints(
    synthetic_fee_tier={
        "mantle": 500,
    },
    synthetic_lp_pair={
        "bsc": (
            "0x55d398326f99059fF775485246999027B3197955",  # USDT (BSC)
            "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",  # WBNB
        ),
    },
)
