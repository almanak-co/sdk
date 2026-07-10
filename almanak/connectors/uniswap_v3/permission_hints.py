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
# Robinhood Chain (4663) has NO USDC/USDT; its canonical stable is USDG (6 dec)
# and the only liquid uniswap_v3 pools are WETH/USDG (primary fee tier 500,
# ~$3.5M TVL). The framework's default stable resolution picks ``USDE`` first
# (Ethena USDe is registered as a stablecoin on robinhood and sorts ahead of
# USDG), so without an override the synthetic SWAP/LP approves land on USDe —
# a token the real WETH/USDG strategy never touches — and every value transfer
# reverts at ``execTransactionWithRole`` (same class as the #1902 bsc trap).
# Pin both the swap and LP pair to ``(USDG, WETH)`` and the liquid fee tier 500.
_ROBINHOOD_USDG = "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168"
_ROBINHOOD_WETH = "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73"

PERMISSION_HINTS = PermissionHints(
    synthetic_fee_tier={
        "mantle": 500,
        "robinhood": 500,
    },
    synthetic_swap_pair={
        "robinhood": (_ROBINHOOD_USDG, _ROBINHOOD_WETH),
    },
    synthetic_lp_pair={
        "bsc": (
            "0x55d398326f99059fF775485246999027B3197955",  # USDT (BSC)
            "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",  # WBNB
        ),
        "robinhood": (_ROBINHOOD_USDG, _ROBINHOOD_WETH),
    },
    # Synthetic-discovery participation (VIB-4928): SWAP + LP. V3-style
    # SwapRouter02 auto-wraps native via msg.value, so emit the native-in
    # SWAP synthetic too (flips send_allowed on the router target for Zodiac).
    synthetic_discovery_intents=frozenset({"SWAP", "LP_OPEN", "LP_CLOSE"}),
    supports_native_in_swap=True,
)
