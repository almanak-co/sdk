"""Permission discovery hints for PancakeSwap V3.

PancakeSwap V3 is the dominant DEX on bsc but the framework's chain-default
LP pair on bsc resolves to ``(USDC, ETH-bridged)``. The canonical liquid
pair on bsc is ``(USDT, WBNB)`` (and ``(USDC, WBNB)`` for stablecoin LPs).
Pinning ``(USDT, WBNB)`` here is preventive — it mirrors the sushiswap_v3
override applied for #1902 so synthetic LP discovery seeds approves on the
right tokens before LP tests grow on bnb for pancakeswap_v3.
"""

from almanak.framework.permissions.hints import PermissionHints

PERMISSION_HINTS = PermissionHints(
    synthetic_lp_pair={
        "bsc": (
            "0x55d398326f99059fF775485246999027B3197955",  # USDT (BSC)
            "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",  # WBNB
        ),
    },
)
