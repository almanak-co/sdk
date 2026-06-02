"""Permission discovery hints for SushiSwap V3.

The framework's chain-default LP pair on bsc resolves to ``(USDC,
ETH-bridged)``, but sushiswap_v3 on bsc has no liquid pool there — the
canonical liquid pair is ``(USDT, WBNB)``. Without this override, synthetic
LP discovery emits ``approve`` permissions on USDC + ETH-bsc instead of
USDT + WBNB, and any real LP test on the canonical pair fails Zodiac
authorisation. Surfaced by #1902.
"""

from almanak.framework.permissions.hints import PermissionHints

PERMISSION_HINTS = PermissionHints(
    synthetic_lp_pair={
        "bsc": (
            "0x55d398326f99059fF775485246999027B3197955",  # USDT (BSC)
            "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",  # WBNB
        ),
    },
    # Synthetic-discovery participation (VIB-4928): SWAP + LP. V3-style
    # SwapRouter02 auto-wraps native via msg.value → emit native-in SWAP too.
    synthetic_discovery_intents=frozenset({"SWAP", "LP_OPEN", "LP_CLOSE"}),
    supports_native_in_swap=True,
)
