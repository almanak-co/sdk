"""Pendle protocol permission hints for permission discovery."""

from almanak.framework.permissions.hints import PermissionHints

# Pendle swaps require one token to be a PT (Principal Token).
# The default USDC/WETH pair doesn't work, so we override with
# known PT token pairs per chain.
PERMISSION_HINTS = PermissionHints(
    synthetic_swap_pair={
        # arbitrum: wstETH -> PT-wstETH-25JUN2026
        "arbitrum": (
            "0x5979D7b546E38E414F7E9822514be443A4800529",  # wstETH
            "PT-wstETH",
        ),
        # ethereum: wstETH -> PT-sUSDe-7MAY2026 (uses sUSDe market)
        "ethereum": (
            "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",  # sUSDe
            "PT-sUSDe",
        ),
    },
)
