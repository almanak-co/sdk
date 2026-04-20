"""Morpho Blue Looping Strategy - Leveraged Yield Farming Demo.

This strategy demonstrates recursive borrowing on Morpho Blue to amplify yield
through leverage. It supplies collateral, borrows against it, swaps back to
collateral, and repeats to build a leveraged position.

Example:
    from strategies.demo.morpho_looping import MorphoLoopingStrategy

    strategy = MorphoLoopingStrategy(
        chain="ethereum",
        wallet_address="0x...",
        config={
            "market_id": "0xb323495f...",
            "collateral_token": "wstETH",
            "borrow_token": "USDC",
            "target_loops": 3,
        }
    )
"""

from .strategy import MorphoLoopingStrategy

__all__ = ["MorphoLoopingStrategy"]
