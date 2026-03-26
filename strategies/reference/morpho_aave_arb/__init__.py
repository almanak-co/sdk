"""Morpho-Aave Cross-Protocol Yield Rotation Strategy.

Monitors supply yields across Morpho Blue and Aave V3, automatically moving
capital (wstETH) to whichever protocol offers better effective yield. Uses
IntentSequence for atomic withdraw -> supply rebalancing.

Example:
    from strategies.reference.morpho_aave_arb import MorphoAaveYieldRotationStrategy

    strategy = MorphoAaveYieldRotationStrategy(
        chain="ethereum",
        wallet_address="0x...",
        config={
            "token": "wstETH",
            "morpho_market_id": "0xb323495f...",
            "min_spread_bps": 50,
        }
    )
"""

from .strategy import MorphoAaveYieldRotationStrategy

__all__ = ["MorphoAaveYieldRotationStrategy"]
