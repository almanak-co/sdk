"""BENQI Leverage Loop — cross-asset levered long with HF defense (Demo).

This strategy demonstrates a leveraged BENQI position (a Compound V2 fork on
Avalanche): it supplies native AVAX, borrows USDC against it, swaps the USDC to
WAVAX, unwraps WAVAX to AVAX, and re-supplies — repeating to build
``target_loops`` of leverage (a levered long on AVAX). It then HOLDS the levered
position while monitoring the health factor, and unwinds it via a
health-factor-aware ``WITHDRAW -> WRAP -> SWAP -> REPAY`` staircase when the HF
crosses a danger threshold OR a teardown signal arrives.

It is the leveraged (archetype #9) counterpart to the ``benqi_lending_lifecycle``
tutorial (archetype #8), and the BENQI/Compound-V2 sibling of ``morpho_looping``.

Example:
    from almanak.demo_strategies.benqi_looping import BenqiLoopingStrategy

    strategy = BenqiLoopingStrategy(
        chain="avalanche",
        wallet_address="0x...",
        config={
            "collateral_token": "AVAX",
            "borrow_token": "USDC",
            "wrapped_native": "WAVAX",
            "initial_collateral": "0.3",
            "target_loops": 2,
            "target_ltv": "0.3",
            "collateral_factor": "0.5",
        },
    )
"""

from .strategy import BenqiLoopingStrategy

__all__ = ["BenqiLoopingStrategy"]
