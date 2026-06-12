"""Aave V3 Anvil-fork yield-poke function.

Triggers on-chain interest accrual on a persistent Anvil fork by sending a
zero-amount supply transaction. This causes ``ReserveLogic.updateState()`` to
run, updating the liquidity index so aToken balances reflect accrued interest
between ticks.

Addresses are Arbitrum-specific (the only chain supported for Aave V3 poking
in V1). Additional chains require separate poke functions with chain-specific
pool addresses.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.yield_poke_base import (
    PokeResult,
    _pad_address,
    _pad_uint256,
    _send_tx,
)

__all__ = ["poke_aave_v3"]

# ---------------------------------------------------------------------------
# Aave V3 constants (Arbitrum)
# ---------------------------------------------------------------------------
AAVE_V3_POOL_ARBITRUM = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
# supply(address asset, uint256 amount, address onBehalfOf, uint16 referralCode)
AAVE_SUPPLY_SIG = "0x617ba037"


async def poke_aave_v3(rpc_url: str, wallet_address: str) -> PokeResult:
    """Poke Aave V3 by calling supply(USDC, 0, wallet, 0).

    A zero-amount supply is the lightest state-changing call that triggers
    ReserveLogic.updateState(), updating the liquidity index and making
    aToken balances reflect accrued interest.
    """
    try:
        data = (
            AAVE_SUPPLY_SIG
            + _pad_address(USDC_ARBITRUM)
            + _pad_uint256(0)
            + _pad_address(wallet_address)
            + _pad_uint256(0)
        )
        tx_hash = await _send_tx(rpc_url, wallet_address, AAVE_V3_POOL_ARBITRUM, data)
        return PokeResult(protocol="aave_v3", success=True, tx_hash=tx_hash)
    except Exception as e:
        return PokeResult(protocol="aave_v3", success=False, error=str(e))
