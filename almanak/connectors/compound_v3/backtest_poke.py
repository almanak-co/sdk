"""Compound V3 Anvil-fork yield-poke function.

Triggers on-chain interest accrual on a persistent Anvil fork by calling
``accrueAccount(wallet)`` on the Comet contract. This explicitly updates the
wallet's Compound V3 position to reflect earned interest between ticks.

Addresses are Arbitrum-specific (the only chain supported for Compound V3
poking in V1). Additional chains require separate poke functions with
chain-specific Comet addresses.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.yield_poke_base import (
    PokeResult,
    _pad_address,
    _send_tx,
)

__all__ = ["poke_compound_v3"]

# ---------------------------------------------------------------------------
# Compound V3 constants (Arbitrum)
# ---------------------------------------------------------------------------
COMPOUND_V3_COMET_ARBITRUM = "0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA"
# accrueAccount(address)
COMPOUND_ACCRUE_SIG = "0xf51e181a"


async def poke_compound_v3(rpc_url: str, wallet_address: str) -> PokeResult:
    """Poke Compound V3 by calling accrueAccount(wallet).

    This explicitly triggers interest accrual for the wallet's Compound V3
    position, updating the balance to reflect earned interest.
    """
    try:
        data = COMPOUND_ACCRUE_SIG + _pad_address(wallet_address)
        tx_hash = await _send_tx(rpc_url, wallet_address, COMPOUND_V3_COMET_ARBITRUM, data)
        return PokeResult(protocol="compound_v3", success=True, tx_hash=tx_hash)
    except Exception as e:
        return PokeResult(protocol="compound_v3", success=False, error=str(e))
