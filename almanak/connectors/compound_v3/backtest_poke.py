"""Compound V3 Anvil-fork yield-poke function.

Triggers on-chain interest accrual on a persistent Anvil fork by calling
``accrueAccount(wallet)`` on every catalogued Arbitrum Comet. Compound V3
deploys one Comet per market (native USDC, bridged USDC.e, WETH, USDT on
Arbitrum) and the ``PokeFunction`` contract carries no market context, so
accruing all catalogued Comets is the only way to guarantee the wallet's
actual position accrues regardless of which market the backtest lent into.
``accrueAccount`` is permissionless and a cheap no-op on Comets where the
wallet holds no position (verified on-chain 2026-06-13, VIB-2630 spike).

Comets are accrued independently: one failing market (e.g. paused) must not
prevent accrual of the wallet's actual market, so per-Comet errors are
collected and reported rather than aborting the loop.

Addresses are Arbitrum-specific (the only chain declared on the connector's
``YieldPokeDecl`` in V1) and sourced from the connector's address table.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.yield_poke_base import (
    PokeResult,
    _pad_address,
    _send_tx,
)
from almanak.connectors.compound_v3.addresses import COMPOUND_V3_COMET_ADDRESSES

__all__ = ["poke_compound_v3"]

# accrueAccount(address) — keccak("accrueAccount(address)")[:4]. The pre-fix
# constant 0xf51e181a was the selector for scale(), which the Comet does not
# implement: every historical poke tx reverted and accrued nothing (VIB-2630).
COMPOUND_ACCRUE_SIG = "0xbfe69c8d"


async def poke_compound_v3(rpc_url: str, wallet_address: str) -> PokeResult:
    """Poke Compound V3 by calling accrueAccount(wallet) on every Arbitrum Comet.

    Iterates the connector's catalogued Arbitrum Comets (deduplicated by
    address, preserving table order) so the poke covers whichever market the
    backtest lent into. Returns success only if every Comet accrued; partial
    failures still accrue the remaining Comets and report which markets failed.
    """
    try:
        data = COMPOUND_ACCRUE_SIG + _pad_address(wallet_address)
    except Exception as e:
        return PokeResult(protocol="compound_v3", success=False, error=str(e))

    tx_hash: str | None = None
    errors: list[str] = []
    seen: set[str] = set()
    for market_id, comet in COMPOUND_V3_COMET_ADDRESSES["arbitrum"].items():
        if comet.lower() in seen:
            continue
        seen.add(comet.lower())
        try:
            sent = await _send_tx(rpc_url, wallet_address, comet, data)
            # _send_tx returns None on a malformed RPC response (neither
            # "result" nor "error"); record it instead of clobbering tx_hash.
            if sent:
                tx_hash = sent
            else:
                errors.append(f"{market_id}: no tx hash returned")
        except Exception as e:
            errors.append(f"{market_id}: {e}")

    if errors:
        return PokeResult(protocol="compound_v3", success=False, error="; ".join(errors), tx_hash=tx_hash)
    return PokeResult(protocol="compound_v3", success=True, tx_hash=tx_hash)
