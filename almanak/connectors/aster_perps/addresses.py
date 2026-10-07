"""Aster Pro on-chain contracts.

``vault`` is Aster's deposit vault ("Aster Deposit Bridge", AstherusVault UUPS
proxy): ``deposit(currency, amount, broker)`` credits ``msg.sender``'s Aster
account. Verified 2026-10-05 against Aster's docs, DefiLlama's adapter and
on-chain holdings; upgrades are 6h-timelocked behind a 4-of-7 Safe.
"""

from __future__ import annotations

ASTER_PRO: dict[str, dict[str, str]] = {
    "bsc": {
        "vault": "0x128463A60784c4D3f46c23Af3f65Ed859Ba87974",
    },
}

# Broker id that credits the FUTURES (perp) account; 1000 would credit spot.
FUTURES_BROKER_ID = 1

__all__ = ["ASTER_PRO", "FUTURES_BROKER_ID"]
