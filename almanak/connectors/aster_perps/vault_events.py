"""Decoder for the Aster deposit vault's ``Deposit`` event (pure, strategy-safe)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from eth_abi import decode as abi_decode
from eth_utils import keccak, to_checksum_address

# Deposit(address indexed account, address indexed currency, bool isNative, uint256 amount, uint256 broker)
DEPOSIT_TOPIC = "0x" + keccak(text="Deposit(address,address,bool,uint256,uint256)").hex()


@dataclass(frozen=True)
class VaultDeposit:
    vault: str
    account: str
    currency: str
    is_native: bool
    amount: int  # amount the vault received, in token base units
    broker: int


def _hex(value: Any) -> str:
    if isinstance(value, bytes | bytearray):
        return "0x" + bytes(value).hex()
    text = str(value)
    return text if text.startswith("0x") else "0x" + text


def decode_deposits(receipt: dict[str, Any], *, vault: str) -> list[VaultDeposit]:
    """Every ``Deposit`` emitted by ``vault`` in ``receipt``."""
    out: list[VaultDeposit] = []
    for log in receipt.get("logs", []):
        topics = [_hex(t).lower() for t in log.get("topics", [])]
        if len(topics) != 3 or topics[0] != DEPOSIT_TOPIC or str(log.get("address", "")).lower() != vault.lower():
            continue
        is_native, amount, broker = abi_decode(["bool", "uint256", "uint256"], bytes.fromhex(_hex(log["data"])[2:]))
        out.append(
            VaultDeposit(
                vault=to_checksum_address(log["address"]),
                account=to_checksum_address("0x" + topics[1][-40:]),
                currency=to_checksum_address("0x" + topics[2][-40:]),
                is_native=bool(is_native),
                amount=int(amount),
                broker=int(broker),
            )
        )
    return out


__all__ = ["DEPOSIT_TOPIC", "VaultDeposit", "decode_deposits"]
