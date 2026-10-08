"""Versioned execution evidence for venue orders and accepted cash withdrawals.

Acceptance proves an off-chain debit, not that a withdrawal's payout has mined.
EVM receipt validation remains a separate contract.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


def measured_decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool) or value is None or value == "":
        return None
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def _valid_transfer_amounts(transfer: dict[str, Any]) -> bool:
    gross = measured_decimal(transfer.get("gross_amount"))
    net = measured_decimal(transfer.get("net_amount"))
    fee = measured_decimal(transfer.get("fee_amount"))
    if gross is None or net is None or fee is None or not (gross > 0 and net > 0 and fee >= 0 and gross - fee == net):
        return False
    return True


def _valid_transfer_fee_usd(transfer: dict[str, Any]) -> bool:
    fee_usd = measured_decimal(transfer.get("fee_usd"))
    fee = measured_decimal(transfer.get("fee_amount"))
    if fee_usd is not None and (transfer["asset"].upper() not in {"USDT", "USDC", "USD1"} or fee_usd != fee):
        return False
    return True


def cash_transfer(extracted: dict[str, Any]) -> dict[str, Any] | None:
    transfer = extracted.get("venue_cash_transfer")
    if (
        not isinstance(transfer, dict)
        or type(transfer.get("schema_version")) is not int
        or transfer.get("schema_version") != 1
        or transfer.get("type") != "WITHDRAW"
    ):
        return None
    if not _valid_transfer_amounts(transfer):
        return None
    if not all(isinstance(transfer.get(k), str) and transfer[k].strip() for k in ("transfer_id", "asset", "receiver")):
        return None
    if not _valid_transfer_fee_usd(transfer):
        return None
    return transfer


def venue_receipt_identity(row: Any, extracted: dict[str, Any]) -> str | None:
    """Dispatch persisted venue evidence to its manifest-owned validator."""
    from almanak.connectors._strategy_base.venue_account_read_registry import VenueAccountReadRegistry

    if isinstance(row, dict):
        protocol = row.get("protocol")
    elif callable(getattr(row, "keys", None)):
        protocol = row["protocol"] if "protocol" in row.keys() else None
    else:
        protocol = getattr(row, "protocol", None)
    return VenueAccountReadRegistry.receipt_identity(protocol, row=row, extracted=extracted)
