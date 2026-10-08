"""Pure validation of persisted Aster fill and withdrawal acceptance records."""

from __future__ import annotations

from typing import Any, TypeGuard

from almanak.framework.accounting.venue_receipts import cash_transfer, measured_decimal


def _row_field(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    keys = getattr(row, "keys", None)
    return row[key] if callable(keys) and key in keys() else getattr(row, key, None)


def _valid_receipt_header(receipt: Any, row: Any) -> TypeGuard[dict[str, Any]]:
    if (
        not isinstance(receipt, dict)
        or type(receipt.get("schema_version")) is not int
        or receipt.get("schema_version") != 1
    ):
        return False
    if receipt.get("protocol") != "aster_perps" or _row_field(row, "protocol") != "aster_perps":
        return False
    return True


def _valid_off_chain_execution(row: Any, extracted: dict[str, Any], receipt: dict[str, Any]) -> bool:
    if receipt.get("chain") != "bsc" or _row_field(row, "chain") != "bsc" or _row_field(row, "tx_hash"):
        return False
    gas = _row_field(row, "gas_used")
    if isinstance(gas, bool) or not isinstance(gas, int) or gas != 0 or extracted.get("sub_transactions"):
        return False
    if extracted.get("clob_status") not in ("matched", "partially_filled"):
        return False
    return True


def _valid_wallet(wallet: Any) -> TypeGuard[str]:
    if not isinstance(wallet, str) or len(wallet) != 42 or not wallet.startswith("0x"):
        return False
    try:
        bytes.fromhex(wallet[2:])
    except ValueError:
        return False
    return True


def venue_receipt_identity(row: Any, extracted: dict[str, Any]) -> str | None:
    """Return the execution identity only when all persisted evidence agrees."""
    receipt = extracted.get("venue_receipt")
    if not _valid_receipt_header(receipt, row):
        return None
    if not _valid_off_chain_execution(row, extracted, receipt):
        return None
    wallet = receipt.get("wallet_address")
    if not _valid_wallet(wallet):
        return None
    kind = receipt.get("kind")
    if kind == "ORDER" and _row_field(row, "intent_type") in ("PERP_OPEN", "PERP_CLOSE"):
        return _order_identity(receipt, extracted, wallet, _row_field(row, "intent_type"))
    if kind == "WITHDRAWAL_ACCEPTED" and _row_field(row, "intent_type") == "PERP_WITHDRAW":
        return _withdrawal_identity(receipt, extracted, wallet)
    return None


def _order_identity(receipt: dict, extracted: dict, wallet: str, intent_type: str) -> str | None:
    order = extracted.get("aster_order")
    if not isinstance(order, dict):
        return None
    order_id = order.get("order_id")
    if not isinstance(order_id, str) or not order_id.isdigit() or int(order_id) <= 0:
        return None
    if receipt.get("execution_id") != str(order_id) or extracted.get("order_id") != str(order_id):
        return None
    request_id = order.get("client_order_id")
    if not isinstance(request_id, str) or not request_id.strip() or receipt.get("request_id") != request_id:
        return None
    close = intent_type == "PERP_CLOSE"
    if order.get("reduce_only") is not close or not isinstance(order.get("is_long"), bool):
        return None
    if order.get("status") not in ("FILLED", "EXPIRED", "CANCELED") or (close and order.get("status") != "FILLED"):
        return None
    if order.get("side") != ("SELL" if order["is_long"] == close else "BUY"):
        return None
    for key in ("executed_qty", "avg_price", "cum_quote"):
        value = measured_decimal(order.get(key))
        if value is None or value <= 0:
            return None
    if not order.get("symbol"):
        return None
    return f"aster_perps:{wallet.lower()}:ORDER:{order_id}"


def _withdrawal_identity(receipt: dict, extracted: dict, wallet: str) -> str | None:
    transfer = cash_transfer(extracted)
    withdrawal = extracted.get("aster_withdraw")
    if transfer is None or not isinstance(withdrawal, dict):
        return None
    if receipt.get("execution_id") != transfer["transfer_id"] or extracted.get("order_id") != transfer["transfer_id"]:
        return None
    if transfer["receiver"].lower() != wallet.lower():
        return None
    if not receipt.get("request_id") or receipt["request_id"] != withdrawal.get("client_request_id"):
        return None
    if transfer["transfer_id"] != withdrawal.get("withdraw_id") or transfer["asset"] != withdrawal.get("asset"):
        return None
    if measured_decimal(transfer["gross_amount"]) != measured_decimal(withdrawal.get("amount")):
        return None
    if measured_decimal(transfer["fee_amount"]) != measured_decimal(withdrawal.get("fee")):
        return None
    return f"aster_perps:{wallet.lower()}:WITHDRAW:{transfer['transfer_id']}"
