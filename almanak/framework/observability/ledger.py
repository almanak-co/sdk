"""Transaction Ledger -- structured trade records.

Every executed intent produces a LedgerEntry that captures the trade in a
structured, queryable format.  This replaces grepping through timeline event
``details`` dicts for post-mortem trade analysis.

The ledger is populated by ``StrategyRunner`` after result enrichment and
stored alongside timeline events in the gateway state store.
"""

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any


@dataclass
class LedgerEntry:
    """A single structured trade record.

    Attributes:
        id: Unique entry identifier (UUID).
        cycle_id: Correlation ID for the decide->execute cycle.
        strategy_id: Strategy that produced this trade.
        timestamp: When the trade was executed.
        intent_type: Intent type (SWAP, LP_OPEN, BORROW, etc.).
        token_in: Input token symbol or address.
        amount_in: Input amount (human-readable decimal string).
        token_out: Output token symbol or address.
        amount_out: Output amount (human-readable decimal string).
        effective_price: Execution price (out/in), if applicable.
        slippage_bps: Actual slippage in basis points.
        gas_used: Gas consumed by the transaction.
        gas_usd: Gas cost in USD.
        tx_hash: On-chain transaction hash.
        chain: Chain where the trade executed.
        protocol: Protocol used (e.g. uniswap_v3, aave_v3).
        success: Whether the trade succeeded.
        error: Error message if the trade failed.
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    cycle_id: str = ""
    strategy_id: str = ""
    deployment_id: str = ""  # Phase 4: canonical identity key (VIB-2835)
    execution_mode: str = ""  # Phase 4: "live", "paper", "dry_run" (VIB-2837)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    intent_type: str = ""
    token_in: str = ""
    amount_in: str = ""
    token_out: str = ""
    amount_out: str = ""
    effective_price: str = ""
    slippage_bps: float | None = None
    gas_used: int = 0
    gas_usd: str = ""
    tx_hash: str = ""
    chain: str = ""
    protocol: str = ""
    success: bool = True
    error: str = ""
    extracted_data_json: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary for storage."""
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LedgerEntry":
        """Deserialize from a dictionary."""
        ts = data.get("timestamp")
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        elif ts is None:
            ts = datetime.now(UTC)
        return cls(
            id=data.get("id", str(uuid.uuid4())),
            cycle_id=data.get("cycle_id", ""),
            strategy_id=data.get("strategy_id", ""),
            deployment_id=data.get("deployment_id", ""),
            execution_mode=data.get("execution_mode", ""),
            timestamp=ts,
            intent_type=data.get("intent_type", ""),
            token_in=data.get("token_in", ""),
            amount_in=data.get("amount_in", ""),
            token_out=data.get("token_out", ""),
            amount_out=data.get("amount_out", ""),
            effective_price=data.get("effective_price", ""),
            slippage_bps=data.get("slippage_bps"),
            gas_used=data.get("gas_used", 0),
            gas_usd=data.get("gas_usd", ""),
            tx_hash=data.get("tx_hash", ""),
            chain=data.get("chain", ""),
            protocol=data.get("protocol", ""),
            success=data.get("success", True),
            error=data.get("error", ""),
            extracted_data_json=data.get("extracted_data_json", ""),
        )


def build_ledger_entry(
    *,
    strategy_id: str,
    cycle_id: str,
    intent: Any,
    result: Any,
    chain: str = "",
    success: bool = True,
    error: str = "",
) -> LedgerEntry:
    """Build a LedgerEntry from an intent and its execution result.

    Extracts structured trade data from the enriched result object
    (swap_amounts, lp_close_data, etc.) so callers don't need to
    know the extraction details.
    """
    intent_type = ""
    if hasattr(intent, "intent_type"):
        it = intent.intent_type
        intent_type = it.value if hasattr(it, "value") else str(it)

    token_in = ""
    amount_in = ""
    token_out = ""
    amount_out = ""
    effective_price = ""
    slippage_bps: float | None = None
    protocol = getattr(intent, "protocol", "") or ""

    # Extract from SwapAmounts (swap, LP close, etc.)
    swap_amounts = getattr(result, "swap_amounts", None) if result else None
    if swap_amounts:
        token_in = swap_amounts.token_in or getattr(intent, "from_token", "") or ""
        token_out = swap_amounts.token_out or getattr(intent, "to_token", "") or ""
        amount_in = str(swap_amounts.amount_in_decimal) if swap_amounts.amount_in_decimal else ""
        amount_out = str(swap_amounts.amount_out_decimal) if swap_amounts.amount_out_decimal else ""
        if swap_amounts.effective_price is not None:
            effective_price = str(swap_amounts.effective_price)
        slippage_bps = swap_amounts.slippage_bps
    else:
        # Fallback: extract tokens from the intent itself.
        # Supports swap-style (from_token/to_token), lending (borrow_token/supply_token),
        # and generic (token/amount) intents.
        token_in = (
            getattr(intent, "from_token", "")
            or getattr(intent, "borrow_token", "")
            or getattr(intent, "supply_token", "")
            or getattr(intent, "token", "")
            or ""
        )
        token_out = getattr(intent, "to_token", "") or ""
        amt = (
            getattr(intent, "amount", None)
            or getattr(intent, "borrow_amount", None)
            or getattr(intent, "supply_amount", None)
            or getattr(intent, "amount_usd", None)
        )
        if amt is not None:
            amount_in = str(amt)

    # Extract tx details from result
    tx_hash = ""
    gas_used = 0
    gas_usd = ""
    if result:
        if hasattr(result, "transaction_results") and result.transaction_results:
            first_tx = result.transaction_results[0]
            tx_hash = first_tx.tx_hash or ""
        gas_used = getattr(result, "total_gas_used", 0) or 0
        gas_cost = getattr(result, "gas_cost_usd", None)
        if gas_cost is not None:
            gas_usd = str(gas_cost)

    if not success and not error and result:
        error = getattr(result, "error", "") or ""

    # Serialize extracted_data with type tags for round-trip fidelity
    extracted_data_json = ""
    if result and hasattr(result, "extracted_data") and result.extracted_data:
        extracted_data_json = serialize_extracted_data(result.extracted_data)

    # Capture all tx results for multi-action bundles (approve+swap, etc.)
    if (
        extracted_data_json
        and result
        and hasattr(result, "transaction_results")
        and result.transaction_results
        and len(result.transaction_results) > 1
    ):
        try:
            parsed = json.loads(extracted_data_json)
            parsed["all_tx_results"] = [
                {
                    "tx_hash": getattr(tr, "tx_hash", "") or "",
                    "gas_used": getattr(tr, "gas_used", 0) or 0,
                    "success": getattr(tr, "success", True),
                }
                for tr in result.transaction_results
            ]
            extracted_data_json = json.dumps(parsed)
        except (json.JSONDecodeError, TypeError):
            pass  # Keep existing serialization on failure

    return LedgerEntry(
        cycle_id=cycle_id,
        strategy_id=strategy_id,
        intent_type=intent_type,
        token_in=token_in,
        amount_in=amount_in,
        token_out=token_out,
        amount_out=amount_out,
        effective_price=effective_price,
        slippage_bps=slippage_bps,
        gas_used=gas_used,
        gas_usd=gas_usd,
        tx_hash=tx_hash,
        chain=chain,
        protocol=protocol,
        success=success,
        error=error,
        extracted_data_json=extracted_data_json,
    )


def serialize_extracted_data(extracted_data: dict[str, Any]) -> str:
    """Serialize extracted_data dict with type tags for round-trip fidelity.

    Each value that has a ``to_dict()`` method (the typed dataclasses like
    SwapAmounts, LPOpenData, PerpData, etc.) is serialized with a ``_type``
    tag so ``deserialize_extracted_data()`` can reconstruct the original type.
    """
    serializable: dict[str, Any] = {}
    for key, val in extracted_data.items():
        if hasattr(val, "to_dict"):
            d = val.to_dict()
            d["_type"] = type(val).__name__
            serializable[key] = d
        elif isinstance(val, Decimal):
            serializable[key] = {"_type": "Decimal", "value": str(val)}
        elif isinstance(val, datetime):
            serializable[key] = {"_type": "datetime", "value": val.isoformat()}
        elif isinstance(val, Enum):
            serializable[key] = {"_type": "Enum", "name": type(val).__name__, "value": val.value}
        else:
            serializable[key] = val
    try:
        return json.dumps(serializable, default=str)
    except (TypeError, ValueError):
        return ""


def deserialize_extracted_data(json_str: str) -> dict[str, Any]:
    """Deserialize extracted_data JSON back into typed objects where possible.

    Values tagged with ``_type`` are reconstructed into their original types.
    Unknown types are returned as plain dicts.
    """
    if not json_str:
        return {}
    try:
        data = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return {}

    # Lazy import to avoid circular deps
    from almanak.framework.execution.extracted_data import (
        BorrowData,
        LPCloseData,
        LPOpenData,
        PerpData,
        StakeData,
        SupplyData,
        SwapAmounts,
    )

    type_map: dict[str, type] = {
        "SwapAmounts": SwapAmounts,
        "LPOpenData": LPOpenData,
        "LPCloseData": LPCloseData,
        "BorrowData": BorrowData,
        "SupplyData": SupplyData,
        "PerpData": PerpData,
        "StakeData": StakeData,
    }

    result: dict[str, Any] = {}
    for key, val in data.items():
        if not isinstance(val, dict) or "_type" not in val:
            result[key] = val
            continue

        type_name = val.pop("_type")
        if type_name == "Decimal":
            result[key] = Decimal(val["value"])
        elif type_name == "datetime":
            result[key] = datetime.fromisoformat(val["value"])
        elif type_name == "Enum":
            result[key] = val  # Return as dict; caller knows the enum type
        elif type_name in type_map:
            try:
                cls = type_map[type_name]
                # Convert string values back to appropriate types
                result[key] = _reconstruct_dataclass(cls, val)
            except (TypeError, ValueError):
                val["_type"] = type_name
                result[key] = val
        else:
            val["_type"] = type_name
            result[key] = val

    return result


def _reconstruct_dataclass(cls: type, data: dict[str, Any]) -> Any:
    """Reconstruct a frozen dataclass from a dict of string values."""
    import dataclasses
    import inspect

    fields = {f.name: f for f in dataclasses.fields(cls)}
    sig = inspect.signature(cls)
    kwargs: dict[str, Any] = {}

    for name, _param in sig.parameters.items():
        if name not in data:
            continue
        val = data[name]
        if val is None:
            kwargs[name] = None
            continue

        f = fields.get(name)
        if f is None:
            continue

        # Infer type from field annotation string
        ann = str(f.type)
        if "Decimal" in ann:
            kwargs[name] = Decimal(val)
        elif "int" in ann and "str" not in ann:
            try:
                kwargs[name] = int(val)
            except (ValueError, TypeError):
                kwargs[name] = val
        elif "float" in ann:
            try:
                kwargs[name] = float(val)
            except (ValueError, TypeError):
                kwargs[name] = val
        elif "bool" in ann:
            if isinstance(val, bool):
                kwargs[name] = val
            elif isinstance(val, str):
                kwargs[name] = val.lower() in ("true", "1", "yes")
            else:
                kwargs[name] = bool(val)
        else:
            kwargs[name] = val

    return cls(**kwargs)
