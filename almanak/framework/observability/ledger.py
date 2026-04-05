"""Transaction Ledger -- structured trade records.

Every executed intent produces a LedgerEntry that captures the trade in a
structured, queryable format.  This replaces grepping through timeline event
``details`` dicts for post-mortem trade analysis.

The ledger is populated by ``StrategyRunner`` after result enrichment and
stored alongside timeline events in the gateway state store.
"""

import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
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
    )
