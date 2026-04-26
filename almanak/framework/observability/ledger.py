"""Transaction Ledger -- structured trade records.

Every executed intent produces a LedgerEntry that captures the trade in a
structured, queryable format.  This replaces grepping through timeline event
``details`` dicts for post-mortem trade analysis.

The ledger is populated by ``StrategyRunner`` after result enrichment and
stored alongside timeline events in the gateway state store.

Phase 5k -- helper extraction layout
------------------------------------
``build_ledger_entry`` is composed from small phase helpers that each
return a piece of the final ``LedgerEntry``.  The helpers run in a fixed
order; ordering is NOT load-bearing (unlike the position-events pipeline)
because none of the helpers depend on output of earlier phases:

    alpha  _extract_intent_type          : enum-or-string dispatch
    beta   _extract_tokens_and_amounts   : dispatch between three sub-helpers:
             _extract_from_swap_amounts       (SwapAmounts + intent fallback
                                               for empty token sides)
             _extract_from_lp_open           (LP_OPEN: LPOpenData amounts +
                                               intent token0/token1 lookup)
             _extract_from_intent_fallback    (intent-attr precedence chain
                                               from_token > borrow_token >
                                               supply_token > token;
                                               amount > borrow_amount >
                                               supply_amount > amount_usd).
    gamma  _extract_tx_and_gas           : first tx_hash + total gas + gas USD
    delta  _coalesce_error               : failure + empty-error -> result.error
    epsilon _build_extracted_data_json   : serialize + multi-tx augmentation

The SQLite INSERT at ``backends/sqlite.py:2291-2322`` names 21 columns and
pairs each with a specific ``entry.<attribute>`` read.  The refactor
preserves every LedgerEntry field-value semantic byte-identical so the
write contract is unaffected.
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


def _extract_intent_type(intent: Any) -> str:
    """Phase alpha -- normalize intent_type to a string.

    Supports both enum-like payloads (``.value`` present) and raw strings.
    Missing ``intent_type`` attribute maps to ``""``.
    """
    if not hasattr(intent, "intent_type"):
        return ""
    it = intent.intent_type
    return it.value if hasattr(it, "value") else str(it)


# Tuple returned by the token/amount phase -- kept as a plain tuple to avoid
# another tiny dataclass and to match the positional assignment style in
# the final LedgerEntry(...) constructor.
_TokensAndAmounts = tuple[str, str, str, str, str, float | None]


def _extract_from_swap_amounts(swap_amounts: Any, intent: Any) -> _TokensAndAmounts:
    """Phase beta-primary -- all fields from ``result.swap_amounts``.

    Token sides fall back to ``intent.from_token`` / ``intent.to_token`` when
    the swap_amounts side is falsy (empty string). Amount sides use
    ``is not None`` checks instead of truthiness so measured-zero amounts
    (``Decimal("0")``) are preserved as ``"0"`` rather than silently dropped
    to ``""`` -- issue #1768 (sibling of #1709 / #1710 fixed in #1751).

    The ``amount_*_decimal_resolved`` flags on ``SwapAmounts`` disambiguate
    a measured ``Decimal(0)`` from an unresolvable-decimals sentinel —
    historically both were recorded as ``"0"`` on the ledger, silently
    corrupting PnL attribution and portfolio accounting for tokens whose
    decimals failed to resolve (issue #1778, Codex finding on PR #1774).
    An unresolved amount now falls through to the same ``""`` that the
    truthiness path used to produce before #1768, but without conflating
    the measured-zero case. Parsers that do not populate the flag default
    to ``True`` (``SwapAmounts`` default) -- preserves existing behavior
    byte-for-byte for the many connectors that have not been audited yet.
    """
    token_in = swap_amounts.token_in or getattr(intent, "from_token", "") or ""
    token_out = swap_amounts.token_out or getattr(intent, "to_token", "") or ""
    amt_in = getattr(swap_amounts, "amount_in_decimal", None)
    amt_out = getattr(swap_amounts, "amount_out_decimal", None)
    amt_in_resolved = getattr(swap_amounts, "amount_in_decimal_resolved", True)
    amt_out_resolved = getattr(swap_amounts, "amount_out_decimal_resolved", True)
    amount_in = str(amt_in) if amt_in is not None and amt_in_resolved else ""
    amount_out = str(amt_out) if amt_out is not None and amt_out_resolved else ""
    effective_price = str(swap_amounts.effective_price) if swap_amounts.effective_price is not None else ""
    return (
        token_in,
        token_out,
        amount_in,
        amount_out,
        effective_price,
        swap_amounts.slippage_bps,
    )


def _extract_from_intent_fallback(intent: Any) -> _TokensAndAmounts:
    """Phase beta-fallback -- no swap_amounts; walk the intent-attr chain.

    Token precedence:
        ``from_token > borrow_token > supply_token > token``
    Amount precedence:
        ``amount > borrow_amount > supply_amount > amount_usd``

    Supports swap-style (from_token/to_token), lending
    (borrow_token/supply_token) and generic (token/amount) intents.
    """
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
    amount_in = str(amt) if amt is not None else ""
    return (token_in, token_out, amount_in, "", "", None)


def _extract_from_lp_open(intent: Any, result: Any) -> _TokensAndAmounts:
    """Phase beta-lp-open -- LP_OPEN has no swap_amounts; pull amounts from
    ``LPOpenData`` in ``result.extracted_data`` and tokens from the intent.

    Field mapping:
    - ``token_in``  : ``intent.token0`` -> ``intent.from_token`` -> ``""``
    - ``token_out`` : ``intent.token1`` -> ``intent.to_token``  -> ``""``
    - ``amount_in`` : ``LPOpenData.amount0`` (raw int) is the on-chain actual
                      deposit for token0; falls back to ``intent.amount0``
                      (Decimal from the intent, the user-requested amount).
    - ``amount_out``: same logic for ``amount1``.

    ``LPOpenData.amount0`` / ``amount1`` are raw integer values (smallest
    unit).  We store them as strings directly so that accounting consumers
    see the on-chain amount.  When only the intent amounts are available the
    human-readable Decimal string is more useful.
    """
    # Token resolution: prefer explicit token0/token1 attrs (test/legacy callers),
    # then parse symbols from the pool string (e.g. "WETH/USDC"), then fallback.
    token_in = getattr(intent, "token0", "") or getattr(intent, "from_token", "") or ""
    token_out = getattr(intent, "token1", "") or getattr(intent, "to_token", "") or ""
    if not token_in or not token_out:
        pool_str = (getattr(intent, "pool", "") or "").strip()
        if "/" in pool_str:
            pool_parts = [p.strip() for p in pool_str.split("/")]
            if not token_in and pool_parts:
                token_in = pool_parts[0]
            if not token_out and len(pool_parts) > 1:
                token_out = pool_parts[1]

    # Prefer on-chain actuals from LPOpenData; fall back to intent amounts.
    extracted_data = getattr(result, "extracted_data", None) or {} if result else {}
    lp_open_data = extracted_data.get("lp_open_data") if isinstance(extracted_data, dict) else None

    if lp_open_data is not None:
        # Per-side fallback: if one side is missing from LPOpenData, fall back
        # to the corresponding intent amount rather than leaving it empty.
        raw0 = getattr(lp_open_data, "amount0", None)
        raw1 = getattr(lp_open_data, "amount1", None)
        if raw0 is None:
            raw0 = getattr(intent, "amount0", None)
        if raw1 is None:
            raw1 = getattr(intent, "amount1", None)
        amount_in = str(raw0) if raw0 is not None else ""
        amount_out = str(raw1) if raw1 is not None else ""
    else:
        intent_amt0 = getattr(intent, "amount0", None)
        intent_amt1 = getattr(intent, "amount1", None)
        amount_in = str(intent_amt0) if intent_amt0 is not None else ""
        amount_out = str(intent_amt1) if intent_amt1 is not None else ""

    return (token_in, token_out, amount_in, amount_out, "", None)


def _extract_tokens_and_amounts(intent: Any, result: Any) -> _TokensAndAmounts:
    """Phase beta -- dispatch between SwapAmounts, LP_OPEN, and intent-attr fallback.

    A truthy ``result.swap_amounts`` drives every field (used by SWAP,
    LP_CLOSE, and anything whose receipt parser emits SwapAmounts). LP_OPEN
    intents carry amounts in ``LPOpenData`` (``result.extracted_data
    ["lp_open_data"]``) and have no ``from_token`` / ``to_token``, so they
    get a dedicated extraction path.  Everything else walks the intent-attr
    precedence chain.
    """
    swap_amounts = getattr(result, "swap_amounts", None) if result else None
    if swap_amounts:
        return _extract_from_swap_amounts(swap_amounts, intent)
    if _extract_intent_type(intent) == "LP_OPEN":
        return _extract_from_lp_open(intent, result)
    return _extract_from_intent_fallback(intent)


def _extract_tx_and_gas(result: Any) -> tuple[str, int, str]:
    """Phase gamma -- (tx_hash, gas_used, gas_usd) from the result envelope.

    - ``tx_hash`` = ``result.transaction_results[0].tx_hash or ""`` when the
      list is non-empty; empty-list or missing attr -> ``""``.
    - ``gas_used`` = ``result.total_gas_used or 0`` (None coalesces to 0).
    - ``gas_usd`` = ``str(result.gas_cost_usd)`` when not None; else ``""``.
    """
    if not result:
        return ("", 0, "")

    tx_hash = ""
    tx_results = getattr(result, "transaction_results", None)
    if tx_results:
        tx_hash = tx_results[0].tx_hash or ""

    gas_used = getattr(result, "total_gas_used", 0) or 0
    gas_cost = getattr(result, "gas_cost_usd", None)
    gas_usd = str(gas_cost) if gas_cost is not None else ""
    return (tx_hash, gas_used, gas_usd)


def _coalesce_error(success: bool, error: str, result: Any) -> str:
    """Phase delta -- if the caller said "failed" and supplied no error
    string, fall back to ``result.error`` (coalescing None -> "").

    Caller-supplied error always wins; success=True skips the branch.
    """
    if not success and not error and result:
        return getattr(result, "error", "") or ""
    return error


def _build_extracted_data_json(result: Any) -> str:
    """Phase epsilon -- serialize ``result.extracted_data`` with type tags,
    and for multi-tx bundles augment the payload with an ``all_tx_results``
    array capturing every leg's hash/gas/success.

    Returns ``""`` when the result lacks extracted_data (attribute absent or
    dict empty). Single-tx results skip the augmentation branch.

    Defensive ``try/except`` around the augmentation keeps the original
    serialization on any JSON decode failure (today unreachable from prod,
    but the safety net is cheap and matches the pre-refactor contract).
    """
    if not result or not getattr(result, "extracted_data", None):
        return ""

    extracted_data_json = serialize_extracted_data(result.extracted_data)

    tx_results = getattr(result, "transaction_results", None) or []
    if not extracted_data_json or len(tx_results) <= 1:
        return extracted_data_json

    try:
        parsed = json.loads(extracted_data_json)
        parsed["all_tx_results"] = [
            {
                "tx_hash": getattr(tr, "tx_hash", "") or "",
                "gas_used": getattr(tr, "gas_used", 0) or 0,
                "success": getattr(tr, "success", True),
            }
            for tr in tx_results
        ]
        return json.dumps(parsed)
    except (json.JSONDecodeError, TypeError):
        return extracted_data_json  # keep existing serialization on failure


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
    know the extraction details. Sequences the phase helpers
    alpha -> beta -> gamma -> delta -> epsilon; see module docstring.
    """
    intent_type = _extract_intent_type(intent)
    (
        token_in,
        token_out,
        amount_in,
        amount_out,
        effective_price,
        slippage_bps,
    ) = _extract_tokens_and_amounts(intent, result)
    tx_hash, gas_used, gas_usd = _extract_tx_and_gas(result)
    final_error = _coalesce_error(success, error, result)
    extracted_data_json = _build_extracted_data_json(result)
    protocol = getattr(intent, "protocol", "") or ""

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
        error=final_error,
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
