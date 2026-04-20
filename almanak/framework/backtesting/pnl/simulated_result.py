"""Simulated execution result for PnL backtester `on_intent_executed` callback.

VIB-2916: Strategies expect their `on_intent_executed(intent, success, result)`
callback to receive a result object with attributes like `position_id`,
`swap_amounts`, and `extracted_data` — populated in production by
`ResultEnricher` after orchestrator execution. The PnL backtester does not
execute on-chain so it has no real receipts to enrich from.

`SimulatedExecutionResult` mirrors the subset of `ExecutionResult` fields that
strategies typically read in their callback, populated from the simulated
`TradeRecord` (which now carries the real `SimulatedPosition.position_id` from
the engine via `SimulatedFill.to_trade_record()`). This lets stateful LP
strategies advance their state machine during backtests AND later close those
positions via `Intent.lp_close(position_id=self._position_id)` because the id
they receive matches the id the engine tracks.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.execution.extracted_data import SwapAmounts

if TYPE_CHECKING:
    from almanak.framework.backtesting.models import TradeRecord

# Intent type names (matches IntentType.value) that produce an LP position.
_LP_OPEN_INTENT_NAMES: frozenset[str] = frozenset({"LP_OPEN"})

# Intent type names that produce swap amounts.
_SWAP_INTENT_NAMES: frozenset[str] = frozenset({"SWAP"})


@dataclass
class SimulatedExecutionResult:
    """Simulated `ExecutionResult` for PnL backtester strategy callbacks.

    Strategies access fields with `result.position_id`, `result.swap_amounts`,
    `result.success`, and so on. This shape matches `ExecutionResult` closely
    enough that strategy callbacks written for production execution work
    unchanged in the PnL backtester.

    Attributes:
        success: Whether the simulated trade succeeded
        position_id: Real `SimulatedPosition.position_id` for LP_OPEN intents
            (e.g. ``LP_uniswap_v3_USDC_WETH_<ts>``); matches what the engine
            tracks so `Intent.lp_close(position_id=...)` will find it.
            None for non-LP intents.
        swap_amounts: Populated for SWAP intents from the TradeRecord
        extracted_data: Free-form dict, mirroring ExecutionResult contract
        error: Failure message (None on success)
        trade_record: The underlying simulated TradeRecord (for advanced
            strategies that need access to fee/slippage/PnL details)
    """

    success: bool
    position_id: int | str | None = None
    swap_amounts: SwapAmounts | None = None
    extracted_data: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    trade_record: TradeRecord | None = None


def is_lp_open_intent(intent: Any) -> bool:
    """Return True if ``intent`` is an LP_OPEN-flavoured intent.

    Accepts both real `Intent` instances (`intent_type` is `IntentType` enum)
    and AlmanakCode-generated stand-ins where `intent_type` is a plain string.
    """
    return _intent_type_name(intent) in _LP_OPEN_INTENT_NAMES


def build_simulated_result(
    intent: Any,
    trade_record: TradeRecord | None,
    success: bool,
    error: str | None = None,
) -> SimulatedExecutionResult:
    """Build a `SimulatedExecutionResult` from a simulated trade.

    For LP_OPEN intents the real `SimulatedPosition.position_id` is read from
    `trade_record.position_id` (populated by `SimulatedFill.to_trade_record()`).
    This guarantees the id surfaced to the strategy matches the id the engine
    tracks internally so a later `Intent.lp_close(position_id=...)` resolves
    against the open position instead of a synthetic placeholder.

    Args:
        intent: The executed intent (for type-based field population)
        trade_record: TradeRecord from successful execution, None on failure
        success: Whether the trade succeeded
        error: Failure reason (when success=False)

    Returns:
        Populated SimulatedExecutionResult that mirrors the ExecutionResult
        attributes strategies read in their on_intent_executed callback.
    """
    intent_type_name = _intent_type_name(intent)

    position_id: int | str | None = None
    if success and intent_type_name in _LP_OPEN_INTENT_NAMES and trade_record is not None:
        position_id = trade_record.position_id

    swap_amounts: SwapAmounts | None = None
    if success and intent_type_name in _SWAP_INTENT_NAMES and trade_record is not None:
        swap_amounts = _build_swap_amounts(trade_record)

    return SimulatedExecutionResult(
        success=success,
        position_id=position_id,
        swap_amounts=swap_amounts,
        error=error,
        trade_record=trade_record,
    )


def _intent_type_name(intent: Any) -> str:
    """Best-effort extraction of an intent's type name as a string."""
    intent_type = getattr(intent, "intent_type", None)
    if intent_type is None:
        return ""
    value = getattr(intent_type, "value", intent_type)
    return str(value)


def _build_swap_amounts(trade_record: TradeRecord) -> SwapAmounts | None:
    """Build a SwapAmounts from a simulated TradeRecord.

    The PnL simulator records `actual_amount_in` / `actual_amount_out` as
    human-readable Decimals. Raw integer (wei-equivalent) amounts are not
    available without token decimals so `amount_in` / `amount_out` are set
    to 0 and only the `_decimal` fields (and their `_human` aliases) carry
    real values. Strategies that compare against on-chain integer balances
    will see 0 in backtests — use the Decimal fields instead.
    """
    actual_in = trade_record.actual_amount_in
    actual_out = trade_record.actual_amount_out
    if actual_in is None or actual_out is None:
        return None

    tokens = trade_record.tokens or []
    token_in = tokens[0] if len(tokens) > 0 else None
    token_out = tokens[1] if len(tokens) > 1 else None

    effective_price: Decimal | None = None
    if actual_in > 0:
        effective_price = actual_out / actual_in

    return SwapAmounts(
        amount_in=0,
        amount_out=0,
        amount_in_decimal=actual_in,
        amount_out_decimal=actual_out,
        effective_price=effective_price,
        slippage_bps=None,
        token_in=token_in,
        token_out=token_out,
    )
