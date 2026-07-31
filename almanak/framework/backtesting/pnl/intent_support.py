"""Exhaustive PnL-engine decisions for the canonical intent vocabulary."""

from __future__ import annotations

from enum import StrEnum

from almanak.core.intent_types import IntentType

__all__ = [
    "BACKTEST_INTENT_DISPOSITIONS",
    "BacktestIntentDisposition",
    "GENERIC_SIMULATED_INTENT_TYPES",
]


class BacktestIntentDisposition(StrEnum):
    """How the PnL engine treats a canonical intent after adapter dispatch."""

    GENERIC_SIMULATED = "generic_simulated"
    REFUSED = "refused"
    PLACEHOLDER_NOT_APPLICABLE = "placeholder_not_applicable"


# Keep every canonical member spelled out.  The import-time coverage assertion
# below turns a newly-added IntentType into a required backtesting design
# decision instead of allowing it to drift into an implicit default lane.
BACKTEST_INTENT_DISPOSITIONS: dict[IntentType, BacktestIntentDisposition] = {
    IntentType.SWAP: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.LP_OPEN: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.LP_CLOSE: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.BORROW: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.REPAY: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.SUPPLY: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.WITHDRAW: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.PERP_OPEN: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.PERP_CLOSE: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.PERP_CANCEL_ORDER: BacktestIntentDisposition.REFUSED,
    IntentType.PERP_WITHDRAW: BacktestIntentDisposition.REFUSED,
    IntentType.BRIDGE: BacktestIntentDisposition.REFUSED,
    IntentType.ENSURE_BALANCE: BacktestIntentDisposition.REFUSED,
    IntentType.FLASH_LOAN: BacktestIntentDisposition.REFUSED,
    IntentType.STAKE: BacktestIntentDisposition.REFUSED,
    IntentType.UNSTAKE: BacktestIntentDisposition.REFUSED,
    IntentType.HOLD: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.PREDICTION_BUY: BacktestIntentDisposition.REFUSED,
    IntentType.PREDICTION_SELL: BacktestIntentDisposition.REFUSED,
    IntentType.PREDICTION_REDEEM: BacktestIntentDisposition.REFUSED,
    IntentType.VAULT_DEPOSIT: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.VAULT_REDEEM: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.VAULT_REALLOCATE: BacktestIntentDisposition.REFUSED,
    IntentType.VAULT_MANAGE: BacktestIntentDisposition.REFUSED,
    IntentType.LP_COLLECT_FEES: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.WRAP_NATIVE: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.UNWRAP_NATIVE: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.DELEVERAGE: BacktestIntentDisposition.GENERIC_SIMULATED,
    IntentType.LIQUIDATE: BacktestIntentDisposition.PLACEHOLDER_NOT_APPLICABLE,
    IntentType.OPEN_CDP: BacktestIntentDisposition.PLACEHOLDER_NOT_APPLICABLE,
    IntentType.MINT_STABLE: BacktestIntentDisposition.PLACEHOLDER_NOT_APPLICABLE,
    IntentType.REPAY_STABLE: BacktestIntentDisposition.PLACEHOLDER_NOT_APPLICABLE,
    IntentType.CLOSE_CDP: BacktestIntentDisposition.PLACEHOLDER_NOT_APPLICABLE,
}

_missing = frozenset(IntentType) - BACKTEST_INTENT_DISPOSITIONS.keys()
_extra = BACKTEST_INTENT_DISPOSITIONS.keys() - frozenset(IntentType)
if _missing or _extra:  # pragma: no cover - fails immediately when the enum drifts
    raise RuntimeError(
        "BACKTEST_INTENT_DISPOSITIONS must classify every canonical IntentType; "
        f"missing={sorted(intent.value for intent in _missing)!r}, "
        f"extra={sorted(intent.value for intent in _extra)!r}"
    )

GENERIC_SIMULATED_INTENT_TYPES: frozenset[IntentType] = frozenset(
    intent_type
    for intent_type, disposition in BACKTEST_INTENT_DISPOSITIONS.items()
    if disposition is BacktestIntentDisposition.GENERIC_SIMULATED
)
