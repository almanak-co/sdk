"""Backward-compat re-export shim + ExecutionResult — VIB-4062.

The market-snapshot DTOs (``TokenBalance``, ``PriceData``, ``PriceOracle``,
``RSIProvider``, ``BalanceProvider``) now live in
``almanak.framework.market.models``. This module re-exports them so existing
deep imports keep working through commit 5; commit 6 deletes the re-exports.

``ExecutionResult`` continues to live here — it is a strategy-execution
result object, not a market-snapshot return type.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..intents import StepResult
from ..intents.vocabulary import AnyIntent
from ..market.models import (
    BalanceProvider,
    PriceData,
    PriceOracle,
    RSIProvider,
    TokenBalance,
)
from ..models.reproduction_bundle import ActionBundle


@dataclass
class ExecutionResult:
    """Result of strategy execution.

    Attributes:
        intent: The intent that was executed (or None if HOLD)
        action_bundle: The compiled action bundle (or None)
        state_machine_result: Final state machine step result
        success: Whether execution was successful
        error: Error message if failed
        execution_time_ms: Time taken for execution in milliseconds
    """

    intent: AnyIntent | None
    action_bundle: ActionBundle | None = None
    state_machine_result: StepResult | None = None
    success: bool = False
    error: str | None = None
    execution_time_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "intent_type": self.intent.intent_type.value if self.intent else None,
            "intent_id": self.intent.intent_id if self.intent else None,
            "action_bundle": self.action_bundle.to_dict() if self.action_bundle else None,
            "success": self.success,
            "error": self.error,
            "execution_time_ms": self.execution_time_ms,
        }


__all__ = [
    "TokenBalance",
    "PriceData",
    "PriceOracle",
    "RSIProvider",
    "BalanceProvider",
    "ExecutionResult",
]
