"""EvmExecutionStrategy — wraps existing EVM execution code behind ChainExecutionStrategy.

This is a thin adapter that delegates to GatewayExecutionOrchestrator (strategy
containers) or ExecutionOrchestrator (gateway-internal). Zero behavior change —
purely adapts the existing EVM path to the ChainExecutionStrategy interface.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from almanak.framework.execution.chain_strategy import ChainExecutionStrategy
from almanak.framework.execution.outcome import ExecutionOutcome

logger = logging.getLogger(__name__)


class EvmExecutionStrategy(ChainExecutionStrategy):
    """EVM execution strategy — delegates to existing orchestrators.

    Wraps either a GatewayExecutionOrchestrator or an ExecutionOrchestrator
    so they can be used through the ChainExecutionStrategy interface.

    Args:
        orchestrator: A GatewayExecutionOrchestrator or ExecutionOrchestrator instance.
        wallet_address: The EVM wallet address.
    """

    chain_family: str = "EVM"

    def __init__(self, orchestrator: Any, wallet_address: str = ""):
        self._orchestrator = orchestrator
        self.wallet_address = wallet_address or getattr(orchestrator, "wallet_address", "") or ""

    async def execute_actions(
        self,
        actions: list[Any],
        context: dict[str, Any] | None = None,
    ) -> ExecutionOutcome:
        """Execute EVM actions through the wrapped orchestrator.

        Args:
            actions: Action bundles (typically a single ActionBundle).
            context: Optional execution context dict.

        Returns:
            ExecutionOutcome converted from the orchestrator's result type.
        """
        if not actions:
            return ExecutionOutcome(success=True, chain_family="EVM")

        # Take the first action bundle — the orchestrator expects a single bundle
        bundle = actions[0]
        ctx = context or {}

        result = await self._orchestrator.execute(
            action_bundle=bundle,
            strategy_id=ctx.get("strategy_id", ""),
            intent_id=ctx.get("intent_id", ""),
            dry_run=ctx.get("dry_run", False),
            simulation_enabled=ctx.get("simulation_enabled", True),
        )

        # Convert to ExecutionOutcome via to_outcome() if available
        if hasattr(result, "to_outcome"):
            return result.to_outcome()

        # Fallback: manual conversion for older result types
        return ExecutionOutcome(
            success=getattr(result, "success", False),
            tx_ids=getattr(result, "tx_hashes", []),
            receipts=getattr(result, "receipts", []),
            total_fee_native=Decimal(getattr(result, "total_gas_used", 0)),
            error=getattr(result, "error", None),
            chain_family="EVM",
            position_id=getattr(result, "position_id", None),
            swap_amounts=getattr(result, "swap_amounts", None),
            lp_close_data=getattr(result, "lp_close_data", None),
            extracted_data=getattr(result, "extracted_data", {}),
            extraction_warnings=getattr(result, "extraction_warnings", []),
        )

    async def check_connection(self) -> bool:
        """Check EVM RPC connectivity through the orchestrator."""
        if hasattr(self._orchestrator, "check_connection"):
            return await self._orchestrator.check_connection()
        # GatewayExecutionOrchestrator doesn't have check_connection — assume OK
        return True
