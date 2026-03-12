"""Solana execution adapter for StrategyRunner compatibility.

Wraps SolanaExecutionPlanner to present the same execute(action_bundle, context)
interface that StrategyRunner expects, returning a result compatible with
ExecutionResult / GatewayExecutionResult.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from almanak.framework.execution.extracted_data import LPCloseData, SwapAmounts
from almanak.framework.execution.solana.planner import SolanaExecutionPlanner

logger = logging.getLogger(__name__)


@dataclass
class SolanaExecutionResult:
    """Execution result compatible with StrategyRunner expectations.

    Provides the same interface as GatewayExecutionResult / ExecutionResult
    so the runner can access .success, .error, .total_gas_used,
    and .transaction_results without knowing the chain family.
    """

    success: bool
    phase: str = "COMPLETE"
    tx_hashes: list[str] = field(default_factory=list)
    total_gas_used: int = 0
    total_gas_cost_wei: int = 0
    receipts: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None
    fee_lamports: int = 0

    # Enrichment fields (populated by ResultEnricher)
    position_id: int | None = None
    swap_amounts: SwapAmounts | None = None
    lp_close_data: LPCloseData | None = None
    extracted_data: dict[str, Any] = field(default_factory=dict)
    extraction_warnings: list[str] = field(default_factory=list)

    @property
    def transaction_results(self) -> list:
        """Compatibility property for StrategyRunner and ResultEnricher."""
        if not self.success or not self.receipts:
            return []

        from almanak.framework.execution.interfaces import TransactionReceipt
        from almanak.framework.execution.orchestrator import TransactionResult

        results = []
        for i, receipt_data in enumerate(self.receipts):
            tx_hash = self.tx_hashes[i] if i < len(self.tx_hashes) else ""
            logs = receipt_data.get("logs", [])
            fee = receipt_data.get("fee_lamports", 0)

            tx_receipt = TransactionReceipt(
                tx_hash=tx_hash,
                block_number=receipt_data.get("slot", 0),
                block_hash="",
                gas_used=fee,
                effective_gas_price=1,
                status=1 if receipt_data.get("success", True) else 0,
                logs=logs,
            )

            results.append(
                TransactionResult(
                    success=receipt_data.get("success", True),
                    tx_hash=tx_hash,
                    gas_used=fee,
                    logs=logs,
                    receipt=tx_receipt,
                )
            )
        return results


class SolanaOrchestratorAdapter:
    """Adapter that wraps SolanaExecutionPlanner for StrategyRunner.

    Presents the same execute(action_bundle, context) interface as
    ExecutionOrchestrator / GatewayExecutionOrchestrator.

    Args:
        wallet_address: Solana wallet public key (base58).
        rpc_url: Solana RPC endpoint URL.
        private_key: Ed25519 private key (base58 or hex seed).
    """

    def __init__(
        self,
        wallet_address: str,
        rpc_url: str,
        private_key: str,
        chain: str = "solana",
    ) -> None:
        self.chain = chain
        self._planner = SolanaExecutionPlanner(
            wallet_address=wallet_address,
            rpc_url=rpc_url,
            private_key=private_key,
        )

    async def execute(
        self,
        action_bundle: Any,
        context: Any | None = None,
        **kwargs: Any,
    ) -> SolanaExecutionResult:
        """Execute an action bundle via SolanaExecutionPlanner.

        Args:
            action_bundle: ActionBundle with serialized Solana transactions.
            context: Optional ExecutionContext with dry_run flag.

        Returns:
            SolanaExecutionResult compatible with StrategyRunner.
        """
        # Extract dry_run from context if provided
        dry_run = False
        if context is not None:
            if hasattr(context, "dry_run"):
                dry_run = context.dry_run
            elif isinstance(context, dict):
                dry_run = context.get("dry_run", False)

        outcome = await self._planner.execute_actions(
            actions=[action_bundle],
            context={"dry_run": dry_run},
        )

        fee_lamports = int(outcome.total_fee_native * 1_000_000_000) if outcome.total_fee_native else 0

        return SolanaExecutionResult(
            success=outcome.success,
            tx_hashes=outcome.tx_ids,
            total_gas_used=fee_lamports,
            receipts=outcome.receipts,
            error=outcome.error,
            fee_lamports=fee_lamports,
        )
