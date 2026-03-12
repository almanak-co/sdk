"""ChainExecutionStrategy ABC — abstraction for chain-family execution.

This module defines the interface that both EVM and Solana execution paths
implement. The MultiChainOrchestrator can route to the correct strategy
based on the chain family without knowing execution mechanics.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from almanak.framework.execution.outcome import ExecutionOutcome


class ChainExecutionStrategy(ABC):
    """Abstract base class for chain-family execution strategies.

    Each chain family (EVM, Solana, ...) provides a concrete implementation
    that knows how to compile, sign, submit, and confirm transactions.

    Attributes:
        chain_family: Identifies the chain family ("EVM" or "SOLANA").
        wallet_address: The wallet address used for execution.
    """

    chain_family: str
    wallet_address: str

    @abstractmethod
    async def execute_actions(
        self,
        actions: list[Any],
        context: dict[str, Any] | None = None,
    ) -> ExecutionOutcome:
        """Execute a list of actions and return a unified outcome.

        Args:
            actions: Chain-specific actions to execute.
                     EVM: list of Action/ActionBundle dicts.
                     Solana: list of SolanaInstruction or SolanaTransaction.
            context: Optional execution context (strategy_id, dry_run, etc.).

        Returns:
            ExecutionOutcome with success status, tx_ids, and enrichment data.
        """

    @abstractmethod
    async def check_connection(self) -> bool:
        """Verify connectivity to the underlying chain.

        Returns:
            True if the chain RPC is reachable, False otherwise.
        """
