"""Unified execution outcome — chain-agnostic result type.

ExecutionOutcome provides a single shape for execution results across EVM
and Solana chains. Existing result types (ExecutionResult, GatewayExecutionResult)
gain a `to_outcome()` method that converts to this common format.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.execution.extracted_data import LPCloseData, SwapAmounts


@dataclass
class ExecutionOutcome:
    """Chain-agnostic execution result.

    Attributes:
        success: Whether all transactions / instructions succeeded.
        tx_ids: Transaction identifiers — tx hashes (EVM) or signatures (Solana).
        receipts: Raw receipt dicts for downstream parsing.
        total_fee_native: Total fee in native units (ETH wei / SOL lamports).
        error: Human-readable error message (None on success).
        chain_family: "EVM" or "SOLANA".
        position_id: LP position ID extracted by ResultEnricher (int for NFT, str for pool address).
        swap_amounts: Swap data extracted by ResultEnricher.
        lp_close_data: LP close data extracted by ResultEnricher.
        extracted_data: Flexible dict for protocol-specific data.
        extraction_warnings: Non-fatal warnings from extraction process.
    """

    success: bool
    tx_ids: list[str] = field(default_factory=list)
    receipts: list[dict[str, Any]] = field(default_factory=list)
    total_fee_native: Decimal = Decimal(0)
    error: str | None = None
    chain_family: str = "EVM"

    # Enrichment fields (same shape as ExecutionResult / GatewayExecutionResult)
    position_id: int | str | None = None
    swap_amounts: SwapAmounts | None = None
    lp_close_data: LPCloseData | None = None
    extracted_data: dict[str, Any] = field(default_factory=dict)
    extraction_warnings: list[str] = field(default_factory=list)
