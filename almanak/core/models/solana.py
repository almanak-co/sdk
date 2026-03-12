"""Solana-specific execution models.

This module provides Solana-specific data structures used by the
SolanaExecutionPlanner and intent compilation pipeline.
"""

from dataclasses import dataclass, field

from almanak.core.enums import ATAPolicy, CommitmentLevel


@dataclass
class SolanaExecutionHints:
    """Solana-specific execution hints attached to intents.

    These hints are optional — the planner uses sensible defaults.
    Strategy authors can override for specific needs.

    Attributes:
        ata_policy: Whether to auto-create ATAs. Default: AUTO_CREATE.
        commitment: Confirmation commitment level. Default: CONFIRMED.
        priority_fee_lamports: Priority fee to pay for scheduling. Default: 0 (auto).
        explicit_accounts: Additional accounts to include in the transaction.
        cu_budget_override: Override CU budget instead of using simulation.
        skip_preflight: Skip preflight simulation before submission.
    """

    ata_policy: ATAPolicy = ATAPolicy.AUTO_CREATE
    commitment: CommitmentLevel = CommitmentLevel.CONFIRMED
    priority_fee_lamports: int = 0
    explicit_accounts: list[str] = field(default_factory=list)
    cu_budget_override: int | None = None
    skip_preflight: bool = False
