"""Solana transaction data types.

Pure Python dataclasses — no external dependencies (solana-py, solders, etc.).
These types model the Solana transaction lifecycle so the rest of the SDK can
reference them without pulling in heavyweight Solana libraries.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class AccountMeta:
    """A single account entry in a Solana instruction.

    Attributes:
        pubkey: Base58-encoded public key of the account.
        is_signer: Whether this account must sign the transaction.
        is_writable: Whether the instruction may write to this account.
    """

    pubkey: str
    is_signer: bool
    is_writable: bool


@dataclass(frozen=True)
class SolanaInstruction:
    """A single Solana instruction (program call).

    Attributes:
        program_id: Base58-encoded program address.
        accounts: Ordered list of account metas.
        data: Raw instruction data as bytes.
    """

    program_id: str
    accounts: tuple[AccountMeta, ...] = ()
    data: bytes = b""


@dataclass
class SolanaTransaction:
    """An unsigned Solana transaction (v0 or legacy).

    Attributes:
        instructions: Ordered list of instructions to execute atomically.
        fee_payer: Base58-encoded public key of the fee payer.
        recent_blockhash: Recent blockhash for transaction expiry (filled JIT).
        address_lookup_tables: List of ALT addresses for v0 transactions.
        compute_units: Requested compute unit limit (0 = no SetComputeUnitLimit).
        priority_fee_lamports: Priority fee in lamports (0 = no SetComputeUnitPrice).
        metadata: Arbitrary metadata for tracing/logging.
    """

    instructions: list[SolanaInstruction] = field(default_factory=list)
    fee_payer: str = ""
    recent_blockhash: str = ""
    address_lookup_tables: list[str] = field(default_factory=list)
    compute_units: int = 0
    priority_fee_lamports: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SignedSolanaTransaction:
    """A signed Solana transaction ready for submission.

    Attributes:
        raw_tx: Serialized transaction bytes (base64 or raw).
        signature: Base58-encoded transaction signature.
        unsigned_tx: Reference to the original unsigned transaction.
    """

    raw_tx: bytes
    signature: str
    unsigned_tx: SolanaTransaction


@dataclass
class SolanaTransactionReceipt:
    """Receipt returned after a Solana transaction is confirmed.

    Attributes:
        signature: Base58-encoded transaction signature.
        slot: Slot in which the transaction was confirmed.
        block_time: Unix timestamp of the block (None if unavailable).
        fee_lamports: Transaction fee in lamports.
        success: Whether the transaction succeeded (no error).
        err: Error object from the RPC response (None on success).
        logs: Program log messages emitted during execution.
        pre_token_balances: Token balances before execution.
        post_token_balances: Token balances after execution.
    """

    signature: str
    slot: int
    block_time: int | None = None
    fee_lamports: int = 0
    success: bool = True
    err: dict[str, Any] | None = None
    logs: list[str] = field(default_factory=list)
    pre_token_balances: list[dict[str, Any]] = field(default_factory=list)
    post_token_balances: list[dict[str, Any]] = field(default_factory=list)
