"""Solana execution engine — types, planner, RPC client, and signer.

This package provides Solana-specific execution primitives for the Almanak SDK.
Uses solders for Ed25519 keypair management and transaction serialization.
"""

from almanak.framework.execution.solana.planner import SolanaExecutionPlanner
from almanak.framework.execution.solana.rpc import (
    SolanaRpcClient,
    SolanaRpcConfig,
    SolanaRpcError,
    SolanaTransactionError,
)
from almanak.framework.execution.solana.signer import SolanaSigner, SolanaSignerError
from almanak.framework.execution.solana.types import (
    AccountMeta,
    SignedSolanaTransaction,
    SolanaInstruction,
    SolanaTransaction,
    SolanaTransactionReceipt,
)

__all__ = [
    "AccountMeta",
    "SignedSolanaTransaction",
    "SolanaExecutionPlanner",
    "SolanaInstruction",
    "SolanaRpcClient",
    "SolanaRpcConfig",
    "SolanaRpcError",
    "SolanaSigner",
    "SolanaSignerError",
    "SolanaTransaction",
    "SolanaTransactionError",
    "SolanaTransactionReceipt",
]
