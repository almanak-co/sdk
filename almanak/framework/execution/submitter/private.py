"""Private transaction submitter interface and local-compatible stub."""

from __future__ import annotations

from dataclasses import dataclass

from almanak.framework.execution.interfaces import (
    SignedTransaction,
    SubmissionError,
    SubmissionResult,
    Submitter,
    TransactionReceipt,
)


@dataclass
class PrivateRelaySubmitter(Submitter):
    """Stub private submitter for local-first environments.

    This adapter is intentionally conservative: unless a real private relay
    backend is configured, submissions are rejected with explicit errors.
    """

    enabled: bool = False
    relay_name: str = "local_private_stub"

    async def submit(self, txs: list[SignedTransaction]) -> list[SubmissionResult]:
        if not self.enabled:
            return [
                SubmissionResult(
                    tx_hash=tx.tx_hash,
                    submitted=False,
                    error="Private relay submitter not configured",
                )
                for tx in txs
            ]

        raise SubmissionError("Private relay backend is not implemented in local-first stub")

    async def get_receipt(self, tx_hash: str, timeout: float = 120.0) -> TransactionReceipt:
        raise SubmissionError(f"Private relay receipt lookup unsupported in stub for tx={tx_hash}")
