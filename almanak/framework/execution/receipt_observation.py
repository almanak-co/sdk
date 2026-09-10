"""Decode gateway receipt observations without inferring action completion."""

from __future__ import annotations

import json
from typing import Any

from hexbytes import HexBytes

from almanak.framework.execution.interfaces import SubmissionError, TransactionReceipt
from almanak.framework.execution.nonce_recovery import build_complete_evm_receipt


def decode_canonical_receipt(response: Any, tx_hash: str) -> TransactionReceipt:
    """Require complete, hash-bound evidence from a compatible gateway.

    Status-only replies from older gateways are insufficient. This validates
    the response envelope, not a sealed execution plan or Safe inner outcome.
    """
    try:
        payload = getattr(response, "canonical_receipt", b"")
        if not isinstance(payload, bytes) or not payload:
            raise ValueError("canonical receipt evidence missing")
        raw = json.loads(payload)
        if not isinstance(raw, dict):
            raise ValueError("canonical receipt must be an object")
        receipt = build_complete_evm_receipt(raw, expected_tx_hash=tx_hash)
        if receipt is None:
            raise ValueError("canonical receipt evidence incomplete")
        if len(HexBytes(receipt.tx_hash)) != 32 or len(HexBytes(receipt.block_hash)) != 32:
            raise ValueError("canonical receipt hash malformed")
        if (
            response.status != ("confirmed" if receipt.success else "reverted")
            or response.block_number != receipt.block_number
            or response.gas_used != receipt.gas_used
        ):
            raise ValueError("canonical receipt contradicts status envelope")
        return receipt
    except (AttributeError, TypeError, ValueError) as exc:
        raise SubmissionError(
            reason="Gateway has no complete canonical receipt observation",
            tx_hash=tx_hash,
            recoverable=True,
        ) from exc
