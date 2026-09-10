"""Read-only transaction observations for execution recovery."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from web3.exceptions import TimeExhausted

from almanak.framework.execution.submitter.canonical_receipt import wait_for_canonical_receipt
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)


async def observe_evm_transaction(
    web3: Any, tx_hash: str, *, chain_id: int, timeout: float = 10.0
) -> gateway_pb2.TxStatus:
    """Observe canonical inclusion without signing or submitting anything.

    A confirmed outer transaction does not certify an action bundle or Safe
    inner operation. Consumers must keep those proofs separate from this receipt.
    """
    try:
        async with asyncio.timeout(timeout):
            if await web3.eth.chain_id != chain_id:
                raise ValueError("transaction_status_chain_mismatch")
            receipt = await wait_for_canonical_receipt(web3, tx_hash, timeout)
            head = await web3.eth.block_number
            if head < receipt.block_number:
                raise ValueError("transaction_status_head_behind_receipt")
            return gateway_pb2.TxStatus(
                status="confirmed" if receipt.success else "reverted",
                confirmations=head - receipt.block_number,
                block_number=receipt.block_number,
                gas_used=receipt.gas_used,
                error="" if receipt.success else "Transaction reverted",
                canonical_receipt=json.dumps(receipt.to_dict()).encode("utf-8"),
            )
    except (TimeoutError, TimeExhausted):
        return gateway_pb2.TxStatus(status="pending", error="canonical_receipt_observation_timed_out")
    except Exception as exc:
        logger.warning("transaction_status_unmeasured tx_hash=%s error_type=%s", tx_hash, type(exc).__name__)
        return gateway_pb2.TxStatus(status="unknown", error="canonical_receipt_observation_unavailable")
