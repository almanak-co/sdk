"""Bind receipt costs to a numbered block before exposing execution results."""

import asyncio
import logging
from typing import Any

from aiohttp import ClientError
from hexbytes import HexBytes
from web3.exceptions import BlockNotFound, ProviderConnectionError, TransactionNotFound

from almanak.framework.execution.interfaces import TransactionReceipt
from almanak.framework.execution.nonce_recovery import build_complete_evm_receipt

logger = logging.getLogger(__name__)


def _hash(value: Any) -> bytes:
    if not isinstance(value, str | bytes):
        raise ValueError("Missing canonical receipt/block identity")
    parsed = bytes(HexBytes(value))
    if len(parsed) != 32:
        raise ValueError("Invalid canonical receipt/block identity")
    return parsed


def _in_block(block: Any, receipt: TransactionReceipt) -> bool:
    return _hash(block["hash"]) == _hash(receipt.block_hash) and _hash(receipt.tx_hash) in {
        _hash(tx) for tx in block["transactions"]
    }


async def wait_for_canonical_receipt(web3: Any, tx_hash: str, timeout: float) -> TransactionReceipt:
    """Refetch inclusion evidence under one deadline, without waiting for a successor block.

    This is canonical inclusion, not finality. A changing numbered block restarts
    the read; RPC failure never authorizes broadcasting the transaction again.
    """
    async with asyncio.timeout(timeout):
        while True:
            stage = "receipt_observation"
            try:
                raw = await web3.eth.wait_for_transaction_receipt(HexBytes(tx_hash), timeout=timeout)
                candidate = build_complete_evm_receipt(raw, expected_tx_hash=tx_hash)
                if candidate is None:
                    raise ValueError("Incomplete transaction receipt identity or fee quantities")
                _hash(candidate.tx_hash)
                stage = "block_membership"
                block = await web3.eth.get_block(candidate.block_number)
                if _in_block(block, candidate):
                    stage = "receipt_refetch"
                    refreshed_raw = await web3.eth.get_transaction_receipt(HexBytes(tx_hash))
                    refreshed = build_complete_evm_receipt(refreshed_raw, expected_tx_hash=tx_hash)
                    if refreshed is None:
                        raise ValueError("Incomplete refreshed receipt identity or fee quantities")
                    if refreshed.block_number == candidate.block_number and _in_block(block, refreshed):
                        stage = "block_recheck"
                        final_block = await web3.eth.get_block(refreshed.block_number)
                        if _in_block(final_block, refreshed):
                            if candidate.gas_cost_wei != refreshed.gas_cost_wei:
                                logger.warning(
                                    "receipt_cost_changed tx_hash=%s observed_wei=%s confirmed_wei=%s block_hash=%s",
                                    tx_hash,
                                    candidate.gas_cost_wei,
                                    refreshed.gas_cost_wei,
                                    refreshed.block_hash,
                                )
                            return refreshed
            except (BlockNotFound, TransactionNotFound, ClientError, OSError, ProviderConnectionError) as exc:
                # A provider can lose visibility between any two reads. Discard
                # the candidate and repeat all inclusion checks under the same
                # deadline; never reuse partial proof or expose RPC credentials.
                logger.warning(
                    "receipt_observation_retry tx_hash=%s stage=%s error_type=%s",
                    tx_hash,
                    stage,
                    type(exc).__name__,
                )
            await asyncio.sleep(0.2)
